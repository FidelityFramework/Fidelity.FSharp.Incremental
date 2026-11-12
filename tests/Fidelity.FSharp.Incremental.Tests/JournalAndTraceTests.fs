module Fidelity.FSharp.Incremental.Tests.JournalAndTraceTests

open System
open System.Reflection
open System.Reflection.Emit
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

module TraceCompilationProbes =
    let mutable sideEffects = 0

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let untracedFourArg (work: WorkId) (slot: ReadSlotId) (value: int) : int =
        Trace.named(
            (fun () ->
                sideEffects <- sideEffects + 1
                sprintf "untraced-four-%d" value),
            work,
            slot,
            value)

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let untracedExplicitOff (work: WorkId) (slot: ReadSlotId) (value: int) : int =
        Trace.named(
            Trace.Off,
            (fun () ->
                sideEffects <- sideEffects + 1
                sprintf "untraced-off-%d" value),
            work,
            slot,
            value)

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let runtimeGated (enabled: bool) (sink: TraceLabel -> unit) (work: WorkId) (slot: ReadSlotId) (value: int) : int =
        Trace.named(
            enabled,
            sink,
            (fun () ->
                sideEffects <- sideEffects + 1
                sprintf "runtime-gated-%d" value),
            work,
            slot,
            value)

module private IlDisassembler =
    let private singleByte, twoByte =
        let one = Array.create 256 OpCodes.Nop
        let two = Array.create 256 OpCodes.Nop
        for field in typeof<OpCodes>.GetFields(BindingFlags.Public ||| BindingFlags.Static) do
            if field.FieldType = typeof<OpCode> then
                let op = field.GetValue(null) :?> OpCode
                let raw = uint16 op.Value
                if raw < 0x100us then
                    one[int raw] <- op
                elif (raw &&& 0xFF00us) = 0xFE00us then
                    two[int (raw &&& 0xFFus)] <- op
        one, two

    let decodeOpCodes (il: byte[]) : OpCode list =
        let ops = ResizeArray<OpCode>()
        let mutable i = 0
        while i < il.Length do
            let op =
                if il[i] = 0xFEuy then
                    i <- i + 1
                    twoByte[int il[i]]
                else
                    singleByte[int il[i]]
            i <- i + 1
            ops.Add op
            match op.OperandType with
            | OperandType.InlineNone -> ()
            | OperandType.ShortInlineBrTarget
            | OperandType.ShortInlineI
            | OperandType.ShortInlineVar -> i <- i + 1
            | OperandType.InlineVar -> i <- i + 2
            | OperandType.InlineBrTarget
            | OperandType.InlineField
            | OperandType.InlineI
            | OperandType.InlineMethod
            | OperandType.InlineSig
            | OperandType.InlineString
            | OperandType.InlineTok
            | OperandType.InlineType
            | OperandType.ShortInlineR -> i <- i + 4
            | OperandType.InlineI8
            | OperandType.InlineR -> i <- i + 8
            | OperandType.InlineSwitch ->
                let count = BitConverter.ToInt32(il, i)
                i <- i + 4 + (count * 4)
            | other -> failwithf "Unexpected CIL operand kind: %A" other
        ops |> Seq.toList

let private containsUInt64Pattern (haystack: byte[]) (needleValue: uint64) =
    let needle = BitConverter.GetBytes needleValue
    if haystack.Length < needle.Length then
        false
    else
        let mutable found = false
        let mutable offset = 0
        while not found && offset <= haystack.Length - needle.Length do
            let mutable matchesAll = true
            let mutable j = 0
            while matchesAll && j < needle.Length do
                if haystack[offset + j] <> needle[j] then
                    matchesAll <- false
                j <- j + 1
            if matchesAll then
                found <- true
            offset <- offset + 1
        found

let private assertEqual (expected: 'T) (actual: 'T) =
    if actual <> expected then
        Assert.Fail(sprintf "Expected:\n%A\nActual:\n%A" expected actual)

let private sendOk (host: Host) (action: Action) =
    match host.Send action with
    | Ok effects -> effects
    | Error err -> Assert.Fail(sprintf "Expected Ok from Host.Send(%A), got Error %A" action err); []

let private admitAndObserveOk (handle: AsyncMailbox.Handle) (action: Action) =
    match AsyncMailbox.admit action handle with
    | Error err -> Assert.Fail(sprintf "Expected Ok admission for %A, got %A" action err)
    | Ok op ->
        match Async.RunSynchronously(AsyncMailbox.observe op) with
        | Ok _ -> ()
        | Error err -> Assert.Fail(sprintf "Expected Ok receipt for %A, got %A" action err)

let rec private awaitSingleSuspension (handle: AsyncMailbox.Handle) : SuspensionHandle =
    let snap, changed = AsyncMailbox.watch handle
    match snap.Suspensions with
    | [ susp ] -> susp
    | _ ->
        Async.RunSynchronously changed
        awaitSingleSuspension handle

let private projectPerTargetEffects (effects: Effect list) =
    effects
    |> List.map (fun eff ->
        let key, normalizedAction =
            match eff.Action with
            | EffectAction.Start req ->
                let (WorkId w) = req.Definition.Work
                (1, w), eff.Action
            | EffectAction.Suspended h ->
                let (AttemptId a) = h.Attempt
                (2, a), eff.Action
            | EffectAction.Continue req ->
                let (WorkId w) = req.Start.Definition.Work
                (1, w), eff.Action
            | EffectAction.Cancel(AttemptId a) ->
                (2, a), eff.Action
            | EffectAction.Drain(AttemptId a) ->
                (2, a), eff.Action
            | EffectAction.Withdraw h ->
                let (WorkId w) = h.Work
                (1, w), EffectAction.Withdraw { h with Eligibility = EligibilityId 0UL }
            | EffectAction.Offer h ->
                let (WorkId w) = h.Work
                (1, w), EffectAction.Offer { h with Eligibility = EligibilityId 0UL }
            | EffectAction.ScopeClosed(ScopeId s) ->
                (3, s), eff.Action
            | EffectAction.EpochDrained ->
                (4, 0UL), eff.Action
        key, { eff with Action = normalizedAction })
    |> List.groupBy fst
    |> List.sortBy fst
    |> List.map (fun (key, items) -> key, items |> List.map snd)

[<TestFixture>]
type JournalAndTraceTests() =

    [<Test>]
    member _.UntracedTraceNamedEvaluatesNeitherThunkNorAllocatesAndErasesInCompiledIL() =
        TraceCompilationProbes.sideEffects <- 0
        let work = WorkId 42UL
        let slot = ReadSlotId 7UL

        // Warmup JIT before thread allocation measurement.
        let noopSink : TraceLabel -> unit = ignore
        let warm1 = TraceCompilationProbes.untracedFourArg work slot 10
        let warm2 = TraceCompilationProbes.untracedExplicitOff work slot 20
        let warm3 = TraceCompilationProbes.runtimeGated false noopSink work slot 30
        Assert.That(warm1, Is.EqualTo 10)
        Assert.That(warm2, Is.EqualTo 20)
        Assert.That(warm3, Is.EqualTo 30)
        Assert.That(TraceCompilationProbes.sideEffects, Is.EqualTo 0)

        let mutable acc = 0
#if !DEBUG
        let beforeReleaseBytes = GC.GetAllocatedBytesForCurrentThread()
        for i in 1 .. 10000 do
            acc <- acc + TraceCompilationProbes.untracedFourArg work slot i
            acc <- acc + TraceCompilationProbes.untracedExplicitOff work slot i
            acc <- acc + TraceCompilationProbes.runtimeGated false noopSink work slot i
            acc <- acc + Trace.named(false, (fun _ -> ()), (fun () -> sprintf "inline-%d" i), work, slot, i)
        let releaseAllocatedDelta = GC.GetAllocatedBytesForCurrentThread() - beforeReleaseBytes

        Assert.That(acc, Is.GreaterThan 0)
        Assert.That(TraceCompilationProbes.sideEffects, Is.EqualTo 0)
        Assert.That(releaseAllocatedDelta, Is.EqualTo 0L)

        // Inspect compiled Release CIL of untracedFourArg and untracedExplicitOff.
        let probeType = typeof<WorkId>.Assembly.GetType("Fidelity.FSharp.Incremental.Trace")
        Assert.That(probeType, Is.Not.Null)

        let containerType =
            Assembly.GetExecutingAssembly().GetTypes()
            |> Array.find (fun t -> t.Name = "TraceCompilationProbes")

        for methodName in [ "untracedFourArg"; "untracedExplicitOff" ] do
            let methodInfo = containerType.GetMethod(methodName, BindingFlags.Public ||| BindingFlags.Static)
            Assert.That(methodInfo, Is.Not.Null, sprintf "Missing probe method %s" methodName)
            let body = methodInfo.GetMethodBody()
            Assert.That(body, Is.Not.Null)
            let opCodes = IlDisassembler.decodeOpCodes (body.GetILAsByteArray())
            let forbidden =
                opCodes
                |> List.filter (fun op ->
                    op = OpCodes.Newobj
                    || op = OpCodes.Ldstr
                    || op = OpCodes.Call
                    || op = OpCodes.Callvirt)
            Assert.That(forbidden, Is.Empty, sprintf "Expected erased identity IL in %s, found %A" methodName forbidden)
#else
        for i in 1 .. 1000 do
            acc <- acc + TraceCompilationProbes.untracedFourArg work slot i
            acc <- acc + TraceCompilationProbes.untracedExplicitOff work slot i
            acc <- acc + TraceCompilationProbes.runtimeGated false noopSink work slot i
            acc <- acc + Trace.named(false, (fun _ -> ()), (fun () -> sprintf "inline-%d" i), work, slot, i)
        Assert.That(acc, Is.GreaterThan 0)
        Assert.That(TraceCompilationProbes.sideEffects, Is.EqualTo 0)
#endif

    [<Test>]
    member _.TracedTraceNamedRecordsCausalEdgesForClefWhy() =
        let epoch = EpochId 1UL
        let scope = ScopeId 1UL
        let inputA = InputId 10UL
        let inputB = InputId 20UL
        let workStable = WorkId 1UL
        let workParse = WorkId 2UL
        let workCheck = WorkId 3UL
        let slotParseInput = ReadSlotId 0UL
        let slotCheckStable = ReadSlotId 0UL
        let slotCheckParse = ReadSlotId 1UL

        let journal = InMemoryInputJournal(epoch)

        let stableDef =
            Trace.named(
                journal.Trace,
                (fun () -> "stable:input-a"),
                workStable,
                ReadSlotId 0UL,
                { Work = workStable
                  Stamp = DefinitionStamp 1UL
                  Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input inputA } ] })

        let parseDef =
            Trace.named(
                journal.Trace,
                (fun () -> "parse:file.clef"),
                workParse,
                slotParseInput,
                { Work = workParse
                  Stamp = DefinitionStamp 1UL
                  Reads = [ { Slot = slotParseInput; Source = ReadSource.Input inputB } ] })

        let checkDef =
            Trace.named(
                journal.Trace,
                (fun () -> "check:parse-tree"),
                workCheck,
                slotCheckParse,
                { Work = workCheck
                  Stamp = DefinitionStamp 1UL
                  Reads = [
                      { Slot = slotCheckStable; Source = ReadSource.Work workStable }
                      { Slot = slotCheckParse; Source = ReadSource.Work workParse }
                  ] })

        let step = Journal.step journal
        let run cmd state =
            match step { Epoch = epoch; Action = cmd } state with
            | Ok(next, _) -> next
            | Error err -> Assert.Fail(sprintf "Unexpected protocol error %A" err); state

        let mutable state = Core.init epoch
        state <- run (Action.SetInputs [
            { Input = inputA; Stamp = InputStamp 1UL; Value = ValueToken 100UL }
            { Input = inputB; Stamp = InputStamp 1UL; Value = ValueToken 200UL }
        ]) state
        state <- run (Action.ReserveScope(scope, RevisionId 1UL)) state
        state <- run (Action.ReplaceScope(scope, RevisionId 1UL, [
            ScopeEntry.Define stableDef
            ScopeEntry.Define parseDef
            ScopeEntry.Define checkDef
        ])) state
        state <- run (Action.Demand(DemandId 1UL, workCheck)) state
        state <- run (Action.Finished(AttemptId 1UL, Completion.Succeeded(ValueToken 101UL))) state
        state <- run (Action.Drained(AttemptId 1UL)) state
        state <- run (Action.Finished(AttemptId 2UL, Completion.Succeeded(ValueToken 201UL))) state
        state <- run (Action.Drained(AttemptId 2UL)) state
        state <- run (Action.Finished(AttemptId 3UL, Completion.Succeeded(ValueToken 301UL))) state
        state <- run (Action.Drained(AttemptId 3UL)) state

        // Cold initial execution has zero invalidation restart edges.
        Assert.That(journal.CausalEdges, Is.Empty)

        // Mutate only inputB: workStable must not restart; workParse and workCheck must restart with causal edges.
        state <- run (Action.SetInputs [
            { Input = inputB; Stamp = InputStamp 2UL; Value = ValueToken 205UL }
        ]) state
        state <- run (Action.Finished(AttemptId 4UL, Completion.Succeeded(ValueToken 206UL))) state
        state <- run (Action.Drained(AttemptId 4UL)) state

        let edges = journal.CausalEdges
        Assert.That(edges.Length, Is.EqualTo 2)

        Assert.That(
            edges[0],
            Is.EqualTo {
                Epoch = epoch
                Trigger = CausalTrigger.InputChanged(inputB, InputStamp 2UL)
                InvalidatedSlot = Some slotParseInput
                SourceWork = None
                AffectedWork = workParse
                RestartedAttempt = Some(AttemptId 4UL)
                Label = Some "parse:file.clef"
            })

        Assert.That(
            edges[1],
            Is.EqualTo {
                Epoch = epoch
                Trigger = CausalTrigger.InputChanged(inputB, InputStamp 2UL)
                InvalidatedSlot = Some slotCheckParse
                SourceWork = Some workParse
                AffectedWork = workCheck
                RestartedAttempt = Some(AttemptId 5UL)
                Label = Some "check:parse-tree"
            })

    [<Test>]
    member _.JournalDoesNotRetainValueTokenPayloadsAtRest() =
        let sentinelInput1 = 0xDEAD_BEEF_CAFE_BABEUL
        let sentinelInput2 = 0x0123_4567_89AB_CDEFUL
        let sentinelSuspendEnv = 0xFEED_FACE_1122_3344UL
        let sentinelResumeResp = 0x5566_7788_99AA_BBCCUL
        let sentinelOutput = 0xF00D_BAAD_C0DE_7777UL

        let epoch = EpochId 7UL
        let journal = InMemoryInputJournal()
        let step = Journal.step journal
        let apply action state =
            match step { Epoch = epoch; Action = action } state with
            | Ok(next, _) -> next
            | Error err -> Assert.Fail(sprintf "Unexpected step error: %A" err); state

        let mutable state = Core.init epoch
        state <- apply (Action.SetInputs [
            { Input = InputId 1UL; Stamp = InputStamp 1UL; Value = ValueToken sentinelInput1 }
            { Input = InputId 2UL; Stamp = InputStamp 1UL; Value = ValueToken sentinelInput2 }
        ]) state
        state <- apply (Action.ReserveScope(ScopeId 1UL, RevisionId 1UL)) state
        state <- apply (Action.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [
            ScopeEntry.Define {
                Work = WorkId 1UL
                Stamp = DefinitionStamp 1UL
                Reads = [
                    { Slot = ReadSlotId 0UL; Source = ReadSource.Input(InputId 1UL) }
                    { Slot = ReadSlotId 1UL; Source = ReadSource.Input(InputId 2UL) }
                ]
            }
        ])) state
        state <- apply (Action.Demand(DemandId 1UL, WorkId 1UL)) state
        state <- apply (Action.Suspend(AttemptId 1UL, StepId 9UL, ValueToken sentinelSuspendEnv)) state
        let susp = (Core.trySuspension (AttemptId 1UL) state).Value
        state <- apply (Action.Resume(susp, ValueToken sentinelResumeResp)) state
        state <- apply (Action.Finished(AttemptId 1UL, Completion.Succeeded(ValueToken sentinelOutput))) state
        state <- apply (Action.Drained(AttemptId 1UL)) state

        let bytes = journal.ToBytes()
        for sentinel in [ sentinelInput1; sentinelInput2; sentinelSuspendEnv; sentinelResumeResp; sentinelOutput ] do
            Assert.That(
                containsUInt64Pattern bytes sentinel,
                Is.False,
                sprintf "Serialized V1 journal leaked raw ValueToken 0x%016X" sentinel)

    [<Test>]
    member _.DiagnosticAndReenactReplayMultiEpochReservationCancellationAndDiamondCutoffBitForBit() : Task = task {
        let journal = InMemoryInputJournal()
        let scope = ScopeId 1UL
        let inputStable = InputId 1UL
        let inputEdited = InputId 2UL
        let workLeft = WorkId 1UL
        let workRight = WorkId 2UL
        let workJoin = WorkId 3UL
        let workSlow = WorkId 4UL

        let leftDef =
            Trace.named(
                journal.Trace,
                (fun () -> "diamond:left"),
                workLeft,
                ReadSlotId 0UL,
                { Work = workLeft
                  Stamp = DefinitionStamp 1UL
                  Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input inputStable } ] })

        let rightDef =
            Trace.named(
                journal.Trace,
                (fun () -> "diamond:right"),
                workRight,
                ReadSlotId 0UL,
                { Work = workRight
                  Stamp = DefinitionStamp 1UL
                  Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input inputEdited } ] })

        let joinDef =
            Trace.named(
                journal.Trace,
                (fun () -> "diamond:join-right"),
                workJoin,
                ReadSlotId 1UL,
                { Work = workJoin
                  Stamp = DefinitionStamp 1UL
                  Reads = [
                      { Slot = ReadSlotId 0UL; Source = ReadSource.Work workLeft }
                      { Slot = ReadSlotId 1UL; Source = ReadSource.Work workRight }
                  ] })

        let slowDef stamp = {
            Work = workSlow
            Stamp = DefinitionStamp stamp
            Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input inputStable } ]
        }

        let makeEvaluator (visitsCounter: int ref) : Evaluator =
            fun request token -> task {
                if request.Definition.Work = workSlow then
                    let tcs = TaskCompletionSource<ValueToken>(TaskCreationOptions.RunContinuationsAsynchronously)
                    use _reg = token.Register(Action(fun () -> tcs.TrySetCanceled(token) |> ignore))
                    return! tcs.Task
                else
                    token.ThrowIfCancellationRequested()
                    Interlocked.Increment(visitsCounter) |> ignore
                    let total =
                        request.Reads
                        |> List.sumBy (fun r ->
                            match r.Value with
                            | ReadValue.Input(_, _, ValueToken v)
                            | ReadValue.Work(_, _, _, ValueToken v) -> v)
                    return ValueToken(total + 1UL)
            }

        let liveVisitsEpoch1 = ref 0
        let liveEpoch1Effects, liveEpoch1Snapshot =
            let runEpoch1 () = task {
                let host = new Host(EpochId 1UL, 1, makeEvaluator liveVisitsEpoch1, journal)
                sendOk host (Action.SetInputs [
                    { Input = inputStable; Stamp = InputStamp 1UL; Value = ValueToken 10UL }
                    { Input = inputEdited; Stamp = InputStamp 1UL; Value = ValueToken 20UL }
                ]) |> ignore
                sendOk host (Action.ReserveScope(scope, RevisionId 1UL)) |> ignore
                sendOk host (Action.ReplaceScope(scope, RevisionId 1UL, [
                    ScopeEntry.Define leftDef
                    ScopeEntry.Define rightDef
                    ScopeEntry.Define joinDef
                    ScopeEntry.Define(slowDef 1UL)
                ])) |> ignore
                sendOk host (Action.Demand(DemandId 1UL, workJoin)) |> ignore
                do! host.WaitForIdleAsync(CancellationToken.None)
                Assert.That(liveVisitsEpoch1.Value, Is.EqualTo 3)

                // Selective diamond reuse: reserve, mutate only inputEdited, and retain all definitions.
                sendOk host (Action.ReserveScope(scope, RevisionId 2UL)) |> ignore
                sendOk host (Action.SetInputs [
                    { Input = inputEdited; Stamp = InputStamp 2UL; Value = ValueToken 50UL }
                ]) |> ignore
                sendOk host (Action.ReplaceScope(scope, RevisionId 2UL, [
                    ScopeEntry.Retain workLeft
                    ScopeEntry.Retain workRight
                    ScopeEntry.Retain workJoin
                    ScopeEntry.Retain workSlow
                ])) |> ignore
                do! host.WaitForIdleAsync(CancellationToken.None)
                // Only workRight and workJoin re-executed; workLeft reused its cached attempt.
                Assert.That(liveVisitsEpoch1.Value, Is.EqualTo 5)

                // In-flight cancellation: demand workSlow and release before completion.
                sendOk host (Action.Demand(DemandId 2UL, workSlow)) |> ignore
                sendOk host (Action.Release(DemandId 2UL)) |> ignore
                do! host.WaitForIdleAsync(CancellationToken.None)

                do! host.CloseAsync()
                let effects = host.DrainEvents() |> List.map Journal.canonicalizeEffect
                let snap = host.Snapshot |> Journal.canonicalizeSnapshot
                return effects, snap
            }
            (runEpoch1 ()).GetAwaiter().GetResult()

        let liveVisitsEpoch2 = ref 0
        let liveEpoch2Effects, liveEpoch2Snapshot =
            let runEpoch2 () = task {
                let host = new Host(EpochId 2UL, 1, makeEvaluator liveVisitsEpoch2, journal)
                sendOk host (Action.SetInputs [
                    { Input = inputStable; Stamp = InputStamp 10UL; Value = ValueToken 7UL }
                ]) |> ignore
                sendOk host (Action.ReserveScope(scope, RevisionId 1UL)) |> ignore
                sendOk host (Action.ReplaceScope(scope, RevisionId 1UL, [
                    ScopeEntry.Define leftDef
                ])) |> ignore
                sendOk host (Action.Demand(DemandId 10UL, workLeft)) |> ignore
                do! host.WaitForIdleAsync(CancellationToken.None)

                sendOk host (Action.ReserveScope(scope, RevisionId 2UL)) |> ignore
                sendOk host (Action.ReplaceScope(scope, RevisionId 2UL, [
                    ScopeEntry.Define { leftDef with Stamp = DefinitionStamp 2UL }
                ])) |> ignore
                do! host.WaitForIdleAsync(CancellationToken.None)
                Assert.That(liveVisitsEpoch2.Value, Is.EqualTo 2)

                do! host.CloseAsync()
                let effects = host.DrainEvents() |> List.map Journal.canonicalizeEffect
                let snap = host.Snapshot |> Journal.canonicalizeSnapshot
                return effects, snap
            }
            (runEpoch2 ()).GetAwaiter().GetResult()

        let bytes = journal.ToBytes()

        // 1. Pure Diagnostic replay over V1 bytes (zero evaluators running).
        let diagReport =
            match Replay.diagnosticBytes bytes with
            | Ok report -> report
            | Error err -> failwithf "Expected Ok from diagnosticBytes, got %A" err

        Assert.That(diagReport.Epochs.Length, Is.EqualTo 2)
        assertEqual (EpochId 1UL) diagReport.Epochs[0].Epoch
        assertEqual liveEpoch1Effects diagReport.Epochs[0].Effects
        assertEqual liveEpoch1Snapshot diagReport.Epochs[0].FinalSnapshot
        assertEqual (EpochId 2UL) diagReport.Epochs[1].Epoch
        assertEqual liveEpoch2Effects diagReport.Epochs[1].Effects
        assertEqual liveEpoch2Snapshot diagReport.Epochs[1].FinalSnapshot
        assertEqual journal.CausalEdges diagReport.Causes

        // 2. Live Reenact replay over V1 bytes (live Host evaluators re-executing).
        let reenactVisitsEpoch1 = ref 0
        let reenactVisitsEpoch2 = ref 0
        let config: ReenactHostConfig = {
            MaxConcurrency = 1
            ResolveInput = fun _ _ (InputStamp stamp) -> ValueToken(stamp * 10UL)
            Evaluator = fun epoch ->
                if epoch = EpochId 1UL then makeEvaluator reenactVisitsEpoch1
                else makeEvaluator reenactVisitsEpoch2
        }

        use cts = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        let! reenactOutcome = Replay.reenactBytesWithHost config bytes cts.Token
        let reenactReport =
            match reenactOutcome with
            | Ok report -> report
            | Error err -> failwithf "Expected Ok from reenactBytesWithHost, got %A" err

        assertEqual diagReport.Version reenactReport.Version
        assertEqual diagReport.Causes reenactReport.Causes
        Assert.That(reenactReport.Epochs.Length, Is.EqualTo 2)
        for idx in 0 .. 1 do
            assertEqual diagReport.Epochs[idx].Epoch reenactReport.Epochs[idx].Epoch
            assertEqual diagReport.Epochs[idx].Causes reenactReport.Epochs[idx].Causes
            assertEqual diagReport.Epochs[idx].FinalSnapshot reenactReport.Epochs[idx].FinalSnapshot
            assertEqual diagReport.Epochs[idx].Effects reenactReport.Epochs[idx].Effects
        Assert.That(reenactVisitsEpoch1.Value, Is.EqualTo 5)
        Assert.That(reenactVisitsEpoch2.Value, Is.EqualTo 2)

        let openEntries =
            journal.Entries
            |> List.filter (function JournalEntry.ExternalCommand(_, ExternalActionFrame.Retire) -> false | _ -> true)
        let openDiagnostic = Replay.diagnostic openEntries
        let! openReenact = Replay.reenactWithHost config openEntries CancellationToken.None
        match openDiagnostic, openReenact with
        | Ok expected, Ok actual ->
            assertEqual expected.Epochs actual.Epochs
        | other -> Assert.Fail(sprintf "Expected open epoch parity, got %A" other)
    }

    [<Test>]
    member _.DiagnosticAndReenactMailboxReplaySuspensionResumptionAndCancellationParity() : Task = task {
        let epoch1 = EpochId 5UL
        let epoch2 = EpochId 6UL
        let scope = ScopeId 1UL
        let workStepped = WorkId 1UL
        let workCancel = WorkId 2UL
        let workLeft = WorkId 3UL
        let workRight = WorkId 4UL
        let workJoin = WorkId 5UL
        let journal = InMemoryInputJournal()

        let makeEvaluator (visits: int ref) : WorkflowEvaluator =
            fun invocation cancellation -> async {
                match invocation with
                | StepInvocation.Start req when req.Definition.Work = workStepped ->
                    return StepOutcome.Suspend(StepId 1UL, ValueToken 777UL)
                | StepInvocation.Resume req when req.Start.Definition.Work = workStepped ->
                    let (ValueToken resp) = req.Response
                    return StepOutcome.Complete(Completion.Succeeded(ValueToken(resp + 1UL)))
                | StepInvocation.Start req when req.Definition.Work = workCancel ->
                    while not (WorkCancellation.isRequested cancellation) do
                        do! Async.Sleep 5
                    return StepOutcome.Complete Completion.Cancelled
                | StepInvocation.Start req ->
                    Interlocked.Increment(visits) |> ignore
                    let sum =
                        req.Reads
                        |> List.sumBy (fun r ->
                            match r.Value with
                            | ReadValue.Input(_, _, ValueToken v)
                            | ReadValue.Work(_, _, _, ValueToken v) -> v)
                    return StepOutcome.Complete(Completion.Succeeded(ValueToken(sum + 1UL)))
                | _ ->
                    return StepOutcome.Complete Completion.Cancelled
            }

        let liveVisits = ref 0
        let settings1: AsyncMailbox.Settings = {
            Epoch = epoch1
            MaxConcurrency = 1
            CommandCapacity = 32
        }
        let handle =
            match AsyncMailbox.createWithJournal settings1 journal (makeEvaluator liveVisits) with
            | Ok h -> h
            | Error err -> failwithf "Unexpected config error: %A" err
        assertEqual (Ok()) (AsyncMailbox.start handle)

        admitAndObserveOk handle (Action.SetInputs [
            { Input = InputId 1UL; Stamp = InputStamp 1UL; Value = ValueToken 42UL }
            { Input = InputId 2UL; Stamp = InputStamp 1UL; Value = ValueToken 10UL }
        ])
        admitAndObserveOk handle (Action.ReserveScope(scope, RevisionId 1UL))
        admitAndObserveOk handle (Action.ReplaceScope(scope, RevisionId 1UL, [
            ScopeEntry.Define {
                Work = workStepped
                Stamp = DefinitionStamp 1UL
                Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input(InputId 1UL) } ]
            }
            ScopeEntry.Define {
                Work = workCancel
                Stamp = DefinitionStamp 1UL
                Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input(InputId 1UL) } ]
            }
            ScopeEntry.Define {
                Work = workLeft
                Stamp = DefinitionStamp 1UL
                Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input(InputId 1UL) } ]
            }
            ScopeEntry.Define {
                Work = workRight
                Stamp = DefinitionStamp 1UL
                Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input(InputId 2UL) } ]
            }
            ScopeEntry.Define {
                Work = workJoin
                Stamp = DefinitionStamp 1UL
                Reads = [
                    { Slot = ReadSlotId 0UL; Source = ReadSource.Work workLeft }
                    { Slot = ReadSlotId 1UL; Source = ReadSource.Work workRight }
                ]
            }
        ]))

        // 1. Diamond evaluation + selective cutoff in AsyncMailbox (FIFO deterministic queue).
        admitAndObserveOk handle (Action.Demand(DemandId 10UL, workJoin))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.waitForIdle handle))
        Assert.That(liveVisits.Value, Is.EqualTo 3)

        admitAndObserveOk handle (Action.ReserveScope(scope, RevisionId 2UL))
        admitAndObserveOk handle (Action.SetInputs [
            { Input = InputId 2UL; Stamp = InputStamp 2UL; Value = ValueToken 25UL }
        ])
        admitAndObserveOk handle (Action.ReplaceScope(scope, RevisionId 2UL, [
            ScopeEntry.Retain workStepped
            ScopeEntry.Retain workCancel
            ScopeEntry.Retain workLeft
            ScopeEntry.Retain workRight
            ScopeEntry.Retain workJoin
        ]))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.waitForIdle handle))
        Assert.That(liveVisits.Value, Is.EqualTo 5)
        admitAndObserveOk handle (Action.Release(DemandId 10UL))

        // 2. Suspension & resumption.
        admitAndObserveOk handle (Action.Demand(DemandId 1UL, workStepped))
        let liveSusp = awaitSingleSuspension handle
        admitAndObserveOk handle (Action.Resume(liveSusp, ValueToken 99UL))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.waitForIdle handle))

        // 3. In-flight cancellation & retirement.
        admitAndObserveOk handle (Action.Demand(DemandId 2UL, workCancel))
        admitAndObserveOk handle (Action.Release(DemandId 2UL))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.waitForIdle handle))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.close handle))

        let liveEffects1 = AsyncMailbox.drainEvents handle |> List.map Journal.canonicalizeEffect
        let liveSnapshot1 = (AsyncMailbox.snapshot handle).Graph |> Journal.canonicalizeSnapshot

        // 4. Second epoch on the same journal.
        let settings2: AsyncMailbox.Settings = {
            Epoch = epoch2
            MaxConcurrency = 1
            CommandCapacity = 32
        }
        let handle2 =
            match AsyncMailbox.createWithJournal settings2 journal (makeEvaluator liveVisits) with
            | Ok h -> h
            | Error err -> failwithf "Unexpected config error: %A" err
        assertEqual (Ok()) (AsyncMailbox.start handle2)
        admitAndObserveOk handle2 (Action.SetInputs [
            { Input = InputId 1UL; Stamp = InputStamp 1UL; Value = ValueToken 7UL }
        ])
        admitAndObserveOk handle2 (Action.ReserveScope(scope, RevisionId 1UL))
        admitAndObserveOk handle2 (Action.ReplaceScope(scope, RevisionId 1UL, [
            ScopeEntry.Define {
                Work = workLeft
                Stamp = DefinitionStamp 1UL
                Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input(InputId 1UL) } ]
            }
        ]))
        admitAndObserveOk handle2 (Action.Demand(DemandId 1UL, workLeft))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.waitForIdle handle2))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.close handle2))

        let liveEffects2 = AsyncMailbox.drainEvents handle2 |> List.map Journal.canonicalizeEffect
        let liveSnapshot2 = (AsyncMailbox.snapshot handle2).Graph |> Journal.canonicalizeSnapshot
        let bytes = journal.ToBytes()

        let diagReport =
            match Replay.diagnosticBytes bytes with
            | Ok r -> r
            | Error err -> failwithf "Expected Ok from diagnosticBytes, got %A" err

        Assert.That(diagReport.Epochs.Length, Is.EqualTo 2)
        assertEqual liveEffects1 diagReport.Epochs[0].Effects
        assertEqual liveSnapshot1 diagReport.Epochs[0].FinalSnapshot
        assertEqual liveEffects2 diagReport.Epochs[1].Effects
        assertEqual liveSnapshot2 diagReport.Epochs[1].FinalSnapshot

        let reenactVisits = ref 0
        let mbConfig: ReenactMailboxConfig = {
            MaxConcurrency = 1
            CommandCapacity = 32
            ResolveInput = fun _ _ (InputStamp s) -> ValueToken(s * 10UL)
            ResolveResume = fun _ _ -> ValueToken 99UL
            Evaluator = fun _ -> makeEvaluator reenactVisits
        }
        use cts = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        let! reenactRes = Replay.reenactBytesWithMailbox mbConfig bytes cts.Token
        match reenactRes with
        | Ok reenactReport ->
            assertEqual diagReport reenactReport
            Assert.That(reenactVisits.Value, Is.EqualTo 6)
        | Error err -> Assert.Fail(sprintf "Expected Ok from reenactBytesWithMailbox, got %A" err)
    }

    [<Test>]
    member _.RejectsCorruptedHeadersUnsupportedVersionsAndTruncatedOrMalformedFrames() =
        // 1. Corrupted magic bytes.
        let badMagic = [| byte 'X'; byte 'F'; byte 'I'; byte 'J'; 1uy; 0uy; 0uy; 0uy |]
        match Replay.diagnosticBytes badMagic with
        | Error(JournalError.InvalidHeader _) -> ()
        | other -> Assert.Fail(sprintf "Expected InvalidHeader for bad magic, got %A" other)

        // 2. Unsupported version (V2 = 2us).
        let validJournal = InMemoryInputJournal(EpochId 1UL)
        let v2Bytes = Array.copy (validJournal.ToBytes())
        v2Bytes[4] <- 2uy
        v2Bytes[5] <- 0uy
        match Replay.diagnosticBytes v2Bytes with
        | Error(JournalError.UnsupportedVersion 2us) -> ()
        | other -> Assert.Fail(sprintf "Expected UnsupportedVersion 2us, got %A" other)

        // 3. Non-zero header flags.
        let badFlags = Array.copy (validJournal.ToBytes())
        badFlags[6] <- 1uy
        match Replay.diagnosticBytes badFlags with
        | Error(JournalError.InvalidHeader _) -> ()
        | other -> Assert.Fail(sprintf "Expected InvalidHeader for flags, got %A" other)

        // 4. Truncated frame bytes.
        (validJournal :> IInputJournal).Record(
            JournalEntry.ExternalCommand(EpochId 1UL, ExternalActionFrame.ReserveScope(ScopeId 1UL, RevisionId 1UL)))
        let fullBytes = validJournal.ToBytes()
        let truncated = fullBytes[0 .. fullBytes.Length - 4]
        match Replay.diagnosticBytes truncated with
        | Error(JournalError.TruncatedFrame 1) -> ()
        | other -> Assert.Fail(sprintf "Expected TruncatedFrame 1, got %A" other)

        // 5. Malformed entry kind tag inside frame 1.
        let malformed = Array.copy fullBytes
        // Header = 20 bytes; frame 0 (BeginEpoch) = 4 len + 9 payload = 13 bytes -> frame 1 starts at offset 33, kind byte at 37.
        malformed[37] <- 0xFEuy
        match Replay.diagnosticBytes malformed with
        | Error(JournalError.CorruptFrame(1, _)) -> ()
        | other -> Assert.Fail(sprintf "Expected CorruptFrame(1, _), got %A" other)

    [<Test>]
    member _.RejectsMismatchedAndNonIncreasingEpochSequencesWithProtocolError() : Task = task {
        // ForeignEpoch inside active epoch 1 segment.
        let foreignEntries = [
            JournalEntry.BeginEpoch(EpochId 1UL)
            JournalEntry.ExternalCommand(EpochId 1UL, ExternalActionFrame.ReserveScope(ScopeId 1UL, RevisionId 1UL))
            JournalEntry.ExternalCommand(EpochId 2UL, ExternalActionFrame.CloseScope(ScopeId 1UL))
        ]
        let foreignBytes = JournalCodec.encodeV1 foreignEntries
        assertEqual
            (Error(JournalError.Protocol ProtocolError.ForeignEpoch))
            (Replay.diagnosticBytes foreignBytes)

        let config: ReenactHostConfig = {
            MaxConcurrency = 1
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            Evaluator = fun _ _ _ -> Task.FromResult(ValueToken 0UL)
        }
        let! foreignLive = Replay.reenactBytesWithHost config foreignBytes CancellationToken.None
        assertEqual
            (Error(JournalError.Protocol ProtocolError.ForeignEpoch))
            foreignLive

        // Non-increasing BeginEpoch sequence (Epoch 2 -> Epoch 1).
        let regressiveBytes =
            JournalCodec.encodeV1 [
                JournalEntry.BeginEpoch(EpochId 2UL)
                JournalEntry.ExternalCommand(EpochId 2UL, ExternalActionFrame.Retire)
                JournalEntry.BeginEpoch(EpochId 1UL)
            ]
        assertEqual
            (Error(JournalError.Protocol ProtocolError.RetiredEpoch))
            (Replay.diagnosticBytes regressiveBytes)
    }

    [<Test>]
    member _.TamperedDefinitionStampAndOmittedReserveScopeFailReplayWithExplicitProtocolError() : Task = task {
        let epoch = EpochId 1UL
        let scope = ScopeId 1UL
        let work = WorkId 1UL
        let journal = InMemoryInputJournal()
        let step = Journal.step journal

        let apply action state =
            match step { Epoch = epoch; Action = action } state with
            | Ok(next, _) -> next
            | Error err -> failwithf "Unexpected setup error: %A" err

        let mutable state = Core.init epoch
        state <- apply (Action.ReserveScope(scope, RevisionId 1UL)) state
        state <- apply (Action.ReplaceScope(scope, RevisionId 1UL, [
            ScopeEntry.Define { Work = work; Stamp = DefinitionStamp 1UL; Reads = [] }
        ])) state
        state <- apply (Action.ReserveScope(scope, RevisionId 2UL)) state
        state <- apply (Action.ReplaceScope(scope, RevisionId 2UL, [
            ScopeEntry.Define { Work = work; Stamp = DefinitionStamp 2UL; Reads = [] }
        ])) state

        let cleanEntries = journal.Entries
        Assert.That(Replay.diagnosticBytes (JournalCodec.encodeV1 cleanEntries) |> Result.isOk, Is.True)

        // Control A: Tamper second ReplaceScope DefinitionStamp from 2UL to 1UL (reused stamp).
        let tamperedStampEntries =
            cleanEntries
            |> List.map (function
                | JournalEntry.ExternalCommand(ep, ExternalActionFrame.ReplaceScope(sc, RevisionId 2UL, _)) ->
                    JournalEntry.ExternalCommand(
                        ep,
                        ExternalActionFrame.ReplaceScope(
                            sc,
                            RevisionId 2UL,
                            [ ScopeEntry.Define { Work = work; Stamp = DefinitionStamp 1UL; Reads = [] } ]))
                | other -> other)
        let tamperedStampBytes = JournalCodec.encodeV1 tamperedStampEntries

        assertEqual
            (Error(JournalError.Protocol(ProtocolError.DefinitionStampReused work)))
            (Replay.diagnosticBytes tamperedStampBytes)

        let config: ReenactHostConfig = {
            MaxConcurrency = 1
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            Evaluator = fun _ _ _ -> Task.FromResult(ValueToken 0UL)
        }
        let! liveTamperedStamp = Replay.reenactBytesWithHost config tamperedStampBytes CancellationToken.None
        assertEqual
            (Error(JournalError.Protocol(ProtocolError.DefinitionStampReused work)))
            liveTamperedStamp

        // Control B1: Omit initial ReserveScope(scope, RevisionId 1UL) -> UnknownScope scope.
        let omittedFirstReserveBytes =
            cleanEntries
            |> List.filter (function
                | JournalEntry.ExternalCommand(_, ExternalActionFrame.ReserveScope(_, RevisionId 1UL)) -> false
                | _ -> true)
            |> JournalCodec.encodeV1

        assertEqual
            (Error(JournalError.Protocol(ProtocolError.UnknownScope scope)))
            (Replay.diagnosticBytes omittedFirstReserveBytes)

        let! liveOmittedFirst = Replay.reenactBytesWithHost config omittedFirstReserveBytes CancellationToken.None
        assertEqual
            (Error(JournalError.Protocol(ProtocolError.UnknownScope scope)))
            liveOmittedFirst

        // Control B2: Omit second ReserveScope(scope, RevisionId 2UL) -> ReservationMismatch scope.
        let omittedSecondReserveBytes =
            cleanEntries
            |> List.filter (function
                | JournalEntry.ExternalCommand(_, ExternalActionFrame.ReserveScope(_, RevisionId 2UL)) -> false
                | _ -> true)
            |> JournalCodec.encodeV1

        assertEqual
            (Error(JournalError.Protocol(ProtocolError.ReservationMismatch scope)))
            (Replay.diagnosticBytes omittedSecondReserveBytes)

        let! liveOmittedSecond = Replay.reenactBytesWithHost config omittedSecondReserveBytes CancellationToken.None
        assertEqual
            (Error(JournalError.Protocol(ProtocolError.ReservationMismatch scope)))
            liveOmittedSecond
    }

    [<Test>]
    member _.MailboxHostJournalOverloadRecordsAndReplaysCleanly() : Task = task {
        let epoch = EpochId 9UL
        let scope = ScopeId 1UL
        let work = WorkId 1UL
        let journal = InMemoryInputJournal()

        let stepEvaluator : StepEvaluator =
            fun invocation _token ->
                match invocation with
                | StepInvocation.Start _ ->
                    Task.FromResult(StepOutcome.Complete(Completion.Succeeded(ValueToken 500UL)))
                | StepInvocation.Resume _ ->
                    Task.FromResult(StepOutcome.Complete Completion.Cancelled)

        let mbHost = new MailboxHost(epoch, 1, 16, stepEvaluator, journal)
        let! r1 = mbHost.PostAsync(Action.ReserveScope(scope, RevisionId 1UL))
        Assert.That(Result.isOk r1, Is.True)
        let! r2 = mbHost.PostAsync(Action.ReplaceScope(scope, RevisionId 1UL, [
            ScopeEntry.Define { Work = work; Stamp = DefinitionStamp 1UL; Reads = [] }
        ]))
        Assert.That(Result.isOk r2, Is.True)
        let! demanded = mbHost.DemandAsync(DemandId 1UL, work)
        match demanded with
        | Ok handle -> Assert.That(handle.Value, Is.EqualTo(ValueToken 500UL))
        | Error err -> Assert.Fail(sprintf "Expected Ok DemandAsync, got %A" err)
        do! mbHost.CloseAsync()

        let liveEffects = mbHost.DrainEvents() |> List.map Journal.canonicalizeEffect
        let bytes = journal.ToBytes()
        match Replay.diagnosticBytes bytes with
        | Error err -> Assert.Fail(sprintf "Expected Ok diagnosticBytes, got %A" err)
        | Ok report ->
            Assert.That(report.Epochs.Length, Is.EqualTo 1)
            assertEqual liveEffects report.Epochs[0].Effects
    }

    [<Test>]
    member _.JournalCodecV1RoundTripsAllEntryActionObservationAndCausalVariants() =
        let e1 = EpochId 1UL
        let e2 = EpochId 2UL
        let entries = [
            JournalEntry.BeginEpoch e1
            JournalEntry.LabelRegistered(e1, { Work = WorkId 1UL; Slot = ReadSlotId 0UL; Label = "slot:zero" })
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.ReserveScope(ScopeId 1UL, RevisionId 1UL))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [
                ScopeEntry.Define {
                    Work = WorkId 1UL
                    Stamp = DefinitionStamp 1UL
                    Reads = [ { Slot = ReadSlotId 0UL; Source = ReadSource.Input(InputId 10UL) } ]
                }
                ScopeEntry.Define {
                    Work = WorkId 2UL
                    Stamp = DefinitionStamp 1UL
                    Reads = [ { Slot = ReadSlotId 1UL; Source = ReadSource.Work(WorkId 1UL) } ]
                }
            ]))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.ReserveScope(ScopeId 1UL, RevisionId 2UL))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.ReplaceScope(ScopeId 1UL, RevisionId 2UL, [
                ScopeEntry.Retain(WorkId 1UL)
                ScopeEntry.Retain(WorkId 2UL)
            ]))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.SetInputs [
                { Input = InputId 10UL; Stamp = InputStamp 1UL }
            ])
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.RemoveInputs [ InputId 10UL ])
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.Demand(DemandId 1UL, WorkId 2UL))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.Release(DemandId 1UL))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.Retry(WorkId 1UL))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.Resume {
                Epoch = e1
                Attempt = AttemptId 1UL
                Id = SuspensionId 1UL
                Step = StepId 3UL
            })
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.CloseScope(ScopeId 1UL))
            JournalEntry.ExternalCommand(e1, ExternalActionFrame.Retire)
            JournalEntry.HostObservation(e1, ObservationFrame.Suspended(AttemptId 1UL, StepId 3UL))
            JournalEntry.HostObservation(e1, ObservationFrame.Finished(AttemptId 1UL, CompletionFrame.Succeeded))
            JournalEntry.HostObservation(e1, ObservationFrame.Finished(AttemptId 2UL, CompletionFrame.Failed { Code = "E01"; Message = "boom" }))
            JournalEntry.HostObservation(e1, ObservationFrame.Finished(AttemptId 3UL, CompletionFrame.Cancelled))
            JournalEntry.HostObservation(e1, ObservationFrame.Drained(AttemptId 1UL))
            JournalEntry.Causal {
                Epoch = e1
                Trigger = CausalTrigger.InputChanged(InputId 10UL, InputStamp 2UL)
                InvalidatedSlot = Some(ReadSlotId 0UL)
                SourceWork = None
                AffectedWork = WorkId 1UL
                RestartedAttempt = Some(AttemptId 4UL)
                Label = Some "slot:zero"
            }
            JournalEntry.Causal {
                Epoch = e1
                Trigger = CausalTrigger.InputRemoved(InputId 10UL)
                InvalidatedSlot = Some(ReadSlotId 1UL)
                SourceWork = Some(WorkId 1UL)
                AffectedWork = WorkId 2UL
                RestartedAttempt = None
                Label = None
            }
            JournalEntry.Causal {
                Epoch = e1
                Trigger = CausalTrigger.ScopeReserved(ScopeId 1UL, RevisionId 2UL)
                InvalidatedSlot = None
                SourceWork = None
                AffectedWork = WorkId 1UL
                RestartedAttempt = Some(AttemptId 5UL)
                Label = None
            }
            JournalEntry.Causal {
                Epoch = e1
                Trigger = CausalTrigger.ScopeClosed(ScopeId 1UL)
                InvalidatedSlot = None
                SourceWork = None
                AffectedWork = WorkId 2UL
                RestartedAttempt = None
                Label = None
            }
            JournalEntry.BeginEpoch e2
            JournalEntry.ExternalCommand(e2, ExternalActionFrame.Retire)
        ]

        let encoded = JournalCodec.encodeV1 entries
        match JournalCodec.decodeV1 encoded with
        | Error err -> Assert.Fail(sprintf "Expected Ok decodeV1, got %A" err)
        | Ok(headerEpoch, decoded) ->
            assertEqual e1 headerEpoch
            assertEqual entries decoded

    [<Test>]
    member _.JournalRejectsInvalidUtf8NullAndMissingEpoch() =
        let epoch = EpochId 1UL
        let label = { Work = WorkId 1UL; Slot = ReadSlotId 2UL; Label = "abc" }
        let bytes = JournalCodec.encodeV1 [ JournalEntry.BeginEpoch epoch; JournalEntry.LabelRegistered(epoch, label) ]
        bytes[bytes.Length - 2] <- 0xFFuy
        match JournalCodec.decodeV1 bytes with
        | Error(JournalError.CorruptFrame(1, _)) -> ()
        | other -> Assert.Fail(sprintf "Expected invalid UTF-8 error, got %A" other)
        Assert.Throws<ArgumentNullException>(fun () ->
            JournalCodec.encodeV1 [ JournalEntry.LabelRegistered(epoch, { label with Label = null }) ] |> ignore)
        |> ignore
        assertEqual (Error JournalError.MissingBeginEpoch) (Replay.diagnostic [ JournalEntry.LabelRegistered(epoch, label) ])

    [<Test>]
    member _.ReenactRejectsIdleAndMissingSuspension() : Task = task {
        let epoch = EpochId 1UL
        let hostConfig: ReenactHostConfig = {
            MaxConcurrency = 1
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            Evaluator = fun _ -> fun _ _ -> Task.FromResult(ValueToken 0UL)
        }
        let mailboxConfig: ReenactMailboxConfig = {
            MaxConcurrency = 1
            CommandCapacity = 4
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            ResolveResume = fun _ _ -> ValueToken 0UL
            Evaluator = fun _ -> fun _ _ -> async { return StepOutcome.Complete(Completion.Succeeded(ValueToken 0UL)) }
        }
        let entries = [
            JournalEntry.BeginEpoch epoch
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.ReserveScope(ScopeId 1UL, RevisionId 1UL))
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [
                ScopeEntry.Define { Work = WorkId 1UL; Stamp = DefinitionStamp 1UL; Reads = [] }
            ]))
            JournalEntry.HostObservation(epoch, ObservationFrame.Finished(AttemptId 1UL, CompletionFrame.Succeeded))
        ]
        let! hostResult = Replay.reenactWithHost hostConfig entries CancellationToken.None
        let! mailboxResult = Replay.reenactWithMailbox mailboxConfig entries CancellationToken.None
        match hostResult, mailboxResult with
        | Error(JournalError.ReplayDivergence _), Error(JournalError.ReplayDivergence _) -> ()
        | other -> Assert.Fail(sprintf "Expected idle divergences, got %A" other)
        let resume = {
            Epoch = epoch; Attempt = AttemptId 7UL; Id = SuspensionId 8UL; Step = StepId 9UL
        }
        let resumeEntries = [ JournalEntry.BeginEpoch epoch; JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Resume resume) ]
        let! hostResume = Replay.reenactWithHost hostConfig resumeEntries CancellationToken.None
        let! mailboxResume = Replay.reenactWithMailbox mailboxConfig resumeEntries CancellationToken.None
        match hostResume, mailboxResume with
        | Error(JournalError.ReplayUnsupported _), Error(JournalError.ReplayDivergence _) -> ()
        | other -> Assert.Fail(sprintf "Expected resume errors, got %A" other)

        let wrongAttempt =
            entries
            |> List.take 3
            |> fun prefix -> prefix @ [
                JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Demand(DemandId 1UL, WorkId 1UL))
                JournalEntry.HostObservation(epoch, ObservationFrame.Finished(AttemptId 99UL, CompletionFrame.Succeeded))
            ]
        let! wrongHost = Replay.reenactWithHost hostConfig wrongAttempt CancellationToken.None
        let! wrongMailbox = Replay.reenactWithMailbox mailboxConfig wrongAttempt CancellationToken.None
        match wrongHost, wrongMailbox with
        | Error(JournalError.ReplayDivergence hostReason), Error(JournalError.ReplayDivergence mailboxReason) ->
            Assert.That(hostReason, Does.Contain "Unexpected observation")
            Assert.That(mailboxReason, Does.Contain "Unexpected observation")
        | other -> Assert.Fail(sprintf "Expected observation mismatch, got %A" other)
        let! badConfiguration = Replay.reenactWithMailbox { mailboxConfig with CommandCapacity = 0 } [ JournalEntry.BeginEpoch epoch ] CancellationToken.None
        match badConfiguration with
        | Error(JournalError.ReplayHostRejected _) -> ()
        | other -> Assert.Fail(sprintf "Expected configuration error, got %A" other)
    }



    [<TestCase(true)>]
    [<TestCase(false)>]
    member _.ReenactRejectsEqualCountMismatch(useHost: bool) : Task = task {
        let epoch = EpochId 1UL
        let prefix = [
            JournalEntry.BeginEpoch epoch
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.ReserveScope(ScopeId 1UL, RevisionId 1UL))
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [
                ScopeEntry.Define { Work = WorkId 1UL; Stamp = DefinitionStamp 1UL; Reads = [] }
            ]))
            JournalEntry.ExternalCommand(epoch, ExternalActionFrame.Demand(DemandId 1UL, WorkId 1UL))
        ]
        let hostConfig: ReenactHostConfig = {
            MaxConcurrency = 1
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            Evaluator = fun _ -> fun _ _ -> Task.FromResult(ValueToken 0UL)
        }
        let mailboxConfig: ReenactMailboxConfig = {
            MaxConcurrency = 1
            CommandCapacity = 4
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            ResolveResume = fun _ _ -> ValueToken 0UL
            Evaluator = fun _ -> fun _ _ -> async { return StepOutcome.Complete(Completion.Succeeded(ValueToken 0UL)) }
        }
        let mismatched = prefix @ [
            JournalEntry.HostObservation(epoch, ObservationFrame.Finished(AttemptId 1UL, CompletionFrame.Failed { Code = "test"; Message = "different" }))
            JournalEntry.HostObservation(epoch, ObservationFrame.Drained(AttemptId 1UL))
        ]
        let bytes = JournalCodec.encodeV1 mismatched
        let! result =
            if useHost then Replay.reenactBytesWithHost hostConfig bytes CancellationToken.None
            else Replay.reenactBytesWithMailbox mailboxConfig bytes CancellationToken.None
        match result with
        | Error(JournalError.ReplayDivergence reason) ->
            Assert.That(reason, Does.Contain "Unexpected observation at 0")
        | other -> Assert.Fail(sprintf "Expected unexpected observation at 0, got %A" other)
    }

    [<TestCase(true)>]
    [<TestCase(false)>]
    member _.ReenactRejectsRecordedInFlightTail(useHost: bool) : Task = task {
        let epoch = EpochId 1UL
        let journal = InMemoryInputJournal(epoch)
        let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let work = WorkId 1UL
        let actions = [
            Action.ReserveScope(ScopeId 1UL, RevisionId 1UL)
            Action.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [
                ScopeEntry.Define { Work = work; Stamp = DefinitionStamp 1UL; Reads = [] }
            ])
            Action.Demand(DemandId 1UL, work)
        ]
        let mutable recorded = [||]
        if useHost then
            let evaluator: Evaluator = fun _ _ -> task {
                started.TrySetResult(()) |> ignore
                do! release.Task
                return ValueToken 0UL
            }
            let host = new Host(epoch, 1, evaluator, journal)
            for action in actions do sendOk host action |> ignore
            do! started.Task
            recorded <- journal.ToBytes()
            release.TrySetResult(()) |> ignore
            do! host.CloseAsync()
        else
            let evaluator: StepEvaluator = fun _ _ -> task {
                started.TrySetResult(()) |> ignore
                do! release.Task
                return StepOutcome.Complete(Completion.Succeeded(ValueToken 0UL))
            }
            let host = new MailboxHost(epoch, 1, 4, evaluator, journal)
            for action in actions do
                let! result = host.PostAsync action
                Assert.That(Result.isOk result, Is.True)
            do! started.Task
            recorded <- journal.ToBytes()
            release.TrySetResult(()) |> ignore
            do! host.CloseAsync()
        let hostConfig: ReenactHostConfig = {
            MaxConcurrency = 1; ResolveInput = fun _ _ _ -> ValueToken 0UL
            Evaluator = fun _ -> fun _ _ -> Task.FromResult(ValueToken 0UL)
        }
        let mailboxConfig: ReenactMailboxConfig = {
            MaxConcurrency = 1; CommandCapacity = 4
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            ResolveResume = fun _ _ -> ValueToken 0UL
            Evaluator = fun _ -> fun _ _ -> async { return StepOutcome.Complete(Completion.Succeeded(ValueToken 0UL)) }
        }
        let! result =
            if useHost then Replay.reenactBytesWithHost hostConfig recorded CancellationToken.None
            else Replay.reenactBytesWithMailbox mailboxConfig recorded CancellationToken.None
        match result with
        | Error(JournalError.ReplayDivergence reason) -> Assert.That(reason, Does.Contain "Unexpected observation at 0")
        | other -> Assert.Fail(sprintf "Expected tail divergence, got %A" other)
    }

    [<Test>]
    member _.ReenactWaitsForSuspendedCancellationCallbacks() : Task = task {
        let epoch = EpochId 1UL
        let journal = InMemoryInputJournal(epoch)
        let evaluator: StepEvaluator = fun _ token ->
            token.Register(System.Action(fun () -> Thread.Sleep 200)) |> ignore
            Task.FromResult(StepOutcome.Suspend(StepId 1UL, ValueToken 0UL))
        let workflow invocation cancellation = ClrInterop.fromTask (evaluator invocation) cancellation
        let settings: AsyncMailbox.Settings = { Epoch = epoch; MaxConcurrency = 1; CommandCapacity = 4 }
        let handle = AsyncMailbox.createWithJournal settings journal workflow |> Result.defaultWith (fun error -> failwithf "%A" error)
        assertEqual (Ok()) (AsyncMailbox.start handle)
        admitAndObserveOk handle (Action.ReserveScope(ScopeId 1UL, RevisionId 1UL))
        admitAndObserveOk handle (Action.ReplaceScope(ScopeId 1UL, RevisionId 1UL, [
            ScopeEntry.Define { Work = WorkId 1UL; Stamp = DefinitionStamp 1UL; Reads = [] }
        ]))
        admitAndObserveOk handle (Action.Demand(DemandId 1UL, WorkId 1UL))
        awaitSingleSuspension handle |> ignore
        admitAndObserveOk handle (Action.Release(DemandId 1UL))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.waitForIdle handle))
        assertEqual (Ok()) (Async.RunSynchronously(AsyncMailbox.close handle))
        let bytes = journal.ToBytes()
        let expected = Replay.diagnosticBytes bytes
        let config: ReenactMailboxConfig = {
            MaxConcurrency = 1; CommandCapacity = 4
            ResolveInput = fun _ _ _ -> ValueToken 0UL
            ResolveResume = fun _ _ -> ValueToken 0UL
            Evaluator = fun _ -> workflow
        }
        let! actual = Replay.reenactBytesWithMailbox config bytes CancellationToken.None
        assertEqual expected actual
        Assert.That(Result.isOk actual, Is.True)
    }
