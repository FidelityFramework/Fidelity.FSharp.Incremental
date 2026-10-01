namespace Fidelity.FSharp.Incremental.Tests

open NUnit.Framework
open Fidelity.FSharp.Incremental

module private CoreChecks =
    let epoch = EpochId 1UL
    let scope = ScopeId 1UL
    let otherScope = ScopeId 2UL
    let work number = WorkId(uint64 number)
    let demand number = DemandId(uint64 number)
    let input number = InputId(uint64 number)
    let token number = ValueToken(uint64 number)
    let revision number = RevisionId(uint64 number)
    let check (condition: bool) (message: string) = Assert.That(condition, Is.True, message)
    let same expected actual = check (expected = actual) (sprintf "Expected %A, got %A" expected actual)
    let step action state =
        match Core.step { Epoch = epoch; Action = action } state with
        | Ok result -> result
        | Error error -> failwithf "Unexpected protocol rejection for %A: %A" action error
    let advance action state = step action state |> fst
    let rejects expected action state =
        let before = Core.snapshot state
        match Core.step { Epoch = epoch; Action = action } state with
        | Error error -> same expected error
        | Ok(_, effects) -> failwithf "Expected %A, got effects %A" expected effects
        same before (Core.snapshot state)
    let source slot source : Read = { Slot = ReadSlotId(uint64 slot); Source = source }
    let fromInput slot number = source slot (ReadSource.Input(input number))
    let fromWork slot number = source slot (ReadSource.Work(work number))
    let definition number stamp reads : Definition =
        { Work = work number; Stamp = DefinitionStamp(uint64 stamp); Reads = reads }
    let define number stamp reads = ScopeEntry.Define(definition number stamp reads)
    let inputValue number stamp value : InputValue =
        { Input = input number; Stamp = InputStamp(uint64 stamp); Value = token value }
    let starts effects =
        effects |> List.choose (fun effect ->
            match effect.Action with EffectAction.Start request -> Some request | _ -> None)
    let offers effects =
        effects |> List.choose (fun effect ->
            match effect.Action with EffectAction.Offer result -> Some result | _ -> None)
    let withdrawals effects =
        effects |> List.choose (fun effect ->
            match effect.Action with EffectAction.Withdraw result -> Some result | _ -> None)
    let started number effects =
        match starts effects |> List.filter (fun request -> request.Definition.Work = work number) with
        | [request] -> request
        | found -> failwithf "Expected one Start for work %d, got %A" number found
    let countEffect action effects = effects |> List.filter (fun effect -> effect.Action = action) |> List.length
    let view number state = (Core.snapshot state).Works |> List.find (fun row -> row.Work = work number)
    let result number state =
        Core.tryResult (work number) state |> Option.defaultWith (fun () -> failwithf "No eligible result for work %d" number)
    let configure owner rev entries state =
        state
        |> advance (Action.ReserveScope(owner, revision rev))
        |> advance (Action.ReplaceScope(owner, revision rev, entries))
    let initial entries =
        Core.init epoch
        |> advance (Action.SetInputs [inputValue 1 1 10; inputValue 2 1 20; inputValue 3 1 30])
        |> configure scope 1 entries
    let start number state =
        let next, effects = step (Action.Demand(demand number, work number)) state
        next, started number effects
    let succeed (request: StartRequest) value state =
        let finished, finishEffects = step (Action.Finished(request.Attempt, Completion.Succeeded(token value))) state
        same [] (offers finishEffects)
        step (Action.Drained request.Attempt) finished
    let successful number value state =
        let running, request = start number state
        let drained, _ = succeed request value running
        drained, result number drained

[<TestFixture>]
type CoreTests() =
    [<Test>]
    member _.``Definitions do not execute without demand``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []]
        CoreChecks.same 0 (Core.pendingAttempts state)
        CoreChecks.same WorkStatus.Idle (CoreChecks.view 1 state).Status
        let running, effects = CoreChecks.step (Action.Demand(CoreChecks.demand 1, CoreChecks.work 1)) state
        CoreChecks.same 1 (CoreChecks.starts effects).Length
        CoreChecks.same 1 (Core.pendingAttempts running)
        let unchanged, repeated = CoreChecks.step (Action.Demand(CoreChecks.demand 1, CoreChecks.work 1)) running
        CoreChecks.same [] repeated
        CoreChecks.same (Core.snapshot running) (Core.snapshot unchanged)

    [<Test>]
    member _.``Success is ineligible until exact drain acknowledgement``() =
        let running, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let finished, effects = CoreChecks.step (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 41))) running
        CoreChecks.same [] (CoreChecks.offers effects)
        CoreChecks.same 1 (CoreChecks.countEffect (EffectAction.Drain request.Attempt) effects)
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) finished)
        CoreChecks.same 1 (Core.pendingAttempts finished)
        let ready, offered = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.same [CoreChecks.result 1 ready] (CoreChecks.offers offered)
        CoreChecks.check (Core.isEligible (CoreChecks.result 1 ready) ready) "Current handle must be eligible"
        CoreChecks.same 0 (Core.pendingAttempts ready)

    [<Test>]
    member _.``Resolved reads retain ordered duplicate source occurrences``() =
        let reads = [CoreChecks.fromInput 9 2; CoreChecks.fromInput 3 1; CoreChecks.fromInput 4 1]
        let _, request = CoreChecks.initial [CoreChecks.define 1 1 reads] |> CoreChecks.start 1
        CoreChecks.same reads request.Definition.Reads
        CoreChecks.same
            [ { Slot = ReadSlotId 9UL; Value = ReadValue.Input(InputId 2UL, InputStamp 1UL, ValueToken 20UL) }
              { Slot = ReadSlotId 3UL; Value = ReadValue.Input(InputId 1UL, InputStamp 1UL, ValueToken 10UL) }
              { Slot = ReadSlotId 4UL; Value = ReadValue.Input(InputId 1UL, InputStamp 1UL, ValueToken 10UL) } ]
            request.Reads

    [<Test>]
    member _.``Failure requires explicit retry with a fresh attempt``() =
        let running, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let failure = { Code = "host-fault"; Message = "controlled failure" }
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Failed failure)) running
        let failed, effects = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.same [] (CoreChecks.offers effects)
        CoreChecks.same [] (CoreChecks.starts effects)
        CoreChecks.same (WorkStatus.Failed failure) (CoreChecks.view 1 failed).Status
        let _, retryEffects = CoreChecks.step (Action.Retry(CoreChecks.work 1)) failed
        CoreChecks.check ((CoreChecks.started 1 retryEffects).Attempt <> request.Attempt) "Retry must allocate a fresh attempt"

    [<Test>]
    member _.``Cancelled before callback entry remains owned until drained``() =
        let running, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let cancelled, effects = CoreChecks.step (Action.Release(CoreChecks.demand 1)) running
        CoreChecks.same 1 (CoreChecks.countEffect (EffectAction.Cancel request.Attempt) effects)
        CoreChecks.same 1 (Core.pendingAttempts cancelled)
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Cancelled)) cancelled
        let drained, after = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.same [] (CoreChecks.offers after)
        CoreChecks.same 0 (Core.pendingAttempts drained)

    [<Test>]
    member _.``Releasing one of two demands does not cancel their shared attempt``() =
        let running, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let shared, added = CoreChecks.step (Action.Demand(CoreChecks.demand 2, CoreChecks.work 1)) running
        CoreChecks.same [] (CoreChecks.starts added)
        let remaining, detached = CoreChecks.step (Action.Release(CoreChecks.demand 1)) shared
        CoreChecks.same 0 (CoreChecks.countEffect (EffectAction.Cancel request.Attempt) detached)
        let ready, _ = CoreChecks.succeed request 7 remaining
        CoreChecks.same (CoreChecks.token 7) (CoreChecks.result 1 ready).Value

    [<Test>]
    member _.``Downstream demand protects producer after direct subscriber releases``() =
        let initial = CoreChecks.initial [CoreChecks.define 1 1 []; CoreChecks.define 2 1 [CoreChecks.fromWork 1 1]]
        let running, producer = CoreChecks.start 1 initial
        let shared = CoreChecks.advance (Action.Demand(CoreChecks.demand 2, CoreChecks.work 2)) running
        let remaining, effects = CoreChecks.step (Action.Release(CoreChecks.demand 1)) shared
        CoreChecks.same 0 (CoreChecks.countEffect (EffectAction.Cancel producer.Attempt) effects)
        let next, after = CoreChecks.succeed producer 7 remaining
        CoreChecks.started 2 after |> ignore
        CoreChecks.check (CoreChecks.view 1 next).Demanded "Producer must remain transitively demanded"

    [<Test>]
    member _.``Diamond starts one producer and waits for both drained branches``() =
        let initial = CoreChecks.initial [CoreChecks.define 1 1 []; CoreChecks.define 2 1 [CoreChecks.fromWork 1 1]; CoreChecks.define 3 1 [CoreChecks.fromWork 1 1]; CoreChecks.define 4 1 [CoreChecks.fromWork 1 2; CoreChecks.fromWork 2 3]]
        let state, first = CoreChecks.step (Action.Demand(CoreChecks.demand 4, CoreChecks.work 4)) initial
        CoreChecks.same [CoreChecks.work 1] (CoreChecks.starts first |> List.map (fun row -> row.Definition.Work))
        let producer = CoreChecks.started 1 first
        let state, branches = CoreChecks.succeed producer 10 state
        CoreChecks.same [CoreChecks.work 2; CoreChecks.work 3] (CoreChecks.starts branches |> List.map (fun row -> row.Definition.Work))
        let state, left = CoreChecks.succeed (CoreChecks.started 2 branches) 20 state
        CoreChecks.same [] (CoreChecks.starts left)
        let _, right = CoreChecks.succeed (CoreChecks.started 3 branches) 30 state
        let final = CoreChecks.started 4 right
        CoreChecks.same 2 final.Reads.Length

    [<Test>]
    member _.``Retain revalidates unchanged cached work with a fresh eligibility``() =
        let accepted, old = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromInput 1 1]] |> CoreChecks.successful 1 42
        let reserved, withdrawals = CoreChecks.step (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2)) accepted
        CoreChecks.same [old] (CoreChecks.withdrawals withdrawals)
        CoreChecks.check (not (Core.isEligible old reserved)) "Reservation must revoke the old handle before writes"
        let current, effects = CoreChecks.step (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [ScopeEntry.Retain(CoreChecks.work 1)])) reserved
        let retained = CoreChecks.result 1 current
        CoreChecks.same old.Attempt retained.Attempt
        CoreChecks.same old.Value retained.Value
        CoreChecks.same (CoreChecks.revision 2) retained.Revision
        CoreChecks.check (old.Eligibility <> retained.Eligibility) "Revalidation must issue a new eligibility"
        CoreChecks.same [] (CoreChecks.starts effects)
        CoreChecks.same [retained] (CoreChecks.offers effects)
        CoreChecks.check (not (Core.isEligible old current)) "Old handle remains invalid after cache reoffer"

    [<Test>]
    member _.``Reservation withdraws an entire offered reverse closure``() =
        let initial = CoreChecks.initial [CoreChecks.define 1 1 []; CoreChecks.define 2 1 [CoreChecks.fromWork 1 1]]
        let state, effects = CoreChecks.step (Action.Demand(CoreChecks.demand 2, CoreChecks.work 2)) initial
        let state, effects = CoreChecks.succeed (CoreChecks.started 1 effects) 1 state
        let ready, _ = CoreChecks.succeed (CoreChecks.started 2 effects) 2 state
        let old = [CoreChecks.result 1 ready; CoreChecks.result 2 ready]
        let reserved, effects = CoreChecks.step (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2)) ready
        CoreChecks.same (Set.ofList old) (CoreChecks.withdrawals effects |> Set.ofList)
        CoreChecks.same [] (CoreChecks.starts effects)
        for handle in old do CoreChecks.check (not (Core.isEligible handle reserved)) "Dependent handle survived reservation"

    [<Test>]
    member _.``Input change invalidates only affected closure even with equal value tokens``() =
        let initial = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromInput 1 1]; CoreChecks.define 2 1 [CoreChecks.fromInput 1 2]; CoreChecks.define 3 1 [CoreChecks.fromWork 1 1]]
        let state, independent = CoreChecks.successful 2 22 initial
        let state, effects = CoreChecks.step (Action.Demand(CoreChecks.demand 3, CoreChecks.work 3)) state
        let state, effects = CoreChecks.succeed (CoreChecks.started 1 effects) 11 state
        let state, _ = CoreChecks.succeed (CoreChecks.started 3 effects) 33 state
        let changed, effects = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue 1 2 10]) state
        CoreChecks.same (Set.ofList [CoreChecks.work 1; CoreChecks.work 3]) (CoreChecks.withdrawals effects |> List.map _.Work |> Set.ofList)
        CoreChecks.check (Core.isEligible independent changed) "Independent branch was invalidated"
        CoreChecks.same [CoreChecks.work 1] (CoreChecks.starts effects |> List.map (fun row -> row.Definition.Work))
        let firstStart = effects |> List.findIndex (fun effect -> match effect.Action with EffectAction.Start _ -> true | _ -> false)
        let lastWithdraw = effects |> List.findIndexBack (fun effect -> match effect.Action with EffectAction.Withdraw _ -> true | _ -> false)
        CoreChecks.check (lastWithdraw < firstStart) "Replacement work started before complete withdrawal"

    [<Test>]
    member _.``Census changes invalidate proofs without changing positive input identities``() =
        let state, old = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromInput 1 1; CoreChecks.fromInput 2 3]] |> CoreChecks.successful 1 77
        let next, effects = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue 3 2 30]) state
        CoreChecks.check (not (Core.isEligible old next)) "Unchanged positive input hid a changed census"
        let request = CoreChecks.started 1 effects
        CoreChecks.same (ReadValue.Input(InputId 1UL, InputStamp 1UL, ValueToken 10UL)) request.Reads[0].Value
        CoreChecks.same (ReadValue.Input(InputId 3UL, InputStamp 2UL, ValueToken 30UL)) request.Reads[1].Value

    [<Test>]
    member _.``Removing one duplicate read slot preserves the surviving dependency``() =
        let state, _ = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromInput 1 1; CoreChecks.fromInput 2 1]] |> CoreChecks.successful 1 1
        let state = CoreChecks.advance (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2)) state
        let state, effects = CoreChecks.step (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [CoreChecks.define 1 2 [CoreChecks.fromInput 2 1]])) state
        let state, _ = CoreChecks.succeed (CoreChecks.started 1 effects) 2 state
        let old = CoreChecks.result 1 state
        let next, effects = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue 1 2 99]) state
        CoreChecks.check (not (Core.isEligible old next)) "Last read occurrence was incorrectly removed"
        CoreChecks.same [ReadSlotId 2UL] ((CoreChecks.started 1 effects).Reads |> List.map _.Slot)

    [<Test>]
    member _.``Changing a read source with the same slot never retains the old result``() =
        let state, old = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromInput 1 1]] |> CoreChecks.successful 1 7
        let state = CoreChecks.advance (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2)) state
        let next, effects = CoreChecks.step (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [CoreChecks.define 1 2 [CoreChecks.fromInput 1 2]])) state
        let request = CoreChecks.started 1 effects
        CoreChecks.check (request.Attempt <> old.Attempt) "Changed read source retained old computation"
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) next)
        CoreChecks.same [ { Slot = ReadSlotId 1UL; Value = ReadValue.Input(InputId 2UL, InputStamp 1UL, ValueToken 20UL) } ] request.Reads

    [<Test>]
    member _.``Removing and readding work cannot revive its old attempt``() =
        let state, old = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.successful 1 1
        let state = CoreChecks.configure CoreChecks.scope 2 [] state
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) state)
        let state = CoreChecks.configure CoreChecks.scope 3 [CoreChecks.define 1 2 []] state
        let next, effects = CoreChecks.step (Action.Demand(CoreChecks.demand 99, CoreChecks.work 1)) state
        // Removal may retire old demands; use a fresh demand regardless of policy.
        let active = match (CoreChecks.view 1 next).Status with WorkStatus.Running id -> id | status -> failwithf "Expected running readded work: %A" status
        CoreChecks.check (active <> old.Attempt) "Readded work revived old attempt"
        CoreChecks.check (not (Core.isEligible old next)) "Removed handle became current"
        CoreChecks.check ((CoreChecks.starts effects).Length <= 1) "Duplicate replacement attempt"

    [<Test>]
    member _.``Foreign epoch rejection leaves original state usable``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []]
        let before = Core.snapshot state
        match Core.step { Epoch = EpochId 2UL; Action = Action.Demand(CoreChecks.demand 1, CoreChecks.work 1) } state with
        | Error ProtocolError.ForeignEpoch -> ()
        | other -> failwithf "Unexpected foreign epoch outcome: %A" other
        CoreChecks.same before (Core.snapshot state)
        let _, effects = CoreChecks.step (Action.Demand(CoreChecks.demand 1, CoreChecks.work 1)) state
        CoreChecks.same 1 (CoreChecks.starts effects).Length

    [<Test>]
    member _.``Cycle rejection is atomic across scope boundaries``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromWork 1 2]]
        let reserved = CoreChecks.advance (Action.ReserveScope(CoreChecks.otherScope, CoreChecks.revision 1)) state
        let before = Core.snapshot reserved
        match Core.step { Epoch = CoreChecks.epoch; Action = Action.ReplaceScope(CoreChecks.otherScope, CoreChecks.revision 1, [CoreChecks.define 2 1 [CoreChecks.fromWork 1 1]]) } reserved with
        | Error(ProtocolError.DependencyCycle _) -> ()
        | other -> failwithf "Cycle was not rejected: %A" other
        CoreChecks.same before (Core.snapshot reserved)
        let corrected = CoreChecks.advance (Action.ReplaceScope(CoreChecks.otherScope, CoreChecks.revision 1, [CoreChecks.define 2 1 []])) reserved
        let _, effects = CoreChecks.step (Action.Demand(CoreChecks.demand 1, CoreChecks.work 1)) corrected
        CoreChecks.same [CoreChecks.work 2] (CoreChecks.starts effects |> List.map (fun row -> row.Definition.Work))

    [<Test>]
    member _.``Duplicate work definitions are refused atomically``() =
        let state = Core.init CoreChecks.epoch |> CoreChecks.advance (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 1))
        CoreChecks.rejects (ProtocolError.DuplicateWork(CoreChecks.work 1)) (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 1, [CoreChecks.define 1 1 []; CoreChecks.define 1 2 []])) state

    [<Test>]
    member _.``Read slots must be unique even for identical source``() =
        let state = Core.init CoreChecks.epoch |> CoreChecks.advance (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 1))
        let repeated = CoreChecks.fromInput 1 1
        CoreChecks.rejects (ProtocolError.DuplicateReadSlot(CoreChecks.work 1, ReadSlotId 1UL)) (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 1, [CoreChecks.define 1 1 [repeated; repeated]])) state

    [<Test>]
    member _.``Input stamp reuse cannot hide a different value``() =
        let state = CoreChecks.initial []
        CoreChecks.rejects (ProtocolError.InputStampReused(CoreChecks.input 1)) (Action.SetInputs [CoreChecks.inputValue 1 1 999]) state
        let _, effects = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue 1 1 10]) state
        CoreChecks.same [] effects

    [<Test>]
    member _.``Invalid input batch applies none of its otherwise valid updates``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromInput 1 2]]
        CoreChecks.rejects (ProtocolError.InputStampReused(CoreChecks.input 1)) (Action.SetInputs [CoreChecks.inputValue 2 2 200; CoreChecks.inputValue 1 1 999]) state
        let _, request = CoreChecks.start 1 state
        CoreChecks.same (ReadValue.Input(InputId 2UL, InputStamp 1UL, ValueToken 20UL)) request.Reads[0].Value

    [<Test>]
    member _.``Removed inputs retain stamp tombstones``() =
        let state = CoreChecks.initial [] |> CoreChecks.advance (Action.RemoveInputs [CoreChecks.input 1])
        CoreChecks.rejects (ProtocolError.InputStampReused(CoreChecks.input 1)) (Action.SetInputs [CoreChecks.inputValue 1 1 10]) state
        CoreChecks.advance (Action.SetInputs [CoreChecks.inputValue 1 2 10]) state |> ignore

    [<Test>]
    member _.``Definition stamps cannot be reused instead of explicit retain``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.advance (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2))
        CoreChecks.rejects (ProtocolError.DefinitionStampReused(CoreChecks.work 1)) (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [CoreChecks.define 1 1 []])) state
        CoreChecks.advance (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [ScopeEntry.Retain(CoreChecks.work 1)])) state |> ignore

    [<Test>]
    member _.``Released demand identity cannot be reassigned``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.advance (Action.Release(CoreChecks.demand 9))
        CoreChecks.rejects (ProtocolError.DemandIdReused(CoreChecks.demand 9)) (Action.Demand(CoreChecks.demand 9, CoreChecks.work 1)) state
        let _, effects = CoreChecks.step (Action.Release(CoreChecks.demand 9)) state
        CoreChecks.same [] effects

    [<Test>]
    member _.``Drain cannot precede completion``() =
        let state, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        CoreChecks.rejects (ProtocolError.DrainBeforeCompletion request.Attempt) (Action.Drained request.Attempt) state
        CoreChecks.same 1 (Core.pendingAttempts state)
        let ready, _ = CoreChecks.succeed request 1 state
        CoreChecks.result 1 ready |> ignore

    [<Test>]
    member _.``Conflicting completion cannot replace the recorded outcome``() =
        let state, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let state = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 1))) state
        CoreChecks.rejects (ProtocolError.ConflictingCompletion request.Attempt) (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 2))) state
        let state = CoreChecks.advance (Action.Drained request.Attempt) state
        CoreChecks.rejects (ProtocolError.ConflictingCompletion request.Attempt) (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 2))) state
        CoreChecks.rejects (ProtocolError.ConflictingCompletion request.Attempt) (Action.Finished(request.Attempt, Completion.Failed { Code = "late-fault"; Message = "Conflicting outcome after drain" })) state
        CoreChecks.same (CoreChecks.token 1) (CoreChecks.result 1 state).Value

    [<Test>]
    member _.``Unknown attempt cannot inject a successful value``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []]
        CoreChecks.rejects (ProtocolError.UnknownAttempt(AttemptId 999UL)) (Action.Finished(AttemptId 999UL, Completion.Succeeded(CoreChecks.token 1))) state
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) state)

    [<Test>]
    member _.``Closing waits for owned cleanup and preserves unrelated scope``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.configure CoreChecks.otherScope 1 [CoreChecks.define 2 1 []]
        let state, other = CoreChecks.successful 2 2 state
        let state, request = CoreChecks.start 1 state
        let closing, effects = CoreChecks.step (Action.CloseScope CoreChecks.scope) state
        CoreChecks.check (not (Core.scopeClosed CoreChecks.scope closing)) "Scope closed before drain"
        CoreChecks.same 0 (CoreChecks.countEffect (EffectAction.ScopeClosed CoreChecks.scope) effects)
        CoreChecks.check (Core.isEligible other closing) "Closing one scope invalidated another"
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 1))) closing
        let closed, effects = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.same 1 (CoreChecks.countEffect (EffectAction.ScopeClosed CoreChecks.scope) effects)
        CoreChecks.check (Core.scopeClosed CoreChecks.scope closed) "Drained scope remained open"
        CoreChecks.same [] (CoreChecks.offers effects)

    [<Test>]
    member _.``Retirement accepts cleanup but never late success``() =
        let state, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let retiring, effects = CoreChecks.step Action.Retire state
        CoreChecks.check (not (Core.isDrained retiring)) "Retired epoch reported drained while work was owned"
        CoreChecks.same 0 (CoreChecks.countEffect EffectAction.EpochDrained effects)
        CoreChecks.rejects ProtocolError.RetiredEpoch (Action.SetInputs [CoreChecks.inputValue 1 2 2]) retiring
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 1))) retiring
        let drained, effects = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.check (Core.isDrained drained) "Retired epoch did not acknowledge drain"
        CoreChecks.same 1 (CoreChecks.countEffect EffectAction.EpochDrained effects)
        CoreChecks.same [] (CoreChecks.offers effects)
        let _, again = CoreChecks.step Action.Retire drained
        CoreChecks.same [] again

    [<Test>]
    member _.``Identical traces produce identical effects and observations``() =
        let trace () =
            let state = CoreChecks.initial [CoreChecks.define 2 1 []; CoreChecks.define 1 1 []]
            let state, a = CoreChecks.step (Action.Demand(CoreChecks.demand 2, CoreChecks.work 2)) state
            let state, b = CoreChecks.step (Action.Demand(CoreChecks.demand 1, CoreChecks.work 1)) state
            let state, c = CoreChecks.succeed (CoreChecks.started 1 b) 10 state
            let state, d = CoreChecks.succeed (CoreChecks.started 2 a) 20 state
            Core.snapshot state, a @ b @ c @ d
        CoreChecks.same (trace()) (trace())

    [<Test>]
    member _.``Superseded work cannot overlap replacement or seed retained cache``() =
        let running, old = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let reserved = CoreChecks.advance (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2)) running
        let replaced, effects = CoreChecks.step (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [ScopeEntry.Retain(CoreChecks.work 1)])) reserved
        CoreChecks.same [] (CoreChecks.starts effects)
        CoreChecks.same 1 (Core.pendingAttempts replaced)
        let finished = CoreChecks.advance (Action.Finished(old.Attempt, Completion.Succeeded(CoreChecks.token 7))) replaced
        let next, effects = CoreChecks.step (Action.Drained old.Attempt) finished
        CoreChecks.same [] (CoreChecks.offers effects)
        let replacement = CoreChecks.started 1 effects
        CoreChecks.check (replacement.Attempt <> old.Attempt) "Late result was promoted as retained cache"
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) next)

    [<Test>]
    member _.``Failure does not prevent an independent branch from completing``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []; CoreChecks.define 2 1 []]
        let state, first = CoreChecks.start 1 state
        let state, second = CoreChecks.start 2 state
        let state = CoreChecks.advance (Action.Finished(first.Attempt, Completion.Failed { Code = "fault"; Message = "one branch" })) state
        let state = CoreChecks.advance (Action.Drained first.Attempt) state
        let state, _ = CoreChecks.succeed second 2 state
        CoreChecks.same (CoreChecks.token 2) (CoreChecks.result 2 state).Value
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) state)

    [<Test>]
    member _.``Duplicate lifecycle acknowledgements emit no duplicate effects``() =
        let state, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        let completion = Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 1))
        let finished = CoreChecks.advance completion state
        let sameFinished, repeated = CoreChecks.step completion finished
        CoreChecks.same [] repeated
        CoreChecks.same (Core.snapshot finished) (Core.snapshot sameFinished)
        let ready = CoreChecks.advance (Action.Drained request.Attempt) sameFinished
        let sameReady, repeated = CoreChecks.step (Action.Drained request.Attempt) ready
        CoreChecks.same [] repeated
        CoreChecks.same (Core.snapshot ready) (Core.snapshot sameReady)

    [<Test>]
    member _.``Missing input waits and its arrival starts the demanded work``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 [CoreChecks.fromInput 1 99]]
        let waiting, effects = CoreChecks.step (Action.Demand(CoreChecks.demand 1, CoreChecks.work 1)) state
        CoreChecks.same [] (CoreChecks.starts effects)
        CoreChecks.same WorkStatus.Waiting (CoreChecks.view 1 waiting).Status
        let _, effects = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue 99 1 99]) waiting
        CoreChecks.started 1 effects |> ignore

    [<Test>]
    member _.``A work identity cannot acquire a second scope owner``() =
        let state = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.advance (Action.ReserveScope(CoreChecks.otherScope, CoreChecks.revision 1))
        CoreChecks.rejects (ProtocolError.ForeignWorkOwner(CoreChecks.work 1)) (Action.ReplaceScope(CoreChecks.otherScope, CoreChecks.revision 1, [CoreChecks.define 1 2 []])) state

    [<Test>]
    member _.``Two hundred edits agree with independent whole DAG evaluation and selective visits``() =
        // Reference semantics: each node adds its own constant to its ordered inputs.
        // This evaluator has no attempts, caches, revisions or calls into Core.
        let model =
            [ 1, [Choice1Of2 1]
              2, [Choice1Of2 2]
              3, [Choice2Of2 1; Choice2Of2 1]
              4, [Choice2Of2 1; Choice2Of2 2]
              5, [Choice2Of2 2; Choice1Of2 3]
              6, [Choice2Of2 3; Choice2Of2 4]
              7, [Choice2Of2 5]
              8, [Choice2Of2 6; Choice2Of2 7] ] |> Map.ofList
        let rec fresh inputs number =
            number * 10 + (model[number] |> List.sumBy (function Choice1Of2 id -> Map.find id inputs | Choice2Of2 id -> fresh inputs id))
        let rec dependsOn inputId number =
            model[number] |> List.exists (function Choice1Of2 id -> id = inputId | Choice2Of2 id -> dependsOn inputId id)
        let entries numbers =
            numbers |> List.map (fun number ->
                let reads =
                    model[number] |> List.mapi (fun index -> function
                        | Choice1Of2 id -> CoreChecks.fromInput (index + 1) id
                        | Choice2Of2 id -> CoreChecks.fromWork (index + 1) id)
                CoreChecks.define number 1 reads)
        let pump initial effects =
            let mutable state = initial
            let mutable queue = CoreChecks.starts effects |> List.rev
            let mutable visits: Map<int, int> = Map.empty
            let mutable completed = 0
            while not queue.IsEmpty do
                completed <- completed + 1
                CoreChecks.check (completed <= 32) "Bounded DAG unexpectedly kept scheduling work"
                let request = queue.Head
                queue <- queue.Tail
                let (WorkId raw) = request.Definition.Work
                let number = int raw
                let sum =
                    request.Reads |> List.sumBy (fun read ->
                        match read.Value with
                        | ReadValue.Input(_, _, ValueToken value)
                        | ReadValue.Work(_, _, _, ValueToken value) -> int value)
                visits <- Map.add number (1 + (Map.tryFind number visits |> Option.defaultValue 0)) visits
                let finished, first = CoreChecks.step (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token (number * 10 + sum)))) state
                CoreChecks.same [] (CoreChecks.offers first)
                let drained, next = CoreChecks.step (Action.Drained request.Attempt) finished
                state <- drained
                queue <- List.rev (CoreChecks.starts (first @ next)) @ queue
            state, visits
        let mutable inputs = Map.ofList [1, 10; 2, 20; 3, 30]
        let mutable stamps = Map.ofList [1, 1; 2, 1; 3, 1]
        let mutable revisions = Map.ofList [CoreChecks.scope, 1; CoreChecks.otherScope, 1]
        let configured =
            CoreChecks.initial (entries [1..4])
            |> CoreChecks.configure CoreChecks.otherScope 1 (entries [5..8])
        let demanded, initialEffects = CoreChecks.step (Action.Demand(CoreChecks.demand 8, CoreChecks.work 8)) configured
        let initialState, initialVisits = pump demanded initialEffects
        let mutable state = initialState
        CoreChecks.same (Map.ofList [for id in 1..8 -> id, 1]) initialVisits
        let assertFresh () =
            for number in 1..8 do
                CoreChecks.same (CoreChecks.token (fresh inputs number)) (CoreChecks.result number state).Value
            CoreChecks.same 0 (Core.pendingAttempts state)
        assertFresh()
        let mutable seed = 1729UL
        for edit in 1..200 do
            seed <- (seed * 1664525UL + 1013904223UL) % 4294967296UL
            let inputId = int (seed % 3UL) + 1
            let next, effects, expectedVisits =
                if edit % 17 = 0 then
                    let owner, members = if edit % 2 = 0 then CoreChecks.scope, [1..4] else CoreChecks.otherScope, [5..8]
                    let nextRevision = revisions[owner] + 1
                    revisions <- Map.add owner nextRevision revisions
                    let reserved, withdrawals = CoreChecks.step (Action.ReserveScope(owner, CoreChecks.revision nextRevision)) state
                    for handle in CoreChecks.withdrawals withdrawals do
                        CoreChecks.check (not (Core.isEligible handle reserved)) "Reservation left an old handle usable"
                    let replacement, changes = CoreChecks.step (Action.ReplaceScope(owner, CoreChecks.revision nextRevision, members |> List.map (CoreChecks.work >> ScopeEntry.Retain))) reserved
                    replacement, withdrawals @ changes, Set.empty
                elif edit % 5 = 0 then
                    let unchanged, changes = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue inputId stamps[inputId] inputs[inputId]]) state
                    unchanged, changes, Set.empty
                else
                    seed <- (seed * 1664525UL + 1013904223UL) % 4294967296UL
                    inputs <- Map.add inputId (int (seed % 90UL) + 1) inputs
                    stamps <- Map.add inputId (stamps[inputId] + 1) stamps
                    let changed, changes = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue inputId stamps[inputId] inputs[inputId]]) state
                    changed, changes, ([1..8] |> List.filter (dependsOn inputId) |> Set.ofList)
            let settled, visits = pump next effects
            state <- settled
            CoreChecks.same expectedVisits (visits |> Map.toSeq |> Seq.map fst |> Set.ofSeq)
            for KeyValue(_, count) in visits do CoreChecks.same 1 count
            assertFresh()

module private SuspensionChecks =
    let hold step environment (request: StartRequest) state =
        let next, effects = CoreChecks.step (Action.Suspend(request.Attempt, StepId step, CoreChecks.token environment)) state
        let handles =
            effects |> List.choose (fun effect ->
                match effect.Action with EffectAction.Suspended handle -> Some handle | _ -> None)
        match handles with
        | [handle] -> next, handle, effects
        | found -> failwithf "Expected one suspension, got %A" found

    let initial reads =
        let state, request = CoreChecks.initial [CoreChecks.define 1 1 reads] |> CoreChecks.start 1
        let held, handle, _ = hold 7UL 91 request state
        held, request, handle

    let continued effects =
        effects |> List.choose (fun effect ->
            match effect.Action with EffectAction.Continue request -> Some request | _ -> None)

[<TestFixture>]
type SuspensionTests() =
    [<Test>]
    member _.``Suspension holds logical ownership without offering a result``() =
        let state, request, handle = SuspensionChecks.initial [CoreChecks.fromInput 1 1]
        CoreChecks.same (CoreChecks.epoch, request.Attempt, StepId 7UL, CoreChecks.token 91)
            (handle.Epoch, handle.Attempt, handle.Step, handle.Environment)
        CoreChecks.same 1 (Core.pendingAttempts state)
        CoreChecks.same (Some handle) (Core.trySuspension request.Attempt state)
        CoreChecks.same None (Core.tryActiveRequest request.Attempt state)
        CoreChecks.same (WorkStatus.AwaitingResume handle) (CoreChecks.view 1 state).Status
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) state)

    [<Test>]
    member _.``Resume emits the exact response and original request before terminal drain``() =
        let state, request, handle = SuspensionChecks.initial [CoreChecks.fromInput 1 1]
        let resumed, effects = CoreChecks.step (Action.Resume(handle, CoreChecks.token 123)) state
        let expected: ResumeRequest = { Start = request; Suspension = handle; Response = CoreChecks.token 123 }
        CoreChecks.same [expected] (SuspensionChecks.continued effects)
        CoreChecks.same [] (CoreChecks.starts effects)
        CoreChecks.same [] (CoreChecks.offers effects)
        CoreChecks.same None (Core.trySuspension request.Attempt resumed)
        CoreChecks.same (Some request) (Core.tryActiveRequest request.Attempt resumed)
        CoreChecks.same 1 (Core.pendingAttempts resumed)
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 41))) resumed
        CoreChecks.same None (Core.tryActiveRequest request.Attempt finished)
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) finished)
        let drained, after = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.same [CoreChecks.result 1 drained] (CoreChecks.offers after)
        CoreChecks.same request.Attempt (CoreChecks.result 1 drained).Attempt

    [<Test>]
    member _.``A consumed resume handle cannot execute twice or reappear after drain``() =
        let state, request, handle = SuspensionChecks.initial []
        let resumed = CoreChecks.advance (Action.Resume(handle, CoreChecks.token 1)) state
        CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 1)) resumed
        CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 2)) resumed
        let finished, _ = CoreChecks.succeed request 3 resumed
        CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 1)) finished
        CoreChecks.same (CoreChecks.token 3) (CoreChecks.result 1 finished).Value

    [<Test>]
    member _.``Every resume handle field belongs to its exact current suspension``() =
        let state, _, handle = SuspensionChecks.initial []
        let forged = [
            { handle with Id = SuspensionId 900UL }
            { handle with Attempt = AttemptId 900UL }
            { handle with Step = StepId 900UL }
            { handle with Environment = CoreChecks.token 900 }
        ]
        for changed in forged do
            CoreChecks.rejects (ProtocolError.InvalidSuspension changed.Id) (Action.Resume(changed, CoreChecks.token 1)) state
        let foreign = { handle with Epoch = EpochId 900UL }
        CoreChecks.rejects ProtocolError.ForeignEpoch (Action.Resume(foreign, CoreChecks.token 1)) state
        let _, valid = CoreChecks.step (Action.Resume(handle, CoreChecks.token 1)) state
        CoreChecks.same 1 (SuspensionChecks.continued valid).Length

    [<Test>]
    member _.``Only one checkpoint is held and later suspensions allocate new identities``() =
        let state, request, first = SuspensionChecks.initial []
        CoreChecks.rejects (ProtocolError.AlreadySuspended request.Attempt)
            (Action.Suspend(request.Attempt, StepId 8UL, CoreChecks.token 92)) state
        let resumed = CoreChecks.advance (Action.Resume(first, CoreChecks.token 1)) state
        let held, second, _ = SuspensionChecks.hold 7UL 91 request resumed
        CoreChecks.check (second.Id > first.Id) "Later checkpoint reused an old suspension identity"
        CoreChecks.same (first.Step, first.Environment) (second.Step, second.Environment)
        CoreChecks.rejects (ProtocolError.InvalidSuspension first.Id) (Action.Resume(first, CoreChecks.token 1)) held
        let _, effects = CoreChecks.step (Action.Resume(second, CoreChecks.token 2)) held
        CoreChecks.same second (SuspensionChecks.continued effects |> List.exactlyOne).Suspension

    [<Test>]
    member _.``Held checkpoints reject success but permit failure or cancellation cleanup``() =
        for outcome in [Completion.Failed { Code = "step"; Message = "controlled failure" }; Completion.Cancelled] do
            let state, request, handle = SuspensionChecks.initial []
            CoreChecks.rejects (ProtocolError.SuccessWhileSuspended request.Attempt)
                (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 1))) state
            let finished, effects = CoreChecks.step (Action.Finished(request.Attempt, outcome)) state
            CoreChecks.same 1 (CoreChecks.countEffect (EffectAction.Drain request.Attempt) effects)
            CoreChecks.same None (Core.trySuspension request.Attempt finished)
            CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 1)) finished
            let drained, after = CoreChecks.step (Action.Drained request.Attempt) finished
            CoreChecks.same 0 (Core.pendingAttempts drained)
            CoreChecks.same [] (CoreChecks.offers after)

    [<Test>]
    member _.``Reservation revokes suspension even when the definition is retained``() =
        let state, request, handle = SuspensionChecks.initial []
        let reserved, effects = CoreChecks.step (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2)) state
        CoreChecks.same 1 (CoreChecks.countEffect (EffectAction.Cancel request.Attempt) effects)
        CoreChecks.same None (Core.trySuspension request.Attempt reserved)
        CoreChecks.same None (Core.tryActiveRequest request.Attempt reserved)
        CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 1)) reserved
        let replaced, after =
            CoreChecks.step (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [ScopeEntry.Retain(CoreChecks.work 1)])) reserved
        CoreChecks.same [] (CoreChecks.starts after)
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Cancelled)) replaced
        let _, ready = CoreChecks.step (Action.Drained request.Attempt) finished
        let next = CoreChecks.started 1 ready
        CoreChecks.check (next.Attempt <> request.Attempt) "Old checkpoint became the replacement attempt"
        CoreChecks.same (CoreChecks.revision 2) next.Revision
        CoreChecks.same [] (SuspensionChecks.continued ready)

    [<Test>]
    member _.``Changed input invalidates a checkpoint and replacement reads fresh input``() =
        let state, request, handle = SuspensionChecks.initial [CoreChecks.fromInput 1 1]
        let changed = CoreChecks.advance (Action.SetInputs [CoreChecks.inputValue 1 2 20]) state
        CoreChecks.same None (Core.trySuspension request.Attempt changed)
        CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 1)) changed
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Cancelled)) changed
        let _, effects = CoreChecks.step (Action.Drained request.Attempt) finished
        let next = CoreChecks.started 1 effects
        CoreChecks.same [{ Slot = ReadSlotId 1UL; Value = ReadValue.Input(InputId 1UL, InputStamp 2UL, CoreChecks.token 20) }] next.Reads
        CoreChecks.same [] (SuspensionChecks.continued effects)

    [<Test>]
    member _.``Declared response inputs govern resumed result retention and invalidation``() =
        // The fixture owner ties the response to input 1. Resume itself does not
        // discover this dependency or prove an opaque token's provenance.
        let held, request, handle = SuspensionChecks.initial [CoreChecks.fromInput 1 1]
        let resumed = CoreChecks.advance (Action.Resume(handle, CoreChecks.token 10)) held
        let completed, _ = CoreChecks.succeed request 11 resumed
        let original = CoreChecks.result 1 completed
        let reserved = CoreChecks.advance (Action.ReserveScope(CoreChecks.scope, CoreChecks.revision 2)) completed
        let retained, effects =
            CoreChecks.step (Action.ReplaceScope(CoreChecks.scope, CoreChecks.revision 2, [ScopeEntry.Retain(CoreChecks.work 1)])) reserved
        let current = CoreChecks.result 1 retained
        CoreChecks.same [] (CoreChecks.starts effects)
        CoreChecks.same [] (SuspensionChecks.continued effects)
        CoreChecks.same (original.Attempt, original.Value) (current.Attempt, current.Value)
        CoreChecks.check (current.Eligibility <> original.Eligibility) "Retention reused the revoked eligibility"
        CoreChecks.check (not (Core.isEligible original retained)) "Old revision handle remained eligible"
        let changed, restarted = CoreChecks.step (Action.SetInputs [CoreChecks.inputValue 1 2 20]) retained
        CoreChecks.same [current] (CoreChecks.withdrawals restarted)
        CoreChecks.check (not (Core.isEligible current changed)) "Changed declared response input retained an old result"
        CoreChecks.same None (Core.tryResult (CoreChecks.work 1) changed)
        let next = CoreChecks.started 1 restarted
        CoreChecks.check (next.Attempt <> request.Attempt) "Changed response input reused the original attempt"
        CoreChecks.same [{ Slot = ReadSlotId 1UL; Value = ReadValue.Input(InputId 1UL, InputStamp 2UL, CoreChecks.token 20) }] next.Reads
        let heldAgain, newHandle, _ = SuspensionChecks.hold 7UL 92 next changed
        let resumedAgain = CoreChecks.advance (Action.Resume(newHandle, CoreChecks.token 20)) heldAgain
        let final, _ = CoreChecks.succeed next 21 resumedAgain
        CoreChecks.same (CoreChecks.token 21) (CoreChecks.result 1 final).Value

    [<Test>]
    member _.``Last demand release revokes checkpoint and late success cannot offer``() =
        let state, request, handle = SuspensionChecks.initial []
        let released, effects = CoreChecks.step (Action.Release(CoreChecks.demand 1)) state
        CoreChecks.same 1 (CoreChecks.countEffect (EffectAction.Cancel request.Attempt) effects)
        CoreChecks.same 1 (Core.pendingAttempts released)
        CoreChecks.same None (Core.trySuspension request.Attempt released)
        CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 1)) released
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 2))) released
        let drained, after = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.same [] (CoreChecks.offers after)
        CoreChecks.same 0 (Core.pendingAttempts drained)

    [<Test>]
    member _.``One consumer release preserves a shared checkpoint for the survivor``() =
        let state, request, handle = SuspensionChecks.initial []
        let shared = CoreChecks.advance (Action.Demand(CoreChecks.demand 2, CoreChecks.work 1)) state
        let surviving, effects = CoreChecks.step (Action.Release(CoreChecks.demand 1)) shared
        CoreChecks.same 0 (CoreChecks.countEffect (EffectAction.Cancel request.Attempt) effects)
        CoreChecks.same (Some handle) (Core.trySuspension request.Attempt surviving)
        let resumed = CoreChecks.advance (Action.Resume(handle, CoreChecks.token 1)) surviving
        let completed, _ = CoreChecks.succeed request 2 resumed
        CoreChecks.same (CoreChecks.token 2) (CoreChecks.result 1 completed).Value

    [<Test>]
    member _.``Closing a checkpoint scope waits for cleanup and preserves other scope``() =
        let state, request, handle = SuspensionChecks.initial []
        let state = CoreChecks.configure CoreChecks.otherScope 1 [CoreChecks.define 2 1 []] state
        let state, other = CoreChecks.successful 2 2 state
        let closing, _ = CoreChecks.step (Action.CloseScope CoreChecks.scope) state
        CoreChecks.check (not (Core.scopeClosed CoreChecks.scope closing)) "Suspended attempt ownership vanished on close"
        CoreChecks.check (Core.isEligible other closing) "Unrelated scope was invalidated"
        CoreChecks.rejects (ProtocolError.InvalidSuspension handle.Id) (Action.Resume(handle, CoreChecks.token 1)) closing
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Cancelled)) closing
        let closed, effects = CoreChecks.step (Action.Drained request.Attempt) finished
        CoreChecks.check (Core.scopeClosed CoreChecks.scope closed) "Scope did not close after actual acknowledgement"
        CoreChecks.same [] (SuspensionChecks.continued effects)
        CoreChecks.check (Core.isEligible other closed) "Unrelated result lost eligibility during cleanup"

    [<Test>]
    member _.``Unknown terminal and retired attempts cannot acquire a checkpoint``() =
        let state, request = CoreChecks.initial [CoreChecks.define 1 1 []] |> CoreChecks.start 1
        CoreChecks.rejects (ProtocolError.UnknownAttempt(AttemptId 999UL))
            (Action.Suspend(AttemptId 999UL, StepId 1UL, CoreChecks.token 1)) state
        let finished = CoreChecks.advance (Action.Finished(request.Attempt, Completion.Succeeded(CoreChecks.token 1))) state
        CoreChecks.rejects (ProtocolError.AttemptNotCurrent request.Attempt)
            (Action.Suspend(request.Attempt, StepId 1UL, CoreChecks.token 1)) finished
        let drained = CoreChecks.advance (Action.Drained request.Attempt) finished
        CoreChecks.rejects (ProtocolError.AttemptNotCurrent request.Attempt)
            (Action.Suspend(request.Attempt, StepId 1UL, CoreChecks.token 1)) drained
        let held, waiting, handle = SuspensionChecks.initial []
        let retired = CoreChecks.advance Action.Retire held
        CoreChecks.rejects ProtocolError.RetiredEpoch (Action.Resume(handle, CoreChecks.token 1)) retired
        let ended =
            retired
            |> CoreChecks.advance (Action.Finished(waiting.Attempt, Completion.Cancelled))
            |> CoreChecks.advance (Action.Drained waiting.Attempt)
        CoreChecks.check (Core.isDrained ended) "Retired suspended epoch did not drain"
