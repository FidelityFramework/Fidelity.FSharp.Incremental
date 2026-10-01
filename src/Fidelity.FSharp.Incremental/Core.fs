namespace Fidelity.FSharp.Incremental

/// Deterministic ownership and scheduling. Payloads and execution stay with the host.
[<RequireQualifiedAccess>]
module Core =
    type private Cached = {
        Attempt: AttemptId
        Value: ValueToken
        Reads: ResolvedRead list
    }

    type private Work = {
        Scope: ScopeId
        Definition: Definition
        Offered: ResultHandle option
        Cached: Cached option
        Current: AttemptId option
        Failure: Failure option
        Cancelled: bool
    }

    type private Attempt = {
        Request: StartRequest
        Completion: Completion option
        Obsolete: bool
        CancelSent: bool
        DrainSent: bool
    }

    /// Opaque immutable state. Commands never execute host code.
    type State = private {
        Epoch: EpochId
        NextAttempt: uint64
        NextEligibility: uint64
        Scopes: Map<ScopeId, ScopePhase>
        Works: Map<WorkId, Work>
        Inputs: Map<InputId, InputValue>
        InputStamps: Set<InputId * InputStamp>
        DefinitionStamps: Set<WorkId * DefinitionStamp>
        Demands: Map<DemandId, WorkId>
        ReleasedDemands: Set<DemandId>
        Attempts: Map<AttemptId, Attempt>
        TerminalCompletions: Map<AttemptId, Completion>
        Retiring: bool
        DrainedNotified: bool
    }

    /// Begin a distinct caller-owned epoch.
    let init epoch = {
        Epoch = epoch
        NextAttempt = 1UL
        NextEligibility = 1UL
        Scopes = Map.empty
        Works = Map.empty
        Inputs = Map.empty
        InputStamps = Set.empty
        DefinitionStamps = Set.empty
        Demands = Map.empty
        ReleasedDemands = Set.empty
        Attempts = Map.empty
        TerminalCompletions = Map.empty
        Retiring = false
        DrainedNotified = false
    }

    let private effect (state: State) action : Effect = { Epoch = state.Epoch; Action = action }

    let private dependencies (work: Work) =
        work.Definition.Reads
        |> List.choose (fun read ->
            match read.Source with
            | ReadSource.Work id -> Some id
            | ReadSource.Input _ -> None)

    let private reverseClosure (roots: Set<WorkId>) (state: State) =
        let rec expand visited =
            let next =
                state.Works
                |> Map.fold (fun found id work ->
                    if dependencies work |> List.exists (fun dep -> Set.contains dep visited) then
                        Set.add id found
                    else found) visited
            if next = visited then visited else expand next
        expand roots

    let private demanded (state: State) =
        let rec visit visited id =
            if Set.contains id visited then visited
            else
                let next = Set.add id visited
                match Map.tryFind id state.Works with
                | None -> next
                | Some work -> dependencies work |> List.fold visit next
        state.Demands |> Map.fold (fun seen _ work -> visit seen work) Set.empty

    let private scopeOpen (scope: ScopeId) (state: State) =
        match Map.tryFind scope state.Scopes with
        | Some (ScopePhase.Open revision) -> Some revision
        | _ -> None

    let private resolve (work: Work) (state: State) : ResolvedRead list option =
        let readOne (read: Read) =
            match read.Source with
            | ReadSource.Input id ->
                state.Inputs
                |> Map.tryFind id
                |> Option.map (fun value ->
                    { Slot = read.Slot; Value = ReadValue.Input(id, value.Stamp, value.Value) })
            | ReadSource.Work id ->
                state.Works
                |> Map.tryFind id
                |> Option.bind (fun source -> source.Offered)
                |> Option.map (fun value ->
                    { Slot = read.Slot
                      Value = ReadValue.Work(id, value.Definition, value.Attempt, value.Value) })
        let rec loop acc = function
            | [] -> Some(List.rev acc)
            | read :: rest ->
                match readOne read with
                | None -> None
                | Some value -> loop (value :: acc) rest
        loop [] work.Definition.Reads

    let private pendingForWork id (state: State) =
        state.Attempts
        |> Map.toList
        |> List.choose (fun (attemptId, attempt) ->
            if attempt.Request.Definition.Work = id then Some attemptId else None)

    let private pendingForScope scope (state: State) =
        state.Attempts
        |> Map.fold (fun count _ attempt ->
            if attempt.Request.Scope = scope then count + 1 else count) 0

    let pendingAttempts (state: State) =
        state.Attempts.Count

    let private obsoleteAttempt id (state: State) =
        match Map.tryFind id state.Attempts with
        | None -> state, []
        | Some attempt ->
            let cancel = not attempt.CancelSent && Option.isNone attempt.Completion
            let drain = not attempt.DrainSent
            let updated = {
                attempt with
                    Obsolete = true
                    CancelSent = attempt.CancelSent || cancel
                    DrainSent = true
            }
            let effects = [
                if cancel then effect state (EffectAction.Cancel id)
                if drain then effect state (EffectAction.Drain id)
            ]
            { state with Attempts = Map.add id updated state.Attempts }, effects

    // Retained completed values are candidates only; their old handle is withdrawn.
    let private invalidate (ids: Set<WorkId>) (state: State) =
        let withdrawals =
            ids
            |> Set.toList
            |> List.choose (fun id ->
                Map.tryFind id state.Works
                |> Option.bind (fun work -> work.Offered)
                |> Option.map (EffectAction.Withdraw >> effect state))
        let cleared =
            ids
            |> Set.fold (fun current id ->
                match Map.tryFind id current.Works with
                | None -> current
                | Some work ->
                    let changed = { work with Offered = None; Current = None; Failure = None; Cancelled = false }
                    { current with Works = Map.add id changed current.Works }) state
        let stateAfter, cancellations =
            state.Attempts
            |> Map.toList
            |> List.filter (fun (_, attempt) ->
                Set.contains attempt.Request.Definition.Work ids)
            |> List.fold (fun (current, output) (id, _) ->
                let next, emitted = obsoleteAttempt id current
                next, output @ emitted) (cleared, [])
        stateAfter, withdrawals @ cancellations

    // Sorting deduplicates readiness only, never the stored read occurrences.
    // Missing definitions block evaluation, but cannot themselves form a cycle.
    let private topological (works: Map<WorkId, Work>) =
        let rec sort ordered remaining =
            if Map.isEmpty remaining then Ok(List.rev ordered)
            else
                let ready =
                    remaining
                    |> Map.toList
                    |> List.tryFind (fun (_, work) ->
                        dependencies work |> List.forall (fun id -> not (Map.containsKey id remaining)))
                match ready with
                | None -> Error(ProtocolError.DependencyCycle(remaining |> Map.toList |> List.map fst))
                | Some(id, _) -> sort (id :: ordered) (Map.remove id remaining)
        sort [] works

    let private finishClosures (state: State) =
        let closed =
            state.Scopes
            |> Map.toList
            |> List.choose (fun (scope, phase) ->
                if phase = ScopePhase.Closing && pendingForScope scope state = 0 then Some scope else None)
        let scopes = closed |> List.fold (fun all scope -> Map.add scope ScopePhase.Closed all) state.Scopes
        let drained = state.Retiring && not state.DrainedNotified && pendingAttempts state = 0
        let next = { state with Scopes = scopes; DrainedNotified = state.DrainedNotified || drained }
        let effects = [
            for scope in closed do effect state (EffectAction.ScopeClosed scope)
            if drained then effect state EffectAction.EpochDrained
        ]
        next, effects

    let private advance (initial: State) =
        let demand = demanded initial
        let unnecessary =
            initial.Works
            |> Map.toList
            |> List.choose (fun (id, work) ->
                if not (Set.contains id demand) then work.Current |> Option.map (fun attempt -> id, attempt)
                else None)
        let state, cancelled =
            unnecessary
            |> List.fold (fun (current, output) (id, attempt) ->
                let next, effects = obsoleteAttempt attempt current
                let work = Map.find id next.Works
                { next with Works = Map.add id { work with Current = None } next.Works }, output @ effects)
                (initial, [])
        match topological state.Works with
        | Error error -> Error error
        | Ok order ->
            let rec schedule current output = function
                | [] ->
                    let final, closures = finishClosures current
                    Ok(final, output @ closures)
                | id :: rest ->
                    let work = Map.find id current.Works
                    match scopeOpen work.Scope current, resolve work current with
                    | Some revision, Some reads when not current.Retiring ->
                        if Option.isSome work.Offered || Option.isSome work.Current
                           || not (List.isEmpty (pendingForWork id current)) then
                            schedule current output rest
                        else
                            match work.Cached with
                            | Some cached when cached.Reads = reads ->
                                if current.NextEligibility = System.UInt64.MaxValue then Error ProtocolError.IdentifierExhausted
                                else
                                    let handle = {
                                        Epoch = current.Epoch; Scope = work.Scope; Revision = revision; Work = id
                                        Definition = work.Definition.Stamp; Attempt = cached.Attempt
                                        Eligibility = EligibilityId current.NextEligibility; Value = cached.Value
                                    }
                                    let next = {
                                        current with
                                            NextEligibility = current.NextEligibility + 1UL
                                            Works = Map.add id { work with Offered = Some handle } current.Works
                                    }
                                    schedule next (output @ [effect next (EffectAction.Offer handle)]) rest
                            | _ when Set.contains id demand && Option.isNone work.Failure && not work.Cancelled ->
                                if current.NextAttempt = System.UInt64.MaxValue then Error ProtocolError.IdentifierExhausted
                                else
                                    let attemptId = AttemptId current.NextAttempt
                                    let request = {
                                        Epoch = current.Epoch; Scope = work.Scope; Revision = revision
                                        Attempt = attemptId; Definition = work.Definition; Reads = reads
                                    }
                                    let attempt = {
                                        Request = request; Completion = None
                                        Obsolete = false; CancelSent = false; DrainSent = false
                                    }
                                    let next = {
                                        current with
                                            NextAttempt = current.NextAttempt + 1UL
                                            Works = Map.add id { work with Current = Some attemptId } current.Works
                                            Attempts = Map.add attemptId attempt current.Attempts
                                    }
                                    schedule next (output @ [effect next (EffectAction.Start request)]) rest
                            | _ -> schedule current output rest
                    | _ -> schedule current output rest
            schedule state cancelled order

    let private reserve scope revision (state: State) =
        let validation =
            match Map.tryFind scope state.Scopes with
            | None -> Ok false
            | Some (ScopePhase.Reserved old) when old = revision -> Ok true
            | Some (ScopePhase.Open old)
            | Some (ScopePhase.Reserved old) ->
                if revision > old then Ok false else Error(ProtocolError.RevisionNotIncreasing scope)
            | Some ScopePhase.Closing
            | Some ScopePhase.Closed -> Error(ProtocolError.ClosedScope scope)
        match validation with
        | Error error -> Error error
        | Ok true -> Ok(state, [])
        | Ok false ->
            let roots =
                state.Works |> Map.toList |> List.choose (fun (id, work) -> if work.Scope = scope then Some id else None) |> Set.ofList
            let changed, effects = invalidate (reverseClosure roots state) state
            Ok({ changed with Scopes = Map.add scope (ScopePhase.Reserved revision) changed.Scopes }, effects)

    let private validateReads (definition: Definition) =
        let rec visit seen = function
            | [] -> Ok()
            | (read: Read) :: rest ->
                if Set.contains read.Slot seen then Error(ProtocolError.DuplicateReadSlot(definition.Work, read.Slot))
                else visit (Set.add read.Slot seen) rest
        visit Set.empty definition.Reads

    let private replace scope revision entries (state: State) =
        match Map.tryFind scope state.Scopes with
        | Some (ScopePhase.Reserved reserved) when reserved = revision ->
            let rec build seen definitions stamps = function
                | [] -> Ok(definitions, stamps)
                | entry :: rest ->
                    let id = match entry with ScopeEntry.Retain id -> id | ScopeEntry.Define value -> value.Work
                    if Set.contains id seen then Error(ProtocolError.DuplicateWork id)
                    else
                        match Map.tryFind id state.Works with
                        | Some work when work.Scope <> scope -> Error(ProtocolError.ForeignWorkOwner id)
                        | existing ->
                            let chosen =
                                match entry, existing with
                                | ScopeEntry.Retain _, None -> Error(ProtocolError.UnknownRetainedWork id)
                                | ScopeEntry.Retain _, Some work -> Ok(work, stamps)
                                | ScopeEntry.Define definition, _ ->
                                    if Set.contains (id, definition.Stamp) state.DefinitionStamps then
                                        Error(ProtocolError.DefinitionStampReused id)
                                    else
                                        validateReads definition
                                        |> Result.map (fun () ->
                                            { Scope = scope; Definition = definition; Offered = None; Cached = None
                                              Current = None; Failure = None; Cancelled = false },
                                            Set.add (id, definition.Stamp) stamps)
                            match chosen with
                            | Error error -> Error error
                            | Ok(work, newStamps) -> build (Set.add id seen) (Map.add id work definitions) newStamps rest
            match build Set.empty Map.empty state.DefinitionStamps entries with
            | Error error -> Error error
            | Ok(owned, stamps) ->
                let other = state.Works |> Map.filter (fun _ work -> work.Scope <> scope)
                let merged = owned |> Map.fold (fun all id work -> Map.add id work all) other
                match topological merged with
                | Error error -> Error error
                | Ok _ ->
                    // Reserve already withdrew the old closure. Adding a formerly
                    // missing producer cannot make an old unresolved result valid.
                    let next = {
                        state with
                            Works = merged
                            DefinitionStamps = stamps
                            Scopes = Map.add scope (ScopePhase.Open revision) state.Scopes
                    }
                    Ok(next, [])
        | None -> Error(ProtocolError.UnknownScope scope)
        | _ -> Error(ProtocolError.ReservationMismatch scope)

    let private setInputs values (state: State) =
        let rec validate seen changed inputs stamps = function
            | [] -> Ok(changed, inputs, stamps)
            | (value: InputValue) :: rest ->
                if Set.contains value.Input seen then Error(ProtocolError.DuplicateInput value.Input)
                else
                    let duplicate = Map.tryFind value.Input state.Inputs = Some value
                    if not duplicate && Set.contains (value.Input, value.Stamp) stamps then
                        Error(ProtocolError.InputStampReused value.Input)
                    else
                        validate (Set.add value.Input seen)
                            (if duplicate then changed else Set.add value.Input changed)
                            (Map.add value.Input value inputs) (Set.add (value.Input, value.Stamp) stamps) rest
        match validate Set.empty Set.empty state.Inputs state.InputStamps values with
        | Error error -> Error error
        | Ok(changed, inputs, stamps) ->
            let roots =
                state.Works
                |> Map.toList
                |> List.choose (fun (id, work) ->
                    if work.Definition.Reads |> List.exists (fun read ->
                        match read.Source with ReadSource.Input input -> Set.contains input changed | _ -> false)
                    then Some id else None)
                |> Set.ofList
            let invalidated, effects = invalidate (reverseClosure roots state) state
            Ok({ invalidated with Inputs = inputs; InputStamps = stamps }, effects)

    let private removeInputs ids (state: State) =
        let removed = ids |> List.filter (fun id -> Map.containsKey id state.Inputs) |> Set.ofList
        let roots =
            state.Works
            |> Map.toList
            |> List.choose (fun (id, work) ->
                if work.Definition.Reads |> List.exists (fun read ->
                    match read.Source with ReadSource.Input input -> Set.contains input removed | _ -> false)
                then Some id else None)
            |> Set.ofList
        let invalidated, effects = invalidate (reverseClosure roots state) state
        let inputs = removed |> Set.fold (fun all id -> Map.remove id all) invalidated.Inputs
        Ok({ invalidated with Inputs = inputs }, effects)

    let private finish id completion (state: State) =
        match Map.tryFind id state.Attempts with
        | None ->
            match Map.tryFind id state.TerminalCompletions with
            | Some previous when previous = completion -> Ok(state, [])
            | Some _ -> Error(ProtocolError.ConflictingCompletion id)
            | None -> Error(ProtocolError.UnknownAttempt id)
        | Some attempt ->
            match attempt.Completion with
            | Some previous when previous = completion -> Ok(state, [])
            | Some _ -> Error(ProtocolError.ConflictingCompletion id)
            | None ->
                let updated = { attempt with Completion = Some completion; DrainSent = true }
                let effects = if attempt.DrainSent then [] else [effect state (EffectAction.Drain id)]
                Ok({ state with Attempts = Map.add id updated state.Attempts }, effects)

    let private drain id (state: State) =
        match Map.tryFind id state.Attempts with
        | None when Map.containsKey id state.TerminalCompletions -> Ok(state, [])
        | None -> Error(ProtocolError.UnknownAttempt id)
        | Some attempt ->
            match attempt.Completion with
            | None -> Error(ProtocolError.DrainBeforeCompletion id)
            | Some completion ->
                // Keep only the terminal outcome for duplicate/conflicting callback
                // checks. A drained request no longer retains its definition/reads.
                // Current work caches separately retain receipts needed for reuse.
                let changed = {
                    state with
                        Attempts = Map.remove id state.Attempts
                        TerminalCompletions = Map.add id completion state.TerminalCompletions
                }
                let workId = attempt.Request.Definition.Work
                match Map.tryFind workId changed.Works with
                | Some work when work.Current = Some id && not attempt.Obsolete
                                 && scopeOpen work.Scope changed = Some attempt.Request.Revision
                                 && resolve work changed = Some attempt.Request.Reads
                                 && Set.contains workId (demanded changed) ->
                    let updated =
                        match completion with
                        | Completion.Succeeded value ->
                            { work with Current = None; Failure = None; Cancelled = false
                                        Cached = Some { Attempt = id; Value = value; Reads = attempt.Request.Reads } }
                        | Completion.Failed failure -> { work with Current = None; Failure = Some failure; Cancelled = false }
                        | Completion.Cancelled -> { work with Current = None; Failure = None; Cancelled = true }
                    Ok({ changed with Works = Map.add workId updated changed.Works }, [])
                | Some work when work.Current = Some id ->
                    Ok({ changed with Works = Map.add workId { work with Current = None } changed.Works }, [])
                | _ -> Ok(changed, [])

    let private close scope (state: State) =
        match Map.tryFind scope state.Scopes with
        | None -> Error(ProtocolError.UnknownScope scope)
        | Some ScopePhase.Closed
        | Some ScopePhase.Closing -> Ok(state, [])
        | Some _ ->
            let owned =
                state.Works |> Map.toList |> List.choose (fun (id, work) -> if work.Scope = scope then Some id else None) |> Set.ofList
            let invalidated, effects = invalidate (reverseClosure owned state) state
            let released =
                invalidated.Demands |> Map.toList |> List.choose (fun (id, work) -> if Set.contains work owned then Some id else None)
            let next = {
                invalidated with
                    Works = invalidated.Works |> Map.filter (fun id _ -> not (Set.contains id owned))
                    Scopes = Map.add scope ScopePhase.Closing invalidated.Scopes
                    Demands = released |> List.fold (fun all id -> Map.remove id all) invalidated.Demands
                    ReleasedDemands = Set.union invalidated.ReleasedDemands (Set.ofList released)
            }
            Ok(next, effects)

    let private retire (state: State) =
        let rec closeAll (current: State) output = function
            | [] -> Ok({ current with Retiring = true }, output)
            | scope :: rest ->
                match close scope current with
                | Error error -> Error error
                | Ok(next, effects) -> closeAll next (output @ effects) rest
        closeAll state [] (state.Scopes |> Map.toList |> List.map fst)

    let private apply action (state: State) =
        match action with
        | Action.ReserveScope(scope, revision) -> reserve scope revision state
        | Action.ReplaceScope(scope, revision, entries) -> replace scope revision entries state
        | Action.SetInputs values -> setInputs values state
        | Action.RemoveInputs ids -> removeInputs ids state
        | Action.Demand(id, work) ->
            if Set.contains id state.ReleasedDemands then Error(ProtocolError.DemandIdReused id)
            else
                match Map.tryFind id state.Demands with
                | Some previous when previous = work -> Ok(state, [])
                | Some _ -> Error(ProtocolError.DemandIdReused id)
                | None when not (Map.containsKey work state.Works) -> Error(ProtocolError.UnknownWork work)
                | None -> Ok({ state with Demands = Map.add id work state.Demands }, [])
        | Action.Release id ->
            let next = {
                state with
                    Demands = Map.remove id state.Demands
                    ReleasedDemands = Set.add id state.ReleasedDemands
            }
            Ok(next, [])
        | Action.Retry id ->
            match Map.tryFind id state.Works with
            | None -> Error(ProtocolError.UnknownWork id)
            | Some work when Option.isSome work.Failure || work.Cancelled ->
                Ok({ state with Works = Map.add id { work with Failure = None; Cancelled = false } state.Works }, [])
            | Some _ -> Error(ProtocolError.RetryNotFailed id)
        | Action.Finished(id, completion) -> finish id completion state
        | Action.Drained id -> drain id state
        | Action.CloseScope scope -> close scope state
        | Action.Retire -> retire state

    /// Validate, update immutable state, and then produce deterministic host effects.
    let step (command: Command) (state: State) =
        if command.Epoch <> state.Epoch then Error ProtocolError.ForeignEpoch
        else
            let permitted =
                match command.Action with
                | Action.Finished _ | Action.Drained _ | Action.Release _ | Action.CloseScope _ | Action.Retire -> true
                | _ -> not state.Retiring
            if not permitted then Error ProtocolError.RetiredEpoch
            else
                match apply command.Action state with
                | Error error -> Error error
                | Ok(changed, effects) ->
                    advance changed |> Result.map (fun (next, scheduled) -> next, effects @ scheduled)

    let tryResult work (state: State) =
        Map.tryFind work state.Works |> Option.bind (fun found -> found.Offered)

    let isEligible (handle: ResultHandle) (state: State) =
        handle.Epoch = state.Epoch && tryResult handle.Work state = Some handle

    let scopeClosed scope (state: State) = Map.tryFind scope state.Scopes = Some ScopePhase.Closed

    let isDrained (state: State) = state.Retiring && state.DrainedNotified && pendingAttempts state = 0

    let snapshot (state: State) : Snapshot =
        let demand = demanded state
        let status id (work: Work) =
            match scopeOpen work.Scope state with
            | None -> WorkStatus.Suspended
            | Some _ ->
                match work.Offered, work.Current, work.Failure, work.Cancelled with
                | Some value, _, _, _ -> WorkStatus.Eligible value
                | _, Some attemptId, _, _ ->
                    let attempt = Map.find attemptId state.Attempts
                    if Option.isSome attempt.Completion then WorkStatus.Draining attemptId
                    else WorkStatus.Running attemptId
                | _, _, Some failure, _ -> WorkStatus.Failed failure
                | _, _, _, true -> WorkStatus.Cancelled
                | _ ->
                    match pendingForWork id state with
                    | attempt :: _ -> WorkStatus.Draining attempt
                    | [] when Set.contains id demand && Option.isNone (resolve work state) -> WorkStatus.Waiting
                    | [] -> WorkStatus.Idle
        {
            Epoch = state.Epoch
            Retiring = state.Retiring
            PendingAttempts = pendingAttempts state
            Scopes = state.Scopes |> Map.toList |> List.map (fun (id, phase) ->
                { Scope = id; Phase = phase; PendingAttempts = pendingForScope id state })
            Works = state.Works |> Map.toList |> List.map (fun (id, work) ->
                { Work = id; Scope = work.Scope; Demanded = Set.contains id demand; Status = status id work })
        }
