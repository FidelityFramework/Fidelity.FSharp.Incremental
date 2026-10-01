module Fidelity.FSharp.Incremental.MailboxSample

open System
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

let post (host: MailboxHost) action = task {
    let! result = host.PostAsync action
    match result with
    | Ok receipt -> return receipt
    | Error error -> return failwithf "Command rejected: %A" error
}

let waitUntil (deadline: CancellationToken) predicate = task {
    while not (predicate ()) do
        deadline.ThrowIfCancellationRequested()
        do! Task.Delay(1, deadline)
}

let run () = task {
    use deadline = new CancellationTokenSource(TimeSpan.FromSeconds 10.)
    let changing, stable, scope = WorkId 1UL, WorkId 2UL, ScopeId 1UL
    let mutable resumes = 0
    let evaluate invocation (_: CancellationToken) = task {
        match invocation with
        | StepInvocation.Start request when request.Definition.Work = changing ->
            // The checkpoint contains immutable data only. Its semantic inputs
            // (including the eventual response) are already declared in Reads.
            return StepOutcome.Suspend(StepId 1UL, ValueToken 100UL)
        | StepInvocation.Start request ->
            let value =
                match request.Reads.Head.Value with
                | ReadValue.Input(_, _, value) -> value
                | _ -> failwith "Expected declared input."
            return StepOutcome.Complete(Completion.Succeeded value)
        | StepInvocation.Resume request ->
            Interlocked.Increment(&resumes) |> ignore
            return StepOutcome.Complete(Completion.Succeeded request.Response)
    }
    use host = new MailboxHost(EpochId 1UL, 1, 16, evaluate)
    let input id stamp value = { Input = InputId id; Stamp = InputStamp stamp; Value = ValueToken value }
    let definition id = ScopeEntry.Define {
        Work = WorkId id; Stamp = DefinitionStamp 1UL
        Reads = [{ Slot = ReadSlotId 1UL; Source = ReadSource.Input(InputId id) }]
    }
    let! _ = post host (Action.SetInputs [input 1UL 1UL 10UL; input 2UL 1UL 20UL])
    let! _ = post host (Action.ReserveScope(scope, RevisionId 1UL))
    let! _ = post host (Action.ReplaceScope(scope, RevisionId 1UL, [definition 1UL; definition 2UL]))
    let! _ = post host (Action.Demand(DemandId 1UL, changing))
    let! _ = post host (Action.Demand(DemandId 2UL, stable))
    do! waitUntil deadline.Token (fun () -> host.Snapshot.Suspensions.Length = 1 && (host.TryResult stable).IsSome)
    let obsolete = host.Snapshot.Suspensions.Head
    let oldStable = (host.TryResult stable).Value

    let! reservation = post host (Action.ReserveScope(scope, RevisionId 2UL))
    if host.IsEligible oldStable then failwith "Reservation did not revoke the old handle."
    // Mutation follows acknowledgement, never merely command enqueue.
    let! _ = post host (Action.SetInputs [input 1UL 2UL 11UL])
    let! stale = host.PostAsync(Action.Resume(obsolete, ValueToken 10UL))
    match stale with
    | Error(MailboxError.InvalidCommand(ProtocolError.InvalidSuspension _)) -> ()
    | other -> failwithf "Expected stale resume rejection, got %A" other
    if resumes <> 0 then failwith "An obsolete step was resumed."
    let! _ = post host (Action.ReplaceScope(scope, RevisionId 2UL, [ScopeEntry.Retain changing; ScopeEntry.Retain stable]))
    do! waitUntil deadline.Token (fun () -> host.Snapshot.Suspensions.Length = 1 && (host.TryResult stable).IsSome)
    let current = host.Snapshot.Suspensions.Head
    // Response is the value of the newly stamped, declared input, not ambient I/O.
    let! _ = post host (Action.Resume(current, ValueToken 11UL))
    do! host.WaitForIdleAsync deadline.Token
    let retained = (host.TryResult stable).Value
    let result = (host.TryResult changing).Value
    if retained.Attempt <> oldStable.Attempt then failwith "Unchanged work was rerun."
    if result.Value <> ValueToken 11UL || resumes <> 1 then failwith "Incorrect resumed result."
    printfn "reservation acknowledged at command %d before input mutation" reservation.Order
    printfn "stale resume refused; current result=%A; resume invocations=%d" result.Value resumes
    printfn "unchanged attempt retained=%b; old eligibility revoked=%b" (retained.Attempt = oldStable.Attempt) (not (host.IsEligible oldStable))
    do! host.CloseAsync()
    printfn "closed with %d pending attempts" host.Snapshot.Graph.PendingAttempts
}

[<EntryPoint>]
let main _ =
    run().GetAwaiter().GetResult()
    0
