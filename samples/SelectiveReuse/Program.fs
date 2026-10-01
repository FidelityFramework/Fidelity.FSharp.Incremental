module Fidelity.FSharp.Incremental.Sample

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

let send (host: Host) action =
    match host.Send action with Ok _ -> () | Error error -> failwithf "%A" error

let definition work reads =
    ScopeEntry.Define {
        Work = WorkId work
        Stamp = DefinitionStamp 1UL
        Reads = reads |> List.mapi (fun slot source -> { Slot = ReadSlotId(uint64 slot); Source = source })
    }

let run () = task {
    let mutable visits = 0
    let evaluate (request: StartRequest) (token: CancellationToken) = task {
        token.ThrowIfCancellationRequested()
        Interlocked.Increment(&visits) |> ignore
        let values = request.Reads |> List.map (fun read ->
            match read.Value with
            | ReadValue.Input (_, _, ValueToken value)
            | ReadValue.Work (_, _, _, ValueToken value) -> value)
        return ValueToken(List.sum values)
    }
    use host = new Host(EpochId 1UL, 2, evaluate)
    let scope, stable, changed, sum = ScopeId 1UL, WorkId 1UL, WorkId 2UL, WorkId 3UL
    send host (Action.SetInputs [
        { Input = InputId 1UL; Stamp = InputStamp 1UL; Value = ValueToken 10UL }
        { Input = InputId 2UL; Stamp = InputStamp 1UL; Value = ValueToken 20UL }])
    send host (Action.ReserveScope(scope, RevisionId 1UL))
    send host (Action.ReplaceScope(scope, RevisionId 1UL, [
        definition 1UL [ReadSource.Input(InputId 1UL)]
        definition 2UL [ReadSource.Input(InputId 2UL)]
        definition 3UL [ReadSource.Work stable; ReadSource.Work changed]]))
    let timer = Stopwatch.StartNew()
    send host (Action.Demand(DemandId 1UL, sum))
    do! host.WaitForIdleAsync(CancellationToken.None)
    let initialElapsed = timer.Elapsed.TotalMilliseconds
    let initialVisits = visits
    let oldStable = (host.TryResult stable).Value
    let oldSum = (host.TryResult sum).Value

    // Reserve before changing the simulated source input.
    send host (Action.ReserveScope(scope, RevisionId 2UL))
    if host.IsEligible oldStable || host.IsEligible oldSum then failwith "Reservation failed to revoke authority."
    send host (Action.SetInputs [{ Input = InputId 2UL; Stamp = InputStamp 2UL; Value = ValueToken 25UL }])
    timer.Restart()
    send host (Action.ReplaceScope(scope, RevisionId 2UL, [ScopeEntry.Retain stable; ScopeEntry.Retain changed; ScopeEntry.Retain sum]))
    do! host.WaitForIdleAsync(CancellationToken.None)
    let editElapsed = timer.Elapsed.TotalMilliseconds
    let currentStable = (host.TryResult stable).Value
    let currentSum = (host.TryResult sum).Value
    let editedVisits = visits - initialVisits
    if currentStable.Attempt <> oldStable.Attempt then failwith "Unchanged computation was evaluated again."
    if currentSum.Value <> ValueToken 35UL then failwith "Changed dependency did not reach its consumer."
    if editedVisits <> 2 then failwithf "Expected 2 affected visits, got %d" editedVisits
    printfn "initial value=%A visits=%d elapsed_ms=%.3f" oldSum.Value initialVisits initialElapsed
    printfn "edited value=%A visits=%d elapsed_ms=%.3f" currentSum.Value editedVisits editElapsed
    printfn "affected_visits/full_visits=%d/%d (%.3f)" editedVisits initialVisits (float editedVisits / float initialVisits)
    printfn "stable attempt retained=%b; old handle revoked=%b" (currentStable.Attempt = oldStable.Attempt) (not (host.IsEligible oldStable))
    printfn "Elapsed times are a smoke measurement, not a compiler or solver benchmark."
}

[<EntryPoint>]
let main _ =
    run().GetAwaiter().GetResult()
    0
