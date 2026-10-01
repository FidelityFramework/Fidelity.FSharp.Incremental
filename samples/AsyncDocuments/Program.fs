module Fidelity.FSharp.Incremental.AsyncDocumentsSample

open System
open System.Threading
open Fidelity.FSharp.Incremental
open Fidelity.FSharp.Incremental.Hosting

type Payload = Document of string | Banner of string | WordCount of int

// Immutable owner data. The scheduler never interprets these payloads or tokens.
let payloads = Map.ofList [
    ValueToken 10UL, Document "hello world"
    ValueToken 11UL, Document "hello async world"
    ValueToken 20UL, Banner "Document analysis"
    ValueToken 102UL, WordCount 2
    ValueToken 103UL, WordCount 3
]

let document token =
    match Map.find token payloads with
    | Document text -> text
    | other -> failwithf "Expected a document, got %A" other

let inputToken (request: StartRequest) =
    match request.Reads with
    | [{ Value = ReadValue.Input(_, _, value) }] -> value
    | _ -> failwith "Expected one declared input occurrence."

let evaluate invocation cancellation = async {
    if WorkCancellation.isRequested cancellation then
        return StepOutcome.Complete Completion.Cancelled
    else
        match invocation with
        | StepInvocation.Start request ->
            let value = inputToken request
            match Map.find value payloads with
            | Document _ -> return StepOutcome.Suspend(StepId 1UL, value)
            | Banner _ -> return StepOutcome.Complete(Completion.Succeeded value)
            | WordCount _ -> return failwith "Unexpected input payload."
        | StepInvocation.Resume request ->
            let declared = inputToken request.Start
            // Both checkpoint and response refer to the declared document snapshot.
            if request.Suspension.Environment <> declared || request.Response <> declared then
                return failwith "Response does not belong to the declared document."
            else
                let count = (document declared).Split(' ', StringSplitOptions.RemoveEmptyEntries).Length
                let result = payloads |> Map.pick (fun token payload ->
                    match payload with WordCount value when value = count -> Some token | _ -> None)
                return StepOutcome.Complete(Completion.Succeeded result)
}

let require = function Ok value -> value | Error error -> failwithf "%A" error
let bounded workflow = async {
    let! observation = Async.StartChild(workflow, millisecondsTimeout = 5000)
    return! observation
}
let until predicate = bounded (async {
    // The predicate establishes ordering; the deadline only bounds this observer.
    while not (predicate ()) do do! Async.Sleep 1
})
let admitted handle action = AsyncMailbox.admit action handle |> require
let post handle action = async {
    let operation = admitted handle action
    let! answer = bounded (AsyncMailbox.observe operation)
    return require answer
}
let send handle action = async { let! _ = post handle action in return () }
let refusal handle action = async {
    let! answer = bounded (AsyncMailbox.observe (admitted handle action))
    match answer with
    | Error(MailboxError.InvalidCommand(ProtocolError.InvalidSuspension _)) -> ()
    | other -> failwithf "Expected invalid suspension, got %A" other
}

let run () = async {
    let changing, stable, scope = WorkId 1UL, WorkId 2UL, ScopeId 1UL
    let handle = AsyncMailbox.create { Epoch = EpochId 1UL; MaxConcurrency = 1; CommandCapacity = 16 } evaluate |> require
    let mutable source = document (ValueToken 10UL)
    AsyncMailbox.start handle |> require
    let scenario = async {
        let input id stamp token = { Input = InputId id; Stamp = InputStamp stamp; Value = ValueToken token }
        let definition id = ScopeEntry.Define {
            Work = WorkId id; Stamp = DefinitionStamp 1UL
            Reads = [{ Slot = ReadSlotId 1UL; Source = ReadSource.Input(InputId id) }]
        }
        do! send handle (Action.SetInputs [input 1UL 1UL 10UL; input 2UL 1UL 20UL])
        do! send handle (Action.ReserveScope(scope, RevisionId 1UL))
        do! send handle (Action.ReplaceScope(scope, RevisionId 1UL, [definition 1UL; definition 2UL]))
        do! send handle (Action.Demand(DemandId 1UL, changing))
        do! send handle (Action.Demand(DemandId 2UL, stable))
        let ready () = (AsyncMailbox.snapshot handle).Suspensions.Length = 1 && (AsyncMailbox.tryResult stable handle).IsSome
        do! until ready
        let obsolete = (AsyncMailbox.snapshot handle).Suspensions.Head
        let originalBanner = (AsyncMailbox.tryResult stable handle).Value
        let reservation = admitted handle (Action.ReserveScope(scope, RevisionId 2UL))
        let! first = bounded (AsyncMailbox.observe reservation)
        let! reconciled = bounded (AsyncMailbox.observe reservation)
        let receipt = require first
        if require reconciled <> receipt then failwith "Exact reservation observation changed."
        if AsyncMailbox.isEligible originalBanner handle then failwith "Old eligibility survived reservation."
        // Source mutation follows the exact successful acknowledgement.
        source <- document (ValueToken 11UL)
        do! send handle (Action.SetInputs [input 1UL 2UL 11UL])
        do! refusal handle (Action.Resume(obsolete, ValueToken 10UL))
        do! send handle (Action.ReplaceScope(scope, RevisionId 2UL, [ScopeEntry.Retain changing; ScopeEntry.Retain stable]))
        do! until ready
        let current = (AsyncMailbox.snapshot handle).Suspensions.Head
        do! send handle (Action.Resume(current, ValueToken 11UL))
        do! refusal handle (Action.Resume(current, ValueToken 11UL))
        let! idle = bounded (AsyncMailbox.waitForIdle handle)
        require idle
        let banner = (AsyncMailbox.tryResult stable handle).Value
        let result = (AsyncMailbox.tryResult changing handle).Value
        if banner.Attempt <> originalBanner.Attempt then failwith "Unchanged banner was rerun."
        if Map.find result.Value payloads <> WordCount 3 then failwith "Incorrect document analysis."
        printfn "reservation %d reconciled before source mutation: %s" receipt.Order source
        printfn "stale and duplicate resumes refused; result=%A; banner attempt retained" (Map.find result.Value payloads)
    }
    let! outcome = Async.Catch scenario
    let! closed = bounded (AsyncMailbox.close handle)
    require closed
    match outcome with Choice1Of2 () -> () | Choice2Of2 error -> raise error
    printfn "closed with %d pending attempts" (AsyncMailbox.snapshot handle).Graph.PendingAttempts
}

[<EntryPoint>]
let main _ =
    Async.RunSynchronously(run (), cancellationToken = CancellationToken.None)
    0
