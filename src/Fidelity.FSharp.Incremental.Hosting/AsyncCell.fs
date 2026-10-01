namespace Fidelity.FSharp.Incremental.Hosting

open System
open System.Collections.Generic
open System.Threading

/// Temporary observer continuations belong to this execution boundary, never to
/// graph values or saved evaluator environments.
module internal AsyncCell =
    [<NoEquality; NoComparison>]
    type private Waiter<'T> = {
        Resume: 'T -> unit
        Cancel: OperationCanceledException -> unit
        mutable Node: LinkedListNode<Waiter<'T>> option
        mutable Registration: CancellationTokenRegistration option
    }

    [<NoEquality; NoComparison>]
    type Cell<'T> = private {
        Gate: obj
        mutable Value: 'T option
        Waiters: LinkedList<Waiter<'T>>
    }

    let create () : Cell<'T> = {
        Gate = obj ()
        Value = None
        Waiters = LinkedList<Waiter<'T>>()
    }

    let private dispatch action =
        Async.Start(async { action () }, cancellationToken = CancellationToken.None)

    // Unregister is nonblocking, including when a cancellation callback is
    // already running. The cell lock decides which terminal observation wins.
    let private unregister registration =
        registration |> Option.iter (fun (value: CancellationTokenRegistration) ->
            value.Unregister() |> ignore)

    let tryRead cell = lock cell.Gate (fun () -> cell.Value)

    let waiterCount cell = lock cell.Gate (fun () -> cell.Waiters.Count)

    let publish value cell =
        let pending = lock cell.Gate (fun () ->
            match cell.Value with
            | Some _ -> None
            | None ->
                cell.Value <- Some value
                let observers =
                    cell.Waiters
                    |> Seq.map (fun waiter ->
                        let registration = waiter.Registration
                        waiter.Node <- None
                        waiter.Registration <- None
                        waiter, registration)
                    |> Seq.toArray
                cell.Waiters.Clear()
                Some observers)
        match pending with
        | None -> false
        | Some observers ->
            for waiter, registration in observers do
                unregister registration
                dispatch (fun () -> waiter.Resume value)
            true

    /// Cold, repeatable observation. Cancellation removes only this waiter.
    let observe cell = async {
        let! token = Async.CancellationToken
        return! Async.FromContinuations(fun (success, _, cancelled) ->
            let waiter = {
                Resume = success
                Cancel = cancelled
                Node = None
                Registration = None
            }
            let ready = lock cell.Gate (fun () ->
                match cell.Value with
                | Some value -> Some value
                | None ->
                    waiter.Node <- Some(cell.Waiters.AddLast waiter)
                    None)
            match ready with
            | Some value -> dispatch (fun () -> success value)
            | None when token.CanBeCanceled ->
                // Register can invoke synchronously. Install the waiter first,
                // then retain the registration only if the waiter still exists.
                let registration = token.Register(fun () ->
                    let removed = lock cell.Gate (fun () ->
                        match waiter.Node with
                        | None -> None
                        | Some node ->
                            cell.Waiters.Remove node
                            waiter.Node <- None
                            let previous = waiter.Registration
                            waiter.Registration <- None
                            Some previous)
                    match removed with
                    | None -> ()
                    | Some previous ->
                        unregister previous
                        dispatch (fun () -> waiter.Cancel(OperationCanceledException token)))
                let retained = lock cell.Gate (fun () ->
                    if waiter.Node.IsSome then
                        waiter.Registration <- Some registration
                        true
                    else false)
                if not retained then unregister (Some registration)
            | None -> ())
    }
