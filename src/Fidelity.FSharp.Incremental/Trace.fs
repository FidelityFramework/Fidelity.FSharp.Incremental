namespace Fidelity.FSharp.Incremental

/// Compile-time disabled trace marker; erased by F# inlining.
[<Struct>]
type TraceOff = TraceOff

/// Active causal label sink wrapper; evaluates lazy labels only when supplied.
[<Struct>]
type TraceOn = TraceOn of (TraceLabel -> unit)

/// Zero-BCL compile-time erasable causal label surface for read slots and work units.
[<AbstractClass; Sealed>]
type Trace private () =
    /// Disabled trace mode token.
    static member inline Off = TraceOff

    /// Creates an active trace mode token forwarding evaluated labels to sink.
    static member inline On(sink: TraceLabel -> unit) = TraceOn sink

    /// Untraced causal annotation: compiles to a zero-allocation identity expression
    /// returning value without allocating a closure or evaluating label.
    static member inline named
        ([<InlineIfLambda>] _label: unit -> string,
         _work: WorkId,
         _slot: ReadSlotId,
         value: 'T) : 'T =
        value

    /// Explicitly disabled causal annotation: compiles to a zero-allocation identity
    /// expression returning value without allocating a closure or evaluating label.
    static member inline named
        (_mode: TraceOff,
         [<InlineIfLambda>] _label: unit -> string,
         _work: WorkId,
         _slot: ReadSlotId,
         value: 'T) : 'T =
        value

    /// Active causal annotation: evaluates label inline and forwards the TraceLabel
    /// for work and slot to the active sink before returning value.
    static member inline named
        (mode: TraceOn,
         [<InlineIfLambda>] label: unit -> string,
         work: WorkId,
         slot: ReadSlotId,
         value: 'T) : 'T =
        let (TraceOn sink) = mode
        sink { Work = work; Slot = slot; Label = label () }
        value

    /// Runtime-gated causal annotation: inlines both sink and label so disabled runs
    /// neither allocate closures nor evaluate label.
    static member inline named
        (enabled: bool,
         [<InlineIfLambda>] sink: TraceLabel -> unit,
         [<InlineIfLambda>] label: unit -> string,
         work: WorkId,
         slot: ReadSlotId,
         value: 'T) : 'T =
        if enabled then
            sink { Work = work; Slot = slot; Label = label () }
        value

    /// Pure immutable map helper recording a lazy causal label only when enabled.
    static member inline withLabel
        (enabled: bool,
         labels: Map<WorkId * ReadSlotId, string>,
         [<InlineIfLambda>] label: unit -> string,
         work: WorkId,
         slot: ReadSlotId) : Map<WorkId * ReadSlotId, string> =
        if enabled then
            Map.add (work, slot) (label ()) labels
        else
            labels
