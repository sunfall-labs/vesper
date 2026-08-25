import { Schema } from 'effect';
import { Prompt, type Response, type Tool } from 'effect/unstable/ai';

import { Stop } from './stop.js';

// What a caller observes while an agent runs.
//
// A bare stream of `Response.StreamPart` would lose the one thing a
// multi-turn loop adds over a single call: turn boundaries. Without them a
// consumer cannot tell the difference between one long answer and three
// short ones, cannot attribute a tool call to the turn that requested it,
// and cannot render "thinking… step 3 of N".
//
// Modelled as a tagged union rather than a callback bag so consumers can
// exhaustively match, and so the whole event stream can be serialized —
// which is what a transport (SSE, a Slack bridge, a durable stream) needs.

/** One tool call parked on a durable, unresolved external interaction. */
export const PendingInteraction = Schema.Struct({
  toolCallId: Schema.String,
  toolName: Schema.String,
  /** Application-facing interaction kind, such as `approval` or `question`. */
  kind: Schema.NonEmptyString,
  /** The tool's decoded call parameters, for presentation to the user. */
  request: Schema.Unknown,
});
export interface PendingInteraction extends Schema.Struct.Type<
  typeof PendingInteraction.fields
> {}

export const Lifecycle = Schema.TaggedUnion({
  TurnStarted: {
    step: Schema.Natural,
  },
  TurnFinished: {
    step: Schema.Natural,
    usage: Stop.Usage,
  },
  Completed: {
    outcome: Schema.Literals(['success', 'cancelled']),
    text: Schema.String,
    steps: Schema.Natural,
    usage: Stop.Usage,
    /** Full final turn; absent only when no provider turn ran. */
    response: Schema.optionalKey(Prompt.Prompt),
  },
  /**
   * The run ended durably parked on one or more external interactions
   * instead of reaching {@link Completed}.
   *
   * Terminal like `Completed`, but deliberately a different case rather
   * than a third `outcome` literal on it: nothing here is a finished
   * answer, and a recording sink that wrote a `Completed` record for this
   * would claim a run had an answer it does not have. A resolved interaction
   * is not replayed as a second `Suspended` — see `ToolSuspended` and
   * `ToolWaitCompleted` in `@sunfall/vesper-log/record`, which are what a
   * resumed conversation actually recovers from.
   *
   * Only reachable from a recorded `Conversation`; an unrecorded run fails
   * instead, since there is nowhere to durably resolve the interaction from.
   */
  Suspended: {
    step: Schema.Natural,
    /**
     * Whatever the model said before its interaction tool call. Often the
     * stated intent behind the request — worth showing to the user, and
     * gone if not carried here: the suspended run's turn never completes.
     */
    text: Schema.String,
    usage: Stop.Usage,
    pendingInteractions: Schema.Array(PendingInteraction),
    /** Partial turn that reached the interaction boundary. */
    response: Schema.optionalKey(Prompt.Prompt),
  },
  /**
   * An out-of-band instruction reached the run and was acted on.
   *
   * Emitted when the authoritative boundary drain consumes it, before that
   * turn's `TurnFinished`. A valid cancel may have already stopped the provider
   * stream, but acknowledgement and ordering remain here. It is followed by
   * `TurnFinished` and `Completed` like any other stopping turn: cancellation
   * ends a run, it does not fail one, and partial work remains available.
   *
   * This is an event and not a private detail of the log sink on purpose.
   * Steering changes what the model is about to be asked, so a consumer
   * rendering a conversation has to be able to show it; and the sink's
   * ordering — text flushed, then the signal, then the turn boundary — falls
   * out of it being in the stream at the right place rather than being
   * arranged separately. Adding a case here is a public-API change: every
   * exhaustive match over `Lifecycle` has to grow a branch, which is the
   * point.
   */
  Signalled: {
    step: Schema.Natural,
    kind: Schema.Literals(['steer', 'cancel']),
    text: Schema.String,
    source: Schema.String,
    /**
     * The signal's offset in the signal stream.
     *
     * A plain string rather than `LogOffset.Offset`: this module describes
     * what a consumer observes and deliberately knows nothing about the log,
     * so the brand is applied where the record is written instead.
     */
    at: Schema.String,
  },
  SignalRejected: {
    step: Schema.Natural,
    kind: Schema.Literals(['steer', 'cancel']),
    text: Schema.String,
    source: Schema.String,
    at: Schema.String,
    reason: Schema.Literals([
      'signal_bytes',
      'signals_per_boundary',
      'steered_bytes',
    ]),
    used: Schema.Natural,
    maximum: Schema.Natural,
  },
  SignalBacklog: {
    step: Schema.Natural,
    maximum: Schema.Natural,
  },
  /**
   * History was summarized to fit the context window.
   *
   * Emitted between the turn that overflowed and its retry, so a consumer
   * sees the rewrite in the position it happened and the log sink writes the
   * `Compacted` record before anything the retried turn produces.
   *
   * This is the case that closed the reconstruction gap, and it had to be a
   * public event rather than a private arrangement between the loop and the
   * sink. Compaction is the only thing a run does that *replaces* history
   * rather than extending it, so a consumer rendering a conversation has to
   * be able to show it — and a resumed conversation that could not see it was
   * rebuilt from records compaction had already thrown away, came back longer
   * than the run it resumed, and compacted again on its first turn.
   *
   * Like `Signalled`, adding it is a public-API change: every exhaustive
   * match over `Lifecycle` grows a branch, which is the point.
   */
  Compacted: {
    /** The turn that overflowed and is about to be retried. */
    step: Schema.Natural,
    /** The model's summary, stored without presentation framing. */
    summary: Schema.String,
    /**
     * Messages replaced, and messages kept verbatim after the summary.
     *
     * `keptMessages` is how the sink locates the boundary in the log: the
     * loop has no offsets — compaction runs against `Chat`'s in-memory
     * history — so it reports the size of the tail it kept and the sink,
     * which does have offsets, resolves that to the record the tail starts
     * at.
     */
    summarizedMessages: Schema.Natural,
    keptMessages: Schema.Natural,
  },
});

/**
 * The events that mark where a run is, rather than what it said.
 *
 * `Part` is kept out of the schema union: `Response.StreamPart` is already a
 * codec that varies by toolkit, and re-declaring it here would duplicate a
 * definition that has to stay in lockstep with the one the provider emits.
 */
export type Lifecycle = typeof Lifecycle.Type;

/** A result supplied by recovery or interception rather than a tool handler. */
export type SubstitutedToolResult<
  Name extends string = string,
  Failure extends boolean = boolean,
> = Response.ToolResultPart<Name, unknown, unknown> & {
  readonly resultSource: 'substituted';
  readonly isFailure: Failure;
};

export type StreamPart<Tools extends Record<string, Tool.Any>> =
  | Response.StreamPart<Tools>
  | SubstitutedToolResult<keyof Tools & string>;

export type Event<Tools extends Record<string, Tool.Any>> =
  | Lifecycle
  | {
      readonly _tag: 'Part';
      readonly step: number;
      readonly part: Response.StreamPart<Tools>;
      /**
       * The provider-facing representation of `part`.
       *
       * `part` is decoded so live consumers can use the tool's typed result
       * and parameter values. The encoded sibling is what a recording sink
       * must persist: a schema transformation (for example `DateFromString`)
       * can make the two representations differ.
       */
      readonly encodedPart: Response.StreamPartEncoded;
      /** Durable interaction semantics for a tool approval request part. */
      readonly interaction?:
        | {
            readonly name: string;
            readonly mode: 'dispatch' | 'answer';
          }
        | undefined;
    };

/** Public events, including results that could not be schema-decoded. */
export type ObservedEvent<Tools extends Record<string, Tool.Any>> =
  | Lifecycle
  | {
      readonly _tag: 'Part';
      readonly step: number;
      readonly part: StreamPart<Tools>;
      readonly encodedPart: Response.StreamPartEncoded;
      readonly interaction?:
        | {
            readonly name: string;
            readonly mode: 'dispatch' | 'answer';
          }
        | undefined;
    };

export const isPart = <Tools extends Record<string, Tool.Any>>(
  event: Event<Tools>,
): event is Extract<Event<Tools>, { readonly _tag: 'Part' }> =>
  event._tag === 'Part';

export * as AgentEvents from './event.js';
