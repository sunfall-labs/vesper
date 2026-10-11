import { LogOffset } from '@sunfall/vesper-log/offset';
import { FORMAT_VERSION } from '@sunfall/vesper-log/record';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Cause, Clock, Effect, Exit, Option, Schema, Stream } from 'effect';
import { Prompt, type Response, type Tool } from 'effect/ai';

import { AgentEvents } from './event.js';
import { ToolDispatch } from './dispatch.js';
import { AgentHistory } from './internal/history.js';
import { DurabilityError } from './conversation-error.js';
import type { Session } from './log.js';
import { RunPolicy } from './run-policy.js';
import type { Stop } from './stop.js';

// The sink turns the loop's events into durable records while emitting the
// same events unchanged. Appends happen through one Session producer and
// complete before the corresponding event reaches the consumer. Text is
// coalesced at semantic boundaries so rebuilt prompts preserve what the model
// said before it called a tool, received a result, consumed a signal, or ended.
//
// Coalescing alone would lose a whole answer to a crash mid-stream, since the
// `Text` record only exists once the turn reaches a boundary. So a long-running
// stream also commits `PartialOutput` progress: at most one append per
// `partialOutputIntervalMillis`, carrying only the chunk since the previous
// one, written from the same tap as every other record so it shares their
// ordering, fencing, and failure surface. The check runs when a delta arrives
// rather than on a timer fiber: a second writer would race the settlement
// append for the producer sequence, and the text a stalled stream has not yet
// produced is not text there is anything to lose. Progress never changes what
// a completed turn records — `Text` still carries the whole answer exactly
// once and supersedes the chunks before it.

/** Record a run's events into the session's conversation. */
export const record = <Tools extends Record<string, Tool.Any>, E, R>(
  session: Session,
  events: Stream.Stream<AgentEvents.Event<Tools>, E, R>,
): Stream.Stream<AgentEvents.Event<Tools>, E | DurabilityError, R> =>
  Stream.unwrap(
    Effect.sync(() => {
      const pending: Pending = {
        step: 0,
        text: '',
        committedText: 0,
        reasoning: '',
        dirtySince: undefined,
        steps: 0,
        usage: { input: 0, output: 0 },
        completed: false,
        cancelled: false,
        toolCalls: new Map(),
      };

      return Stream.tap(events, (event) => {
        if (event._tag === 'Compacted') {
          return compaction(session, pending, event);
        }
        if (
          event._tag === 'Part' &&
          event.part.type === 'tool-result' &&
          'resultSource' in event.part &&
          event.part.resultSource === 'recovered'
        ) {
          // Recovery appended this outcome before rebuilding the prompt. The
          // event exists for live consumers only; persisting it here would
          // turn one settled call into two durable outcomes.
          return session.append(flush(pending));
        }
        return Effect.flatMap(digestOf(session, event), (digest) => {
          const records = recordsFor(pending, event, digest);
          return isStreamedDelta(event)
            ? Effect.flatMap(progress(session, pending), (partial) =>
                session.append([...records, ...partial]),
              )
            : session.append(records);
        });
      }).pipe(Stream.onExit((exit) => settle(session, pending, exit)));
    }),
  );

/**
 * A tool call's `StepOpened.inputDigest`, taken here — before `append`
 * applies the recording policy — so the identity a resumed run compares
 * against survives a policy that redacts the parameters.
 */
const digestOf = <Tools extends Record<string, Tool.Any>>(
  session: Session,
  event: AgentEvents.Event<Tools>,
): Effect.Effect<string | undefined> =>
  event._tag === 'Part' && event.encodedPart.type === 'tool-call'
    ? session.inputDigest(event.encodedPart.name, event.encodedPart.params)
    : Effect.undefined;

const isStreamedDelta = <Tools extends Record<string, Tool.Any>>(
  event: AgentEvents.Event<Tools>,
): boolean =>
  event._tag === 'Part' &&
  (event.encodedPart.type === 'text-delta' ||
    event.encodedPart.type === 'reasoning-delta');

/**
 * The `PartialOutput` records due now, if the oldest uncommitted delta has
 * waited out the session's interval.
 *
 * Text is committed as the suffix of `pending.text` past `committedText`, so
 * the in-memory coalescing the final `Text` record relies on is untouched.
 * Reasoning is never recorded as history, so its buffer holds only what is not
 * yet committed.
 */
const progress = (
  session: Session,
  pending: Pending,
): Effect.Effect<ReadonlyArray<ConversationRecord.Record>> =>
  Effect.gen(function* () {
    const interval = session.partialOutputIntervalMillis;
    const uncommitted =
      pending.text.length > pending.committedText || pending.reasoning !== '';
    if (interval === undefined || !uncommitted) {
      return [];
    }
    const now = yield* Clock.currentTimeMillis;
    pending.dirtySince ??= now;
    if (now - pending.dirtySince < interval) {
      return [];
    }
    const records: Array<ConversationRecord.Record> = [];
    if (pending.reasoning !== '') {
      records.push({
        _tag: 'PartialOutput',
        step: pending.step,
        kind: 'reasoning',
        text: pending.reasoning,
      });
    }
    if (pending.text.length > pending.committedText) {
      records.push({
        _tag: 'PartialOutput',
        step: pending.step,
        kind: 'text',
        text: pending.text.slice(pending.committedText),
      });
    }
    pending.reasoning = '';
    pending.committedText = pending.text.length;
    pending.dirtySince = undefined;
    return records;
  });

/** Write down that history was replaced, and what it was replaced by. */
const compaction = (
  session: Session,
  pending: Pending,
  event: Extract<AgentEvents.Lifecycle, { readonly _tag: 'Compacted' }>,
): Effect.Effect<void, DurabilityError> =>
  Effect.gen(function* () {
    yield* session.append(flush(pending));

    const recorded = yield* session.recorded;
    const firstKept = yield* AgentHistory.compactionBoundary(recorded, {
      summarizedMessages: event.summarizedMessages,
      keptMessages: event.keptMessages,
    }).pipe(
      Effect.mapError(
        (error) =>
          new DurabilityError({
            source: 'log',
            operation: 'compact',
            reason: 'history_mismatch',
            detail: error.message,
            cause: error,
          }),
      ),
    );

    yield* session.append([
      {
        _tag: 'Compacted',
        formatVersion: FORMAT_VERSION,
        agent: session.compatibility.agent,
        agentRevision: session.compatibility.revision,
        step: event.step,
        summary: event.summary,
        firstKept,
        summarizedMessages: event.summarizedMessages,
        keptMessages: event.keptMessages,
      },
    ]);
  });

/** Maximum time run teardown waits for the settlement append. */
export const SETTLEMENT_TIMEOUT_MILLIS = 5_000;

/** Write down how the run ended, including the ways that end no stream. */
const settle = (
  session: Session,
  pending: Pending,
  exit: Exit.Exit<unknown, unknown>,
): Effect.Effect<void> =>
  Effect.suspend(() => {
    const settlement: ConversationRecord.Record = {
      _tag: 'RunSettled',
      ...outcomeOf(pending, exit),
      steps: pending.steps,
      usage: pending.usage,
    };

    const write = Effect.flatMap(session.pendingToolState, (state) => {
      switch (state) {
        case 'none':
          return session.append([settlement], session.settlementTimeoutMillis);
        case 'suspended':
          // The absent RunSettled record is deliberate: Effect Workflow will
          // re-enter this handler after its external result arrives.
          return Effect.void;
        case 'indeterminate':
          return Effect.logError(
            'Agent run has indeterminate tool execution; leaving the run orphaned',
          );
        default: {
          const exhaustive: never = state;
          return exhaustive;
        }
      }
    }).pipe(
      Effect.annotateLogs({
        'vesper.component': 'agent',
        'vesper.event': 'run_settlement_indeterminate_tool',
      }),
      Effect.catchCause((cause) =>
        Effect.logError(
          'Agent run could not record how its run settled',
          cause,
        ).pipe(
          Effect.annotateLogs({
            'vesper.component': 'agent',
            'vesper.event': 'run_settlement_append_failure',
          }),
        ),
      ),
    );

    return Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const completed = yield* restore(write).pipe(
          Effect.timeoutOption(session.settlementTimeoutMillis),
        );
        if (Option.isNone(completed)) {
          yield* Effect.logError(
            `Agent settlement append timed out after ${String(session.settlementTimeoutMillis)}ms; leaving the run orphaned`,
          ).pipe(
            Effect.annotateLogs({
              'vesper.component': 'agent',
              'vesper.event': 'run_settlement_timeout',
            }),
          );
        }
      }),
    );
  });

const isRunPolicyExhausted = Schema.is(RunPolicy.RunPolicyExhausted);

const outcomeOf = (
  pending: Pending,
  exit: Exit.Exit<unknown, unknown>,
): Pick<
  ConversationRecord.RecordOf<'RunSettled'>,
  'outcome' | 'detail' | 'stoppedBy'
> => {
  if (Exit.isFailure(exit)) {
    if (Cause.hasInterrupts(exit.cause)) {
      return { outcome: 'interrupted', detail: 'the run was interrupted' };
    }
    const detail = Cause.pretty(exit.cause);
    const exhausted = Option.filter(
      Cause.findErrorOption(exit.cause),
      isRunPolicyExhausted,
    );
    return Option.isSome(exhausted)
      ? {
          outcome: 'failure',
          detail,
          stoppedBy: {
            limit: exhausted.value.limit,
            used: exhausted.value.used,
            ceiling: exhausted.value.maximum,
          },
        }
      : { outcome: 'failure', detail };
  }
  if (pending.cancelled) {
    return { outcome: 'cancelled', detail: 'a cancel signal ended the run' };
  }
  if (pending.completed) {
    return { outcome: 'success', detail: '' };
  }
  return {
    outcome: 'interrupted',
    detail: 'the event stream was abandoned before the run completed',
  };
};

interface Pending {
  step: number;
  text: string;
  /** Length of the `text` prefix already committed as `PartialOutput`. */
  committedText: number;
  /** Reasoning streamed since the last `PartialOutput` commit. */
  reasoning: string;
  /** When the oldest delta not yet committed as progress arrived. */
  dirtySince: number | undefined;
  steps: number;
  usage: Stop.Usage;
  completed: boolean;
  cancelled: boolean;
  /**
   * Tool calls seen this run, by provider call id.
   *
   * A `tool-approval-request` part names only the call it gates, not its
   * name or parameters — those are the co-occurring `tool-call` part's,
   * tracked here so the `StepSuspended` record below can carry them.
   */
  readonly toolCalls: Map<
    string,
    { readonly name: string; readonly params: unknown }
  >;
}

const flush = (pending: Pending): ReadonlyArray<ConversationRecord.Record> => {
  // Whatever was committed as progress is superseded by the record below (or,
  // for reasoning, by the turn having moved on), so the next window starts
  // clean. A `Text` always carries the whole turn text, committed prefix
  // included, which is what keeps completed history identical to before.
  pending.committedText = 0;
  pending.reasoning = '';
  pending.dirtySince = undefined;
  if (pending.text === '') {
    return [];
  }
  const textRecord: ConversationRecord.Record = {
    _tag: 'Text',
    step: pending.step,
    text: pending.text,
  };
  pending.text = '';
  return [textRecord];
};

const signalOffset = (offset: string): LogOffset.Offset => {
  try {
    return LogOffset.Offset.make(offset);
  } catch (cause) {
    throw new Error(`Signal event carried an invalid log offset: ${offset}`, {
      cause,
    });
  }
};

const recordsFor = <Tools extends Record<string, Tool.Any>>(
  pending: Pending,
  lifecycleEvent: AgentEvents.Event<Tools>,
  digest: string | undefined,
): ReadonlyArray<ConversationRecord.Record> => {
  if (lifecycleEvent._tag === 'Part') {
    return partRecords(
      pending,
      lifecycleEvent.step,
      lifecycleEvent.encodedPart,
      lifecycleEvent.interaction,
      digest,
    );
  }

  return AgentEvents.Lifecycle.match(lifecycleEvent, {
    TurnStarted: () => [],
    TurnFinished: (event) => {
      pending.steps = event.step;
      pending.usage = event.usage;
      return [
        ...flush(pending),
        { _tag: 'TurnFinished', step: event.step, usage: event.usage },
      ];
    },
    Signalled: (event) => {
      if (event.kind === 'cancel') {
        pending.cancelled = true;
      }
      return [
        ...flush(pending),
        {
          _tag: 'SignalReceived',
          kind: event.kind,
          text: event.text,
          source: event.source,
          step: event.step,
          at: signalOffset(event.at),
        },
      ];
    },
    SignalRejected: (event) => [
      ...flush(pending),
      {
        _tag: 'SignalReceived',
        kind: event.kind,
        text: event.text,
        source: event.source,
        step: event.step,
        at: signalOffset(event.at),
        disposition: 'rejected',
        reason: event.reason,
      },
    ],
    SignalBacklog: () => [],
    Completed: (event) => {
      pending.completed = true;
      pending.steps = event.steps;
      pending.usage = event.usage;
      return [
        ...flush(pending),
        {
          _tag: 'Completed',
          outcome: event.outcome,
          text: event.text,
          steps: event.steps,
          usage: event.usage,
          ...(event.response === undefined
            ? {}
            : {
                response: Schema.encodeSync(Prompt.Prompt)(event.response),
              }),
        },
      ];
    },
    Compacted: () => [],
    // The `StepSuspended` record was already written per-part above, at the
    // `tool-approval-request` that caused this. `Suspended` itself is a
    // live-only terminal marker — see its doc in `event.ts` for why it is
    // not a durable `Completed` record.
    Suspended: () => [],
  });
};

const partRecords = (
  pending: Pending,
  step: number,
  encoded: Response.StreamPartEncoded,
  interaction:
    | { readonly name: string; readonly mode: 'dispatch' | 'answer' }
    | undefined,
  digest: string | undefined,
): ReadonlyArray<ConversationRecord.Record> => {
  switch (encoded.type) {
    case 'text-delta':
      pending.step = step;
      pending.text += encoded.delta;
      return [];
    case 'tool-call':
      if (digest === undefined) {
        throw new Error(`Tool call ${encoded.id} reached the log undigested`);
      }
      pending.toolCalls.set(encoded.id, {
        name: encoded.name,
        params: encoded.params,
      });
      return [
        ...flush(pending),
        {
          _tag: 'StepOpened',
          step,
          id: LogVocabulary.ToolCallId.make(encoded.id),
          name: encoded.name,
          ...(encoded.providerExecuted === true
            ? { providerExecuted: true }
            : {}),
          inputDigest: digest,
          params: encoded.params,
        },
      ];
    case 'tool-result':
      // Preliminary results are progress events, not conversation history and
      // not evidence that execution settled. The final result will follow.
      if (encoded.preliminary === true) {
        return [];
      }
      return [
        ...flush(pending),
        {
          _tag: 'StepSettled',
          step,
          id: LogVocabulary.ToolCallId.make(encoded.id),
          name: encoded.name,
          ...(encoded.providerExecuted === true
            ? { providerExecuted: true }
            : {}),
          outcome: encoded.isFailure ? 'failure' : 'success',
          result: encoded.result,
        },
      ];
    case 'tool-approval-request': {
      const call = pending.toolCalls.get(encoded.toolCallId);
      return [
        ...flush(pending),
        {
          _tag: 'StepSuspended',
          id: LogVocabulary.ToolCallId.make(encoded.toolCallId),
          name: call?.name ?? '',
          wait: ToolDispatch.INTERACTION_WAIT,
          token: encoded.approvalId,
          request: call?.params,
          ...(interaction === undefined ? {} : { interaction }),
        },
      ];
    }
    case 'reasoning-delta':
      pending.step = step;
      pending.reasoning += encoded.delta;
      return [];
    case 'text-start':
    case 'text-end':
    case 'reasoning-start':
    case 'reasoning-end':
    case 'tool-params-start':
    case 'tool-params-delta':
    case 'tool-params-end':
    case 'file':
    case 'source':
    case 'response-metadata':
    case 'finish':
    case 'error':
      return [];
    default: {
      const exhaustive: never = encoded;
      return exhaustive;
    }
  }
};
