import { LogOffset } from '@sunfall/vesper-log/offset';
import { ConversationRecord, FORMAT_VERSION } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Cause, Effect, Exit, Option, Stream } from 'effect';
import type { Response, Tool } from 'effect/unstable/ai';

import { AgentEvents } from './event.js';
import { ToolDispatch } from './dispatch.js';
import { AgentHistory } from './internal/history.js';
import { DurabilityError } from './conversation-error.js';
import type { Session } from './log.js';
import type { Stop } from './stop.js';

// The sink turns the loop's events into durable records while emitting the
// same events unchanged. Appends happen through one Session producer and
// complete before the corresponding event reaches the consumer. Text is
// coalesced at semantic boundaries so rebuilt prompts preserve what the model
// said before it called a tool, received a result, consumed a signal, or ended.

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
        steps: 0,
        usage: { input: 0, output: 0 },
        completed: false,
        cancelled: false,
        toolCalls: new Map(),
      };

      return Stream.tap(events, (event) =>
        event._tag === 'Compacted'
          ? compaction(session, pending, event)
          : session.append(recordsFor(pending, event)),
      ).pipe(Stream.onExit((exit) => settle(session, pending, exit)));
    }),
  );

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
            `Agent settlement append timed out after ${session.settlementTimeoutMillis}ms; leaving the run orphaned`,
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

const outcomeOf = (
  pending: Pending,
  exit: Exit.Exit<unknown, unknown>,
): { readonly outcome: SettledOutcome; readonly detail: string } => {
  if (Exit.isFailure(exit)) {
    return Cause.hasInterrupts(exit.cause)
      ? { outcome: 'interrupted', detail: 'the run was interrupted' }
      : { outcome: 'failure', detail: Cause.pretty(exit.cause) };
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

type SettledOutcome = ConversationRecord.RecordOf<'RunSettled'>['outcome'];

interface Pending {
  step: number;
  text: string;
  steps: number;
  usage: Stop.Usage;
  completed: boolean;
  cancelled: boolean;
  /**
   * Tool calls seen this run, by provider call id.
   *
   * A `tool-approval-request` part names only the call it gates, not its
   * name or parameters — those are the co-occurring `tool-call` part's,
   * tracked here so the `ToolSuspended` record below can carry them.
   */
  readonly toolCalls: Map<
    string,
    { readonly name: string; readonly params: unknown }
  >;
}

const flush = (pending: Pending): ReadonlyArray<ConversationRecord.Record> => {
  if (pending.text === '') return [];
  const record: ConversationRecord.Record = {
    _tag: 'Text',
    step: pending.step,
    text: pending.text,
  };
  pending.text = '';
  return [record];
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
  event: AgentEvents.Event<Tools>,
): ReadonlyArray<ConversationRecord.Record> => {
  if (event._tag === 'Part') {
    return partRecords(pending, event.step, event.encodedPart);
  }

  return AgentEvents.Lifecycle.match(event, {
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
      if (event.kind === 'cancel') pending.cancelled = true;
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
        },
      ];
    },
    Compacted: () => [],
    // The `ToolSuspended` record was already written per-part above, at the
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
): ReadonlyArray<ConversationRecord.Record> => {
  switch (encoded.type) {
    case 'text-delta':
      pending.step = step;
      pending.text += encoded.delta;
      return [];
    case 'tool-call':
      pending.toolCalls.set(encoded.id, {
        name: encoded.name,
        params: encoded.params,
      });
      return [
        ...flush(pending),
        {
          _tag: 'ToolCall',
          step,
          id: LogVocabulary.ToolCallId.make(encoded.id),
          name: encoded.name,
          ...(encoded.providerExecuted === true
            ? { providerExecuted: true }
            : {}),
          params: encoded.params,
        },
      ];
    case 'tool-result':
      return [
        ...flush(pending),
        {
          _tag: 'ToolOutcome',
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
          _tag: 'ToolSuspended',
          id: LogVocabulary.ToolCallId.make(encoded.toolCallId),
          name: call?.name ?? '',
          wait: ToolDispatch.APPROVAL_WAIT,
          token: encoded.approvalId,
          request: call?.params,
        },
      ];
    }
    case 'text-start':
    case 'text-end':
    case 'reasoning-start':
    case 'reasoning-delta':
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
