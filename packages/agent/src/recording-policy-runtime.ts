import { ConversationRecord } from '@sunfall/vesper-log/record';
import { Context, Effect } from 'effect';

import type { RecordingPolicy } from './recording-policy.js';

/** Compiled policy carried by a session; it no longer has a requirement channel. */
export interface Runtime {
  readonly filter: (
    record: ConversationRecord.Record,
  ) => Effect.Effect<ConversationRecord.Record>;
}

/** Explicit default: records are persisted raw. */
export const raw: Runtime = { filter: Effect.succeed };

type CompiledFilter<R> = (
  record: ConversationRecord.Record,
) => Effect.Effect<ConversationRecord.Record, never, R>;

export const compile = <R>(
  policy: RecordingPolicy.Policy<R>,
  context: Context.Context<R>,
): Runtime => {
  const compiled = compileFilter(policy);
  return {
    filter: (record) => compiled(record).pipe(Effect.provide(context)),
  };
};

export const filter = <R>(
  policy: RecordingPolicy.Policy<R>,
  record: ConversationRecord.Record,
): Effect.Effect<ConversationRecord.Record, never, R> =>
  compileFilter(policy)(record);

const compileFilter = <R>(
  policy: RecordingPolicy.Policy<R>,
): CompiledFilter<R> =>
  ConversationRecord.Record.match<
    Effect.Effect<ConversationRecord.Record, never, R>
  >({
    RunStarted: (record) =>
      policy.prompt === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.prompt(record.prompt),
            (prompt): ConversationRecord.Record => ({
              ...record,
              prompt,
            }),
          ),
    ToolCall: (record) =>
      policy.toolParameters === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.toolParameters(record.params, record),
            (params): ConversationRecord.Record => ({
              ...record,
              params,
            }),
          ),
    ToolStarted: (record) => Effect.succeed<ConversationRecord.Record>(record),
    ToolSuspended: (record) =>
      policy.externalRequest === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.externalRequest(record.request, record),
            (request): ConversationRecord.Record => ({ ...record, request }),
          ),
    ToolResumed: (record) => Effect.succeed<ConversationRecord.Record>(record),
    ToolWaitCompleted: (record) =>
      policy.externalResult === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.externalResult(record.result, record),
            (result): ConversationRecord.Record => ({ ...record, result }),
          ),
    ToolWaitRestarted: (record) =>
      Effect.succeed<ConversationRecord.Record>(record),
    ToolOutcome: (record) =>
      policy.toolResult === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.toolResult(record.result, record),
            (result): ConversationRecord.Record => ({
              ...record,
              result,
            }),
          ),
    Signal: (record) =>
      policy.signal === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.signal(record),
            (signal): ConversationRecord.Record => ({
              ...record,
              ...signal,
            }),
          ),
    SignalReceived: (record) =>
      policy.signal === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.signal(record),
            (signal): ConversationRecord.Record => ({
              ...record,
              ...signal,
            }),
          ),
    RunSettled: (record) =>
      policy.cause === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.cause(record.detail),
            (detail): ConversationRecord.Record => ({
              ...record,
              detail,
            }),
          ),
    Text: (record) => Effect.succeed<ConversationRecord.Record>(record),
    TurnFinished: (record) => Effect.succeed<ConversationRecord.Record>(record),
    Compacted: (record) => Effect.succeed<ConversationRecord.Record>(record),
    BranchedFrom: (record) => Effect.succeed<ConversationRecord.Record>(record),
    Completed: (record) => Effect.succeed<ConversationRecord.Record>(record),
    StateCheckpoint: (record) =>
      Effect.succeed<ConversationRecord.Record>(record),
    CodeStateCheckpoint: (record) =>
      policy.codeState === undefined
        ? Effect.succeed<ConversationRecord.Record>(record)
        : Effect.map(
            policy.codeState(record.state, record),
            (state): ConversationRecord.Record => ({ ...record, state }),
          ),
    ChildSession: (record) => Effect.succeed<ConversationRecord.Record>(record),
  });

export * as RecordingPolicyRuntime from './recording-policy-runtime.js';
