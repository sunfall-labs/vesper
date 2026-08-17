import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Context, Effect } from 'effect';

import type { RecordingPolicy } from './recording-policy.js';

/** Compiled policy carried by a session; it no longer has a requirement channel. */
export interface Runtime {
  readonly filter: <Tag extends ConversationRecord.Record['_tag']>(
    record: ConversationRecord.RecordOf<Tag>,
  ) => Effect.Effect<ConversationRecord.RecordOf<Tag>>;
}

/** Explicit default: records are persisted raw. */
export const raw: Runtime = { filter: Effect.succeed };

export const compile = <R>(
  policy: RecordingPolicy.Policy<R>,
  context: Context.Context<R>,
): Runtime => ({
  filter: (record) =>
    filter(policy, record).pipe(Effect.provide(context)) as Effect.Effect<
      typeof record
    >,
});

export const filter = <R>(
  policy: RecordingPolicy.Policy<R>,
  record: ConversationRecord.Record,
): Effect.Effect<ConversationRecord.Record, never, R> => {
  switch (record._tag) {
    case 'RunStarted':
      return policy.prompt === undefined
        ? Effect.succeed(record)
        : Effect.map(policy.prompt(record.prompt), (prompt) => ({
            ...record,
            prompt,
          }));
    case 'ToolCall':
      return policy.toolParameters === undefined
        ? Effect.succeed(record)
        : Effect.map(
            policy.toolParameters(record.params, record),
            (params) => ({
              ...record,
              params,
            }),
          );
    case 'ToolStarted':
      return Effect.succeed(record);
    case 'ToolOutcome':
      return policy.toolResult === undefined
        ? Effect.succeed(record)
        : Effect.map(policy.toolResult(record.result, record), (result) => ({
            ...record,
            result,
          }));
    case 'Signal':
    case 'SignalReceived':
      return policy.signal === undefined
        ? Effect.succeed(record)
        : Effect.map(policy.signal(record), (signal) => ({
            ...record,
            ...signal,
          }));
    case 'RunSettled':
      return policy.cause === undefined
        ? Effect.succeed(record)
        : Effect.map(policy.cause(record.detail), (detail) => ({
            ...record,
            detail,
          }));
    default:
      return Effect.succeed(record);
  }
};

export * as RecordingPolicyRuntime from './recording-policy-runtime.js';
