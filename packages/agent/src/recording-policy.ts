import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Context, Effect } from 'effect';

type SignalRecord = ConversationRecord.RecordOf<'Signal'>;

/** Persistence-only filtering. Returned values must remain record-schema valid. */
export interface Policy<R = never> {
  readonly prompt?: (value: unknown) => Effect.Effect<unknown, never, R>;
  readonly toolParameters?: (
    value: unknown,
    record: ConversationRecord.RecordOf<'ToolCall'>,
  ) => Effect.Effect<unknown, never, R>;
  readonly toolResult?: (
    value: unknown,
    record: ConversationRecord.RecordOf<'ToolOutcome'>,
  ) => Effect.Effect<unknown, never, R>;
  readonly signal?: (
    signal: Pick<SignalRecord, 'kind' | 'text' | 'source'>,
  ) => Effect.Effect<Pick<SignalRecord, 'kind' | 'text' | 'source'>, never, R>;
  readonly cause?: (rendered: string) => Effect.Effect<string, never, R>;
}

type FunctionServices<F> = F extends (...args: infer _Args) => infer Result
  ? Result extends Effect.Effect<infer _A, infer _E, infer R>
    ? R
    : never
  : never;

export type Services<P> =
  | FunctionServices<P extends { readonly prompt: infer F } ? F : never>
  | FunctionServices<P extends { readonly toolParameters: infer F } ? F : never>
  | FunctionServices<P extends { readonly toolResult: infer F } ? F : never>
  | FunctionServices<P extends { readonly signal: infer F } ? F : never>
  | FunctionServices<P extends { readonly cause: infer F } ? F : never>;

/** Compiled policy carried by a session; it no longer has a requirement channel. */
export interface Runtime {
  readonly filter: <Tag extends ConversationRecord.Record['_tag']>(
    record: ConversationRecord.RecordOf<Tag>,
  ) => Effect.Effect<ConversationRecord.RecordOf<Tag>>;
}

/** Explicit default: records are persisted raw. */
export const raw: Runtime = { filter: Effect.succeed };

export const compile = <R>(
  policy: Policy<R>,
  context: Context.Context<R>,
): Runtime => ({
  filter: (record) =>
    filter(policy, record).pipe(Effect.provide(context)) as Effect.Effect<
      typeof record
    >,
});

const filter = <R>(
  policy: Policy<R>,
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

export * as RecordingPolicy from './recording-policy.js';
