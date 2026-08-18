import type { ConversationRecord } from '@sunfall/vesper-log/record';
import type { Effect } from 'effect';

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
  readonly externalRequest?: (
    value: unknown,
    record: ConversationRecord.RecordOf<'ToolSuspended'>,
  ) => Effect.Effect<unknown, never, R>;
  readonly externalResult?: (
    value: unknown,
    record: ConversationRecord.RecordOf<'ToolWaitCompleted'>,
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
  | FunctionServices<
      P extends { readonly externalRequest: infer F } ? F : never
    >
  | FunctionServices<P extends { readonly externalResult: infer F } ? F : never>
  | FunctionServices<P extends { readonly signal: infer F } ? F : never>
  | FunctionServices<P extends { readonly cause: infer F } ? F : never>;

export * as RecordingPolicy from './recording-policy.js';
