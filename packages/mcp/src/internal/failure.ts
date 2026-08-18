import { LogStore } from '@sunfall/vesper-log/log-store';
import { Match, Predicate, Schema } from 'effect';
import { AiError } from 'effect/unstable/ai';

import {
  CompatibilityError,
  DurabilityError,
  SuspendedConversationError,
} from '@sunfall/vesper-agent/conversation';
import { RunPolicy } from '@sunfall/vesper-agent/run-policy';
import type { FailureClassification } from '../agent.js';

export interface Fields {
  readonly classification: FailureClassification;
  readonly code: string;
  readonly message: string;
  readonly retryable: boolean;
  readonly details: Readonly<Record<string, string>>;
}

const fields = (value: Fields): Fields => value;

const messageOf = (error: unknown): string =>
  error instanceof Error
    ? error.message
    : Predicate.hasProperty(error, 'message') &&
        typeof error.message === 'string'
      ? error.message
      : String(error);

const isDurabilityError = Schema.is(DurabilityError);
const isLogStoreError = Schema.is(LogStore.LogStoreError);
const isCompatibilityError = Schema.is(CompatibilityError);
const isSuspendedConversationError = Schema.is(SuspendedConversationError);
const isRunPolicyExhausted = Schema.is(RunPolicy.RunPolicyExhausted);

const applicationCode = (error: unknown): string =>
  Predicate.hasProperty(error, '_tag') && typeof error._tag === 'string'
    ? error._tag
    : 'unknown';

/** Reduce every internal failure to stable, serialization-safe MCP fields. */
export const classify = (error: unknown): Fields =>
  Match.value(error).pipe(
    Match.when(isDurabilityError, (error) =>
      fields({
        classification: 'durability',
        code: `${error.source}.${error.reason}`,
        message: error.detail,
        retryable: error.source === 'timeout' || error.reason === 'storage',
        details: {
          source: error.source,
          operation: error.operation,
          reason: error.reason,
        },
      }),
    ),
    Match.when(isLogStoreError, (error) =>
      fields({
        classification: 'durability',
        code: `log.${error.reason}`,
        message: error.detail,
        retryable: error.reason === 'storage',
        details: {
          source: 'log',
          operation: error.operation,
          reason: error.reason,
        },
      }),
    ),
    Match.when(isCompatibilityError, (error) =>
      fields({
        classification: 'compatibility',
        code: 'conversation.incompatible',
        message: error.message,
        retryable: false,
        details: {
          expectedAgent: error.expectedAgent,
          expectedRevision: error.expectedRevision,
        },
      }),
    ),
    Match.when(isSuspendedConversationError, (error) =>
      fields({
        classification: 'suspended',
        code: 'conversation.suspended',
        message: error.message,
        retryable: false,
        details: { wait: error.wait },
      }),
    ),
    Match.when(isRunPolicyExhausted, (error) =>
      fields({
        classification: 'run-policy',
        code: `run-policy.${error.limit}`,
        message: error.message,
        retryable: false,
        details: {
          limit: error.limit,
          used: String(error.used),
          maximum: String(error.maximum),
        },
      }),
    ),
    Match.when(AiError.isAiError, (error) =>
      fields({
        classification: error.module === 'AgentLog' ? 'durability' : 'provider',
        code: `ai.${error.reason._tag}`,
        message: error.message,
        retryable: error.isRetryable,
        details: {
          module: error.module,
          method: error.method,
          reason: error.reason._tag,
        },
      }),
    ),
    Match.orElse((error) =>
      fields({
        classification: 'application',
        code: applicationCode(error),
        message: messageOf(error),
        retryable: false,
        details: {},
      }),
    ),
  );
