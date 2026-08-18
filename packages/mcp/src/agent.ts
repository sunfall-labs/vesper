import { LogStore } from '@sunfall/vesper-log/log-store';
import { Crypto, Effect, Layer, Match, Predicate, Schema } from 'effect';
import { AiError, McpServer, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '@sunfall/vesper-agent/agent';
import {
  CompatibilityError,
  Conversation,
  DurabilityError,
  SuspendedConversationError,
} from '@sunfall/vesper-agent/conversation';
import { RunPolicy } from '@sunfall/vesper-agent/run-policy';

/** Input accepted by the MCP tool exposed for one agent definition. */
export const RunInput = Schema.Struct({
  conversationId: Schema.String,
  input: Schema.String,
});

/** Stable categories MCP clients may safely branch on. */
export const FailureClassification = Schema.Literals([
  'durability',
  'compatibility',
  'suspended',
  'provider',
  'run-policy',
  'application',
]);

/** A Vesper run failed after the MCP request was accepted. */
export class RunError extends Schema.TaggedError<RunError>(
  '@sunfall/vesper-mcp/RunError',
)('RunError', {
  classification: FailureClassification,
  code: Schema.String,
  message: Schema.String,
  retryable: Schema.Boolean,
  details: Schema.Record(Schema.String, Schema.String),
}) {}

/**
 * The stable MCP tool interface.
 *
 * One MCP server adapter represents one agent, so the tool name stays stable
 * while the agent's durable identity remains in its conversation records.
 */
export const RunAgent: Tool.Tool<
  'run_agent',
  {
    readonly parameters: typeof RunInput;
    readonly success: typeof Agent.Result;
    readonly failure: typeof RunError;
    readonly failureMode: 'error';
  },
  never
> = Tool.make('run_agent', {
  description:
    'Run or continue the configured agent in one durable conversation.',
  parameters: RunInput,
  success: Agent.Result,
  failure: RunError,
});

type Tools = { readonly run_agent: typeof RunAgent };

/** Effect-native MCP registration for one Vesper agent definition. */
export interface Composition<A extends Agent.Any> {
  readonly toolkit: Toolkit.Toolkit<Tools>;
  readonly handlers: Layer.Layer<
    Tool.HandlersFor<Tools>,
    never,
    Agent.Requires<A> | LogStore.Service | Crypto.Crypto
  >;
  /**
   * Register the tool with Effect's `McpServer`.
   *
   * The application still chooses Effect's stdio or HTTP transport layer.
   */
  readonly layer: Layer.Layer<
    never,
    never,
    McpServer.McpServer | Agent.Requires<A> | LogStore.Service | Crypto.Crypto
  >;
}

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

/** Reduce every internal failure to a stable, serialization-safe MCP shape. */
export const failure = (error: unknown): RunError =>
  Match.value(error).pipe(
    Match.when(
      isDurabilityError,
      (error) =>
        new RunError({
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
    Match.when(
      isLogStoreError,
      (error) =>
        new RunError({
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
    Match.when(
      isCompatibilityError,
      (error) =>
        new RunError({
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
    Match.when(
      isSuspendedConversationError,
      (error) =>
        new RunError({
          classification: 'suspended',
          code: 'conversation.suspended',
          message: error.message,
          retryable: false,
          details: { wait: error.wait },
        }),
    ),
    Match.when(
      isRunPolicyExhausted,
      (error) =>
        new RunError({
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
    Match.when(
      AiError.isAiError,
      (error) =>
        new RunError({
          classification:
            error.module === 'AgentLog' ? 'durability' : 'provider',
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
    Match.orElse(
      (error) =>
        new RunError({
          classification: 'application',
          code: applicationCode(error),
          message: messageOf(error),
          retryable: false,
          details: {},
        }),
    ),
  );

/** Expose one durable Vesper agent through Effect's native MCP server. */
export const make = <A extends Agent.Any>(agent: A): Composition<A> => {
  const toolkit = Toolkit.make(RunAgent);
  const handlers = toolkit.toLayer(
    Effect.map(
      Effect.context<Agent.Requires<A> | LogStore.Service | Crypto.Crypto>(),
      (context) => ({
        run_agent: ({ conversationId, input }) =>
          Conversation.make(agent, conversationId)
            .run(input)
            .pipe(Effect.mapError(failure), Effect.provideContext(context)),
      }),
    ),
  );
  const layer = Layer.effectDiscard(McpServer.registerToolkit(toolkit)).pipe(
    Layer.provide(handlers),
  );

  return { toolkit, handlers, layer };
};

export * as AgentMcp from './agent.js';
