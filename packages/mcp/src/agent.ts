import { LogStore } from '@sunfall/vesper-log/log-store';
import { Crypto, Effect, Layer, Schema } from 'effect';
import { McpServer, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '@sunfall/vesper-agent/agent';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { classify } from './internal/failure.js';

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
export type FailureClassification = typeof FailureClassification.Type;

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

const failure = (error: unknown): RunError => new RunError(classify(error));

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
