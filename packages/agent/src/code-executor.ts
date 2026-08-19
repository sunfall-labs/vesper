import { Context, Effect, Layer, Stream } from 'effect';

/** A value that can cross the isolated code-executor boundary. */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | ReadonlyArray<JsonValue>
  | { readonly [key: string]: JsonValue };

/** One hidden tool made available inside the code runtime. */
export interface ToolDescriptor {
  readonly name: string;
  readonly description: string;
  readonly parameters: JsonValue;
  readonly result: JsonValue;
}

/** Hard limits enforced by an executor implementation. */
export interface Limits {
  readonly maxSourceBytes: number;
  readonly maxHeapBytes: number;
  readonly wallClockMillis: number;
  readonly maxOutputBytes: number;
  readonly maxNestedCalls: number;
}

export const defaultLimits: Limits = Object.freeze({
  maxSourceBytes: 64 * 1024,
  maxHeapBytes: 64 * 1024 * 1024,
  wallClockMillis: 30_000,
  maxOutputBytes: 256 * 1024,
  maxNestedCalls: 128,
});

/** Everything an executor needs; it receives no application services. */
export interface Request {
  readonly source: string;
  readonly tools: ReadonlyArray<ToolDescriptor>;
  readonly state: Readonly<Record<string, JsonValue>>;
  readonly limits: Limits;
}

export interface ToolCall {
  readonly _tag: 'ToolCall';
  readonly id: string;
  readonly name: string;
  readonly input: JsonValue;
}

export interface Output {
  readonly _tag: 'Output';
  readonly value: string;
}

export interface Completion {
  readonly _tag: 'Completion';
  readonly state: Readonly<Record<string, JsonValue>>;
}

export interface Failure {
  readonly _tag: 'Failure';
  readonly message: string;
}

export type Event = ToolCall | Output | Completion | Failure;

export interface ToolResponse {
  readonly id: string;
  readonly outcome: 'success' | 'failure';
  readonly value: JsonValue;
}

export class ExecutorError extends Error {
  readonly _tag = 'CodeExecutorError';

  constructor(
    readonly reason: 'unavailable' | 'protocol',
    message: string,
  ) {
    super(message);
  }
}

/** One isolated execution and its duplex nested-call channel. */
export interface Execution {
  readonly events: Stream.Stream<Event, ExecutorError>;
  readonly respond: (
    response: ToolResponse,
  ) => Effect.Effect<void, ExecutorError>;
  readonly interrupt: Effect.Effect<void>;
}

export interface Interface {
  readonly start: (request: Request) => Effect.Effect<Execution, ExecutorError>;
}

export class Service extends Context.Service<Service, Interface>()(
  '@sunfall/vesper-agent/CodeExecutor',
) {}

export interface Fake {
  readonly layer: Layer.Layer<Service>;
  readonly requests: Effect.Effect<ReadonlyArray<Request>>;
  readonly responses: Effect.Effect<ReadonlyArray<ToolResponse>>;
}

/** Deterministic executor used to drive the agent integration without a VM. */
export const fake = (events: ReadonlyArray<Event>): Fake => {
  const requests: Request[] = [];
  const responses: ToolResponse[] = [];
  return {
    layer: Layer.succeed(Service, {
      start: (request) =>
        Effect.sync(() => {
          requests.push(request);
          return {
            events: Stream.fromIterable(events),
            respond: (response) =>
              Effect.sync(() => {
                responses.push(response);
              }),
            interrupt: Effect.void,
          } satisfies Execution;
        }),
    }),
    requests: Effect.sync(() => Array.from(requests)),
    responses: Effect.sync(() => Array.from(responses)),
  };
};

export * as CodeExecutor from './code-executor.js';
