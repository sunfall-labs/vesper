import type { ConversationRecord } from '@sunfall/vesper-log/record';
import {
  Context,
  Effect,
  Option,
  Schema,
  Semaphore,
  Stream,
  SynchronizedRef,
} from 'effect';
import { AiError, Tool, Toolkit } from 'effect/unstable/ai';

import { CodeExecutor } from './code-executor.js';
import * as AgentLog from './log.js';
import { ResumeProjection } from './resume-projection.js';

export const TOOL_NAME = 'exec' as const;

export interface StateLimits {
  readonly maxValueBytes: number;
  readonly maxTotalBytes: number;
}

export const defaultStateLimits: StateLimits = Object.freeze({
  maxValueBytes: 64 * 1024,
  maxTotalBytes: 256 * 1024,
});

export class StateError extends Error {
  readonly _tag = 'CodeStateError';
}

export interface StateHandle {
  readonly get: Effect.Effect<Readonly<Record<string, CodeExecutor.JsonValue>>>;
  readonly commit: (
    state: Readonly<Record<string, CodeExecutor.JsonValue>>,
  ) => Effect.Effect<void, StateError | AgentLog.DurabilityError>;
}

const capturedContext: Effect.Effect<Context.Context<unknown>> = Effect.map(
  Effect.context<never>(),
  (context) => context as Context.Context<unknown>,
);

const execTool = (description: string) =>
  Tool.make(TOOL_NAME, {
    description,
    parameters: Schema.Struct({ source: Schema.String }),
    success: Schema.String,
    failure: Schema.String,
    failureMode: 'return',
  });

export type ExecTools = {
  readonly exec: ReturnType<typeof execTool>;
};

/** Tools exposed at the provider seam for the selected execution mode. */
export type ModelTools<
  Hidden extends Record<string, Tool.Any>,
  Enabled extends boolean,
> = Enabled extends true ? ExecTools : Hidden;

/** Executor service required only by agents that enable code mode. */
export type Requires<Enabled extends boolean> = Enabled extends true
  ? CodeExecutor.Service
  : never;

/**
 * Select the provider toolkit in lockstep with {@link ModelTools}.
 *
 * This is the single conditional boundary: the runtime boolean and the
 * conditional result type are the same `Enabled` value.
 */
export function selectToolkit<
  Hidden extends Record<string, Tool.Any>,
  Enabled extends boolean,
  DirectError,
  DirectRequires,
  CodeError,
  CodeRequires,
>(
  enabled: Enabled | undefined,
  direct: () => Effect.Effect<
    Toolkit.WithHandler<Hidden>,
    DirectError,
    DirectRequires
  >,
  code: () => Effect.Effect<
    Toolkit.WithHandler<ExecTools>,
    CodeError,
    CodeRequires
  >,
): Effect.Effect<
  Toolkit.WithHandler<ModelTools<Hidden, Enabled>>,
  DirectError | CodeError,
  DirectRequires | CodeRequires
>;
export function selectToolkit<
  Hidden extends Record<string, Tool.Any>,
  DirectError,
  DirectRequires,
  CodeError,
  CodeRequires,
>(
  enabled: boolean | undefined,
  direct: () => Effect.Effect<
    Toolkit.WithHandler<Hidden>,
    DirectError,
    DirectRequires
  >,
  code: () => Effect.Effect<
    Toolkit.WithHandler<ExecTools>,
    CodeError,
    CodeRequires
  >,
): Effect.Effect<
  Toolkit.WithHandler<Hidden> | Toolkit.WithHandler<ExecTools>,
  DirectError | CodeError,
  DirectRequires | CodeRequires
> {
  return enabled === true ? code() : direct();
}

const jsonValue = (value: unknown, path = '$'): CodeExecutor.JsonValue => {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean'
  ) {
    return value;
  }
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (Array.isArray(value)) {
    return value.map((item, index) => jsonValue(item, `${path}[${index}]`));
  }
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        jsonValue(item, `${path}.${key}`),
      ]),
    );
  }
  throw new TypeError(`${path} is not JSON-safe`);
};

const isJsonRecord = (
  value: CodeExecutor.JsonValue,
): value is Readonly<Record<string, CodeExecutor.JsonValue>> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);

const stateValue = (
  value: unknown,
  limits: StateLimits,
): Readonly<Record<string, CodeExecutor.JsonValue>> => {
  const prepared = jsonValue(value);
  if (!isJsonRecord(prepared)) {
    throw new StateError('Code scratch state must be a JSON object');
  }
  const encoder = new TextEncoder();
  for (const [key, item] of Object.entries(prepared)) {
    const bytes = encoder.encode(JSON.stringify(item)).byteLength;
    if (bytes > limits.maxValueBytes) {
      throw new StateError(
        `Code scratch value "${key}" exceeds ${limits.maxValueBytes} bytes`,
      );
    }
  }
  const total = encoder.encode(JSON.stringify(prepared)).byteLength;
  if (total > limits.maxTotalBytes) {
    throw new StateError(
      `Code scratch state exceeds ${limits.maxTotalBytes} bytes`,
    );
  }
  return prepared;
};

const checkedState = (value: unknown, limits: StateLimits) =>
  Effect.try({
    try: () => stateValue(value, limits),
    catch: (error) =>
      error instanceof StateError
        ? error
        : new StateError(`Code scratch state is invalid: ${String(error)}`),
  });

/** Open one isolated scratch-state handle for a run or conversation. */
export const openState = (
  session: AgentLog.Session | undefined,
  limits: StateLimits = defaultStateLimits,
): Effect.Effect<StateHandle, StateError> =>
  Effect.gen(function* () {
    const checkpoint =
      session === undefined
        ? undefined
        : ResumeProjection.codeStateFrom(session.stateHistory);
    const initial =
      checkpoint === undefined
        ? {}
        : yield* checkedState(checkpoint.state, limits);
    const current = yield* SynchronizedRef.make(initial);
    const persist = (
      next: Readonly<Record<string, CodeExecutor.JsonValue>>,
    ): Effect.Effect<
      Readonly<Record<string, CodeExecutor.JsonValue>>,
      StateError | AgentLog.DurabilityError
    > =>
      Effect.gen(function* () {
        const prepared = yield* checkedState(next, limits);
        if (session !== undefined) {
          const record: ConversationRecord.Record = {
            _tag: 'CodeStateCheckpoint',
            state: prepared,
          };
          yield* session.append([record]);
        }
        return prepared;
      });
    return {
      get: SynchronizedRef.get(current),
      commit: (next) =>
        SynchronizedRef.updateEffect(current, () => persist(next)),
    };
  });

export const emptyState: StateHandle = {
  get: Effect.succeed({}),
  commit: () => Effect.void,
};

const aiError = (method: string, description: string): AiError.AiError =>
  new AiError.AiError({
    module: 'CodeMode',
    method,
    reason: new AiError.InvalidRequestError({ description }),
  });

const descriptor = (tool: Tool.Any): CodeExecutor.ToolDescriptor => ({
  name: tool.name,
  description: Tool.getDescription(tool) ?? '',
  parameters: jsonValue(Tool.getJsonSchema(tool)),
  result: jsonValue(Tool.getJsonSchemaFromSchema(tool.successSchema)),
});

const invoke = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
  event: CodeExecutor.ToolCall,
): Effect.Effect<
  CodeExecutor.ToolResponse,
  AiError.AiError,
  Tool.HandlerServices<Tools[keyof Tools]>
> =>
  Effect.gen(function* () {
    const tool = Object.hasOwn(toolkit.tools, event.name)
      ? toolkit.tools[event.name]
      : undefined;
    if (tool === undefined) {
      return yield* Effect.fail(
        aiError('nestedToolCall', `Unknown code-mode tool "${event.name}"`),
      );
    }
    if (tool.needsApproval !== undefined && tool.needsApproval !== false) {
      return yield* Effect.fail(
        aiError(
          'nestedToolCall',
          `Tool "${event.name}" requires provider-mediated approval and is unavailable in code mode`,
        ),
      );
    }
    const name = event.name as Extract<keyof Tools, string>;
    const input = event.input as Tool.Parameters<Tools[typeof name]>;
    const stream = yield* toolkit.handle(name, input, event.id);
    const result = yield* Stream.runLast(stream);
    if (Option.isNone(result)) {
      return yield* Effect.fail(
        aiError('nestedToolCall', `Tool "${event.name}" returned no result`),
      );
    }
    return {
      id: event.id,
      outcome: result.value.isFailure ? 'failure' : 'success',
      value: jsonValue(result.value.encodedResult),
    };
  });

const catalogDescription = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
): string => {
  const catalog = Object.values(toolkit.tools)
    .map((tool) => `${tool.name}: ${Tool.getDescription(tool) ?? ''}`)
    .join('\n');
  return [
    'Execute JavaScript that composes the hidden tools below.',
    'Only values passed to text(...) are returned.',
    catalog,
  ].join('\n');
};

/** Build the one model-visible tool around an already gated hidden toolkit. */
export const toolkit = <Tools extends Record<string, Tool.Any>>(
  hidden: Toolkit.WithHandler<Tools>,
  state: StateHandle = emptyState,
): Effect.Effect<Toolkit.WithHandler<ExecTools>, never, CodeExecutor.Service> =>
  Effect.gen(function* () {
    const executor = yield* CodeExecutor.Service;
    const services = yield* capturedContext;
    const exec = execTool(catalogDescription(hidden));
    const visible = Toolkit.make(exec);
    const executionLock = yield* Semaphore.make(1);
    return yield* visible.pipe(
      Effect.provide(
        visible.toLayer({
          exec: ({ source }) =>
            executionLock.withPermits(1)(
              Effect.gen(function* () {
                const execution = yield* executor
                  .start({
                    source,
                    tools: Object.values(hidden.tools).map(descriptor),
                    state: yield* state.get,
                    limits: CodeExecutor.defaultLimits,
                  })
                  .pipe(
                    Effect.mapError((error) =>
                      aiError('execute', error.message),
                    ),
                  );
                const output: string[] = [];
                let completed = false;
                let nextState:
                  | Readonly<Record<string, CodeExecutor.JsonValue>>
                  | undefined;
                yield* execution.events.pipe(
                  Stream.mapEffect(
                    (event) => {
                      switch (event._tag) {
                        case 'ToolCall':
                          return Effect.flatMap(
                            invoke(hidden, event).pipe(
                              Effect.provide(services),
                            ),
                            execution.respond,
                          );
                        case 'Output':
                          return Effect.sync(() => {
                            output.push(event.value);
                          });
                        case 'Completion':
                          return Effect.sync(() => {
                            completed = true;
                            nextState = event.state;
                          });
                        case 'Failure':
                          return Effect.fail(aiError('execute', event.message));
                      }
                    },
                    { concurrency: 'unbounded' },
                  ),
                  Stream.runDrain,
                  Effect.mapError((error) =>
                    AiError.isAiError(error)
                      ? error
                      : aiError('execute', error.message),
                  ),
                  Effect.ensuring(execution.interrupt),
                );
                if (!completed) {
                  return yield* Effect.fail(
                    aiError('execute', 'Executor ended without completion'),
                  );
                }
                yield* state
                  .commit(nextState ?? {})
                  .pipe(
                    Effect.mapError((error) => aiError('state', error.message)),
                  );
                return output.join('');
              }).pipe(Effect.mapError((error) => error.message)),
            ),
        }),
      ),
    );
  });

export * as CodeMode from './code-mode.js';
