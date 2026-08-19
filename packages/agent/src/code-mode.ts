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
import { renderCodeSdk } from './code-sdk.js';
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
    success: Schema.Struct({
      output: Schema.String,
      result: Schema.optionalKey(Schema.Json),
    }),
    failure: Schema.Struct({
      code: Schema.Literal('execution_failed'),
      message: Schema.String,
    }),
    failureMode: 'return',
  });

export type ExecTools = {
  readonly exec: ReturnType<typeof execTool>;
};

/**
 * How an agent's own toolkit is exposed to the model.
 *
 * `true` brokers every tool behind the one `exec` tool; `false` (or omitting
 * the option) advertises them all directly. `{ except: [...] }` brokers
 * everything *but* the named tools, which stay directly advertised — with
 * ordinary session gating, interception, metering, and provider-mediated
 * approval, exactly as if code mode were off for them. Naming a tool the
 * toolkit does not define is a compile error, the same way `Stop.toolCalled`
 * checks its name. `Agent.make` rejects any known approval-gated tool omitted
 * from `except`; dynamically resolved approval tools fail closed at dispatch.
 *
 * Only the agent's own toolkit names can be excepted. Generated tools —
 * delegation, skills, `read_attachment` — are brokered like everything else.
 */
export type Option<Tools extends Record<string, Tool.Any>> =
  | boolean
  | { readonly except: ReadonlyArray<keyof Tools & string> };

/** The excepted tool names carried by a mode, `never` outside except-mode. */
export type Except<Mode> = Mode extends {
  readonly except: ReadonlyArray<infer Names extends string>;
}
  ? Names
  : never;

/**
 * Tools exposed at the provider seam for the selected execution mode.
 *
 * Distributive on purpose: a mode only known as `boolean` yields
 * `ExecTools | Hidden`, the honest "could be either" a non-literal flag
 * deserves, rather than silently claiming one side.
 */
export type ModelTools<
  Hidden extends Record<string, Tool.Any>,
  Mode,
> = Mode extends true
  ? ExecTools
  : Mode extends { readonly except: ReadonlyArray<string> }
    ? ExecTools & Pick<Hidden, Except<Mode> & keyof Hidden>
    : Hidden;

/** Executor service required only by agents that enable code mode. */
export type Requires<Mode> = Mode extends false | undefined
  ? never
  : CodeExecutor.Service;

/** Whether a mode value brokers anything at all. */
export const isEnabled = (
  mode: boolean | { readonly except: ReadonlyArray<string> } | undefined,
): mode is true | { readonly except: ReadonlyArray<string> } =>
  mode !== undefined && mode !== false;

const exceptNames = (
  mode: true | { readonly except: ReadonlyArray<string> },
): ReadonlyArray<string> => (mode === true ? [] : mode.except);

/**
 * Select the provider toolkit in lockstep with {@link ModelTools}.
 *
 * This is the single conditional boundary in code mode's typing: TypeScript
 * cannot relate a runtime branch on `mode` to the conditional
 * `ModelTools<Hidden, Mode>`, so each branch is named for what the
 * conditional resolves to on that branch — once, here, and nowhere
 * downstream. The `code` builder receives the excepted names — empty for
 * `codeMode: true` — and returns the whole visible toolkit, `exec` plus
 * whatever stayed advertised.
 */
export function selectToolkit<
  Hidden extends Record<string, Tool.Any>,
  Mode extends boolean | { readonly except: ReadonlyArray<string> },
  DirectError,
  DirectRequires,
  CodeError,
  CodeRequires,
>(
  mode: Mode | undefined,
  direct: () => Effect.Effect<
    Toolkit.WithHandler<Hidden>,
    DirectError,
    DirectRequires
  >,
  code: (
    except: ReadonlyArray<Except<Mode> & keyof Hidden & string>,
  ) => Effect.Effect<
    Toolkit.WithHandler<ExecTools & Pick<Hidden, Except<Mode> & keyof Hidden>>,
    CodeError,
    CodeRequires
  >,
): Effect.Effect<
  Toolkit.WithHandler<ModelTools<Hidden, Mode>>,
  DirectError | CodeError,
  DirectRequires | CodeRequires
>;
export function selectToolkit<
  Hidden extends Record<string, Tool.Any>,
  Mode extends boolean | { readonly except: ReadonlyArray<string> },
  DirectError,
  DirectRequires,
  CodeError,
  CodeRequires,
>(
  mode: Mode | undefined,
  direct: () => Effect.Effect<
    Toolkit.WithHandler<Hidden>,
    DirectError,
    DirectRequires
  >,
  code: (
    except: ReadonlyArray<Except<Mode> & keyof Hidden & string>,
  ) => Effect.Effect<
    Toolkit.WithHandler<ExecTools & Pick<Hidden, Except<Mode> & keyof Hidden>>,
    CodeError,
    CodeRequires
  >,
): Effect.Effect<
  | Toolkit.WithHandler<Hidden>
  | Toolkit.WithHandler<ExecTools & Pick<Hidden, Except<Mode> & keyof Hidden>>,
  DirectError | CodeError,
  DirectRequires | CodeRequires
> {
  if (!isEnabled(mode)) return direct();
  // Runtime names are plain strings; that they were drawn from the toolkit's
  // keys is what `Option<Tools>` proved at the definition site, so the one
  // assertion in code mode's typing names that fact here.
  const names = exceptNames(mode) as ReadonlyArray<
    Except<Mode> & keyof Hidden & string
  >;
  return code(names);
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

const toolFailureMessage = (
  name: string,
  value: CodeExecutor.JsonValue,
): string => {
  if (typeof value === 'string') return value;
  if (isJsonRecord(value) && typeof value.message === 'string') {
    return value.message;
  }
  return `Tool "${name}" failed`;
};

const failedToolResponse = (
  event: CodeExecutor.ToolCall,
  error: CodeExecutor.ToolFailure,
): CodeExecutor.ToolFailureResponse => ({
  id: event.id,
  outcome: 'failure',
  error,
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
      return failedToolResponse(event, {
        code: 'approval_required',
        message: `Tool "${event.name}" requires provider-mediated approval and cannot run inside code mode; move it to the agent toolkit and add it to codeMode.except`,
      });
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
    const value = jsonValue(result.value.encodedResult);
    return result.value.isFailure
      ? failedToolResponse(event, {
          code: 'tool_failure',
          message: toolFailureMessage(event.name, value),
          value,
        })
      : ({
          id: event.id,
          outcome: 'success',
          value,
        } satisfies CodeExecutor.ToolSuccessResponse);
  }).pipe(
    Effect.result,
    Effect.map((result) =>
      result._tag === 'Success'
        ? result.success
        : failedToolResponse(event, {
            code: 'dispatch_failed',
            message: result.failure.message,
          }),
    ),
  );

const catalogDescription = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
): string => {
  const tools = Object.values(toolkit.tools).map(descriptor);
  return [
    'Execute erasable TypeScript that composes the hidden tools below.',
    'The source is the body of an async function, so top-level await and return work.',
    'Imports and TypeScript syntax that requires transformation are unavailable.',
    'Use the SDK and standard TypeScript globals; host-specific APIs are unavailable.',
    'Values passed to text(...) become output; return a JSON value for a structured result.',
    renderCodeSdk(tools),
  ].join('\n');
};

/**
 * Split one resolved toolkit into the brokered half and the half that stays
 * directly advertised.
 *
 * Both halves keep the original `handle`: a handle routes by name, so
 * restricting `tools` — what the broker catalogs, what the provider seam
 * advertises — is the whole split. A name in `except` that the toolkit does
 * not define was already rejected by `Agent.make`, so it is ignored here
 * rather than re-validated.
 */
export const split = <
  Tools extends Record<string, Tool.Any>,
  Names extends keyof Tools & string,
>(
  resolved: Toolkit.WithHandler<Tools>,
  except: ReadonlyArray<Names>,
): {
  /**
   * The brokered half, deliberately name-erased: it exists only to be
   * wrapped into `exec`, whose catalog is a runtime rendering — precise
   * keys here would be carried nowhere.
   */
  readonly hidden: Toolkit.WithHandler<Record<string, Tool.Any>>;
  /** The advertised half, precise: these names reach the provider seam. */
  readonly excepted: Toolkit.WithHandler<Pick<Tools, Names>>;
} => {
  const names = new Set<string>(except);
  const hidden: Record<string, Tool.Any> = {};
  const excepted: Record<string, Tool.Any> = {};
  for (const [name, tool] of Object.entries<Tool.Any>(resolved.tools)) {
    (names.has(name) ? excepted : hidden)[name] = tool;
  }
  // The excepted record was built by keeping exactly the `Names` membership
  // its type describes; TypeScript cannot see through the loop, so it is
  // named once here. The shared `handle` narrows the same way — a handle
  // routes by name, and each half only advertises names the original handle
  // serves.
  return {
    hidden: {
      tools: hidden,
      handle: resolved.handle as Toolkit.WithHandler<
        Record<string, Tool.Any>
      >['handle'],
    },
    excepted: {
      tools: excepted as Pick<Tools, Names>,
      handle: resolved.handle as Toolkit.WithHandler<
        Pick<Tools, Names>
      >['handle'],
    },
  };
};

/**
 * One visible toolkit from the generated `exec` and the excepted tools.
 *
 * `exec` wins a name collision by construction, but there is none to win:
 * `Agent.make` reserves the `exec` name whenever code mode is enabled.
 */
export const merge = <
  Tools extends Record<string, Tool.Any>,
  Names extends keyof Tools & string,
>(
  visible: Toolkit.WithHandler<ExecTools>,
  excepted: Toolkit.WithHandler<Pick<Tools, Names>>,
): Toolkit.WithHandler<ExecTools & Pick<Tools, Names>> => {
  type Merged = ExecTools & Pick<Tools, Names>;
  type AnyHandle = Toolkit.WithHandler<Record<string, Tool.Any>>['handle'];
  const tools = {
    ...excepted.tools,
    ...visible.tools,
  } as Merged;
  // Each handle is widened once to the name-erased shape so the router can
  // dispatch on advertised-name membership, then the router is named as the
  // merged handle — the same membership the `tools` spread above encodes,
  // which is what makes the final assertion true.
  const exceptedHandle = excepted.handle as AnyHandle;
  const visibleHandle = visible.handle as AnyHandle;
  const route: AnyHandle = (name, params, toolCallId) =>
    Object.hasOwn(excepted.tools, name)
      ? exceptedHandle(name, params, toolCallId)
      : visibleHandle(name, params, toolCallId);
  return { tools, handle: route as Toolkit.WithHandler<Merged>['handle'] };
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
                let result: CodeExecutor.JsonValue | undefined;
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
                            result = event.result;
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
                return {
                  output: output.join(''),
                  ...(result === undefined ? {} : { result }),
                };
              }).pipe(
                Effect.mapError((error) => ({
                  code: 'execution_failed' as const,
                  message: error.message,
                })),
              ),
            ),
        }),
      ),
    );
  });

export * as CodeMode from './code-mode.js';
