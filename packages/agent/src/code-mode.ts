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
import { AiError, Tool, Toolkit } from 'effect/ai';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

import { CodeExecutor } from './code-executor.js';
import { renderCodeSdk } from './code-sdk.js';
import * as ToolExecution from './internal/tool-execution.js';
import type * as AgentLog from './log.js';
import { ResumeProjection } from './resume-projection.js';
import * as ToolLookup from './internal/tool-lookup.js';

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
  Effect.context(),
  (context) => Context.makeUnsafe<unknown>(context.mapUnsafe),
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
  if (!isEnabled(mode)) {
    return direct();
  }
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
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item, index) =>
      jsonValue(item, `${path}[${String(index)}]`),
    );
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
        `Code scratch value "${key}" exceeds ${String(limits.maxValueBytes)} bytes`,
      );
    }
  }
  const total = encoder.encode(JSON.stringify(prepared)).byteLength;
  if (total > limits.maxTotalBytes) {
    throw new StateError(
      `Code scratch state exceeds ${String(limits.maxTotalBytes)} bytes`,
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
        : ResumeProjection.codeStateFrom(session.stateHistory).committed;
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
          // Committed by the `exec` call running now, and in effect only once
          // that call settles.
          const producer = yield* Effect.serviceOption(ToolExecution.Current);
          const record: ConversationRecord.Record = {
            _tag: 'CodeStateCheckpoint',
            ...(Option.isSome(producer)
              ? { producer: producer.value.toolCallId }
              : {}),
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
  name: String(tool.name),
  description: Tool.getDescription(tool) ?? '',
  parameters: jsonValue(Tool.getJsonSchema(tool)),
  result: jsonValue(Tool.getJsonSchemaFromSchema(tool.successSchema)),
});

const toolFailureMessage = (
  name: string,
  value: CodeExecutor.JsonValue,
): string => {
  if (typeof value === 'string') {
    return value;
  }
  if (isJsonRecord(value) && typeof value['message'] === 'string') {
    return value['message'];
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

const invokeOnce = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
  event: CodeExecutor.ToolCall,
): Effect.Effect<
  CodeExecutor.ToolResponse,
  AiError.AiError,
  Tool.HandlerServices<Tools[keyof Tools]>
> =>
  Effect.gen(function* () {
    const name: string = event.name;
    const tool = ToolLookup.toolNamed(toolkit.tools, name);
    if (tool === undefined || !ToolLookup.isToolName(toolkit.tools, name)) {
      return yield* aiError(
        'nestedToolCall',
        `Unknown code-mode tool "${name}"`,
      );
    }
    if (tool.needsApproval !== undefined && tool.needsApproval !== false) {
      return failedToolResponse(event, {
        code: 'approval_required',
        message: `Tool "${event.name}" requires provider-mediated approval and cannot run inside code mode; move it to the agent toolkit and add it to codeMode.except`,
      });
    }
    // Sandbox input is untyped JSON; `handle` decodes it against the tool's
    // parameter schema before anything runs.
    const stream = yield* toolkit.handle(
      name,
      event.input as Tool.ParametersEncoded<Tools[typeof name]>,
      event.id,
    );
    const result = yield* Stream.runLast(stream);
    if (Option.isNone(result)) {
      return yield* aiError(
        'nestedToolCall',
        `Tool "${event.name}" returned no result`,
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

// ## Nested calls are durable, keyed, and served from the log
//
// A script's tool calls used to leave no trace: the hidden gate has no session,
// so after a crash the `exec` call was indeterminate and whatever its nested
// calls had done was invisible. When `exec` runs under a recording gate, each
// nested call writes the same `StepOpened`, `StepStarted` and `StepSettled`
// records a top-level call does, with `parent` set to the `exec` tool-call id
// and `id` set to the call's key within that execution. The key is
// `k:<name>` when the script passed `{ key }` and otherwise `#<n>`, the nth
// keyless call this execution made. Keyless ordinals are stable across a
// re-execution because the sandbox is deterministic for the same source, and
// the recorded name and input digest turn a script that drifts from its
// earlier run into a loud failure instead of a wrong answer served from the
// log. Scripts that branch on time or randomness should pass explicit keys.
//
// These records stay out of the model's prompt: the model sees one `exec`
// call and its result, and history rebuilding ignores any step with a parent.
//
// The rules mirror `dispatch.ts`'s, applied at the sandbox boundary:
//
//   1. a nested call an unsettled earlier run settled is served from the log,
//      before interception and metering — it already ran, and metering counts
//      real executions once;
//   2. one that was started but never settled runs again if its start
//      recorded the tool replay-safe (`Replay.safe`), and is otherwise
//      indeterminate: the script sees a `ToolCallError` with code
//      `indeterminate` and decides for itself, because only the script knows
//      whether it can carry on without that result;
//   3. otherwise the call goes to the hidden gate, whose interceptor and
//      metering run exactly as before, with the nested `StepStarted` appended
//      where a top-level one would be — after interception, before the handler.
//
// Like top-level recovery, the lookup indexes only what an *earlier* run wrote.
// The serving only matters when the same `exec` call id is entered again,
// which is `onIndeterminateToolCall` answering Retry.

const StoredFailure = Schema.Struct({
  code: Schema.Literals([
    'tool_failure',
    'approval_required',
    'dispatch_failed',
    'indeterminate',
  ]),
  message: Schema.String,
  value: Schema.optionalKey(Schema.Json),
});
const decodeStoredFailure = Schema.decodeUnknownOption(StoredFailure);

const servedResponse = (
  event: CodeExecutor.ToolCall,
  settled: AgentLog.Settled,
): CodeExecutor.ToolResponse => {
  if (settled.outcome === 'success') {
    return {
      id: event.id,
      outcome: 'success',
      value: jsonValue(settled.result),
    };
  }
  const failure = decodeStoredFailure(settled.result);
  return failedToolResponse(
    event,
    Option.isSome(failure)
      ? failure.value
      : {
          code: 'dispatch_failed',
          message: `Recorded failure of nested call "${event.name}" is unreadable`,
        },
  );
};

/** The `exec` call whose nested calls are being recorded. */
interface NestedOwner {
  readonly session: AgentLog.Session;
  readonly parent: LogVocabulary.ToolCallId;
  /** Keys this execution has used, so a duplicate cannot alias another call. */
  readonly seen: Set<string>;
}

const invoke = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
  event: CodeExecutor.ToolCall,
  key: string,
  owner: NestedOwner | undefined,
): Effect.Effect<
  CodeExecutor.ToolResponse,
  AiError.AiError | AgentLog.DurabilityError,
  Tool.HandlerServices<Tools[keyof Tools]>
> => {
  if (owner === undefined) {
    return invokeOnce(toolkit, event);
  }
  const { session, parent, seen } = owner;
  return Effect.gen(function* () {
    if (seen.has(key)) {
      return failedToolResponse(event, {
        code: 'dispatch_failed',
        message: `Nested call key "${key}" was used twice in one execution`,
      });
    }
    seen.add(key);

    const id = LogVocabulary.ToolCallId.make(key);
    const digest = yield* session.inputDigest(event.name, event.input);
    const prior = session.nestedRecovery(parent, id);
    if (Option.isSome(prior)) {
      if (
        prior.value.name !== event.name ||
        prior.value.inputDigest !== digest
      ) {
        return failedToolResponse(event, {
          code: 'dispatch_failed',
          message: `Nested call "${key}" differs from the one a earlier execution recorded under that key (${prior.value.name}); pass explicit keys if the script is not deterministic`,
        });
      }
      if (prior.value.settled !== undefined) {
        return servedResponse(event, prior.value.settled);
      }
      if (prior.value.started === 'unsafe') {
        return failedToolResponse(event, {
          code: 'indeterminate',
          message: `Nested call "${event.name}" (${key}) started in an earlier execution and never settled; it is not re-run because it may have committed`,
        });
      }
    } else {
      yield* session.append([
        {
          _tag: 'StepOpened',
          id,
          parent,
          name: event.name,
          inputDigest: digest,
          params: event.input,
        },
      ]);
    }

    let started = false;
    const response = yield* invokeOnce(toolkit, event).pipe(
      Effect.provideService(ToolExecution.NestedStart, {
        // Once only: the context also reaches any gate inside the handler
        // (a delegated child agent's tools), and those are not this call.
        started: (replay) =>
          Effect.suspend(() => {
            if (started) {
              return Effect.void;
            }
            started = true;
            return session.append([
              { _tag: 'StepStarted', id, parent, name: event.name, replay },
            ]);
          }),
      }),
    );
    // A dispatch failure is unsettled on purpose: past its `StepStarted` the
    // handler may have run, and before it a retry is harmless either way.
    if (
      response.outcome === 'failure' &&
      response.error.code === 'dispatch_failed'
    ) {
      return response;
    }
    yield* session.append([
      {
        _tag: 'StepSettled',
        id,
        parent,
        name: event.name,
        outcome: response.outcome,
        result:
          response.outcome === 'success' ? response.value : response.error,
      },
    ]);
    return response;
  });
};

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
      handle: resolved.handle,
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
  };
  // Each handle is widened once to the name-erased shape so the router can
  // dispatch on advertised-name membership, then the router is named as the
  // merged handle — the same membership the `tools` spread above encodes,
  // which is what makes the final assertion true.
  const exceptedHandle = excepted.handle as AnyHandle;
  const visibleHandle = visible.handle as AnyHandle;
  const route: AnyHandle = (name, params, toolCallId) =>
    ToolLookup.isToolName(excepted.tools, name)
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
                const owning = yield* Effect.serviceOption(
                  ToolExecution.Current,
                );
                const owner: NestedOwner | undefined = Option.isSome(owning)
                  ? {
                      session: owning.value.session,
                      parent: owning.value.toolCallId,
                      seen: new Set(),
                    }
                  : undefined;
                let ordinal = 0;
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
                        case 'ToolCall': {
                          // Assigned here, synchronously in event order, not
                          // inside the concurrent effect that follows.
                          const key =
                            event.key === undefined
                              ? `#${String((ordinal += 1))}`
                              : `k:${event.key}`;
                          return Effect.flatMap(
                            invoke(hidden, event, key, owner).pipe(
                              Effect.provide(services),
                            ),
                            execution.respond,
                          );
                        }
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
                  return yield* aiError(
                    'execute',
                    'Executor ended without completion',
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
