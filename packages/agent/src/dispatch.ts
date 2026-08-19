import {
  Context,
  Effect,
  Exit,
  Option,
  Ref,
  Schema,
  Stream,
  SubscriptionRef,
} from 'effect';
import { AiError, type Tool, type Toolkit } from 'effect/unstable/ai';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

import type { Interception } from './interception.js';
import type * as AgentLog from './log.js';
import * as Observability from './internal/observability.js';
import * as ToolExecution from './internal/tool-execution.js';
import { APPROVAL_WAIT } from './recovery.js';
import { RunPolicy } from './run-policy.js';
import { RunPolicyRuntime } from './run-policy-runtime.js';

type RunError = AiError.AiError | RunPolicy.RunPolicyExhausted;

/**
 * The reserved `ToolSuspended.wait` name for a tool's own `needsApproval`
 * gate. Declared in `recovery.ts` — see its doc for why — and re-exported
 * here because this is where `resolveIndeterminate` uses it.
 */
export { APPROVAL_WAIT };

const isRunPolicyExhausted = Schema.is(RunPolicy.RunPolicyExhausted);

// Toolkit resolution captures handler and schema services in the ambient
// context, but Effect intentionally exposes that captured context as
// `Context<never>`. Widen it once at this internal boundary so schema decoders
// can consume the services they declare without each decoder asserting its
// entire Effect type.
const capturedContext: Effect.Effect<Context.Context<unknown>> = Effect.map(
  Effect.context<never>(),
  (context) => context as Context.Context<unknown>,
);

const preserveRunPolicy = <T extends Tool.Any, E, R>(
  stream: Stream.Stream<Tool.HandlerResult<T>, E, R>,
): Stream.Stream<Tool.HandlerResult<T>, E | RunPolicy.RunPolicyExhausted, R> =>
  Stream.mapEffect(stream, (result) =>
    result.isFailure && isRunPolicyExhausted(result.result)
      ? Effect.fail(result.result)
      : Effect.succeed(result),
  );

// The tool-dispatch seam: consult the log before running a tool.
//
// ## The gap this closes
//
// The deleted `@sunfall/vesper-durable` wrapped two functions — `generateText` and
// `streamText` — so a checkpoint covered the provider call and nothing after
// it. Tool execution happens inside `LanguageModel`'s resolution of the turn,
// past where every checkpoint sat. A run that died after tool A and before
// tool B therefore recovered by replaying the model call for free and
// **re-running tool A**, with its side effects, a second time. For a while
// the log did not prevent it either: it had the record — one `ToolOutcome`
// per settled call — and nothing read it back.
//
// ## Why this needs no change to the `LanguageModel` contract
//
// It looked like it would. Tool dispatch is buried in `LanguageModel`'s
// internals: `handleToolCall` calls `toolkit.handle(name, params, id)` on the
// resolved toolkit and there is no hook anywhere near it.
//
// But the toolkit is not a fixed value the loop hands over — the option is
// typed `ToolkitInput<Tools>`, which admits `Toolkit.WithHandler<Tools>` *or*
// an `Effect` producing one, and `LanguageModel` resolves it with
// `resolveToolkit` before use. A `Toolkit` is itself that effect. So the seam
// is a toolkit whose `handle` checks the log first and delegates otherwise —
// a value passed through a documented public option, not a patch, not a
// subclass, and not a fork of the turn loop. Every rule `LanguageModel`
// enforces around dispatch (approvals, concurrency, deferring `finish` until
// handlers settle) is untouched, because none of them are downstream of this.
//
// ## Recovery has two phases
//
// Indeterminate starts are resolved from their recorded ToolCall before the
// next provider request. Retry uses that call's original name, id, and params;
// reconciliation validates and records an outcome. The rebuilt prompt then
// contains the result, so recovery never depends on a provider re-emitting an
// id. Completed outcomes still have a provider-dispatch fallback: if a call id
// from an orphaned run is encountered again, its durable result is served
// rather than executing the handler twice.
//
// ## Two things with an opinion, and which one wins
//
// This is also where an interceptor's `beforeToolCall` sits, so two mechanisms
// can have a view of the same call. The order is fixed and it is not a
// toss-up:
//
//   1. before the model call, indeterminate starts are resolved through their
//      dedicated callback;
//   2. during provider dispatch, a completed recovery outcome is served
//      without interception;
//   3. `beforeToolCall` may answer a fresh call in the tool's place;
//   4. immediately after `ToolStarted` is durable, the real handler runs.
//
// A completed recovery wins because the call already ran. An indeterminate
// start is different: the handler was entered but may or may not have committed.
// It has no safe default, so ordinary `beforeToolCall` permission cannot repeat
// it; only `onIndeterminateToolCall` can explicitly Retry or Answer.
//
// The consequence worth stating plainly: **an interceptor cannot revoke
// permission for a tool call a crashed run already completed.** The way to
// stop that call from being served is to settle the run — a `RunSettled`
// record empties the index — not to refuse it here.
//
// The reverse direction has no such asymmetry. A call the interceptor answers
// is recorded as an ordinary `ToolOutcome`, so if *this* run then crashes, the
// substituted answer is what a later run recovers. That is the same rule
// applied consistently: what the log says happened is what happened.

/**
 * What may have an opinion about a call, besides the tool itself.
 *
 * One options object rather than two positional arguments, because the two
 * are independent — an agent may be intercepted without recording, and
 * recording without being intercepted — and because a boolean-blind
 * `gate(toolkit, undefined, interceptor)` at the one call site would be worse
 * than either.
 */
export interface GateOptions<InterceptorRequires = never> {
  /** The run's log claim, when it is recording. */
  readonly session?: AgentLog.Session | undefined;
  /** The agent's interceptor, when it has one. */
  readonly interceptor?:
    | Interception.Interceptor<InterceptorRequires>
    | undefined;
  /** The agent's name, for {@link Interception.ToolCallContext}. */
  readonly agent: string;
  /** Root-run budget shared by this loop and every descendant. */
  readonly runtime?: RunPolicyRuntime.Runtime | undefined;
  /** Delegation calls use the child semaphore and must not hold a tool permit. */
  readonly unmeteredToolNames?: ReadonlySet<string> | undefined;
  /** Atomic winner between responsive cancellation and dispatch commits. */
  readonly arbitration?: TurnArbitration | undefined;
}

/** Resolve every orphaned handler start before a resumed provider call. */
export const resolveIndeterminate = <
  Tools extends Record<string, Tool.Any>,
  ToolkitRequires,
  InterceptorRequires = never,
>(
  toolkit: Effect.Effect<Toolkit.WithHandler<Tools>, never, ToolkitRequires>,
  options: GateOptions<InterceptorRequires> & {
    readonly session: AgentLog.Session;
    readonly arbitration: TurnArbitration;
  },
): Effect.Effect<void, RunError, ToolkitRequires | InterceptorRequires> =>
  Effect.gen(function* () {
    if (options.session.recoveryCorruption !== undefined) {
      return yield* Effect.fail(
        recoveryCorruptionError(options.session.recoveryCorruption),
      );
    }
    const pending = options.session.pendingToolCalls.filter((call) => {
      const recovery = options.session.recovery(call.name, call.toolCallId);
      return Option.isSome(recovery) && recovery.value._tag !== 'Settled';
    });
    if (pending.length === 0) return;
    const resolve = options.interceptor?.onIndeterminateToolCall;
    const unresolved = pending.find((call) => {
      const recovery = options.session.recovery(call.name, call.toolCallId);
      return Option.isSome(recovery) && recovery.value._tag === 'Indeterminate';
    });
    if (unresolved !== undefined && resolve === undefined) {
      return yield* Effect.fail(
        indeterminateError(unresolved.name, unresolved.toolCallId),
      );
    }

    const resolved = yield* toolkit;
    const services = yield* capturedContext;
    for (const call of pending) {
      const recovery = options.session.recovery(call.name, call.toolCallId);
      if (Option.isNone(recovery) || recovery.value._tag === 'Settled')
        continue;

      // A durable approval never falls into the ordinary replay-or-ask
      // branch below: an undecided one must not dispatch (this function's
      // caller has already refused to reach here for one still undecided —
      // this is only a defensive no-op), a denied one settles as a
      // refusal without ever entering the handler, and an approved one
      // falls through to the same "Suspended -> Retry" path a durable
      // wait's resumption already uses, which genuinely dispatches the
      // handler for the first time.
      if (
        recovery.value._tag === 'Suspended' &&
        recovery.value.wait === APPROVAL_WAIT
      ) {
        const decided = options.session.completedWait(recovery.value.token);
        if (Option.isNone(decided)) continue;
        if (decided.value.outcome === 'failure') {
          yield* options.session.append([
            {
              _tag: 'ToolResumed',
              id: call.toolCallId,
              name: call.name,
              token: recovery.value.token,
            },
            {
              _tag: 'ToolOutcome',
              step: call.step,
              id: call.toolCallId,
              name: call.name,
              outcome: 'failure',
              result: decided.value.result,
            },
          ]);
          continue;
        }
      }

      const replayable =
        recovery.value._tag === 'Suspended' ||
        recovery.value._tag === 'Restarting';
      let decision: Interception.IndeterminateToolDecision;
      if (replayable) {
        decision = { _tag: 'Retry' };
      } else {
        if (resolve === undefined) {
          return yield* Effect.fail(
            indeterminateError(call.name, call.toolCallId),
          );
        }
        decision = yield* resolve({
          agent: options.agent,
          conversationId: options.session.conversationId,
          name: call.name,
          toolCallId: call.toolCallId,
          params: call.params,
        }).pipe(Effect.provide(services));
      }

      if (decision._tag === 'Answer') {
        yield* decodeResult(
          resolved,
          services,
          call.name,
          decision.result,
          decision.isFailure,
        );
        yield* options.session.append([
          {
            _tag: 'ToolOutcome',
            step: call.step,
            id: call.toolCallId,
            name: call.name,
            outcome: decision.isFailure ? 'failure' : 'success',
            result: decision.result,
          },
        ]);
        continue;
      }

      if (!hasTool(resolved.tools, call.name)) {
        return yield* Effect.fail(unknownToolError(call.name));
      }

      const params = yield* decodeParameters(
        resolved,
        services,
        call.name,
        call.params,
      );
      const permit = yield* options.arbitration.commit;
      if (Option.isNone(permit)) return;

      yield* Effect.gen(function* () {
        if (recovery.value._tag === 'Suspended') {
          yield* options.session.append([
            {
              _tag: 'ToolResumed',
              id: call.toolCallId,
              name: call.name,
              token: recovery.value.token,
            },
          ]);
        } else {
          yield* options.session.append([
            {
              _tag: 'ToolStarted',
              id: call.toolCallId,
              name: call.name,
            },
          ]);
        }
        const execution: ToolExecution.Execution = {
          session: options.session,
          name: call.name,
          toolCallId: call.toolCallId,
        };
        const invoke = resolved.handle(call.name, params, call.toolCallId).pipe(
          Effect.provideService(ToolExecution.Current, execution),
          Effect.map((stream) =>
            stream.pipe(
              Stream.provideService(ToolExecution.Current, execution),
            ),
          ),
        );
        const metered =
          options.runtime === undefined ||
          options.unmeteredToolNames?.has(call.name) === true
            ? yield* invoke
            : options.runtime.toolStream(Stream.unwrap(invoke));
        const guarded = preserveRunPolicy(metered).pipe(
          // `Toolkit.handle` exposes a per-tool `HandlerError` on the
          // returned stream. The erased Dispatch type cannot carry that
          // relationship, so normalize it at this boundary before the
          // retry joins the agent's public RunError channel. Preserve the
          // errors that are already part of that channel.
          Stream.mapError((error) =>
            isRunPolicyExhausted(error) || AiError.isAiError(error)
              ? error
              : indeterminateHandlerError(call.name, error),
          ),
        );
        const result = yield* options.runtime === undefined
          ? Stream.runLast(guarded)
          : Effect.gen(function* () {
              const remaining = yield* options.runtime!.remainingMillis;
              return yield* Stream.runLast(guarded).pipe(
                Effect.timeoutOrElse({
                  duration: remaining,
                  orElse: () =>
                    Effect.fail(
                      RunPolicyRuntime.error({
                        limit: 'deadline',
                        used: options.runtime!.limits.wallClockMillis,
                        maximum: options.runtime!.limits.wallClockMillis,
                      }),
                    ),
                }),
              );
            });
        if (Option.isNone(result)) {
          return yield* Effect.fail(emptyToolResultError(call.name));
        }
        yield* options.session.append([
          {
            _tag: 'ToolOutcome',
            step: call.step,
            id: call.toolCallId,
            name: call.name,
            outcome: result.value.isFailure ? 'failure' : 'success',
            result: result.value.encodedResult,
          },
        ]);
      }).pipe(Effect.ensuring(permit.value.settle));
    }
  }).pipe(
    Effect.catchTag('DurabilityError', (error) =>
      Effect.fail(durabilityAiError(error)),
    ),
  );

const hasTool = <Tools extends Record<string, Tool.Any>>(
  tools: Tools,
  name: string,
): name is Extract<keyof Tools, string> => Object.hasOwn(tools, name);

const decodeUnknownToolValue = (
  schema: Schema.Constraint | undefined,
  value: unknown,
  services: Context.Context<unknown>,
  onError: (detail: string) => AiError.AiError,
): Effect.Effect<unknown, AiError.AiError> =>
  schema === undefined
    ? Effect.fail(onError('tool is not defined'))
    : Schema.decodeUnknownEffect(schema)(value).pipe(
        Effect.provide(services),
        Effect.mapError((error) => onError(String(error))),
      );

const decodeResult = (
  toolkit: { readonly tools: Record<string, Tool.Any> },
  services: Context.Context<unknown>,
  name: string,
  result: unknown,
  isFailure: boolean,
): Effect.Effect<unknown, AiError.AiError> => {
  const tool = Object.hasOwn(toolkit.tools, name)
    ? toolkit.tools[name]
    : undefined;
  const schema =
    tool === undefined
      ? undefined
      : isFailure
        ? Schema.Union([tool.failureSchema, AiError.AiError])
        : tool.successSchema;
  return decodeUnknownToolValue(schema, result, services, (detail) =>
    toolResultDecodeError(name, detail),
  );
};

const decodeParameters = <
  Tools extends Record<string, Tool.Any>,
  Name extends Extract<keyof Tools, string>,
>(
  toolkit: { readonly tools: Tools },
  services: Context.Context<never>,
  name: Name,
  params: unknown,
): Effect.Effect<Tool.Parameters<Tools[Name]>, AiError.AiError> =>
  toolkit.tools[name] === undefined
    ? Effect.fail(unknownToolError(name))
    : (Schema.decodeUnknownEffect(toolkit.tools[name].parametersSchema)(
        params,
      ).pipe(
        Effect.provide(services),
        Effect.mapError((error) =>
          toolParameterDecodeError(name, String(error)),
        ),
      ) as Effect.Effect<Tool.Parameters<Tools[Name]>, AiError.AiError>);

interface ArbitrationState {
  readonly cancelled: boolean;
  readonly dispatches: number;
}

export interface TurnArbitration {
  /** Wait until every dispatch commit is durably settled, then cancel. */
  readonly cancel: Effect.Effect<void>;
  /** Atomically commit one dispatch, or return none after cancellation wins. */
  readonly commit: Effect.Effect<Option.Option<DispatchPermit>>;
}

/** One committed dispatch, released exactly once regardless of exit path. */
export interface DispatchPermit {
  readonly settle: Effect.Effect<void>;
}

/** One turn's atomic cancellation/dispatch race. */
export const makeTurnArbitration: Effect.Effect<TurnArbitration> = Effect.gen(
  function* () {
    const state = yield* SubscriptionRef.make<ArbitrationState>({
      cancelled: false,
      dispatches: 0,
    });
    const awaitIdle = SubscriptionRef.changes(state).pipe(
      Stream.filter((current) => current.dispatches === 0),
      Stream.runHead,
      Effect.asVoid,
    );

    return {
      cancel: Effect.gen(function* () {
        while (true) {
          const won = yield* SubscriptionRef.modify(state, (current) =>
            current.dispatches === 0
              ? [true, { ...current, cancelled: true }]
              : [false, current],
          );
          if (won) return;
          yield* awaitIdle;
        }
      }),
      commit: Effect.uninterruptible(
        Effect.gen(function* () {
          const released = yield* Ref.make(false);
          const committed = yield* SubscriptionRef.modify(state, (current) =>
            current.cancelled
              ? [false, current]
              : [true, { ...current, dispatches: current.dispatches + 1 }],
          );
          if (!committed) return Option.none<DispatchPermit>();

          const settle = Effect.gen(function* () {
            const first = yield* Ref.modify(released, (current) =>
              current ? [false, true] : [true, true],
            );
            if (!first) return;
            yield* SubscriptionRef.update(state, (current) => ({
              ...current,
              dispatches: current.dispatches - 1,
            }));
          });
          return Option.some({ settle });
        }),
      ),
    };
  },
);

/**
 * Wrap a toolkit so settled calls are served from the log instead of re-run,
 * and the rest are offered to an interceptor before they run.
 *
 * Returns the `Effect` form of a toolkit, which is what a plain `Toolkit`
 * already is, so it drops into `streamText({ toolkit })` unchanged. Neither
 * option is required: an agent that neither records nor intercepts never
 * reaches this function, because the loop passes its toolkit through
 * untouched.
 */
export const gate = <
  Tools extends Record<string, Tool.Any>,
  ToolkitRequires,
  InterceptorRequires = never,
>(
  toolkit: Effect.Effect<Toolkit.WithHandler<Tools>, never, ToolkitRequires>,
  options: GateOptions<InterceptorRequires>,
): Effect.Effect<
  Toolkit.WithHandler<Tools>,
  never,
  ToolkitRequires | InterceptorRequires
> =>
  Effect.gen(function* () {
    const session = options.session;
    const interceptor = options.interceptor;
    const resolved = yield* toolkit;

    // Captured the way `Subagent.delegateTo` captures its children's
    // services, and for the same reason: decoding a stored result may need
    // the tool's own decoding services, and `handle`'s signature fixes what
    // its stream is allowed to require. Providing the ambient context inward
    // keeps the requirement off the signature instead of casting it away.
    const services = yield* capturedContext;
    const decoders = new Map<string, Decode>();

    const decoderFor = (name: string): Decode => {
      const cached = decoders.get(name);
      if (cached !== undefined) return cached;

      const tool = Object.hasOwn(resolved.tools, name)
        ? resolved.tools[name]
        : undefined;

      // Mirrors `Toolkit`'s own `resultSchema`: with `failureMode: 'return'`
      // a failure comes back as a value, so the recorded result may be a
      // success, a declared failure, or an `AiError`.
      const schema =
        tool === undefined
          ? undefined
          : tool.failureMode === 'return'
            ? Schema.Union([
                tool.successSchema,
                tool.failureSchema,
                AiError.AiError,
              ])
            : tool.successSchema;

      const decode: Decode = (stored: unknown) =>
        // The requirement channel is erased rather than declared. A tool's
        // decoding services are already in the agent's `WithOwnHandlers`, so
        // the caller has provided them and they are in `services` — but
        // `handle`'s signature fixes what its effect may require, and
        // declaring them here would put a requirement on a value
        // `LanguageModel` resolves internally. Capturing and providing them is
        // the same move `Subagent.delegateTo` makes for a child's services.
        decodeUnknownToolValue(schema, stored, services, (detail) =>
          toolResultDecodeError(name, detail),
        );

      decoders.set(name, decode);
      return decode;
    };

    /**
     * A result nobody's handler produced, in the two forms a part carries.
     *
     * Shared by the two paths that answer without dispatching, because they
     * differ only in where the encoded value came from. The decoded half goes
     * back through the tool's own codec so a consumer of the live stream reads
     * what its type says; the encoded half is served as-is, because that is
     * the field `Prompt` builds the tool-result message from.
     */
    const answered = <Name extends keyof Tools>(
      name: Name,
      encoded: unknown,
      isFailure: boolean,
      requireDecoded: boolean,
    ): Effect.Effect<
      Stream.Stream<Tool.HandlerResult<Tools[Name]>>,
      AiError.AiError
    > =>
      Effect.map(
        requireDecoded
          ? decoderFor(String(name))(encoded)
          : decoderFor(String(name))(encoded).pipe(
              // Only a typed schema failure gets the encoded fallback. Defects
              // and interruption are control-flow signals and must survive.
              Effect.catch(() => Effect.succeed(encoded)),
            ),
        (decoded) =>
          Stream.make({
            // The value has just passed this tool's result schema. Effect's
            // widened runtime `failureMode` property prevents TypeScript from
            // deriving the conditional `Tool.Result` type from that schema.
            result: decoded as Tool.Result<Tools[Name]>,
            encodedResult: encoded,
            isFailure,
            preliminary: false,
            // `LanguageModel` spreads handler results into the emitted part.
            // This discriminant keeps an encoded fallback from masquerading as
            // a schema-decoded tool success in the public event stream.
            resultSource: 'substituted' as const,
          }),
      );

    const handle = <Name extends keyof Tools>(
      name: Name,
      params: Tool.Parameters<Tools[Name]>,
      toolCallId?: string,
    ): ReturnType<Toolkit.WithHandler<Tools>['handle']> => {
      const toolName = String(name);
      return Effect.gen(function* () {
        yield* Observability.toolCall;
        const normalizedToolCallId =
          toolCallId === undefined
            ? undefined
            : LogVocabulary.ToolCallId.make(toolCallId);
        const prior =
          normalizedToolCallId === undefined || session === undefined
            ? Option.none<AgentLog.Recovery>()
            : session.recovery(toolName, normalizedToolCallId);

        // Step 1. A call an unsettled earlier run already completed is served
        // from the log and goes no further — not to the interceptor, and not
        // to the tool. See the ordering note above.
        if (Option.isSome(prior) && prior.value._tag === 'Settled') {
          yield* Observability.recoveredToolCall;
          return yield* answered(
            name,
            prior.value.result,
            prior.value.outcome === 'failure',
            true,
          );
        }

        if (Option.isSome(prior)) {
          yield* Observability.indeterminateToolCall;
          if (normalizedToolCallId === undefined) {
            return yield* Effect.fail(missingToolCallIdError(toolName));
          }
          return yield* Effect.fail(
            indeterminateError(toolName, normalizedToolCallId),
          );
        }

        // Step 3. The ordinary interceptor, which may answer in the tool's place.
        //
        // Its requirement channel is erased in the same way and for the same
        // reason as the decoders': `handle`'s signature fixes what its effect
        // may require, so the services the seam needs are provided from the
        // context captured above rather than declared here. `intercepting` is
        // what makes them present — it puts the interceptor's `R` on the
        // agent's public requirement channel, so a caller who did not provide
        // them did not compile.
        if (interceptor?.beforeToolCall !== undefined) {
          const decision = yield* interceptor
            .beforeToolCall({
              agent: options.agent,
              conversationId: session?.conversationId,
              name: toolName,
              toolCallId: normalizedToolCallId,
              params,
            })
            .pipe(Effect.provide(services));

          if (decision._tag === 'Answer') {
            return yield* answered(
              name,
              decision.result,
              decision.isFailure,
              false,
            );
          }
        }

        // Step 4. The tool. Dispatch commits before its start becomes durable.
        const arbitration = options.arbitration;
        const permit =
          arbitration === undefined
            ? Option.none<DispatchPermit>()
            : yield* arbitration.commit;
        if (arbitration !== undefined && Option.isNone(permit)) {
          // Cancellation owns the turn. The provider stream is being stopped
          // concurrently; parking here prevents a handler from crossing the
          // gate in the small interval before that interruption reaches it.
          return yield* Effect.never;
        }
        const settle = Option.isSome(permit) ? permit.value.settle : undefined;
        const setup = Effect.gen(function* () {
          if (session !== undefined) {
            if (normalizedToolCallId === undefined) {
              return yield* Effect.fail(missingToolCallIdError(toolName));
            }
            yield* session
              .append([
                {
                  _tag: 'ToolStarted',
                  id: normalizedToolCallId,
                  name: toolName,
                },
              ])
              .pipe(Effect.mapError(durabilityAiError));
            // Register only after ToolStarted is durable. The handler cannot
            // begin before this effect returns, so no ToolOutcome can race the
            // registration; a failed append leaves no stale callback behind.
            if (settle !== undefined) {
              session.onToolSettled(toolName, normalizedToolCallId, settle);
            }
          }
          const execution: ToolExecution.Execution | undefined =
            session === undefined || normalizedToolCallId === undefined
              ? undefined
              : {
                  session,
                  name: toolName,
                  toolCallId: normalizedToolCallId,
                };
          const invoked = resolved.handle(name, params, toolCallId);
          const invoke =
            execution === undefined
              ? invoked
              : invoked.pipe(
                  Effect.provideService(ToolExecution.Current, execution),
                  Effect.map((stream) =>
                    stream.pipe(
                      Stream.provideService(ToolExecution.Current, execution),
                    ),
                  ),
                );
          const stream =
            options.runtime === undefined ||
            options.unmeteredToolNames?.has(toolName) === true
              ? yield* invoke
              : options.runtime.toolStream(Stream.unwrap(invoke));
          return preserveRunPolicy(stream).pipe(
            Stream.tap((result) =>
              result.isFailure ? Observability.toolFailure : Effect.void,
            ),
            Stream.onExit((exit) =>
              Exit.isFailure(exit) ? Observability.toolFailure : Effect.void,
            ),
            // Toolkit.handle returns a stream whose handler work is pull
            // driven. Keep this span around the returned stream, not merely
            // around the effect that constructs it.
            Stream.withSpan('Agent.tool', {
              attributes: {
                'vesper.tool.name': toolName,
                ...(session === undefined
                  ? {}
                  : { 'vesper.conversation.id': session.conversationId }),
                ...(normalizedToolCallId === undefined
                  ? {}
                  : { 'vesper.tool.call_id': normalizedToolCallId }),
              },
            }),
          );
        });
        const guarded = yield* settle === undefined
          ? setup
          : setup.pipe(
              Effect.catchCause((cause) =>
                Effect.uninterruptible(settle).pipe(
                  Effect.andThen(Effect.failCause(cause)),
                ),
              ),
            );
        return settle === undefined
          ? guarded
          : Stream.onExit(guarded, (exit) =>
              Exit.isFailure(exit) ? settle : Effect.void,
            );
      }) as ReturnType<Toolkit.WithHandler<Tools>['handle']>;
    };

    return {
      tools: resolved.tools,
      handle,
    };
  });

/** A decoder for one tool's substituted encoded result. */
type Decode = (stored: unknown) => Effect.Effect<unknown, AiError.AiError>;

const indeterminateError = (
  name: string,
  toolCallId: LogVocabulary.ToolCallId,
): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'dispatchTool',
    reason: new AiError.UnknownError({
      description: `Tool ${name} (${toolCallId}) has indeterminate execution; configure onIndeterminateToolCall to Retry or Answer explicitly`,
      metadata: { name, toolCallId },
    }),
  });

const durabilityAiError = (error: AgentLog.DurabilityError): AiError.AiError =>
  new AiError.AiError({
    module: 'AgentLog',
    method: error.operation,
    reason: new AiError.UnknownError({
      description: error.detail,
      metadata: {
        tag: error._tag,
        source: error.source,
        reason: error.reason,
      },
    }),
  });

const recoveryCorruptionError = (description: string): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'resolveIndeterminateToolCall',
    reason: new AiError.InvalidRequestError({ description }),
  });

const unknownToolError = (name: string): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'resolveIndeterminateToolCall',
    reason: new AiError.InvalidRequestError({
      description: `Stored tool "${name}" is not present in the current toolkit. Use the matching agent revision.`,
      metadata: { name },
    }),
  });

const missingToolCallIdError = (name: string): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'dispatchTool',
    reason: new AiError.InvalidRequestError({
      description: `Recorded tool ${name} cannot be dispatched without a tool call id`,
      metadata: { name },
    }),
  });

const emptyToolResultError = (name: string): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'resolveIndeterminateToolCall',
    reason: new AiError.UnknownError({
      description: `Retried tool ${name} completed without a final result`,
      metadata: { name },
    }),
  });

const indeterminateHandlerError = (
  name: string,
  error: unknown,
): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'resolveIndeterminateToolCall',
    reason: new AiError.UnknownError({
      description: `Tool ${name} failed during retry: ${String(error)}`,
      metadata: { name },
    }),
  });

const toolResultDecodeError = (name: string, detail: string): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'dispatchTool',
    reason: new AiError.InvalidRequestError({
      description:
        `Stored or reconciled result for tool "${name}" does not match its current result schema. ` +
        'Use the matching agent revision or return a schema-valid reconciliation answer.',
      metadata: { name, detail },
    }),
  });

const toolParameterDecodeError = (
  name: string,
  detail: string,
): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'resolveIndeterminateToolCall',
    reason: new AiError.InvalidRequestError({
      description: `Stored parameters for tool "${name}" do not match its current parameter schema. Use the matching agent revision.`,
      metadata: { name, detail },
    }),
  });

export * as ToolDispatch from './dispatch.js';
