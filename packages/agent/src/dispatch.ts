import {
  Context,
  Effect,
  Exit,
  Option,
  PubSub,
  Ref,
  Schema,
  Stream,
} from 'effect';
import { AiError, type Tool, type Toolkit } from 'effect/unstable/ai';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

import type { Interception } from './interception.js';
import type { AgentLog } from './log.js';
import { RunPolicy } from './run-policy.js';

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
export interface GateOptions {
  /** The run's log claim, when it is recording. */
  readonly session?: AgentLog.Session | undefined;
  /** The agent's interceptor, when it has one. */
  readonly interceptor?: Interception.Interceptor | undefined;
  /** The agent's name, for {@link Interception.ToolCallContext}. */
  readonly agent: string;
  /** Root-run budget shared by this loop and every descendant. */
  readonly runtime?: RunPolicy.Runtime | undefined;
  /** Delegation calls use the child semaphore and must not hold a tool permit. */
  readonly unmeteredToolNames?: ReadonlySet<string> | undefined;
  /** Atomic winner between responsive cancellation and dispatch commits. */
  readonly arbitration?: TurnArbitration | undefined;
}

/** Resolve every orphaned handler start before a resumed provider call. */
export const resolveIndeterminate = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  options: GateOptions & {
    readonly session: AgentLog.Session;
    readonly arbitration: TurnArbitration;
  },
): Effect.Effect<void, AiError.AiError, Tool.HandlersFor<Tools>> =>
  Effect.gen(function* () {
    if (options.session.recoveryCorruption !== undefined) {
      return yield* Effect.fail(
        recoveryCorruptionError(options.session.recoveryCorruption),
      );
    }
    const pending = options.session.indeterminateToolCalls.filter((call) => {
      const recovery = options.session.recovery(call.name, call.toolCallId);
      return Option.isSome(recovery) && recovery.value._tag === 'Indeterminate';
    });
    if (pending.length === 0) return;
    const resolve = options.interceptor?.onIndeterminateToolCall;
    if (resolve === undefined) {
      const first = pending[0]!;
      return yield* Effect.fail(
        indeterminateError(first.name, first.toolCallId),
      );
    }

    const resolved = yield* toolkit;
    const underlying = resolved.handle as unknown as Dispatch;
    const services = yield* Effect.context<never>();
    for (const call of pending) {
      const decision = yield* resolve({
        agent: options.agent,
        conversationId: options.session.conversationId,
        name: call.name,
        toolCallId: call.toolCallId,
        params: call.params,
      }).pipe(Effect.provide(services));

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

      const params = yield* decodeParameters(
        resolved,
        services,
        call.name,
        call.params,
      );
      if (!(yield* options.arbitration.dispatchCommits)) return;

      yield* Effect.gen(function* () {
        yield* options.session.append([
          {
            _tag: 'ToolStarted',
            id: call.toolCallId,
            name: call.name,
          },
        ]);
        const metered =
          options.runtime === undefined ||
          options.unmeteredToolNames?.has(call.name) === true
            ? yield* underlying(call.name, params, call.toolCallId)
            : options.runtime.toolStream(
                Stream.unwrap(underlying(call.name, params, call.toolCallId)),
              );
        const result = yield* options.runtime === undefined
          ? Stream.runLast(metered)
          : Effect.gen(function* () {
              const remaining = yield* options.runtime!.remainingMillis;
              return yield* Stream.runLast(metered).pipe(
                Effect.timeoutOrElse({
                  duration: remaining,
                  orElse: () =>
                    Effect.fail(
                      RunPolicy.error({
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
      }).pipe(Effect.ensuring(options.arbitration.settled));
    }
  }) as Effect.Effect<void, AiError.AiError, Tool.HandlersFor<Tools>>;

const decodeResult = (
  toolkit: { readonly tools: Record<string, Tool.Any> },
  services: Context.Context<never>,
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
  return schema === undefined
    ? Effect.fail(toolResultDecodeError(name, 'tool is not defined'))
    : (Schema.decodeUnknownEffect(schema)(result).pipe(
        Effect.provide(services),
        Effect.mapError((error) => toolResultDecodeError(name, String(error))),
      ) as Effect.Effect<unknown, AiError.AiError>);
};

const decodeParameters = (
  toolkit: { readonly tools: Record<string, Tool.Any> },
  services: Context.Context<never>,
  name: string,
  params: unknown,
): Effect.Effect<unknown, AiError.AiError> => {
  const tool = Object.hasOwn(toolkit.tools, name)
    ? toolkit.tools[name]
    : undefined;
  return tool === undefined
    ? Effect.fail(toolParameterDecodeError(name, 'tool is not defined'))
    : (Schema.decodeUnknownEffect(tool.parametersSchema)(params).pipe(
        Effect.provide(services),
        Effect.mapError((error) =>
          toolParameterDecodeError(name, String(error)),
        ),
      ) as Effect.Effect<unknown, AiError.AiError>);
};

interface ArbitrationState {
  readonly cancelled: boolean;
  readonly dispatches: number;
}

export interface TurnArbitration {
  /** Wait until every dispatch commit is durably settled, then cancel. */
  readonly cancel: Effect.Effect<void>;
  readonly dispatchCommits: Effect.Effect<boolean>;
  readonly settled: Effect.Effect<void>;
}

/** One turn's atomic cancellation/dispatch race. */
export const makeTurnArbitration: Effect.Effect<TurnArbitration> = Effect.gen(
  function* () {
    const state = yield* Ref.make<ArbitrationState>({
      cancelled: false,
      dispatches: 0,
    });
    const changes = yield* PubSub.unbounded<void>();

    const awaitIdle = Effect.scoped(
      Effect.gen(function* () {
        // Subscribe before inspecting state so the last settlement cannot land
        // between the check and the wait.
        const subscription = yield* PubSub.subscribe(changes);
        while ((yield* Ref.get(state)).dispatches > 0) {
          yield* Stream.fromSubscription(subscription).pipe(Stream.runHead);
        }
      }),
    );

    return {
      cancel: Effect.gen(function* () {
        while (true) {
          const won = yield* Ref.modify(state, (current) =>
            current.dispatches === 0
              ? [true, { ...current, cancelled: true }]
              : [false, current],
          );
          if (won) return;
          yield* awaitIdle;
        }
      }),
      dispatchCommits: Ref.modify(state, (current) =>
        current.cancelled
          ? [false, current]
          : [true, { ...current, dispatches: current.dispatches + 1 }],
      ),
      settled: Effect.gen(function* () {
        yield* Ref.update(state, (current) => ({
          ...current,
          dispatches: Math.max(0, current.dispatches - 1),
        }));
        yield* PubSub.publish(changes, undefined);
      }),
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
export const gate = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.Toolkit<Tools>,
  options: GateOptions,
): Effect.Effect<Toolkit.WithHandler<Tools>, never, Tool.HandlersFor<Tools>> =>
  Effect.gen(function* () {
    const session = options.session;
    const interceptor = options.interceptor;
    const resolved = yield* toolkit;

    // Captured the way `Subagent.delegateTo` captures its children's
    // services, and for the same reason: decoding a stored result may need
    // the tool's own decoding services, and `handle`'s signature fixes what
    // its stream is allowed to require. Providing the ambient context inward
    // keeps the requirement off the signature instead of casting it away.
    const services = yield* Effect.context<never>();
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

      const decode: Decode =
        schema === undefined
          ? () =>
              Effect.fail(toolResultDecodeError(name, 'tool is not defined'))
          : (stored: unknown) =>
              // The requirement channel is erased rather than declared. A
              // tool's decoding services are already in the agent's
              // `WithOwnHandlers`, so the caller has provided them and they
              // are in `services` — but `handle`'s signature fixes what its
              // effect may require, and declaring them here would put a
              // requirement on a value `LanguageModel` resolves internally.
              // Capturing and providing them is the same move
              // `Subagent.delegateTo` makes for a child's services.
              Schema.decodeUnknownEffect(schema)(stored).pipe(
                Effect.provide(services),
                Effect.mapError((error) =>
                  toolResultDecodeError(name, String(error)),
                ),
              ) as Effect.Effect<unknown, AiError.AiError>;

      decoders.set(name, decode);
      return decode;
    };

    // Loosely typed on purpose: nothing inside can honour the per-tool
    // relationship between a name and its result type, because a stored
    // result arrives as `unknown` from a `Schema.Unknown` column. The one
    // cast is at the end, and what it asserts — that this handles the same
    // names the wrapped toolkit does — is guaranteed by `tools` being passed
    // straight through.
    const underlying = resolved.handle as unknown as Dispatch;

    /**
     * A result nobody's handler produced, in the two forms a part carries.
     *
     * Shared by the two paths that answer without dispatching, because they
     * differ only in where the encoded value came from. The decoded half goes
     * back through the tool's own codec so a consumer of the live stream reads
     * what its type says; the encoded half is served as-is, because that is
     * the field `Prompt` builds the tool-result message from.
     */
    const answered = (
      name: string,
      encoded: unknown,
      isFailure: boolean,
      requireDecoded: boolean,
    ): Effect.Effect<
      Stream.Stream<Tool.HandlerResult<Tool.Any>>,
      AiError.AiError
    > =>
      Effect.map(
        requireDecoded
          ? decoderFor(name)(encoded)
          : decoderFor(name)(encoded).pipe(
              Effect.catchCause(() => Effect.succeed(encoded)),
            ),
        (decoded) =>
          Stream.make({
            result: decoded,
            encodedResult: encoded,
            isFailure,
            preliminary: false,
            // `LanguageModel` spreads handler results into the emitted part.
            // This discriminant keeps an encoded fallback from masquerading as
            // a schema-decoded tool success in the public event stream.
            resultSource: 'substituted' as const,
          }),
      );

    const handle: Dispatch = (name, params, toolCallId) =>
      Effect.gen(function* () {
        const normalizedToolCallId =
          toolCallId === undefined
            ? undefined
            : LogVocabulary.ToolCallId.make(toolCallId);
        const prior =
          normalizedToolCallId === undefined || session === undefined
            ? Option.none<AgentLog.Recovery>()
            : session.recovery(name, normalizedToolCallId);

        // Step 1. A call an unsettled earlier run already completed is served
        // from the log and goes no further — not to the interceptor, and not
        // to the tool. See the ordering note above.
        if (Option.isSome(prior) && prior.value._tag === 'Settled') {
          return yield* answered(
            name,
            prior.value.result,
            prior.value.outcome === 'failure',
            true,
          );
        }

        if (Option.isSome(prior)) {
          return yield* Effect.fail(
            indeterminateError(name, normalizedToolCallId!),
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
              name,
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
        if (
          options.arbitration !== undefined &&
          !(yield* options.arbitration.dispatchCommits)
        ) {
          // Cancellation owns the turn. The provider stream is being stopped
          // concurrently; parking here prevents a handler from crossing the
          // gate in the small interval before that interruption reaches it.
          return yield* Effect.never;
        }
        if (session !== undefined) {
          if (normalizedToolCallId === undefined) {
            return yield* Effect.fail(missingToolCallIdError(name));
          }
          if (options.arbitration !== undefined) {
            session.onToolSettled(
              name,
              normalizedToolCallId,
              options.arbitration.settled,
            );
          }
          yield* session.append([
            {
              _tag: 'ToolStarted',
              id: normalizedToolCallId,
              name,
            },
          ]);
        }
        const guarded =
          options.runtime === undefined ||
          options.unmeteredToolNames?.has(name) === true
            ? yield* underlying(name, params, toolCallId)
            : options.runtime.toolStream(
                Stream.unwrap(underlying(name, params, toolCallId)),
              );
        return options.arbitration === undefined
          ? guarded
          : guarded.pipe(
              Stream.onExit((exit) =>
                Exit.isFailure(exit)
                  ? options.arbitration!.settled
                  : Effect.void,
              ),
            );
      });

    return {
      tools: resolved.tools,
      handle: handle as unknown as Toolkit.WithHandler<Tools>['handle'],
    };
  });

/** A decoder for one tool's substituted encoded result. */
type Decode = (stored: unknown) => Effect.Effect<unknown, AiError.AiError>;

/** `Toolkit.WithHandler['handle']` with the per-tool types erased. */
type Dispatch = (
  name: string,
  params: unknown,
  toolCallId?: string,
) => Effect.Effect<
  Stream.Stream<Tool.HandlerResult<Tool.Any>, unknown, unknown>,
  AiError.AiError
>;

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

const recoveryCorruptionError = (description: string): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'resolveIndeterminateToolCall',
    reason: new AiError.InvalidRequestError({ description }),
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
