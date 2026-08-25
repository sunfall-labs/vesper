import { Effect, Exit, Layer, Option, Ref, Stream } from 'effect';
import {
  AiError,
  Chat,
  LanguageModel,
  Prompt,
  type Response,
  type Tool,
  type Toolkit,
} from 'effect/unstable/ai';

import type { RunFailure, VisibleTools, WithOwnHandlers } from '../agent.js';
import { CodeMode } from '../code-mode.js';
import { Compaction } from '../compaction.js';
import { ContextWindow } from '../context-window.js';
import { ToolDispatch } from '../dispatch.js';
import type { AgentEvents } from '../event.js';
import { AgentHistory } from '../history.js';
import { DynamicToolkit } from '../dynamic-toolkit.js';
import type { Interception } from '../interception.js';
import { Interaction } from '../interaction.js';
import type * as AgentLog from '../log.js';
import { ResultOverflow } from '../result-overflow.js';
import type { RunPolicy } from '../run-policy.js';
import { RunPolicyRuntime } from '../run-policy-runtime.js';
import type { Stop } from '../stop.js';
import { AgentState } from '../state.js';
import { TurnControl } from '../turn-control.js';
import { CompactionRuntime } from './compaction.js';
import { AgentEventRuntime } from './event.js';
import * as Observability from './observability.js';
import { encodePart } from './part-encoding.js';
import {
  incompleteOutputError,
  interactionRequiresConversationError,
  normalizeProviderError,
} from './provider-error.js';

// The loop, in CONTEXT.md's sense: repeated turns until a stop condition
// holds, which is the piece `effect/unstable/ai` does not have. `agent.ts`
// keeps the public surface — the `Agent.*` types, `make`'s compile step, and
// the method attachment — and threads the run's session and interceptor into
// {@link makeEntry} lexically. Nothing here reads them from the context.

/**
 * Replay a recorded conversation, then follow it live.
 *
 * The live reader behind `Conversation.follow`, independent of an agent
 * a method because reading a conversation needs no agent: a UI reattaching
 * after a refresh, a second pod tailing a run it did not start, and an
 * operator inspecting a finished conversation all want this and none of them
 * has the agent that wrote it.
 *
 * `Tail.from` does the read-then-follow, including the part that is easy to
 * get wrong — a record appended between the catch-up read and the
 * subscription is not lost, because every drain is driven by a wake-up and
 * the backend must emit one as soon as the subscription is live. It never
 * completes on its own; `Stream.take` it, interrupt it, or let its scope
 * close.
 *
 * Records, not `AgentEvents.Event`. Synthesising events from records would
 * mean inventing text deltas that were never sent, and the log deliberately
 * does not store them — see `log.ts` on why text is coalesced. What a
 * resuming reader gets is what actually happened, at the granularity it was
 * written.
 */
/**
 * The two primitive event streams for one session.
 *
 * Split out from `Parts` because they are the part that varies with the
 * session and the rest is not. `withHandlers` composes over an entry;
 * durable recording replaces one.
 */
export interface Entry<
  EntryTools extends Record<string, Tool.Any>,
  EntryRequirements,
  EntryError,
> {
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.Event<EntryTools>,
    EntryError,
    EntryRequirements
  >;
  readonly streamIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.Event<EntryTools>,
    EntryError,
    EntryRequirements
  >;
}

/** Provide one implementation layer across both primitive stream shapes. */
export const provideEntry = <
  EntryTools extends Record<string, Tool.Any>,
  EntryRequirements,
  EntryError,
  Provided,
  LayerError,
  LayerRequires,
>(
  entry: Entry<EntryTools, EntryRequirements, EntryError>,
  layer: Layer.Layer<Provided, LayerError, LayerRequires>,
): Entry<
  EntryTools,
  Exclude<EntryRequirements, Provided> | LayerRequires,
  EntryError | LayerError
> => ({
  stream: (input) => Stream.provide(entry.stream(input), layer),
  streamIn: (chat, input) => Stream.provide(entry.streamIn(chat, input), layer),
});

/**
 * What one run's loop is built around, beyond the definition.
 *
 * Both members are optional capabilities, both are passed rather than looked
 * up, and both have to reach *inside* the loop rather than wrap it from
 * outside — the dispatch seam needs both, the turn boundary needs the session,
 * the provider call needs the interceptor.
 */
export interface Wiring<
  InterceptorRequires = never,
  DynamicTools extends Record<string, Tool.Any> = Record<never, never>,
> {
  readonly session: AgentLog.Session | undefined;
  readonly interceptor:
    | Interception.Interceptor<InterceptorRequires>
    | undefined;
  readonly runtime?: RunPolicyRuntime.Runtime | undefined;
  readonly dynamicToolkit?: Toolkit.WithHandler<DynamicTools> | undefined;
  readonly codeState?: CodeMode.StateHandle | undefined;
  readonly startRun?:
    | ((
        input: Prompt.RawInput,
      ) => Effect.Effect<void, AgentLog.DurabilityError>)
    | undefined;
  readonly initialUsage?: Stop.Usage | undefined;
  readonly lastTurn?: ContextWindow.TurnUsage | undefined;
}

/** A generated capability whose handler layer depends on the run's wiring. */
interface DelegationCapability {
  readonly toolkit: Toolkit.Any;
  readonly layer: (
    session: AgentLog.Session | undefined,
    runtime?: RunPolicyRuntime.Runtime,
  ) => Layer.Layer<never, never, unknown>;
}

/** A generated capability whose handler layer is closure-complete. */
interface StaticCapability {
  readonly toolkit: Toolkit.Any;
  readonly layer: Layer.Layer<never, never, unknown>;
}

/**
 * What the loop captures from `Agent.make`: the definition fields it reads
 * plus the values `make` computes from them once per agent. The generated
 * capability layers are typed by what the loop does with them rather than by
 * their construction — the boundary assertion at the end of {@link makeEntry}
 * names the public requirement channel exactly as it did before the loop
 * moved here.
 */
export interface LoopDefinition<
  RuntimeTools extends Record<string, Tool.Any>,
  DynamicSources extends ReadonlyArray<DynamicToolkit.Any>,
  StopR,
  TurnControlR,
  StateDefinition extends AgentState.AnyDefinition | undefined,
  CodeModeOption extends boolean | { readonly except: ReadonlyArray<string> },
> {
  readonly name: string;
  readonly concurrency: number | 'unbounded' | undefined;
  readonly codeMode: CodeModeOption | undefined;
  readonly resultOverflow: ResultOverflow.Policy | undefined;
  readonly state: StateDefinition | undefined;
  readonly dynamicTools: DynamicSources | undefined;
  readonly instructions: string;
  readonly toolkit: Toolkit.Toolkit<RuntimeTools>;
  readonly delegation: DelegationCapability | undefined;
  readonly delegationToolNames: ReadonlySet<string>;
  readonly loader: StaticCapability | undefined;
  readonly overflow: StaticCapability | undefined;
  readonly compaction: Compaction.Policy | undefined;
  readonly compactionWarning: Effect.Effect<void>;
  readonly stopWhen: Stop.StopCondition<
    CodeMode.ModelTools<
      VisibleTools<RuntimeTools, DynamicToolkit.Tools<DynamicSources>>,
      CodeModeOption
    >,
    StopR
  >;
  readonly nextTurn:
    | TurnControl.Policy<
        CodeMode.ModelTools<
          VisibleTools<RuntimeTools, DynamicToolkit.Tools<DynamicSources>>,
          CodeModeOption
        >,
        TurnControlR
      >
    | undefined;
  readonly runPolicy: RunPolicy.Limits;
}

// Everything below is built per run wiring rather than once per agent, which
// is the shape Phases 5, 6 and 7 all needed. Four things vary with it: the
// toolkit a turn dispatches through, whether the loop drains signals,
// whether delegation opens child sessions, and who gets a say at the seams.
// A closure over `wiring` is how that stays lexical — both the session and
// the interceptor are passed in, not looked up, so nothing here depends on
// what happens to be in the context.
export const makeEntry = <
  RuntimeTools extends Record<string, Tool.Any>,
  DynamicSources extends ReadonlyArray<DynamicToolkit.Any>,
  BaseRequires,
  StopR,
  TurnControlR,
  StateDefinition extends AgentState.AnyDefinition | undefined,
  CodeModeOption extends boolean | { readonly except: ReadonlyArray<string> },
>(
  definition: LoopDefinition<
    RuntimeTools,
    DynamicSources,
    StopR,
    TurnControlR,
    StateDefinition,
    CodeModeOption
  >,
) => {
  type DynamicTools = DynamicToolkit.Tools<DynamicSources>;
  type RunTools = VisibleTools<RuntimeTools, DynamicTools>;
  type ModelTools = CodeMode.ModelTools<RunTools, CodeModeOption>;

  const {
    compaction,
    compactionWarning,
    delegation,
    delegationToolNames,
    instructions,
    loader,
    nextTurn,
    overflow,
    runPolicy,
    stopWhen,
    toolkit,
  } = definition;

  const entryFor = <InterceptorR>(
    wiring: Wiring<InterceptorR, DynamicTools>,
  ): Entry<ModelTools, BaseRequires | InterceptorR, RunFailure> => {
    const session = wiring.session;
    const interceptor = wiring.interceptor;
    const runtime = wiring.runtime;
    const runInstructions = dynamicContextFor(
      instructions,
      wiring.dynamicToolkit,
    );
    // Oversized results are spilled before the log or the interceptor ever
    // see them, so `gate`'s recording and `resolveIndeterminate`'s recovery
    // — both consumers of this same `runToolkit` — see only the pointer, not
    // the payload it stands in for. See `result-overflow.ts` for why a
    // storage failure here is a defect rather than a typed tool failure.
    const runToolkit = ResultOverflow.wrap(
      definition.resultOverflow,
      Effect.map(toolkit, (staticallyDefined) =>
        withDynamicToolkit(staticallyDefined, wiring.dynamicToolkit),
      ),
    );

    // A `Toolkit` already *is* an `Effect` producing a resolved toolkit, and
    // `streamText`'s `toolkit` option takes either form, so both branches have
    // one type and `LanguageModel` resolves them identically. That is the whole
    // reason the dispatch seam needs no change to the `LanguageModel` contract.
    //
    // An agent with no dispatch seams takes the first branch. A root runtime
    // is also a seam because its tool semaphore is shared by descendants.
    const dispatching = (arbitration: ToolDispatch.TurnArbitration) =>
      CodeMode.selectToolkit(
        definition.codeMode,
        () =>
          ToolDispatch.gate(runToolkit, {
            agent: definition.name,
            session,
            interceptor,
            runtime,
            unmeteredToolNames: delegationToolNames,
            arbitration,
          }),
        (except) =>
          Effect.gen(function* () {
            const { hidden, excepted } = CodeMode.split(
              yield* runToolkit,
              except,
            );
            const hiddenGated = yield* ToolDispatch.gate(
              Effect.succeed(hidden),
              {
                agent: definition.name,
                conversationId: session?.conversationId,
                interceptor,
                runtime,
                unmeteredToolNames: delegationToolNames,
                arbitration,
              },
            );
            const visible = yield* CodeMode.toolkit(
              hiddenGated,
              wiring.codeState ?? CodeMode.emptyState,
            );
            const visibleGated = yield* ToolDispatch.gate(
              Effect.succeed(visible),
              {
                agent: definition.name,
                session,
                arbitration,
              },
            );
            // An excepted tool is an ordinary advertised tool: it takes the
            // same full gate the direct branch applies — durable session
            // recording, interception, run-policy metering — while `exec`
            // keeps its narrower session-only gate, because its nested calls
            // were already intercepted and metered at the hidden layer. With
            // nothing excepted the merge is with an empty half, which is
            // today's pure-exec toolkit.
            const exceptedGated = yield* ToolDispatch.gate(
              Effect.succeed(excepted),
              {
                agent: definition.name,
                session,
                interceptor,
                runtime,
                unmeteredToolNames: delegationToolNames,
                arbitration,
              },
            );
            return CodeMode.merge<
              RunTools,
              CodeMode.Except<CodeModeOption> & keyof RunTools & string
            >(visibleGated, exceptedGated);
          }),
      );

    // Fixed arity rather than spreading `...(x ? [l] : [])`: the spread form
    // widens `mergeAll`'s result, which then leaks into anything that provides
    // this layer.
    const generated = Layer.mergeAll(
      delegation === undefined
        ? Layer.empty
        : delegation.layer(session, runtime),
      loader === undefined ? Layer.empty : loader.layer,
      overflow === undefined ? Layer.empty : overflow.layer,
    );
    const layer =
      definition.state === undefined
        ? generated
        : Layer.merge(
            generated,
            Layer.effect(
              definition.state,
              AgentState.open(definition.state, session).pipe(
                Effect.mapError(
                  (error) =>
                    new AiError.AiError({
                      module: 'AgentState',
                      method: 'open',
                      reason: new AiError.InvalidRequestError({
                        description: error.message,
                        metadata: {
                          vesper: stateErrorMetadata(error),
                        },
                      }),
                    }),
                ),
              ),
            ),
          );

    /**
     * The provider call, and the seam that sits in front of it.
     *
     * Factored out because a turn makes this call in two places — once, and
     * once more if compaction retried it — and duplicating the fold that
     * observes its parts is how the two would drift. It is also the only place
     * `beforeModelCall` needs to be, which is the point of naming the seam:
     * "before the provider is called" is a position in the code, not a
     * description of one.
     */
    const askModel = (
      chat: Chat.Service,
      seen: TurnState,
      step: number,
      input: Prompt.RawInput,
      attempt: Interception.Attempt,
      arbitration: ToolDispatch.TurnArbitration,
    ): Stream.Stream<
      AgentEvents.Event<ModelTools>,
      RunFailure,
      WithOwnHandlers<RuntimeTools> | InterceptorR
    > =>
      Stream.unwrap(
        Effect.gen(function* () {
          // Chat exposes decoded parts so tool handlers receive their typed
          // parameters. Resolve the same toolkit here to encode each part
          // back to the provider representation before it reaches observers
          // or the durable recording sink.
          const resolvedToolkit = yield* dispatching(arbitration);
          if (interceptor?.beforeModelCall !== undefined) {
            yield* interceptor.beforeModelCall({
              agent: definition.name,
              conversationId: session?.conversationId,
              step,
              attempt,
            });
          }
          if (runtime !== undefined) {
            yield* runtime.modelCall;
          }
          // Count attempts at the provider boundary, including calls that
          // fail before producing a part. Token counters are updated only
          // once a provider finish part reports usage below.
          yield* Observability.modelCall;
          const remaining =
            runtime === undefined ? undefined : yield* runtime.remainingMillis;
          const asked = chat
            .streamText({
              prompt: input,
              toolkit: resolvedToolkit,
              concurrency: definition.concurrency,
            })
            .pipe(
              // `part` is reasserted to the same `Response.StreamPart<RunTools>`
              // at each of the three points below that need it, rather than
              // cast once and reused. Effect's mapped tool-part union is not
              // idempotent under this compiled intersection: TypeScript's
              // control-flow narrowing (excluding the `'error'` member below)
              // produces a structural type this generic union no longer
              // recognises as itself, so a single upstream cast stops
              // type-checking at exactly the two downstream call sites that
              // need the full, unnarrowed union again. Reasserting the same
              // target type at each site is what the toolkit value passed
              // alongside it already guarantees at runtime; nothing here
              // asserts a *different* type than the one Chat actually decoded
              // the part against.
              Stream.mapEffect((part) =>
                part.type === 'error'
                  ? Effect.fail(
                      normalizeProviderError(part.error, part.metadata),
                    )
                  : encodePart(
                      part as Response.StreamPart<ModelTools>,
                      resolvedToolkit,
                    ).pipe(
                      Effect.map((encodedPart) => ({ part, encodedPart })),
                    ),
              ),
              Stream.tap(({ part, encodedPart }) =>
                Effect.gen(function* () {
                  seen.started = true;
                  observe(
                    seen,
                    part as Response.StreamPart<RunTools>,
                    encodedPart,
                    resolvedToolkit,
                  );
                  if (encodedPart.type === 'finish') {
                    yield* Observability.usage(encodedPart.usage);
                  }
                  if (runtime !== undefined) {
                    yield* runtime.remainingMillis;
                  }
                }),
              ),
              Stream.map(
                ({ part, encodedPart }): AgentEvents.Event<ModelTools> => {
                  const interaction =
                    encodedPart.type === 'tool-approval-request'
                      ? seen.callsById.get(encodedPart.toolCallId)?.interaction
                      : undefined;
                  return {
                    _tag: 'Part',
                    step,
                    // Effect's mapped tool-part union is not idempotent under
                    // this compiled intersection, although the toolkit value is
                    // exactly the one Chat used to decode the part.
                    part: part as Response.StreamPart<ModelTools>,
                    encodedPart,
                    ...(interaction === undefined ? {} : { interaction }),
                  };
                },
              ),
            );
          const model =
            remaining === undefined
              ? asked
              : asked.pipe(
                  Stream.timeoutOrElse({
                    duration: remaining,
                    orElse: () =>
                      Stream.fail(
                        RunPolicyRuntime.error({
                          limit: 'deadline',
                          used: runPolicy.wallClockMillis,
                          maximum: runPolicy.wallClockMillis,
                        }),
                      ),
                  }),
                );
          return model.pipe(
            Stream.withSpan('Agent.model', {
              attributes: {
                'vesper.agent.step': step,
                'vesper.model.attempt': attempt,
                ...(session === undefined
                  ? {}
                  : { 'vesper.conversation.id': session.conversationId }),
              },
            }),
          );
        }),
      ) as Stream.Stream<
        AgentEvents.Event<ModelTools>,
        RunFailure,
        WithOwnHandlers<RuntimeTools>
      >;

    /**
     * The seam in front of a turn, resolved to the input the turn will use.
     *
     * Runs before `TurnStarted`, so a turn an interceptor refused produces no
     * boundary events for a turn that never happened.
     */
    const openTurn = (
      step: number,
      totals: Stop.Usage,
      input: Prompt.RawInput,
    ): Effect.Effect<Prompt.RawInput, RunFailure, InterceptorR> =>
      interceptor?.beforeTurn === undefined
        ? Effect.succeed(input)
        : Effect.map(
            interceptor.beforeTurn({
              agent: definition.name,
              conversationId: session?.conversationId,
              step,
              usage: totals,
              input: Prompt.make(input),
            }),
            (decision) =>
              decision._tag === 'Proceed' ? input : decision.input,
          );

    /**
     * The proactive trigger: compact before a turn that would not have fit.
     *
     * Silent unless the caller told us the model's context window, because
     * without one there is no threshold to compare against — the loop targets
     * the `LanguageModel` tag, and that tag does not carry a window. When
     * there is one, this is strictly cheaper than the reactive path below: an
     * overflow costs a rejected request and a re-run of the turn, and this
     * costs nothing the compaction itself did not already cost.
     *
     * The estimate is anchored on the previous turn's reported usage, which is
     * the difference between knowing what the provider counted and guessing at
     * it. `input` is folded in because `streamText` is about to append it and
     * the question is whether the *next* request fits, not the last one.
     */
    const compactAhead = (
      chat: Chat.Service,
      lastTurn: Ref.Ref<ContextWindow.TurnUsage | undefined>,
      input: Prompt.RawInput,
    ): Effect.Effect<
      CompactionRuntime.Summarized | undefined,
      RunFailure,
      LanguageModel.LanguageModel
    > =>
      Effect.gen(function* () {
        if (compaction?.contextWindow === undefined) {
          return undefined;
        }

        const history = yield* Ref.get(chat.history);
        const effectivePrompt = Prompt.concat(history, Prompt.make(input));
        const over = yield* CompactionRuntime.shouldCompact(
          effectivePrompt,
          compaction.contextWindow,
          compaction,
          yield* Ref.get(lastTurn),
        );

        if (!over) {
          return undefined;
        }
        // Compact the same prompt the estimator measured. In a recorded run,
        // `RunStarted` has already persisted this turn's input; leaving it out
        // of the live Chat makes durable reconstruction one message longer
        // than the compaction split. The input stays in the compacted history,
        // so the provider call below uses `Prompt.empty` rather than appending
        // it a second time.
        yield* Ref.set(chat.history, effectivePrompt);
        const summarized = yield* compactWithBudget(chat, compaction).pipe(
          Effect.onError(() => Ref.set(chat.history, history)),
        );
        if (summarized === undefined) {
          // `compact` can decline when there is no old history to summarize.
          // Restore the original shape so the ordinary provider call remains
          // responsible for appending this turn's input.
          yield* Ref.set(chat.history, history);
          return undefined;
        }
        if (runtime !== undefined) {
          yield* runtime.addUsage(summarized.usage);
        }
        return summarized;
      });

    const compactWithBudget = (
      chat: Chat.Service,
      policy: Compaction.Policy,
    ): Effect.Effect<
      CompactionRuntime.Summarized | undefined,
      RunFailure,
      LanguageModel.LanguageModel
    > =>
      Effect.gen(function* () {
        if (runtime === undefined) {
          yield* Observability.modelCall;
          return yield* CompactionRuntime.compact(chat, policy);
        }
        yield* runtime.modelCall;
        yield* Observability.modelCall;
        const remaining = yield* runtime.remainingMillis;
        return yield* Effect.timeoutOrElse(
          CompactionRuntime.compact(chat, policy),
          {
            duration: remaining,
            orElse: () =>
              Effect.fail(
                RunPolicyRuntime.error({
                  limit: 'deadline',
                  used: runPolicy.wallClockMillis,
                  maximum: runPolicy.wallClockMillis,
                }),
              ),
          },
        );
      });

    const turn = (
      chat: Chat.Service,
      usage: Ref.Ref<Stop.Usage>,
      toolCallCounts: Ref.Ref<Readonly<Record<string, number>>>,
      lastTurn: Ref.Ref<ContextWindow.TurnUsage | undefined>,
      step: number,
      pending: Prompt.RawInput,
    ): Stream.Stream<
      AgentEvents.Event<ModelTools>,
      RunFailure,
      WithOwnHandlers<RuntimeTools> | StopR | TurnControlR | InterceptorR
    > =>
      Stream.unwrap(
        Effect.gen(function* () {
          const input = yield* openTurn(
            step,
            yield* Ref.get(usage),
            pending,
          ).pipe(
            Effect.catch((error) =>
              step === 1 && wiring.startRun !== undefined
                ? wiring
                    .startRun(pending)
                    .pipe(Effect.andThen(Effect.fail(error)))
                : Effect.fail(error),
            ),
          );
          if (step === 1 && wiring.startRun !== undefined) {
            yield* wiring.startRun(input);
          }
          if (runtime !== undefined) {
            yield* runtime.turn;
          }

          // Before the request, not after it is refused. Announced the same
          // way the reactive rewrite is, because from the log's point of view
          // the two are the same event: history was replaced by a summary, and
          // a reader rebuilding this conversation has to know that.
          const ahead = yield* compactAhead(chat, lastTurn, input);
          if (ahead !== undefined) {
            yield* Ref.update(usage, (current) =>
              addStopUsage(current, ahead.usage),
            );
          }
          const modelInput: Prompt.RawInput =
            ahead === undefined ? input : Prompt.empty;

          // A turn's outcome is only knowable once its parts have gone by, but
          // the stop decision needs it. Accumulating through a `tap` while the
          // parts stream past is what lets a consumer see tokens live and
          // still have the decision made on complete information.
          //
          // Only what the stop condition and the result actually read is kept:
          // text, tool calls, usage. Rebuilding whole content parts would retain
          // data nothing here consumes.
          const initial = emptyTurnState();
          const active = yield* Ref.make(initial);
          const arbitration = yield* ToolDispatch.makeTurnArbitration;

          // The conversation as it stood before this turn was attempted, kept
          // for the reactive path below. See the `catchIf` there for why.
          const historyBefore = yield* Ref.get(chat.history);

          const parts = askModel(
            chat,
            initial,
            step,
            modelInput,
            'initial',
            arbitration,
          );

          const guarded =
            compaction === undefined
              ? parts
              : parts.pipe(
                  // Reactive compaction: the turn is retried once against a
                  // compacted history when the provider says the prompt no
                  // longer fits. Wrapping the stream rather than the loop
                  // keeps the retry scoped to the call that overflowed.
                  //
                  // The rewrite is announced before the retry rather than
                  // swallowed. It used to produce nothing observable, which
                  // made it the one thing a run did that its own log could not
                  // describe: `Compacted` had no producer, so a resumed
                  // conversation was rebuilt from records compaction had
                  // already replaced and compacted again on its first turn. A
                  // no-op compaction announces nothing, because nothing was
                  // replaced.
                  Stream.catchIf(
                    (error) =>
                      AiError.isAiError(error) &&
                      Compaction.isContextOverflow(error),
                    (error) =>
                      Stream.unwrap(
                        Effect.gen(function* () {
                          // Once a part escaped, retrying can duplicate visible
                          // text or tool side effects. Provider retries belong
                          // below this seam; compaction is safe only for a request
                          // rejected before it produced anything.
                          if (initial.emitted) {
                            return Stream.fail(error);
                          }

                          // State the history the summarizer should see rather
                          // than inherit whatever the rejected turn left behind.
                          //
                          // It is the conversation as it was, plus the input this
                          // turn was rejected for — the request the provider
                          // refused, which is exactly what has to be made to fit.
                          // Including the input matters: it is the newest message,
                          // so `splitAt` protects it as the recent tail and
                          // summarizes the history behind it, which is the only
                          // split that can shrink a request whose bulk *is* the
                          // input.
                          //
                          // `Chat.streamText` happens to leave something close to
                          // this behind — it writes `history + input` from a
                          // finalizer that runs on failure too — but that is one
                          // implementation's detail and the retry below depends
                          // on it being exactly right, so it is not assumed.
                          yield* Ref.set(
                            chat.history,
                            Prompt.concat(
                              historyBefore,
                              Prompt.make(modelInput),
                            ),
                          );
                          const summarized = yield* compactWithBudget(
                            chat,
                            compaction,
                          );
                          // Retrying an unchanged prompt repeats the same refusal
                          // and can loop provider-side work for no gain.
                          if (summarized === undefined) {
                            return Stream.fail(error);
                          }
                          if (runtime !== undefined) {
                            yield* runtime.addUsage(summarized.usage);
                          }
                          yield* Ref.update(usage, (current) =>
                            addStopUsage(current, summarized.usage),
                          );

                          const retry = emptyTurnState();
                          yield* Ref.set(active, retry);
                          // Empty, not `input`. The input is in the history above
                          // and `Chat.streamText` appends whatever it is given.
                          const retried = askModel(
                            chat,
                            retry,
                            step,
                            Prompt.empty,
                            'after-compaction',
                            arbitration,
                          );
                          return Stream.concat(
                            Stream.make(
                              AgentEventRuntime.compacted(step, summarized),
                            ),
                            retried,
                          );
                        }),
                      ),
                  ),
                );

          const decide = Stream.unwrap(
            Effect.gen(function* () {
              const seen = yield* Ref.get(active);
              const totals = yield* Ref.updateAndGet(usage, (current) =>
                addUsage(current, seen.usage),
              );
              const toolCallTotals = yield* Ref.updateAndGet(
                toolCallCounts,
                (current) => addToolCallCounts(current, seen.toolCalls),
              );
              if (runtime !== undefined && seen.usage !== undefined) {
                const accounted = yield* Effect.exit(
                  runtime.addUsage({
                    input: seen.usage.inputTokens.total ?? 0,
                    output: seen.usage.outputTokens.total ?? 0,
                  }),
                );
                if (Exit.isFailure(accounted)) {
                  return Stream.concat(
                    Stream.make(AgentEventRuntime.turnFinished(step, totals)),
                    Stream.failCause(accounted.cause),
                  );
                }
              }

              // This turn's own figures, kept apart from the running totals.
              // The estimator needs what the provider counted for *one*
              // request; summing turns would count the same prompt once per
              // turn and diverge without bound. A turn that reported nothing
              // leaves the previous anchor in place rather than clearing it —
              // a stale anchor understates the trailing text by one turn,
              // while no anchor throws the whole conversation back to a
              // character count.
              if (seen.usage !== undefined) {
                yield* Ref.set(
                  lastTurn,
                  ContextWindow.usageFromTurn(seen.usage),
                );
              }

              const incomplete =
                seen.finishReason === undefined
                  ? undefined
                  : incompleteOutputError('streamText', seen.finishReason);
              if (incomplete !== undefined) {
                return Stream.concat(
                  Stream.make(AgentEventRuntime.turnFinished(step, totals)),
                  Stream.fail(incomplete),
                );
              }

              // The turn boundary is where out-of-band input lands. Draining
              // here rather than racing it against the provider stream is what
              // keeps a steer from arriving in the middle of a tool call, and
              // the drain itself is one bounded read from a cursor — see
              // `signal.ts` on why anything resembling a blocking receive is
              // the wrong primitive for this.
              const drained: AgentLog.SignalDrain =
                session === undefined
                  ? { signals: [], backlog: false }
                  : yield* session.drainSignalsBounded(
                      runtime?.limits.maxSignalsPerBoundary ??
                        runPolicy.maxSignalsPerBoundary,
                    );
              type Decision = RunPolicyRuntime.SignalDecision & {
                readonly signal: AgentLog.Delivered;
              };
              const decisions = yield* Effect.forEach(
                drained.signals,
                (signal, index): Effect.Effect<Decision> =>
                  runtime === undefined
                    ? Effect.succeed({ signal, accepted: true, bytes: 0 })
                    : Effect.map(
                        runtime.signal(signal.kind, signal.text, index),
                        (decision) => ({ signal, ...decision }),
                      ),
              );
              const delivered = decisions.flatMap((decision) =>
                decision.accepted ? [decision.signal] : [],
              );

              const cancelled = delivered.some(
                (signal) => signal.kind === 'cancel',
              );
              const steers = delivered.filter(
                (signal) => signal.kind === 'steer',
              );

              const announced = Stream.concat(
                Stream.fromIterable(
                  decisions
                    .map((decision) =>
                      decision.accepted
                        ? AgentEventRuntime.signalled(step, decision.signal)
                        : AgentEventRuntime.signalRejected(
                            step,
                            decision.signal,
                            decision.exhaustion,
                          ),
                    )
                    .concat(
                      drained.backlog
                        ? [
                            AgentEventRuntime.signalBacklog(
                              step,
                              runtime?.limits.maxSignalsPerBoundary ??
                                runPolicy.maxSignalsPerBoundary,
                            ),
                          ]
                        : [],
                    ),
                ),
                Stream.make(AgentEventRuntime.turnFinished(step, totals)),
              );

              const response = outputOf(seen);
              const wanted = yield* stopWhen({
                step,
                toolCalls: seen.toolCalls,
                response,
                toolResults: seen.toolResults,
                finishReason: seen.finishReason,
                text: seen.text,
                reasoning: seen.reasoning,
                usage: totals,
                toolCallCounts: toolCallTotals,
              });

              // A tool this turn called requires approval that is not yet
              // durably decided. Nothing productive can follow: the model
              // cannot be asked again with an unanswered tool call in its own
              // last turn, so this outranks a steer exactly like a cancel
              // does. Unlike a cancel, it needs somewhere durable to resolve
              // from — an unrecorded run has nowhere to record the decision
              // this would wait on, so it fails outright instead of
              // returning a `Result` nothing can ever act on.
              const pendingInteractions = seen.pendingInteractions;
              if (pendingInteractions.length > 0 && session === undefined) {
                return Stream.fail(
                  interactionRequiresConversationError(pendingInteractions),
                );
              }

              const prepared =
                cancelled ||
                pendingInteractions.length > 0 ||
                nextTurn === undefined
                  ? TurnControl.keep
                  : yield* nextTurn({
                      step,
                      toolCalls: seen.toolCalls,
                      response,
                      toolResults: seen.toolResults,
                      finishReason: seen.finishReason,
                      text: seen.text,
                      reasoning: seen.reasoning,
                      usage: totals,
                      toolCallCounts: toolCallTotals,
                      wouldStop: wanted,
                    });

              const policyWantsStop = Option.isNone(prepared) && wanted;

              // A steer, or a signal backlog this boundary could not fully
              // drain, outranks the stop condition for one more turn,
              // including a step ceiling — `Stop.maxSteps` is not a hard
              // ceiling once a run takes signal traffic; `runPolicy.maxTurns`
              // is. The ceiling is a runaway-loop guard and a steer is a
              // person asking for more work; stopping anyway would consume
              // the instruction and ignore it, which is the one outcome
              // nobody can debug. A backlog is the same shape: it means more
              // signals are already waiting, so stopping now would drop them
              // on the floor rather than let the next boundary see them. A
              // cancel outranks everything.
              const stop =
                cancelled ||
                pendingInteractions.length > 0 ||
                (policyWantsStop && steers.length === 0 && !drained.backlog);
              const completedSteps = seen.started ? step : step - 1;

              return stop
                ? Stream.concat(
                    announced,
                    Stream.make(
                      cancelled
                        ? AgentEventRuntime.completed(
                            seen.text,
                            completedSteps,
                            totals,
                            'cancelled',
                            seen.started ? response : undefined,
                          )
                        : pendingInteractions.length > 0
                          ? AgentEventRuntime.suspended(
                              completedSteps,
                              seen.text,
                              totals,
                              pendingInteractions,
                              seen.started ? response : undefined,
                            )
                          : AgentEventRuntime.completed(
                              seen.text,
                              completedSteps,
                              totals,
                              'success',
                              response,
                            ),
                    ),
                  )
                : Stream.concat(
                    announced,
                    // Later turns continue the stored conversation; the tool
                    // results `streamText` appended are already in history, so
                    // nothing new is supplied unless a steer arrived.
                    continueTurn(
                      prepared,
                      turn(
                        chat,
                        usage,
                        toolCallCounts,
                        lastTurn,
                        step + 1,
                        continuationInput(prepared, steers),
                      ),
                    ),
                  );
            }),
          );

          const responsiveCancel =
            session === undefined
              ? undefined
              : session
                  .signalPages(
                    runtime?.limits.maxSignalsPerBoundary ??
                      runPolicy.maxSignalsPerBoundary,
                  )
                  .pipe(
                    Stream.flatMap((page) =>
                      Stream.fromIterable(
                        page.signals.map((signal, index) => ({
                          signal,
                          index,
                        })),
                      ),
                    ),
                    Stream.filter(({ signal }) => signal.kind === 'cancel'),
                    Stream.filter(({ signal, index }) =>
                      RunPolicyRuntime.acceptsCancel(
                        runtime?.limits ?? runPolicy,
                        signal.text,
                        index,
                      ),
                    ),
                    Stream.tap(() => arbitration.cancel),
                    Stream.runHead,
                    Effect.flatMap((cancel) =>
                      Option.isSome(cancel) ? Effect.void : Effect.never,
                    ),
                    Effect.catch((error) =>
                      Effect.logError(
                        'Agent responsive cancel watcher failed; cancellation remains available at the next turn boundary',
                        error,
                      ).pipe(
                        Effect.annotateLogs({
                          'vesper.component': 'agent',
                          'vesper.event': 'responsive_cancel_watcher_failure',
                        }),
                        Effect.andThen(Effect.never),
                      ),
                    ),
                  );

          const opened =
            ahead === undefined
              ? Stream.make(AgentEventRuntime.turnStarted(step))
              : Stream.make(
                  AgentEventRuntime.turnStarted(step),
                  AgentEventRuntime.compacted(step, ahead),
                );
          const cancellable =
            responsiveCancel === undefined
              ? guarded
              : guarded.pipe(Stream.interruptWhen(responsiveCancel));

          return opened.pipe(
            Stream.concat(cancellable),
            Stream.concat(decide),
            Stream.withSpan('Agent.turn', {
              attributes: {
                'vesper.agent.step': step,
                ...(session === undefined
                  ? {}
                  : { 'vesper.conversation.id': session.conversationId }),
              },
            }),
          );
        }),
      ) as Stream.Stream<
        AgentEvents.Event<ModelTools>,
        RunFailure,
        WithOwnHandlers<RuntimeTools> | StopR | TurnControlR | InterceptorR
      >;

    const streamIn = (chat: Chat.Service, input: Prompt.RawInput) =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (
            CodeMode.isEnabled(definition.codeMode) &&
            wiring.codeState === undefined
          ) {
            const codeState = yield* CodeMode.openState(session).pipe(
              Effect.mapError(
                (error) =>
                  new AiError.AiError({
                    module: 'CodeMode',
                    method: 'openState',
                    reason: new AiError.InvalidRequestError({
                      description: error.message,
                    }),
                  }),
              ),
            );
            return entryFor({ ...wiring, codeState }).streamIn(
              chat,
              input,
            ) as Stream.Stream<
              AgentEvents.Event<ModelTools>,
              RunFailure,
              | WithOwnHandlers<RuntimeTools>
              | StopR
              | TurnControlR
              | InterceptorR
            >;
          }
          if (
            wiring.dynamicToolkit === undefined &&
            definition.dynamicTools !== undefined &&
            definition.dynamicTools.length > 0
          ) {
            const dynamicToolkit = yield* DynamicToolkit.open(
              definition.dynamicTools,
            );
            const nextWiring = { ...wiring, dynamicToolkit };
            yield* replaceSystemInstructions(
              chat,
              dynamicContextFor(instructions, dynamicToolkit),
            );
            return entryFor(nextWiring).streamIn(chat, input) as Stream.Stream<
              AgentEvents.Event<ModelTools>,
              RunFailure,
              | WithOwnHandlers<RuntimeTools>
              | StopR
              | TurnControlR
              | InterceptorR
            >;
          }
          if (runtime === undefined) {
            const root = yield* RunPolicyRuntime.create(runPolicy);
            return entryFor({ ...wiring, runtime: root }).streamIn(
              chat,
              input,
            ) as Stream.Stream<
              AgentEvents.Event<ModelTools>,
              RunFailure,
              | WithOwnHandlers<RuntimeTools>
              | StopR
              | TurnControlR
              | InterceptorR
            >;
          }
          if (session !== undefined && (yield* session.hasPendingToolCalls)) {
            const drained = yield* session.drainSignalsBounded(
              runtime.limits.maxSignalsPerBoundary,
            );
            const decisions = yield* Effect.forEach(
              drained.signals,
              (signal, index) =>
                Effect.map(
                  runtime.signal(signal.kind, signal.text, index),
                  (decision) => ({ signal, ...decision }),
                ),
            );
            const delivered = decisions.flatMap((decision) =>
              decision.accepted ? [decision.signal] : [],
            );
            const steers = delivered.filter(
              (signal) => signal.kind === 'steer',
            );
            const effective =
              steers.length === 0
                ? input
                : Prompt.concat(
                    Prompt.make(input),
                    Prompt.make(steeringInput(steers)),
                  );
            const announced = Stream.fromIterable(
              decisions
                .map((decision) =>
                  decision.accepted
                    ? AgentEventRuntime.signalled(0, decision.signal)
                    : AgentEventRuntime.signalRejected(
                        0,
                        decision.signal,
                        decision.exhaustion,
                      ),
                )
                .concat(
                  drained.backlog
                    ? [
                        AgentEventRuntime.signalBacklog(
                          0,
                          runtime.limits.maxSignalsPerBoundary,
                        ),
                      ]
                    : [],
                ),
            );
            if (delivered.some((signal) => signal.kind === 'cancel')) {
              if (wiring.startRun !== undefined) {
                yield* wiring.startRun(effective);
              }
              return Stream.concat(
                announced,
                Stream.make(
                  AgentEventRuntime.completed(
                    '',
                    0,
                    wiring.initialUsage ?? { input: 0, output: 0 },
                    'cancelled',
                  ),
                ),
              );
            }

            const arbitration = yield* ToolDispatch.makeTurnArbitration;
            const cancelObserved = yield* Ref.make(false);
            const cancelDuringRecovery = session
              .signalPages(runtime.limits.maxSignalsPerBoundary)
              .pipe(
                Stream.flatMap((page) =>
                  Stream.fromIterable(
                    page.signals.map((signal, index) => ({ signal, index })),
                  ),
                ),
                Stream.filter(({ signal }) => signal.kind === 'cancel'),
                Stream.filter(({ signal, index }) =>
                  RunPolicyRuntime.acceptsCancel(
                    runtime.limits,
                    signal.text,
                    index,
                  ),
                ),
                Stream.tap(() => Ref.set(cancelObserved, true)),
                Stream.tap(() => arbitration.cancel),
                Stream.runHead,
                Effect.flatMap((cancel) =>
                  Option.isSome(cancel) ? Effect.void : Effect.never,
                ),
                Effect.catch((error) =>
                  Effect.logError(
                    'Agent recovery cancel watcher failed; cancellation remains available at the next boundary',
                    error,
                  ).pipe(
                    Effect.annotateLogs({
                      'vesper.component': 'agent',
                      'vesper.event': 'responsive_cancel_watcher_failure',
                    }),
                    Effect.andThen(Effect.never),
                  ),
                ),
              );

            return Stream.concat(
              announced,
              Stream.unwrap(
                Effect.gen(function* () {
                  const remaining = yield* runtime.remainingMillis;
                  const recovery = ToolDispatch.resolveIndeterminate(
                    runToolkit,
                    {
                      agent: definition.name,
                      session,
                      interceptor,
                      runtime,
                      unmeteredToolNames: delegationToolNames,
                      arbitration,
                    },
                  ).pipe(
                    Effect.timeoutOrElse({
                      duration: remaining,
                      orElse: () =>
                        Effect.fail(
                          RunPolicyRuntime.error({
                            limit: 'deadline',
                            used: runtime.limits.wallClockMillis,
                            maximum: runtime.limits.wallClockMillis,
                          }),
                        ),
                    }),
                  );
                  yield* Effect.raceFirst(recovery, cancelDuringRecovery);
                  if (yield* Ref.get(cancelObserved)) {
                    const after = yield* session.drainSignalsBounded(
                      runtime.limits.maxSignalsPerBoundary,
                    );
                    const afterDecisions = yield* Effect.forEach(
                      after.signals,
                      (signal, index) =>
                        Effect.map(
                          runtime.signal(signal.kind, signal.text, index),
                          (decision) => ({ signal, ...decision }),
                        ),
                    );
                    const afterSteers = afterDecisions
                      .filter(
                        (decision) =>
                          decision.accepted && decision.signal.kind === 'steer',
                      )
                      .map((decision) => decision.signal);
                    const cancelledInput =
                      afterSteers.length === 0
                        ? effective
                        : Prompt.concat(
                            Prompt.make(effective),
                            Prompt.make(steeringInput(afterSteers)),
                          );
                    if (wiring.startRun !== undefined) {
                      yield* wiring.startRun(cancelledInput);
                    }
                    return Stream.concat(
                      Stream.fromIterable(
                        afterDecisions.map((decision) =>
                          decision.accepted
                            ? AgentEventRuntime.signalled(0, decision.signal)
                            : AgentEventRuntime.signalRejected(
                                0,
                                decision.signal,
                                decision.exhaustion,
                              ),
                        ),
                      ),
                      Stream.make(
                        AgentEventRuntime.completed(
                          '',
                          0,
                          wiring.initialUsage ?? { input: 0, output: 0 },
                          'cancelled',
                        ),
                      ),
                    );
                  }

                  // `resolveIndeterminate` settles every durable approval this
                  // session already has a decision for, but it does not — and
                  // must not — invent one for a call still waiting on
                  // `Conversation.resolveApproval`. Re-check by identity
                  // rather than trusting `suspendedToolCalls`, which is the
                  // snapshot from when this session opened: the recovery
                  // index it is read through here is the same one
                  // `resolveIndeterminate` just updated.
                  const approvalWaits = session.suspendedToolCalls.filter(
                    (call) => call.wait === ToolDispatch.INTERACTION_WAIT,
                  );
                  // The toolkit is resolved once, and only when an approval
                  // wait exists at all: resolution may include dynamic
                  // sources whose work is real (an MCP discovery
                  // round-trip), and almost every run has nothing suspended.
                  const stillPendingInteractions: AgentEvents.PendingInteraction[] =
                    approvalWaits.length === 0
                      ? []
                      : yield* Effect.gen(function* () {
                          const approvalToolkit = Effect.succeed(
                            yield* runToolkit,
                          );
                          const batches = yield* Effect.forEach(
                            approvalWaits,
                            (call) => {
                              const current = session.recovery(
                                call.name,
                                call.toolCallId,
                              );
                              if (
                                Option.isNone(current) ||
                                current.value._tag !== 'Suspended'
                              ) {
                                return Effect.succeed<
                                  ReadonlyArray<AgentEvents.PendingInteraction>
                                >([]);
                              }
                              const pendingRecovery = current.value;
                              // Re-decoded against the tool's current
                              // parameter schema rather than surfaced from
                              // the durable encoded form: a caller
                              // re-reading this pending approval sees the
                              // same typed value the first suspension did,
                              // not the toolkit's wire encoding of it.
                              return Effect.map(
                                ToolDispatch.decodeSuspendedRequest(
                                  approvalToolkit,
                                  call.name,
                                  call.request,
                                ),
                                (
                                  decodedInput,
                                ): ReadonlyArray<AgentEvents.PendingInteraction> => [
                                  {
                                    toolCallId: call.toolCallId,
                                    toolName: call.name,
                                    kind:
                                      pendingRecovery.interaction?.name ??
                                      'approval',
                                    request: decodedInput,
                                  },
                                ],
                              );
                            },
                          );
                          return batches.flat();
                        });
                  if (stillPendingInteractions.length > 0) {
                    return Stream.make(
                      AgentEventRuntime.suspended(
                        0,
                        // No model call happened on this path — the run was
                        // refused before turn 1 — so there is no partial text
                        // to preserve.
                        '',
                        wiring.initialUsage ?? { input: 0, output: 0 },
                        stillPendingInteractions,
                      ),
                    );
                  }

                  yield* Ref.set(
                    chat.history,
                    Prompt.concat(
                      Prompt.make([
                        { role: 'system', content: runInstructions },
                      ]),
                      AgentHistory.messagesFrom(yield* session.recorded),
                    ),
                  );
                  return entryFor({
                    session: wiring.session,
                    interceptor: wiring.interceptor,
                    runtime,
                    ...(wiring.dynamicToolkit === undefined
                      ? {}
                      : { dynamicToolkit: wiring.dynamicToolkit }),
                    ...(wiring.startRun === undefined
                      ? {}
                      : { startRun: wiring.startRun }),
                    ...(wiring.initialUsage === undefined
                      ? {}
                      : { initialUsage: wiring.initialUsage }),
                    ...(wiring.lastTurn === undefined
                      ? {}
                      : { lastTurn: wiring.lastTurn }),
                    ...(wiring.codeState === undefined
                      ? {}
                      : { codeState: wiring.codeState }),
                  }).streamIn(chat, effective) as Stream.Stream<
                    AgentEvents.Event<ModelTools>,
                    RunFailure,
                    | WithOwnHandlers<RuntimeTools>
                    | StopR
                    | TurnControlR
                    | InterceptorR
                  >;
                }),
              ),
            ) as Stream.Stream<
              AgentEvents.Event<ModelTools>,
              RunFailure,
              | WithOwnHandlers<RuntimeTools>
              | StopR
              | TurnControlR
              | InterceptorR
            >;
          }
          // The only point every path above funnels through exactly once
          // before a run's first turn — including dynamic-toolkit resolution,
          // runtime creation, and signal recovery, which each re-enter this
          // function and return before reaching here. That makes it the one
          // place left to log the misconfiguration once per run rather than
          // once per proactive-compaction check.
          yield* compactionWarning;
          const usage = yield* Ref.make<Stop.Usage>(
            wiring.initialUsage ?? { input: 0, output: 0 },
          );
          const toolCallCounts = yield* Ref.make<
            Readonly<Record<string, number>>
          >({});
          const lastTurn = yield* Ref.make<ContextWindow.TurnUsage | undefined>(
            wiring.lastTurn,
          );
          const remaining = yield* runtime.remainingMillis;
          return turn(chat, usage, toolCallCounts, lastTurn, 1, input).pipe(
            Stream.interruptWhen(
              Effect.sleep(remaining).pipe(
                Effect.andThen(
                  Effect.fail(
                    RunPolicyRuntime.error({
                      limit: 'deadline',
                      used: runPolicy.wallClockMillis,
                      maximum: runPolicy.wallClockMillis,
                    }),
                  ),
                ),
              ),
            ),
          );
        }),
      );

    const stream = (input: Prompt.RawInput) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const chat = yield* Chat.fromPrompt(
            Prompt.make([{ role: 'system', content: instructions }]),
          );
          return streamIn(chat, input);
        }),
      );

    // The agent provides its own handlers — subagent delegation and skill
    // loading — so a call site never has to provide them.
    //
    // Generated layers discharge generated handlers. TS cannot subtract those
    // keys from an intersected compiled record, so these boundary assertions
    // name the public requirement channel. Exact type tests pin all four.

    return provideEntry({ stream, streamIn }, layer) as Entry<
      ModelTools,
      BaseRequires | InterceptorR,
      RunFailure
    >;
  };

  return entryFor;
};

const stateErrorMetadata = (error: AgentState.Error) => {
  switch (error._tag) {
    case 'StateDefinitionError':
      return {
        tag: error._tag,
        ...(error.stateId === undefined ? {} : { stateId: error.stateId }),
      };
    case 'StateCompatibilityError':
      return {
        tag: error._tag,
        stateId: error.stateId,
        stateVersion: error.stateVersion,
        persistedId: error.persistedId,
        persistedVersion: error.persistedVersion,
      };
    case 'StateDecodeError':
    case 'StateEncodeError':
    case 'StateJsonError':
      return {
        tag: error._tag,
        stateId: error.stateId,
        stateVersion: error.stateVersion,
        cause: error.cause,
      };
    case 'DurabilityError':
      return {
        tag: error._tag,
        source: error.source,
        operation: error.operation,
        reason: error.reason,
      };
  }
};

/**
 * What a batch of steering instructions becomes for the next turn.
 *
 * Plain user messages. A steer is somebody talking to the agent
 * out-of-band, and the model already knows what to do with a user turn;
 * inventing a bespoke role would mean every provider adapter had to learn
 * one. Several steers delivered together are joined rather than sent as
 * several messages, so the turn count does not depend on how a sender
 * happened to batch them.
 */
const steeringInput = (
  steers: ReadonlyArray<{ readonly text: string }>,
): Prompt.RawInput =>
  steers.length === 0
    ? Prompt.empty
    : Prompt.make([
        {
          role: 'user',
          content: steers.map((steer) => steer.text).join('\n\n'),
        },
      ]);

const dynamicContextFor = (instructions: string, toolkit: unknown): string => {
  const context = DynamicToolkit.resourceContext(toolkit);
  return context === '' ? instructions : `${instructions}\n\n${context}`;
};

/** Keep runtime toolkit composition out of DynamicToolkit's public interface. */
function withDynamicToolkit<
  StaticTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
>(
  staticallyDefined: Toolkit.WithHandler<StaticTools>,
  dynamic: Toolkit.WithHandler<DynamicTools> | undefined,
): Toolkit.WithHandler<StaticTools & DynamicTools>;
function withDynamicToolkit(
  staticallyDefined: Toolkit.WithHandler<Record<string, Tool.Any>>,
  dynamic: Toolkit.WithHandler<Record<string, Tool.Any>> | undefined,
): Toolkit.WithHandler<Record<string, Tool.Any>> {
  return dynamic === undefined
    ? staticallyDefined
    : DynamicToolkit.merge(staticallyDefined, dynamic);
}

const replaceSystemInstructions = (
  chat: Chat.Service,
  instructions: string,
): Effect.Effect<void> =>
  Ref.update(chat.history, (history) => {
    const rest =
      history.content[0]?.role === 'system'
        ? history.content.slice(1)
        : history.content;
    return Prompt.fromMessages([
      Prompt.makeMessage('system', { content: instructions }),
      ...rest,
    ]);
  });

interface TurnState {
  text: string;
  reasoning: string;
  parts: Response.AnyPart[];
  toolCalls: Response.ToolCallPartEncoded[];
  toolResults: Response.ToolResultPartEncoded[];
  usage: Response.FinishPartEncoded['usage'] | undefined;
  finishReason: Response.FinishReason | undefined;
  emitted: boolean;
  started: boolean;
  /** Decoded call params seen this turn, keyed by tool call id. */
  callsById: Map<
    string,
    {
      readonly name: string;
      readonly input: unknown;
      readonly interaction?: Interaction.Metadata;
    }
  >;
  /** External interaction requests observed this turn, in provider order. */
  pendingInteractions: AgentEvents.PendingInteraction[];
}

const emptyTurnState = (): TurnState => ({
  text: '',
  reasoning: '',
  parts: [],
  toolCalls: [],
  toolResults: [],
  usage: undefined,
  finishReason: undefined,
  emitted: false,
  started: false,
  callsById: new Map(),
  pendingInteractions: [],
});

/**
 * Accumulate what the stop decision and the result need from one turn.
 *
 * Takes both the decoded and encoded sibling of the same part: `encoded` is
 * what every existing accumulation here reads, and a `tool-approval-request`
 * carries no parameters of its own — the params worth showing an approver are
 * the same tool call's decoded ones, tracked from `decoded` as calls stream
 * by and looked up when the matching approval request arrives.
 */
const observe = <PartTools extends Record<string, Tool.Any>>(
  state: TurnState,
  decoded: Response.StreamPart<PartTools>,
  encoded: Response.StreamPartEncoded,
  toolkit: { readonly tools: Record<string, Tool.Any> },
): void => {
  state.emitted = true;
  state.parts.push(decoded);
  switch (encoded.type) {
    case 'text-delta':
      state.text += encoded.delta;
      break;
    case 'reasoning-delta':
      state.reasoning += encoded.delta;
      break;
    case 'tool-call':
      state.toolCalls.push(encoded);
      if (decoded.type === 'tool-call') {
        const tool = toolkit.tools[decoded.name];
        const interaction =
          tool === undefined
            ? undefined
            : Option.getOrUndefined(Interaction.metadata(tool));
        state.callsById.set(decoded.id, {
          name: decoded.name,
          input: decoded.params,
          ...(interaction === undefined ? {} : { interaction }),
        });
      }
      break;
    case 'tool-result':
      if (encoded.preliminary !== true) {
        state.toolResults.push(encoded);
      }
      break;
    case 'tool-approval-request': {
      const call = state.callsById.get(encoded.toolCallId);
      state.pendingInteractions.push({
        toolCallId: encoded.toolCallId,
        toolName: call?.name ?? '',
        kind: call?.interaction?.name ?? 'approval',
        request: call?.input,
      });
      break;
    }
    case 'finish':
      state.usage = encoded.usage;
      state.finishReason = encoded.reason;
      break;
    default:
      break;
  }
};

const outputOf = (state: TurnState): Prompt.Prompt =>
  Prompt.fromResponseParts(state.parts);

const continuationInput = (
  continuation: Option.Option<TurnControl.Continuation>,
  steers: ReadonlyArray<{ readonly text: string }>,
): Prompt.RawInput =>
  Prompt.concat(
    Option.isSome(continuation)
      ? Prompt.make(continuation.value.input)
      : Prompt.empty,
    Prompt.make(steeringInput(steers)),
  );

/** Apply a model override without narrowing the caller's requirement type. */
const continueTurn = <A, E, R>(
  continuation: Option.Option<TurnControl.Continuation>,
  stream: Stream.Stream<A, E, R>,
): Stream.Stream<A, E, R> =>
  Option.isSome(continuation) && continuation.value.model !== undefined
    ? Stream.provideService(
        stream,
        LanguageModel.LanguageModel,
        continuation.value.model,
      )
    : stream;

const addUsage = (
  current: Stop.Usage,
  usage: Response.FinishPartEncoded['usage'] | undefined,
): Stop.Usage =>
  usage === undefined
    ? current
    : {
        input: current.input + (usage.inputTokens.total ?? 0),
        output: current.output + (usage.outputTokens.total ?? 0),
      };

const addStopUsage = (left: Stop.Usage, right: Stop.Usage): Stop.Usage => ({
  input: left.input + right.input,
  output: left.output + right.output,
});

const addToolCallCounts = (
  current: Readonly<Record<string, number>>,
  calls: ReadonlyArray<Response.ToolCallPartEncoded>,
): Readonly<Record<string, number>> => {
  if (calls.length === 0) {
    return current;
  }
  // Tool names here come from the model's response, not the toolkit — a call
  // to a nonexistent tool still lands in `toolCalls` — so `__proto__` is a
  // possible key. On a default-prototype object that assignment hits the
  // inherited accessor and silently drops the count; a null-prototype object
  // makes it an ordinary own property.
  const next: Record<string, number> = Object.create(null);
  Object.assign(next, current);
  for (const call of calls) {
    next[call.name] = (next[call.name] ?? 0) + 1;
  }
  return next;
};
