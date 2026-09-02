import { Effect, Exit, Option, Ref, Schema, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  Prompt,
  type Chat,
  type Response,
  type Tool,
  type Toolkit,
} from 'effect/unstable/ai';

import type { RunFailure, WithOwnHandlers } from '../agent.js';
import { Compaction } from '../compaction.js';
import { ContextWindow } from '../context-window.js';
import { ToolDispatch } from '../dispatch.js';
import type { AgentEvents } from '../event.js';
import type { Interception } from '../interception.js';
import { Interaction } from '../interaction.js';
import type * as AgentLog from '../log.js';
import { RunPolicy } from '../run-policy.js';
import { RunPolicyRuntime } from '../run-policy-runtime.js';
import type { Stop } from '../stop.js';
import { TurnControl } from '../turn-control.js';
import { CompactionRuntime } from './compaction.js';
import { AgentEventRuntime } from './event.js';
import * as Observability from './observability.js';
import { encodePart, type ModelTurnPart } from './part-encoding.js';
import {
  incompleteOutputError,
  interactionRequiresConversationError,
  normalizeProviderError,
} from './provider-error.js';
import { steeringInput, watchForCancel } from './signals-run.js';

// Single-turn execution: one model call plus the resolution of any tool
// calls it requested, in CONTEXT.md's "Turn" sense. `loop.ts` keeps the
// repeated-turns orchestration; everything here is what happens *within*
// one step of it — the provider call and its seam (`askModel`), part
// observation and state folding (`observe`, `TurnState`), tool-result and
// usage bookkeeping, `TurnFinished` emission, and the final-answer turn
// `onExhaustion: 'final-answer'` performs once a soft-fallback-eligible hard
// budget is exhausted.
//
// `makeTurnRunner` closes over one run's wiring — session, interceptor,
// runtime, the resolved dispatch seam — exactly as `loop.ts`'s `entryFor`
// did before this split, so nothing here depends on what happens to be in
// the context.

/**
 * What a turn runner needs from the run it belongs to. Bundles exactly what
 * `askModel`, `openTurn`, `compactAhead`/`compactWithBudget`, `turn`, and
 * `finalAnswerTurn` closed over in `loop.ts`'s `entryFor` before this split,
 * so the turn loop's own recursion stays lexical: nothing here is looked up
 * from the context.
 */
export interface TurnContext<
  ModelTools extends Record<string, Tool.Any>,
  RuntimeTools extends Record<string, Tool.Any>,
  StopR,
  TurnControlR,
  InterceptorR,
> {
  readonly agentName: string;
  readonly concurrency: number | 'unbounded' | undefined;
  readonly session: AgentLog.Session | undefined;
  readonly interceptor: Interception.Interceptor<InterceptorR> | undefined;
  readonly runtime: RunPolicyRuntime.Runtime | undefined;
  readonly runPolicy: RunPolicy.Limits;
  readonly compaction: Compaction.Policy | undefined;
  readonly stopWhen: Stop.StopCondition<ModelTools, StopR>;
  readonly nextTurn: TurnControl.Policy<ModelTools, TurnControlR> | undefined;
  readonly startRun:
    | ((
        input: Prompt.RawInput,
      ) => Effect.Effect<void, AgentLog.DurabilityError>)
    | undefined;
  /**
   * The resolved dispatch seam (`ToolDispatch.gate` or the code-mode split
   * of it), built once per run in `loop.ts` because it also depends on the
   * result-overflow/result-bounds wrapping and the dynamic toolkit — none
   * of which the turn loop itself needs to know about.
   */
  readonly dispatching: (
    arbitration: ToolDispatch.TurnArbitration,
  ) => Effect.Effect<
    Toolkit.WithHandler<ModelTools>,
    RunFailure,
    WithOwnHandlers<RuntimeTools> | InterceptorR
  >;
}

/**
 * Build the recursive per-turn stepper and the final-answer fallback turn
 * for one run. Returned as a pair rather than individual exports because
 * both close over the same `TurnContext` and `loop.ts` always needs them
 * together — `finalAnswerTurn` only ever runs as `turn`'s own fallback.
 */
export const makeTurnRunner = <
  ModelTools extends Record<string, Tool.Any>,
  RuntimeTools extends Record<string, Tool.Any>,
  StopR,
  TurnControlR,
  InterceptorR,
>(
  context: TurnContext<
    ModelTools,
    RuntimeTools,
    StopR,
    TurnControlR,
    InterceptorR
  >,
) => {
  const {
    agentName,
    concurrency,
    session,
    interceptor,
    runtime,
    runPolicy,
    compaction,
    stopWhen,
    nextTurn,
    startRun,
    dispatching,
  } = context;

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
    // The one extra model call `onExhaustion: 'final-answer'` performs once
    // a soft-fallback-eligible hard budget is exhausted (see
    // `finalAnswerTurn` below). It skips `runtime.modelCall` — the budget
    // that call already crossed is not re-charged for the call that
    // answers within it — and forces `toolChoice: 'none'` so the model
    // cannot spend the run further on a tool call it has no budget left to
    // resolve. Everything else about the provider call — interception,
    // observability, the deadline, part encoding — stays identical, which
    // is what lets this turn's output "settle the run normally".
    finalAnswer = false,
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
            agent: agentName,
            conversationId: session?.conversationId,
            step,
            attempt,
          });
        }
        if (runtime !== undefined && !finalAnswer) {
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
            concurrency,
            // A call that never reaches a handler (an invented tool name,
            // invalid or unparseable arguments) is returned to the model as
            // a failed tool result instead of failing the turn. Handler
            // failures keep obeying each tool's own `failureMode`.
            invalidToolCalls: 'return',
            // The final-answer call still advertises the toolkit — the
            // model boundary already resolved it above, and building a
            // second, empty one would fork the encode/observe path this
            // call shares with every ordinary turn — but forbids using it.
            ...(finalAnswer ? { toolChoice: 'none' as const } : {}),
          })
          .pipe(
            Stream.mapEffect((part) =>
              part.type === 'error'
                ? Effect.fail(normalizeProviderError(part.error, part.metadata))
                : encodePart(resolvedToolkit, part).pipe(
                    Effect.map((encodedPart) => ({ part, encodedPart })),
                  ),
            ),
            Stream.tap(({ part, encodedPart }) =>
              Effect.gen(function* () {
                seen.started = true;
                observe(seen, part, encodedPart, resolvedToolkit);
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
                  part,
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
      // TS cannot carry `InterceptorR` through this generator's inferred
      // requirement type across the conditional `interceptor?.beforeModelCall`
      // yield, so the declared signature above is asserted here instead of
      // narrowing on its own.
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
            agent: agentName,
            conversationId: session?.conversationId,
            step,
            usage: totals,
            input: Prompt.make(input),
          }),
          (decision) => (decision._tag === 'Proceed' ? input : decision.input),
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
    // The last step this recursion reached, kept for `onExhaustion:
    // 'final-answer'`: a `RunPolicyExhausted` raised anywhere in this
    // recursive stream is caught outside it, where `step` itself is no
    // longer in scope, so the fallback turn reads it from here instead.
    stepRef: Ref.Ref<number>,
    step: number,
    pending: Prompt.RawInput,
  ): Stream.Stream<
    AgentEvents.Event<ModelTools>,
    RunFailure,
    WithOwnHandlers<RuntimeTools> | StopR | TurnControlR | InterceptorR
  > =>
    Stream.unwrap(
      Effect.gen(function* () {
        yield* Ref.set(stepRef, step);
        const input = yield* openTurn(
          step,
          yield* Ref.get(usage),
          pending,
        ).pipe(
          Effect.catch((error) =>
            step === 1 && startRun !== undefined
              ? startRun(pending).pipe(Effect.andThen(Effect.fail(error)))
              : Effect.fail(error),
          ),
        );
        if (step === 1 && startRun !== undefined) {
          yield* startRun(input);
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
            addStopUsage(
              current,
              pricedUsage(runtime?.limits.costModel, ahead.usage),
            ),
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
                          Prompt.concat(historyBefore, Prompt.make(modelInput)),
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
                          addStopUsage(
                            current,
                            pricedUsage(
                              runtime?.limits.costModel,
                              summarized.usage,
                            ),
                          ),
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
              addUsage(current, seen.usage, runtime?.limits.costModel),
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
                  cachedInput: seen.usage.inputTokens.cacheRead ?? 0,
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
              yield* Ref.set(lastTurn, ContextWindow.usageFromTurn(seen.usage));
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
                      stepRef,
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
            : watchForCancel({
                session,
                runtime,
                runPolicy,
                arbitration,
                failureMessage:
                  'Agent responsive cancel watcher failed; cancellation remains available at the next turn boundary',
              });

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
      // A generic function's own body cannot use its recursive self-call's
      // inferred type to check the declared return type above — this is
      // TS's ordinary limit on self-referential generic inference, not
      // something specific to this recursion.
    ) as Stream.Stream<
      AgentEvents.Event<ModelTools>,
      RunFailure,
      WithOwnHandlers<RuntimeTools> | StopR | TurnControlR | InterceptorR
    >;

  /**
   * The one extra call `onExhaustion: 'final-answer'` performs once a
   * soft-fallback-eligible hard budget (see {@link finalAnswerEligibleLimits})
   * is exhausted: no tools, a short appended instruction that the budget is
   * spent, and its output settles the run as an ordinary success — with
   * `exhausted` recording which limit forced it.
   *
   * Reuses `askModel` rather than calling `chat.streamText` directly, so
   * interception, observability, part encoding, and the deadline timeout
   * stay exactly what an ordinary turn gets — a hard limit this call itself
   * crosses (chiefly the wall-clock deadline) still fails the run, because
   * that timeout is `askModel`'s, not something layered on top here.
   *
   * Does not attempt to replay whatever input the aborted turn would have
   * sent — `chat.history` already holds every turn that completed, which is
   * everything a "best answer so far" needs, and reconstructing the one
   * turn that never ran would require tracking exactly how far it got
   * before failing. What it adds is the instruction below, nothing more.
   */
  const finalAnswerTurn = (
    chat: Chat.Service,
    usage: Ref.Ref<Stop.Usage>,
    toolCallCounts: Ref.Ref<Readonly<Record<string, number>>>,
    step: number,
    exhaustion: RunPolicy.RunPolicyExhausted,
  ): Stream.Stream<
    AgentEvents.Event<ModelTools>,
    RunFailure,
    WithOwnHandlers<RuntimeTools> | InterceptorR
  > =>
    Stream.unwrap(
      Effect.gen(function* () {
        const seen = emptyTurnState();
        const arbitration = yield* ToolDispatch.makeTurnArbitration;
        const parts = askModel(
          chat,
          seen,
          step,
          Prompt.make([{ role: 'user', content: FINAL_ANSWER_INSTRUCTION }]),
          'initial',
          arbitration,
          true,
        );
        const decide = Stream.unwrap(
          Effect.gen(function* () {
            const totals = yield* Ref.updateAndGet(usage, (current) =>
              addUsage(current, seen.usage, runtime?.limits.costModel),
            );
            yield* Ref.update(toolCallCounts, (current) =>
              addToolCallCounts(current, seen.toolCalls),
            );
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
            return Stream.concat(
              Stream.make(AgentEventRuntime.turnFinished(step, totals)),
              Stream.make(
                AgentEventRuntime.completed(
                  seen.text,
                  step,
                  totals,
                  'success',
                  outputOf(seen),
                  {
                    limit: exhaustion.limit,
                    used: exhaustion.used,
                    maximum: exhaustion.maximum,
                  },
                ),
              ),
            );
          }),
        );
        return Stream.concat(
          Stream.make(AgentEventRuntime.turnStarted(step)),
          Stream.concat(parts, decide),
        ).pipe(
          Stream.withSpan('Agent.finalAnswerTurn', {
            attributes: {
              'vesper.agent.step': step,
              'vesper.run_policy.exhausted_limit': exhaustion.limit,
              ...(session === undefined
                ? {}
                : { 'vesper.conversation.id': session.conversationId }),
            },
          }),
        );
      }),
    );

  return { turn, finalAnswerTurn };
};

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
  /** Raw provider call params seen this turn, keyed by tool call id. */
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
 * Takes both the live and provider-facing sibling of the same part. A
 * `tool-approval-request` carries no parameters of its own, so the raw request
 * is tracked from its tool call and looked up when the approval arrives.
 */
const observe = <PartTools extends Record<string, Tool.Any>>(
  state: TurnState,
  part: ModelTurnPart<PartTools>,
  encoded: Response.StreamPartEncoded,
  toolkit: { readonly tools: Record<string, Tool.Any> },
): void => {
  state.emitted = true;
  state.parts.push(part);
  switch (encoded.type) {
    case 'text-delta':
      state.text += encoded.delta;
      break;
    case 'reasoning-delta':
      state.reasoning += encoded.delta;
      break;
    case 'tool-call':
      state.toolCalls.push(encoded);
      if (part.type === 'tool-call') {
        const tool = Object.hasOwn(toolkit.tools, part.name)
          ? toolkit.tools[part.name]
          : undefined;
        const interaction =
          tool === undefined
            ? undefined
            : Option.getOrUndefined(Interaction.metadata(tool));
        state.callsById.set(part.id, {
          name: part.name,
          input: part.params,
          ...(interaction === undefined ? {} : { interaction }),
        });
      }
      break;
    case 'tool-call-error':
      state.toolCalls.push({
        type: 'tool-call',
        id: encoded.id,
        name: encoded.name,
        params: encoded.params,
        ...(encoded.providerExecuted === true
          ? { providerExecuted: true }
          : {}),
      });
      state.toolResults.push({
        type: 'tool-result',
        id: encoded.id,
        name: encoded.name,
        result: encoded.error,
        isFailure: true,
        ...(encoded.providerExecuted === true
          ? { providerExecuted: true }
          : {}),
      });
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

/**
 * Limits `onExhaustion: 'final-answer'` may recover from with one extra
 * no-tools model call rather than failing the run outright — see
 * `RunPolicy.Limits.onExhaustion`. `deadline`, `maxToolConcurrency`'s clamp
 * (which never raises `RunPolicyExhausted` at all), and the signal limits are
 * deliberately excluded: a run that is out of *time*, rather than out of
 * turns, calls, tokens, cost, or delegation budget, has no safe way to spend
 * more of it on one further call.
 */
const finalAnswerEligibleLimits: ReadonlySet<RunPolicy.Limit> = new Set([
  'turns',
  'model_calls',
  'input_tokens',
  'output_tokens',
  'maxCostMicrousd',
  'delegated_tasks',
]);

const isRunPolicyExhausted = Schema.is(RunPolicy.RunPolicyExhausted);

/**
 * Exported for `loop.ts`'s `streamIn`, which wraps the primary turn stream
 * in `Stream.catchIf(isFinalAnswerEligible, ...)` to reach
 * {@link makeTurnRunner}'s `finalAnswerTurn` — the orchestration of *when*
 * to fall back stays in `loop.ts`; this is only the predicate for it.
 */
export const isFinalAnswerEligible = (
  error: RunFailure,
): error is RunPolicy.RunPolicyExhausted =>
  isRunPolicyExhausted(error) && finalAnswerEligibleLimits.has(error.limit);

const FINAL_ANSWER_INSTRUCTION =
  'The run budget is spent: no further turns, model calls, tokens, or delegated tasks remain, and no tools are available for this call. Answer now with the best final response you can give using only what you already have.';

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
/**
 * Price a plain `{input, output}` figure against a cost model, the same way
 * `RunPolicyRuntime.addUsage` prices it for enforcement — so the local usage
 * projection a caller observes and the budget a run is charged against never
 * disagree. Absent `costModel` leaves `costMicrousd` unset, matching
 * `Stop.Usage`'s "never priced" reading of a missing figure.
 */
const pricedUsage = (
  costModel: RunPolicy.CostModel | undefined,
  plain: {
    readonly input: number;
    readonly output: number;
    readonly cachedInput?: number;
  },
): Stop.Usage =>
  costModel === undefined
    ? { input: plain.input, output: plain.output }
    : {
        input: plain.input,
        output: plain.output,
        costMicrousd: RunPolicy.costOf(costModel, plain),
      };

const addUsage = (
  current: Stop.Usage,
  usage: Response.FinishPartEncoded['usage'] | undefined,
  costModel: RunPolicy.CostModel | undefined,
): Stop.Usage => {
  if (usage === undefined) {
    return current;
  }
  const priced = pricedUsage(costModel, {
    input: usage.inputTokens.total ?? 0,
    output: usage.outputTokens.total ?? 0,
    cachedInput: usage.inputTokens.cacheRead ?? 0,
  });
  return addStopUsage(current, priced);
};

const addStopUsage = (left: Stop.Usage, right: Stop.Usage): Stop.Usage => {
  const costMicrousd =
    left.costMicrousd === undefined && right.costMicrousd === undefined
      ? undefined
      : (left.costMicrousd ?? 0) + (right.costMicrousd ?? 0);
  return {
    input: left.input + right.input,
    output: left.output + right.output,
    ...(costMicrousd === undefined ? {} : { costMicrousd }),
  };
};

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
