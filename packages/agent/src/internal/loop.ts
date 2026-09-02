import { Effect, Layer, Ref, Stream } from 'effect';
import {
  AiError,
  Chat,
  Prompt,
  type Tool,
  type Toolkit,
} from 'effect/unstable/ai';

import type { RunFailure, VisibleTools } from '../agent.js';
import { CodeMode } from '../code-mode.js';
import type { Compaction } from '../compaction.js';
import type { ContextWindow } from '../context-window.js';
import { ToolDispatch } from '../dispatch.js';
import type { AgentEvents } from '../event.js';
import { DynamicToolkit } from '../dynamic-toolkit.js';
import type { Interception } from '../interception.js';
import type * as AgentLog from '../log.js';
import { ResultBounds } from '../result-bounds.js';
import { ResultOverflow } from '../result-overflow.js';
import type { RunPolicy } from '../run-policy.js';
import { RunPolicyRuntime } from '../run-policy-runtime.js';
import type { Stop } from '../stop.js';
import { AgentState } from '../state.js';
import type { TurnControl } from '../turn-control.js';
import * as Bootstrap from './bootstrap.js';
import * as RecoveryRun from './recovery-run.js';
import * as TurnRun from './turn.js';

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

/**
 * The recursive re-entry into `entryFor(...).streamIn(...)` that every
 * `streamIn` precondition (code-mode bootstrap, dynamic-toolkit bootstrap,
 * runtime creation, signal recovery — see `bootstrap.ts` and
 * `recovery-run.ts`) funnels through once it has resolved its own concern.
 * Naming the shape here, once, is what lets each precondition's extracted
 * function declare its return type against this signature instead of
 * asserting the resolved `Stream` shape at every call site.
 */
export type ReEnter<
  ModelTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorR,
> = (
  wiring: Wiring<InterceptorR, DynamicTools>,
) => Entry<ModelTools, BaseRequires | InterceptorR, RunFailure>;

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
  readonly resultBounds: ResultBounds.Policy | false | undefined;
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
    const runInstructions = Bootstrap.dynamicContextFor(
      instructions,
      wiring.dynamicToolkit,
    );
    // Oversized results are spilled before the log or the interceptor ever
    // see them, so `gate`'s recording and `resolveIndeterminate`'s recovery
    // — both consumers of this same `runToolkit` — see only the pointer, not
    // the payload it stands in for. See `result-overflow.ts` for why a
    // storage failure here is a defect rather than a typed tool failure.
    //
    // `ResultBounds.wrap` wraps *outside* `ResultOverflow.wrap`, so overflow's
    // spill always runs first: a spilled result is already a small pointer by
    // the time bounds sees it, and bounds only ever truncates a result
    // overflow did not spill. See `result-bounds.ts`.
    const runToolkit = ResultBounds.wrap(
      definition.resultBounds,
      ResultOverflow.wrap(
        definition.resultOverflow,
        Effect.map(toolkit, (staticallyDefined) =>
          withDynamicToolkit(staticallyDefined, wiring.dynamicToolkit),
        ),
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

    const turnRunner = TurnRun.makeTurnRunner({
      agentName: definition.name,
      concurrency: definition.concurrency,
      session,
      interceptor,
      runtime,
      runPolicy,
      compaction,
      stopWhen,
      nextTurn,
      startRun: wiring.startRun,
      dispatching,
    });

    // The five preconditions a run's first `streamIn` call has to resolve,
    // in the order they have to resolve: code mode's execution state,
    // a dynamic toolkit's sources, the root run-policy runtime, recovery
    // from a session with pending tool calls, and — once all four hold —
    // the ordinary turn path. The first four each re-enter `entryFor` with
    // an updated `Wiring` once resolved; only the fifth actually starts a
    // turn.
    const streamIn = (chat: Chat.Service, input: Prompt.RawInput) =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (
            CodeMode.isEnabled(definition.codeMode) &&
            wiring.codeState === undefined
          ) {
            return yield* Bootstrap.bootstrapCodeMode<
              ModelTools,
              DynamicTools,
              BaseRequires,
              InterceptorR
            >({
              session,
              wiring,
              entryFor,
              chat,
              input,
            });
          }
          if (
            wiring.dynamicToolkit === undefined &&
            definition.dynamicTools !== undefined &&
            definition.dynamicTools.length > 0
          ) {
            return yield* Bootstrap.bootstrapDynamicToolkit<
              ModelTools,
              DynamicSources,
              BaseRequires,
              InterceptorR
            >({
              dynamicTools: definition.dynamicTools,
              instructions,
              wiring,
              entryFor,
              chat,
              input,
            });
          }
          if (runtime === undefined) {
            return yield* Bootstrap.bootstrapRuntime<
              ModelTools,
              DynamicTools,
              BaseRequires,
              InterceptorR
            >({
              runPolicy,
              wiring,
              entryFor,
              chat,
              input,
            });
          }
          if (session !== undefined && (yield* session.hasPendingToolCalls)) {
            return yield* RecoveryRun.runRecovery<
              ModelTools,
              RunTools,
              DynamicTools,
              BaseRequires,
              InterceptorR
            >({
              agentName: definition.name,
              session,
              interceptor,
              runtime,
              delegationToolNames,
              runToolkit,
              runInstructions,
              wiring,
              entryFor,
              chat,
              input,
            });
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
          const stepRef = yield* Ref.make(0);
          const remaining = yield* runtime.remainingMillis;
          const primary = turnRunner.turn(
            chat,
            usage,
            toolCallCounts,
            lastTurn,
            stepRef,
            1,
            input,
          );
          const withFallback =
            (runtime.limits.onExhaustion ?? 'fail') === 'final-answer'
              ? primary.pipe(
                  Stream.catchIf(TurnRun.isFinalAnswerEligible, (exhaustion) =>
                    Stream.unwrap(
                      Ref.get(stepRef).pipe(
                        Effect.map((lastStep) =>
                          turnRunner.finalAnswerTurn(
                            chat,
                            usage,
                            toolCallCounts,
                            lastStep + 1,
                            exhaustion,
                          ),
                        ),
                      ),
                    ),
                  ),
                )
              : primary;
          return withFallback.pipe(
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
