import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Predicate, Schema, Stream } from 'effect';
import {
  Chat,
  Prompt,
  Toolkit,
  type AiError,
  type LanguageModel,
  type Tool,
} from 'effect/unstable/ai';

import { Compaction } from './compaction.js';
import { CodeMode } from './code-mode.js';
import type { CompatibilityError } from './conversation-error.js';
import type { DynamicToolkit } from './dynamic-toolkit.js';
import { AgentEvents } from './event.js';
import { AgentHistory } from './history.js';
import { AgentEventRuntime } from './internal/event.js';
import * as DefinitionDigest from './internal/definition-digest.js';
import { foldToResult } from './internal/fold-to-result.js';
import {
  makeEntry,
  provideEntry,
  type Entry,
  type Wiring,
} from './internal/loop.js';
import {
  hasProtocol,
  register as registerProtocol,
} from './internal/protocol.js';
import type { AgentProtocol } from './internal/protocol.js';
import type { Interception } from './interception.js';
import * as AgentLog from './log.js';
import { RecordingPolicyRuntime } from './recording-policy-runtime.js';
import type { ResultBounds } from './result-bounds.js';
import { ResultOverflow } from './result-overflow.js';
import { RunPolicy } from './run-policy.js';
import type { RunPolicyRuntime } from './run-policy-runtime.js';
import * as AgentSkill from './skill.js';
import { Stop } from './stop.js';
import type { TurnControl } from './turn-control.js';
import type { AgentState } from './state.js';
import { Subagent } from './subagent.js';
import { SubagentRuntime } from './subagent-runtime.js';

/** Recoverable failures produced by an unrecorded agent run. */
export type RunFailure = AiError.AiError | RunPolicy.RunPolicyExhausted;

// The loop. This is the piece `effect/unstable/ai` does not have:
// `LanguageModel.generateText` performs exactly one turn — prompt, model,
// resolve tool calls, return — and `Chat` adds history to that, still one
// turn per call. An autonomous agent is that, repeated until a stop
// condition holds.
//
// `stream` is the primitive and `run` is a fold of it. Written the other way
// round, a streaming consumer and a blocking one would take different paths
// through the loop and drift — and the streaming path, where turn
// boundaries, usage accounting, and the stop decision interleave, is the
// easier one to get subtly wrong.
//
// This module keeps the public `Agent.*` surface: the types, `make`'s
// compile step, and the method attachment. The turn machinery itself lives
// in `internal/loop.ts` and is threaded the run's session and interceptor
// lexically from here.
//
// This module targets the `LanguageModel` tag, so the official provider package
// and its HTTP policy are chosen at application wiring.
// See `docs/contributing.md`.
//
// It does import `@sunfall/vesper-log`, which depends on nothing but `effect`. That
// is a data vocabulary — records, offsets, a store interface — not a provider
// and not a durability strategy, so it cannot smuggle either back in.
// Recording is opt-in through `Conversation`, the only public API that names
// `LogStore`.

/**
 * @category type ids
 * @since 0.1.0
 */
export const TypeId: TypeId = '~sunfall/vesper/Agent';

/**
 * @category type ids
 * @since 0.1.0
 */
export type TypeId = '~sunfall/vesper/Agent';

const ChildTypeId: unique symbol = Symbol.for(
  '@sunfall/vesper-agent/Agent/Child',
);

/**
 * The declarative shape an agent is built from.
 *
 * A plain options object rather than a branded type, mirroring the options
 * `Tool.make` takes: it is input to a constructor, not a value that flows
 * through the API.
 *
 * @category models
 * @since 0.1.0
 */
export interface Definition<
  AgentName extends string,
  AgentTools extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Child> = readonly [],
  Skills extends ReadonlyArray<AgentSkill.Skill> = readonly [],
  StopR = never,
  TurnControlR = never,
  StateDefinition extends AgentState.AnyDefinition | undefined = undefined,
  DynamicSources extends ReadonlyArray<DynamicToolkit.Any> = readonly [],
  OverflowPolicy extends ResultOverflow.Policy | undefined = undefined,
  CodeModeOption extends CodeMode.Option<AgentTools> = false,
> {
  readonly name: AgentName;
  /** Stable application-defined compatibility revision for durable history. */
  readonly revision: string;
  readonly description?: string;
  /** Prepended as a system message on every run. */
  readonly instructions: string;
  readonly toolkit: Toolkit.Toolkit<AgentTools>;
  /**
   * Scoped toolkits discovered once at the beginning of each run.
   * The resolved snapshot is both model advertisement and dispatch authority.
   * Runtime authorization belongs in handlers or `beforeToolCall`; this is
   * for definitions that genuinely are not known until the run starts.
   */
  readonly dynamicTools?: DynamicSources;
  readonly stopWhen?: Stop.StopCondition<
    CodeMode.ModelTools<
      VisibleTools<
        CompiledTools<AgentTools, Children, Skills, OverflowPolicy>,
        DynamicToolkit.Tools<DynamicSources>
      >,
      CodeModeOption
    >,
    StopR
  >;
  /**
   * Refine the next boundary after `stopWhen` has made its ordinary decision.
   * Returning a continuation supplies follow-up input or a new Effect
   * LanguageModel service; `Option.none` preserves the ordinary decision.
   */
  readonly nextTurn?: TurnControl.Policy<
    CodeMode.ModelTools<
      VisibleTools<
        CompiledTools<AgentTools, Children, Skills, OverflowPolicy>,
        DynamicToolkit.Tools<DynamicSources>
      >,
      CodeModeOption
    >,
    TurnControlR
  >;
  /** Concurrency for resolving the tool calls within one turn. */
  readonly concurrency?: number | 'unbounded';

  /**
   * Agents this one may delegate to. Each compiles to a tool on this agent's
   * toolkit, and the agent supplies their handlers itself.
   *
   * Captured as a tuple rather than `ReadonlyArray<Any>` so each child's own
   * service requirements survive into this agent's. Erased, they do not: a
   * parent whose subagent reads a database would compile without that
   * database and fail at the moment the model first delegates.
   */
  readonly subagents?: Children;

  /**
   * Instructions loadable on demand. Their catalog is appended to
   * `instructions` (so it stays in the cacheable prefix) while the bodies
   * load through a tool.
   */
  readonly skills?: Skills;

  /**
   * Automatic compaction when a turn overflows the context window. Defaults
   * to {@link Compaction.defaultPolicy}; pass `false` to opt out.
   *
   * Applied by default because the failure it prevents — a long conversation
   * dying on a context-window error — is one every agent hits eventually,
   * and the recovery is always the same.
   */
  readonly compaction?: Compaction.Policy | false;
  /** Hard limits shared by this run and every descendant delegation. */
  readonly runPolicy?: Partial<RunPolicy.Limits>;
  /** One optional state document, opened separately for every run. */
  readonly state?: StateDefinition;

  /**
   * Spill a tool result over this many UTF-8 bytes into the attachments
   * service and replace it with a pointer, instead of handing the whole
   * thing to the model. Adds a `read_attachment` tool the model uses to read
   * the spilled content back in ranges.
   *
   * Unset — the default — is exactly today's behaviour: every result reaches
   * the model and the log as the tool returned it, and no extra tool or
   * service requirement appears.
   */
  readonly resultOverflow?: OverflowPolicy;
  /**
   * Bound every tool result to this many UTF-8 bytes of encoded size, so one
   * oversized result cannot poison a conversation that never configured
   * `resultOverflow`. Excess is replaced with a small, schema-encodable
   * truncation envelope; unlike `resultOverflow` there is nothing left to
   * read back.
   *
   * Defaults to {@link ResultBounds.defaultPolicy} (64 KiB) when unset —
   * this bound is on by default, unlike `resultOverflow`. Pass `false` to
   * disable it and restore unbounded results.
   *
   * When `resultOverflow` is also set, it always spills first: a spilled
   * result is already a small pointer, so this bound only ever applies to a
   * result overflow did not spill.
   */
  readonly resultBounds?: ResultBounds.Policy | false;
  /**
   * Broker tools behind the isolated `exec` tool. `true` brokers the whole
   * toolkit; `{ except: [...] }` keeps the named tools directly advertised —
   * gated, interceptable, meterable, and approvable exactly as if code mode
   * were off for them — and brokers the rest. Names are checked against the
   * toolkit at compile time for literal arrays, and at construction
   * otherwise.
   */
  readonly codeMode?: CodeModeOption;
}

/**
 * What a completed run produced.
 *
 * Schema-modelled because a result is the natural thing to checkpoint, hand
 * to a workflow, or return over a transport, and every one of those needs a
 * codec rather than a bare interface.
 */
const ResultFields = {
  /** Concatenated text of the final turn. Empty when `outcome` is `suspended`. */
  text: Schema.String,
  steps: Schema.Natural,
  usage: Stop.Usage,
  /** Full final turn; absent only when no provider call ran. */
  response: Schema.optionalKey(Prompt.Prompt),
} as const;

export const Result = Schema.Union([
  Schema.Struct({
    outcome: Schema.Literal('success'),
    ...ResultFields,
    /**
     * Present only when `RunPolicy.Limits.onExhaustion: 'final-answer'`
     * settled this run with its one allowed extra no-tools call instead of
     * failing with `RunPolicy.RunPolicyExhausted`, naming the budget that
     * forced it. See the agent README's "Run policy and budgets" section.
     */
    exhausted: Schema.optionalKey(AgentEvents.Exhausted),
  }),
  Schema.Struct({
    outcome: Schema.Literal('cancelled'),
    ...ResultFields,
  }),
  Schema.Struct({
    outcome: Schema.Literal('suspended'),
    ...ResultFields,
    /**
     * Tool calls durably parked on an external interaction.
     *
     * Present only when `outcome` is `suspended`. Resolve each one through
     * Resolve each interaction and call `run` again to continue.
     */
    pendingInteractions: Schema.Array(AgentEvents.PendingInteraction),
  }),
]);
export type Result = typeof Result.Type;

/**
 * Every service a run needs, given a toolkit.
 *
 * The handler union is what makes the design pay off: a tool that reads a
 * database, and a subagent whose own tools read another, both surface here,
 * so an agent cannot be run until application wiring provides all of it.
 * `ResultDecodingServices` is included because a tool whose success schema
 * has decoding dependencies needs them at result-decode time, not just at
 * handler time.
 *
 * Named as the counterpart of {@link WithoutOwnHandlers}: this is the form
 * that still includes `Tool.HandlersFor`, that one is the form with it
 * removed. It used to be `Requirements`, four characters from the `Requires`
 * extractor and meaning something else entirely — one computes from a
 * toolkit, the other reads off a finished agent.
 *
 * @category utility types
 * @since 0.1.0
 */
export type WithOwnHandlers<HandlerTools extends Record<string, Tool.Any>> =
  | LanguageModel.LanguageModel
  | Tool.HandlersFor<HandlerTools>
  // An empty toolkit is fine here: `Tools[keyof Tools]` is `never`, and
  // `HandlerServices<never>` resolves to `never` rather than widening to
  // `any`. An earlier comment claimed otherwise; `types.test.ts` pins the
  // truth, because the difference decides whether callers inherit an `any`.
  | Tool.HandlerServices<HandlerTools[keyof HandlerTools]>
  | Tool.ResultDecodingServices<HandlerTools[keyof HandlerTools]>;

type WithOwnHandlersForState<
  StateTools extends Record<string, Tool.Any>,
  StateDefinition extends AgentState.AnyDefinition | undefined,
> =
  | LanguageModel.LanguageModel
  | Tool.HandlersFor<StateTools>
  | WithoutState<
      Tool.HandlerServices<StateTools[keyof StateTools]>,
      StateDefinition
    >
  | Tool.ResultDecodingServices<StateTools[keyof StateTools]>;

/**
 * Discharge an agent's own tool handlers from whatever it required.
 *
 * Subtractive rather than reconstructed, and that distinction is load-bearing.
 * Attaching tool handlers discharges exactly one term — `Tool.HandlersFor` —
 * and nothing else. Naming the result directly instead, as this used to, threw
 * away every requirement that did not fit the reconstruction: an agent with
 * subagents lost their services, because attaching *tool* handlers was
 * described as if it also satisfied what a *subagent* needs. It does not, and
 * the run then failed the first time the model delegated.
 *
 * @category utility types
 * @since 0.1.0
 */
export type WithoutOwnHandlers<
  HandlerRequirements,
  HandlerTools extends Record<string, Tool.Any>,
> = Exclude<HandlerRequirements, Tool.HandlersFor<HandlerTools>>;

/**
 * The state layer opens the definition's handle for the run. Remove that
 * handle from tool/handler requirements while retaining codec services, which
 * the layer deliberately leaves for the caller to provide.
 */
export type WithoutState<
  StateRequirements,
  StateDefinitionType extends AgentState.AnyDefinition | undefined,
> = StateDefinitionType extends undefined
  ? StateRequirements
  : Exclude<StateRequirements, StateDefinitionType>;

/**
 * An autonomous loop over a `LanguageModel`, with a toolkit, optional
 * subagents, and optional on-demand skills.
 *
 * Three parameters, and `Requires` is the only one a caller normally writes:
 * it is what still has to be provided to run this agent. There used to be two
 * more, `Provides` and `Needs`, which existed solely to type a `layer` field
 * exposing the agent's own handlers — and nothing ever read it, because `make`
 * already provides them. Worse, `Requires` and `Needs` are synonyms in English
 * describing different objects: what the *agent* needs from you, and what its
 * *layer* needed from the context. Deleting the field deleted the confusion.
 *
 * Named `Instance` rather than `Agent` because this module re-exports itself
 * as the `Agent` namespace, and a type sharing that name is unreachable from
 * outside it — which `tsc` reports, correctly, as a private name escaping into
 * the declaration output. The public spelling is `Agent.Instance`.
 *
 * @category models
 * @since 0.1.0
 */
export interface Instance<
  out AgentName extends string,
  in out AgentOwnTools extends Record<string, Tool.Any>,
  /**
   * What a caller must supply to run this agent.
   *
   * Carried as a parameter so `withHandlers` can hand back a genuinely
   * narrower agent. Without it, the interface re-widens `run` back to
   * {@link WithOwnHandlers} and attaching handlers changes nothing a caller can
   * observe.
   */
  out AgentRequirements = WithOwnHandlers<AgentOwnTools>,
  in out RuntimeTools extends Record<string, Tool.Any> = AgentOwnTools,
  in out DynamicTools extends Record<string, Tool.Any> = Record<never, never>,
  out BaseRequires = AgentRequirements,
  out InterceptorRequires = never,
  out RunError extends RunFailure | CompatibilityError = RunFailure,
  out StateDefinition extends AgentState.AnyDefinition | undefined = undefined,
  in out ModelTools extends Record<string, Tool.Any> = VisibleTools<
    RuntimeTools,
    DynamicTools
  >,
> {
  readonly [TypeId]: TypeId;
  readonly [ChildTypeId]: typeof ChildTypeId;
  readonly name: AgentName;
  readonly revision: LogVocabulary.AgentRevision;
  /**
   * Canonical SHA-256 over the compiled definition's durable compatibility
   * surface: tool names and their parameter/success/failure JSON schemas,
   * subagent names and digests, the skill catalog, `codeMode`, and
   * `resultOverflow.threshold`. Computed once in `Agent.make`; read-only.
   * See `docs/conversations.md`'s "Compatibility and revisions".
   */
  readonly digest: LogVocabulary.AgentDefinitionDigest;
  /** Shown to a parent when this agent is used as a subagent. */
  readonly description?: string | undefined;
  readonly toolkit: Toolkit.Toolkit<RuntimeTools>;
  /**
   * The system prompt this agent runs with, including any skill catalog.
   *
   * Exposed so a caller restoring a session can seed a fresh `Chat`
   * identically to the one `run` would have built.
   */
  readonly instructions: string;
  readonly state: StateDefinition | undefined;
  /**
   * Observe the run as it happens: model output arrives token by token,
   * across every turn, with turn boundaries marked.
   */
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<ModelTools>,
    RunError,
    AgentRequirements
  >;

  /** Run to completion. A fold of `stream`, not a second implementation. */
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, RunError, AgentRequirements>;

  /**
   * Continue an existing conversation instead of starting one.
   *
   * `stream` and `run` open a fresh `Chat` each time, which is right for a
   * one-shot. Resuming a stored session means handing back the `Chat` it was
   * restored into, so the loop appends to that history rather than a new
   * one. See the recording and resumption helpers below.
   */
  readonly streamIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<ModelTools>,
    RunError,
    AgentRequirements
  >;

  readonly runIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, RunError, AgentRequirements>;

  /**
   * Declare handlers for this agent's tools without attaching them, purely
   * for the type checking.
   *
   * Mirrors `Toolkit.of`, and exists for the same reason: handlers defined
   * away from their agent otherwise get checked only at the point of use.
   */
  of<Handlers extends Toolkit.HandlersFrom<AgentOwnTools>>(
    handlers: Handlers,
  ): Handlers;

  /**
   * Attach this agent's own tool handlers, so it stops needing them from
   * callers.
   *
   * A method rather than a `Definition` field, mirroring
   * `toolkit.toLayer(handlers)` — the library this is built on already models
   * "definition, then handlers" that way, and matching it keeps one idiom
   * instead of two. It also sidesteps a real inference problem: expressed as
   * an optional `Definition.handlers` with a defaulted type parameter, the
   * parameter resolves to its `undefined` default rather than to the supplied
   * object, and the field stops type-checking.
   *
   * The handlers' own requirements are not swallowed: a handler that reads
   * `OrderRepo` still surfaces `OrderRepo` in {@link WithoutOwnHandlers},
   * because those are the application's to provide.
   *
   * A handler is also the typed execution authority for its operation. Check
   * live availability, authorization, and durable approval here; keep the
   * advertised tool definition stable unless its schema genuinely changes.
   *
   * Calling it again replaces the handlers rather than layering a second set
   * underneath, so the tools advertised always have exactly one handler each.
   */
  withHandlers<Handlers extends Toolkit.HandlersFrom<AgentOwnTools>>(
    handlers: Handlers,
  ): Instance<
    AgentName,
    AgentOwnTools,
    WithoutOwnHandlers<BaseRequires, AgentOwnTools> | InterceptorRequires,
    RuntimeTools,
    DynamicTools,
    WithoutOwnHandlers<BaseRequires, AgentOwnTools>,
    InterceptorRequires,
    RunError,
    StateDefinition,
    ModelTools
  >;

  /**
   * Give something a say at the loop's named seams.
   *
   * `interception.ts` is where the seams and their permissions are written
   * down; this is only how one is attached. Attaching is the same shape of
   * decision as `Conversation.make` with a recording policy: a call whose consequence is in the
   * type — the agent this returns requires whatever the interceptor's seams
   * require, and an agent that never calls it requires exactly what it
   * required before and runs the same code, because the loop checks for the
   * interceptor's absence and skips the seams rather than calling no-ops.
   *
   * It is deliberately not a `Context.Reference` holding a do-nothing
   * interceptor. That shape reads well and makes forgetting invisible: a
   * caller who never provides one gets plausible behaviour, a run with no
   * policy applied, and no signal anywhere that a policy was meant to
   * apply. Here forgetting is a compile
   * error, and there is no way to be intercepted without having said so.
   *
   * Calling it again replaces the interceptor rather than stacking a second
   * one under it, matching {@link withHandlers}. Two
   * interceptors at one seam would need a composition order, and every order
   * is wrong for somebody; composing them is the caller's job, where the
   * intent is known.
   */
  intercepting<const I extends object>(
    interceptor: I & Interception.Interceptor<Interception.Services<I>>,
  ): Instance<
    AgentName,
    AgentOwnTools,
    BaseRequires | Interception.Services<I>,
    RuntimeTools,
    DynamicTools,
    BaseRequires,
    Interception.Services<I>,
    RunError,
    StateDefinition,
    ModelTools
  >;
}

/**
 * The minimum a subagent must expose, with its name preserved as a literal.
 *
 * {@link Any} erases the name to `string`; this keeps it, which is what lets
 * a parent's tool record be keyed `task_<child>` at the type level instead of
 * collapsing to `Record<string, Tool.Any>`.
 *
 * `R` is a real type parameter defaulting to `unknown` rather than `any`.
 * Effect declares `Effect<out A, out E, out R>`, so `unknown` accepts every
 * agent in that covariant position exactly as `any` did — but unlike `any` it
 * does not poison inference downstream. That difference is what lets
 * `Subagent.Services` recover a child's real services instead of collapsing
 * them, which is what makes delegation propagate requirements at all.
 *
 * This branded view is declared directly rather than as `extends
 * Instance<Name, any, R, …>`. TypeScript will not recover `R` through that
 * interface inheritance, while the brand ensures only agents created by
 * {@link make} can cross this boundary.
 *
 * @category utility types
 * @since 0.1.0
 */
export interface Child<
  ChildName extends string = string,
  R = unknown,
  RunError extends RunFailure | CompatibilityError = RunFailure,
> {
  readonly [ChildTypeId]: typeof ChildTypeId;
  readonly name: ChildName;
  readonly revision: LogVocabulary.AgentRevision;
  readonly digest: LogVocabulary.AgentDefinitionDigest;
  readonly description?: string | undefined;
  readonly run: (input: Prompt.RawInput) => Effect.Effect<Result, RunError, R>;
}

/**
 * Any agent, regardless of its toolkit.
 *
 * Declared by instantiating {@link Agent} with `any`, which is how Effect
 * declares `Tool.Any`. Heterogeneous collections — a parent's `subagents`
 * list — cannot preserve each member's `Tools`, and pretending otherwise with
 * `never` would make every real agent unassignable.
 *
 * Use it only as a constraint — for a collection whose members' requirements
 * genuinely do not matter to the caller. `Definition.subagents` deliberately
 * does *not*: it captures a tuple of {@link Child} instead, because erasing a
 * child's `R` there produced a parent that compiled without its children's
 * services and failed the first time the model delegated.
 *
 * @category utility types
 * @since 0.1.0
 */
export interface Any extends Child {
  readonly [TypeId]: TypeId;
}

/**
 * Extract an agent's name.
 *
 * @category utility types
 * @since 0.1.0
 */
export type Name<A> =
  A extends Instance<
    infer _Name,
    infer _Own,
    infer _R,
    infer _Runtime,
    infer _Dynamic,
    infer _Base,
    infer _Interceptor,
    infer _Error,
    infer _State,
    infer _Model
  >
    ? _Name
    : never;

/**
 * Extract an agent's tool record.
 *
 * @category utility types
 * @since 0.1.0
 */
export type Tools<A> =
  A extends Instance<
    infer _Name,
    infer _Own,
    infer _R,
    infer _Runtime,
    infer _Dynamic,
    infer _Base,
    infer _Interceptor,
    infer _Error,
    infer _State,
    infer _Model
  >
    ? _Model
    : never;

/** Extract only the tools supplied by the agent definition's toolkit. */
export type OwnTools<A> =
  A extends Instance<
    infer _Name,
    infer _Own,
    infer _R,
    infer _Runtime,
    infer _Dynamic,
    infer _Base,
    infer _Interceptor,
    infer _Error,
    infer _State,
    infer _Model
  >
    ? _Own
    : never;

/**
 * Extract what still has to be provided to run an agent.
 *
 * @category utility types
 * @since 0.1.0
 */
export type Requires<A> =
  A extends Instance<
    infer _Name,
    infer _Own,
    infer _R,
    infer _Runtime,
    infer _Dynamic,
    infer _Base,
    infer _Interceptor,
    infer _Error,
    infer _State,
    infer _Model
  >
    ? _R
    : never;

/** Extract the typed failure channel of an agent run. */
export type Error<A> =
  A extends Instance<
    infer _Name,
    infer _Own,
    infer _R,
    infer _Runtime,
    infer _Dynamic,
    infer _Base,
    infer _Interceptor,
    infer RunError,
    infer _State,
    infer _Model
  >
    ? RunError
    : never;

/**
 * @category guards
 * @since 0.1.0
 */
export const isAgent = (u: unknown): u is Any =>
  Predicate.isObject(u) &&
  u[TypeId] === TypeId &&
  ChildTypeId in u &&
  hasProtocol<unknown, RunFailure>(u);

/** Every tool visible to the model after declarative capabilities compile. */
export type CompiledTools<
  Own extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Child>,
  Skills extends ReadonlyArray<AgentSkill.Skill>,
  OverflowPolicy extends ResultOverflow.Policy | undefined = undefined,
> = Own &
  Subagent.Tools<Children> &
  AgentSkill.Tools<Skills> &
  ResultOverflow.Tools<OverflowPolicy>;

/** Every statically-defined and runtime-discovered tool visible to a run. */
export type VisibleTools<
  StaticTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
> = StaticTools & DynamicTools;

/**
 * Preserve the declarative capability types while assembling their runtime
 * toolkits. The overload is the construction contract: generated subagent,
 * skill, and overflow-reader toolkits are derived from the same `Children`,
 * `Skills`, and `resultOverflow` values, and `validateGeneratedToolNames`
 * rejects collisions before this is called.
 */
function mergeCompiledToolkit<
  Tools extends Record<string, Tool.Any>,
  const Children extends ReadonlyArray<Child>,
  const Skills extends ReadonlyArray<AgentSkill.Skill>,
  const OverflowPolicy extends ResultOverflow.Policy | undefined = undefined,
>(
  own: Toolkit.Toolkit<Tools>,
  children: Children | undefined,
  skills: Skills | undefined,
  resultOverflow: OverflowPolicy | undefined,
  delegation: Toolkit.Any | undefined,
  loader: Toolkit.Any | undefined,
  reader: Toolkit.Any | undefined,
): Toolkit.Toolkit<CompiledTools<Tools, Children, Skills, OverflowPolicy>>;
function mergeCompiledToolkit(
  own: Toolkit.Any,
  _children: ReadonlyArray<Child> | undefined,
  _skills: ReadonlyArray<AgentSkill.Skill> | undefined,
  _resultOverflow: ResultOverflow.Policy | undefined,
  delegation: Toolkit.Any | undefined,
  loader: Toolkit.Any | undefined,
  reader: Toolkit.Any | undefined,
): Toolkit.Any {
  return Toolkit.merge(
    own,
    ...(delegation === undefined ? [] : [delegation]),
    ...(loader === undefined ? [] : [loader]),
    ...(reader === undefined ? [] : [reader]),
  );
}

type ChildNames<Children extends ReadonlyArray<Child>> = {
  [K in keyof Children]: Children[K]['name'];
};

type HasDuplicates<Values extends ReadonlyArray<string>> =
  Values extends readonly [
    infer Head extends string,
    ...infer Tail extends ReadonlyArray<string>,
  ]
    ? Head extends Tail[number]
      ? true
      : HasDuplicates<Tail>
    : false;

type GeneratedNames<
  Children extends ReadonlyArray<Child>,
  Skills extends ReadonlyArray<AgentSkill.Skill>,
> =
  | (number extends Children['length']
      ? never
      : `task_${Children[number]['name']}`)
  | (number extends Skills['length']
      ? never
      : Skills extends readonly []
        ? never
        : typeof AgentSkill.TOOL_NAME);

type DefinitionCollision<
  Own extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Child>,
  Skills extends ReadonlyArray<AgentSkill.Skill>,
> =
  HasDuplicates<ChildNames<Children>> extends true
    ? 'duplicate subagent names'
    : Extract<keyof Own, GeneratedNames<Children, Skills>> extends never
      ? never
      : Extract<keyof Own, GeneratedNames<Children, Skills>>;

type CollisionFreeDefinition<
  Own extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Child>,
  Skills extends ReadonlyArray<AgentSkill.Skill>,
> = [DefinitionCollision<Own, Children, Skills>] extends [never]
  ? unknown
  : {
      readonly __generatedToolNameCollision__: DefinitionCollision<
        Own,
        Children,
        Skills
      >;
    };

type DynamicDefinition<Sources extends ReadonlyArray<DynamicToolkit.Any>> =
  Sources extends readonly []
    ? { readonly dynamicTools?: undefined }
    : { readonly dynamicTools: Sources };

/**
 * @category constructors
 * @since 0.1.0
 */
export const make = <
  const AgentName extends string,
  AgentTools extends Record<string, Tool.Any>,
  const Children extends ReadonlyArray<Child> = readonly [],
  const Skills extends ReadonlyArray<AgentSkill.Skill> = readonly [],
  StopR = never,
  TurnControlR = never,
  const StateDefinition extends AgentState.AnyDefinition | undefined =
    undefined,
  const DynamicSources extends ReadonlyArray<DynamicToolkit.Any> = readonly [],
  const OverflowPolicy extends ResultOverflow.Policy | undefined = undefined,
  const CodeModeOption extends CodeMode.Option<AgentTools> = false,
>(
  definition: Definition<
    AgentName,
    AgentTools,
    Children,
    Skills,
    StopR,
    TurnControlR,
    StateDefinition,
    DynamicSources,
    OverflowPolicy,
    CodeModeOption
  > &
    CollisionFreeDefinition<AgentTools, Children, Skills> &
    DynamicDefinition<DynamicSources>,
): Instance<
  AgentName,
  AgentTools,
  | WithOwnHandlersForState<AgentTools, StateDefinition>
  | Subagent.Services<Children>
  | ResultOverflow.Services<OverflowPolicy>
  | StopR
  | TurnControlR
  | DynamicToolkit.Services<DynamicSources>
  | CodeMode.Requires<CodeModeOption>
  | (StateDefinition extends AgentState.AnyDefinition
      ? AgentState.Services<StateDefinition>
      : never),
  CompiledTools<AgentTools, Children, Skills, OverflowPolicy>,
  DynamicToolkit.Tools<DynamicSources>,
  | WithOwnHandlersForState<AgentTools, StateDefinition>
  | Subagent.Services<Children>
  | ResultOverflow.Services<OverflowPolicy>
  | StopR
  | TurnControlR
  | DynamicToolkit.Services<DynamicSources>
  | CodeMode.Requires<CodeModeOption>
  | (StateDefinition extends AgentState.AnyDefinition
      ? AgentState.Services<StateDefinition>
      : never),
  never,
  RunFailure,
  StateDefinition,
  CodeMode.ModelTools<
    VisibleTools<
      CompiledTools<AgentTools, Children, Skills, OverflowPolicy>,
      DynamicToolkit.Tools<DynamicSources>
    >,
    CodeModeOption
  >
> => {
  type RuntimeTools = CompiledTools<
    AgentTools,
    Children,
    Skills,
    OverflowPolicy
  >;
  type DynamicTools = DynamicToolkit.Tools<DynamicSources>;
  type RunTools = VisibleTools<RuntimeTools, DynamicTools>;
  type ModelTools = CodeMode.ModelTools<RunTools, CodeModeOption>;
  type BaseRequires =
    | WithOwnHandlersForState<AgentTools, StateDefinition>
    | Subagent.Services<Children>
    | ResultOverflow.Services<OverflowPolicy>
    | StopR
    | TurnControlR
    | DynamicToolkit.Services<DynamicSources>
    | CodeMode.Requires<CodeModeOption>
    | (StateDefinition extends AgentState.AnyDefinition
        ? AgentState.Services<StateDefinition>
        : never);

  if (definition.revision.trim() === '') {
    throw new Error(`Agent "${definition.name}" revision must be non-empty`);
  }
  const revision = LogVocabulary.AgentRevision.make(definition.revision);

  const stopWhen = definition.stopWhen ?? Stop.defaultCondition<ModelTools>();
  const runPolicy = RunPolicy.make(definition.runPolicy);

  // Subagents and skills are compiled into the toolkit here rather than by
  // the caller. Merging them by hand is mechanical, and the failure mode
  // when it is done wrong — a tool advertised with no handler, or a handler
  // for a tool nobody advertised — surfaces as a confusing model-facing
  // error rather than a compile error.
  const children = definition.subagents ?? [];
  const skills = definition.skills ?? [];

  for (const child of children) {
    if (!isAgent(child)) {
      throw new Error(
        `Agent "${definition.name}" subagent "${child.name}" was not created by Agent.make`,
      );
    }
  }
  validateGeneratedToolNames(
    definition.toolkit,
    children,
    skills,
    definition.resultOverflow !== undefined,
    definition.codeMode,
  );
  for (const child of children) {
    if (child.revision.trim() === '') {
      throw new Error(`Agent "${child.name}" revision must be non-empty`);
    }
  }

  const delegation =
    children.length > 0 ? SubagentRuntime.delegateTo(...children) : undefined;
  const delegationToolNames = new Set(
    children.map((child) => Subagent.toolName(child.name)),
  );
  const loader = skills.length > 0 ? AgentSkill.loader(skills) : undefined;
  const overflow =
    definition.resultOverflow === undefined
      ? undefined
      : ResultOverflow.reader(definition.resultOverflow);

  const toolkit = mergeCompiledToolkit(
    definition.toolkit,
    definition.subagents,
    definition.skills,
    definition.resultOverflow,
    delegation?.toolkit,
    loader?.toolkit,
    overflow?.toolkit,
  );

  // The skill catalog joins the system prompt: a model cannot ask for a
  // skill it does not know exists. The bodies stay out, so the prefix is
  // still identical across turns and still cacheable.
  const catalog = AgentSkill.catalog(skills);
  const instructions =
    catalog === ''
      ? definition.instructions
      : `${definition.instructions}\n\n${catalog}`;

  // `false` opts out; anything else gets the default policy. Compaction is
  // on by default because the failure it prevents — a long conversation
  // dying on a context-window error the provider will report identically
  // forever — is one every agent eventually hits, and the recovery never
  // varies.
  const compaction =
    definition.compaction === false
      ? undefined
      : (definition.compaction ?? Compaction.defaultPolicy);

  // A policy without `contextWindow` compiles and runs; it just never
  // proactively compacts, because `compactAhead` in the loop has nothing to
  // compare an estimate against. That is silent by construction — the run
  // looks identical to one that never overflows, right up until it does — so
  // it is logged once per run rather than left for someone to notice in a
  // postmortem.
  const compactionWarning =
    compaction !== undefined && compaction.contextWindow === undefined
      ? Effect.logWarning(
          'Proactive compaction is inactive: Compaction.Policy.contextWindow is not set. Without it there is no context-window ceiling to estimate against, so this agent only compacts reactively, after a provider rejects a prompt as too long. Set contextWindow to enable proactive compaction.',
        ).pipe(
          Effect.annotateLogs({
            'vesper.component': 'compaction',
            'vesper.agent.name': definition.name,
          }),
        )
      : Effect.void;

  // The loop itself lives in `internal/loop.ts`. `makeEntry` closes over the
  // compiled definition once per agent; the session and interceptor are
  // passed per run through `Wiring`, not looked up, so nothing in the loop
  // depends on what happens to be in the context.
  const entryFor = makeEntry<
    RuntimeTools,
    DynamicSources,
    BaseRequires,
    StopR,
    TurnControlR,
    StateDefinition,
    CodeModeOption
  >({
    name: definition.name,
    concurrency: definition.concurrency,
    codeMode: definition.codeMode,
    resultOverflow: definition.resultOverflow,
    resultBounds: definition.resultBounds,
    state: definition.state,
    dynamicTools: definition.dynamicTools,
    instructions,
    toolkit,
    delegation,
    delegationToolNames,
    loader,
    overflow,
    compaction,
    compactionWarning,
    stopWhen,
    nextTurn: definition.nextTurn,
    runPolicy,
  });

  // Computed once, from exactly the parts of `definition` that change what
  // durable history means — see `internal/definition-digest.ts` for the
  // full inclusion/exclusion rationale.
  const digest = DefinitionDigest.compute({
    name: definition.name,
    tools: definition.toolkit.tools,
    subagents: children.map((child) => ({
      name: child.name,
      digest: child.digest,
    })),
    skills: skills.map((skill) => ({ name: skill.name })),
    codeMode: definition.codeMode,
    resultOverflowThreshold: definition.resultOverflow?.threshold,
  });

  return fromParts<
    AgentName,
    AgentTools,
    RuntimeTools,
    DynamicTools,
    ModelTools,
    BaseRequires,
    never,
    RunFailure,
    StateDefinition
  >({
    name: definition.name,
    revision,
    digest,
    description: definition.description,
    ownToolkit: definition.toolkit,
    toolkit,
    instructions,
    state: definition.state,
    interceptor: undefined,
    runPolicy,
    entry: entryFor,
  });
};

const validateGeneratedToolNames = (
  own: Toolkit.Any,
  children: ReadonlyArray<Child>,
  skills: ReadonlyArray<AgentSkill.Skill>,
  resultOverflow: boolean,
  codeMode: boolean | { readonly except: ReadonlyArray<string> } | undefined,
): void => {
  const ownNames = new Set(Object.keys(own.tools));
  const generated = new Set<string>();

  const reserve = (name: string, source: string): void => {
    if (ownNames.has(name)) {
      throw new Error(
        `Agent generated tool "${name}" from ${source}, but the toolkit already defines it`,
      );
    }
    if (generated.has(name)) {
      throw new Error(
        `Agent generated duplicate tool "${name}" from ${source}`,
      );
    }
    generated.add(name);
  };

  for (const child of children) {
    reserve(Subagent.toolName(child.name), `subagent "${child.name}"`);
  }
  if (skills.length > 0) {
    reserve(AgentSkill.TOOL_NAME, 'skills');
  }
  if (resultOverflow) {
    reserve(ResultOverflow.TOOL_NAME, 'resultOverflow');
  }
  if (CodeMode.isEnabled(codeMode)) {
    reserve(CodeMode.TOOL_NAME, 'codeMode');
    const excepted = new Set(codeMode === true ? [] : codeMode.except);
    for (const [name, tool] of Object.entries(own.tools)) {
      if (
        tool.needsApproval !== undefined &&
        tool.needsApproval !== false &&
        !excepted.has(name)
      ) {
        throw new Error(
          `Agent codeMode brokers approval-gated tool "${name}"; add it to codeMode.except`,
        );
      }
    }
    if (codeMode !== true) {
      // The compile-time `keyof Tools & string` check on `except` degrades
      // to this when the array is built dynamically, the same trade
      // `subagents`/`skills` collision checking makes.
      for (const name of codeMode.except) {
        if (!ownNames.has(name)) {
          throw new Error(
            `Agent codeMode excepts tool "${name}", but the toolkit does not define it`,
          );
        }
      }
    }
  }
};

/**
 * Everything an {@link Agent} exposes except the methods.
 *
 * `entry` is a function of the run's {@link Wiring} rather than four
 * already-built closures, which is what Phase 5 changed and why. A child
 * session has to reach *inside* the loop — the dispatch seam consults it, the
 * turn boundary drains signals through it, delegation opens grandchildren from
 * it — and wrapping the finished stream from outside, which is all
 * durable recording does, cannot reach any of that. An interceptor has the
 * same requirement for the same reason, which is why Phase 7 needed no new
 * mechanism. Threading them as arguments keeps them lexical: there is no
 * ambient value to provide, forget, or default.
 *
 * The interceptor is a field here as well as a member of `Wiring` because it
 * belongs to the agent, not to the run: `intercepting` replaces the field,
 * every entry point reads it from here, and nothing downstream can end up
 * intercepted by something the agent's type did not declare.
 */
interface Parts<
  PartsName extends string,
  PartsOwnTools extends Record<string, Tool.Any>,
  RuntimeTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  ModelTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorRequires,
  RunError,
  StateDefinition extends AgentState.AnyDefinition | undefined,
> {
  readonly name: PartsName;
  readonly revision: LogVocabulary.AgentRevision;
  readonly digest: LogVocabulary.AgentDefinitionDigest;
  readonly description: string | undefined;
  readonly ownToolkit: Toolkit.Toolkit<PartsOwnTools>;
  readonly toolkit: Toolkit.Toolkit<RuntimeTools>;
  readonly instructions: string;
  readonly state: StateDefinition | undefined;
  readonly interceptor:
    | Interception.Interceptor<InterceptorRequires>
    | undefined;
  readonly runPolicy: RunPolicy.Limits;
  readonly entry: <WiringRequires>(
    wiring: Wiring<WiringRequires, DynamicTools>,
  ) => Entry<ModelTools, BaseRequires | WiringRequires, RunError>;
}

/**
 * Attach the methods to a set of parts.
 *
 * Recursive so that `withHandlers` survives on the agent it returns: the
 * result is a whole `Agent`, not a stripped-down record that happens to be
 * callable.
 */
const fromParts = <
  PartsName extends string,
  PartsOwnTools extends Record<string, Tool.Any>,
  RuntimeTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  ModelTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorRequires,
  RunError extends RunFailure | CompatibilityError = RunFailure,
  StateDefinition extends AgentState.AnyDefinition | undefined = undefined,
>(
  parts: Parts<
    PartsName,
    PartsOwnTools,
    RuntimeTools,
    DynamicTools,
    ModelTools,
    BaseRequires,
    InterceptorRequires,
    RunError,
    StateDefinition
  >,
): Instance<
  PartsName,
  PartsOwnTools,
  BaseRequires | InterceptorRequires,
  RuntimeTools,
  DynamicTools,
  BaseRequires,
  InterceptorRequires,
  RunError,
  StateDefinition,
  ModelTools
> => {
  // How every entry point below reaches the loop: the agent's interceptor,
  // and whichever session that entry point has. Read from `parts` rather than
  // passed along, so the only way to be intercepted is to have said so on this
  // agent.
  const wiring = (
    session: AgentLog.Session | undefined,
    options?: Omit<
      Wiring<InterceptorRequires, DynamicTools>,
      'session' | 'interceptor'
    >,
  ): Wiring<InterceptorRequires, DynamicTools> => ({
    session,
    interceptor: parts.interceptor,
    ...options,
  });

  // Keep one stable stream span around every run. Stream spans are important
  // here: a run is pull-based, so an Effect span around stream construction
  // would end before the provider, tools, and finalizers are consumed.
  const runStream = <A, E, R>(
    stream: Stream.Stream<A, E, R>,
    recorded: boolean,
    session?: AgentLog.Session,
  ) =>
    stream.pipe(
      Stream.withSpan('Agent.run', {
        attributes: {
          'vesper.agent.name': parts.name,
          'vesper.agent.revision': parts.revision,
          'vesper.run.recorded': recorded,
          ...(session === undefined
            ? {}
            : {
                'vesper.conversation.id': session.conversationId,
              }),
        },
      }),
    );

  // The unrecorded entry, which is what the four public entry points are.
  // Construct the entry per invocation: its state layer owns exactly one
  // handle, so sharing an entry would share ephemeral state across runs.
  const plain = (input: Prompt.RawInput) =>
    runStream(parts.entry(wiring(undefined)).stream(input), false);
  const plainIn = (chat: Chat.Service, input: Prompt.RawInput) =>
    runStream(parts.entry(wiring(undefined)).streamIn(chat, input), false);
  const publicPlain = {
    stream: plain,
    streamIn: plainIn,
    run: (input: Prompt.RawInput) => foldToResult(plain(input)),
    runIn: (chat: Chat.Service, input: Prompt.RawInput) =>
      foldToResult(plainIn(chat, input)),
  };

  // The writer half of the log, and the reason `@sunfall/vesper-durable`'s
  // checkpointer could be deleted rather than merely shrunk. Shared by every
  // Conversation execution, which differs only in how the session was claimed
  // — a branch writes its marker during `open`, so by the time this runs the
  // difference is already inside `session.history` and nothing below can
  // forget to account for it.
  //
  // It goes through `parts.entry(...).streamIn` rather than `stream`, which is
  // the whole difference from an ordinary recorded run: `stream` opens a fresh
  // `Chat` seeded with instructions alone, and this seeds one with the
  // conversation the records describe. Everything else — recording, signals,
  // the dispatch seam — is shared by every Conversation execution.
  const continuingStream = (
    session: AgentLog.Session,
    input: Prompt.RawInput,
    runtime?: RunPolicyRuntime.Runtime,
  ) =>
    Stream.unwrap(
      Effect.gen(function* () {
        // Read before the run writes anything. `session.history` is the scan
        // `open` already performed, so this cannot see this run's own records —
        // which is the same timing property the recovery index rests on.
        //
        // `usageFrom` reads the whole history and `messagesFrom` reads only the
        // active path within it. That asymmetry is deliberate and is the reason
        // neither of them takes a pre-filtered array: an abandoned branch costs
        // money that still has to be reported and says nothing the model should
        // be shown. `branch.ts` carries the table.
        const total = session.usage;
        const prior = {
          input: Math.max(0, total.input - session.inheritedUsage.input),
          output: Math.max(0, total.output - session.inheritedUsage.output),
        };
        const latest = session.latestTurnUsage;
        const chat = yield* Chat.fromPrompt(
          Prompt.concat(
            Prompt.make([{ role: 'system', content: parts.instructions }]),
            AgentHistory.messagesFrom(session.history),
          ),
        );

        const entry = parts.entry(
          wiring(session, {
            runtime,
            startRun: (effective) =>
              AgentLog.start(session, {
                agent: parts.name,
                revision: parts.revision,
                digest: parts.digest,
                input: effective,
              }),
            lastTurn:
              latest === undefined
                ? undefined
                : {
                    inputTokens: latest.input,
                    outputTokens: latest.output,
                  },
          }),
        );
        // Persist per-run usage, then expose lifetime conversation usage to the
        // caller. Mapping before the recording sink would write cumulative totals
        // into every run and count the same history again on the next open.
        return AgentLog.record(session, entry.streamIn(chat, input)).pipe(
          Stream.map((event) =>
            event._tag === 'Completed'
              ? AgentEventRuntime.completed(
                  event.text,
                  event.steps,
                  {
                    input: prior.input + event.usage.input,
                    output: prior.output + event.usage.output,
                  },
                  event.outcome,
                  event.response,
                )
              : event,
          ),
        );
      }),
    );

  // The claim differs for ordinary continuation, branch, and fork; the stream
  // after that claim does not.
  const streamFrom = <E, R>(
    opener: Effect.Effect<AgentLog.Session, E, R>,
    input: Prompt.RawInput,
    runtime?: RunPolicyRuntime.Runtime,
  ) =>
    Stream.unwrap(
      Effect.map(opener, (session) =>
        runStream(continuingStream(session, input, runtime), true, session),
      ),
    );

  const agent: Instance<
    PartsName,
    PartsOwnTools,
    BaseRequires | InterceptorRequires,
    RuntimeTools,
    DynamicTools,
    BaseRequires,
    InterceptorRequires,
    RunError,
    StateDefinition,
    ModelTools
  > = {
    [TypeId]: TypeId,
    [ChildTypeId]: ChildTypeId,
    name: parts.name,
    revision: parts.revision,
    digest: parts.digest,
    description: parts.description,
    toolkit: parts.toolkit,
    instructions: parts.instructions,
    state: parts.state,
    ...publicPlain,

    of: (handlers) => handlers,

    // The one assertion in this file, and — unlike the two that shipped bugs —
    // it can only widen. Those subtracted a term from a requirement channel,
    // so getting one wrong dropped a service and the call site still compiled;
    // this one erases the interceptor's `R` *internally* while the public
    // signature adds it, so the worst a mistake here can do is demand a
    // service nobody needs, loudly, at every call site.
    //
    // It is sound because Effect resolves requirements from the ambient
    // context at run time and never from the static `R`: the returned agent's
    // type forces the caller to provide `R`, so `R` is in the context by the
    // time any seam runs, and the dispatch seam additionally provides the
    // captured context inward because `handle`'s signature will not carry a
    // requirement. `assertions.test.ts`'s interception cases pin the widening,
    // because a silently-narrowed version of this would compile.
    intercepting: <const I extends object>(
      interceptor: I & Interception.Interceptor<Interception.Services<I>>,
    ) =>
      fromParts<
        PartsName,
        PartsOwnTools,
        RuntimeTools,
        DynamicTools,
        ModelTools,
        BaseRequires,
        Interception.Services<I>,
        RunError,
        StateDefinition
      >({
        ...parts,
        interceptor,
      }),

    withHandlers: (handlers) => {
      const own = parts.ownToolkit.toLayer(handlers);

      // The same narrowing assertion `make` uses, for the same reason:
      // `Layer.provide` cannot show inference that the handler requirement is
      // discharged, but it demonstrably is. Asserting `WithoutOwnHandlers`
      // removes a term from every caller rather than adding one.
      //
      // Rebuilt from `parts` rather than from the current agent, so calling
      // this twice replaces the handlers instead of stacking a second set
      // beneath the first — and so a session reaches the loop through the
      // rebuilt entry rather than being sealed behind the old one.
      return fromParts<
        PartsName,
        PartsOwnTools,
        RuntimeTools,
        DynamicTools,
        ModelTools,
        WithoutOwnHandlers<BaseRequires, PartsOwnTools>,
        InterceptorRequires,
        RunError,
        StateDefinition
      >({
        ...parts,
        entry: <WiringRequires>(
          incoming: Wiring<WiringRequires, DynamicTools>,
        ) =>
          provideEntry(parts.entry(incoming), own) as Entry<
            ModelTools,
            WithoutOwnHandlers<BaseRequires, PartsOwnTools> | WiringRequires,
            RunError
          >,
      });
    },
  };
  const protocol: AgentProtocol<
    BaseRequires | InterceptorRequires,
    RunError,
    ModelTools
  > = {
    stream: (conversationId, input, options) => {
      const compatibility = {
        agent: parts.name,
        revision: parts.revision,
        digest: parts.digest,
      };
      const policy = options?.policy;
      const opener =
        options?.forkConversationId === undefined
          ? AgentLog.open(conversationId, {
              compatibility,
              ...(options?.branchFrom === undefined
                ? {}
                : { branchFrom: options.branchFrom }),
              ...(options?.pendingWait === undefined
                ? {}
                : { pendingWait: options.pendingWait }),
            })
          : AgentLog.fork(
              conversationId,
              options.branchFrom ?? LogOffset.START,
              options.forkConversationId,
              compatibility,
              options.pendingWait,
            );
      return streamFrom(
        Effect.flatMap(opener, (session) =>
          policy === undefined
            ? Effect.succeed(session)
            : Effect.map(Effect.context(), (context) =>
                AgentLog.withRecordingPolicy(
                  session,
                  RecordingPolicyRuntime.compile(policy, context),
                ),
              ),
        ),
        input,
      );
    },
    run: (
      runtime: RunPolicyRuntime.Runtime,
      session: AgentLog.Session | undefined,
      input: Prompt.RawInput,
    ) =>
      session === undefined
        ? foldToResult(
            runStream(
              parts.entry(wiring(undefined, { runtime })).stream(input),
              false,
            ),
          )
        : Effect.andThen(
            AgentLog.assertCompatible(session, {
              agent: parts.name,
              revision: parts.revision,
              digest: parts.digest,
            }),
            Effect.suspend(() => {
              const completed = session.completed;
              return completed === undefined
                ? foldToResult(
                    runStream(
                      continuingStream(session, input, runtime),
                      true,
                      session,
                    ),
                  )
                : Effect.gen(function* () {
                    const response =
                      completed.response === undefined
                        ? undefined
                        : yield* Schema.decodeEffect(Prompt.Prompt)(
                            completed.response,
                          ).pipe(Effect.orDie);
                    // Narrowed by `outcome` rather than spread once: the
                    // 'success' branch of `Result` carries an `exhausted`
                    // field 'cancelled' does not, so an object literal typed
                    // from the wider `'success' | 'cancelled'` union no
                    // longer structurally matches either member. A durably
                    // resumed `Completed` carries no `exhausted` figure of
                    // its own — that marker lives only on the live event
                    // stream and a result produced in the same run, not on
                    // `RunSettled.resume.completed` — so it is never set here.
                    //
                    // Annotated `: Result` rather than `satisfies Result`:
                    // `satisfies` keeps the narrower literal type of each
                    // branch, which then fails to unify with `foldToResult`'s
                    // full `Result` in the ternary above this `Effect.gen`.
                    const result: Result =
                      completed.outcome === 'success'
                        ? {
                            outcome: 'success',
                            text: completed.text,
                            steps: completed.steps,
                            usage: completed.usage,
                            ...(response === undefined ? {} : { response }),
                          }
                        : {
                            outcome: 'cancelled',
                            text: completed.text,
                            steps: completed.steps,
                            usage: completed.usage,
                            ...(response === undefined ? {} : { response }),
                          };
                    return result;
                  });
            }),
          ),
  };
  return registerProtocol(agent, protocol);
};

export * as Agent from './agent.js';
