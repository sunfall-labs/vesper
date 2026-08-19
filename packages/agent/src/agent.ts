import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Effect,
  Exit,
  Layer,
  Option,
  Predicate,
  Ref,
  Schema,
  Stream,
} from 'effect';
import {
  AiError,
  Chat,
  LanguageModel,
  Prompt,
  Response,
  type Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Compaction } from './compaction.js';
import { CodeMode } from './code-mode.js';
import { CompatibilityError } from './conversation-error.js';
import { ContextWindow } from './context-window.js';
import { ToolDispatch } from './dispatch.js';
import { AgentEvents } from './event.js';
import { AgentHistory } from './history.js';
import { CompactionRuntime } from './internal/compaction.js';
import { AgentEventRuntime } from './internal/event.js';
import { foldToResult } from './internal/fold-to-result.js';
import {
  hasProtocol,
  register as registerProtocol,
} from './internal/protocol.js';
import type { AgentProtocol } from './internal/protocol.js';
import type { Interception } from './interception.js';
import { DynamicToolkit } from './dynamic-toolkit.js';
import * as AgentLog from './log.js';
import { RecordingPolicyRuntime } from './recording-policy-runtime.js';
import { ResultOverflow } from './result-overflow.js';
import { RunPolicy } from './run-policy.js';
import { RunPolicyRuntime } from './run-policy-runtime.js';
import * as AgentSkill from './skill.js';
import { Stop } from './stop.js';
import { AgentState } from './state.js';
import { Subagent } from './subagent.js';
import { SubagentRuntime } from './subagent-runtime.js';
import * as Observability from './internal/observability.js';

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
  Name extends string,
  Tools extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Child> = readonly [],
  Skills extends ReadonlyArray<AgentSkill.Skill> = readonly [],
  StopR = never,
  StateDefinition extends AgentState.AnyDefinition | undefined = undefined,
  DynamicSources extends ReadonlyArray<DynamicToolkit.Any> = readonly [],
  OverflowPolicy extends ResultOverflow.Policy | undefined = undefined,
  CodeModeEnabled extends boolean = false,
> {
  readonly name: Name;
  /** Stable application-defined compatibility revision for durable history. */
  readonly revision: string;
  readonly description?: string;
  /** Prepended as a system message on every run. */
  readonly instructions: string;
  readonly toolkit: Toolkit.Toolkit<Tools>;
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
        CompiledTools<Tools, Children, Skills, OverflowPolicy>,
        DynamicToolkit.Tools<DynamicSources>
      >,
      CodeModeEnabled
    >,
    StopR
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
  /** Replace direct tool advertisement with the isolated `exec` tool. */
  readonly codeMode?: CodeModeEnabled;
}

/**
 * What a completed run produced.
 *
 * Schema-modelled because a result is the natural thing to checkpoint, hand
 * to a workflow, or return over a transport, and every one of those needs a
 * codec rather than a bare interface.
 */
export const Result = Schema.Struct({
  outcome: Schema.Literals(['success', 'cancelled', 'suspended']),
  /** Concatenated text of the final turn. Empty when `outcome` is `suspended`. */
  text: Schema.String,
  steps: Schema.Natural,
  usage: Stop.Usage,
  /**
   * Tool calls durably parked on a `needsApproval` gate.
   *
   * Present only when `outcome` is `suspended`. Resolve each one through
   * `Conversation.resolveApproval` and call `run` again to continue.
   */
  pendingApprovals: Schema.optionalKey(
    Schema.Array(AgentEvents.PendingApproval),
  ),
});
export interface Result extends Schema.Struct.Type<typeof Result.fields> {}

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
export type WithOwnHandlers<Tools extends Record<string, Tool.Any>> =
  | LanguageModel.LanguageModel
  | Tool.HandlersFor<Tools>
  // An empty toolkit is fine here: `Tools[keyof Tools]` is `never`, and
  // `HandlerServices<never>` resolves to `never` rather than widening to
  // `any`. An earlier comment claimed otherwise; `types.test.ts` pins the
  // truth, because the difference decides whether callers inherit an `any`.
  | Tool.HandlerServices<Tools[keyof Tools]>
  | Tool.ResultDecodingServices<Tools[keyof Tools]>
  // The live model stream yields decoded tool-call parameters. Recording the
  // provider-facing form requires encoding those parameters again, so expose
  // the corresponding codec services at the same boundary.
  | ParameterEncodingServices<Tools>;

type ParameterEncodingServices<Tools extends Record<string, Tool.Any>> =
  Tools[keyof Tools] extends infer Candidate
    ? Candidate extends Tool.Any
      ? Tool.ParametersSchema<Candidate>['EncodingServices']
      : never
    : never;

type WithOwnHandlersForState<
  Tools extends Record<string, Tool.Any>,
  StateDefinition extends AgentState.AnyDefinition | undefined,
> =
  | LanguageModel.LanguageModel
  | Tool.HandlersFor<Tools>
  | WithoutState<Tool.HandlerServices<Tools[keyof Tools]>, StateDefinition>
  | Tool.ResultDecodingServices<Tools[keyof Tools]>
  | ParameterEncodingServices<Tools>;

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
  Requires,
  Tools extends Record<string, Tool.Any>,
> = Exclude<Requires, Tool.HandlersFor<Tools>>;

/**
 * The state layer opens the definition's handle for the run. Remove that
 * handle from tool/handler requirements while retaining codec services, which
 * the layer deliberately leaves for the caller to provide.
 */
export type WithoutState<
  Requires,
  Definition extends AgentState.AnyDefinition | undefined,
> = Definition extends undefined ? Requires : Exclude<Requires, Definition>;

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
  out Name extends string,
  in out OwnTools extends Record<string, Tool.Any>,
  /**
   * What a caller must supply to run this agent.
   *
   * Carried as a parameter so `withHandlers` can hand back a genuinely
   * narrower agent. Without it, the interface re-widens `run` back to
   * {@link WithOwnHandlers} and attaching handlers changes nothing a caller can
   * observe.
   */
  out Requires = WithOwnHandlers<OwnTools>,
  in out RuntimeTools extends Record<string, Tool.Any> = OwnTools,
  in out DynamicTools extends Record<string, Tool.Any> = {},
  out BaseRequires = Requires,
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
  readonly name: Name;
  readonly revision: LogVocabulary.AgentRevision;
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
  ) => Stream.Stream<AgentEvents.ObservedEvent<ModelTools>, RunError, Requires>;

  /** Run to completion. A fold of `stream`, not a second implementation. */
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, RunError, Requires>;

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
  ) => Stream.Stream<AgentEvents.ObservedEvent<ModelTools>, RunError, Requires>;

  readonly runIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, RunError, Requires>;

  /**
   * Declare handlers for this agent's tools without attaching them, purely
   * for the type checking.
   *
   * Mirrors `Toolkit.of`, and exists for the same reason: handlers defined
   * away from their agent otherwise get checked only at the point of use.
   */
  of<Handlers extends Toolkit.HandlersFrom<OwnTools>>(
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
  withHandlers<Handlers extends Toolkit.HandlersFrom<OwnTools>>(
    handlers: Handlers,
  ): Instance<
    Name,
    OwnTools,
    WithoutOwnHandlers<BaseRequires, OwnTools> | InterceptorRequires,
    RuntimeTools,
    DynamicTools,
    WithoutOwnHandlers<BaseRequires, OwnTools>,
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
    Name,
    OwnTools,
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
  Name extends string = string,
  R = unknown,
  RunError extends RunFailure | CompatibilityError = RunFailure,
> {
  readonly [ChildTypeId]: typeof ChildTypeId;
  readonly name: Name;
  readonly revision: LogVocabulary.AgentRevision;
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
export interface Any extends Child<string, unknown> {
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
  hasProtocol<unknown, RunFailure, Record<string, Tool.Any>>(u);

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
  const Name extends string,
  Tools extends Record<string, Tool.Any>,
  const Children extends ReadonlyArray<Child> = readonly [],
  const Skills extends ReadonlyArray<AgentSkill.Skill> = readonly [],
  StopR = never,
  const StateDefinition extends AgentState.AnyDefinition | undefined =
    undefined,
  const DynamicSources extends ReadonlyArray<DynamicToolkit.Any> = readonly [],
  const OverflowPolicy extends ResultOverflow.Policy | undefined = undefined,
  const CodeModeEnabled extends boolean = false,
>(
  definition: Definition<
    Name,
    Tools,
    Children,
    Skills,
    StopR,
    StateDefinition,
    DynamicSources,
    OverflowPolicy,
    CodeModeEnabled
  > &
    CollisionFreeDefinition<Tools, Children, Skills> &
    DynamicDefinition<DynamicSources>,
): Instance<
  Name,
  Tools,
  | WithOwnHandlersForState<Tools, StateDefinition>
  | Subagent.Services<Children>
  | ResultOverflow.Services<OverflowPolicy>
  | StopR
  | DynamicToolkit.Services<DynamicSources>
  | CodeMode.Requires<CodeModeEnabled>
  | (StateDefinition extends AgentState.AnyDefinition
      ? AgentState.Services<StateDefinition>
      : never),
  CompiledTools<Tools, Children, Skills, OverflowPolicy>,
  DynamicToolkit.Tools<DynamicSources>,
  | WithOwnHandlersForState<Tools, StateDefinition>
  | Subagent.Services<Children>
  | ResultOverflow.Services<OverflowPolicy>
  | StopR
  | DynamicToolkit.Services<DynamicSources>
  | CodeMode.Requires<CodeModeEnabled>
  | (StateDefinition extends AgentState.AnyDefinition
      ? AgentState.Services<StateDefinition>
      : never),
  never,
  RunFailure,
  StateDefinition,
  CodeMode.ModelTools<
    VisibleTools<
      CompiledTools<Tools, Children, Skills, OverflowPolicy>,
      DynamicToolkit.Tools<DynamicSources>
    >,
    CodeModeEnabled
  >
> => {
  type RuntimeTools = CompiledTools<Tools, Children, Skills, OverflowPolicy>;
  type DynamicTools = DynamicToolkit.Tools<DynamicSources>;
  type RunTools = VisibleTools<RuntimeTools, DynamicTools>;
  type ModelTools = CodeMode.ModelTools<RunTools, CodeModeEnabled>;
  type BaseRequires =
    | WithOwnHandlersForState<Tools, StateDefinition>
    | Subagent.Services<Children>
    | ResultOverflow.Services<OverflowPolicy>
    | StopR
    | DynamicToolkit.Services<DynamicSources>
    | CodeMode.Requires<CodeModeEnabled>
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
  // proactively compacts, because `compactAhead` below has nothing to compare
  // an estimate against. That is silent by construction — the run looks
  // identical to one that never overflows, right up until it does — so it is
  // logged once per run rather than left for someone to notice in a postmortem.
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

  // Everything below is built per run wiring rather than once per agent, which
  // is the shape Phases 5, 6 and 7 all needed. Four things vary with it: the
  // toolkit a turn dispatches through, whether the loop drains signals,
  // whether delegation opens child sessions, and who gets a say at the seams.
  // A closure over `wiring` is how that stays lexical — both the session and
  // the interceptor are passed in, not looked up, so nothing here depends on
  // what happens to be in the context.
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
        () =>
          Effect.gen(function* () {
            const hidden = yield* ToolDispatch.gate(runToolkit, {
              agent: definition.name,
              conversationId: session?.conversationId,
              interceptor,
              runtime,
              unmeteredToolNames: delegationToolNames,
              arbitration,
            });
            const visible = yield* CodeMode.toolkit(
              hidden,
              wiring.codeState ?? CodeMode.emptyState,
            );
            return yield* ToolDispatch.gate(Effect.succeed(visible), {
              agent: definition.name,
              session,
              arbitration,
            });
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
          if (runtime !== undefined) yield* runtime.modelCall;
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
                  );
                  if (encodedPart.type === 'finish') {
                    yield* Observability.usage(encodedPart.usage);
                  }
                  if (runtime !== undefined) yield* runtime.remainingMillis;
                }),
              ),
              Stream.map(
                ({ part, encodedPart }): AgentEvents.Event<ModelTools> => ({
                  _tag: 'Part',
                  step,
                  // Effect's mapped tool-part union is not idempotent under
                  // this compiled intersection, although the toolkit value is
                  // exactly the one Chat used to decode the part.
                  part: part as Response.StreamPart<ModelTools>,
                  encodedPart,
                }),
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
        if (compaction?.contextWindow === undefined) return undefined;

        const over = yield* CompactionRuntime.shouldCompact(
          Prompt.concat(yield* Ref.get(chat.history), Prompt.make(input)),
          compaction.contextWindow,
          compaction,
          yield* Ref.get(lastTurn),
        );

        if (!over) return undefined;
        const summarized = yield* compactWithBudget(chat, compaction);
        if (summarized !== undefined && runtime !== undefined) {
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
      WithOwnHandlers<RuntimeTools> | StopR | InterceptorR
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
          if (runtime !== undefined) yield* runtime.turn;

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
            input,
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
                          if (initial.emitted) return Stream.fail(error);

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
                            Prompt.concat(historyBefore, Prompt.make(input)),
                          );
                          const summarized = yield* compactWithBudget(
                            chat,
                            compaction,
                          );
                          // Retrying an unchanged prompt repeats the same refusal
                          // and can loop provider-side work for no gain.
                          if (summarized === undefined)
                            return Stream.fail(error);
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

              const wanted = yield* stopWhen({
                step,
                toolCalls: seen.toolCalls,
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
              const pendingApprovals = seen.pendingApprovals;
              if (pendingApprovals.length > 0 && session === undefined) {
                return Stream.fail(
                  approvalRequiresConversationError(pendingApprovals),
                );
              }

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
                pendingApprovals.length > 0 ||
                (wanted && steers.length === 0 && !drained.backlog);
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
                          )
                        : pendingApprovals.length > 0
                          ? AgentEventRuntime.suspended(
                              completedSteps,
                              seen.text,
                              totals,
                              pendingApprovals,
                            )
                          : AgentEventRuntime.completed(
                              seen.text,
                              completedSteps,
                              totals,
                              'success',
                            ),
                    ),
                  )
                : Stream.concat(
                    announced,
                    // Later turns continue the stored conversation; the tool
                    // results `streamText` appended are already in history, so
                    // nothing new is supplied unless a steer arrived.
                    turn(
                      chat,
                      usage,
                      toolCallCounts,
                      lastTurn,
                      step + 1,
                      steeringInput(steers),
                    ),
                  );
            }),
          );

          const responsiveCancel =
            session === undefined
              ? Effect.never
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

          return opened.pipe(
            Stream.concat(guarded.pipe(Stream.interruptWhen(responsiveCancel))),
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
        WithOwnHandlers<RuntimeTools> | StopR | InterceptorR
      >;

    const streamIn = (chat: Chat.Service, input: Prompt.RawInput) =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (definition.codeMode === true && wiring.codeState === undefined) {
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
              WithOwnHandlers<RuntimeTools> | StopR | InterceptorR
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
              WithOwnHandlers<RuntimeTools> | StopR | InterceptorR
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
              WithOwnHandlers<RuntimeTools> | StopR | InterceptorR
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
              if (wiring.startRun !== undefined)
                yield* wiring.startRun(effective);
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
                  // Resolved once for every pending approval below: the run
                  // toolkit may include dynamic sources whose resolution is
                  // real work (an MCP discovery round-trip), and each
                  // approval only needs the one resolved snapshot.
                  const approvalToolkit = Effect.succeed(yield* runToolkit);
                  const stillPendingApprovals: AgentEvents.PendingApproval[] =
                    yield* Effect.forEach(
                      session.suspendedToolCalls.filter(
                        (call) => call.wait === ToolDispatch.APPROVAL_WAIT,
                      ),
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
                            ReadonlyArray<AgentEvents.PendingApproval>
                          >([]);
                        }
                        // Re-decoded against the tool's current parameter
                        // schema rather than surfaced from the durable
                        // encoded form: a caller re-reading this pending
                        // approval sees the same typed value the first
                        // suspension did, not the toolkit's wire encoding of
                        // it.
                        return Effect.map(
                          ToolDispatch.decodeSuspendedRequest(
                            approvalToolkit,
                            call.name,
                            call.request,
                          ),
                          (input) => [
                            {
                              toolCallId: call.toolCallId,
                              toolName: call.name,
                              input,
                            },
                          ],
                        );
                      },
                    ).pipe(Effect.map((batches) => batches.flat()));
                  if (stillPendingApprovals.length > 0) {
                    return Stream.make(
                      AgentEventRuntime.suspended(
                        0,
                        // No model call happened on this path — the run was
                        // refused before turn 1 — so there is no partial text
                        // to preserve.
                        '',
                        wiring.initialUsage ?? { input: 0, output: 0 },
                        stillPendingApprovals,
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
                    WithOwnHandlers<RuntimeTools> | StopR | InterceptorR
                  >;
                }),
              ),
            ) as Stream.Stream<
              AgentEvents.Event<ModelTools>,
              RunFailure,
              WithOwnHandlers<RuntimeTools> | StopR | InterceptorR
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

  return fromParts<
    Name,
    Tools,
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
  if (skills.length > 0) reserve(AgentSkill.TOOL_NAME, 'skills');
  if (resultOverflow) reserve(ResultOverflow.TOOL_NAME, 'resultOverflow');
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
 * Split out from {@link Parts} because they are the part that varies with the
 * session and the rest is not. `withHandlers` composes over an entry;
 * durable recording replaces one.
 */
interface Entry<Tools extends Record<string, Tool.Any>, Requires, Error> {
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<AgentEvents.Event<Tools>, Error, Requires>;
  readonly streamIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Stream.Stream<AgentEvents.Event<Tools>, Error, Requires>;
}

/** Provide one implementation layer across both primitive stream shapes. */
const provideEntry = <
  Tools extends Record<string, Tool.Any>,
  Requires,
  Error,
  Provided,
  LayerError,
  LayerRequires,
>(
  entry: Entry<Tools, Requires, Error>,
  layer: Layer.Layer<Provided, LayerError, LayerRequires>,
): Entry<
  Tools,
  Exclude<Requires, Provided> | LayerRequires,
  Error | LayerError
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
interface Wiring<
  InterceptorRequires = never,
  DynamicTools extends Record<string, Tool.Any> = {},
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
  Name extends string,
  OwnTools extends Record<string, Tool.Any>,
  RuntimeTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  ModelTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorRequires,
  RunError,
  StateDefinition extends AgentState.AnyDefinition | undefined,
> {
  readonly name: Name;
  readonly revision: LogVocabulary.AgentRevision;
  readonly description: string | undefined;
  readonly ownToolkit: Toolkit.Toolkit<OwnTools>;
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
  Name extends string,
  OwnTools extends Record<string, Tool.Any>,
  RuntimeTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  ModelTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorRequires,
  RunError extends RunFailure | CompatibilityError = RunFailure,
  StateDefinition extends AgentState.AnyDefinition | undefined = undefined,
>(
  parts: Parts<
    Name,
    OwnTools,
    RuntimeTools,
    DynamicTools,
    ModelTools,
    BaseRequires,
    InterceptorRequires,
    RunError,
    StateDefinition
  >,
): Instance<
  Name,
  OwnTools,
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
    Name,
    OwnTools,
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
        Name,
        OwnTools,
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
        Name,
        OwnTools,
        RuntimeTools,
        DynamicTools,
        ModelTools,
        WithoutOwnHandlers<BaseRequires, OwnTools>,
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
            WithoutOwnHandlers<BaseRequires, OwnTools> | WiringRequires,
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
      const compatibility = { agent: parts.name, revision: parts.revision };
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
                : Effect.succeed<Result>(completed);
            }),
          ),
  };
  return registerProtocol(agent, protocol);
};

interface TurnState {
  text: string;
  toolCalls: Response.ToolCallPartEncoded[];
  usage: Response.FinishPartEncoded['usage'] | undefined;
  emitted: boolean;
  started: boolean;
  /** Decoded call params seen this turn, keyed by tool call id. */
  callsById: Map<string, { readonly name: string; readonly input: unknown }>;
  /** `tool-approval-request` parts observed this turn, in provider order. */
  pendingApprovals: AgentEvents.PendingApproval[];
}

const emptyTurnState = (): TurnState => ({
  text: '',
  toolCalls: [],
  usage: undefined,
  emitted: false,
  started: false,
  callsById: new Map(),
  pendingApprovals: [],
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
const observe = <Tools extends Record<string, Tool.Any>>(
  state: TurnState,
  decoded: Response.StreamPart<Tools>,
  encoded: Response.StreamPartEncoded,
): void => {
  state.emitted = true;
  switch (encoded.type) {
    case 'text-delta':
      state.text += encoded.delta;
      break;
    case 'tool-call':
      state.toolCalls.push(encoded);
      if (decoded.type === 'tool-call') {
        state.callsById.set(decoded.id, {
          name: decoded.name,
          input: decoded.params,
        });
      }
      break;
    case 'tool-approval-request': {
      const call = state.callsById.get(encoded.toolCallId);
      state.pendingApprovals.push({
        toolCallId: encoded.toolCallId,
        toolName: call?.name ?? '',
        input: call?.input,
      });
      break;
    }
    case 'finish':
      state.usage = encoded.usage;
      break;
    default:
      break;
  }
};

type EncodableToolCall<Tools extends Record<string, Tool.Any>> = Extract<
  Response.StreamPart<Tools>,
  { readonly type: 'tool-call' }
>;
type EncodableToolResult<Tools extends Record<string, Tool.Any>> = Extract<
  Response.StreamPart<Tools>,
  { readonly type: 'tool-result' }
>;
type EncodableFile<Tools extends Record<string, Tool.Any>> = Extract<
  Response.StreamPart<Tools>,
  { readonly type: 'file' }
>;
const StandardPart = Schema.Union([
  Response.TextStartPart,
  Response.TextDeltaPart,
  Response.TextEndPart,
  Response.ReasoningStartPart,
  Response.ReasoningDeltaPart,
  Response.ReasoningEndPart,
  Response.ToolParamsStartPart,
  Response.ToolParamsDeltaPart,
  Response.ToolParamsEndPart,
  Response.ToolApprovalRequestPart,
  Response.DocumentSourcePart,
  Response.UrlSourcePart,
  Response.ResponseMetadataPart,
  Response.FinishPart,
  Response.ErrorPart,
]);
type StandardPart = typeof StandardPart.Type;

const encodePartError = (error: Schema.SchemaError): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'encodePart',
    reason: AiError.InvalidOutputError.fromSchemaError(error),
  });

const encodeStandardPart = (
  part: StandardPart,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> =>
  Schema.encodeEffect(StandardPart)(part).pipe(
    Effect.mapError(encodePartError),
  );

const encodeToolCall = <Tools extends Record<string, Tool.Any>>(
  part: EncodableToolCall<Tools>,
  toolkit: Toolkit.WithHandler<Tools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> => {
  const tool = Object.hasOwn(toolkit.tools, part.name)
    ? toolkit.tools[part.name]
    : undefined;
  if (tool === undefined) {
    return Effect.fail(
      new AiError.AiError({
        module: 'Agent',
        method: 'encodePart',
        reason: new AiError.InvalidOutputError({
          description: `Model emitted unknown tool ${part.name}`,
        }),
      }),
    );
  }
  // The toolkit lookup erases the name-to-schema relationship. The runtime
  // schema is still the one that produced `part.params`; this assertion only
  // restores its encoding-service requirement for the generic helper, just as
  // dispatch restores the handler relationship.
  const encoded = Schema.encodeEffect(tool.parametersSchema)(
    part.params,
  ) as Effect.Effect<
    unknown,
    Schema.SchemaError,
    ParameterEncodingServices<Tools>
  >;
  return encoded.pipe(
    Effect.mapError(encodePartError),
    Effect.map((params) => ({
      type: 'tool-call',
      id: part.id,
      name: part.name,
      params,
      providerExecuted: part.providerExecuted,
    })),
  );
};

const encodeToolResult = <Tools extends Record<string, Tool.Any>>(
  part: EncodableToolResult<Tools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> =>
  // The decoded result can be a substituted value that deliberately does not
  // satisfy the tool schema. `encodedResult` is already the exact
  // provider-facing value in this case.
  Effect.succeed({
    type: 'tool-result',
    id: part.id,
    name: part.name,
    result: part.encodedResult,
    isFailure: part.isFailure,
    providerExecuted: part.providerExecuted,
    preliminary: part.preliminary,
  });

const encodeFile = <Tools extends Record<string, Tool.Any>>(
  part: EncodableFile<Tools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> =>
  Schema.encodeEffect(Response.FilePart)(part).pipe(
    Effect.mapError(encodePartError),
  );

const assertPartEncodingStrategy = (part: never): never => {
  throw new Error(`Unhandled response part encoding strategy: ${String(part)}`);
};

/** Encode a decoded model part before it reaches observers or persistence. */
const encodePart = <Tools extends Record<string, Tool.Any>>(
  part: Response.StreamPart<Tools>,
  toolkit: Toolkit.WithHandler<Tools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> => {
  switch (part.type) {
    case 'tool-call':
      return encodeToolCall(part, toolkit);
    case 'tool-result':
      return encodeToolResult(part);
    case 'file':
      return encodeFile(part);
    case 'text-start':
    case 'text-delta':
    case 'text-end':
    case 'reasoning-start':
    case 'reasoning-delta':
    case 'reasoning-end':
    case 'tool-params-start':
    case 'tool-params-delta':
    case 'tool-params-end':
    case 'tool-approval-request':
    case 'source':
    case 'response-metadata':
    case 'finish':
    case 'error':
      return encodeStandardPart(part);
    default:
      return assertPartEncodingStrategy(part);
  }
};

const approvalRequiresConversationError = (
  pendingApprovals: ReadonlyArray<AgentEvents.PendingApproval>,
): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'run',
    reason: new AiError.InvalidRequestError({
      description:
        `Tool call${pendingApprovals.length === 1 ? '' : 's'} ` +
        `${pendingApprovals.map((approval) => `"${approval.toolName}" (${approval.toolCallId})`).join(', ')} ` +
        'require approval, which can only be resolved durably. Bind this ' +
        'agent to a Conversation and call Conversation.resolveApproval ' +
        'instead of running it directly.',
      metadata: {
        pendingApprovals: pendingApprovals.map((approval) => ({
          toolCallId: approval.toolCallId,
          toolName: approval.toolName,
        })),
      },
    }),
  });

const normalizeProviderError = (
  error: unknown,
  partMetadata?: Record<string, unknown> | undefined,
): AiError.AiError => {
  if (AiError.isAiError(error)) return error;

  const structured = describeProviderError(error, partMetadata);
  const common = {
    description: structured.description,
    metadata: structured.metadata,
  };

  return new AiError.AiError({
    module: 'Agent',
    method: 'streamText',
    reason:
      /(?:context(?: length| window)?(?: is)?(?: exceeded| too long)|maximum context length|model_context_window_exceeded|prompt is too long|too many tokens|input is too long)/i.test(
        structured.description,
      )
        ? new AiError.InvalidRequestError({
            ...common,
            constraint: Compaction.CONTEXT_OVERFLOW,
          })
        : new AiError.UnknownError(common),
  });
};

const describeProviderError = (
  error: unknown,
  partMetadata?: Record<string, unknown> | undefined,
): {
  readonly description: string;
  readonly metadata: Record<string, unknown>;
} => {
  if (!Predicate.isObject(error)) {
    return { description: String(error), metadata: partMetadata ?? {} };
  }

  const value = error;
  const metadata = Predicate.isObject(value.metadata) ? value.metadata : {};
  const code =
    value.code ??
    (typeof value.type === 'string' && value.type !== 'error'
      ? value.type
      : undefined);
  const details = [code, value.message, value.error]
    .map((part) =>
      typeof part === 'string'
        ? part
        : part === undefined
          ? ''
          : stringifyErrorPart(part),
    )
    .filter((part) => part !== '')
    .join(': ');

  return {
    description: details === '' ? stringifyErrorPart(value) : details,
    metadata: {
      ...partMetadata,
      ...metadata,
      ...(value.code === undefined ? {} : { code: value.code }),
      ...(typeof value.type !== 'string' || value.type === 'error'
        ? {}
        : { type: value.type }),
    },
  };
};

const stringifyErrorPart = (value: unknown): string => {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
};

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
  if (calls.length === 0) return current;
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

export * as Agent from './agent.js';
