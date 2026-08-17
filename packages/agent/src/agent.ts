import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Tail } from '@sunfall/vesper-log/tail';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Exit, Layer, Option, Ref, Schema, Stream } from 'effect';
import {
  AiError,
  Chat,
  LanguageModel,
  Prompt,
  type Response,
  type Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Compaction } from './compaction.js';
import { ContextWindow } from './context-window.js';
import { ToolDispatch } from './dispatch.js';
import { AgentEvents } from './event.js';
import { AgentHistory } from './history.js';
import { hasProtocol, register, Session, StateCleanup } from './internal.js';
import type { Interception } from './interception.js';
import { AgentLog } from './log.js';
import { RecordingPolicy } from './recording-policy.js';
import { RecordingPolicyRuntime } from './recording-policy-runtime.js';
import { RunPolicy } from './run-policy.js';
import { RunPolicyRuntime } from './run-policy-runtime.js';
import { Skill } from './skill.js';
import { Stop } from './stop.js';
import { Subagent } from './subagent.js';
import { SubagentRuntime } from './subagent-runtime.js';

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
// Recording is opt-in through `recordingTo` and `resume`, the only two things
// here that mention `LogStore`.

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
  Children extends ReadonlyArray<Named> = readonly [],
  Skills extends ReadonlyArray<Skill.Skill> = readonly [],
  StopR = never,
> {
  readonly name: Name;
  /** Stable application-defined compatibility revision for durable history. */
  readonly revision: string;
  readonly description?: string;
  /** Prepended as a system message on every run. */
  readonly instructions: string;
  readonly toolkit: Toolkit.Toolkit<Tools>;
  readonly stopWhen?: Stop.StopCondition<
    CompiledTools<Tools, Children, Skills>,
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
}

/**
 * What a completed run produced.
 *
 * Schema-modelled because a result is the natural thing to checkpoint, hand
 * to a workflow, or return over a transport, and every one of those needs a
 * codec rather than a bare interface.
 */
export const Result = Schema.Struct({
  outcome: Schema.Literals(['success', 'cancelled']),
  /** Concatenated text of the final turn. */
  text: Schema.String,
  steps: Schema.Number,
  usage: Stop.Usage,
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
  | Tool.ResultDecodingServices<Tools[keyof Tools]>;

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
  out BaseRequires = Requires,
  out InterceptorRequires = never,
  out RunError extends AiError.AiError | AgentLog.CompatibilityError =
    AiError.AiError,
> {
  readonly [TypeId]: TypeId;
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

  /**
   * Observe the run as it happens: model output arrives token by token,
   * across every turn, with turn boundaries marked.
   */
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<RuntimeTools>,
    RunError,
    Requires
  >;

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
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<RuntimeTools>,
    RunError,
    Requires
  >;

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
    WithoutOwnHandlers<BaseRequires, OwnTools>,
    InterceptorRequires,
    RunError
  >;

  /**
   * Record every run into `conversationId`'s conversation log.
   *
   * All four entry points on the returned agent append as they go — text
   * coalesced, tool calls and their outcomes as they settle, turn boundaries,
   * delegations, signal deliveries, completion, and how the run settled — and
   * emit exactly the events they emitted before. {@link streamFrom} reads that
   * log back.
   *
   * A recording run is also the only kind that can be **steered or
   * cancelled** (a signal is addressed to a conversation, and this is what
   * gives a run one) and the only kind whose tool calls **survive a crash**
   * (the dispatch seam serves outcomes an unsettled earlier run recorded
   * rather than re-running the tool).
   *
   * **This is how logging stays optional without being hidden.** `LogStore`
   * is not a requirement of `run`, so every existing call site is untouched;
   * and it is not an ambient `Context.Reference` with a no-op default either,
   * because a defaulted reference is how persistence gets hidden — a caller
   * who forgot gets plausible behaviour and no signal. Here the choice
   * is a method call, and its consequence is in the type: the agent this
   * returns requires `LogStore.Service`, and a caller who has not provided
   * one does not compile. Nothing about a recording agent is discoverable
   * only at runtime.
   *
   * Claiming the stream fences whatever producer held it last, so two
   * concurrent runs against one conversation do not interleave: the older
   * one fails its next append rather than writing a history that never
   * happened.
   */
  recordingTo(
    conversationId: string,
  ): Instance<
    Name,
    OwnTools,
    BaseRequires | LogStore.Service | InterceptorRequires,
    RuntimeTools,
    BaseRequires | LogStore.Service,
    InterceptorRequires,
    RunError | AgentLog.CompatibilityError
  >;
  recordingTo<const P extends object>(
    conversationId: string,
    policy: P & RecordingPolicy.Policy<RecordingPolicy.Services<P>>,
  ): Instance<
    Name,
    OwnTools,
    | BaseRequires
    | LogStore.Service
    | InterceptorRequires
    | RecordingPolicy.Services<P>,
    RuntimeTools,
    BaseRequires | LogStore.Service | RecordingPolicy.Services<P>,
    InterceptorRequires,
    RunError | AgentLog.CompatibilityError
  >;

  /**
   * Continue a recorded conversation, rebuilding it from its own log.
   *
   * The counterpart of {@link recordingTo}: that one writes a conversation
   * down, this one picks it back up. History comes from the records — model
   * text, tool calls and their results, steers that redirected it — seeded
   * under this agent's current instructions, and the run continues from the
   * next turn. A conversation that does not exist yet starts as one, so a
   * caller does not branch on first contact.
   *
   * **This is what makes the log a durability mechanism.** A run that crashed
   * mid-conversation resumes without re-asking the provider for turns it
   * already completed and without re-running the tool calls those turns made
   * — the two things provider-seam checkpointing could offer only the first
   * of, and only by replaying the whole loop to get there.
   *
   * Everything {@link recordingTo} says about recording applies: the run is
   * written down as it happens, it can be steered or cancelled, and its tool
   * calls survive a crash. `LogStore.Service` is in the type for the same
   * reason.
   *
   * `usage` on the result is cumulative across the whole conversation rather
   * than this run alone. A resumed conversation that reset the count would
   * under-report every turn after the first.
   *
   * Call it on the agent, not on one already returned by {@link recordingTo}:
   * this names a conversation itself, so chaining the two would open two
   * claims and write every record twice.
   */
  readonly resume: (
    conversationId: string,
    input: Prompt.RawInput,
  ) => Effect.Effect<
    Result,
    RunError | AgentLog.CompatibilityError,
    Requires | LogStore.Service
  >;

  /**
   * Continue a recorded conversation from an **earlier point in it**.
   *
   * {@link resume} picks a conversation up at its end; this picks it up in the
   * middle. Everything after `at` becomes an abandoned branch — still in the
   * log, still readable, no longer in the prompt — and the run continues as if
   * the conversation had stopped there. That is "edit an earlier message and
   * re-run", and it is the one capability the log was missing against a
   * session model that stores a tree.
   *
   * `at` is the offset of the last record to keep, **inclusive**, as
   * {@link streamFrom} hands them out. Branching to a point that is already
   * the conversation's end is legal and does nothing observable, which is what
   * lets a caller pass a position it read without first checking whether
   * anything came after it.
   *
   * **It costs one record.** No history is copied and no offsets are rewritten
   * — a `ConversationRecord.BranchedFrom` marker names the point, and the same
   * stream carries every branch. So the conversation stays one stream with one
   * id: one tail follows it across a branch, `usage` still counts what the
   * abandoned attempts cost, and a steer that was already delivered stays
   * delivered rather than arriving a second time on the new path.
   *
   * The consequence of one stream is that branches are **sequential, not
   * concurrent**. The store gives a conversation one writer, so this cannot
   * run two variants of a prompt side by side; it moves where the single
   * conversation continues from. {@link forkFrom} is the answer to that, and
   * it makes the opposite trade. Everything {@link recordingTo} says about
   * recording otherwise applies unchanged.
   */
  readonly branchFrom: (
    conversationId: string,
    at: LogOffset.Offset,
    input: Prompt.RawInput,
  ) => Effect.Effect<
    Result,
    RunError | AgentLog.CompatibilityError,
    Requires | LogStore.Service
  >;

  /**
   * Start a **new conversation** from a prefix of an existing one, and run it.
   *
   * {@link branchFrom} re-roots one conversation; this makes a second one. The
   * fork is seeded with the ancestor's records up to and including `at` — the
   * same prefix `branchFrom` would have continued from — and then lives its
   * own life under `forkConversationId`.
   *
   * **This is the concurrent case, and the only reason to prefer it.** A
   * branch shares the ancestor's stream and therefore its single writer, so
   * two branches cannot run at once. Two forks are two streams with two
   * producer claims, so they can: side-by-side variants of a prompt, explored
   * at the same time, neither fencing the other.
   *
   * The ancestor is read and not claimed. A fork does not disturb it: it
   * writes nothing into it, does not fence a run that is live on it, and
   * leaves no record in it that a fork was taken — so records the ancestor
   * appends after the fork are the ancestor's alone and never appear here.
   * The relationship is therefore not navigable from the ancestor's side, and
   * that is the trade against `branchFrom`'s single tail.
   *
   * The other trade is cost and identity. A branch costs one record; a fork
   * copies the prefix, so it is O(prefix) records and the copy has its own
   * offsets. `usage` on the result counts this conversation, which now starts
   * at the fork — the ancestor's spend stays reported against the ancestor,
   * where it was billed, rather than being counted a second time by every fork
   * taken from it.
   *
   * Forking into an id that already holds a conversation is a defect rather
   * than an append into it. Everything {@link recordingTo} says about
   * recording applies to the fork.
   */
  readonly forkFrom: (
    conversationId: string,
    at: LogOffset.Offset,
    forkConversationId: string,
    input: Prompt.RawInput,
  ) => Effect.Effect<
    Result,
    RunError | AgentLog.CompatibilityError,
    Requires | LogStore.Service
  >;

  /**
   * Give something a say at the loop's named seams.
   *
   * `interception.ts` is where the seams and their permissions are written
   * down; this is only how one is attached. Attaching is the same shape of
   * decision as {@link recordingTo}: a method call whose consequence is in the
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
   * one under it, matching {@link withHandlers} and {@link recordingTo}. Two
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
    BaseRequires,
    Interception.Services<I>,
    RunError
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
 * Declared structurally rather than as `extends Instance<Name, any, R, …>`.
 * Effect declares `Tool.Any` by instantiation, but that convention is for
 * types used as *constraints*; this one is an inference source, and TypeScript
 * will not recover `R` through interface inheritance — every child built with
 * `withHandlers` fell back to `unknown`. {@link Any} keeps the instantiated
 * form, because it really is only ever a constraint.
 *
 * @category utility types
 * @since 0.1.0
 */
export interface Named<Name extends string = string, R = unknown> {
  readonly name: Name;
  readonly revision: LogVocabulary.AgentRevision;
  readonly description?: string | undefined;
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, R>;
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
 * does *not*: it captures a tuple of {@link Named} instead, because erasing a
 * child's `R` there produced a parent that compiled without its children's
 * services and failed the first time the model delegated.
 *
 * @category utility types
 * @since 0.1.0
 */
// oxlint-disable-next-line no-explicit-any
export interface Any extends Instance<any, any, any, any, any, any, any> {}

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
    infer _Base,
    infer _Interceptor,
    infer _Error
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
    infer _Base,
    infer _Interceptor,
    infer _Error
  >
    ? _Runtime
    : never;

/** Extract only the tools supplied by the agent definition's toolkit. */
export type OwnTools<A> =
  A extends Instance<
    infer _Name,
    infer _Own,
    infer _R,
    infer _Runtime,
    infer _Base,
    infer _Interceptor,
    infer _Error
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
    infer _Base,
    infer _Interceptor,
    infer _Error
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
    infer _Base,
    infer _Interceptor,
    infer RunError
  >
    ? RunError
    : never;

/**
 * @category guards
 * @since 0.1.0
 */
export const isAgent = (u: unknown): u is Any =>
  typeof u === 'object' &&
  u !== null &&
  (u as { readonly [TypeId]?: unknown })[TypeId] === TypeId &&
  hasProtocol(u);

/** Every tool visible to the model after declarative capabilities compile. */
export type CompiledTools<
  Own extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Named>,
  Skills extends ReadonlyArray<Skill.Skill>,
> = Own & Subagent.Tools<Children> & Skill.Tools<Skills>;

type ChildNames<Children extends ReadonlyArray<Named>> = {
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
  Children extends ReadonlyArray<Named>,
  Skills extends ReadonlyArray<Skill.Skill>,
> =
  | (number extends Children['length']
      ? never
      : `task_${Children[number]['name']}`)
  | (number extends Skills['length']
      ? never
      : Skills extends readonly []
        ? never
        : typeof Skill.TOOL_NAME);

type DefinitionCollision<
  Own extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Named>,
  Skills extends ReadonlyArray<Skill.Skill>,
> =
  HasDuplicates<ChildNames<Children>> extends true
    ? 'duplicate subagent names'
    : Extract<keyof Own, GeneratedNames<Children, Skills>> extends never
      ? never
      : Extract<keyof Own, GeneratedNames<Children, Skills>>;

type CollisionFreeDefinition<
  Own extends Record<string, Tool.Any>,
  Children extends ReadonlyArray<Named>,
  Skills extends ReadonlyArray<Skill.Skill>,
> = [DefinitionCollision<Own, Children, Skills>] extends [never]
  ? unknown
  : {
      readonly __generatedToolNameCollision__: DefinitionCollision<
        Own,
        Children,
        Skills
      >;
    };

/**
 * @category constructors
 * @since 0.1.0
 */
export const make = <
  const Name extends string,
  Tools extends Record<string, Tool.Any>,
  const Children extends ReadonlyArray<Named> = readonly [],
  const Skills extends ReadonlyArray<Skill.Skill> = readonly [],
  StopR = never,
>(
  definition: Definition<Name, Tools, Children, Skills, StopR> &
    CollisionFreeDefinition<Tools, Children, Skills>,
): Instance<
  Name,
  Tools,
  WithOwnHandlers<Tools> | Subagent.Services<Children> | StopR,
  CompiledTools<Tools, Children, Skills>,
  WithOwnHandlers<Tools> | Subagent.Services<Children> | StopR,
  never,
  AiError.AiError
> => {
  type RuntimeTools = CompiledTools<Tools, Children, Skills>;
  type BaseRequires =
    | WithOwnHandlers<Tools>
    | Subagent.Services<Children>
    | StopR;

  if (definition.revision.trim() === '') {
    throw new Error(`Agent "${definition.name}" revision must be non-empty`);
  }
  const revision = LogVocabulary.AgentRevision.make(definition.revision);

  const stopWhen = definition.stopWhen ?? Stop.defaultCondition<RuntimeTools>();
  const runPolicy = RunPolicy.make(definition.runPolicy);

  // Subagents and skills are compiled into the toolkit here rather than by
  // the caller. Merging them by hand is mechanical, and the failure mode
  // when it is done wrong — a tool advertised with no handler, or a handler
  // for a tool nobody advertised — surfaces as a confusing model-facing
  // error rather than a compile error.
  const children = (definition.subagents ?? []) as Children;
  const skills = (definition.skills ?? []) as Skills;

  for (const child of children) {
    if (!isAgent(child)) {
      throw new Error(
        `Agent "${definition.name}" subagent "${child.name}" was not created by Agent.make`,
      );
    }
  }
  validateGeneratedToolNames(definition.toolkit, children, skills);
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
  const loader = skills.length > 0 ? Skill.loader(skills) : undefined;

  // `Toolkit.merge` wants `Toolkit.Any`, and while `Children` is generic TS
  // cannot reduce `ToolsByName<ToolTuple<Children>>` to `Record<string,
  // Tool.Any>` — the mapped tuple is structurally a tuple until it is
  // instantiated. The widening is safe: every element of `ToolTuple` is a
  // `Tool` by construction. The merged result is asserted back to `Tools`
  // regardless, so callers see only their own tools' requirements while the
  // agent's own layer serves the rest.
  const toolkit = Toolkit.merge(
    definition.toolkit,
    ...(delegation === undefined
      ? []
      : [delegation.toolkit as unknown as Toolkit.Any]),
    ...(loader === undefined ? [] : [loader.toolkit]),
  ) as unknown as Toolkit.Toolkit<RuntimeTools>;

  // The skill catalog joins the system prompt: a model cannot ask for a
  // skill it does not know exists. The bodies stay out, so the prefix is
  // still identical across turns and still cacheable.
  const catalog = Skill.catalog(skills);
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

  // Everything below is built per run wiring rather than once per agent, which
  // is the shape Phases 5, 6 and 7 all needed. Four things vary with it: the
  // toolkit a turn dispatches through, whether the loop drains signals,
  // whether delegation opens child sessions, and who gets a say at the seams.
  // A closure over `wiring` is how that stays lexical — both the session and
  // the interceptor are passed in, not looked up, so nothing here depends on
  // what happens to be in the context.
  const entryFor = (
    wiring: Wiring,
  ): Entry<RuntimeTools, BaseRequires, AiError.AiError> => {
    const session = wiring.session;
    const interceptor = wiring.interceptor;
    const runtime = wiring.runtime;

    // A `Toolkit` already *is* an `Effect` producing a resolved toolkit, and
    // `streamText`'s `toolkit` option takes either form, so both branches have
    // one type and `LanguageModel` resolves them identically. That is the whole
    // reason the dispatch seam needs no change to the `LanguageModel` contract.
    //
    // An agent with no dispatch seams takes the first branch. A root runtime
    // is also a seam because its tool semaphore is shared by descendants.
    const dispatching = (
      arbitration: ToolDispatch.TurnArbitration,
    ): Effect.Effect<
      Toolkit.WithHandler<RuntimeTools>,
      never,
      Tool.HandlersFor<RuntimeTools>
    > =>
      ToolDispatch.gate(toolkit, {
        agent: definition.name,
        session,
        interceptor,
        runtime,
        unmeteredToolNames: delegationToolNames,
        arbitration,
      });

    // Fixed arity rather than spreading `...(x ? [l] : [])`: the spread form
    // widens `mergeAll`'s result, which then leaks into anything that provides
    // this layer.
    const layer = Layer.mergeAll(
      delegation === undefined
        ? Layer.empty
        : delegation.layer(session, runtime),
      loader === undefined ? Layer.empty : loader.layer,
    );

    const turnOptions = (arbitration: ToolDispatch.TurnArbitration) => ({
      toolkit: dispatching(arbitration),
      concurrency: definition.concurrency,
    });

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
      AgentEvents.Event<RuntimeTools>,
      AiError.AiError,
      WithOwnHandlers<RuntimeTools>
    > =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (interceptor?.beforeModelCall !== undefined) {
            yield* interceptor.beforeModelCall({
              agent: definition.name,
              conversationId: session?.conversationId,
              step,
              attempt,
            });
          }
          if (runtime !== undefined) yield* runtime.modelCall;
          const remaining =
            runtime === undefined ? undefined : yield* runtime.remainingMillis;
          const asked = chat
            .streamText({ prompt: input, ...turnOptions(arbitration) })
            .pipe(
              Stream.mapEffect((part) =>
                part.type === 'error'
                  ? Effect.fail(
                      normalizeProviderError(part.error, part.metadata),
                    )
                  : Effect.succeed(part),
              ),
              Stream.tap((part) =>
                Effect.sync(() => {
                  seen.started = true;
                  observe(seen, part as Response.StreamPartEncoded);
                }).pipe(
                  Effect.andThen(
                    runtime === undefined
                      ? Effect.void
                      : runtime.remainingMillis,
                  ),
                ),
              ),
              Stream.map(
                (part): AgentEvents.Event<RuntimeTools> => ({
                  _tag: 'Part',
                  step,
                  part: part as Response.StreamPart<RuntimeTools>,
                }),
              ),
            );
          return remaining === undefined
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
        }),
      ) as Stream.Stream<
        AgentEvents.Event<RuntimeTools>,
        AiError.AiError,
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
    ): Effect.Effect<Prompt.RawInput, AiError.AiError> =>
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
      Compaction.Summarized | undefined,
      AiError.AiError,
      LanguageModel.LanguageModel
    > =>
      Effect.gen(function* () {
        if (compaction?.contextWindow === undefined) return undefined;

        const over = yield* Compaction.shouldCompact(
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
      Compaction.Summarized | undefined,
      AiError.AiError,
      LanguageModel.LanguageModel
    > =>
      Effect.gen(function* () {
        if (runtime === undefined)
          return yield* Compaction.compact(chat, policy);
        yield* runtime.modelCall;
        const remaining = yield* runtime.remainingMillis;
        return yield* Effect.timeoutOrElse(Compaction.compact(chat, policy), {
          duration: remaining,
          orElse: () =>
            Effect.fail(
              RunPolicyRuntime.error({
                limit: 'deadline',
                used: runPolicy.wallClockMillis,
                maximum: runPolicy.wallClockMillis,
              }),
            ),
        });
      });

    const turn = (
      chat: Chat.Service,
      usage: Ref.Ref<Stop.Usage>,
      lastTurn: Ref.Ref<ContextWindow.TurnUsage | undefined>,
      step: number,
      pending: Prompt.RawInput,
    ): Stream.Stream<
      AgentEvents.Event<RuntimeTools>,
      AiError.AiError,
      WithOwnHandlers<RuntimeTools> | StopR
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
                  Stream.catchIf(Compaction.isContextOverflow, (error) =>
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
                        if (summarized === undefined) return Stream.fail(error);
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
                          Stream.make(AgentEvents.compacted(step, summarized)),
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
              if (runtime !== undefined && seen.usage !== undefined) {
                const accounted = yield* Effect.exit(
                  runtime.addUsage({
                    input: seen.usage.inputTokens.total ?? 0,
                    output: seen.usage.outputTokens.total ?? 0,
                  }),
                );
                if (Exit.isFailure(accounted)) {
                  return Stream.concat(
                    Stream.make(AgentEvents.turnFinished(step, totals)),
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
                        ? AgentEvents.signalled(step, decision.signal)
                        : AgentEvents.signalRejected(
                            step,
                            decision.signal,
                            decision.exhaustion!,
                          ),
                    )
                    .concat(
                      drained.backlog
                        ? [
                            AgentEvents.signalBacklog(
                              step,
                              runtime?.limits.maxSignalsPerBoundary ??
                                runPolicy.maxSignalsPerBoundary,
                            ),
                          ]
                        : [],
                    ),
                ),
                Stream.make(AgentEvents.turnFinished(step, totals)),
              );

              const wanted = yield* stopWhen({
                step,
                toolCalls: seen.toolCalls,
                usage: totals,
              });

              // A steer outranks the stop condition for one more turn,
              // including a step ceiling. The ceiling is a runaway-loop guard
              // and a steer is a person asking for more work; stopping anyway
              // would consume the instruction and ignore it, which is the one
              // outcome nobody can debug. A cancel outranks everything.
              const stop =
                cancelled ||
                (wanted && steers.length === 0 && !drained.backlog);
              const completedSteps = seen.started ? step : step - 1;

              return stop
                ? Stream.concat(
                    announced,
                    Stream.make(
                      AgentEvents.completed(
                        seen.text,
                        completedSteps,
                        totals,
                        cancelled ? 'cancelled' : 'success',
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
                        `Conversation ${session.conversationId} responsive cancel watcher failed; cancellation remains available at the next turn boundary`,
                        error,
                      ).pipe(Effect.andThen(Effect.never)),
                    ),
                  );

          const opened =
            ahead === undefined
              ? Stream.make(AgentEvents.turnStarted(step))
              : Stream.make(
                  AgentEvents.turnStarted(step),
                  AgentEvents.compacted(step, ahead),
                );

          return opened.pipe(
            Stream.concat(guarded.pipe(Stream.interruptWhen(responsiveCancel))),
            Stream.concat(decide),
          );
        }),
      ) as Stream.Stream<
        AgentEvents.Event<RuntimeTools>,
        AiError.AiError,
        WithOwnHandlers<RuntimeTools> | StopR
      >;

    const streamIn = (chat: Chat.Service, input: Prompt.RawInput) =>
      Stream.unwrap(
        Effect.gen(function* () {
          if (runtime === undefined) {
            const root = yield* RunPolicyRuntime.create(runPolicy);
            return entryFor({ ...wiring, runtime: root }).streamIn(
              chat,
              input,
            ) as Stream.Stream<
              AgentEvents.Event<RuntimeTools>,
              AiError.AiError,
              WithOwnHandlers<RuntimeTools> | StopR
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
                    ? AgentEvents.signalled(0, decision.signal)
                    : AgentEvents.signalRejected(
                        0,
                        decision.signal,
                        decision.exhaustion!,
                      ),
                )
                .concat(
                  drained.backlog
                    ? [
                        AgentEvents.signalBacklog(
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
                  AgentEvents.completed(
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
                    `Conversation ${session.conversationId} recovery cancel watcher failed; cancellation remains available at the next boundary`,
                    error,
                  ).pipe(Effect.andThen(Effect.never)),
                ),
              );

            return Stream.concat(
              announced,
              Stream.unwrap(
                Effect.gen(function* () {
                  const remaining = yield* runtime.remainingMillis;
                  const recovery = ToolDispatch.resolveIndeterminate(toolkit, {
                    agent: definition.name,
                    session,
                    interceptor,
                    runtime,
                    unmeteredToolNames: delegationToolNames,
                    arbitration,
                  }).pipe(
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
                            ? AgentEvents.signalled(0, decision.signal)
                            : AgentEvents.signalRejected(
                                0,
                                decision.signal,
                                decision.exhaustion!,
                              ),
                        ),
                      ),
                      Stream.make(
                        AgentEvents.completed(
                          '',
                          0,
                          wiring.initialUsage ?? { input: 0, output: 0 },
                          'cancelled',
                        ),
                      ),
                    );
                  }
                  yield* Ref.set(
                    chat.history,
                    Prompt.concat(
                      Prompt.make([{ role: 'system', content: instructions }]),
                      AgentHistory.messagesFrom(yield* session.recorded),
                    ),
                  );
                  return entryFor({
                    session: wiring.session,
                    interceptor: wiring.interceptor,
                    runtime,
                    ...(wiring.startRun === undefined
                      ? {}
                      : { startRun: wiring.startRun }),
                    ...(wiring.initialUsage === undefined
                      ? {}
                      : { initialUsage: wiring.initialUsage }),
                    ...(wiring.lastTurn === undefined
                      ? {}
                      : { lastTurn: wiring.lastTurn }),
                  }).streamIn(chat, effective) as Stream.Stream<
                    AgentEvents.Event<RuntimeTools>,
                    AiError.AiError,
                    WithOwnHandlers<RuntimeTools> | StopR
                  >;
                }),
              ),
            ) as Stream.Stream<
              AgentEvents.Event<RuntimeTools>,
              AiError.AiError,
              WithOwnHandlers<RuntimeTools> | StopR
            >;
          }
          const usage = yield* Ref.make<Stop.Usage>(
            wiring.initialUsage ?? { input: 0, output: 0 },
          );
          const lastTurn = yield* Ref.make<ContextWindow.TurnUsage | undefined>(
            wiring.lastTurn,
          );
          const remaining = yield* runtime.remainingMillis;
          return turn(chat, usage, lastTurn, 1, input).pipe(
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
      RuntimeTools,
      BaseRequires,
      AiError.AiError
    >;
  };

  return fromParts<
    Name,
    Tools,
    RuntimeTools,
    BaseRequires,
    never,
    AiError.AiError
  >({
    name: definition.name,
    revision,
    description: definition.description,
    ownToolkit: definition.toolkit,
    toolkit,
    instructions,
    interceptor: undefined,
    runPolicy,
    entry: entryFor,
  });
};

const validateGeneratedToolNames = (
  own: Toolkit.Any,
  children: ReadonlyArray<Named>,
  skills: ReadonlyArray<Skill.Skill>,
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
  if (skills.length > 0) reserve(Skill.TOOL_NAME, 'skills');
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

/**
 * Collapse an event stream into a {@link Result}.
 *
 * Module scope rather than a closure inside `make`, because `recordingTo`
 * needs it too: a recording agent's `run` has to be a fold of its *recorded*
 * stream, not of the unrecorded one `make` closed over. Keeping one fold is
 * what stops `run` and `stream` from drifting — the property the comment at
 * the top of this file exists to protect.
 */
const foldToResult = <Tools extends Record<string, Tool.Any>, E, R>(
  events: Stream.Stream<AgentEvents.Event<Tools>, E, R>,
): Effect.Effect<Result, E, R> =>
  Effect.gen(function* () {
    const completed = yield* events.pipe(
      Stream.runFold(
        (): AgentEvents.Lifecycle | undefined => undefined,
        (last, event) => (event._tag === 'Completed' ? event : last),
      ),
    );

    // The loop always terminates by emitting `Completed`. Reaching here
    // without one means the loop changed and this fold did not, which is a
    // defect rather than a failure a caller could handle.
    if (completed?._tag !== 'Completed') {
      return yield* Effect.die(
        new Error('Agent stream ended without completing'),
      );
    }

    return {
      outcome: completed.outcome,
      text: completed.text,
      steps: completed.steps,
      usage: completed.usage,
    } satisfies Result;
  });

/**
 * Replay a recorded conversation, then follow it live.
 *
 * The counterpart of `agent.recordingTo(id)`, and a free function rather than
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
export const streamFrom = (
  conversationId: string,
  after: LogOffset.Offset = LogOffset.START,
): Stream.Stream<
  ConversationRecord.Envelope,
  LogStore.LogStoreError,
  LogStore.Service
> =>
  Tail.from(
    AgentLog.pathFor(LogVocabulary.ConversationId.make(conversationId)),
    after,
  );

/**
 * The two primitive event streams for one session.
 *
 * Split out from {@link Parts} because they are the part that varies with the
 * session and the rest is not. `withHandlers` composes over an entry;
 * `recordingTo` replaces one.
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
interface Wiring {
  readonly session: AgentLog.Session | undefined;
  readonly interceptor: Interception.Interceptor | undefined;
  readonly runtime?: RunPolicyRuntime.Runtime | undefined;
  readonly startRun?:
    | ((input: Prompt.RawInput) => Effect.Effect<void>)
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
 * `recordingTo` used to do, cannot reach any of that. An interceptor has the
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
  BaseRequires,
  RunError,
> {
  readonly name: Name;
  readonly revision: LogVocabulary.AgentRevision;
  readonly description: string | undefined;
  readonly ownToolkit: Toolkit.Toolkit<OwnTools>;
  readonly toolkit: Toolkit.Toolkit<RuntimeTools>;
  readonly instructions: string;
  readonly interceptor: Interception.Interceptor | undefined;
  readonly runPolicy: RunPolicy.Limits;
  readonly entry: (
    wiring: Wiring,
  ) => Entry<RuntimeTools, BaseRequires, RunError>;
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
  BaseRequires,
  InterceptorRequires,
  RunError extends AiError.AiError | AgentLog.CompatibilityError =
    AiError.AiError,
>(
  parts: Parts<Name, OwnTools, RuntimeTools, BaseRequires, RunError>,
): Instance<
  Name,
  OwnTools,
  BaseRequires | InterceptorRequires,
  RuntimeTools,
  BaseRequires,
  InterceptorRequires,
  RunError
> => {
  // How every entry point below reaches the loop: the agent's interceptor,
  // and whichever session that entry point has. Read from `parts` rather than
  // passed along, so the only way to be intercepted is to have said so on this
  // agent.
  const wiring = (
    session: AgentLog.Session | undefined,
    options?: Omit<Wiring, 'session' | 'interceptor'>,
  ): Wiring => ({
    session,
    interceptor: parts.interceptor,
    ...options,
  });

  // Every public result fold crosses the same tracing seam. Keeping the span
  // beside the fold means decorators can replace streams without quietly
  // removing runs from telemetry.
  const span = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.withSpan(effect, `Agent.${parts.name}.run`);

  const withSession = <A, E, R>(
    session: AgentLog.Session,
    effect: Effect.Effect<A, E, R>,
  ) => {
    const cleanup = new Set<
      (session: AgentLog.Session) => Effect.Effect<void>
    >();
    return effect.pipe(
      Effect.provideService(Session, session),
      Effect.provideService(StateCleanup, cleanup),
      Effect.ensuring(
        Effect.forEach(cleanup, (release) => release(session), {
          discard: true,
        }),
      ),
    );
  };

  const streamWithSession = <A, E, R>(
    session: AgentLog.Session,
    stream: Stream.Stream<A, E, R>,
  ) => {
    const cleanup = new Set<
      (session: AgentLog.Session) => Effect.Effect<void>
    >();
    return stream.pipe(
      Stream.provideService(Session, session),
      Stream.provideService(StateCleanup, cleanup),
      Stream.ensuring(
        Effect.forEach(cleanup, (release) => release(session), {
          discard: true,
        }),
      ),
    );
  };

  // The unrecorded entry, which is what the four public entry points are.
  // Built once rather than per call so an agent value stays cheap to hold.
  const plain = parts.entry(wiring(undefined));
  const publicPlain = {
    ...plain,
    run: (input: Prompt.RawInput) => span(foldToResult(plain.stream(input))),
    runIn: (chat: Chat.Service, input: Prompt.RawInput) =>
      span(foldToResult(plain.streamIn(chat, input))),
  };

  // The writer half of the log, and the reason `@sunfall/vesper-durable`'s
  // checkpointer could be deleted rather than merely shrunk. Shared by `resume`
  // and `branchFrom`, which differ only in how the session was claimed — a
  // branch writes its marker during `open`, so by the time this runs the
  // difference is already inside `session.history` and nothing below can
  // forget to account for it.
  //
  // It goes through `parts.entry(...).streamIn` rather than `stream`, which is
  // the whole difference from an ordinary recorded run: `stream` opens a fresh
  // `Chat` seeded with instructions alone, and this seeds one with the
  // conversation the records describe. Everything else — recording, signals,
  // the dispatch seam — is the same wiring `recordingTo` builds.
  const continuing = (
    session: AgentLog.Session,
    input: Prompt.RawInput,
    runtime?: RunPolicyRuntime.Runtime,
  ) =>
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
      const result = yield* withSession(
        session,
        foldToResult(AgentLog.record(session, entry.streamIn(chat, input))),
      );

      return {
        ...result,
        usage: {
          input: prior.input + result.usage.input,
          output: prior.output + result.usage.output,
        },
      } satisfies Result;
    });

  // The claim differs for resume, branch, and fork; continuation does not.
  const continueFrom = <E, R>(
    opener: Effect.Effect<AgentLog.Session, E, R>,
    input: Prompt.RawInput,
  ) =>
    span(
      Effect.flatMap(opener, (session) =>
        withSession(session, continuing(session, input)),
      ),
    );

  const agent: Instance<
    Name,
    OwnTools,
    BaseRequires | InterceptorRequires,
    RuntimeTools,
    BaseRequires,
    InterceptorRequires,
    RunError
  > = {
    [TypeId]: TypeId,
    name: parts.name,
    revision: parts.revision,
    description: parts.description,
    toolkit: parts.toolkit,
    instructions: parts.instructions,
    ...publicPlain,

    of: (handlers) => handlers,

    resume: (conversationId: string, input: Prompt.RawInput) =>
      continueFrom(
        AgentLog.open(LogVocabulary.ConversationId.make(conversationId), {
          compatibility: { agent: parts.name, revision: parts.revision },
        }),
        input,
      ),

    branchFrom: (
      conversationId: string,
      at: LogOffset.Offset,
      input: Prompt.RawInput,
    ) =>
      continueFrom(
        AgentLog.open(LogVocabulary.ConversationId.make(conversationId), {
          branchFrom: at,
          compatibility: { agent: parts.name, revision: parts.revision },
        }),
        input,
      ),

    forkFrom: (
      conversationId: string,
      at: LogOffset.Offset,
      forkConversationId: string,
      input: Prompt.RawInput,
    ) =>
      continueFrom(
        AgentLog.fork(
          LogVocabulary.ConversationId.make(conversationId),
          at,
          LogVocabulary.ConversationId.make(forkConversationId),
          { agent: parts.name, revision: parts.revision },
        ),
        input,
      ),

    // Rebuilt from `parts` like `withHandlers` is, and for the same reason:
    // recording twice replaces the target conversation instead of appending a
    // run to two logs at once, which no caller wants and which would double
    // every record if it happened by accident.
    //
    // `run` and `runIn` fold the *recorded* stream rather than delegating to
    // the plain entry. Delegating would run the loop through an unrecorded
    // path and silently produce results with no history — the exact failure
    // this whole file's "run is a fold of stream" rule prevents.
    //
    // The session argument to the new `entry` is ignored: naming a
    // conversation explicitly is a stronger statement than inheriting a
    // parent's, and quietly recording somewhere else would make
    // `recordingTo` mean different things depending on who called it.
    recordingTo: (
      conversationId: string,
      policy?: RecordingPolicy.Policy<never>,
    ) => {
      const target = LogVocabulary.ConversationId.make(conversationId);
      const recorded = (
        input: Prompt.RawInput,
        events: (
          session: AgentLog.Session,
        ) => Stream.Stream<
          AgentEvents.Event<RuntimeTools>,
          RunError | AgentLog.CompatibilityError,
          BaseRequires
        >,
      ): Stream.Stream<
        AgentEvents.Event<RuntimeTools>,
        RunError | AgentLog.CompatibilityError,
        BaseRequires | LogStore.Service
      > =>
        Stream.unwrap(
          Effect.gen(function* () {
            let session = yield* AgentLog.open(target, {
              compatibility: { agent: parts.name, revision: parts.revision },
            });
            if (policy !== undefined) {
              const context = yield* Effect.context<never>();
              session = AgentLog.withRecordingPolicy(
                session,
                RecordingPolicyRuntime.compile(policy, context),
              );
            }
            return AgentLog.record(
              session,
              streamWithSession(session, events(session)),
            );
          }),
        );

      const recordedEntry = (
        build: (
          session: AgentLog.Session,
        ) => Entry<RuntimeTools, BaseRequires, RunError>,
      ): Entry<
        RuntimeTools,
        BaseRequires | LogStore.Service,
        RunError | AgentLog.CompatibilityError
      > => ({
        stream: (input) =>
          recorded(input, (session) => build(session).stream(input)),
        streamIn: (chat, input) =>
          recorded(input, (session) => build(session).streamIn(chat, input)),
      });

      return fromParts<
        Name,
        OwnTools,
        RuntimeTools,
        BaseRequires | LogStore.Service,
        InterceptorRequires,
        RunError | AgentLog.CompatibilityError
      >({
        ...parts,
        entry: () => {
          const entry = recordedEntry((session) =>
            parts.entry(
              wiring(session, {
                startRun: (effective) =>
                  AgentLog.start(session, {
                    agent: parts.name,
                    revision: parts.revision,
                    input: effective,
                  }),
              }),
            ),
          );
          return {
            stream: (input) => entry.stream(input),
            streamIn: (chat, input) => entry.streamIn(chat, input),
          };
        },
      });
    },

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
        BaseRequires,
        Interception.Services<I>,
        RunError
      >({
        ...parts,
        interceptor: interceptor as unknown as Interception.Interceptor,
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
        WithoutOwnHandlers<BaseRequires, OwnTools>,
        InterceptorRequires,
        RunError
      >({
        ...parts,
        entry: (incoming: Wiring) =>
          provideEntry(parts.entry(incoming), own) as Entry<
            RuntimeTools,
            WithoutOwnHandlers<BaseRequires, OwnTools>,
            RunError
          >,
      });
    },
  };
  register(agent, {
    run: (
      runtime: RunPolicyRuntime.Runtime,
      session: AgentLog.Session | undefined,
      input: Prompt.RawInput,
    ) =>
      span(
        session === undefined
          ? foldToResult(
              parts.entry(wiring(undefined, { runtime })).stream(input),
            )
          : Effect.andThen(
              AgentLog.assertCompatible(session, {
                agent: parts.name,
                revision: parts.revision,
              }),
              Effect.suspend(() => {
                const completed = session.completed;
                return completed === undefined
                  ? continuing(session, input, runtime)
                  : Effect.succeed(completed);
              }),
            ),
      ),
  });
  return agent;
};

interface TurnState {
  text: string;
  toolCalls: Response.ToolCallPartEncoded[];
  usage: Response.FinishPartEncoded['usage'] | undefined;
  emitted: boolean;
  started: boolean;
}

const emptyTurnState = (): TurnState => ({
  text: '',
  toolCalls: [],
  usage: undefined,
  emitted: false,
  started: false,
});

const observe = (state: TurnState, part: Response.StreamPartEncoded): void => {
  state.emitted = true;
  switch (part.type) {
    case 'text-delta':
      state.text += part.delta;
      break;
    case 'tool-call':
      state.toolCalls.push(part);
      break;
    case 'finish':
      state.usage = part.usage;
      break;
    default:
      break;
  }
};

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
  if (typeof error !== 'object' || error === null) {
    return { description: String(error), metadata: partMetadata ?? {} };
  }

  const value = error as Record<string, unknown>;
  const metadata =
    typeof value.metadata === 'object' && value.metadata !== null
      ? (value.metadata as Record<string, unknown>)
      : {};
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

export * as Agent from './agent.js';
