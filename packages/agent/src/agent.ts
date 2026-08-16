import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Tail } from '@sunfall/vesper-log/tail';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
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
import type { Interception } from './interception.js';
import { AgentLog } from './log.js';
import { Skill } from './skill.js';
import { Stop } from './stop.js';
import { Subagent } from './subagent.js';

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
// This module must not import `@sunfall/vesper-pi`. It targets the `LanguageModel`
// tag, so the provider and its retry policy are chosen at application wiring.
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
> {
  readonly name: Name;
  readonly description?: string;
  /** Prepended as a system message on every run. */
  readonly instructions: string;
  readonly toolkit: Toolkit.Toolkit<Tools>;
  readonly stopWhen?: Stop.StopCondition<Tools>;
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
  readonly skills?: ReadonlyArray<Skill.Skill>;

  /**
   * Automatic compaction when a turn overflows the context window. Defaults
   * to {@link Compaction.defaultPolicy}; pass `false` to opt out.
   *
   * Applied by default because the failure it prevents — a long conversation
   * dying on a context-window error — is one every agent hits eventually,
   * and the recovery is always the same.
   */
  readonly compaction?: Compaction.Policy | false;
}

/**
 * What a completed run produced.
 *
 * Schema-modelled because a result is the natural thing to checkpoint, hand
 * to a workflow, or return over a transport, and every one of those needs a
 * codec rather than a bare interface.
 */
export const Result = Schema.Struct({
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
  in out Tools extends Record<string, Tool.Any>,
  /**
   * What a caller must supply to run this agent.
   *
   * Carried as a parameter so `withHandlers` can hand back a genuinely
   * narrower agent. Without it, the interface re-widens `run` back to
   * {@link WithOwnHandlers} and attaching handlers changes nothing a caller can
   * observe.
   */
  out Requires = WithOwnHandlers<Tools>,
> {
  readonly [TypeId]: TypeId;
  readonly name: Name;
  /** Shown to a parent when this agent is used as a subagent. */
  readonly description?: string | undefined;
  readonly toolkit: Toolkit.Toolkit<Tools>;
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
  ) => Stream.Stream<AgentEvents.Event<Tools>, AiError.AiError, Requires>;

  /** Run to completion. A fold of `stream`, not a second implementation. */
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, Requires>;

  /**
   * Continue an existing conversation instead of starting one.
   *
   * `stream` and `run` open a fresh `Chat` each time, which is right for a
   * one-shot. Resuming a stored session means handing back the `Chat` it was
   * restored into, so the loop appends to that history rather than a new
   * one. See `@sunfall/vesper-runtime`'s session helpers.
   */
  readonly streamIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Stream.Stream<AgentEvents.Event<Tools>, AiError.AiError, Requires>;

  readonly runIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, Requires>;

  /**
   * Declare handlers for this agent's tools without attaching them, purely
   * for the type checking.
   *
   * Mirrors `Toolkit.of`, and exists for the same reason: handlers defined
   * away from their agent otherwise get checked only at the point of use.
   */
  of<Handlers extends Toolkit.HandlersFrom<Tools>>(
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
  withHandlers<Handlers extends Toolkit.HandlersFrom<Tools>>(
    handlers: Handlers,
  ): Instance<Name, Tools, WithoutOwnHandlers<Requires, Tools>>;

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
  ): Instance<Name, Tools, Requires | LogStore.Service>;

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
   * claims and write every record twice. That is the same shape as
   * {@link runInSession}, which has always had it.
   */
  readonly resume: (
    conversationId: string,
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, Requires | LogStore.Service>;

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
  ) => Effect.Effect<Result, AiError.AiError, Requires | LogStore.Service>;

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
  ) => Effect.Effect<Result, AiError.AiError, Requires | LogStore.Service>;

  /**
   * Give something a say at the loop's three named seams.
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
  intercepting<R = never>(
    interceptor: Interception.Interceptor<R>,
  ): Instance<Name, Tools, Requires | R>;

  /**
   * Run against a conversation someone else already claimed.
   *
   * This is how a parent runs a subagent as a **child session**: the parent
   * opens the child's conversation through its own session — which is what
   * writes the `ChildSession` reference into both logs — and hands the result
   * here. The child records its own conversation under its own id, and the
   * two are linked from either end.
   *
   * The requirement channel does not grow, unlike {@link recordingTo}'s. A
   * `Session` is a claim that already holds the store, so nothing about
   * running into one needs `LogStore.Service` from the caller. That is also
   * why it can be handed across a delegation boundary at all: the child's
   * declared services are exactly what they were.
   *
   * Signals reach a run through the session too, so a child session can be
   * steered or cancelled independently of its parent.
   */
  readonly runInSession: (
    session: AgentLog.Session,
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, Requires>;
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
  readonly description?: string | undefined;
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, R>;

  /**
   * How a parent runs this child as a recorded child session.
   *
   * Optional, so a hand-written `Named` — a stub, a fake in a test — is still
   * one. Every `Agent` has it. A parent that is recording uses it when it is
   * there and falls back to `run` when it is not, so a child that cannot
   * record is a child whose conversation is not retained rather than a
   * delegation that fails.
   *
   * Note what `R` does *not* pick up here: the session carries the store, so
   * running into one requires exactly what running normally requires. That is
   * what lets `Subagent.Services` keep reading a child's real services off
   * `run` without child sessions widening every parent's channel.
   */
  readonly runInSession?:
    | ((
        session: AgentLog.Session,
        input: Prompt.RawInput,
      ) => Effect.Effect<Result, AiError.AiError, R>)
    | undefined;
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
export interface Any extends Instance<any, any, any> {}

/**
 * Extract an agent's name.
 *
 * @category utility types
 * @since 0.1.0
 */
export type Name<A> =
  A extends Instance<infer _Name, infer _Tools, infer _R> ? _Name : never;

/**
 * Extract an agent's tool record.
 *
 * @category utility types
 * @since 0.1.0
 */
export type Tools<A> =
  A extends Instance<infer _Name, infer _Tools, infer _R> ? _Tools : never;

/**
 * Extract what still has to be provided to run an agent.
 *
 * @category utility types
 * @since 0.1.0
 */
export type Requires<A> =
  A extends Instance<infer _Name, infer _Tools, infer _R> ? _R : never;

/**
 * @category guards
 * @since 0.1.0
 */
export const isAgent = (u: unknown): u is Any =>
  typeof u === 'object' && u !== null && TypeId in u;

/**
 * @category constructors
 * @since 0.1.0
 */
export const make = <
  const Name extends string,
  Tools extends Record<string, Tool.Any>,
  const Children extends ReadonlyArray<Named> = readonly [],
>(
  definition: Definition<Name, Tools, Children>,
) => {
  const stopWhen = definition.stopWhen ?? Stop.defaultCondition<Tools>();

  // Subagents and skills are compiled into the toolkit here rather than by
  // the caller. Merging them by hand is mechanical, and the failure mode
  // when it is done wrong — a tool advertised with no handler, or a handler
  // for a tool nobody advertised — surfaces as a confusing model-facing
  // error rather than a compile error.
  const children = definition.subagents ?? [];
  const skills = definition.skills ?? [];

  const delegation =
    children.length > 0 ? Subagent.delegateTo(...children) : undefined;
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
  ) as unknown as Toolkit.Toolkit<Tools>;

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
  const entryFor = (wiring: Wiring) => {
    const session = wiring.session;
    const interceptor = wiring.interceptor;

    // A `Toolkit` already *is* an `Effect` producing a resolved toolkit, and
    // `streamText`'s `toolkit` option takes either form, so both branches have
    // one type and `LanguageModel` resolves them identically. That is the whole
    // reason the dispatch seam needs no change to the `LanguageModel` contract.
    //
    // An agent that neither records nor intercepts takes the first branch and
    // runs precisely the code it ran before either existed.
    const dispatching: Effect.Effect<
      Toolkit.WithHandler<Tools>,
      never,
      Tool.HandlersFor<Tools>
    > = session === undefined && interceptor === undefined
      ? toolkit
      : ToolDispatch.gate(toolkit, {
          agent: definition.name,
          session,
          interceptor,
        });

    // Fixed arity rather than spreading `...(x ? [l] : [])`: the spread form
    // widens `mergeAll`'s result, which then leaks into anything that provides
    // this layer.
    const layer = Layer.mergeAll(
      delegation === undefined ? Layer.empty : delegation.layer(session),
      loader === undefined ? Layer.empty : loader.layer,
    );

    const turnOptions = {
      toolkit: dispatching,
      ...(definition.concurrency === undefined
        ? {}
        : { concurrency: definition.concurrency }),
    };

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
    ) =>
      Stream.unwrap(
        Effect.map(
          interceptor?.beforeModelCall === undefined
            ? Effect.void
            : interceptor.beforeModelCall({
                agent: definition.name,
                conversationId: session?.conversationId,
                step,
                attempt,
              }),
          () =>
            chat.streamText({ prompt: input, ...turnOptions }).pipe(
              Stream.tap((part) =>
                Effect.sync(() => {
                  observe(seen, part as Response.StreamPartEncoded);
                }),
              ),
              Stream.map(
                (part): AgentEvents.Event<Tools> => ({
                  _tag: 'Part',
                  step,
                  part: part as Response.StreamPart<Tools>,
                }),
              ),
            ),
        ),
      );

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

        return over ? yield* Compaction.compact(chat, compaction) : undefined;
      });

    const turn = (
      chat: Chat.Service,
      usage: Ref.Ref<Stop.Usage>,
      lastTurn: Ref.Ref<ContextWindow.TurnUsage | undefined>,
      step: number,
      pending: Prompt.RawInput,
    ): Stream.Stream<
      AgentEvents.Event<Tools>,
      AiError.AiError,
      WithOwnHandlers<Tools>
    > =>
      Stream.unwrap(
        Effect.gen(function* () {
          const input = yield* openTurn(step, yield* Ref.get(usage), pending);

          // Before the request, not after it is refused. Announced the same
          // way the reactive rewrite is, because from the log's point of view
          // the two are the same event: history was replaced by a summary, and
          // a reader rebuilding this conversation has to know that.
          const ahead = yield* compactAhead(chat, lastTurn, input);

          // A turn's outcome is only knowable once its parts have gone by, but
          // the stop decision needs it. Accumulating through a `tap` while the
          // parts stream past is what lets a consumer see tokens live and
          // still have the decision made on complete information.
          //
          // Only what the stop condition and the result actually read is kept:
          // text, tool calls, usage. Rebuilding whole content parts would mean
          // a second copy of the fold `@sunfall/vesper-pi` already carries, for data
          // nothing here consumes.
          const seen: TurnState = { text: '', toolCalls: [], usage: undefined };

          // The conversation as it stood before this turn was attempted, kept
          // for the reactive path below. See the `catchIf` there for why.
          const historyBefore = yield* Ref.get(chat.history);

          const parts = askModel(chat, seen, step, input, 'initial');

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
                  Stream.catchIf(Compaction.isContextOverflow, () =>
                    Stream.unwrap(
                      Effect.map(
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
                        Effect.andThen(
                          Ref.set(
                            chat.history,
                            Prompt.concat(historyBefore, Prompt.make(input)),
                          ),
                          Compaction.compact(chat, compaction),
                        ),
                        (summarized) => {
                          // Empty, not `input`. The input is in the history
                          // above and `Chat.streamText` appends whatever it is
                          // given, so passing it again sent the overflowing
                          // message twice — which, when that message was the
                          // 136k tokens that caused the overflow, meant the
                          // retry was refused for the same reason and the run
                          // died having paid for a summary. A live 272k-token
                          // rejection against Anthropic did precisely that.
                          const retried = askModel(
                            chat,
                            seen,
                            step,
                            Prompt.empty,
                            'after-compaction',
                          );
                          return summarized === undefined
                            ? retried
                            : Stream.concat(
                                Stream.make(
                                  AgentEvents.compacted(step, summarized),
                                ),
                                retried,
                              );
                        },
                      ),
                    ),
                  ),
                );

          const decide = Stream.unwrap(
            Effect.gen(function* () {
              const totals = yield* Ref.updateAndGet(usage, (current) =>
                addUsage(current, seen.usage),
              );

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
              const delivered =
                session === undefined ? [] : yield* session.drainSignals;

              const cancelled = delivered.some(
                (signal) => signal.kind === 'cancel',
              );
              const steers = delivered.filter(
                (signal) => signal.kind === 'steer',
              );

              const announced = Stream.concat(
                Stream.fromIterable(
                  delivered.map((signal) =>
                    AgentEvents.signalled(step, signal),
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
              const stop = cancelled || (wanted && steers.length === 0);

              return stop
                ? Stream.concat(
                    announced,
                    Stream.make(AgentEvents.completed(seen.text, step, totals)),
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

          const opened =
            ahead === undefined
              ? Stream.make(AgentEvents.turnStarted(step))
              : Stream.make(
                  AgentEvents.turnStarted(step),
                  AgentEvents.compacted(step, ahead),
                );

          return opened.pipe(Stream.concat(guarded), Stream.concat(decide));
        }),
      );

    const streamIn = (chat: Chat.Service, input: Prompt.RawInput) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const usage = yield* Ref.make<Stop.Usage>({ input: 0, output: 0 });
          const lastTurn = yield* Ref.make<ContextWindow.TurnUsage | undefined>(
            undefined,
          );
          return turn(chat, usage, lastTurn, 1, input);
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

    const run = Effect.fn(`Agent.${definition.name}.run`)(
      (input: Prompt.RawInput) => foldToResult(stream(input)),
    );

    const runIn = Effect.fn(`Agent.${definition.name}.runIn`)(
      (chat: Chat.Service, input: Prompt.RawInput) =>
        foldToResult(streamIn(chat, input)),
    );

    // The agent provides its own handlers — subagent delegation and skill
    // loading — so a call site never has to provide them.
    //
    // Nothing below is asserted. These four used to carry hand-written `as`
    // clauses naming the requirement channel, on the belief that `Effect.provide`
    // reported `any` here. It does not — it computes the channel exactly, and the
    // assertions were papering over an imprecision introduced upstream by
    // widening `children` with `?? []`. Both instances of the drop-a-service bug
    // this library has had lived inside such an assertion, so the types are left
    // computed and `assertions.test.ts` pins what they must mean.

    return {
      stream: (input: Prompt.RawInput) => Stream.provide(stream(input), layer),
      run: (input: Prompt.RawInput) => Effect.provide(run(input), layer),
      streamIn: (chat: Chat.Service, input: Prompt.RawInput) =>
        Stream.provide(streamIn(chat, input), layer),
      runIn: (chat: Chat.Service, input: Prompt.RawInput) =>
        Effect.provide(runIn(chat, input), layer),
    };
  };

  return fromParts({
    name: definition.name,
    description: definition.description,
    toolkit,
    instructions,
    interceptor: undefined,
    entry: entryFor,
  });
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
> => Tail.from(AgentLog.pathFor(conversationId), after);

/**
 * The four entry points, for one session.
 *
 * Split out from {@link Parts} because they are the part that varies with the
 * session and the rest is not. `withHandlers` composes over an entry;
 * `recordingTo` replaces one.
 */
interface Entry<Tools extends Record<string, Tool.Any>, Requires> {
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<AgentEvents.Event<Tools>, AiError.AiError, Requires>;
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, Requires>;
  readonly streamIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Stream.Stream<AgentEvents.Event<Tools>, AiError.AiError, Requires>;
  readonly runIn: (
    chat: Chat.Service,
    input: Prompt.RawInput,
  ) => Effect.Effect<Result, AiError.AiError, Requires>;
}

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
  Tools extends Record<string, Tool.Any>,
  Requires,
> {
  readonly name: Name;
  readonly description: string | undefined;
  readonly toolkit: Toolkit.Toolkit<Tools>;
  readonly instructions: string;
  readonly interceptor: Interception.Interceptor | undefined;
  readonly entry: (wiring: Wiring) => Entry<Tools, Requires>;
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
  Tools extends Record<string, Tool.Any>,
  Requires,
>(
  parts: Parts<Name, Tools, Requires>,
): Instance<Name, Tools, Requires> => {
  // How every entry point below reaches the loop: the agent's interceptor,
  // and whichever session that entry point has. Read from `parts` rather than
  // passed along, so the only way to be intercepted is to have said so on this
  // agent.
  const wiring = (session: AgentLog.Session | undefined): Wiring => ({
    session,
    interceptor: parts.interceptor,
  });

  // The unrecorded entry, which is what the four public entry points are.
  // Built once rather than per call so an agent value stays cheap to hold.
  const plain = parts.entry(wiring(undefined));

  // Re-applied wherever a fold bypasses `make`'s `Effect.fn`-wrapped `run`.
  // Without it, turning recording on would quietly remove a run from tracing
  // — a telemetry regression that shows up as an absence, which is the
  // hardest kind to notice.
  const span = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.withSpan(effect, `Agent.${parts.name}.run`);

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
  const continuing = (session: AgentLog.Session, input: Prompt.RawInput) =>
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
      const prior = AgentHistory.usageFrom(session.history);
      const chat = yield* Chat.fromPrompt(
        Prompt.concat(
          Prompt.make([{ role: 'system', content: parts.instructions }]),
          AgentHistory.messagesFrom(session.history),
        ),
      );

      const result = yield* foldToResult(
        AgentLog.record(
          session,
          { agent: parts.name, input },
          parts.entry(wiring(session)).streamIn(chat, input),
        ),
      );

      return {
        ...result,
        usage: {
          input: prior.input + result.usage.input,
          output: prior.output + result.usage.output,
        },
      } satisfies Result;
    });

  return {
    [TypeId]: TypeId,
    name: parts.name,
    description: parts.description,
    toolkit: parts.toolkit,
    instructions: parts.instructions,
    ...plain,

    of: (handlers) => handlers,

    runInSession: (session: AgentLog.Session, input: Prompt.RawInput) =>
      span(
        foldToResult(
          AgentLog.record(
            session,
            { agent: parts.name, input },
            parts.entry(wiring(session)).stream(input),
          ),
        ),
      ),

    // A resumed run is an ordinary recorded run that did not start empty. See
    // {@link continuing}, which is the whole of it.
    resume: (conversationId: string, input: Prompt.RawInput) =>
      span(
        Effect.flatMap(AgentLog.open(conversationId), (session) =>
          continuing(session, input),
        ),
      ),

    // Identical to `resume` past the claim, which is the point. Branching is a
    // property of *how the conversation was claimed* — `open` writes the
    // marker before it reads anything back — so the run below rebuilds its
    // prompt, recovers its tool outcomes, and resumes its signal cursor from a
    // history that already describes the branch. Nothing here has to know.
    branchFrom: (
      conversationId: string,
      at: LogOffset.Offset,
      input: Prompt.RawInput,
    ) =>
      span(
        Effect.flatMap(
          AgentLog.open(conversationId, { branchFrom: at }),
          (session) => continuing(session, input),
        ),
      ),

    // The third member of the same family, and the reason `continuing` takes a
    // session rather than a conversation id. `resume` claims at the end,
    // `branchFrom` claims at a point, and this claims a *different stream* that
    // `AgentLog.fork` has already seeded with the ancestor's prefix. By the
    // time the run below builds its prompt the copy is in `session.history`,
    // so it rebuilds, recovers and drains exactly as the other two do — and,
    // unlike them, holds a claim nothing else is contending for.
    forkFrom: (
      conversationId: string,
      at: LogOffset.Offset,
      forkConversationId: string,
      input: Prompt.RawInput,
    ) =>
      span(
        Effect.flatMap(
          AgentLog.fork(conversationId, at, forkConversationId),
          (session) => continuing(session, input),
        ),
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
    recordingTo: (conversationId: string) => {
      const recorded = (
        input: Prompt.RawInput,
        events: (
          session: AgentLog.Session,
        ) => Stream.Stream<AgentEvents.Event<Tools>, AiError.AiError, Requires>,
      ): Stream.Stream<
        AgentEvents.Event<Tools>,
        AiError.AiError,
        Requires | LogStore.Service
      > =>
        Stream.unwrap(
          Effect.map(AgentLog.open(conversationId), (session) =>
            AgentLog.record(
              session,
              { agent: parts.name, input },
              events(session),
            ),
          ),
        );

      return fromParts({
        ...parts,
        entry: () => ({
          stream: (input: Prompt.RawInput) =>
            recorded(input, (session) =>
              parts.entry(wiring(session)).stream(input),
            ),
          run: (input: Prompt.RawInput) =>
            span(
              foldToResult(
                recorded(input, (session) =>
                  parts.entry(wiring(session)).stream(input),
                ),
              ),
            ),
          streamIn: (chat: Chat.Service, input: Prompt.RawInput) =>
            recorded(input, (session) =>
              parts.entry(wiring(session)).streamIn(chat, input),
            ),
          runIn: (chat: Chat.Service, input: Prompt.RawInput) =>
            span(
              foldToResult(
                recorded(input, (session) =>
                  parts.entry(wiring(session)).streamIn(chat, input),
                ),
              ),
            ),
        }),
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
    intercepting: <R>(interceptor: Interception.Interceptor<R>) =>
      fromParts({
        ...parts,
        interceptor: interceptor as Interception.Interceptor,
      }),

    withHandlers: (handlers) => {
      const own = parts.toolkit.toLayer(handlers);

      // The same narrowing assertion `make` uses, for the same reason:
      // `Layer.provide` cannot show inference that the handler requirement is
      // discharged, but it demonstrably is. Asserting `WithoutOwnHandlers`
      // removes a term from every caller rather than adding one.
      //
      // Rebuilt from `parts` rather than from the current agent, so calling
      // this twice replaces the handlers instead of stacking a second set
      // beneath the first — and so a session reaches the loop through the
      // rebuilt entry rather than being sealed behind the old one.
      return fromParts({
        ...parts,
        entry: (incoming: Wiring) => {
          const inner = parts.entry(incoming);
          return {
            stream: (input: Prompt.RawInput) =>
              Stream.provide(inner.stream(input), own),
            run: (input: Prompt.RawInput) =>
              Effect.provide(inner.run(input), own),
            streamIn: (chat: Chat.Service, input: Prompt.RawInput) =>
              Stream.provide(inner.streamIn(chat, input), own),
            runIn: (chat: Chat.Service, input: Prompt.RawInput) =>
              Effect.provide(inner.runIn(chat, input), own),
          };
        },
      });
    },
  };
};

interface TurnState {
  text: string;
  toolCalls: Response.ToolCallPartEncoded[];
  usage: Response.FinishPartEncoded['usage'] | undefined;
}

const observe = (state: TurnState, part: Response.StreamPartEncoded): void => {
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

export * as Agent from './agent.js';
