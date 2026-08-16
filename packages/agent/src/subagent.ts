import { Context, Effect, Schema } from 'effect';
import { AiError, Tool, Toolkit } from 'effect/unstable/ai';

import type { Agent } from './agent.js';
import type { AgentLog } from './log.js';

// Delegation. A subagent is not a special mechanism — it compiles to an
// ordinary `Tool` on the parent, so it flows through the toolkit machinery
// already in place: the model chooses it like any other tool, its result
// comes back as a tool result, and — the part that matters — the child's
// service requirements surface in the parent's `R`.
//
// That last property is what a closure-based design cannot give you. A
// subagent whose tools read a database makes the parent un-runnable until
// the application provides that database, checked at compile time rather
// than discovered at 2am.
//
// Cycles are structurally impossible: an `Agent` value must exist before it
// can be listed as someone's subagent, so a cycle would need a forward
// reference that does not typecheck.
//
// Depth is a different problem, and the type system says nothing about it.
// `a -> b -> c -> ...` is perfectly constructible, and each level multiplies
// model calls, so an innocent-looking chain becomes an unbounded bill, which
// is why delegation is capped.

const Parameters = Schema.Struct({
  prompt: Schema.String,
});

const Success = Schema.Struct({
  result: Schema.String,
  steps: Schema.Number,
});

const Failure = Schema.Struct({
  refused: Schema.String,
});

/**
 * Maximum nesting of subagent delegation.
 *
 * Four, because each level multiplies the model calls below it and nothing in
 * the type system bounds the chain. Deep enough that no honest delegation
 * hits it; shallow enough that a runaway one stops.
 */
export const MAX_DEPTH = 4;

/**
 * How deep the current delegation chain is; 0 at the top level.
 *
 * A `Context.Reference` rather than a service because the safe default is
 * real: code that never delegates never has to provide it, and a caller
 * cannot forget to.
 */
export const Depth = Context.Reference<number>(
  '@sunfall/vesper-agent/DelegationDepth',
  { defaultValue: () => 0 },
);

export const toolName = <const Name extends string>(
  name: Name,
): `task_${Name}` => `task_${name}`;

/**
 * The tool a parent sees for one child.
 *
 * `dependencies` is how Effect models a handler's services, and it takes
 * `Context.Key` *values* — services cannot be inferred from a type alone.
 * That is the ceiling on automatic propagation here: a delegation tool built
 * from a child's type has no way to discover which keys that child's tools
 * need, so its `HandlerServices` is `never` and the handler is required to
 * be self-contained.
 *
 * A caller who needs a child's services propagated declares them, which is
 * the same contract any other tool has:
 *
 * ```ts
 * Tool.make('task_researcher', { …, dependencies: [SqlClient, Notebook] })
 * ```
 *
 * The child runs against a fresh conversation, not the parent's — that is
 * the reason to delegate at all. The parent gets back only what the child
 * chose to say, which also keeps the child's intermediate work out of the
 * parent's context window. When the parent is recording, that conversation is
 * no longer discarded: it becomes a child session with its own id, referenced
 * from both logs. See {@link handler}.
 */
export const tool = <const Name extends string>(
  child: Pick<Agent.Named<Name>, 'name' | 'description'>,
) =>
  Tool.make(toolName(child.name), {
    description:
      child.description ??
      `Delegate a self-contained task to the ${child.name} agent.`,
    parameters: Parameters,
    success: Success,
    failure: Failure,
    // Refusal goes back to the model rather than aborting the run: an agent
    // told it cannot delegate any further can still do the work itself.
    failureMode: 'return',
  });

/** What the toolkit hands a handler alongside its parameters. */
interface CallContext {
  readonly toolCallId?: string | undefined;
}

/**
 * The handler that runs one child when its tool is called.
 *
 * Typed against {@link Agent.Named} rather than `Agent<Name, Tools>`: the
 * latter fixes `Requires` to its default, so any child whose requirements had
 * been narrowed — every child built with `withHandlers` — failed to match and
 * silently fell back to `any` in the requirement channel.
 *
 * ## Child sessions
 *
 * With a `session`, the child's conversation stops being thrown away: the
 * parent opens a child session, which writes one `ChildSession` record into
 * each log, and the child records its own run under its own id. Without one —
 * an agent that is not recording — this is exactly what it always was, and
 * the child's `run` is called directly.
 *
 * The session is a parameter rather than something looked up, so "is this
 * delegation recorded" is decided by whoever built the layer and is visible
 * at that call site. `Depth` stays a `Context.Reference` because its default
 * is genuinely the right answer for code that never delegates; a session's
 * would be persistence behind a plausible-looking default, which is how a
 * caller who forgot gets no signal at all.
 *
 * The child id is derived from the parent's id and the tool call id, so a
 * recovered delegation resumes the child it already started instead of
 * opening a second one beside it.
 */
export const handler =
  <Name extends string, R>(
    child: Pick<
      Agent.Named<Name, R>,
      'name' | 'description' | 'run' | 'runInSession'
    >,
    session?: AgentLog.Session,
  ) =>
  (input: { readonly prompt: string }, call?: CallContext) =>
    Effect.gen(function* () {
      const depth = yield* Depth;

      if (depth >= MAX_DEPTH) {
        return yield* Effect.fail({
          refused:
            `Delegation depth ${MAX_DEPTH} reached; complete this task ` +
            'directly instead of delegating further.',
        });
      }

      const runInSession = child.runInSession;
      const delegated =
        session === undefined || runInSession === undefined
          ? child.run(input.prompt)
          : Effect.gen(function* () {
              const childSession = yield* session.child({
                // A tool call always carries an id when `LanguageModel`
                // dispatches it. The fallback is for a handler invoked
                // directly — a test, a caller wiring delegation by hand —
                // where there is no call to derive an id from.
                toolCallId: call?.toolCallId ?? crypto.randomUUID(),
                agent: child.name,
                depth: depth + 1,
              });
              return yield* runInSession(childSession, input.prompt);
            });

      const result = yield* delegated.pipe(
        Effect.provideService(Depth, depth + 1),
      );

      return { result: result.text, steps: result.steps };
    }).pipe(Effect.withSpan(`Agent.delegate.${child.name}`));

/**
 * Each child's delegation tool, positionally, so literal names survive.
 *
 * Mapping over the tuple rather than calling `.map()` at the type level is
 * what keeps `task_researcher` a literal key instead of collapsing the whole
 * record to `Record<string, Tool.Any>`.
 */
type ToolTuple<Children extends ReadonlyArray<Agent.Named>> = {
  readonly [K in keyof Children]: ReturnType<typeof tool<Children[K]['name']>>;
};

/**
 * The services a set of children collectively need, read off their `run`
 * signatures rather than declared anywhere.
 *
 * This is what a parent inherits by listing subagents. It is exported because
 * `Agent.make` has to name it: a parent's own requirement channel is its
 * tools' requirements *plus* this, and leaving it out is not a cosmetic
 * imprecision — it compiles a parent that cannot actually run its children.
 *
 * @category utility types
 * @since 0.1.0
 */
export type Services<Children extends ReadonlyArray<Agent.Named>> = [
  Children[number],
] extends [never]
  ? // No children, no inherited services. This branch is load-bearing rather
    // than defensive: `Children[number]` is `never` for an empty tuple,
    // `never extends X` is *true*, and the true branch below has no
    // inference site for `R` — so without this guard a childless agent
    // inherits `unknown`, which then swallows its whole requirement channel.
    never
  : Children[number] extends {
        // oxlint-disable-next-line no-explicit-any
        readonly run: (input: any) => Effect.Effect<any, any, infer R>;
      }
    ? R
    : never;

/** What one child's delegation handler must look like. */
type DelegationHandler = (
  input: { readonly prompt: string },
  call: CallContext,
) => Effect.Effect<
  { readonly result: string; readonly steps: number },
  AiError.AiError | { readonly refused: string },
  // `unknown`, not `never`: a child's requirements only narrow at the concrete
  // call site, so at this definition they are still abstract. `Effect` is
  // covariant in `R`, so this accepts any of them — leaving the requirement
  // channel unchecked here while the result and failure channels, which are
  // fixed by `Success` and `Failure` above, are checked.
  unknown
>;

/**
 * Toolkit and handlers for a set of children, ready to merge into a parent.
 *
 * Requirements are *captured*, not declared. `Effect.context` reads whatever
 * the caller has in scope and the handlers provide it inward, so each child's
 * services land on this layer's input channel — inferred from their `run`
 * signatures, with nothing for an author to remember or keep in sync.
 *
 * This is the pattern Effect uses for the same problem: `Model` exposes
 * `captureRequirements`, which is `Effect.contextWith(context =>
 * Layer.provide(self, Layer.succeedContext(context)))`. A tool's
 * `dependencies` option exists for the case where a handler is defined apart
 * from the effect that needs the services; here they arrive together, so
 * capture is both automatic and exact.
 *
 * Variadic rather than array-taking, mirroring `Toolkit.make`, because that
 * is what preserves each child's literal name through to the tool record.
 *
 * `layer` is a function of the parent's session rather than a value, which is
 * the Phase 5 API change. It has to be: whether a delegation opens a child
 * session is decided per run, and the handlers are built when the layer is,
 * so a layer built once for the agent's lifetime could only ever answer that
 * question one way.
 */
export const delegateTo = <const Children extends ReadonlyArray<Agent.Named>>(
  ...children: Children
) => {
  const tools = children.map((child) => tool(child)) as ToolTuple<Children>;
  const kit = Toolkit.make(...tools);

  const layer = (session: AgentLog.Session | undefined) =>
    kit.toLayer(
      Effect.gen(function* () {
        const context = yield* Effect.context<Services<Children>>();

        // One cast, and it elides only the *key*.
        //
        // TypeScript cannot build a record type from a computed key whose
        // name is generic: `{ [k]: v }` with `k: K extends string` yields
        // `{ [x: string]: V }`, and annotating the target does not help —
        // `Record<K, V>` is rejected outright. `Object.fromEntries` discards
        // the key type on top of that (microsoft/TypeScript#31393). Inside a
        // function generic over `Children`, `Children[number]['name']` is
        // `string` as far as the constraint knows, so there is no literal
        // union to recover in the first place.
        //
        // Typing the record before the cast recovers the other half. What
        // that buys, precisely: a bug in the composition below — this
        // lambda's own wiring of `handler` and the captured context — becomes
        // a compile error. Bugs inside `handler` itself were already covered
        // by `capture.test.ts`, which exercises it against concrete types;
        // this adds the step between. Both were verified by mutation, and the
        // second claim is the one `as never` genuinely swallowed.
        //
        // That matters because the failure is silent: `Toolkit.toHandlers`
        // skips any handler whose name is not in the toolkit rather than
        // raising, so a mismatch here would vanish at runtime. The key
        // mapping remains on trust; `definition.test.ts` covers that end at a
        // concrete instantiation.
        const handlers: Record<string, DelegationHandler> = Object.fromEntries(
          children.map((child) => [
            toolName(child.name),
            (input: { readonly prompt: string }, call: CallContext) =>
              handler(child, session)(input, call).pipe(
                Effect.provide(context),
              ),
          ]),
        );

        return handlers as never;
      }),
    );

  return { toolkit: kit, layer };
};

export * as Subagent from './subagent.js';
