import { Context, Crypto, Schema } from 'effect';
import { Tool } from 'effect/unstable/ai';

import type { Agent } from './agent.js';
import { RunPolicy } from './run-policy.js';

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
  steps: Schema.Natural,
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
 * from both logs.
 */
const makeTool = <const Name extends string>(name: Name, description: string) =>
  Tool.make(name, {
    description,
    parameters: Parameters,
    success: Success,
    failure: Schema.Union([Failure, RunPolicy.RunPolicyExhausted]),
    // Refusal goes back to the model rather than aborting the run: an agent
    // told it cannot delegate any further can still do the work itself.
    failureMode: 'return',
  });

export const tool = <const Name extends string>(
  child: Pick<Agent.Child<Name>, 'name' | 'description'>,
) =>
  makeTool(
    toolName(child.name),
    child.description ??
      `Delegate a self-contained task to the ${child.name} agent.`,
  );

/** Runtime form whose dynamic name lets an arbitrary child tuple build a layer. */
export const runtimeTool = (child: Pick<Agent.Child, 'name' | 'description'>) =>
  makeTool(
    String(toolName(child.name)),
    child.description ??
      `Delegate a self-contained task to the ${child.name} agent.`,
  );

/**
 * Each child's delegation tool, positionally, so literal names survive.
 *
 * Mapping over the tuple rather than calling `.map()` at the type level is
 * what keeps `task_researcher` a literal key instead of collapsing the whole
 * record to `Record<string, Tool.Any>`.
 */
export type ToolTuple<Children extends ReadonlyArray<Agent.Child>> = {
  readonly [K in keyof Children]: ReturnType<typeof tool<Children[K]['name']>>;
};

/** The generated delegation tools, keyed by their model-facing names. */
export type Tools<Children extends ReadonlyArray<Agent.Child>> = {
  [Child in Children[number] as `task_${Child['name']}`]: ReturnType<
    typeof tool<Child['name']>
  >;
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
export type Services<Children extends ReadonlyArray<Agent.Child>> = [
  Children[number],
] extends [never]
  ? // No children, no inherited services. This branch is load-bearing rather
    // than defensive: `Children[number]` is `never` for an empty tuple,
    // `never extends X` is *true*, and the true branch below has no
    // inference site for `R` — so without this guard a childless agent
    // inherits `unknown`, which then swallows its whole requirement channel.
    never
  : Children[number] extends Agent.Child<infer _Name, infer R>
    ? Crypto.Crypto | R
    : Crypto.Crypto;

export * as Subagent from './subagent.js';
