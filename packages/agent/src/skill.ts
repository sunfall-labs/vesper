import { Effect, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

// Skills are reusable instructions the model loads on demand.
//
// The alternative — concatenating every skill into the system prompt — is
// what makes prompt caching stop paying. The system prefix is the largest
// cacheable span in a conversation, and it only stays cached while it is
// byte-identical across turns. Splicing in a different set of skills per
// request invalidates it every time, so the tokens are re-read at full price
// on every turn of every conversation.
//
// Loading through a tool keeps the prefix stable. The cost is one extra turn
// when a skill is actually needed, which is far cheaper than never caching.
// The catalog — names and one-line descriptions — does go in the prompt,
// because the model cannot ask for something it does not know exists.

/**
 * Reusable instructions the model can load on demand.
 *
 * Left as a plain interface rather than a `Schema.Struct` + same-name
 * interface, unlike `Agent.Result` and `Stop.Usage`. This module
 * self-exports its namespace as `Skill`, so an exported value of the same
 * name cannot coexist with it — Effect avoids the clash by namespacing in
 * an index barrel instead. Decode skills authored outside the codebase with
 * a schema declared at that boundary.
 */
export interface Skill {
  readonly name: string;
  /** One line. This is what the model sees when deciding to load it. */
  readonly description: string;
  /** The full instructions, revealed only when loaded. */
  readonly instructions: string;
}

export const TOOL_NAME = 'load_skill';

/** The tool record compiled into an agent that declares at least one skill. */
export type Tools<
  Skills extends ReadonlyArray<{
    readonly name: string;
    readonly description: string;
    readonly instructions: string;
  }>,
> = Skills extends readonly []
  ? {}
  : Record<typeof TOOL_NAME, ReturnType<typeof makeTool>>;

/**
 * The catalog line for each skill, for splicing into instructions.
 *
 * Kept separate from the loading tool so callers control where it lands in
 * the prompt — it must sit in the cacheable prefix, which means it has to be
 * stable for a given agent definition.
 */
export const catalog = (skills: ReadonlyArray<Skill>): string =>
  skills.length === 0
    ? ''
    : [
        'Available skills — load one with the `load_skill` tool before',
        'attempting work it covers:',
        ...skills.map((skill) => `- ${skill.name}: ${skill.description}`),
      ].join('\n');

const Success = Schema.Struct({ instructions: Schema.String });

const makeTool = (names: ReadonlyArray<string>) => {
  const name: Schema.Codec<string, string> =
    names.length === 0 ? Schema.String : Schema.Literals(names);
  return Tool.make(TOOL_NAME, {
    description:
      'Load the full instructions for one of the available skills. Do this ' +
      'before attempting work the skill covers.',
    parameters: Schema.Struct({
      name,
    }),
    success: Success,
    failure: Schema.Struct({ unknownSkill: Schema.String }),
    failureMode: 'return',
  });
};

/**
 * The loading tool and its handler.
 *
 * Returned together because the tool's parameter schema is derived from the
 * skill set — an unknown name is rejected by validation rather than reaching
 * the handler, so the model gets a usable error and can retry with a real
 * name instead of receiving an empty string it may not notice.
 */
export const loader = (skills: ReadonlyArray<Skill>) => {
  const byName = new Map(skills.map((skill) => [skill.name, skill]));
  const names = skills.map((skill) => skill.name);

  const tool = makeTool(names);

  const handler = (input: { readonly name: string }) => {
    const skill = byName.get(input.name);
    return skill === undefined
      ? Effect.fail({ unknownSkill: input.name })
      : Effect.succeed({ instructions: skill.instructions });
  };

  const kit = Toolkit.make(tool);
  return {
    tool,
    // Exposed so callers (and tests) can exercise loading without going
    // through model dispatch.
    handler,
    toolkit: kit,
    layer: kit.toLayer({ [TOOL_NAME]: handler }),
  };
};

export * as Skill from './skill.js';
