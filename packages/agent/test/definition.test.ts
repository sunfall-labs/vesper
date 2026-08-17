import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

import { Agent } from '../src/agent.js';
import { Skill } from '../src/skill.js';
import { Subagent } from '../src/subagent.js';

// The declarative surface. `Definition` taking `subagents`, `skills`, and
// `compaction` is the difference between a library you wire and a framework
// you configure — but sugar that does not actually wire anything is worse
// than no sugar, so these assert the wiring rather than the shape.

const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

/** Records the tools and system prompt the provider was offered. */
const recording = (
  seenTools: Ref.Ref<ReadonlyArray<string>>,
  seenSystem: Ref.Ref<string>,
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: (options) =>
        Effect.gen(function* () {
          yield* Ref.set(
            seenTools,
            options.tools.map((tool) => tool.name),
          );
          yield* Ref.set(seenSystem, systemOf(options));
          return [{ type: 'text' as const, text: 'ok' }, finish];
        }),
      streamText: (options) =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* Ref.set(
              seenTools,
              options.tools.map((tool) => tool.name),
            );
            yield* Ref.set(seenSystem, systemOf(options));
            return Stream.fromIterable<Response.StreamPartEncoded>([
              { type: 'text-start', id: 't' },
              { type: 'text-delta', id: 't', delta: 'ok' },
              { type: 'text-end', id: 't' },
              finish,
            ]);
          }),
        ),
    }),
  );

const systemOf = (options: LanguageModel.ProviderOptions): string =>
  options.prompt.content
    .filter((message) => message.role === 'system')
    .map((message) => String(message.content))
    .join('\n');

const ping = Tool.make('ping', {
  description: 'A tool the agent owns, to prove the merge is additive.',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ pong: Schema.Boolean }),
});

const pingHandlers = Toolkit.make(ping).toLayer({
  ping: () => Effect.succeed({ pong: true }),
});

const reservedSkill = Tool.make(Skill.TOOL_NAME, {
  description: 'reserved',
  parameters: Schema.Struct({}),
  success: Schema.Struct({}),
});

const reservedChild = Tool.make('task_researcher', {
  description: 'reserved',
  parameters: Schema.Struct({}),
  success: Schema.Struct({}),
});

const researcher = Agent.make({
  name: 'researcher',
  revision: '1',
  description: 'Looks things up.',
  instructions: 'Answer concisely.',
  toolkit: Toolkit.make(),
});

const refunds: Skill.Skill = {
  name: 'refunds',
  description: 'How to process a refund.',
  instructions: 'STEP 1: verify the order.',
};

// Literal tuples reject collisions before runtime. Widened arrays are covered
// by the runtime tests below.
const compileTimeCollisionAssertions = (): void => {
  // @ts-expect-error every definition must declare its durable revision
  Agent.make({
    name: 'missing-revision',
    instructions: 'x',
    toolkit: Toolkit.make(),
  });

  // @ts-expect-error load_skill is generated when a literal skill tuple exists
  Agent.make({
    name: 'bad-skill',
    revision: '1',
    instructions: 'x',
    toolkit: Toolkit.make(reservedSkill),
    skills: [refunds],
  });

  // @ts-expect-error task_researcher is generated from the literal child tuple
  Agent.make({
    name: 'bad-child',
    revision: '1',
    instructions: 'x',
    toolkit: Toolkit.make(reservedChild),
    subagents: [researcher],
  });

  // @ts-expect-error duplicate child names generate the same tool name
  Agent.make({
    name: 'duplicate-child',
    revision: '1',
    instructions: 'x',
    toolkit: Toolkit.make(),
    subagents: [researcher, researcher],
  });
};
void compileTimeCollisionAssertions;

// `Agent.Named` rather than a hand-written shape: it is precisely "an agent
// whose requirements I want to keep", which is what this helper needs.
//
// There is no cast here any more, and that is the point. The two layers below
// discharge the requirement channel completely, checked by the compiler — so
// if `make` ever stopped providing its own subagent and skill handlers, this
// would stop compiling rather than fail at dispatch with a missing-handler
// error from the model.
const drive = <R>(agent: Agent.Named<string, R>) =>
  Effect.gen(function* () {
    const tools = yield* Ref.make<ReadonlyArray<string>>([]);
    const system = yield* Ref.make('');

    yield* agent
      .run('go')
      .pipe(
        Effect.provide(pingHandlers),
        Effect.provide(recording(tools, system)),
      );

    return { tools: yield* Ref.get(tools), system: yield* Ref.get(system) };
  });

describe('declarative definition', () => {
  it('rejects an empty revision at construction', () => {
    expect(() =>
      Agent.make({
        name: 'empty-revision',
        revision: '   ',
        instructions: 'x',
        toolkit: Toolkit.make(),
      }),
    ).toThrow('revision must be non-empty');
  });

  it('rejects a hand-written child even when it matches the public shape', () => {
    const child: Agent.Named<'hand-written', never> = {
      name: 'hand-written',
      revision: LogVocabulary.AgentRevision.make('1'),
      run: () =>
        Effect.succeed({
          outcome: 'success',
          text: 'ok',
          steps: 1,
          usage: { input: 0, output: 0 },
        }),
    };

    expect(() =>
      Agent.make({
        name: 'parent',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(),
        subagents: [child],
      }),
    ).toThrow('was not created by Agent.make');
  });

  it.effect(
    'offers subagents to the model as tools without manual merging',
    () =>
      Effect.gen(function* () {
        const parent = Agent.make({
          name: 'parent',
          revision: '1',
          instructions: 'delegate when useful',
          toolkit: Toolkit.make(ping),
          subagents: [researcher],
        });

        const { tools } = yield* drive(parent).pipe(Effect.orDie);

        expect(tools).toContain(Subagent.toolName('researcher'));
        // The agent's own tools survive the merge.
        expect(tools).toContain('ping');
      }),
  );

  it.effect(
    'offers the skill loader and puts only the catalog in the prompt',
    () =>
      Effect.gen(function* () {
        const helper = Agent.make({
          name: 'helper',
          revision: '1',
          instructions: 'be helpful',
          toolkit: Toolkit.make(),
          skills: [refunds],
        });

        const { tools, system } = yield* drive(helper).pipe(Effect.orDie);

        expect(tools).toContain(Skill.TOOL_NAME);
        // The catalog line is visible so the model knows the skill exists...
        expect(system).toContain('refunds');
        expect(system).toContain('How to process a refund.');
        // ...but the body stays out, so the prefix remains cacheable.
        expect(system).not.toContain('STEP 1');
      }),
  );

  it.effect('leaves the toolkit alone when nothing is declared', () =>
    Effect.gen(function* () {
      const plain = Agent.make({
        name: 'plain',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(ping),
      });

      const { tools } = yield* drive(plain).pipe(Effect.orDie);

      expect(tools).toEqual(['ping']);
    }),
  );

  // The layer and the toolkit are produced together precisely so they cannot
  // disagree: every advertised tool has a handler.
  it.effect('provides handlers for everything it advertises', () =>
    Effect.gen(function* () {
      const parent = Agent.make({
        name: 'parent',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(),
        subagents: [researcher],
        skills: [refunds],
      });

      const { tools } = yield* drive(parent).pipe(Effect.orDie);

      // Running at all proves the handlers resolved: `make` merges the subagent
      // and skill tools into the toolkit and provides their handlers itself, so
      // a drift between the two would fail here at dispatch.
      expect(tools).toContain(Subagent.toolName('researcher'));
      expect(tools).toContain(Skill.TOOL_NAME);
    }),
  );

  it('rejects generated collisions from widened arrays at runtime', () => {
    const children: ReadonlyArray<Agent.Named> = [researcher, researcher];
    const skills: ReadonlyArray<Skill.Skill> = [refunds];

    expect(() =>
      Agent.make({
        name: 'duplicate-child',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(),
        subagents: children,
      }),
    ).toThrow('duplicate tool "task_researcher"');

    expect(() =>
      Agent.make({
        name: 'own-child-collision',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(reservedChild),
        subagents: children.slice(0, 1),
      }),
    ).toThrow('toolkit already defines it');

    expect(() =>
      Agent.make({
        name: 'own-skill-collision',
        revision: '1',
        instructions: 'x',
        toolkit: Toolkit.make(reservedSkill),
        skills,
      }),
    ).toThrow('toolkit already defines it');
  });

  it('checks the agent brand value, not merely the property', () => {
    expect(Agent.isAgent(researcher)).toBe(true);
    expect(Agent.isAgent({ [Agent.TypeId]: 'forged' })).toBe(false);
  });
});
