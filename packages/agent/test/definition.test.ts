import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

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

const researcher = Agent.make({
  name: 'researcher',
  description: 'Looks things up.',
  instructions: 'Answer concisely.',
  toolkit: Toolkit.make(),
});

const refunds: Skill.Skill = {
  name: 'refunds',
  description: 'How to process a refund.',
  instructions: 'STEP 1: verify the order.',
};

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
  Effect.runPromise(Effect.orDie(effect));

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
  it('offers subagents to the model as tools without manual merging', async () => {
    const parent = Agent.make({
      name: 'parent',
      instructions: 'delegate when useful',
      toolkit: Toolkit.make(ping),
      subagents: [researcher],
    });

    const { tools } = await run(drive(parent));

    expect(tools).toContain(Subagent.toolName('researcher'));
    // The agent's own tools survive the merge.
    expect(tools).toContain('ping');
  });

  it('offers the skill loader and puts only the catalog in the prompt', async () => {
    const helper = Agent.make({
      name: 'helper',
      instructions: 'be helpful',
      toolkit: Toolkit.make(),
      skills: [refunds],
    });

    const { tools, system } = await run(drive(helper));

    expect(tools).toContain(Skill.TOOL_NAME);
    // The catalog line is visible so the model knows the skill exists...
    expect(system).toContain('refunds');
    expect(system).toContain('How to process a refund.');
    // ...but the body stays out, so the prefix remains cacheable.
    expect(system).not.toContain('STEP 1');
  });

  it('leaves the toolkit alone when nothing is declared', async () => {
    const plain = Agent.make({
      name: 'plain',
      instructions: 'x',
      toolkit: Toolkit.make(ping),
    });

    const { tools } = await run(drive(plain));

    expect(tools).toEqual(['ping']);
  });

  // The layer and the toolkit are produced together precisely so they cannot
  // disagree: every advertised tool has a handler.
  it('provides handlers for everything it advertises', async () => {
    const parent = Agent.make({
      name: 'parent',
      instructions: 'x',
      toolkit: Toolkit.make(),
      subagents: [researcher],
      skills: [refunds],
    });

    const { tools } = await run(drive(parent));

    // Running at all proves the handlers resolved: `make` merges the subagent
    // and skill tools into the toolkit and provides their handlers itself, so
    // a drift between the two would fail here at dispatch.
    expect(tools).toContain(Subagent.toolName('researcher'));
    expect(tools).toContain(Skill.TOOL_NAME);
  });
});
