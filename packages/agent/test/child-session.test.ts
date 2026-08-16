import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Effect, Layer, Option, Ref, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { AgentLog } from '../src/log.js';
import { MAX_DEPTH } from '../src/subagent.js';

// Child sessions.
//
// Before this, a subagent ran against a fresh `Chat` and only its final text
// came back; everything it said in between was discarded. What these have to
// prove:
//
//   - the parent's log names the child conversation, and the child's log
//     names the parent — one record type, written into both, so neither end
//     depends on a convention the other might not share;
//   - the child's own conversation is really recorded, turn by turn;
//   - the child id is derived, so a re-run lands on the same conversation
//     rather than orphaning the first;
//   - depth is carried into the child session and keeps counting through a
//     grandchild, with the cap unchanged;
//   - an agent that is not recording delegates exactly as it did before, and
//     writes nothing.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 5, uncached: 5, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

const says = (text: string): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id: text },
  { type: 'text-delta' as const, id: text, delta: text },
  { type: 'text-end' as const, id: text },
  finish(),
];

const delegates = (id: string, child: string): Response.StreamPartEncoded[] => [
  {
    type: 'tool-call' as const,
    id,
    name: `task_${child}`,
    params: { prompt: `do the ${child} part` },
  },
  finish('tool-calls'),
];

/**
 * One model shared by every agent in the tree, replying in script order.
 *
 * A per-agent fake would be easier to read and would not exercise the thing
 * that matters: parent, child, and grandchild all run against one
 * `LanguageModel`, so the turns interleave exactly as they would in
 * production and a child that silently reused its parent's `Chat` would show
 * up here.
 */
const scripted = (turns: ReadonlyArray<Response.StreamPartEncoded[]>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: () =>
          Stream.unwrap(
            Effect.gen(function* () {
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              return Stream.fromIterable(
                turns[Math.min(index, turns.length - 1)]!,
              );
            }),
          ),
      });
    }),
  );

const PARENT = 'parent-conversation';

const researcher = Agent.make({
  name: 'researcher',
  description: 'Looks things up.',
  instructions: 'Answer concisely.',
  toolkit: Toolkit.make(),
});

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  turns: ReadonlyArray<Response.StreamPartEncoded[]>,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.orDie,
      Effect.provide(scripted(turns)),
      Effect.provide(LogStoreMemory.layer),
      Effect.scoped,
    ),
  );

const readAll = Effect.fn('test.readAll')(function* (conversationId: string) {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(AgentLog.pathFor(conversationId), { limit: 1000 })
    .pipe(Effect.orDie);
  return page.records;
});

const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map((envelope) => envelope.record._tag);

const childSessions = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'ChildSession' ? [envelope.record] : [],
  );

const supervisor = Agent.make({
  name: 'supervisor',
  instructions: 'delegate',
  toolkit: Toolkit.make(),
  subagents: [researcher],
});

const oneDelegation = [
  delegates('call-a', 'researcher'),
  says('researched'),
  says('summarised'),
];

describe('a recorded delegation', () => {
  it('names the child conversation in the parent’s log', async () => {
    const written = await run(
      Effect.gen(function* () {
        yield* supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie);
        return yield* readAll(PARENT);
      }),
      oneDelegation,
    );

    expect(childSessions(written)).toEqual([
      {
        _tag: 'ChildSession',
        toolCallId: 'call-a',
        agent: 'researcher',
        parentConversationId: PARENT,
        childConversationId: `${PARENT}/call-a`,
        depth: 1,
      },
    ]);
  });

  it('opens the child’s log with the same reference', async () => {
    const written = await run(
      Effect.gen(function* () {
        yield* supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie);
        return yield* readAll(`${PARENT}/call-a`);
      }),
      oneDelegation,
    );

    // The canonical reference, from the other end. A reader that opens the
    // child knows whose child it is without consulting the parent, and vice
    // versa, because it is one record rather than two conventions.
    expect(written[0]?.record).toEqual({
      _tag: 'ChildSession',
      toolCallId: 'call-a',
      agent: 'researcher',
      parentConversationId: PARENT,
      childConversationId: `${PARENT}/call-a`,
      depth: 1,
    });
  });

  it('records the child’s own conversation, not just its answer', async () => {
    const written = await run(
      Effect.gen(function* () {
        yield* supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie);
        return yield* readAll(`${PARENT}/call-a`);
      }),
      oneDelegation,
    );

    expect(tags(written)).toEqual([
      'ChildSession',
      'RunStarted',
      'Text',
      'TurnFinished',
      'Completed',
      'RunSettled',
    ]);
    expect(written[2]?.record).toMatchObject({ text: 'researched' });
  });

  it('derives the child id, so a re-run resumes it instead of forking', async () => {
    const first = await run(
      Effect.gen(function* () {
        yield* supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie);
        yield* supervisor.recordingTo(PARENT).run('again').pipe(Effect.orDie);
        const store = yield* LogStore.Service;
        return yield* store
          .meta(AgentLog.pathFor(`${PARENT}/call-a`))
          .pipe(Effect.orDie);
      }),
      [...oneDelegation, ...oneDelegation],
    );

    // Two delegations of the same tool call, one child conversation. A random
    // id would have left the first one orphaned with nothing referring to it.
    expect(Option.isSome(first)).toBe(true);
  });

  it('still hands the parent only what the child said', async () => {
    const result = await run(
      supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie),
      oneDelegation,
    );

    expect(result.text).toBe('summarised');
  });
});

describe('depth', () => {
  const junior = Agent.make({
    name: 'junior',
    description: 'Does the legwork.',
    instructions: 'work',
    toolkit: Toolkit.make(),
  });

  const senior = Agent.make({
    name: 'senior',
    description: 'Delegates further.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [junior],
  });

  const chief = Agent.make({
    name: 'chief',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [senior],
  });

  const twoLevels = [
    delegates('call-a', 'senior'),
    delegates('call-b', 'junior'),
    says('legwork'),
    says('senior summary'),
    says('chief summary'),
  ];

  it('keeps counting through a grandchild, and nests its id', async () => {
    const written = await run(
      Effect.gen(function* () {
        yield* chief.recordingTo(PARENT).run('go').pipe(Effect.orDie);
        return yield* readAll(`${PARENT}/call-a`);
      }),
      twoLevels,
    );

    expect(childSessions(written)).toEqual([
      // Its own reference, as the senior's child.
      expect.objectContaining({ agent: 'senior', depth: 1 }),
      // And the one it wrote when it delegated on.
      {
        _tag: 'ChildSession',
        toolCallId: 'call-b',
        agent: 'junior',
        parentConversationId: `${PARENT}/call-a`,
        childConversationId: `${PARENT}/call-a/call-b`,
        depth: 2,
      },
    ]);
  });

  it('leaves the cap where it was', () => {
    // Child sessions add conversations, not levels. Four is the cap and
    // the reason is unchanged: each level multiplies model calls.
    expect(MAX_DEPTH).toBe(4);
  });
});

// The cap, reached by actually nesting rather than by handing the handler a
// `Depth` to start from.
//
// `subagent.test.ts` proves the refusal fires when `Depth` says
// `MAX_DEPTH`, and the case above proves one grandchild carries depth 2. What
// neither covers is the arithmetic in between: `Depth` is a
// `Context.Reference` with a default of 0, threaded by `Effect.provideService`
// on the delegated effect, while `ChildSession.depth` is a separate `depth + 1`
// written into two logs. Nothing makes those two numbers agree except that
// they are both written correctly, and an off-by-one in either — a chain that
// refuses one level early, or one that runs a fifth level and bills for it —
// is invisible until a real tree gets deep.
//
// Each level multiplies model calls, so this is the guard on the unbounded
// bill the cap exists to prevent.
describe('a delegation chain that reaches the cap', () => {
  // The agent nobody is allowed to reach: it is a real subagent of `l4`, so
  // the only thing standing between the model asking for it and it running is
  // the depth check.
  const sink = Agent.make({
    name: 'sink',
    description: 'The one that never gets to run.',
    instructions: 'work',
    toolkit: Toolkit.make(),
  });

  const l4 = Agent.make({
    name: 'l4',
    description: 'Delegates to sink.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [sink],
  });

  const l3 = Agent.make({
    name: 'l3',
    description: 'Delegates to l4.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [l4],
  });

  const l2 = Agent.make({
    name: 'l2',
    description: 'Delegates to l3.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [l3],
  });

  const l1 = Agent.make({
    name: 'l1',
    description: 'Delegates to l2.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [l2],
  });

  // top -> l1 -> l2 -> l3 -> l4, which is four levels below the top-level run,
  // and then l4's own attempt to delegate is the fifth.
  const top = Agent.make({
    name: 'top',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [l1],
  });

  const toTheCap = [
    delegates('call-1', 'l1'),
    delegates('call-2', 'l2'),
    delegates('call-3', 'l3'),
    delegates('call-4', 'l4'),
    delegates('call-5', 'sink'),
    says('did it myself'),
    says('l3 summary'),
    says('l2 summary'),
    says('l1 summary'),
    says('top summary'),
  ];

  it('carries depth down four levels and refuses the fifth', async () => {
    const observed = await run(
      Effect.gen(function* () {
        const result = yield* top
          .recordingTo(PARENT)
          .run('go')
          .pipe(Effect.orDie);

        const deepest = `${PARENT}/call-1/call-2/call-3/call-4`;
        const records = yield* readAll(deepest);

        return {
          result,
          deepest,
          depths: [
            ...childSessions(yield* readAll(PARENT)),
            ...childSessions(yield* readAll(`${PARENT}/call-1`)),
            ...childSessions(yield* readAll(`${PARENT}/call-1/call-2`)),
            ...childSessions(yield* readAll(`${PARENT}/call-1/call-2/call-3`)),
          ].map((record) => record.depth),
          outcomes: records.flatMap((envelope) =>
            envelope.record._tag === 'ToolOutcome' ? [envelope.record] : [],
          ),
          children: childSessions(records),
        };
      }),
      toTheCap,
    );

    // 1, then 1 and 2, then 2 and 3, then 3 and 4 — each conversation holds
    // its own reference and the one it wrote when it delegated on.
    expect(observed.depths).toEqual([1, 1, 2, 2, 3, 3, 4]);

    // The deepest conversation is at depth 4, and it opened no child of its
    // own: its delegation was refused before a session was created, so no
    // fifth conversation exists.
    expect(observed.children).toEqual([
      expect.objectContaining({ agent: 'l4', depth: 4 }),
    ]);

    // The refusal came back as a failed tool result rather than aborting the
    // run — which is the point of `failureMode: 'return'`: an agent told it
    // cannot delegate further can still do the work.
    expect(observed.outcomes).toMatchObject([
      {
        name: 'task_sink',
        outcome: 'failure',
        result: { refused: expect.stringContaining('depth 4') },
      },
    ]);

    // And the whole tree still produced an answer.
    expect(observed.result.text).toBe('top summary');
  });
});

describe('delegation without recording', () => {
  it('writes nothing at all', async () => {
    const found = await run(
      Effect.gen(function* () {
        yield* supervisor.run('go').pipe(Effect.orDie);
        const store = yield* LogStore.Service;
        return {
          parent: yield* store
            .meta(AgentLog.pathFor(PARENT))
            .pipe(Effect.orDie),
          child: yield* store
            .meta(AgentLog.pathFor(`${PARENT}/call-a`))
            .pipe(Effect.orDie),
        };
      }),
      oneDelegation,
    );

    // A `LogStore` being reachable is not consent to write to it — for a
    // child conversation any more than for the parent's.
    expect(Option.isNone(found.parent)).toBe(true);
    expect(Option.isNone(found.child)).toBe(true);
  });

  it('still delegates and still returns the child’s answer', async () => {
    const result = await Effect.runPromise(
      supervisor
        .run('go')
        .pipe(Effect.orDie, Effect.provide(scripted(oneDelegation))),
    );

    // No `LogStoreMemory.layer` in this pipeline. If delegation had started
    // requiring one, this would not compile.
    expect(result.text).toBe('summarised');
  });
});
