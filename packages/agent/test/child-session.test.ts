import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Exit, Layer, Option, Ref, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  Prompt,
  type Response,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { protocolOf } from '../src/internal.js';
import { AgentLog } from '../src/log.js';
import { RunPolicy } from '../src/run-policy.js';
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
const scripted = (
  turns: ReadonlyArray<Response.StreamPartEncoded[]>,
  prompts: Prompt.Prompt[] = [],
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              prompts.push(options.prompt);
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              return Stream.fromIterable(
                turns[Math.min(index, turns.length - 1)]!,
              );
            }),
          ),
      });
    }),
  );

const PARENT = LogVocabulary.ConversationId.make('parent-conversation');
const CHILD = AgentLog.childIdFor(
  PARENT,
  LogVocabulary.ToolCallId.make('call-a'),
);

const researcher = Agent.make({
  name: 'researcher',
  revision: '1',
  description: 'Looks things up.',
  instructions: 'Answer concisely.',
  toolkit: Toolkit.make(),
});

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  turns: ReadonlyArray<Response.StreamPartEncoded[]>,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scripted(turns)),
    Effect.provide(LogStoreMemory.layer),
    Effect.scoped,
  );

const runInSession = <R>(
  child: Agent.Named<string, R>,
  session: AgentLog.Session,
  input: string,
) =>
  Effect.flatMap(RunPolicy.create(RunPolicy.defaultLimits), (runtime) =>
    protocolOf<R>(child)!.run(runtime, session, input),
  );

const readAll = Effect.fn('test.readAll')(function* (conversationId: string) {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(AgentLog.pathFor(LogVocabulary.ConversationId.make(conversationId)), {
      limit: 1000,
    })
    .pipe(Effect.orDie);
  return page.records;
});

const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map((envelope) => envelope.record._tag);

const childSessions = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'ChildSession' ? [envelope.record] : [],
  );

const failsOnceAfterChildStage = (
  stage: 'parent-link' | 'child-open' | 'child-link',
  childId: string,
): Layer.Layer<LogStore.Service> =>
  Layer.effect(
    LogStore.Service,
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const failed = yield* Ref.make(false);
      const inject = <A>(
        operation: LogStore.LogStoreError['operation'],
        path: string,
        effect: Effect.Effect<A, LogStore.LogStoreError>,
      ) =>
        Effect.gen(function* () {
          const value = yield* effect;
          if (!(yield* Ref.getAndSet(failed, true))) {
            return yield* Effect.fail(
              new LogStore.LogStoreError({
                path,
                operation,
                reason: 'storage',
                detail: `crashed after ${stage}`,
              }),
            );
          }
          return value;
        });

      return LogStore.Service.of({
        ...store,
        create: (path, identity) =>
          stage === 'child-open' &&
          path === AgentLog.pathFor(LogVocabulary.ConversationId.make(childId))
            ? inject('create', path, store.create(path, identity))
            : store.create(path, identity),
        append: (input) => {
          const isLink = input.records.some(
            ({ record }) => record._tag === 'ChildSession',
          );
          const target =
            stage === 'parent-link'
              ? AgentLog.pathFor(PARENT)
              : AgentLog.pathFor(LogVocabulary.ConversationId.make(childId));
          return stage !== 'child-open' && input.path === target && isLink
            ? inject('append', input.path, store.append(input))
            : store.append(input);
        },
      });
    }),
  ).pipe(Layer.provide(LogStoreMemory.layer));

const supervisor = Agent.make({
  name: 'supervisor',
  revision: '1',
  instructions: 'delegate',
  toolkit: Toolkit.make(),
  subagents: [researcher],
});

const oneDelegation = [
  delegates('call-a', 'researcher'),
  says('researched'),
  says('summarised'),
];

describe('child conversation ids', () => {
  it('cannot collide when separators occur in either input', () => {
    expect(
      AgentLog.childIdFor(
        LogVocabulary.ConversationId.make('a/b'),
        LogVocabulary.ToolCallId.make('c'),
      ),
    ).not.toBe(
      AgentLog.childIdFor(
        LogVocabulary.ConversationId.make('a'),
        LogVocabulary.ToolCallId.make('b/c'),
      ),
    );
  });

  it('is deterministic and preserves distinct Unicode strings', () => {
    const childIdFor = (conversationId: string, toolCallId: string) =>
      AgentLog.childIdFor(
        LogVocabulary.ConversationId.make(conversationId),
        LogVocabulary.ToolCallId.make(toolCallId),
      );
    const composed = childIdFor('café/親', '工具/é');
    expect(childIdFor('café/親', '工具/é')).toBe(composed);
    expect(childIdFor('cafe\u0301/親', '工具/é')).not.toBe(composed);
    expect(childIdFor('café/親', '工具/e\u0301')).not.toBe(composed);
  });
});

describe('a recorded delegation', () => {
  for (const stage of ['parent-link', 'child-open', 'child-link'] as const) {
    it.effect(`repairs both links after a crash following ${stage}`, () =>
      Effect.gen(function* () {
        const toolCallId = LogVocabulary.ToolCallId.make(`call-${stage}`);
        const childId = AgentLog.childIdFor(PARENT, toolCallId);
        const result = yield* Effect.gen(function* () {
          const firstParent = yield* AgentLog.open(PARENT, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          const first = yield* firstParent
            .child({
              toolCallId,
              agent: researcher.name,
              revision: LogVocabulary.AgentRevision.make(researcher.revision),
              depth: 1,
            })
            .pipe(Effect.exit);

          const retryParent = yield* AgentLog.open(PARENT, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          yield* retryParent.child({
            toolCallId,
            agent: researcher.name,
            revision: LogVocabulary.AgentRevision.make(researcher.revision),
            depth: 1,
          });
          return {
            first,
            parent: childSessions(yield* readAll(PARENT)),
            child: childSessions(yield* readAll(childId)),
          };
        }).pipe(Effect.provide(failsOnceAfterChildStage(stage, childId)));

        expect(Exit.isFailure(result.first)).toBe(true);
        expect(result.parent).toHaveLength(1);
        expect(result.child).toHaveLength(1);
        expect(result.parent[0]).toEqual(result.child[0]);
      }).pipe(Effect.scoped),
    );
  }

  it.effect('names the child conversation in the parent’s log', () =>
    Effect.gen(function* () {
      const written = yield* run(
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
          childConversationId: CHILD,
          depth: 1,
        },
      ]);
    }),
  );

  it.effect('opens the child’s log with the same reference', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie);
          return yield* readAll(CHILD);
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
        childConversationId: CHILD,
        depth: 1,
      });
    }),
  );

  it.effect('records the child’s own conversation, not just its answer', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie);
          return yield* readAll(CHILD);
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
    }),
  );

  it.live(
    'derives the child id, so a re-run resumes it instead of forking',
    () =>
      Effect.gen(function* () {
        const first = yield* run(
          Effect.gen(function* () {
            yield* supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie);
            yield* supervisor
              .recordingTo(PARENT)
              .run('again')
              .pipe(Effect.orDie);
            const store = yield* LogStore.Service;
            return yield* store
              .meta(AgentLog.pathFor(CHILD))
              .pipe(Effect.orDie);
          }),
          [...oneDelegation, ...oneDelegation],
        );

        // Two delegations of the same tool call, one child conversation. A random
        // id would have left the first one orphaned with nothing referring to it.
        expect(Option.isSome(first)).toBe(true);
      }),
  );

  it.effect('still hands the parent only what the child said', () =>
    Effect.gen(function* () {
      const result = yield* run(
        supervisor.recordingTo(PARENT).run('go').pipe(Effect.orDie),
        oneDelegation,
      );

      expect(result.text).toBe('summarised');
    }),
  );

  it.effect(
    'returns a completed reopened child without repeating model work',
    () =>
      Effect.gen(function* () {
        const prompts: Prompt.Prompt[] = [];
        const result = yield* Effect.gen(function* () {
          const parent = yield* AgentLog.open(PARENT, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          const first = yield* parent.child({
            toolCallId: LogVocabulary.ToolCallId.make('call-a'),
            agent: researcher.name,
            revision: LogVocabulary.AgentRevision.make(researcher.revision),
            depth: 1,
          });
          yield* runInSession(researcher, first, 'do the work');

          const reopened = yield* parent.child({
            toolCallId: LogVocabulary.ToolCallId.make('call-a'),
            agent: researcher.name,
            revision: LogVocabulary.AgentRevision.make(researcher.revision),
            depth: 1,
          });
          const resumed = yield* runInSession(
            researcher,
            reopened,
            'do the work',
          );
          return { resumed, records: yield* readAll(CHILD) };
        }).pipe(
          Effect.orDie,
          Effect.provide(
            scripted([says('researched'), says('repeated')], prompts),
          ),
          Effect.provide(LogStoreMemory.layer),
        );

        expect(result.resumed.text).toBe('researched');
        expect(prompts).toHaveLength(1);
        expect(
          result.records.filter(
            (envelope) => envelope.record._tag === 'RunStarted',
          ),
        ).toHaveLength(1);
      }).pipe(Effect.scoped),
  );

  it.live(
    'does not rerun a completed child when the parent outcome was never written',
    () =>
      Effect.gen(function* () {
        const prompts: Prompt.Prompt[] = [];
        const result = yield* Effect.gen(function* () {
          const parent = yield* AgentLog.open(PARENT, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          yield* parent.append([
            {
              _tag: 'RunStarted',
              agent: supervisor.name,
              formatVersion: 1,
              agentRevision: LogVocabulary.AgentRevision.make('1'),
              prompt: Prompt.make('go').content,
            },
            {
              _tag: 'ToolCall',
              step: 1,
              id: LogVocabulary.ToolCallId.make('call-a'),
              name: 'task_researcher',
              params: { prompt: 'do the researcher part' },
            },
          ]);
          const child = yield* parent.child({
            toolCallId: LogVocabulary.ToolCallId.make('call-a'),
            agent: researcher.name,
            revision: LogVocabulary.AgentRevision.make(researcher.revision),
            depth: 1,
          });
          yield* runInSession(
            researcher,
            {
              ...child,
              settlementTimeoutMillis: 10,
              append: (records) =>
                records.some((record) => record._tag === 'RunSettled')
                  ? Effect.never
                  : child.append(records),
            },
            'do the researcher part',
          );

          const resumed = yield* supervisor.resume(PARENT, 'continue');
          return { resumed, childRecords: yield* readAll(CHILD) };
        }).pipe(
          Effect.orDie,
          Effect.provide(
            scripted(
              [
                says('child side effect'),
                delegates('call-a', 'researcher'),
                says('parent finished'),
              ],
              prompts,
            ),
          ),
          Effect.provide(LogStoreMemory.layer),
        );

        expect(result.resumed.text).toBe('parent finished');
        expect(prompts).toHaveLength(3);
        expect(
          result.childRecords.filter(
            ({ record }) => record._tag === 'RunStarted',
          ),
        ).toHaveLength(1);
        expect(
          result.childRecords.filter(
            ({ record }) => record._tag === 'RunSettled',
          ),
        ).toHaveLength(0);
      }).pipe(Effect.scoped),
  );

  it.effect('continues a crashed child from its recorded next turn', () =>
    Effect.gen(function* () {
      const prompts: Prompt.Prompt[] = [];
      let calls = 0;
      const crash = new AiError.AiError({
        module: 'test',
        method: 'streamText',
        reason: new AiError.ContentPolicyError({ description: 'crash' }),
      });
      const worker = Agent.make({
        name: 'worker',
        revision: '1',
        instructions: 'work',
        toolkit: Toolkit.make(),
        compaction: false,
        stopWhen: () => Effect.sync(() => calls >= 3),
      });
      const models = Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          return yield* LanguageModel.make({
            generateText: () =>
              Effect.succeed<Response.PartEncoded[]>([finish()]),
            streamText: (options) => {
              prompts.push(options.prompt);
              calls += 1;
              if (calls === 2) return Stream.fail(crash);
              return Stream.fromIterable(
                calls === 1
                  ? says('first turn survived')
                  : says('resumed turn'),
              );
            },
          });
        }),
      );

      const result = yield* Effect.gen(function* () {
        const parent = yield* AgentLog.open(PARENT, {
          compatibility: {
            agent: 'test',
            revision: LogVocabulary.AgentRevision.make('1'),
          },
        });
        const first = yield* parent.child({
          toolCallId: LogVocabulary.ToolCallId.make('call-crash'),
          agent: worker.name,
          revision: LogVocabulary.AgentRevision.make(worker.revision),
          depth: 1,
        });
        const failed = yield* runInSession(worker, first, 'start').pipe(
          Effect.exit,
        );

        const reopened = yield* parent.child({
          toolCallId: LogVocabulary.ToolCallId.make('call-crash'),
          agent: worker.name,
          revision: LogVocabulary.AgentRevision.make(worker.revision),
          depth: 1,
        });
        const resumed = yield* runInSession(worker, reopened, 'continue');
        return { failed, resumed };
      }).pipe(Effect.provide(models), Effect.provide(LogStoreMemory.layer));

      expect(result.failed._tag).toBe('Failure');
      expect(result.resumed.text).toBe('resumed turn');
      expect(calls).toBe(3);
      expect(JSON.stringify(prompts[2])).toContain('first turn survived');
      expect(JSON.stringify(prompts[2])).toContain('continue');
    }).pipe(Effect.scoped),
  );
});

describe('depth', () => {
  const junior = Agent.make({
    name: 'junior',
    revision: '1',
    description: 'Does the legwork.',
    instructions: 'work',
    toolkit: Toolkit.make(),
  });

  const senior = Agent.make({
    name: 'senior',
    revision: '1',
    description: 'Delegates further.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [junior],
  });

  const chief = Agent.make({
    name: 'chief',
    revision: '1',
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

  it.effect('keeps counting through a grandchild, and nests its id', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* chief.recordingTo(PARENT).run('go').pipe(Effect.orDie);
          return yield* readAll(CHILD);
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
          parentConversationId: CHILD,
          childConversationId: AgentLog.childIdFor(
            CHILD,
            LogVocabulary.ToolCallId.make('call-b'),
          ),
          depth: 2,
        },
      ]);
    }),
  );

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
    revision: '1',
    description: 'The one that never gets to run.',
    instructions: 'work',
    toolkit: Toolkit.make(),
  });

  const l4 = Agent.make({
    name: 'l4',
    revision: '1',
    description: 'Delegates to sink.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [sink],
  });

  const l3 = Agent.make({
    name: 'l3',
    revision: '1',
    description: 'Delegates to l4.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [l4],
  });

  const l2 = Agent.make({
    name: 'l2',
    revision: '1',
    description: 'Delegates to l3.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [l3],
  });

  const l1 = Agent.make({
    name: 'l1',
    revision: '1',
    description: 'Delegates to l2.',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [l2],
  });

  // top -> l1 -> l2 -> l3 -> l4, which is four levels below the top-level run,
  // and then l4's own attempt to delegate is the fifth.
  const top = Agent.make({
    name: 'top',
    revision: '1',
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

  it.effect('carries depth down four levels and refuses the fifth', () =>
    Effect.gen(function* () {
      const observed = yield* run(
        Effect.gen(function* () {
          const result = yield* top
            .recordingTo(PARENT)
            .run('go')
            .pipe(Effect.orDie);

          const l1Id = AgentLog.childIdFor(
            PARENT,
            LogVocabulary.ToolCallId.make('call-1'),
          );
          const l2Id = AgentLog.childIdFor(
            l1Id,
            LogVocabulary.ToolCallId.make('call-2'),
          );
          const l3Id = AgentLog.childIdFor(
            l2Id,
            LogVocabulary.ToolCallId.make('call-3'),
          );
          const deepest = AgentLog.childIdFor(
            l3Id,
            LogVocabulary.ToolCallId.make('call-4'),
          );
          const records = yield* readAll(deepest);

          return {
            result,
            deepest,
            depths: [
              ...childSessions(yield* readAll(PARENT)),
              ...childSessions(yield* readAll(l1Id)),
              ...childSessions(yield* readAll(l2Id)),
              ...childSessions(yield* readAll(l3Id)),
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
    }),
  );
});

describe('delegation without recording', () => {
  it.effect('writes nothing at all', () =>
    Effect.gen(function* () {
      const found = yield* run(
        Effect.gen(function* () {
          yield* supervisor.run('go').pipe(Effect.orDie);
          const store = yield* LogStore.Service;
          return {
            parent: yield* store
              .meta(AgentLog.pathFor(PARENT))
              .pipe(Effect.orDie),
            child: yield* store
              .meta(AgentLog.pathFor(CHILD))
              .pipe(Effect.orDie),
          };
        }),
        oneDelegation,
      );

      // A `LogStore` being reachable is not consent to write to it — for a
      // child conversation any more than for the parent's.
      expect(Option.isNone(found.parent)).toBe(true);
      expect(Option.isNone(found.child)).toBe(true);
    }),
  );

  it.effect('still delegates and still returns the child’s answer', () =>
    Effect.gen(function* () {
      const result = yield* supervisor
        .run('go')
        .pipe(Effect.orDie, Effect.provide(scripted(oneDelegation)));

      // No `LogStoreMemory.layer` in this pipeline. If delegation had started
      // requiring one, this would not compile.
      expect(result.text).toBe('summarised');
    }),
  );
});
