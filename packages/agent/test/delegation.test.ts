import { LogStore } from '@sunfall/vesper-log/log-store';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import { Context, Effect, Layer, Ref, Schema, type Crypto } from 'effect';
import type { LanguageModel, Response } from 'effect/ai';
import { Tool, Toolkit } from 'effect/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import {
  calls,
  childSessions,
  delegates,
  recordsOf,
  says,
  scripted,
  show,
  first,
  tags,
  testLogLayer,
  type Seen,
} from './delegation-fixtures.js';

// Delegation scenarios: a parent whose model asks for `task_<child>` runs the
// child in its own, derived conversation, and only the child's final text
// returns to the parent.

const researcher = Agent.make({
  name: 'researcher',
  revision: '1',
  description: 'Looks things up.',
  instructions: 'Answer concisely.',
  toolkit: Toolkit.make(),
});

const supervisor = Agent.make({
  name: 'supervisor',
  revision: '1',
  instructions: 'delegate',
  toolkit: Toolkit.make(),
  subagents: [researcher],
});

// A parent that crashed mid-delegation re-issues the call when resumed.
const resuming = supervisor.intercepting({
  onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
});

const oneDelegation = [
  delegates('call-a', 'researcher'),
  says('researched'),
  says('summarised'),
];

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  turns: ReadonlyArray<Response.StreamPartEncoded[]>,
  seen?: Seen,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(Layer.merge(scripted(turns, seen), testLogLayer)),
    Effect.scoped,
  );

/** Run once and return the parent's records plus the child's. */
const delegateOnce = (parent: string) =>
  Effect.gen(function* () {
    const result = yield* Conversation.make(supervisor, parent).run('go');
    const parentRecords = yield* recordsOf(supervisor, parent);
    const link = first(childSessions(parentRecords));
    const childRecords = yield* recordsOf(supervisor, link.childConversationId);
    return { result, parentRecords, link, childRecords };
  });

interface Crash {
  readonly op: 'create' | 'append';
  /** Matches the target, the records appended, and the conversation's first path. */
  readonly when: (input: {
    readonly path: string;
    readonly tags: ReadonlyArray<string>;
    readonly root: string;
  }) => boolean;
  /** Perform the write, then crash (the write is durable). */
  readonly after: boolean;
}

/** Each crash fires once, the first time its write is attempted. */
const crashing = (
  ...crashes: ReadonlyArray<Crash>
): Layer.Layer<LogStore.Service> =>
  Layer.effect(
    LogStore.Service,
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const fired = yield* Ref.make<ReadonlyArray<Crash>>([]);
      const root = yield* Ref.make('');
      const guard = <A>(
        op: Crash['op'],
        path: string,
        tagsOf: ReadonlyArray<string>,
        write: Effect.Effect<A, LogStore.LogStoreError>,
      ) =>
        Effect.gen(function* () {
          if (op === 'create') {
            yield* Ref.update(root, (r) => (r === '' ? path : r));
          }
          const rootPath = yield* Ref.get(root);
          const hit = yield* Ref.modify(fired, (done) => {
            const crash = crashes.find(
              (c) =>
                !done.includes(c) &&
                c.op === op &&
                c.when({ path, tags: tagsOf, root: rootPath }),
            );
            return crash ? [crash, [...done, crash]] : [undefined, done];
          });
          const fail = new LogStore.LogStoreError({
            path,
            operation: op,
            reason: 'storage',
            detail: 'injected crash',
          });
          if (hit !== undefined && !hit.after) {
            return yield* fail;
          }
          const value = yield* write;
          return hit ? yield* fail : value;
        });
      return LogStore.Service.of({
        ...store,
        create: (path, identity) =>
          guard('create', path, [], store.create(path, identity)),
        append: (input) =>
          guard(
            'append',
            input.path,
            input.records.map(({ record }) => record._tag),
            store.append(input),
          ),
      });
    }),
  ).pipe(Layer.provide(testLogLayer));

/** Crash the parent's tool-outcome write so the delegated call is re-issued. */
const parentLosesOutcome: Crash = {
  op: 'append',
  after: false,
  when: ({ path, tags: t, root }) => path === root && t.includes('StepSettled'),
};

describe('a recorded delegation', () => {
  it.effect('links parent and child, and records the child turn by turn', () =>
    Effect.gen(function* () {
      const seen: Seen = { prompts: [], tools: [] };
      const out = yield* run(delegateOnce('parent'), oneDelegation, seen);

      // The model was offered a tool named from the child, with its description.
      expect(show(seen.tools[0])).toContain('task_researcher');
      expect(show(seen.tools[0])).toContain('Looks things up.');

      // The parent only receives what the child said.
      expect(out.result.text).toBe('summarised');
      expect(out.link).toMatchObject({
        toolCallId: 'call-a',
        agent: 'researcher',
        parentConversationId: 'parent',
        depth: 1,
      });
      // The child's log opens with the same reference and holds its own run.
      expect(out.childRecords[0]?.record).toEqual(out.link);
      expect(tags(out.childRecords)).toEqual([
        'ChildSession',
        'RunStarted',
        'Text',
        'TurnFinished',
        'Completed',
        'RunSettled',
      ]);
      expect(out.childRecords[2]?.record).toMatchObject({ text: 'researched' });
      // The child ran in a fresh conversation: its prompt lacks the parent's.
      expect(show(seen.prompts[1])).not.toContain('"go"');
    }),
  );

  it.effect(
    'derives child ids: stable on re-run, no separator collisions',
    () =>
      Effect.gen(function* () {
        const ids = yield* run(
          Effect.gen(function* () {
            const conversation = Conversation.make(supervisor, 'a/b');
            yield* conversation.run('go');
            yield* conversation.run('again');
            const twice = childSessions(yield* recordsOf(supervisor, 'a/b'));
            const other = yield* Conversation.make(supervisor, 'a').run('go');
            const sibling = childSessions(yield* recordsOf(supervisor, 'a'));
            return { twice, other, sibling };
          }),
          // Same tool-call id ("b/c" vs "c") would collide under naive joining.
          [
            delegates('c', 'researcher'),
            says('r1'),
            says('s1'),
            delegates('c', 'researcher'),
            says('r2'),
            says('s2'),
            delegates('b/c', 'researcher'),
            says('r3'),
            says('s3'),
          ],
        );

        // Re-running the same call resumes the same child rather than forking.
        expect(new Set(ids.twice.map((l) => l.childConversationId)).size).toBe(
          1,
        );
        expect(ids.sibling[0]?.childConversationId).not.toBe(
          ids.twice[0]?.childConversationId,
        );
      }),
  );

  for (const stage of ['parent-link', 'child-open', 'child-link'] as const) {
    it.effect(`repairs both links after a crash following ${stage}`, () =>
      Effect.gen(function* () {
        const inject = crashing(
          stage === 'child-open'
            ? {
                op: 'create',
                after: true,
                when: ({ path, root }) => path !== root,
              }
            : {
                op: 'append',
                after: true,
                when: ({ path, tags: t, root }) =>
                  t.includes('ChildSession') &&
                  (stage === 'parent-link' ? path === root : path !== root),
              },
          parentLosesOutcome,
        );
        const out = yield* Effect.gen(function* () {
          const crashed = yield* Conversation.make(resuming, 'repair')
            .run('go')
            .pipe(Effect.exit);
          yield* Conversation.make(resuming, 'repair').run('go');
          const parent = childSessions(yield* recordsOf(supervisor, 'repair'));
          const child = childSessions(
            yield* recordsOf(supervisor, first(parent).childConversationId),
          );
          return { crashed, parent, child };
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              inject,
              scripted([...oneDelegation, ...oneDelegation]),
              NodeCrypto.layer,
            ),
          ),
          Effect.scoped,
        );

        expect(out.crashed._tag).toBe('Failure');
        expect(out.parent).toHaveLength(1);
        expect(out.child).toHaveLength(1);
        expect(out.parent[0]).toEqual(out.child[0]);
      }),
    );
  }

  it.effect(
    'does not repeat completed child work when the parent crashed',
    () =>
      Effect.gen(function* () {
        const seen: Seen = { prompts: [], tools: [] };
        const inject = crashing(parentLosesOutcome);
        const out = yield* Effect.gen(function* () {
          const conversation = Conversation.make(resuming, 'resume');
          const crashed = yield* conversation.run('go').pipe(Effect.exit);
          const second = yield* conversation.run('go');
          const link = first(
            childSessions(yield* recordsOf(supervisor, 'resume')),
          );
          const child = yield* recordsOf(supervisor, link.childConversationId);
          return { crashed, second, child };
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              inject,
              scripted(oneDelegation, seen),
              NodeCrypto.layer,
            ),
          ),
          Effect.scoped,
        );

        expect(out.crashed._tag).toBe('Failure');
        expect(out.second.text).toBe('summarised');
        expect(tags(out.child).filter((t) => t === 'RunStarted')).toHaveLength(
          1,
        );
        // parent delegate, child, parent final: the child was not asked twice.
        expect(seen.prompts).toHaveLength(3);
      }),
  );
});

describe('a child that crashed partway through its turns', () => {
  it.effect('continues from its log without repeating turns or tools', () =>
    Effect.gen(function* () {
      const runs = { lookups: 0 };
      const lookup = Tool.make('lookup', {
        description: 'look something up',
        parameters: Schema.Struct({ q: Schema.String }),
        success: Schema.Struct({ answer: Schema.String }),
      });
      const digger = Agent.make({
        name: 'digger',
        revision: '1',
        description: 'Digs.',
        instructions: 'dig',
        toolkit: Toolkit.make(lookup),
      }).withHandlers({
        lookup: () =>
          Effect.sync(() => {
            runs.lookups += 1;
            return { answer: 'found' };
          }),
      });
      const boss = Agent.make({
        name: 'boss',
        revision: '1',
        instructions: 'delegate',
        toolkit: Toolkit.make(),
        subagents: [digger],
      }).intercepting({
        onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
      });
      const seen: Seen = { prompts: [], tools: [] };
      // The child's tool outcome is durable, then the process dies before
      // the parent can record how the delegation ended.
      const inject = crashing(
        {
          op: 'append',
          after: true,
          when: ({ path, tags: t, root }) =>
            path !== root && t.includes('StepSettled'),
        },
        parentLosesOutcome,
      );
      const out = yield* Effect.gen(function* () {
        const conversation = Conversation.make(boss, 'child-crash');
        const crashed = yield* conversation.run('go').pipe(Effect.exit);
        const lookupsAtCrash = runs.lookups;
        const second = yield* conversation.run('go');
        const link = first(
          childSessions(yield* recordsOf(boss, 'child-crash')),
        );
        const child = yield* recordsOf(boss, link.childConversationId);
        return { crashed, lookupsAtCrash, second, child };
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            inject,
            scripted(
              [
                delegates('call-a', 'digger'),
                calls('call-l', 'lookup', { q: 'x' }),
                says('dug'),
                says('parent done'),
              ],
              seen,
            ),
            NodeCrypto.layer,
          ),
        ),
        Effect.scoped,
      );

      expect(out.crashed._tag).toBe('Failure');
      expect(out.lookupsAtCrash).toBe(1);
      expect(out.second.text).toBe('parent done');
      expect(runs.lookups).toBe(1);
      expect(tags(out.child).filter((t) => t === 'StepOpened')).toHaveLength(1);
      expect(tags(out.child).at(-1)).toBe('RunSettled');
      // parent, child tool turn, child final, parent final: nothing repeated.
      expect(seen.prompts).toHaveLength(4);
    }),
  );
});

const delegating = (
  name: string,
  child: Agent.Child<string, LanguageModel.LanguageModel | Crypto.Crypto>,
) =>
  Agent.make({
    name,
    revision: '1',
    description: `Delegates to ${child.name}.`,
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [child],
  });

describe('delegation depth', () => {
  const sink = Agent.make({
    name: 'sink',
    revision: '1',
    description: 'Never gets to run.',
    instructions: 'work',
    toolkit: Toolkit.make(),
  });
  const l1 = delegating('l1', sink);

  it.effect('carries depth through nesting, refusing past the cap', () =>
    Effect.gen(function* () {
      // top -> l1 -> l2 -> l3 -> l4; l4's delegation to the sink is the fifth.
      const root = delegating(
        'top',
        delegating(
          'l1',
          delegating('l2', delegating('l3', delegating('l4', sink))),
        ),
      );
      const observed = yield* run(
        Effect.gen(function* () {
          const result = yield* Conversation.make(root, 'deep').run('go');
          const depths: number[] = [];
          let id = 'deep';
          for (let level = 0; level < 4; level += 1) {
            const links = childSessions(yield* recordsOf(root, id));
            depths.push(...links.map((link) => link.depth));
            const next = links.at(-1);
            if (next === undefined) {
              break;
            }
            id = next.childConversationId;
          }
          const deepest = yield* recordsOf(root, id);
          return { result, depths, deepest };
        }),
        [
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
        ],
      );

      expect(observed.depths).toEqual([1, 1, 2, 2, 3, 3, 4]);
      expect(observed.result.text).toBe('top summary');
      // The refusal returned to the model as a failed tool result, and no
      // fifth conversation was opened.
      expect(childSessions(observed.deepest)).toHaveLength(1);
      const [refusal] = observed.deepest.flatMap(({ record }) =>
        record._tag === 'StepSettled' ? [record] : [],
      );
      expect(refusal).toMatchObject({ name: 'task_sink', outcome: 'failure' });
      expect(show(refusal)).toContain('depth 4');
    }),
  );

  it.effect('honours an application depth limit from the run policy', () =>
    Effect.gen(function* () {
      const root = Agent.make({
        name: 'top',
        revision: '1',
        instructions: 'delegate',
        toolkit: Toolkit.make(),
        subagents: [l1],
        runPolicy: { maxDelegationDepth: 1 },
      });
      const seen: Seen = { prompts: [], tools: [] };
      const result = yield* run(
        Conversation.make(root, 'limited').run('go'),
        [
          delegates('c1', 'l1'),
          delegates('c2', 'sink'),
          says('did it myself'),
          says('top summary'),
        ],
        seen,
      );

      expect(result.text).toBe('top summary');
      // Turns: root, l1 (asks sink), l1 (after refusal), root. The sink never ran.
      expect(seen.prompts).toHaveLength(4);
      expect(show(seen.prompts[2])).toContain('depth 1');
    }),
  );
});

describe('delegation without recording', () => {
  it.effect('delegates and answers with no LogStore at all', () =>
    Effect.gen(function* () {
      const result = yield* supervisor
        .run('go')
        .pipe(
          Effect.orDie,
          Effect.provide(
            Layer.merge(scripted(oneDelegation), NodeCrypto.layer),
          ),
        );

      // No log layer is provided: delegation writes nothing and needs nothing.
      expect(result.text).toBe('summarised');
    }),
  );
});

describe('a child with its own service requirements', () => {
  class Notebook extends Context.Service<Notebook, { readonly note: string }>()(
    'test/Notebook',
  ) {}

  const write = Tool.make('write', {
    description: 'write a note',
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.Struct({ note: Schema.String }),
    dependencies: [Notebook],
  });
  const scribe = Agent.make({
    name: 'scribe',
    revision: '1',
    description: 'Writes notes.',
    instructions: 'write it down',
    toolkit: Toolkit.make(write),
  }).withHandlers({
    write: ({ text }) =>
      Effect.gen(function* () {
        const notebook = yield* Notebook;
        return { note: `${notebook.note}:${text}` };
      }),
  });
  const boss = Agent.make({
    name: 'boss',
    revision: '1',
    instructions: 'delegate',
    toolkit: Toolkit.make(),
    subagents: [scribe],
  });

  it.effect(
    'requires the service from the parent and provides it to the child',
    () =>
      Effect.gen(function* () {
        const result = yield* run(
          Conversation.make(boss, 'scribing')
            .run('go')
            .pipe(Effect.provideService(Notebook, { note: 'recorded' })),
          [
            delegates('c1', 'scribe'),
            calls('w1', 'write', { text: 'hi' }),
            says('written'),
            says('all done'),
          ],
        );

        expect(result.text).toBe('all done');
      }),
  );
});
