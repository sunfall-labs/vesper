import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Deferred, Effect, Fiber, Layer, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { AgentLog } from '../src/log.js';

const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

const provider = (calls: { count: number }) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([finish]),
      streamText: () => {
        calls.count += 1;
        return Stream.fromIterable<Response.StreamPartEncoded>([
          { type: 'text-start', id: 'text' },
          { type: 'text-delta', id: 'text', delta: 'ok' },
          { type: 'text-end', id: 'text' },
          finish,
        ]);
      },
    }),
  );

const definition = (name = 'test', revision = '1') =>
  Agent.make({
    name,
    revision,
    instructions: 'be terse',
    toolkit: Toolkit.make(),
  });

const started = (
  agent = 'test',
  revision: string | undefined = '1',
  formatVersion: number | undefined = 1,
): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  agent,
  ...(revision === undefined ? {} : { agentRevision: revision }),
  ...(formatVersion === undefined ? {} : { formatVersion }),
  prompt: [],
});

const resume = (
  agent = 'test',
  revision = '1',
  formatVersion = 1,
): ConversationRecord.Record => ({
  _tag: 'RunSettled',
  outcome: 'success',
  detail: '',
  steps: 1,
  usage: { input: 1, output: 1 },
  resume: {
    formatVersion,
    agent,
    agentRevision: revision,
    usage: { input: 1, output: 1 },
    signalCursor: LogOffset.START,
  },
});

const seed = Effect.fn('test.seedCompatibility')(function* (
  id: string,
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  const path = AgentLog.pathFor(id);
  yield* store.create(path, id);
  const claim = yield* store.acquire(path, 'fixture');
  yield* store.append({
    path,
    producerId: claim.producerId,
    epoch: claim.epoch,
    sequence: claim.nextSequence,
    records: records.map((record) => ({
      conversationId: id,
      timestamp: 1_700_000_000_000,
      record,
    })),
  });
  return (yield* store.read(path)).records;
});

const run = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  calls: { count: number },
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(provider(calls)),
      Effect.provide(LogStoreMemory.layer),
      Effect.scoped,
    ) as Effect.Effect<A, E>,
  );

describe('durable compatibility', () => {
  it.each([
    [{ agent: '', revision: '1' }, 'agent name must be non-empty'],
    [{ agent: 'test', revision: ' ' }, 'revision must be non-empty'],
  ] as const)(
    'rejects invalid compatibility input',
    async (compatibility, message) => {
      const calls = { count: 0 };
      const caught = await run(
        AgentLog.open('invalid-input', { compatibility }).pipe(
          Effect.catchTag('@sunfall/vesper-agent/CompatibilityError', (error) =>
            Effect.succeed(error),
          ),
        ),
        calls,
      );

      expect(caught).toBeInstanceOf(AgentLog.CompatibilityError);
      if (caught instanceof AgentLog.CompatibilityError) {
        expect(caught.message).toContain(message);
      }
      expect(calls.count).toBe(0);
    },
  );

  it('validates fork compatibility input before reading either stream', async () => {
    const calls = { count: 0 };
    const outcome = await run(
      AgentLog.fork('missing-source', LogOffset.START, 'target', {
        agent: 'test',
        revision: '',
      }).pipe(Effect.result),
      calls,
    );

    expect(outcome).toMatchObject({
      _tag: 'Failure',
      failure: {
        _tag: '@sunfall/vesper-agent/CompatibilityError',
        expectedAgent: 'test',
      },
    });
  });

  it('resumes matching history', async () => {
    const calls = { count: 0 };
    const result = await run(
      Effect.gen(function* () {
        yield* seed('matching', [started()]);
        return yield* definition().resume('matching', 'continue');
      }),
      calls,
    );

    expect(result.text).toBe('ok');
    expect(calls.count).toBe(1);
  });

  it.each([
    ['revision mismatch', started('test', 'old'), 'revision "old"'],
    ['agent-name mismatch', started('other'), 'agent "other"'],
    ['future format', started('test', '1', 2), 'format 2 is unsupported'],
    [
      'legacy metadata',
      { _tag: 'RunStarted', agent: 'test', prompt: [] },
      'predates',
    ],
  ] as const)('rejects %s before a model call', async (_, record, message) => {
    const calls = { count: 0 };

    await expect(
      run(
        Effect.gen(function* () {
          yield* seed('incompatible', [record]);
          yield* definition().resume('incompatible', 'continue');
        }),
        calls,
      ),
    ).rejects.toThrow(message);
    expect(calls.count).toBe(0);
  });

  it.each([
    ['RunStarted', started('test', '2'), 'contradictory revision "2"'],
    [
      'Compacted',
      {
        _tag: 'Compacted',
        formatVersion: 1,
        agent: 'other',
        agentRevision: '1',
        step: 2,
        summary: 'contradiction',
        firstKept: LogOffset.START,
        summarizedMessages: 1,
        keptMessages: 0,
      } satisfies ConversationRecord.Record,
      'contradictory agent "other"',
    ],
    ['RunSettled.resume', resume('test', '1', 2), 'format 2 is unsupported'],
  ] as const)(
    'rejects contradictory %s metadata before acquiring',
    async (_, contradictory, message) => {
      const calls = { count: 0 };
      const observed = await run(
        Effect.gen(function* () {
          yield* seed('contradictory', [resume(), contradictory]);
          const store = yield* LogStore.Service;
          const path = AgentLog.pathFor('contradictory');
          const before = yield* store.meta(path);
          const opened = yield* AgentLog.open('contradictory', {
            compatibility: { agent: 'test', revision: '1' },
          }).pipe(Effect.result);
          const after = yield* store.meta(path);
          return { before, opened, after };
        }),
        calls,
      );

      expect(observed.opened).toMatchObject({
        _tag: 'Failure',
        failure: { message: expect.stringContaining(message) },
      });
      expect(observed.after).toEqual(observed.before);
      expect(calls.count).toBe(0);
    },
  );

  it('branches against compatibility at the retained point', async () => {
    const calls = { count: 0 };
    const result = await run(
      Effect.gen(function* () {
        const records = yield* seed('branch-source', [
          started('test', '1'),
          { _tag: 'Text', step: 1, text: 'keep' },
          started('test', '2'),
          { _tag: 'Text', step: 1, text: 'abandon' },
        ]);
        return yield* definition().branchFrom(
          'branch-source',
          records[1]!.offset,
          'continue',
        );
      }),
      calls,
    );

    expect(result.text).toBe('ok');
    expect(calls.count).toBe(1);
  });

  it('preserves compatibility when a prefix is forked', async () => {
    const calls = { count: 0 };
    const result = await run(
      Effect.gen(function* () {
        const records = yield* seed('fork-source', [
          started(),
          { _tag: 'Text', step: 1, text: 'keep' },
        ]);
        return yield* definition().forkFrom(
          'fork-source',
          records[1]!.offset,
          'fork-target',
          'continue',
        );
      }),
      calls,
    );

    expect(result.text).toBe('ok');
    expect(calls.count).toBe(1);
  });

  it('rejects incompatible branch and fork targets without changing history', async () => {
    const calls = { count: 0 };
    const observed = await run(
      Effect.gen(function* () {
        const records = yield* seed('incompatible-source', [
          started(),
          { _tag: 'Text', step: 1, text: 'keep' },
        ]);
        const branch = yield* definition('test', '2')
          .branchFrom('incompatible-source', records[1]!.offset, 'continue')
          .pipe(Effect.exit);
        const fork = yield* definition('test', '2')
          .forkFrom(
            'incompatible-source',
            records[1]!.offset,
            'incompatible-target',
            'continue',
          )
          .pipe(Effect.exit);
        const store = yield* LogStore.Service;
        return {
          branch,
          fork,
          source: (yield* store.read(AgentLog.pathFor('incompatible-source')))
            .records,
          target: yield* store.meta(AgentLog.pathFor('incompatible-target')),
        };
      }),
      calls,
    );

    expect(observed.branch._tag).toBe('Failure');
    expect(observed.fork._tag).toBe('Failure');
    expect(
      observed.source.some(({ record }) => record._tag === 'BranchedFrom'),
    ).toBe(false);
    expect(observed.target._tag).toBe('None');
    expect(calls.count).toBe(0);
  });

  it('does not fence a live producer when its append races an incompatible open', async () => {
    const path = AgentLog.pathFor('live-compatible');
    const acquireReached = await Effect.runPromise(Deferred.make<void>());
    const allowAcquire = await Effect.runPromise(Deferred.make<void>());
    let gateNextAcquire = false;
    const gated = Layer.effect(
      LogStore.Service,
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        return LogStore.Service.of({
          ...store,
          acquire: (requested, producerId, expected) =>
            Effect.gen(function* () {
              if (requested === path && gateNextAcquire) {
                gateNextAcquire = false;
                yield* Deferred.succeed(acquireReached, undefined);
                yield* Deferred.await(allowAcquire);
              }
              return yield* store.acquire(requested, producerId, expected);
            }),
        });
      }),
    ).pipe(Layer.provide(LogStoreMemory.layer));

    const observed = await Effect.runPromise(
      Effect.gen(function* () {
        const original = yield* AgentLog.open('live-compatible', {
          compatibility: { agent: 'test', revision: '1' },
        });
        gateNextAcquire = true;
        const incompatibleFiber = yield* AgentLog.open('live-compatible', {
          compatibility: { agent: 'test', revision: '2' },
        }).pipe(Effect.exit, Effect.forkChild);

        // The second opener has validated the empty stream but has not yet
        // attempted its compare-and-acquire.
        yield* Deferred.await(acquireReached);
        yield* original.append([started()]);
        yield* Deferred.succeed(allowAcquire, undefined);

        const incompatible = yield* Fiber.join(incompatibleFiber);
        yield* original.append([
          { _tag: 'Text', step: 1, text: 'still owns the producer epoch' },
        ]);
        const store = yield* LogStore.Service;
        return {
          incompatible,
          records: (yield* store.read(path)).records,
        };
      }).pipe(Effect.provide(gated), Effect.scoped),
    );

    expect(observed.incompatible._tag).toBe('Failure');
    expect(observed.records.at(-1)?.record).toMatchObject({
      _tag: 'Text',
      text: 'still owns the producer epoch',
    });
  });

  it('bounds compare-and-acquire retries under continuous contention', async () => {
    const path = AgentLog.pathFor('contended');
    let attempts = 0;
    const contended = Layer.effect(
      LogStore.Service,
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        return LogStore.Service.of({
          ...store,
          acquire: (requested, producerId, expected) => {
            if (requested !== path || expected === undefined) {
              return store.acquire(requested, producerId, expected);
            }
            attempts += 1;
            return Effect.fail(
              new LogStore.LogStoreError({
                path,
                operation: 'acquire',
                reason: 'conflict',
                detail: 'forced contention',
              }),
            );
          },
        });
      }),
    ).pipe(Layer.provide(LogStoreMemory.layer));

    const outcome = await Effect.runPromise(
      AgentLog.open('contended', {
        compatibility: { agent: 'test', revision: '1' },
      }).pipe(Effect.exit, Effect.provide(contended), Effect.scoped),
    );

    expect(outcome._tag).toBe('Failure');
    expect(attempts).toBe(4);
  });

  it('retains normal fencing for a compatible concurrent opener', async () => {
    const observed = await run(
      Effect.gen(function* () {
        const original = yield* AgentLog.open('compatible-handover', {
          compatibility: { agent: 'test', revision: '1' },
        });
        yield* original.append([started()]);
        const replacement = yield* AgentLog.open('compatible-handover', {
          compatibility: { agent: 'test', revision: '1' },
        });
        const staleAppend = yield* original
          .append([{ _tag: 'Text', step: 1, text: 'stale' }])
          .pipe(Effect.exit);
        yield* replacement.append([
          { _tag: 'Text', step: 1, text: 'replacement owns the epoch' },
        ]);
        const store = yield* LogStore.Service;
        return {
          staleAppend,
          records: (yield* store.read(AgentLog.pathFor('compatible-handover')))
            .records,
        };
      }),
      { count: 0 },
    );

    expect(observed.staleAppend._tag).toBe('Failure');
    expect(observed.records.at(-1)?.record).toMatchObject({
      _tag: 'Text',
      text: 'replacement owns the epoch',
    });
  });

  it('validates a child against the child revision independently', async () => {
    const calls = { count: 0 };

    await expect(
      run(
        Effect.gen(function* () {
          const parent = yield* AgentLog.open('parent', {
            compatibility: { agent: 'parent', revision: '1' },
          });
          const childSession = yield* parent.child({
            toolCallId: 'call-1',
            agent: 'child',
            revision: '1',
            depth: 1,
          });
          yield* childSession.append([started('child', '1')]);

          yield* parent.child({
            toolCallId: 'call-1',
            agent: 'child',
            revision: '2',
            depth: 1,
          });
        }),
        calls,
      ),
    ).rejects.toThrow('revision "1", not "2"');
    expect(calls.count).toBe(0);
  });
});
