import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import * as AgentLog from '../src/log.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

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
  digest?: string,
): ConversationRecord.Record => {
  return {
    _tag: 'RunStarted',
    agent,
    ...(revision === undefined
      ? {}
      : { agentRevision: LogVocabulary.AgentRevision.make(revision) }),
    ...(formatVersion === undefined ? {} : { formatVersion }),
    ...(digest === undefined
      ? {}
      : { agentDigest: LogVocabulary.AgentDefinitionDigest.make(digest) }),
    prompt: [],
  };
};

const invalidCompatibility = (value: unknown) =>
  value as NonNullable<Parameters<typeof AgentLog.open>[1]>['compatibility'];

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
    agentRevision: LogVocabulary.AgentRevision.make(revision),
    usage: { input: 1, output: 1 },
    signalCursor: LogOffset.START,
  },
});

const seed = Effect.fn('test.seedCompatibility')(function* (
  id: string,
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  const conversationId = LogVocabulary.ConversationId.make(id);
  const path = AgentLog.pathFor(conversationId);
  yield* store.create(path, id);
  const claim = yield* store.acquire(
    path,
    LogVocabulary.ProducerId.make('fixture'),
  );
  yield* store.append({
    path,
    producerId: claim.producerId,
    epoch: claim.epoch,
    sequence: claim.nextSequence,
    records: records.map((record) => ({
      conversationId,
      timestamp: 1_700_000_000_000,
      record,
    })),
  });
  return (yield* store.read(path)).records;
});

const run = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  calls: { count: number },
) => effect.pipe(Effect.provide(Layer.merge(provider(calls), testLogLayer)));

describe('durable compatibility', () => {
  it.effect.each([
    [{ agent: '', revision: '1' }, 'agent name must be non-empty'],
    [{ agent: 'test', revision: ' ' }, 'revision must be non-empty'],
  ] as const)(
    'rejects invalid compatibility input',
    ([compatibility, message]) =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const caught = yield* run(
          AgentLog.open(LogVocabulary.ConversationId.make('invalid-input'), {
            compatibility: invalidCompatibility(compatibility),
          }).pipe(
            Effect.catchTag('CompatibilityError', (error) =>
              Effect.succeed(error),
            ),
          ),
          calls,
        );

        expect(caught).toBeInstanceOf(Conversation.CompatibilityError);
        if (caught instanceof Conversation.CompatibilityError) {
          expect(caught.message).toContain(message);
        }
        expect(calls.count).toBe(0);
      }),
  );

  it.effect(
    'validates fork compatibility input before reading either stream',
    () =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const outcome = yield* run(
          AgentLog.fork(
            LogVocabulary.ConversationId.make('missing-source'),
            LogOffset.START,
            LogVocabulary.ConversationId.make('target'),
            invalidCompatibility({ agent: 'test', revision: '' }),
          ).pipe(Effect.result),
          calls,
        );

        expect(outcome).toMatchObject({
          _tag: 'Failure',
          failure: {
            _tag: 'CompatibilityError',
            expectedAgent: 'test',
          },
        });
      }),
  );

  it.effect('resumes matching history', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const result = yield* run(
        Effect.gen(function* () {
          yield* seed('matching', [started()]);
          return yield* Conversation.make(definition(), 'matching').run(
            'continue',
          );
        }),
        calls,
      );

      expect(result.text).toBe('ok');
      expect(calls.count).toBe(1);
    }),
  );

  it.effect('resumes history recorded before the digest field existed', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const result = yield* run(
        Effect.gen(function* () {
          // `started()` writes no `agentDigest`, simulating a `RunStarted`
          // recorded before this feature existed. `definition()` builds a
          // real `Agent`, whose `digest` is always defined — this is the
          // "absent digest in an older record is accepted as compatible"
          // half of the contract.
          yield* seed('no-digest', [started('test', '1', 1)]);
          return yield* Conversation.make(definition(), 'no-digest').run(
            'continue',
          );
        }),
        calls,
      );

      expect(result.text).toBe('ok');
      expect(calls.count).toBe(1);
    }),
  );

  it.effect(
    'rejects a same-revision history recorded under a different digest',
    () =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const agent = definition();
        // A digest that cannot coincidentally equal the real one: it is not
        // even the right shape's worth of entropy, only its length.
        const staleDigest = '0'.repeat(63) + '1';

        const failure = yield* run(
          Effect.gen(function* () {
            yield* seed('digest-mismatch', [
              started('test', '1', 1, staleDigest),
            ]);
            yield* Conversation.make(agent, 'digest-mismatch').run('continue');
          }),
          calls,
        ).pipe(Effect.flip);

        expect(failure).toBeInstanceOf(Conversation.CompatibilityError);
        expect(failure.message).toContain('bump revision');
        expect(failure.message).toContain(staleDigest);
        expect(failure.message).toContain(agent.digest);
        if (failure._tag === 'CompatibilityError') {
          expect(failure.persistedDigest).toBe(staleDigest);
          expect(failure.expectedDigest).toBe(agent.digest);
        }
        expect(calls.count).toBe(0);
      }),
  );

  it.effect.each([
    ['revision mismatch', started('test', 'old'), 'revision "old"'],
    ['agent-name mismatch', started('other'), 'agent "other"'],
    ['future format', started('test', '1', 2), 'format 2 is unsupported'],
    [
      'legacy metadata',
      { _tag: 'RunStarted', agent: 'test', prompt: [] },
      'predates',
    ],
  ] as const)('rejects %s before a model call', ([, record, message]) =>
    Effect.gen(function* () {
      const calls = { count: 0 };

      const failure = yield* run(
        Effect.gen(function* () {
          yield* seed('incompatible', [record]);
          yield* Conversation.make(definition(), 'incompatible').run(
            'continue',
          );
        }),
        calls,
      ).pipe(Effect.flip);
      expect(failure.message).toContain(message);
      expect(calls.count).toBe(0);
    }),
  );

  it.effect.each([
    ['RunStarted', started('test', '2'), 'contradictory revision "2"'],
    [
      'Compacted',
      {
        _tag: 'Compacted',
        formatVersion: 1,
        agent: 'other',
        agentRevision: LogVocabulary.AgentRevision.make('1'),
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
    ([, contradictory, message]) =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const observed = yield* run(
          Effect.gen(function* () {
            yield* seed('contradictory', [resume(), contradictory]);
            const store = yield* LogStore.Service;
            const path = AgentLog.pathFor(
              LogVocabulary.ConversationId.make('contradictory'),
            );
            const before = yield* store.meta(path);
            const opened = yield* AgentLog.open(
              LogVocabulary.ConversationId.make('contradictory'),
              {
                compatibility: {
                  agent: 'test',
                  revision: LogVocabulary.AgentRevision.make('1'),
                },
              },
            ).pipe(Effect.result);
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
      }),
  );

  it.effect('branches against compatibility at the retained point', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const result = yield* run(
        Effect.gen(function* () {
          const records = yield* seed('branch-source', [
            started('test', '1'),
            { _tag: 'Text', step: 1, text: 'keep' },
            started('test', '2'),
            { _tag: 'Text', step: 1, text: 'abandon' },
          ]);
          const record = records.at(1);
          if (record === undefined) {
            throw new Error('missing branch record');
          }
          return yield* Conversation.make(
            definition(),
            'branch-source',
          ).branchFrom(record.offset, 'continue');
        }),
        calls,
      );

      expect(result.text).toBe('ok');
      expect(calls.count).toBe(1);
    }),
  );

  it.effect('preserves compatibility when a prefix is forked', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const result = yield* run(
        Effect.gen(function* () {
          const records = yield* seed('fork-source', [
            started(),
            { _tag: 'Text', step: 1, text: 'keep' },
          ]);
          const record = records.at(1);
          if (record === undefined) {
            throw new Error('missing fork record');
          }
          return yield* Conversation.make(definition(), 'fork-source').forkFrom(
            record.offset,
            'fork-target',
            'continue',
          );
        }),
        calls,
      );

      expect(result.text).toBe('ok');
      expect(calls.count).toBe(1);
    }),
  );

  it.effect(
    'rejects incompatible branch and fork targets without changing history',
    () =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const observed = yield* run(
          Effect.gen(function* () {
            const records = yield* seed('incompatible-source', [
              started(),
              { _tag: 'Text', step: 1, text: 'keep' },
            ]);
            const record = records.at(1);
            if (record === undefined) {
              throw new Error('missing source record');
            }
            const incompatible = Conversation.make(
              definition('test', '2'),
              'incompatible-source',
            );
            const branch = yield* incompatible
              .branchFrom(record.offset, 'continue')
              .pipe(Effect.exit);
            const fork = yield* incompatible
              .forkFrom(record.offset, 'incompatible-target', 'continue')
              .pipe(Effect.exit);
            const store = yield* LogStore.Service;
            return {
              branch,
              fork,
              source: (yield* store.read(
                AgentLog.pathFor(
                  LogVocabulary.ConversationId.make('incompatible-source'),
                ),
              )).records,
              target: yield* store.meta(
                AgentLog.pathFor(
                  LogVocabulary.ConversationId.make('incompatible-target'),
                ),
              ),
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
      }),
  );

  it.effect(
    'does not fence a live producer when its append races an incompatible open',
    () =>
      Effect.gen(function* () {
        const conversationId =
          LogVocabulary.ConversationId.make('live-compatible');
        const path = AgentLog.pathFor(conversationId);
        const acquireReached = yield* Deferred.make<void>();
        const allowAcquire = yield* Deferred.make<void>();
        let gateNextAcquire = false;
        const gated = Layer.mergeAll(
          Layer.effect(
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
                    return yield* store.acquire(
                      requested,
                      producerId,
                      expected,
                    );
                  }),
              });
            }),
          ).pipe(Layer.provide(testLogLayer)),
          NodeServices.layer,
        );

        const observed = yield* Effect.gen(function* () {
          const original = yield* AgentLog.open(conversationId, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          gateNextAcquire = true;
          const incompatibleFiber = yield* AgentLog.open(conversationId, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('2'),
            },
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
        }).pipe(Effect.provide(gated));

        expect(observed.incompatible._tag).toBe('Failure');
        expect(observed.records.at(-1)?.record).toMatchObject({
          _tag: 'Text',
          text: 'still owns the producer epoch',
        });
      }),
  );

  it.effect(
    'bounds compare-and-acquire retries under continuous contention',
    () =>
      Effect.gen(function* () {
        const conversationId = LogVocabulary.ConversationId.make('contended');
        const path = AgentLog.pathFor(conversationId);
        let attempts = 0;
        const contended = Layer.mergeAll(
          Layer.effect(
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
          ).pipe(Layer.provide(testLogLayer)),
          NodeServices.layer,
        );

        const outcome = yield* AgentLog.open(conversationId, {
          compatibility: {
            agent: 'test',
            revision: LogVocabulary.AgentRevision.make('1'),
          },
        }).pipe(Effect.exit, Effect.provide(contended));

        expect(outcome._tag).toBe('Failure');
        expect(attempts).toBe(4);
      }),
  );

  it.effect('retains normal fencing for a compatible concurrent opener', () =>
    Effect.gen(function* () {
      const observed = yield* run(
        Effect.gen(function* () {
          const conversationId = LogVocabulary.ConversationId.make(
            'compatible-handover',
          );
          const original = yield* AgentLog.open(conversationId, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          yield* original.append([started()]);
          const replacement = yield* AgentLog.open(conversationId, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
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
            records: (yield* store.read(AgentLog.pathFor(conversationId)))
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
    }),
  );

  it.effect('validates a child against the child revision independently', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };

      const failure = yield* run(
        Effect.gen(function* () {
          const parent = yield* AgentLog.open(
            LogVocabulary.ConversationId.make('parent'),
            {
              compatibility: {
                agent: 'parent',
                revision: LogVocabulary.AgentRevision.make('1'),
              },
            },
          );
          const childSession = yield* parent.child({
            toolCallId: LogVocabulary.ToolCallId.make('call-1'),
            agent: 'child',
            revision: LogVocabulary.AgentRevision.make('1'),
            depth: 1,
          });
          yield* childSession.append([started('child', '1')]);

          yield* parent.child({
            toolCallId: LogVocabulary.ToolCallId.make('call-1'),
            agent: 'child',
            revision: LogVocabulary.AgentRevision.make('2'),
            depth: 1,
          });
        }),
        calls,
      ).pipe(Effect.flip);
      expect(failure.message).toContain('revision "1", not "2"');
      expect(calls.count).toBe(0);
    }),
  );
});
