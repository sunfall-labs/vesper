import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { type LanguageModel, Toolkit } from 'effect/ai';
import { type Crypto, Deferred, Effect, Fiber, Layer } from 'effect';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import * as AgentLog from '../src/log.js';
import {
  compatibility,
  openSession,
  present,
  provider,
  readRecords,
  run,
  said,
  says,
  seed,
  settled,
  started,
  testLogLayer,
} from './durability-fixtures.js';

// Durable compatibility: a conversation recorded by one agent revision is
// only resumed, branched, or forked by that revision, and the check happens
// before anything is fenced, written, or sent to a model.

// A caller that bypasses the type, which `open` still has to reject.
const invalidCompatibility = (value: unknown) =>
  value as NonNullable<Parameters<typeof AgentLog.open>[1]>['compatibility'];

const startedAs = (agent: string, revision = '1', formatVersion = 1) =>
  started('hi', agent, revision, formatVersion);

const definition = (name = 'test', revision = '1') =>
  Agent.make({
    name,
    revision,
    instructions: 'be terse',
    toolkit: Toolkit.make(),
  });

const resumeAggregate = (formatVersion: number): ConversationRecord.Record => ({
  ...settled(),
  resume: {
    formatVersion,
    agent: 'test',
    agentRevision: LogVocabulary.AgentRevision.make('1'),
    usage: { input: 1, output: 1 },
    signalCursor: LogOffset.START,
  },
});

/** Failures stay typed so a scenario can assert on them. */
const withModel = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
) => {
  const models = provider([says('ok')]);
  return {
    models,
    effect: effect.pipe(
      Effect.provide(Layer.merge(models.layer, testLogLayer)),
      Effect.scoped,
    ),
  };
};

describe('durable compatibility', () => {
  it.effect('resumes matching history', () =>
    Effect.gen(function* () {
      const { models, effect } = withModel(
        Effect.gen(function* () {
          yield* seed('matching', [started()]);
          return yield* Conversation.make(definition(), 'matching').run(
            'continue',
          );
        }),
      );
      expect((yield* effect).text).toBe('ok');
      expect(models.asked).toHaveLength(1);
    }),
  );

  it.effect.each([
    ['revision mismatch', startedAs('test', 'old'), 'revision "old"'],
    ['agent-name mismatch', startedAs('other'), 'agent "other"'],
    ['future format', startedAs('test', '1', 2), 'format 2 is unsupported'],
  ] as const)('rejects %s before a model call', ([, record, message]) =>
    Effect.gen(function* () {
      const { models, effect } = withModel(
        Effect.gen(function* () {
          yield* seed('incompatible', [record]);
          yield* Conversation.make(definition(), 'incompatible').run(
            'continue',
          );
        }),
      );
      const failure = yield* Effect.flip(effect);
      expect(failure.message).toContain(message);
      expect(models.asked).toHaveLength(0);
    }),
  );

  it.effect.each([
    [{ agent: '', revision: '1' }, 'agent name must be non-empty'],
    [{ agent: 'test', revision: ' ' }, 'revision must be non-empty'],
  ] as const)('rejects invalid compatibility input %#', ([invalid, message]) =>
    Effect.gen(function* () {
      const caught = yield* run(
        AgentLog.open(LogVocabulary.ConversationId.make('invalid-input'), {
          // Deliberately outside the type: a caller bypassing it.
          compatibility: invalidCompatibility(invalid),
        }).pipe(Effect.flip),
        provider([says('ok')]).layer,
      );
      expect(caught).toBeInstanceOf(Conversation.CompatibilityError);
      expect(caught.message).toContain(message);
    }),
  );

  // A history that contradicts itself is refused before the producer is
  // acquired, so a bad open cannot fence a healthy writer.
  it.effect.each([
    ['RunStarted', startedAs('test', '2'), 'contradictory revision "2"'],
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
    ['RunSettled.resume', resumeAggregate(2), 'format 2 is unsupported'],
  ] as const)(
    'rejects contradictory %s metadata before acquiring',
    ([, contradictory, message]) =>
      Effect.gen(function* () {
        const { effect } = withModel(
          Effect.gen(function* () {
            yield* seed('contradictory', [resumeAggregate(1), contradictory]);
            const store = yield* LogStore.Service;
            const path = AgentLog.pathFor(
              LogVocabulary.ConversationId.make('contradictory'),
            );
            const before = yield* store.meta(path);
            const opened = yield* openSession('contradictory').pipe(
              Effect.flip,
            );
            return { before, opened, after: yield* store.meta(path) };
          }),
        );
        const observed = yield* effect;

        expect(observed.opened.message).toContain(message);
        expect(observed.after).toEqual(observed.before);
      }),
  );

  it.effect(
    'branches and forks against compatibility at the retained point',
    () =>
      Effect.gen(function* () {
        const { models, effect } = withModel(
          Effect.gen(function* () {
            // The abandoned range was recorded by revision 2; the retained point
            // is revision 1, which is what the branch is validated against.
            yield* seed('branch-source', [
              startedAs('test', '1'),
              said('keep'),
              startedAs('test', '2'),
              said('abandon'),
            ]);
            const keep = present(
              (yield* readRecords('branch-source'))[1],
            ).offset;
            const conversation = Conversation.make(
              definition(),
              'branch-source',
            );
            const branched = yield* conversation.branchFrom(keep, 'continue');
            const forked = yield* conversation.forkFrom(
              keep,
              'fork-target',
              'continue',
            );
            return { branched, forked };
          }),
        );
        const result = yield* effect;
        expect(result.branched.text).toBe('ok');
        expect(result.forked.text).toBe('ok');
        expect(models.asked).toHaveLength(2);
      }),
  );

  it.effect(
    'rejects incompatible branch and fork targets without changing history',
    () =>
      Effect.gen(function* () {
        const { models, effect } = withModel(
          Effect.gen(function* () {
            yield* seed('incompatible-source', [started(), said('keep')]);
            const keep = present(
              (yield* readRecords('incompatible-source'))[1],
            ).offset;
            const incompatible = Conversation.make(
              definition('test', '2'),
              'incompatible-source',
            );
            const branch = yield* incompatible
              .branchFrom(keep, 'continue')
              .pipe(Effect.exit);
            const fork = yield* incompatible
              .forkFrom(keep, 'incompatible-target', 'continue')
              .pipe(Effect.exit);
            const store = yield* LogStore.Service;
            return {
              branch,
              fork,
              source: yield* readRecords('incompatible-source'),
              target: yield* store.meta(
                AgentLog.pathFor(
                  LogVocabulary.ConversationId.make('incompatible-target'),
                ),
              ),
            };
          }),
        );
        const observed = yield* effect;

        expect(observed.branch._tag).toBe('Failure');
        expect(observed.fork._tag).toBe('Failure');
        expect(observed.source.map(({ record }) => record._tag)).toEqual([
          'RunStarted',
          'Text',
        ]);
        expect(observed.target._tag).toBe('None');
        expect(models.asked).toHaveLength(0);
      }),
  );

  it.effect('validates a child against the child revision independently', () =>
    Effect.gen(function* () {
      const { models, effect } = withModel(
        Effect.gen(function* () {
          const parent = yield* openSession('parent');
          const child = (revision: string) =>
            parent.child({
              toolCallId: LogVocabulary.ToolCallId.make('call-1'),
              agent: 'child',
              revision: LogVocabulary.AgentRevision.make(revision),
              depth: 1,
            });
          yield* (yield* child('1')).append([startedAs('child', '1')]);
          return yield* Effect.flip(child('2'));
        }),
      );
      expect((yield* effect).message).toContain('revision "1", not "2"');
      expect(models.asked).toHaveLength(0);
    }),
  );
});

/** Wraps the store's `acquire`, which the tests below gate or sabotage. */
const wrapAcquire = (
  wrap: (
    store: LogStore.Interface,
    requested: string,
    producerId: LogVocabulary.ProducerId,
    expected: Parameters<LogStore.Interface['acquire']>[2],
  ) => ReturnType<LogStore.Interface['acquire']>,
) =>
  Layer.mergeAll(
    Layer.effect(
      LogStore.Service,
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        return LogStore.Service.of({
          ...store,
          acquire: (requested, producerId, expected) =>
            wrap(store, requested, producerId, expected),
        });
      }),
    ).pipe(Layer.provide(testLogLayer)),
    NodeServices.layer,
  );

// Fencing: opening a conversation claims its producer epoch, and the previous
// holder's next append fails. Compatibility checking must neither weaken that
// nor let a *rejected* opener fence a live, compatible writer.
describe('producer fencing across openers', () => {
  const path = AgentLog.pathFor(LogVocabulary.ConversationId.make('fenced'));
  const FENCED = LogVocabulary.ConversationId.make('fenced');

  it.effect(
    'does not fence a live producer when its append races an incompatible open',
    () =>
      Effect.gen(function* () {
        const acquireReached = yield* Deferred.make<void>();
        const allowAcquire = yield* Deferred.make<void>();
        let gateNextAcquire = false;
        const gated = wrapAcquire((store, requested, producerId, expected) =>
          Effect.gen(function* () {
            if (requested === path && gateNextAcquire) {
              gateNextAcquire = false;
              yield* Deferred.succeed(acquireReached, undefined);
              yield* Deferred.await(allowAcquire);
            }
            return yield* store.acquire(requested, producerId, expected);
          }),
        );

        const observed = yield* Effect.gen(function* () {
          const original = yield* AgentLog.open(FENCED, {
            compatibility: compatibility('1'),
          });
          gateNextAcquire = true;
          const incompatible = yield* AgentLog.open(FENCED, {
            compatibility: compatibility('2'),
          }).pipe(Effect.exit, Effect.forkChild);

          // The second opener has validated the empty stream but has not yet
          // attempted its compare-and-acquire; meanwhile the first records
          // history that makes the second incompatible.
          yield* Deferred.await(acquireReached);
          yield* original.append([started()]);
          yield* Deferred.succeed(allowAcquire, undefined);

          const exit = yield* Fiber.join(incompatible);
          yield* original.append([said('still owns the producer epoch')]);
          return { exit, records: yield* readRecords('fenced') };
        }).pipe(Effect.provide(gated));

        expect(observed.exit._tag).toBe('Failure');
        expect(observed.records.at(-1)?.record).toMatchObject({
          text: 'still owns the producer epoch',
        });
      }),
  );

  it.effect(
    'bounds compare-and-acquire retries under continuous contention',
    () =>
      Effect.gen(function* () {
        let attempts = 0;
        const contended = wrapAcquire(
          (store, requested, producerId, expected) => {
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
        );

        const outcome = yield* AgentLog.open(FENCED, {
          compatibility: compatibility('1'),
        }).pipe(Effect.exit, Effect.provide(contended));

        expect(outcome._tag).toBe('Failure');
        expect(attempts).toBe(4);
      }),
  );

  it.effect('retains normal fencing for a compatible concurrent opener', () =>
    run(
      Effect.gen(function* () {
        const original = yield* openSession('fenced');
        yield* original.append([started()]);
        const replacement = yield* openSession('fenced');
        const stale = yield* original.append([said('stale')]).pipe(Effect.exit);
        yield* replacement.append([said('replacement owns the epoch')]);

        expect(stale._tag).toBe('Failure');
        expect((yield* readRecords('fenced')).at(-1)?.record).toMatchObject({
          text: 'replacement owns the epoch',
        });
      }),
      provider([says('ok')]).layer,
    ),
  );
});
