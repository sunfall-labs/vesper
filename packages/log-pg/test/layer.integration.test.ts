import { PgClient } from '@effect/sql-pg';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, beforeAll, describe, expect, it } from '@effect/vitest';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Tail } from '@sunfall/vesper-log/tail';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Deferred,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Queue,
  Redacted,
  type Result,
  Stream,
} from 'effect';

import { SqlClient } from 'effect/sql';

import { VesperPgClient } from '../src/client.js';
import { channelFor, LogStorePg } from '../src/layer.js';
import { chainTamperContract } from '../../log/test/chain-tamper-contract.js';
import { logStoreContract } from '../../log/test/log-store-contract.js';
import {
  createPostgresTestHarness,
  type PostgresTestHarness,
  type ProvisionedTestDatabase,
} from './pg-test-harness.js';

const describeIntegration =
  process.env['RUN_POSTGRES_INTEGRATION'] === '1' ? describe : describe.skip;

describeIntegration('LogStore Postgres backend', () => {
  let harness: PostgresTestHarness;
  let database: ProvisionedTestDatabase;

  beforeAll(async () => {
    harness = await createPostgresTestHarness();
    database = await harness.provisionDatabase({ namePrefix: 'ai_log' });
  }, 180_000);

  afterAll(async () => {
    if (database) {
      await database.cleanup();
    }
    if (harness) {
      await harness.stop();
    }
  }, 120_000);

  const pgLayer = Layer.unwrap(
    Effect.sync(() =>
      VesperPgClient.layer({ url: Redacted.make(database.connectionString) }),
    ),
  );
  const storeLayer = LogStorePg.layer().pipe(
    Layer.provide(pgLayer),
    Layer.provide(NodeServices.layer),
  );

  logStoreContract('postgres', { layer: storeLayer });

  describe('official PgClient behavior', () => {
    it(
      'delivers notifications across independent client instances',
      { timeout: 30_000 },
      async () => {
        const reader = ManagedRuntime.make(
          LogStorePg.layer().pipe(
            Layer.provide(
              VesperPgClient.layer({
                url: Redacted.make(database.connectionString),
              }),
            ),
            Layer.provide(NodeServices.layer),
          ),
        );
        const writer = ManagedRuntime.make(
          LogStorePg.layer().pipe(
            Layer.provide(
              VesperPgClient.layer({
                url: Redacted.make(database.connectionString),
              }),
            ),
            Layer.provide(NodeServices.layer),
          ),
        );

        try {
          const claim = await writer.runPromise(
            Effect.gen(function* () {
              const store = yield* LogStore.Service;
              yield* store.create('cross-instance', 'identity');
              return yield* store.acquire(
                'cross-instance',
                LogVocabulary.ProducerId.make('writer'),
              );
            }),
          );
          const collected = await reader.runPromise(
            Effect.gen(function* () {
              const fiber = yield* Tail.from(
                'cross-instance',
                LogOffset.START,
              ).pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);
              yield* Effect.promise(() =>
                waitForListener(database, channelFor('cross-instance')),
              );
              yield* Effect.promise(() =>
                writer.runPromise(
                  Effect.flatMap(LogStore.Service, (store) =>
                    store.append({
                      path: 'cross-instance',
                      producerId: claim.producerId,
                      epoch: claim.epoch,
                      sequence: LogVocabulary.ProducerSequence.make(0),
                      records: [text('from another instance')],
                    }),
                  ),
                ),
              );
              return yield* Fiber.join(fiber);
            }),
          );
          expect(collected[0]?.record).toMatchObject({
            text: 'from another instance',
          });
        } finally {
          await reader.dispose();
          await writer.dispose();
        }
      },
    );

    it.effect(
      'surfaces listener backend termination as a stream failure',
      () =>
        Effect.promise(() =>
          database.runtime.runPromise(
            Effect.scoped(
              Effect.gen(function* () {
                const sql = yield* PgClient.PgClient;
                const notifications = yield* sql.listen(
                  'vesper_listener_failure',
                );
                const fiber = yield* Stream.fromQueue(notifications).pipe(
                  Stream.runDrain,
                  Effect.result,
                  Effect.forkChild,
                );
                yield* Effect.promise(() =>
                  waitForListener(database, 'vesper_listener_failure'),
                );
                const rows = yield* sql.unsafe<{ pid: number }>(
                  `SELECT pid FROM pg_stat_activity
               WHERE datname = current_database()
                 AND query = 'LISTEN "vesper_listener_failure"'
               ORDER BY backend_start DESC LIMIT 1`,
                );
                const listener = rows[0];
                if (listener === undefined) {
                  throw new Error('listener backend row missing');
                }
                yield* Effect.promise(() =>
                  awaitTerminate(database, listener.pid),
                );
                return yield* Fiber.join(fiber);
              }),
            ),
          ),
        ).pipe(
          Effect.tap((outcome) =>
            Effect.sync(() => {
              expect(outcome._tag).toBe('Failure');
            }),
          ),
        ),
      { timeout: 30_000 },
    );

    it.effect(
      'delivers empty and non-empty payloads without an opening race',
      () =>
        Effect.promise(() =>
          database.runtime.runPromise(
            Effect.scoped(
              Effect.gen(function* () {
                const sql = yield* PgClient.PgClient;
                // `listen` resolves only after LISTEN is established, so a
                // notify issued straight after it cannot be missed.
                const notifications = yield* sql.listen('vesper_payloads');
                yield* sql.notify('vesper_payloads', '');
                yield* sql.notify('vesper_payloads', 'value');

                const received = yield* Queue.takeN(notifications, 2);
                expect(received.map(({ payload }) => payload)).toEqual([
                  '',
                  'value',
                ]);
              }),
            ),
          ),
        ),
      { timeout: 30_000 },
    );

    it.effect('delivers transactional notifications only after commit', () =>
      Effect.promise(() =>
        database.runtime.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const sql = yield* PgClient.PgClient;
              const received = yield* Deferred.make<string>();
              const notifications = yield* sql.listen(
                'vesper_transactional_notify',
              );
              const listener = yield* Queue.take(notifications).pipe(
                Effect.flatMap(({ payload }) =>
                  Deferred.succeed(received, payload),
                ),
                Effect.forkChild,
              );
              yield* sql.withTransaction(
                Effect.gen(function* () {
                  // `sql.notify` borrows its own pool connection and so
                  // ignores the ambient transaction; appends issue
                  // `pg_notify` inside theirs, which is what this pins.
                  yield* sql.unsafe('SELECT pg_notify($1, $2)', [
                    'vesper_transactional_notify',
                    'committed',
                  ]);
                  expect(yield* Deferred.isDone(received)).toBe(false);
                }),
              );
              expect(yield* Deferred.await(received)).toBe('committed');
              yield* Fiber.join(listener);
            }),
          ),
        ),
      ),
    );
  });

  describe('storage invariants', () => {
    it.effect(
      'atomically appends and idempotently retries 10,000 records',
      () => {
        const records = Array.from({ length: 10_000 }, (_, index) =>
          text(`record-${index}`),
        );
        return Effect.gen(function* () {
          const store = yield* LogStore.Service;
          yield* store.create('large-append', 'identity');
          const claim = yield* store.acquire(
            'large-append',
            LogVocabulary.ProducerId.make('producer'),
          );
          const input = {
            path: 'large-append',
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: LogVocabulary.ProducerSequence.make(0),
            records,
          };
          const first = yield* store.append(input);
          const retry = yield* store.append(input);
          return {
            first,
            retry,
            page: yield* store.read('large-append', { limit: 10_000 }),
          };
        }).pipe(
          Effect.provide(storeLayer),
          Effect.tap((result) =>
            Effect.sync(() => {
              expect(result.first).toBe(LogOffset.fromSeq(9_999n));
              expect(result.retry).toBe(result.first);
              expect(result.page.records).toHaveLength(10_000);
            }),
          ),
        );
      },
      { timeout: 120_000 },
    );
  });
  chainTamperContract('postgres', {
    layer: Layer.merge(storeLayer, pgLayer),
    editText: (path, seq) =>
      Effect.flatMap(SqlClient.SqlClient, (sql) =>
        sql.unsafe(
          `UPDATE ai_log.records SET record = jsonb_set(record, '{text}', '"x"')
           WHERE path = $1 AND seq = $2`,
          [path, seq],
        ),
      ).pipe(Effect.asVoid),
    deleteRecord: (path, seq) =>
      Effect.flatMap(SqlClient.SqlClient, (sql) =>
        sql.unsafe('DELETE FROM ai_log.records WHERE path = $1 AND seq = $2', [
          path,
          seq,
        ]),
      ).pipe(Effect.asVoid),
  });

  // Real two-connection fencing races: each writer owns its own pool, and a
  // shared gate releases both at once so they contend for the stream row's
  // `SELECT ... FOR UPDATE` lock rather than running one after the other.
  describe('contended fencing', () => {
    const connectionLayer = () =>
      LogStorePg.layer().pipe(
        Layer.provide(
          Layer.unwrap(
            Effect.sync(() =>
              VesperPgClient.layer({
                url: Redacted.make(database.connectionString),
              }),
            ),
          ),
        ),
        Layer.provide(NodeServices.layer),
      );
    const writerA: Writer = ManagedRuntime.make(connectionLayer());
    const writerB: Writer = ManagedRuntime.make(connectionLayer());

    afterAll(() =>
      Effect.runPromise(
        Effect.all([
          Effect.promise(() => writerA.dispose()),
          Effect.promise(() => writerB.dispose()),
        ]),
      ),
    );

    const race = <A>(effectA: Op<A>, effectB: Op<A>) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const gated = (writer: Writer, effect: Op<A>) =>
          Effect.promise(() =>
            writer.runPromise(
              Deferred.await(gate).pipe(Effect.andThen(Effect.result(effect))),
            ),
          ).pipe(Effect.forkChild);
        const fiberA = yield* gated(writerA, effectA);
        const fiberB = yield* gated(writerB, effectB);
        yield* Deferred.succeed(gate, undefined);
        return {
          resultA: yield* Fiber.join(fiberA),
          resultB: yield* Fiber.join(fiberB),
        };
      });

    const acquire = (path: string, id: string) =>
      store((log) => log.acquire(path, LogVocabulary.ProducerId.make(id)));
    const appendTo = (
      path: string,
      claim: LogStore.ProducerClaim,
      sequence: number,
      ...values: ReadonlyArray<string>
    ) =>
      store((log) =>
        log.append({
          path,
          producerId: claim.producerId,
          epoch: claim.epoch,
          sequence: LogVocabulary.ProducerSequence.make(sequence),
          records: values.map(text),
        }),
      );
    const texts = (path: string) =>
      on(
        writerA,
        store((log) => log.read(path)),
      ).pipe(Effect.map((page) => page.records.map(recordText)));
    const create = (path: string) =>
      on(
        writerA,
        store((log) => log.create(path, 'identity')),
      );

    it.live(
      'fences the loser when two live connections race acquire, then append',
      () =>
        Effect.gen(function* () {
          for (let iteration = 0; iteration < 10; iteration += 1) {
            const path = `race-acquire-${String(iteration)}`;
            yield* create(path);
            const acquired = yield* race(
              acquire(path, 'a'),
              acquire(path, 'b'),
            );
            const claimA = yield* success(acquired.resultA);
            const claimB = yield* success(acquired.resultB);
            // The row lock must serialize the two epoch bumps.
            expect(claimA.epoch).not.toBe(claimB.epoch);
            const aWins = claimA.epoch > claimB.epoch;

            const appended = yield* race(
              appendTo(path, claimA, 0, aWins ? 'winner' : 'loser'),
              appendTo(path, claimB, 0, aWins ? 'loser' : 'winner'),
            );
            expect(aWins ? appended.resultA : appended.resultB).toMatchObject({
              _tag: 'Success',
            });
            expect(aWins ? appended.resultB : appended.resultA).toMatchObject({
              _tag: 'Failure',
              failure: { operation: 'append', reason: 'fenced' },
            });
            expect(yield* texts(path)).toEqual(['winner']);
          }
        }),
      { timeout: 120_000 },
    );

    it.live(
      'fails the fenced writer under lock contention while the new epoch appends',
      () =>
        Effect.gen(function* () {
          for (let iteration = 0; iteration < 5; iteration += 1) {
            const path = `race-interleave-${String(iteration)}`;
            yield* create(path);
            const claimA = yield* on(writerA, acquire(path, 'a'));
            yield* on(writerA, appendTo(path, claimA, 0, 'batch-1'));
            const claimB = yield* on(writerB, acquire(path, 'b'));
            expect(claimB.epoch).toBeGreaterThan(claimA.epoch);

            const raced = yield* race(
              appendTo(path, claimA, 1, 'from-fenced-a'),
              appendTo(path, claimB, 0, 'from-b'),
            );
            expect(raced.resultA).toMatchObject({
              _tag: 'Failure',
              failure: { operation: 'append', reason: 'fenced' },
            });
            expect(raced.resultB._tag).toBe('Success');
            expect(yield* texts(path)).toEqual(['batch-1', 'from-b']);
          }
        }),
      { timeout: 60_000 },
    );

    it.live(
      'converges two connections racing an identical batch to a single write',
      () =>
        Effect.gen(function* () {
          for (let iteration = 0; iteration < 10; iteration += 1) {
            const path = `race-idempotent-${String(iteration)}`;
            yield* create(path);
            const claim = yield* on(writerA, acquire(path, 'p'));
            const raced = yield* race(
              appendTo(path, claim, 0, 'same', 'same-2'),
              appendTo(path, claim, 0, 'same', 'same-2'),
            );
            expect(raced.resultA._tag).toBe('Success');
            expect(raced.resultB._tag).toBe('Success');
            if (
              raced.resultA._tag === 'Success' &&
              raced.resultB._tag === 'Success'
            ) {
              expect(raced.resultA.success).toBe(raced.resultB.success);
            }
            expect(yield* texts(path)).toEqual(['same', 'same-2']);
          }
        }),
      { timeout: 60_000 },
    );
  });
});

type Op<A> = Effect.Effect<A, LogStore.LogStoreError, LogStore.Service>;
type Writer = ManagedRuntime.ManagedRuntime<LogStore.Service, unknown>;

const store = <A>(
  use: (log: LogStore.Interface) => Effect.Effect<A, LogStore.LogStoreError>,
): Op<A> => Effect.flatMap(LogStore.Service, use);
const on = <A>(writer: Writer, effect: Op<A>) =>
  Effect.promise(() => writer.runPromise(effect));
const recordText = (envelope: ConversationRecord.Envelope) =>
  envelope.record._tag === 'Text' ? envelope.record.text : envelope.record._tag;
const success = <A>(result: Result.Result<A, unknown>) =>
  result._tag === 'Success'
    ? Effect.succeed(result.success)
    : Effect.die('an uncontested operation failed');

const text = (value: string): ConversationRecord.Entry => ({
  conversationId: LogVocabulary.ConversationId.make('conversation-1'),
  timestamp: 1_700_000_000_000,
  record: { _tag: 'Text', step: 1, text: value },
});

const awaitTerminate = async (
  database: ProvisionedTestDatabase,
  pid: number,
) => {
  await database.adminPool.query('SELECT pg_terminate_backend($1)', [pid]);
};

const waitForListener = async (
  database: ProvisionedTestDatabase,
  channel: string,
) => {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const result = await database.adminPool.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM pg_stat_activity
       WHERE datname = current_database() AND query = $1`,
      [`LISTEN "${channel}"`],
    );
    if (result.rows[0]?.count !== '0') {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Listener did not become ready for ${channel}`);
};
