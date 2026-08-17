import { PgClient } from '@effect/sql-pg';
import { afterAll, beforeAll, describe, expect, it } from '@effect/vitest';
import {
  Deferred,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Redacted,
  Stream,
} from 'effect';

import { channelFor, LogStorePg } from '../src/layer-pg.js';
import { LogStore } from '../src/log-store.js';
import { LogStoreContract } from '../src/log-store-contract.js';
import { LogOffset } from '../src/offset.js';
import { VesperPgClient } from '../src/pg-client.js';
import { ConversationRecord } from '../src/record.js';
import { Tail } from '../src/tail.js';
import { LogVocabulary } from '../src/vocabulary.js';
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
    if (database) await database.cleanup();
    if (harness) await harness.stop();
  }, 120_000);

  const pgLayer = Layer.unwrap(
    Effect.sync(() =>
      VesperPgClient.layer({ url: Redacted.make(database.connectionString) }),
    ),
  );
  const storeLayer = LogStorePg.layer().pipe(Layer.provide(pgLayer));

  LogStoreContract.logStoreContract('postgres', { layer: storeLayer });

  describe('corrected official PgClient behavior', () => {
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
          ),
        );
        const writer = ManagedRuntime.make(
          LogStorePg.layer().pipe(
            Layer.provide(
              VesperPgClient.layer({
                url: Redacted.make(database.connectionString),
              }),
            ),
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
                const fiber = yield* sql.listen('vesper_listener_failure').pipe(
                  Stream.tap(() => Effect.void),
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
                yield* Effect.promise(() =>
                  awaitTerminate(database, rows[0]!.pid),
                );
                return yield* Fiber.join(fiber);
              }),
            ),
          ),
        ).pipe(
          Effect.tap((outcome) =>
            Effect.sync(() => expect(outcome._tag).toBe('Failure')),
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
                const listener = yield* sql
                  .listen('vesper_payloads')
                  .pipe(Stream.take(3), Stream.runCollect, Effect.forkChild);

                // The first element is emitted only after LISTEN is established.
                yield* Effect.promise(() =>
                  waitForListener(database, 'vesper_payloads'),
                );
                yield* sql.notify('vesper_payloads', '');
                yield* sql.notify('vesper_payloads', 'value');

                expect(Array.from(yield* Fiber.join(listener))).toEqual([
                  '',
                  '',
                  'value',
                ]);
              }),
            ),
          ),
        ),
      { timeout: 30_000 },
    );

    it.effect(
      'unlistens on scope close and reconnects a later subscription',
      () =>
        Effect.promise(() =>
          database.runtime.runPromise(
            Effect.gen(function* () {
              const sql = yield* PgClient.PgClient;
              yield* Effect.scoped(
                sql.listen('vesper_reconnect').pipe(Stream.runHead),
              );

              const listener = yield* sql
                .listen('vesper_reconnect')
                .pipe(Stream.take(2), Stream.runCollect, Effect.forkChild);
              yield* Effect.promise(() =>
                waitForListener(database, 'vesper_reconnect'),
              );
              yield* sql.notify('vesper_reconnect', 'again');
              expect(Array.from(yield* Fiber.join(listener))).toEqual([
                '',
                'again',
              ]);
            }),
          ),
        ),
      { timeout: 30_000 },
    );

    it.effect('rolls back an interrupted transaction', () => {
      const path = 'interrupted-transaction';
      return Effect.promise(() =>
        database.runtime.runPromise(
          Effect.gen(function* () {
            const sql = yield* PgClient.PgClient;
            const started = yield* Deferred.make<void>();
            const fiber = yield* sql
              .withTransaction(
                Effect.gen(function* () {
                  yield* sql.unsafe(
                    'INSERT INTO ai_log.streams (path, identity) VALUES ($1, $2)',
                    [path, 'identity'],
                  );
                  yield* Deferred.succeed(started, undefined);
                  return yield* Effect.never;
                }),
              )
              .pipe(Effect.forkChild);
            yield* Deferred.await(started);
            yield* Fiber.interrupt(fiber);
            const rows = yield* sql.unsafe<{ count: string }>(
              'SELECT count(*)::text AS count FROM ai_log.streams WHERE path = $1',
              [path],
            );
            expect(rows[0]?.count).toBe('0');
          }),
        ),
      );
    });

    it.effect('isolates SET LOCAL statement_timeout to its transaction', () =>
      Effect.promise(() =>
        database.runtime.runPromise(
          Effect.gen(function* () {
            const sql = yield* PgClient.PgClient;
            yield* sql.withTransaction(
              sql.unsafe("SET LOCAL statement_timeout = '25ms'"),
            );
            const rows = yield* sql.unsafe<{ statement_timeout: string }>(
              'SHOW statement_timeout',
            );
            expect(rows[0]?.statement_timeout).toBe('0');
          }),
        ),
      ),
    );

    it.effect('delivers transactional notifications only after commit', () =>
      Effect.promise(() =>
        database.runtime.runPromise(
          Effect.scoped(
            Effect.gen(function* () {
              const sql = yield* PgClient.PgClient;
              const received = yield* Deferred.make<string>();
              const listener = yield* sql
                .listen('vesper_transactional_notify')
                .pipe(
                  Stream.drop(1),
                  Stream.runHead,
                  Effect.flatMap((value) =>
                    value._tag === 'Some'
                      ? Deferred.succeed(received, value.value)
                      : Effect.void,
                  ),
                  Effect.forkChild,
                );
              yield* Effect.promise(() =>
                waitForListener(database, 'vesper_transactional_notify'),
              );
              yield* sql.withTransaction(
                Effect.gen(function* () {
                  yield* sql.notify('vesper_transactional_notify', 'committed');
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
    it('uses C collation for read-from-the-beginning', async () => {
      const zero = LogOffset.fromSeq(0n);
      const compared = await database.adminPool.query<{
        db: boolean;
        c: boolean;
      }>(`SELECT $1 < $2 AS db, ($1 COLLATE "C") < ($2 COLLATE "C") AS c`, [
        LogOffset.START,
        zero,
      ]);
      expect(compared.rows[0]?.c).toBe(true);
    });

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
});

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
    if (result.rows[0]?.count !== '0') return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Listener did not become ready for ${channel}`);
};
