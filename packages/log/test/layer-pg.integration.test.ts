import { Cause, Effect, Fiber, Layer, Queue, Stream } from 'effect';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { LogStorePg } from '../src/layer-pg.js';
import { LogStore } from '../src/log-store.js';
import { LogStoreContract } from '../src/log-store-contract.js';
import { LogOffset } from '../src/offset.js';
import {
  createPostgresTestHarness,
  type PostgresTestHarness,
  type ProvisionedTestDatabase,
} from './pg-test-harness.js';
import { ConversationRecord } from '../src/record.js';
import { Tail } from '../src/tail.js';

// The Postgres backend against real Postgres.
//
// The contract suite is imported unmodified — that is the point of it. What
// this file adds is the two things a memory backend cannot demonstrate: that
// the schema in `pg-test-harness.ts` is the schema the layer actually queries,
// and that a `LISTEN` connection dying reaches the consumer as a failure rather
// than as a tail that looks healthy and delivers nothing.
//
// Skipped unless a container runtime is available and opted into, because a
// suite that silently needs Docker is a suite that fails for a contributor who
// has not been told:
//
//   RUN_POSTGRES_INTEGRATION=1 pnpm test
//
// Podman works too; point `DOCKER_HOST` at its socket first.

const describeIntegration =
  process.env['RUN_POSTGRES_INTEGRATION'] === '1' ? describe : describe.skip;

describeIntegration('LogStore Postgres backend', () => {
  let harness: PostgresTestHarness;
  let database: ProvisionedTestDatabase;
  let pool: pg.Pool;
  let client: LogStorePg.Client;

  beforeAll(async () => {
    harness = await createPostgresTestHarness();
    database = await harness.provisionDatabase({ namePrefix: 'ai_log' });
    pool = database.pool;
    // Terminating a backend below surfaces on the pool as well as on the
    // checked-out client; an unhandled 'error' event on a pg pool takes the
    // process with it.
    pool.on('error', () => {});
    client = LogStorePg.fromPool(pool);
  }, 180_000);

  afterAll(async () => {
    if (database) await database.cleanup();
    if (harness) await harness.stop();
  }, 120_000);

  // The pool only exists once `beforeAll` has run, and the contract suite takes
  // a layer at registration time. Suspending each operation defers the lookup
  // to the point of use without making the layer itself lazy.
  const deferred: LogStorePg.Client = {
    query: (text, params) => Effect.suspend(() => client.query(text, params)),
    transaction: (body) => Effect.suspend(() => client.transaction(body)),
    listen: (channel) => Effect.suspend(() => client.listen(channel)),
  };

  /**
   * The same backend with the `LISTEN` session killed out from under it.
   *
   * Not a stubbed-out stream: this takes a real connection, registers a real
   * `LISTEN`, and then terminates that backend from the pool — which is the
   * production failure this case exists for (a pod's notification connection
   * dropped by a network blip, a failover, or an idle-session reaper). What is
   * being checked is that the drop arrives as a typed failure on `changes`
   * rather than as silence.
   */
  const listenThenTerminate = (channel: string) =>
    Effect.gen(function* () {
      const connection = yield* Effect.acquireRelease(
        Effect.promise(() => pool.connect()),
        (held) =>
          Effect.sync(() => {
            try {
              held.release(true);
            } catch {
              // Already destroyed by the termination below.
            }
          }),
      );

      const wakeups = yield* Queue.sliding<
        void,
        LogStorePg.SqlFailure | Cause.Done
      >(1);
      connection.on('notification', () => {
        Queue.offerUnsafe(wakeups, undefined);
      });
      connection.on('error', (error: Error) => {
        Queue.failCauseUnsafe(
          wakeups,
          Cause.fail(new LogStorePg.SqlFailure({ detail: error.message })),
        );
      });

      const pid = yield* Effect.promise(async () => {
        const result = await connection.query<{ pid: number }>(
          'SELECT pg_backend_pid() AS pid',
        );
        return result.rows[0]?.pid;
      });
      yield* Effect.promise(() => connection.query(`LISTEN "${channel}"`));
      yield* Effect.promise(() =>
        pool.query('SELECT pg_terminate_backend($1)', [pid]),
      );

      return Stream.fromQueue(wakeups);
    });

  const failingChanges = (path: string): Layer.Layer<LogStore.Service> =>
    LogStorePg.layer({
      ...deferred,
      listen: (channel) =>
        channel === LogStorePg.channelFor(path)
          ? listenThenTerminate(channel)
          : deferred.listen(channel),
    });

  LogStoreContract.logStoreContract('postgres', {
    layer: LogStorePg.layer(deferred),
    layerWithFailingChanges: failingChanges,
  });

  // What the contract cannot ask for, because it is written against an
  // interface and these are properties of this backend's storage.
  describe('beyond the contract', () => {
    // The headline reason this backend exists. An in-process change feed is
    // process-local: one replica does not wake on another replica's append,
    // and such designs get away with it by re-reading on a timer. Two
    // independent pools over the same database are two replicas as far as
    // Postgres is concerned — nothing in-process is shared between the reader
    // and the writer here.
    it('wakes a tail on another connection pool entirely', async () => {
      const readerPool = new pg.Pool({
        connectionString: database.connectionString,
      });
      const writerPool = new pg.Pool({
        connectionString: database.connectionString,
      });
      readerPool.on('error', () => {});
      writerPool.on('error', () => {});

      try {
        const reader = LogStorePg.layer(LogStorePg.fromPool(readerPool));
        const writer = LogStorePg.layer(LogStorePg.fromPool(writerPool));
        const path = 'cross-pod';

        const written = await Effect.runPromise(
          Effect.gen(function* () {
            const store = yield* LogStore.Service;
            yield* store.create(path, 'identity');
            const claim = yield* store.acquire(path, 'writer-pod');
            return claim;
          }).pipe(Effect.provide(writer)),
        );

        const collected = await Effect.runPromise(
          Effect.gen(function* () {
            const fiber = yield* Tail.from(path, LogOffset.START).pipe(
              Stream.take(1),
              Stream.runCollect,
              Effect.forkChild,
            );

            // The stream is empty, so the tail catches up to nothing and is
            // parked on a wake-up before the append below happens. Reaching
            // the record therefore requires a notification crossing from the
            // writer pool's connection to the reader pool's LISTEN session —
            // there is no in-process path between them.
            yield* Effect.sleep('250 millis');

            yield* Effect.gen(function* () {
              const store = yield* LogStore.Service;
              yield* store.append({
                path,
                producerId: written.producerId,
                epoch: written.epoch,
                sequence: 0,
                records: [
                  {
                    conversationId: 'conversation-1',
                    timestamp: 1_700_000_000_000,
                    record: { _tag: 'Text', step: 1, text: 'from another pod' },
                  } satisfies ConversationRecord.Entry,
                ],
              });
            }).pipe(Effect.provide(writer));

            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(reader)),
        );

        expect(collected.length).toBe(1);
        expect(collected[0]?.record).toMatchObject({
          text: 'from another pod',
        });
      } finally {
        await readerPool.end();
        await writerPool.end();
      }
    }, 30_000);

    // The trap the explicit `COLLATE "C"` in every offset comparison exists
    // for. Under a glibc locale, punctuation is ignored at the primary
    // comparison level and `'-1'` — read-from-the-beginning — sorts *after*
    // every real offset, so an unpinned read returns an empty log to every
    // fresh reader. Delete the COLLATE and this fires.
    it('needs the pinned C collation for read-from-the-beginning', async () => {
      const zero = LogOffset.fromSeq(0n);
      const database_ = await pool.query<{ datcollate: string }>(
        'SELECT datcollate FROM pg_database WHERE datname = current_database()',
      );
      const collation = database_.rows[0]?.datcollate ?? 'unknown';

      const compared = await pool.query<{ db: boolean; c: boolean }>(
        `SELECT $1 < $2 AS db, ($1 COLLATE "C") < ($2 COLLATE "C") AS c`,
        [LogOffset.START, zero],
      );

      // What the offset format promises, and what JavaScript's `<` does.
      expect(compared.rows[0]?.c).toBe(true);
      if (collation !== 'C' && collation !== 'POSIX') {
        expect(compared.rows[0]?.db).toBe(false);
      }
    });

    // The idempotency mechanism is the unique index, not the code above it.
    // Bypass the layer entirely and re-insert a batch slot the database has
    // already seen.
    it('refuses a duplicated batch slot at the index, not the application', async () => {
      const path = 'index-guard';
      await Effect.runPromise(
        Effect.gen(function* () {
          const store = yield* LogStore.Service;
          yield* store.create(path, 'identity');
          const claim = yield* store.acquire(path, 'producer-1');
          yield* store.append({
            path,
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: 0,
            records: [
              {
                conversationId: 'conversation-1',
                timestamp: 1_700_000_000_000,
                record: { _tag: 'Text', step: 1, text: 'one' },
              } satisfies ConversationRecord.Entry,
            ],
          });
        }).pipe(Effect.provide(LogStorePg.layer(deferred))),
      );

      // Same producer triple and batch position, a different record sequence:
      // application-level state would happily accept this.
      const duplicate = pool.query(
        `INSERT INTO ai_log.records
           (path, seq, record_offset, producer_id, producer_epoch,
            producer_sequence, batch_index, conversation_id,
            record_timestamp, record)
         SELECT path, seq + 100, record_offset, producer_id, producer_epoch,
                producer_sequence, batch_index, conversation_id,
                record_timestamp, record
         FROM ai_log.records WHERE path = $1`,
        [path],
      );

      await expect(duplicate).rejects.toMatchObject({ code: '23505' });
    });
  });
});
