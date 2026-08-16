import {
  Cause,
  Data,
  Effect,
  Layer,
  Option,
  Queue,
  Scope,
  Stream,
} from 'effect';

import { LogStore } from './log-store.js';
import { LogOffset } from './offset.js';
import { ConversationRecord } from './record.js';

// Postgres log store.
//
// The durable backend. Everything the memory backend keeps in a
// `MutableHashMap` lives in two tables here, and the two things that have no
// memory analogue — batch atomicity across a crash, and wake-ups that reach a
// pod that did not write — are the reason this file exists.
//
// ## No driver, no connection lifecycle
//
// This module imports nothing but `effect`. A caller supplies a {@link Client}
// — query, transaction, listen — and keeps its own pool. The usual
// `PersistenceAdapter` lifecycle (`connect`/`migrate`/`close`) is `Layer`
// reinvented for a framework that does not have one, and this package has
// `Layer`. {@link fromPool} adapts anything shaped
// like a `pg.Pool`, structurally, so `pg` stays out of this package's
// dependency list and out of the family's layering rule.
//
// Schema and migrations are the application's, not this module's. Nothing
// here creates a table or stamps a version: a backend that migrates itself is
// a second migration system, and it disagrees with the first one exactly once,
// in production. The DDL this layer expects is spelled out in
// `test/pg-test-harness.ts`, which is also what the integration suite applies,
// so the reference cannot drift from the thing under test.
//
// ## Where the invariants actually live
//
// Two mechanisms, and it is worth being precise about which does what.
//
// `SELECT ... FOR UPDATE` on the stream row is what makes an append a
// read-check-write that no concurrent append can interleave with. Fencing
// state — epoch, holder, next producer sequence, last fingerprint, last
// offset — is columns on that row, mirroring the memory backend field for
// field so the two cannot drift.
//
// The `UNIQUE (path, producer_id, producer_epoch, producer_sequence,
// batch_index)` index on the records table is the durable half: a given
// producer's batch slot can be filled exactly once, enforced by the database
// rather than by the code above it. Note the fifth column. The natural index
// for this is four columns, which is right for a log that writes one row per
// *batch*; these offsets are per record, so one batch is many rows sharing a
// producer sequence and a four-column index would reject its own second row.
// `batch_index` is the minimal correction that keeps the property — insert
// this batch twice and the second insert raises 23505 and takes the whole
// transaction with it.
//
// ## Ordering and collation
//
// Offsets are fixed-width zero-padded decimals precisely so ordering is byte
// comparison everywhere. "Everywhere" does not include Postgres by default:
// under a glibc locale like `en_US.UTF-8`, punctuation is ignored at the
// primary comparison level, and `'-1'` — {@link LogOffset.START} — collates
// *after* `'0000…'` rather than before it, so a read from the beginning would
// return nothing. Every comparison and every ordering on `record_offset` here
// therefore pins `COLLATE "C"` explicitly, which is byte order and is exactly
// what JavaScript's `<` does. Do not remove those; the failure they prevent is
// an empty log that looks like an empty conversation.

/** A row as the driver hands it back. */
export type Row = Readonly<Record<string, unknown>>;

/**
 * Whatever the driver failed with, flattened.
 *
 * Not a `Schema` error: nothing on this path crosses a checkpoint, and the
 * driver's own error object is not something this package can model.
 */
export class SqlFailure extends Data.TaggedError(
  '@sunfall/vesper-log/SqlFailure',
)<{
  readonly detail: string;
  /** SQLSTATE, when the driver reports one. */
  readonly code?: string | undefined;
}> {}

/** Somewhere statements can run: the pool, or one transaction's connection. */
export interface Sql {
  readonly query: (
    text: string,
    params?: ReadonlyArray<unknown>,
  ) => Effect.Effect<ReadonlyArray<Row>, SqlFailure>;
}

/**
 * The whole Postgres surface this backend needs.
 *
 * Three operations, no lifecycle. `Layer` acquires and releases; a caller
 * hands over a pool it already has and keeps owning it.
 */
export interface Client extends Sql {
  /**
   * Run `body` inside one transaction on one connection.
   *
   * A failure or an interruption must roll back. This is the entire basis of
   * append atomicity — the contract requires that a rejected batch leave the
   * log exactly as it was, and nothing above this line re-checks it.
   */
  readonly transaction: <A, E, R>(
    body: (tx: Sql) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlFailure, R>;

  /**
   * Take a dedicated session and `LISTEN` on `channel`.
   *
   * **The returned effect must not complete until the `LISTEN` has taken
   * effect.** That completion is the only evidence a subscription exists, and
   * {@link Interface.changes}'s opening wake-up is emitted on the strength of
   * it. An implementation that returns early re-opens precisely the race the
   * opening tick exists to close, and the symptom is a tail that hangs rather
   * than a test that fails.
   *
   * The stream emits one element per notification and **fails** if the session
   * drops. A change feed that goes quiet is indistinguishable from a
   * conversation where nothing is happening; that is why `changes` has an
   * error channel at all.
   */
  readonly listen: (
    channel: string,
  ) => Effect.Effect<Stream.Stream<void, SqlFailure>, SqlFailure, Scope.Scope>;
}

export interface Options {
  /**
   * Postgres schema holding `streams` and `records`. Defaults to `ai_log`,
   * which is what the reference DDL in `test/pg-test-harness.ts` creates.
   */
  readonly schema?: string;
}

type Operation = LogStore.LogStoreError['operation'];
type Reason = LogStore.LogStoreError['reason'];

const DEFAULT_SCHEMA = 'ai_log';
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

/**
 * Wake-up channel for a path.
 *
 * `LISTEN` takes an identifier, not a value, so the path cannot be the channel
 * — paths are arbitrary strings and identifiers truncate at 63 bytes. A 64-bit
 * FNV-1a of the path is, and two paths that collide wake each other. That is
 * allowed and cheap: the interface permits spurious wake-ups, and a spurious
 * one costs one empty read. Deriving the channel from a hash rather than
 * sharing one channel for the whole store is what keeps a busy conversation
 * from waking every reader in the fleet.
 */
const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const SIXTY_FOUR_BITS = 0xffffffffffffffffn;

export const channelFor = (path: string): string => {
  let hash = FNV_OFFSET_BASIS;
  for (let index = 0; index < path.length; index += 1) {
    hash =
      ((hash ^ BigInt(path.charCodeAt(index))) * FNV_PRIME) & SIXTY_FOUR_BITS;
  }
  return `ai_log_${hash.toString(16).padStart(16, '0')}`;
};

/**
 * `NOTIFY` payloads are capped at 8000 bytes and the payload is never read —
 * the wake-up carries nothing by design. It is here so `pg_notify` calls are
 * legible in `pg_stat_activity` when someone is working out why a tail woke.
 */
const NOTIFY_PAYLOAD_LIMIT = 200;

// Drivers disagree about how they hand back `bigint`: node-postgres returns a
// string, others a number or a `bigint`. Coercing here rather than trusting one
// of them is the difference between a driver swap being a layer change and a
// driver swap being a silent decode failure.
const asNumber = (value: unknown): number =>
  typeof value === 'number' ? value : Number(value as string);

const asBigInt = (value: unknown): bigint =>
  typeof value === 'bigint' ? value : BigInt(value as string);

const asString = (value: unknown): string =>
  typeof value === 'string' ? value : String(value);

interface StreamRow {
  readonly identity: string;
  readonly epoch: number;
  readonly producerId: string | undefined;
  readonly nextSequence: bigint;
  readonly nextProducerSequence: number;
  readonly lastFingerprint: string;
  readonly lastOffset: LogOffset.Offset;
}

const readStreamRow = (row: Row): StreamRow => ({
  identity: asString(row['identity']),
  epoch: asNumber(row['epoch']),
  producerId:
    row['producer_id'] === null || row['producer_id'] === undefined
      ? undefined
      : asString(row['producer_id']),
  nextSequence: asBigInt(row['next_sequence']),
  nextProducerSequence: asNumber(row['next_producer_sequence']),
  lastFingerprint: asString(row['last_fingerprint']),
  lastOffset: LogOffset.Offset.make(asString(row['last_offset'])),
});

const metaOf = (path: string, row: StreamRow): LogStore.StreamMeta => ({
  path,
  identity: row.identity,
  epoch: row.epoch,
  producerId:
    row.producerId === undefined ? Option.none() : Option.some(row.producerId),
  head: row.lastOffset,
  // Record sequences start at zero and advance by one per record, so the next
  // one to assign is also how many exist. No counter to keep in step.
  records: Number(row.nextSequence),
});

export const make = (client: Client, options?: Options): LogStore.Interface => {
  const schema = options?.schema ?? DEFAULT_SCHEMA;
  if (!IDENTIFIER.test(schema)) {
    // A defect, not a failure: the schema name is wiring, not input, and it is
    // interpolated into SQL because Postgres has no parameter for an
    // identifier.
    throw new Error(`Not a usable Postgres schema name: ${schema}`);
  }
  const streams = `${schema}.streams`;
  const records = `${schema}.records`;

  const failure = (
    path: string,
    operation: Operation,
    reason: Reason,
    detail: string,
  ): LogStore.LogStoreError =>
    new LogStore.LogStoreError({ path, operation, reason, detail });

  /** Driver failures become `storage`; our own failures pass through. */
  const asLogStoreError =
    (path: string, operation: Operation) =>
    (error: SqlFailure | LogStore.LogStoreError): LogStore.LogStoreError =>
      error instanceof LogStore.LogStoreError
        ? error
        : failure(
            path,
            operation,
            'storage',
            error.code === undefined
              ? error.detail
              : `${error.detail} (SQLSTATE ${error.code})`,
          );

  const create = Effect.fn('AiLog.LogStorePg.create')(function* (
    path: string,
    identity: string,
  ) {
    const rows = yield* client
      .query(
        `INSERT INTO ${streams} (path, identity)
         VALUES ($1, $2)
         ON CONFLICT (path) DO NOTHING
         RETURNING identity, epoch, producer_id, next_sequence,
                   next_producer_sequence, last_fingerprint, last_offset`,
        [path, identity],
      )
      .pipe(Effect.mapError(asLogStoreError(path, 'create')));

    const row = rows[0];
    if (row === undefined) {
      return yield* Effect.fail(
        failure(path, 'create', 'conflict', 'a stream already exists here'),
      );
    }
    return metaOf(path, readStreamRow(row));
  });

  const acquire = Effect.fn('AiLog.LogStorePg.acquire')(function* (
    path: string,
    producerId: string,
  ) {
    const rows = yield* client
      .query(
        `UPDATE ${streams}
         SET epoch = epoch + 1,
             producer_id = $2,
             next_producer_sequence = 0,
             last_fingerprint = ''
         WHERE path = $1
         RETURNING epoch`,
        [path, producerId],
      )
      .pipe(Effect.mapError(asLogStoreError(path, 'acquire')));

    const row = rows[0];
    if (row === undefined) {
      return yield* Effect.fail(
        failure(path, 'acquire', 'not_found', 'no stream at this path'),
      );
    }

    return {
      path,
      producerId,
      epoch: asNumber(row['epoch']),
      nextSequence: 0,
    } satisfies LogStore.ProducerClaim;
  });

  const append = Effect.fn('AiLog.LogStorePg.append')(function* (
    input: LogStore.AppendInput,
  ) {
    const reject = (reason: Reason, detail: string) =>
      Effect.fail(failure(input.path, 'append', reason, detail));

    // Cheap rejections before a connection is taken. Nothing below can write
    // for these, so there is no reason to open a transaction to find out.
    if (input.records.length === 0) {
      return yield* reject('empty', 'append carried no records');
    }
    if (!Number.isInteger(input.sequence) || input.sequence < 0) {
      return yield* reject(
        'conflict',
        `sequence ${input.sequence} is not a non-negative integer`,
      );
    }

    return yield* client
      .transaction((tx) =>
        Effect.gen(function* () {
          // The row lock. Every check below reads state that the write at the
          // bottom then advances, so the two have to be one critical section
          // per stream — which producer fencing already implies, since a
          // stream has one writer at a time by construction.
          const locked = yield* tx.query(
            `SELECT identity, epoch, producer_id, next_sequence,
                    next_producer_sequence, last_fingerprint, last_offset
             FROM ${streams}
             WHERE path = $1
             FOR UPDATE`,
            [input.path],
          );

          const found = locked[0];
          if (found === undefined) {
            return yield* reject('not_found', 'no stream at this path');
          }
          const state = readStreamRow(found);

          if (input.epoch !== state.epoch) {
            return yield* reject(
              'fenced',
              `epoch ${input.epoch} is not the current epoch ${state.epoch}`,
            );
          }
          if (input.producerId !== state.producerId) {
            return yield* reject(
              'conflict',
              `producer ${input.producerId} does not hold epoch ${state.epoch}`,
            );
          }

          // Same position as the memory backend puts it: after the identity
          // checks, before the sequence ones, so an unencodable payload from a
          // fenced producer still reports `fenced`.
          const digest = yield* ConversationRecord.fingerprint(
            input.records,
          ).pipe(
            Effect.mapError((error) =>
              failure(input.path, 'append', 'encoding', error.detail),
            ),
          );

          const expected = state.nextProducerSequence;
          if (expected > 0 && input.sequence === expected - 1) {
            // A retry. Idempotent only if it repeats the same batch: reusing a
            // sequence for different records is overwriting, and answering it
            // with the earlier offset drops the new records silently.
            if (digest !== state.lastFingerprint) {
              return yield* reject(
                'conflict',
                `sequence ${input.sequence} was reused with different content`,
              );
            }
            return state.lastOffset;
          }
          if (input.sequence !== expected) {
            return yield* reject(
              input.sequence > expected ? 'gap' : 'conflict',
              `expected sequence ${expected}, got ${input.sequence}`,
            );
          }

          // Everything above rejects without writing. From here the batch
          // either commits whole or the transaction rolls back.
          const encoded = yield* Effect.forEach(input.records, (entry) =>
            ConversationRecord.encodeEntry(entry),
          ).pipe(
            Effect.mapError((error) =>
              failure(
                input.path,
                'append',
                'encoding',
                `records do not encode: ${String(error)}`,
              ),
            ),
          );

          const values: string[] = [];
          const params: unknown[] = [];
          let offset = state.lastOffset;
          let sequence = state.nextSequence;

          for (const [index, entry] of encoded.entries()) {
            offset = LogOffset.fromSeq(sequence);
            const base = params.length;
            values.push(
              `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, ` +
                `$${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, ` +
                `$${base + 9}, $${base + 10}::jsonb)`,
            );
            params.push(
              input.path,
              sequence.toString(),
              offset,
              input.producerId,
              input.epoch,
              input.sequence,
              index,
              entry.conversationId,
              entry.timestamp,
              JSON.stringify(entry.record),
            );
            sequence += 1n;
          }

          yield* tx.query(
            `INSERT INTO ${records}
               (path, seq, record_offset, producer_id, producer_epoch,
                producer_sequence, batch_index, conversation_id,
                record_timestamp, record)
             VALUES ${values.join(', ')}`,
            params,
          );

          yield* tx.query(
            `UPDATE ${streams}
             SET next_sequence = $2,
                 next_producer_sequence = $3,
                 last_fingerprint = $4,
                 last_offset = $5
             WHERE path = $1`,
            [
              input.path,
              sequence.toString(),
              input.sequence + 1,
              digest,
              offset,
            ],
          );

          // Inside the transaction on purpose. Postgres queues notifications
          // and delivers them at commit, so a woken reader can never arrive
          // before the records it was woken for — and a rolled-back append
          // wakes nobody.
          yield* tx.query('SELECT pg_notify($1, $2)', [
            channelFor(input.path),
            input.path.slice(0, NOTIFY_PAYLOAD_LIMIT),
          ]);

          return offset;
        }),
      )
      .pipe(Effect.mapError(asLogStoreError(input.path, 'append')));
  });

  const read = Effect.fn('AiLog.LogStorePg.read')(function* (
    path: string,
    options?: LogStore.ReadOptions,
  ) {
    const after = options?.after ?? LogOffset.START;
    // A limit of zero would return an empty page that is not up to date, which
    // `Tail` would page forever.
    const limit = Math.max(1, options?.limit ?? LogStore.DEFAULT_READ_LIMIT);

    // One round trip, and the `LEFT JOIN LATERAL` is what makes it one: the
    // outer row proves the stream exists (zero rows is `not_found`, which a
    // plain record query cannot distinguish from an empty log), and the
    // lateral side pages it. `limit + 1` answers `upToDate` without a second
    // count — a backend that guesses `false` here makes `Tail` spin.
    const rows = yield* client
      .query(
        `SELECT r.record_offset, r.conversation_id, r.record_timestamp, r.record
         FROM ${streams} s
         LEFT JOIN LATERAL (
           SELECT record_offset, conversation_id, record_timestamp, record
           FROM ${records}
           WHERE path = s.path
             AND record_offset COLLATE "C" > $2 COLLATE "C"
           ORDER BY record_offset COLLATE "C" ASC
           LIMIT $3
         ) r ON true
         WHERE s.path = $1`,
        [path, after, limit + 1],
      )
      .pipe(Effect.mapError(asLogStoreError(path, 'read')));

    if (rows.length === 0) {
      return yield* Effect.fail(
        failure(path, 'read', 'not_found', 'no stream at this path'),
      );
    }

    // The stream exists but has nothing past `after`: the lateral side
    // produced no row and the outer join filled it with nulls.
    const first = rows[0];
    if (first?.['record_offset'] == null) {
      return {
        records: [],
        cursor: after,
        upToDate: true,
      } satisfies LogStore.Page;
    }

    const upToDate = rows.length <= limit;
    const page = upToDate ? rows : rows.slice(0, limit);

    const decoded = yield* Effect.forEach(page, (row) =>
      ConversationRecord.decodeEnvelope({
        offset: asString(row['record_offset']),
        conversationId: asString(row['conversation_id']),
        timestamp: asNumber(row['record_timestamp']),
        record: row['record'],
      }),
    ).pipe(
      Effect.mapError((error) =>
        // A row this backend wrote that it can no longer decode is the
        // backend being wrong, not the caller — schema drift, a bad manual
        // edit, a driver that mangled jsonb.
        failure(
          path,
          'read',
          'storage',
          `stored record does not decode: ${String(error)}`,
        ),
      ),
    );

    const last = decoded[decoded.length - 1];
    return {
      records: decoded,
      cursor: last === undefined ? after : last.offset,
      upToDate,
    } satisfies LogStore.Page;
  });

  const meta = Effect.fn('AiLog.LogStorePg.meta')(function* (path: string) {
    const rows = yield* client
      .query(
        `SELECT identity, epoch, producer_id, next_sequence,
                next_producer_sequence, last_fingerprint, last_offset
         FROM ${streams}
         WHERE path = $1`,
        [path],
      )
      .pipe(Effect.mapError(asLogStoreError(path, 'meta')));

    const row = rows[0];
    return row === undefined
      ? Option.none<LogStore.StreamMeta>()
      : Option.some(metaOf(path, readStreamRow(row)));
  });

  // Subscribe, *then* emit the opening wake-up — the same order as the memory
  // backend, for the same reason. `listen` resolving is the proof that the
  // `LISTEN` took effect, so nothing appended after this point can be missed.
  // Emitting first and subscribing after would put the race back.
  const changes = (path: string): Stream.Stream<void, LogStore.LogStoreError> =>
    Stream.unwrap(
      client.listen(channelFor(path)).pipe(
        Effect.map((notifications) => {
          const opening: Stream.Stream<void> = Stream.make(undefined);
          return Stream.concat(opening, notifications);
        }),
      ),
    ).pipe(Stream.mapError(asLogStoreError(path, 'changes')));

  return LogStore.Service.of({
    create,
    acquire,
    append,
    read,
    meta,
    changes,
  });
};

export const layer = (
  client: Client,
  options?: Options,
): Layer.Layer<LogStore.Service> =>
  Layer.sync(LogStore.Service, () => make(client, options));

// ---------------------------------------------------------------------------
// node-postgres adapter
// ---------------------------------------------------------------------------

/** What this adapter uses of a `pg.QueryResult`. */
export interface QueryResultLike {
  readonly rows: ReadonlyArray<Row>;
}

/** What this adapter uses of a `pg.PoolClient`. */
export interface PoolConnectionLike {
  query(
    text: string,
    values?: ReadonlyArray<unknown>,
  ): Promise<QueryResultLike>;
  on(
    event: 'notification',
    listener: (message: { readonly payload?: string | undefined }) => void,
  ): unknown;
  on(event: 'error', listener: (error: Error) => void): unknown;
  /** `true` destroys the connection instead of returning it to the pool. */
  release(destroy?: boolean): void;
}

/**
 * What this adapter uses of a `pg.Pool`.
 *
 * Structural on purpose. `pg` is not a dependency of `@sunfall/vesper-log` and should
 * not become one — the family's layering rule is `log -> effect` — so an
 * application passes the pool it already owns and this module never learns
 * which driver it came from.
 */
export interface PoolLike {
  connect(): Promise<PoolConnectionLike>;
  query(
    text: string,
    values?: ReadonlyArray<unknown>,
  ): Promise<QueryResultLike>;
}

const sqlFailure = (cause: unknown): SqlFailure => {
  const code =
    typeof cause === 'object' && cause !== null && 'code' in cause
      ? String((cause as { code: unknown }).code)
      : undefined;
  return new SqlFailure({
    detail: cause instanceof Error ? cause.message : String(cause),
    code,
  });
};

/**
 * Turn a pool into a {@link Client}.
 *
 * The pool stays the caller's: this never creates one and never ends one. The
 * only connection it owns is the dedicated session `listen` holds, which is
 * released — destroyed, not returned — when the subscribing scope closes,
 * because a connection that has issued `LISTEN` is not safe to hand back to a
 * pool that does not reset it.
 */
export const fromPool = (pool: PoolLike): Client => {
  const runQuery =
    (
      run: (
        text: string,
        values?: ReadonlyArray<unknown>,
      ) => Promise<QueryResultLike>,
    ) =>
    (text: string, params?: ReadonlyArray<unknown>) =>
      Effect.tryPromise({
        try: () => run(text, params ?? []),
        catch: sqlFailure,
      }).pipe(Effect.map((result) => result.rows));

  const query = runQuery((text, values) => pool.query(text, values));

  const transaction = <A, E, R>(
    body: (tx: Sql) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | SqlFailure, R> =>
    Effect.suspend(() => {
      // A connection whose ROLLBACK did not land is still inside an aborted
      // transaction, and `pg` does not reset one on release. Returning it to
      // the pool would hand the next caller a connection that fails every
      // statement with 25P02 — so it gets destroyed instead.
      let poisoned = false;

      return Effect.acquireUseRelease(
        Effect.tryPromise({ try: () => pool.connect(), catch: sqlFailure }),
        (connection) => {
          const tx: Sql = {
            query: runQuery((text, values) => connection.query(text, values)),
          };
          return Effect.tryPromise({
            try: () => connection.query('BEGIN'),
            catch: sqlFailure,
          }).pipe(
            Effect.flatMap(() => body(tx)),
            Effect.tap(() =>
              Effect.tryPromise({
                try: () => connection.query('COMMIT'),
                catch: sqlFailure,
              }),
            ),
            // Runs for a failure and for an interruption alike; both leave an
            // open transaction that has to be undone. A rollback that itself
            // fails must not replace the failure that caused it — the caller
            // needs to know it was fenced, not that the socket also died on
            // the way out.
            Effect.onError(() =>
              Effect.tryPromise({
                try: () => connection.query('ROLLBACK'),
                catch: sqlFailure,
              }).pipe(
                Effect.catchCause(() =>
                  Effect.sync(() => {
                    poisoned = true;
                  }),
                ),
              ),
            ),
          );
        },
        (connection) => Effect.sync(() => connection.release(poisoned)),
      );
    });

  const listen = (channel: string) =>
    Effect.gen(function* () {
      const connection = yield* Effect.acquireRelease(
        Effect.tryPromise({ try: () => pool.connect(), catch: sqlFailure }),
        // Destroy rather than return: `LISTEN` is session state and the pool
        // does not reset it, so a recycled connection would keep delivering
        // notifications to whoever picked it up next.
        (held) => Effect.sync(() => held.release(true)),
      );

      // Sliding, capacity one. Publishing must never block on a reader, and
      // dropping a wake-up is only safe because a newer one replaces it.
      const wakeups = yield* Queue.sliding<void, SqlFailure | Cause.Done>(1);

      connection.on('notification', () => {
        Queue.offerUnsafe(wakeups, undefined);
      });
      // The case `changes`'s error channel exists for. A dropped `LISTEN`
      // connection stops delivering; if that reached the consumer as silence,
      // a stalled tail would be indistinguishable from a quiet conversation.
      connection.on('error', (error) => {
        Queue.failCauseUnsafe(wakeups, Cause.fail(sqlFailure(error)));
      });

      // Resolves only once the LISTEN is registered, which is what the caller
      // treats as proof of subscription.
      yield* Effect.tryPromise({
        try: () => connection.query(`LISTEN "${channel}"`),
        catch: sqlFailure,
      });

      return Stream.fromQueue(wakeups);
    });

  return { query, transaction, listen };
};

export * as LogStorePg from './layer-pg.js';
