import { Crypto, Data, Effect, Exit, Layer } from 'effect';
import type { Config } from 'effect';
import { PlatformError } from 'effect';
import { Reactivity } from 'effect/reactivity';
import { SqlClient, SqlError } from 'effect/sql';

import { compiler, makeConnection } from './sqlite-connection.js';
import { sha256 } from './sha256.js';
import type { SqliteDatabase } from './sqlite-connection.js';

/**
 * SQLite storage for a Cloudflare Durable Object.
 *
 * Nothing here imports a Node module or `cloudflare:workers`: the Durable
 * Object's storage is passed in by the caller and typed structurally, so this
 * entry loads in workerd without a Cloudflare dependency. The interfaces below
 * mirror the documented `SqlStorage` and `DurableObjectStorage` surface that
 * Vesper uses and nothing more.
 */

/** A value Durable Object SQLite accepts as a binding or returns in a row. */
export type DurableObjectSqlValue = ArrayBuffer | string | number | null;

/** Structural subset of Cloudflare's `SqlStorageCursor`. */
export interface DurableObjectSqlCursor {
  /** Drain the cursor into row objects. */
  readonly toArray: () => Array<Record<string, DurableObjectSqlValue>>;
  /** Drain the cursor into positional rows. */
  readonly raw: () => IterableIterator<Array<DurableObjectSqlValue>>;
  readonly rowsWritten: number;
}

/** Structural subset of Cloudflare's `SqlStorage` (`ctx.storage.sql`). */
export interface DurableObjectSql {
  readonly exec: (
    query: string,
    ...bindings: Array<unknown>
  ) => DurableObjectSqlCursor;
}

/** Structural subset of Cloudflare's `DurableObjectStorage` (`ctx.storage`). */
export interface DurableObjectStorage {
  readonly sql: DurableObjectSql;
  readonly transactionSync: <T>(closure: () => T) => T;
}

// A Durable Object rejects `BEGIN`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT`
// statements; its only transactions are `transactionSync` and `transaction`.
// Statements are therefore never wrapped in SQL-level transaction controls
// here: `withTransaction` below is the one place atomicity is established.
const makeDatabase = (storage: DurableObjectStorage): SqliteDatabase => ({
  prepare: (sql) => ({
    // Cursors are always drained: a Durable Object runs a write as the cursor
    // is consumed, and `rowsWritten` is only final afterwards.
    all: (...params) => storage.sql.exec(sql, ...params).toArray(),
    values: (...params) => Array.from(storage.sql.exec(sql, ...params).raw()),
  }),
  close: () => undefined,
});

// Thrown inside `transactionSync` to make the Durable Object roll back; it
// never escapes `withTransaction`.
class Rollback extends Data.TaggedError('Rollback') {}

/**
 * Run a transaction body inside `transactionSync`.
 *
 * `transactionSync` takes a synchronous closure, but a transaction body is an
 * Effect. The log store's transaction bodies are made only of synchronous SQL
 * steps, so the body is run to completion with `runSyncExit` inside the
 * closure; a body that suspends asynchronously dies with an
 * `AsyncFiberException` and the transaction is rolled back. A failure or defect
 * in the body also rolls back, then reaches the caller unchanged. Nested
 * `withTransaction` calls nest `transactionSync`, which a Durable Object
 * supports.
 */
const makeWithTransaction =
  (storage: DurableObjectStorage): SqlClient.SqlClient['withTransaction'] =>
  <R, E, A>(effect: Effect.Effect<A, E, R>) =>
    Effect.flatMap(Effect.context<R>(), (context) =>
      Effect.suspend((): Effect.Effect<A, E | SqlError.SqlError> => {
        const run = Effect.runSyncExitWith(context);
        let outcome: Exit.Exit<A, E> | undefined;
        try {
          storage.transactionSync(() => {
            const exit = run(effect);
            outcome = exit;
            if (Exit.isFailure(exit)) {
              throw new Rollback();
            }
          });
        } catch (cause) {
          if (!(cause instanceof Rollback)) {
            return Effect.fail(
              new SqlError.SqlError({
                reason: SqlError.classifySqliteError(cause, {
                  operation: 'transaction',
                  message: cause instanceof Error ? cause.message : undefined,
                }),
              }),
            );
          }
        }
        return outcome === undefined
          ? Effect.die(new Error('transactionSync did not run its closure'))
          : outcome;
      }),
    );

const makeSqlClient = (storage: DurableObjectStorage) =>
  Effect.gen(function* () {
    const client = yield* SqlClient.make({
      acquirer: Effect.succeed(makeConnection(makeDatabase(storage))),
      compiler,
      spanAttributes: [['db.system', 'sqlite']],
    });
    // `SqlClient.make` only knows SQL-level transaction statements, which a
    // Durable Object forbids. The client is a callable object, so replace the
    // member in place rather than spreading it.
    return Object.assign(client, {
      withTransaction: makeWithTransaction(storage),
    });
  });

/** Effect `SqlClient` over a Durable Object's `ctx.storage`. */
export const layer = (
  storage: DurableObjectStorage,
): Layer.Layer<SqlClient.SqlClient> =>
  Layer.effect(SqlClient.SqlClient)(makeSqlClient(storage)).pipe(
    Layer.provide(Reactivity.layer),
  );

/** Configured variant for storage obtained through Effect Config. */
export const layerConfig = (options: {
  readonly storage: Config.Config<DurableObjectStorage>;
}): Layer.Layer<SqlClient.SqlClient, Config.ConfigError> =>
  Layer.effect(
    SqlClient.SqlClient,
    Effect.flatMap(options.storage, makeSqlClient),
  ).pipe(Layer.provide(Reactivity.layer));

const digestError = (description: string, cause?: unknown) =>
  PlatformError.systemError({
    module: 'Crypto',
    method: 'digest',
    _tag: 'Unknown',
    description,
    cause,
  });

/**
 * Effect `Crypto` for a Worker. `@effect/platform-node`'s layer is not
 * available there.
 *
 * SHA-256 is computed synchronously in plain TypeScript: the log store hashes
 * inside `transactionSync`, whose body cannot wait for Web Crypto's
 * promise-returning `subtle.digest` (it would die with `AsyncFiberException`
 * and roll back every append). Other algorithms fall back to Web Crypto and
 * must not be used inside a transaction. Random bytes use the synchronous
 * `crypto.getRandomValues`.
 */
export const cryptoLayer: Layer.Layer<Crypto.Crypto> = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) =>
      globalThis.crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, data) =>
      algorithm === 'SHA-256'
        ? Effect.try({
            try: () => sha256(data),
            catch: (cause) => digestError('Could not compute digest', cause),
          })
        : Effect.tryPromise({
            try: () =>
              globalThis.crypto.subtle
                .digest(algorithm, Uint8Array.from(data))
                .then((digest) => new Uint8Array(digest)),
            catch: (cause) => digestError('Could not compute digest', cause),
          }),
  }),
);

export * as SqliteCloudflare from './layer-cloudflare.js';
