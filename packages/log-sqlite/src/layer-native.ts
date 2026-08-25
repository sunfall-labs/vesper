// oxlint-disable-next-line effecttsgo/node-builtin-import -- this entry is the explicit native Node/Bun adapter
import { mkdirSync } from 'node:fs';
import { createRequire } from 'node:module';
// oxlint-disable-next-line effecttsgo/node-builtin-import -- this entry is the explicit native Node/Bun adapter
import { dirname } from 'node:path';

import type { Config } from 'effect';
import { Effect, Layer, Schema, Stream } from 'effect';
import { Reactivity } from 'effect/unstable/reactivity';
import { SqlClient, SqlError, Statement } from 'effect/unstable/sql';
import type { SqlConnection } from 'effect/unstable/sql';

/**
 * Structural subset shared by Bun's `bun:sqlite` Database and Node's
 * `node:sqlite` DatabaseSync. Keeping the driver behind this small surface
 * means the rest of Vesper only depends on Effect's SqlClient.
 */
export interface SqliteDatabase {
  readonly prepare: (sql: string) => SqliteStatement;
  readonly close: () => void;
}

interface SqliteStatement {
  readonly all: (...params: readonly unknown[]) => readonly unknown[];
  readonly values?: (
    ...params: readonly unknown[]
  ) => ReadonlyArray<ReadonlyArray<unknown>>;
  readonly setReturnArrays?: (value: boolean) => void;
}

const loadModule: (specifier: string) => unknown = createRequire(
  import.meta.url,
);

const isBun =
  typeof process !== 'undefined' && typeof process.versions['bun'] === 'string';

const compiler = Statement.makeCompilerSqlite((name) =>
  name.replaceAll(/[A-Z]/g, (character) => `_${character.toLowerCase()}`),
);

const wrapError = (cause: unknown): SqlError.SqlError =>
  new SqlError.SqlError({ reason: new SqlError.UnknownError({ cause }) });

const isObjectRow = (value: unknown): value is object =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isValueRow = (value: unknown): value is ReadonlyArray<unknown> =>
  Array.isArray(value);

const invalidRows = (kind: string): TypeError =>
  new TypeError(`Native SQLite returned invalid ${kind} rows`);

const runAll = (
  database: SqliteDatabase,
  sql: string,
  params: readonly unknown[],
): readonly object[] =>
  database
    .prepare(sql)
    .all(...params)
    .map((row) => {
      if (!isObjectRow(row)) {
        throw invalidRows('object');
      }
      return row;
    });

const runValues = (
  database: SqliteDatabase,
  sql: string,
  params: readonly unknown[],
): ReadonlyArray<ReadonlyArray<unknown>> => {
  const statement = database.prepare(sql);
  if (typeof statement.values === 'function') {
    return statement.values(...params);
  }
  statement.setReturnArrays?.(true);
  return statement.all(...params).map((row) => {
    if (!isValueRow(row)) {
      throw invalidRows('value');
    }
    return row;
  });
};

const isSqliteDatabase = (value: unknown): value is SqliteDatabase =>
  typeof value === 'object' &&
  value !== null &&
  typeof Reflect.get(value, 'prepare') === 'function' &&
  typeof Reflect.get(value, 'close') === 'function';

const isDatabaseConstructor = (
  value: unknown,
): value is new (path: string) => unknown => typeof value === 'function';

const BunSqliteModule = Schema.Struct({ Database: Schema.Unknown });
const NodeSqliteModule = Schema.Struct({ DatabaseSync: Schema.Unknown });

const loadDatabase = (
  specifier: 'bun:sqlite' | 'node:sqlite',
  exportName: 'Database' | 'DatabaseSync',
  path: string,
): SqliteDatabase => {
  const loaded = loadModule(specifier);
  const constructor: unknown =
    exportName === 'Database'
      ? Schema.is(BunSqliteModule)(loaded)
        ? loaded.Database
        : undefined
      : Schema.is(NodeSqliteModule)(loaded)
        ? loaded.DatabaseSync
        : undefined;
  if (!isDatabaseConstructor(constructor)) {
    throw new TypeError(`${specifier} did not export ${exportName}`);
  }
  const database = new constructor(path);
  if (!isSqliteDatabase(database)) {
    throw new TypeError(`${specifier}.${exportName} is not a SQLite database`);
  }
  return database;
};

const makeConnection = (
  database: SqliteDatabase,
): SqlConnection.Connection => ({
  execute: (sql, params, transformRows) =>
    Effect.try({
      try: () => {
        const rows = runAll(database, sql, params);
        return transformRows ? transformRows(rows) : rows;
      },
      catch: wrapError,
    }),

  executeRaw: (sql, params) =>
    Effect.try({
      try: () => runAll(database, sql, params),
      catch: wrapError,
    }),

  executeStream: (sql, params, transformRows) =>
    Stream.fromEffect(
      Effect.try({
        try: () => {
          const rows = runAll(database, sql, params);
          return transformRows ? transformRows(rows) : rows;
        },
        catch: wrapError,
      }),
    ).pipe(Stream.flatMap(Stream.fromIterable)),

  executeUnprepared: (sql, params, transformRows) =>
    Effect.try({
      try: () => {
        const rows = runAll(database, sql, params);
        return transformRows ? transformRows(rows) : rows;
      },
      catch: wrapError,
    }),

  executeValues: (sql, params) =>
    Effect.try({
      try: () => runValues(database, sql, params),
      catch: wrapError,
    }),

  executeValuesUnprepared: (sql, params) =>
    Effect.try({
      try: () => runValues(database, sql, params),
      catch: wrapError,
    }),
});

/** Open a native SQLite database using the current JavaScript runtime. */
export const makeDatabase = (path: string): SqliteDatabase => {
  if (path !== ':memory:') {
    mkdirSync(dirname(path), { recursive: true });
  }

  if (isBun) {
    return loadDatabase('bun:sqlite', 'Database', path);
  }

  return loadDatabase('node:sqlite', 'DatabaseSync', path);
};

const makeSqlClient = (path: string) =>
  Effect.gen(function* () {
    const database = yield* Effect.acquireRelease(
      Effect.sync(() => makeDatabase(path)),
      (instance) =>
        Effect.sync(() => {
          instance.close();
        }),
    );
    return yield* SqlClient.make({
      acquirer: Effect.succeed(makeConnection(database)),
      compiler,
      spanAttributes: [['db.system', 'sqlite']],
    });
  });

/** Effect layer for a native Bun or Node SQLite database. */
export const layer = (path: string): Layer.Layer<SqlClient.SqlClient> =>
  Layer.effect(SqlClient.SqlClient)(makeSqlClient(path)).pipe(
    Layer.provide(Reactivity.layer),
  );

/** Configured variant for applications that obtain the database path via Effect Config. */
export const layerConfig = (options: {
  readonly path: Config.Config<string>;
}): Layer.Layer<SqlClient.SqlClient, Config.ConfigError> =>
  Layer.effect(
    SqlClient.SqlClient,
    Effect.flatMap(options.path, (path) => makeSqlClient(path)),
  ).pipe(Layer.provide(Reactivity.layer));

export * as SqliteNative from './layer-native.js';
