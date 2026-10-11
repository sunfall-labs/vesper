// oxlint-disable-next-line effecttsgo/node-builtin-import -- this entry is the explicit native Node/Bun adapter
import { mkdirSync } from 'node:fs';
import { createRequire } from 'node:module';
// oxlint-disable-next-line effecttsgo/node-builtin-import -- this entry is the explicit native Node/Bun adapter
import { dirname } from 'node:path';

import type { Config } from 'effect';
import { Effect, Layer, Schema } from 'effect';
import { Reactivity } from 'effect/reactivity';
import { SqlClient } from 'effect/sql';

import { compiler, makeConnection } from './sqlite-connection.js';
import type { SqliteDatabase } from './sqlite-connection.js';

export type { SqliteDatabase } from './sqlite-connection.js';

const loadModule: (specifier: string) => unknown = createRequire(
  import.meta.url,
);

const isBun =
  typeof process !== 'undefined' && typeof process.versions['bun'] === 'string';

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
