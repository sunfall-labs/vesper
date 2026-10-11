import { Effect, Stream } from 'effect';
import { SqlError, Statement } from 'effect/sql';
import type { SqlConnection } from 'effect/sql';

/**
 * Structural subset shared by Bun's `bun:sqlite` Database and Node's
 * `node:sqlite` DatabaseSync. Keeping the driver behind this small surface
 * means the rest of Vesper only depends on Effect's SqlClient.
 */
export interface SqliteDatabase {
  readonly prepare: (sql: string) => SqliteStatement;
  readonly close: () => void;
}

export interface SqliteStatement {
  readonly all: (...params: readonly unknown[]) => readonly unknown[];
  readonly values?: (
    ...params: readonly unknown[]
  ) => ReadonlyArray<ReadonlyArray<unknown>>;
  readonly setReturnArrays?: (value: boolean) => void;
}

export const compiler = Statement.makeCompilerSqlite((name) =>
  name.replaceAll(/[A-Z]/g, (character) => `_${character.toLowerCase()}`),
);

// Classified by the driver's SQLite code, so a busy or locked database
// surfaces as a retryable `LockTimeoutError` rather than an unknown failure.
const wrapError =
  (operation: string) =>
  (cause: unknown): SqlError.SqlError =>
    new SqlError.SqlError({
      reason: SqlError.classifySqliteError(cause, {
        operation,
        message: cause instanceof Error ? cause.message : undefined,
      }),
    });

const isObjectRow = (value: unknown): value is object =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isValueRow = (value: unknown): value is ReadonlyArray<unknown> =>
  Array.isArray(value);

const invalidRows = (kind: string): TypeError =>
  new TypeError(`SQLite returned invalid ${kind} rows`);

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

export const makeConnection = (
  database: SqliteDatabase,
): SqlConnection.Connection => ({
  execute: (sql, params, transformRows) =>
    Effect.try({
      try: () => {
        const rows = runAll(database, sql, params);
        return transformRows ? transformRows(rows) : rows;
      },
      catch: wrapError('execute'),
    }),

  executeRaw: (sql, params) =>
    Effect.try({
      try: () => runAll(database, sql, params),
      catch: wrapError('executeRaw'),
    }),

  executeStream: (sql, params, transformRows) =>
    Stream.fromEffect(
      Effect.try({
        try: () => {
          const rows = runAll(database, sql, params);
          return transformRows ? transformRows(rows) : rows;
        },
        catch: wrapError('executeStream'),
      }),
    ).pipe(Stream.flatMap(Stream.fromIterable)),

  executeUnprepared: (sql, params, transformRows) =>
    Effect.try({
      try: () => {
        const rows = runAll(database, sql, params);
        return transformRows ? transformRows(rows) : rows;
      },
      catch: wrapError('executeUnprepared'),
    }),

  executeValues: (sql, params) =>
    Effect.try({
      try: () => runValues(database, sql, params),
      catch: wrapError('executeValues'),
    }),

  executeValuesUnprepared: (sql, params) =>
    Effect.try({
      try: () => runValues(database, sql, params),
      catch: wrapError('executeValuesUnprepared'),
    }),
});
