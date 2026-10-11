import { DatabaseSync } from 'node:sqlite';

import type {
  DurableObjectSqlCursor,
  DurableObjectStorage,
} from '../src/layer-cloudflare.js';

/**
 * A stand-in for a Durable Object's `ctx.storage`, built on `node:sqlite`.
 *
 * It models the parts of Cloudflare's SQLite storage the adapter depends on,
 * as documented: `sql.exec` returns a cursor, `transactionSync` is the only
 * way to open a transaction, and transaction-control SQL is rejected. It is
 * not workerd, so it proves the adapter never needs SQL-level transactions,
 * not that every Durable Object edge case behaves identically.
 */

const TRANSACTION_CONTROL =
  /^\s*(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)\b/i;

const isBinding = (value: unknown): value is string | number | null =>
  value === null ||
  typeof value === 'string' ||
  typeof value === 'number' ||
  value instanceof ArrayBuffer;

const toValue = (value: unknown): string | number | null => {
  if (value === null || typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return Number(value);
  }
  throw new TypeError('Stand-in storage returned an unsupported value');
};

export interface StandInStorage extends DurableObjectStorage {
  /** Statements `exec` ran, in order. */
  readonly statements: ReadonlyArray<string>;
  /** How many top-level `transactionSync` calls committed or rolled back. */
  readonly transactions: () => number;
}

export const makeStandInStorage = (): StandInStorage => {
  const database = new DatabaseSync(':memory:');
  const statements: string[] = [];
  let depth = 0;
  let transactions = 0;

  const exec = (
    query: string,
    ...bindings: unknown[]
  ): DurableObjectSqlCursor => {
    if (TRANSACTION_CONTROL.test(query)) {
      throw new Error(
        'To execute a transaction, please use the state.storage.transaction() or state.storage.transactionSync() APIs instead of the SQL BEGIN TRANSACTION or SAVEPOINT statements.',
      );
    }
    if (!bindings.every(isBinding)) {
      throw new TypeError('Type of binding is not supported');
    }
    const params = bindings.filter(isBinding);
    statements.push(query);
    const statement = database.prepare(query);
    if (statement.columns().length === 0) {
      const { changes } = statement.run(...params);
      return {
        toArray: () => [],
        raw: () => [][Symbol.iterator](),
        rowsWritten: Number(changes),
      };
    }
    const rows = statement
      .all(...params)
      .map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([column, value]) => [
            column,
            toValue(value),
          ]),
        ),
      );
    return {
      toArray: () => rows,
      raw: () => rows.map((row) => Object.values(row))[Symbol.iterator](),
      rowsWritten: 0,
    };
  };

  const transactionSync = <T>(closure: () => T): T => {
    const name = `sp_${String(depth)}`;
    database.exec(depth === 0 ? 'BEGIN' : `SAVEPOINT ${name}`);
    depth += 1;
    try {
      const result = closure();
      depth -= 1;
      database.exec(depth === 0 ? 'COMMIT' : `RELEASE ${name}`);
      return result;
    } catch (error) {
      depth -= 1;
      database.exec(
        depth === 0 ? 'ROLLBACK' : `ROLLBACK TO ${name}; RELEASE ${name}`,
      );
      throw error;
    } finally {
      if (depth === 0) {
        transactions += 1;
      }
    }
  };

  return {
    sql: { exec },
    transactionSync,
    statements,
    transactions: () => transactions,
  };
};
