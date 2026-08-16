import { randomUUID } from 'node:crypto';

import { PostgreSqlContainer } from '@testcontainers/postgresql';
import pg from 'pg';

// Test support for `layer-pg.integration.test.ts`. Not part of the package's
// public surface — nothing in `exports` points here.
//
// In the repository this package was extracted from, the `ai_log` tables were
// owned by a central migration system and this file's job was done by a shared
// testkit that shelled out to that system's schema tool. Standalone, there is
// no central migration system to defer to, so the DDL below *is* the schema.
//
// That makes this file a second source of truth, which is normally the thing
// to avoid: a store that carries its own schema is a migration system that
// disagrees with the real one exactly once, in production. Here it is the
// honest arrangement — this package ships a backend, not a database. An
// application supplies its own migrations, and this DDL is the reference for
// what they have to produce. `layer-pg.ts` still issues no DDL of its own.

/**
 * The tables `LogStorePg` reads and writes.
 *
 * Two details are load-bearing rather than stylistic:
 *
 * - `ai_log_records_producer_batch_unique` has **five** columns. Offsets are
 *   per record, not per append batch, so one batch is many rows sharing a
 *   producer sequence; a four-column index would reject the batch's own second
 *   row. `batch_index` is what keeps write-once-per-slot enforceable at the
 *   database rather than in application state.
 * - the read index pins `COLLATE "C"`, matching every comparison in
 *   `layer-pg.ts`. Under a glibc locale, punctuation is ignored at the primary
 *   comparison level and `'-1'` — read-from-the-beginning — sorts *after*
 *   every real offset, so an unpinned read returns an empty log to every fresh
 *   reader. Without the matching collation here the index is merely unused;
 *   without it in the queries the result is wrong.
 */
export const SCHEMA_DDL = `
CREATE SCHEMA IF NOT EXISTS ai_log;

CREATE TABLE IF NOT EXISTS ai_log.streams (
  path                   text PRIMARY KEY,
  identity               text NOT NULL,
  epoch                  bigint NOT NULL DEFAULT 0,
  producer_id            text,
  next_sequence          bigint NOT NULL DEFAULT 0,
  next_producer_sequence bigint NOT NULL DEFAULT 0,
  last_fingerprint       text NOT NULL DEFAULT '',
  last_offset            text NOT NULL DEFAULT '-1',
  created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ai_log.records (
  path              text NOT NULL,
  seq               bigint NOT NULL,
  record_offset     text NOT NULL,
  producer_id       text NOT NULL,
  producer_epoch    bigint NOT NULL,
  producer_sequence bigint NOT NULL,
  batch_index       integer NOT NULL,
  conversation_id   text NOT NULL,
  record_timestamp  bigint NOT NULL,
  record            jsonb NOT NULL,
  written_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT ai_log_records_pkey PRIMARY KEY (path, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS ai_log_records_producer_batch_unique
  ON ai_log.records (
    path, producer_id, producer_epoch, producer_sequence, batch_index
  );

CREATE INDEX IF NOT EXISTS ai_log_records_path_offset_idx
  ON ai_log.records (path, record_offset COLLATE "C");
`;

const IMAGE = process.env['ARBOR_POSTGRES_TEST_IMAGE'] ?? 'postgres:16-alpine';

export interface ProvisionedTestDatabase {
  readonly connectionString: string;
  readonly pool: pg.Pool;
  readonly cleanup: () => Promise<void>;
}

export interface PostgresTestHarness {
  readonly adminConnectionString: string;
  readonly provisionDatabase: (options?: {
    readonly namePrefix?: string;
  }) => Promise<ProvisionedTestDatabase>;
  readonly stop: () => Promise<void>;
}

const withDatabase = (connectionString: string, databaseName: string) => {
  const url = new URL(connectionString);
  url.pathname = `/${databaseName}`;
  return url.toString();
};

const delay = (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/** `object_in_use` — "database is being accessed by other users". */
const PG_OBJECT_IN_USE = '55006';

// Sockets can outlive `pool.end()` briefly. Keep the wait short: `afterAll`
// rarely carries a generous timeout, and a genuinely leaked connection is
// handled by the escalation in `dropDatabase` rather than by waiting longer.
const DRAIN_ATTEMPTS = 20;
const DRAIN_INTERVAL_MS = 25;

const waitForConnectionsToClose = async (
  admin: pg.Pool,
  databaseName: string,
) => {
  for (let attempt = 0; attempt < DRAIN_ATTEMPTS; attempt += 1) {
    const result = await admin.query<{ count: string }>(
      `SELECT count(*)::text AS count
       FROM pg_stat_activity
       WHERE datname = $1 AND pid <> pg_backend_pid()`,
      [databaseName],
    );
    if (Number(result.rows[0]?.count ?? 0) === 0) return;
    await delay(DRAIN_INTERVAL_MS);
  }
};

/**
 * Drop the test database, escalating only if something is still attached.
 *
 * `WITH (FORCE)` is not used unconditionally: terminating a backend the suite
 * still holds surfaces to that client as an unhandled 57P01 and fails the run
 * after its assertions have already passed.
 */
const dropDatabase = async (admin: pg.Pool, databaseName: string) => {
  try {
    await admin.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
    return;
  } catch (error) {
    if ((error as { code?: string })?.code !== PG_OBJECT_IN_USE) throw error;
  }

  await admin.query(
    `SELECT pg_terminate_backend(pid)
     FROM pg_stat_activity
     WHERE datname = $1 AND pid <> pg_backend_pid()`,
    [databaseName],
  );
  await admin.query(`DROP DATABASE IF EXISTS "${databaseName}"`);
};

/**
 * Start one Postgres container and hand out fresh databases inside it.
 *
 * A database per test file rather than a container per test file: container
 * startup dominates, and the two suites that share this harness must not see
 * each other's rows.
 */
export const createPostgresTestHarness =
  async (): Promise<PostgresTestHarness> => {
    process.env['TESTCONTAINERS_RYUK_DISABLED'] ??= 'true';

    const container = await new PostgreSqlContainer(IMAGE)
      .withDatabase('postgres')
      .withUsername('postgres')
      .withPassword('postgres')
      .start();

    const adminConnectionString = container.getConnectionUri();
    const admin = new pg.Pool({ connectionString: adminConnectionString });

    return {
      adminConnectionString,
      async provisionDatabase(options = {}) {
        const databaseName = `${options.namePrefix ?? 'test'}_${randomUUID().replace(/-/g, '_')}`;
        await admin.query(`CREATE DATABASE "${databaseName}"`);

        const connectionString = withDatabase(
          adminConnectionString,
          databaseName,
        );
        const pool = new pg.Pool({ connectionString });
        await pool.query(SCHEMA_DDL);

        return {
          connectionString,
          pool,
          cleanup: async () => {
            await pool.end();
            await waitForConnectionsToClose(admin, databaseName);
            await dropDatabase(admin, databaseName);
          },
        };
      },
      async stop() {
        await admin.end();
        await container.stop();
      },
    };
  };
