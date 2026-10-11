import { PgClient } from '@effect/sql-pg';
import type { Layer } from 'effect';
import type { SqlClient, SqlError } from 'effect/sql';

/**
 * Constructs the official Effect PgClient. Since `@effect/sql-pg` 4.0.0 its
 * `listen` registers LISTEN before resolving and fails its queue when the
 * connection dies, so no part of the client is replaced.
 */
export const make = (config: PgClient.PgPoolConfig) => PgClient.make(config);

/** Provides both official `PgClient.PgClient` and generic `SqlClient`. */
export const layer = (
  config: PgClient.PgPoolConfig,
): Layer.Layer<PgClient.PgClient | SqlClient.SqlClient, SqlError.SqlError> =>
  PgClient.layer(config);

export * as VesperPgClient from './client.js';
