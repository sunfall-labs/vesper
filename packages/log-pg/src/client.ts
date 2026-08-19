import { PgClient } from '@effect/sql-pg';
import { Effect } from 'effect';
import type { Layer } from 'effect';
import type { SqlClient } from 'effect/unstable/sql/SqlClient';
import type { SqlError } from 'effect/unstable/sql/SqlError';

import { correctedListen } from './internal/pg-listen.js';

/**
 * Constructs the official Effect PgClient with corrected rc.109 LISTEN
 * lifecycle semantics. SQL queries and transactions remain the unmodified
 * `@effect/sql-pg` implementation; only `PgClient.listen` is replaced.
 */
export const make = Effect.fn('VesperPgClient.make')(function* (
  config: PgClient.PgPoolConfig,
) {
  const client = yield* PgClient.make(config);
  return Object.assign(client, { listen: correctedListen(config) });
});

/** Provides both official `PgClient.PgClient` and generic `SqlClient`. */
export const layer = (
  config: PgClient.PgPoolConfig,
): Layer.Layer<PgClient.PgClient | SqlClient, SqlError> =>
  PgClient.layerFrom(make(config));

export * as VesperPgClient from './client.js';
