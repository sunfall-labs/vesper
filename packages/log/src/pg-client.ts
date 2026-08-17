import { PgClient } from '@effect/sql-pg';
import {
  Cause,
  Duration,
  Effect,
  Layer,
  Queue,
  Redacted,
  Stream,
} from 'effect';
import { SqlClient } from 'effect/unstable/sql/SqlClient';
import {
  ConnectionError,
  SqlError,
  UnknownError,
} from 'effect/unstable/sql/SqlError';
import pg from 'pg';

const escapeIdentifier = (identifier: string): string =>
  `"${identifier.replaceAll('"', '""')}"`;

const connectionError = (
  cause: unknown,
  message: string,
  operation: string,
): SqlError =>
  new SqlError({
    reason: new ConnectionError({ cause, message, operation }),
  });

const queryError = (
  cause: unknown,
  message: string,
  operation: string,
): SqlError =>
  new SqlError({ reason: new UnknownError({ cause, message, operation }) });

const clientConfig = (config: PgClient.PgPoolConfig): pg.ClientConfig => ({
  connectionString: config.url ? Redacted.value(config.url) : undefined,
  user: config.username,
  host: config.host,
  database: config.database,
  password: config.password ? Redacted.value(config.password) : undefined,
  ssl: config.ssl,
  port: config.port,
  ...(config.stream ? { stream: config.stream } : {}),
  connectionTimeoutMillis: config.connectTimeout
    ? Duration.toMillis(Duration.fromInputUnsafe(config.connectTimeout))
    : undefined,
  application_name: config.applicationName ?? '@sunfall/vesper-log-listener',
  types: config.types,
});

const correctedListen = (config: PgClient.PgPoolConfig) => (channel: string) =>
  Stream.callback<string, SqlError>(
    Effect.fnUntraced(function* (queue) {
      const client = new pg.Client(clientConfig(config));
      let terminal = false;
      let connected = false;
      let listening = false;

      const fail = (error: SqlError) => {
        if (terminal) return;
        terminal = true;
        Queue.failCauseUnsafe(queue, Cause.fail(error));
      };
      const onNotification = (message: pg.Notification) => {
        if (message.channel === channel) {
          Queue.offerUnsafe(queue, message.payload ?? '');
        }
      };
      const onError = (cause: Error) =>
        fail(
          connectionError(
            cause,
            'Postgres listener connection failed',
            'listen',
          ),
        );
      const onEnd = () =>
        fail(
          connectionError(
            new Error('Postgres listener connection ended'),
            'Postgres listener connection ended',
            'listen',
          ),
        );
      const ignoreError = () => {};

      // Install every terminal/data handler before connecting or issuing LISTEN.
      client.on('notification', onNotification);
      client.on('error', onError);
      client.on('end', onEnd);

      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          const wasTerminal = terminal;
          terminal = true;
          client.off('notification', onNotification);
          client.off('end', onEnd);
          client.off('error', onError);
          client.on('error', ignoreError);

          if (listening && !wasTerminal) {
            try {
              await client.query(`UNLISTEN ${escapeIdentifier(channel)}`);
            } catch {
              // Cleanup cannot recover a failed UNLISTEN; end the client below.
            }
          }
          if (connected) await client.end().catch(() => undefined);
        }).pipe(Effect.timeoutOption('1 second')),
      );

      yield* Effect.tryPromise({
        try: async () => {
          await client.connect();
          connected = true;
          await client.query(`LISTEN ${escapeIdentifier(channel)}`);
          listening = true;
          Queue.offerUnsafe(queue, '');
        },
        catch: (cause) =>
          queryError(cause, 'Failed to establish Postgres listener', 'listen'),
      });
    }),
  );

/**
 * Constructs the official Effect PgClient with corrected rc.109 LISTEN
 * lifecycle semantics. SQL queries and transactions remain the unmodified
 * `@effect/sql-pg` implementation; only `PgClient.listen` is replaced.
 */
export const make = Effect.fnUntraced(function* (
  config: PgClient.PgPoolConfig,
) {
  const client = yield* PgClient.make(config);
  return Object.assign(client, { listen: correctedListen(config) });
});

/** Provides both official `PgClient.PgClient` and generic `SqlClient`. */
export const layer = (
  config: PgClient.PgPoolConfig,
): Layer.Layer<PgClient.PgClient | SqlClient, SqlError, never> =>
  PgClient.layerFrom(make(config));

export * as VesperPgClient from './pg-client.js';
