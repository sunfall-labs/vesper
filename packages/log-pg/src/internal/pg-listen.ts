import type { PgClient } from '@effect/sql-pg';
import { Cause, Duration, Effect, Queue, Redacted, Stream } from 'effect';
import {
  ConnectionError,
  SqlError,
  UnknownError,
} from 'effect/unstable/sql/SqlError';
import { Client } from 'pg';
import type { ClientConfig, Notification } from 'pg';

const escapeIdentifier = (identifier: string): string =>
  `"${identifier.replaceAll('"', '""')}"`;
const ignoreError = (): void => {};

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

const clientConfig = (config: PgClient.PgPoolConfig): ClientConfig => ({
  connectionString: config.url ? Redacted.value(config.url) : undefined,
  user: config.username,
  host: config.host,
  database: config.database,
  password: config.password ? Redacted.value(config.password) : undefined,
  ssl: config.ssl,
  port: config.port,
  ...(config.stream !== undefined ? { stream: config.stream } : {}),
  connectionTimeoutMillis:
    config.connectTimeout !== undefined
      ? Duration.toMillis(Duration.fromInputUnsafe(config.connectTimeout))
      : undefined,
  application_name: config.applicationName ?? '@sunfall/vesper-log-pg-listener',
});

/** The listener lifecycle surface used here, kept small enough to fake fully. */
export interface ListenerClient {
  on(event: 'notification', listener: (message: Notification) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'end', listener: () => void): this;
  off(event: 'notification', listener: (message: Notification) => void): this;
  off(event: 'error', listener: (error: Error) => void): this;
  off(event: 'end', listener: () => void): this;
  connect(): Promise<unknown>;
  query(text: string): Promise<unknown>;
  end(): Promise<void>;
}

/** Internal corrected LISTEN implementation, injectable for lifecycle tests. */
export const correctedListen =
  (config: PgClient.PgPoolConfig, makeClient?: () => ListenerClient) =>
  (channel: string) =>
    Stream.callback<string, SqlError>(
      Effect.fnUntraced(function* (queue) {
        const client: ListenerClient =
          makeClient?.() ?? new Client(clientConfig(config));
        let terminal = false;
        let connectAttempted = false;
        let listening = false;

        const fail = (error: SqlError) => {
          if (terminal) {
            return;
          }
          terminal = true;
          Queue.failCauseUnsafe(queue, Cause.fail(error));
        };
        const onNotification = (message: Notification) => {
          if (message.channel === channel) {
            Queue.offerUnsafe(queue, message.payload ?? '');
          }
        };
        const onError = (cause: Error): void => {
          fail(
            connectionError(
              cause,
              'Postgres listener connection failed',
              'listen',
            ),
          );
        };
        const onEnd = (): void => {
          fail(
            connectionError(
              new Error('Postgres listener connection ended'),
              'Postgres listener connection ended',
              'listen',
            ),
          );
        };
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
            // An attempted connection can own a socket even if connect rejects
            // or is interrupted; end is also how pg cancels a pending attempt.
            if (connectAttempted) {
              await client.end().catch(() => undefined);
            }
          }).pipe(Effect.timeoutOption('1 second')),
        );

        yield* Effect.tryPromise({
          try: async () => {
            connectAttempted = true;
            await client.connect();
            await client.query(`LISTEN ${escapeIdentifier(channel)}`);
            listening = true;
            Queue.offerUnsafe(queue, '');
          },
          catch: (cause) =>
            queryError(
              cause,
              'Failed to establish Postgres listener',
              'listen',
            ),
        }).pipe(
          // Callback setup failures do not close its queue automatically.
          Effect.catchCause((cause) =>
            Effect.sync(() => Queue.failCauseUnsafe(queue, cause)),
          ),
        );
      }),
    );
