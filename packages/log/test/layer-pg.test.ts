import { Effect, Fiber, Stream } from 'effect';
import { describe, expect, it } from 'vitest';

import { LogStorePg } from '../src/layer-pg.js';
import { LogOffset } from '../src/offset.js';

describe('LogStore Postgres pool adapter', () => {
  const fakePool = () => {
    const statements: string[] = [];
    const releases: Array<boolean | undefined> = [];
    const connections: Array<{
      readonly emitNotification: (channel: string) => void;
      readonly emitError: (error: Error) => void;
      readonly emitEnd: () => void;
    }> = [];
    let connectCalls = 0;

    const pool: LogStorePg.PoolLike = {
      async connect() {
        connectCalls += 1;
        const listeners: {
          end?: () => void;
          error?: (error: Error) => void;
          notification?: (message: {
            readonly channel: string;
            readonly payload?: string;
          }) => void;
        } = {};
        const controls = {
          emitNotification: (channel: string) =>
            listeners.notification?.({ channel }),
          emitError: (error: Error) => listeners.error?.(error),
          emitEnd: () => listeners.end?.(),
        };
        connections.push(controls);
        return {
          async query(text: string) {
            statements.push(text);
            return { rows: [] };
          },
          on(event: 'notification' | 'error' | 'end', listener: never) {
            Object.assign(listeners, { [event]: listener });
          },
          release(destroy?: boolean) {
            releases.push(destroy);
          },
        };
      },
      async query() {
        return { rows: [] };
      },
    };

    return {
      pool,
      statements,
      releases,
      connections,
      connectCalls: () => connectCalls,
    };
  };

  it('shares one connection and reference-counts channels', async () => {
    const fake = fakePool();
    const client = LogStorePg.fromPool(fake.pool);

    await Effect.runPromise(
      Effect.scoped(
        Effect.all(
          [client.listen('one'), client.listen('one'), client.listen('two')],
          { concurrency: 'unbounded' },
        ),
      ),
    );

    expect(fake.connectCalls()).toBe(1);
    expect(
      fake.statements.filter((text) => text === 'LISTEN "one"'),
    ).toHaveLength(1);
    expect(
      fake.statements.filter((text) => text === 'LISTEN "two"'),
    ).toHaveLength(1);
    expect(
      fake.statements.filter((text) => text === 'UNLISTEN "one"'),
    ).toHaveLength(1);
    expect(
      fake.statements.filter((text) => text === 'UNLISTEN "two"'),
    ).toHaveLength(1);
    expect(fake.releases).toEqual([true]);
  });

  it('keeps a channel listened until its final subscriber leaves', async () => {
    const fake = fakePool();
    const client = LogStorePg.fromPool(fake.pool);
    let firstReady!: () => void;
    let secondReady!: () => void;
    const firstStarted = new Promise<void>((resolve) => (firstReady = resolve));
    const secondStarted = new Promise<void>(
      (resolve) => (secondReady = resolve),
    );
    const hold = (ready: () => void) =>
      Effect.scoped(
        Effect.gen(function* () {
          yield* client.listen('shared');
          ready();
          return yield* Effect.never;
        }),
      );

    const first = Effect.runFork(hold(firstReady));
    await firstStarted;
    const second = Effect.runFork(hold(secondReady));
    await secondStarted;

    await Effect.runPromise(Fiber.interrupt(first));
    expect(fake.statements).not.toContain('UNLISTEN "shared"');
    expect(fake.releases).toEqual([]);

    await Effect.runPromise(Fiber.interrupt(second));
    expect(
      fake.statements.filter((text) => text === 'UNLISTEN "shared"'),
    ).toHaveLength(1);
    expect(fake.releases).toEqual([true]);
  });

  it('routes notifications only to subscribers of their channel', async () => {
    const fake = fakePool();
    const client = LogStorePg.fromPool(fake.pool);
    let oneReceived!: () => void;
    let twoReceived!: () => void;
    let secondReceived = false;
    const receivedOne = new Promise<void>((resolve) => (oneReceived = resolve));
    const receivedTwo = new Promise<void>((resolve) => (twoReceived = resolve));

    const secondWasWaiting = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const one = yield* client.listen('one');
          const two = yield* client.listen('two');
          const oneFiber = yield* one.pipe(
            Stream.tap(() => Effect.sync(oneReceived)),
            Stream.runDrain,
            Effect.forkChild,
          );
          const twoFiber = yield* two.pipe(
            Stream.tap(() =>
              Effect.sync(() => {
                secondReceived = true;
                twoReceived();
              }),
            ),
            Stream.runDrain,
            Effect.forkChild,
          );
          yield* Effect.yieldNow;
          fake.connections[0]!.emitNotification('one');
          yield* Effect.promise(() => receivedOne);
          const before = !secondReceived;
          fake.connections[0]!.emitNotification('two');
          yield* Effect.promise(() => receivedTwo);
          yield* Fiber.interrupt(oneFiber);
          yield* Fiber.interrupt(twoFiber);
          return before;
        }),
      ),
    );

    expect(secondWasWaiting).toBe(true);
  });

  it('rejects unsafe channel identifiers before taking a connection', async () => {
    const fake = fakePool();
    const outcome = await Effect.runPromise(
      Effect.scoped(
        LogStorePg.fromPool(fake.pool)
          .listen('bad"; NOTIFY injected')
          .pipe(Effect.result),
      ),
    );

    expect(outcome._tag).toBe('Failure');
    expect(fake.connectCalls()).toBe(0);
  });

  it.each(['error', 'end'] as const)(
    'fans out listener %s and reconnects a later subscription',
    async (event) => {
      const fake = fakePool();
      const client = LogStorePg.fromPool(fake.pool);

      const outcomes = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const one = yield* client.listen('one');
            const two = yield* client.listen('two');
            const oneFiber = yield* Stream.runCollect(one).pipe(
              Effect.result,
              Effect.forkChild,
            );
            const twoFiber = yield* Stream.runCollect(two).pipe(
              Effect.result,
              Effect.forkChild,
            );
            yield* Effect.yieldNow;
            yield* Effect.sync(() => {
              if (event === 'error') {
                fake.connections[0]!.emitError(new Error('listener failed'));
              } else {
                fake.connections[0]!.emitEnd();
              }
            });
            return [yield* Fiber.join(oneFiber), yield* Fiber.join(twoFiber)];
          }),
        ),
      );

      expect(outcomes.map((outcome) => outcome._tag)).toEqual([
        'Failure',
        'Failure',
      ]);
      expect(fake.releases).toEqual([true]);

      await Effect.runPromise(Effect.scoped(client.listen('three')));
      expect(fake.connectCalls()).toBe(2);
      expect(fake.statements).toContain('LISTEN "three"');
    },
  );

  it('fails subscribers and releases the session when notifications close', async () => {
    const fake = fakePool();
    const client = LogStorePg.fromPool(fake.pool);
    const outcome = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const stream = yield* client.listen('one');
          const fiber = yield* Stream.runCollect(stream).pipe(
            Effect.result,
            Effect.forkChild,
          );
          yield* client.closeNotifications!;
          return yield* Fiber.join(fiber);
        }),
      ),
    );

    expect(outcome._tag).toBe('Failure');
    expect(fake.releases).toEqual([true]);
  });

  it('uses the existing listener while the normal connection slot is saturated', async () => {
    const fake = fakePool();
    let normalConnectionTaken!: () => void;
    const normalTaken = new Promise<void>((resolve) => {
      normalConnectionTaken = resolve;
    });
    let connectCalls = 0;
    const baseConnect = fake.pool.connect.bind(fake.pool);
    const pool: LogStorePg.PoolLike = {
      ...fake.pool,
      async connect() {
        connectCalls += 1;
        if (connectCalls > 2) return new Promise(() => {});
        const connection = await baseConnect();
        if (connectCalls === 2) normalConnectionTaken();
        return connection;
      },
    };
    const client = LogStorePg.fromPool(pool);
    let listenerReady!: () => void;
    const listenerStarted = new Promise<void>((resolve) => {
      listenerReady = resolve;
    });
    const listener = Effect.runFork(
      Effect.scoped(
        Effect.gen(function* () {
          yield* client.listen('one');
          listenerReady();
          return yield* Effect.never;
        }),
      ),
    );
    await listenerStarted;
    const transaction = Effect.runFork(client.transaction(() => Effect.never));
    await normalTaken;

    await Effect.runPromise(Effect.scoped(client.listen('two')));
    expect(connectCalls).toBe(2);
    expect(fake.statements).toContain('LISTEN "two"');

    await Effect.runPromise(Fiber.interrupt(transaction));
    await Effect.runPromise(Fiber.interrupt(listener));
  });

  it('turns a graceful LISTEN connection end into a feed failure', async () => {
    const listeners: {
      end?: () => void;
      error?: (error: Error) => void;
      notification?: (message: { readonly payload?: string }) => void;
    } = {};
    const connection: LogStorePg.PoolConnectionLike = {
      async query(text) {
        if (text.startsWith('LISTEN')) queueMicrotask(() => listeners.end?.());
        return { rows: [] };
      },
      on(event: 'notification' | 'error' | 'end', listener: never) {
        Object.assign(listeners, { [event]: listener });
      },
      release() {},
    };
    const pool: LogStorePg.PoolLike = {
      async connect() {
        return connection;
      },
      async query() {
        return { rows: [] };
      },
    };

    const outcome = await Effect.runPromise(
      Effect.scoped(
        LogStorePg.fromPool(pool)
          .listen('channel')
          .pipe(
            Effect.flatMap((stream) => Stream.runCollect(stream)),
            Effect.result,
          ),
      ),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure.detail).toContain('ended');
    }
  });

  it('rolls back and releases a transaction after a body failure', async () => {
    const statements: string[] = [];
    let released: boolean | undefined;
    const connection: LogStorePg.PoolConnectionLike = {
      async query(text) {
        statements.push(text);
        if (text === 'body') throw new Error('body failed');
        return { rows: [] };
      },
      on() {},
      release(destroy) {
        released = destroy;
      },
    };
    const pool: LogStorePg.PoolLike = {
      async connect() {
        return connection;
      },
      async query() {
        return { rows: [] };
      },
    };

    const outcome = await Effect.runPromise(
      LogStorePg.fromPool(pool)
        .transaction((tx) => tx.query('body'))
        .pipe(Effect.result),
    );

    expect(outcome._tag).toBe('Failure');
    expect(statements).toEqual([
      'BEGIN; SET LOCAL statement_timeout = 30000',
      'body',
      'ROLLBACK',
    ]);
    expect(released).toBe(false);
  });

  it('rolls back an interrupted transaction before releasing its connection', async () => {
    const statements: string[] = [];
    let released: boolean | undefined;
    let bodyStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      bodyStarted = resolve;
    });
    const connection: LogStorePg.PoolConnectionLike = {
      async query(text) {
        statements.push(text);
        return { rows: [] };
      },
      on() {},
      release(destroy) {
        released = destroy;
      },
    };
    const pool: LogStorePg.PoolLike = {
      async connect() {
        return connection;
      },
      async query() {
        return { rows: [] };
      },
    };

    const fiber = Effect.runFork(
      LogStorePg.fromPool(pool).transaction(() =>
        Effect.gen(function* () {
          bodyStarted();
          return yield* Effect.never;
        }),
      ),
    );
    await started;
    await Effect.runPromise(Fiber.interrupt(fiber));

    expect(statements).toEqual([
      'BEGIN; SET LOCAL statement_timeout = 30000',
      'ROLLBACK',
    ]);
    expect(released).toBe(false);
  });

  it('releases a scoped LISTEN connection so a saturated pool can progress', async () => {
    type Waiter = (connection: LogStorePg.PoolConnectionLike) => void;
    const waiters: Waiter[] = [];
    const statements: string[] = [];
    let checkedOut = false;
    let listenReady!: () => void;
    const listening = new Promise<void>((resolve) => {
      listenReady = resolve;
    });

    const connection = (): LogStorePg.PoolConnectionLike => ({
      async query(text) {
        statements.push(text);
        if (text.startsWith('LISTEN')) listenReady();
        return { rows: [] };
      },
      on() {},
      release() {
        const waiter = waiters.shift();
        if (waiter === undefined) checkedOut = false;
        else waiter(connection());
      },
    });
    const pool: LogStorePg.PoolLike = {
      connect() {
        if (!checkedOut) {
          checkedOut = true;
          return Promise.resolve(connection());
        }
        return new Promise((resolve) => waiters.push(resolve));
      },
      async query() {
        return { rows: [] };
      },
    };
    const client = LogStorePg.fromPool(pool);
    const listener = Effect.runFork(
      Effect.scoped(
        Effect.gen(function* () {
          yield* client.listen('channel');
          return yield* Effect.never;
        }),
      ),
    );
    await listening;

    const transaction = Effect.runPromise(
      client.transaction(() => Effect.succeed('completed')),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(statements).toEqual(['LISTEN "channel"']);

    await Effect.runPromise(Fiber.interrupt(listener));
    await expect(transaction).resolves.toBe('completed');
    expect(statements.slice(1)).toEqual([
      'UNLISTEN "channel"',
      'BEGIN; SET LOCAL statement_timeout = 30000',
      'COMMIT',
    ]);
  });
});

describe('LogStore Postgres append statement', () => {
  it('uses one fixed-parameter recordset statement for the whole batch', async () => {
    const statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }> = [];
    const client: LogStorePg.Client = {
      query: () => Effect.succeed([]),
      transaction: (body) =>
        body({
          query: (text, params = []) => {
            statements.push({ text, params });
            if (text.includes('FOR UPDATE')) {
              return Effect.succeed([
                {
                  identity: 'identity',
                  epoch: 1,
                  producer_id: 'producer',
                  next_sequence: '0',
                  next_producer_sequence: '0',
                  last_fingerprint: '',
                  last_offset: '-1',
                },
              ]);
            }
            return Effect.succeed([]);
          },
        }),
      listen: () => Effect.succeed(Stream.empty),
    };
    const records = Array.from({ length: 1_000 }, (_, index) => ({
      conversationId: 'conversation',
      timestamp: 1_700_000_000_000 + index,
      record: {
        _tag: 'ToolCall' as const,
        step: index,
        id: `call-${index}`,
        name: 'tool',
        params: { z: index, a: index },
      },
    }));

    await Effect.runPromise(
      LogStorePg.make(client).append({
        path: 'large',
        producerId: 'producer',
        epoch: 1,
        sequence: 0,
        records,
      }),
    );

    expect(statements).toHaveLength(2);
    const write = statements[1]!;
    expect(write.text).toContain('jsonb_array_elements($6::jsonb)');
    expect(write.text).toContain('inserted AS');
    expect(write.text).toContain('advanced AS');
    expect(write.text).toContain('pg_notify($11, $12)');
    expect(write.params).toHaveLength(12);
    const encoded = JSON.parse(String(write.params[5])) as Array<{
      record: { params: Record<string, number> };
    }>;
    expect(encoded).toHaveLength(1_000);
    expect(Object.keys(encoded[0]!.record.params)).toEqual(['a', 'z']);
  });
});

describe('LogStore Postgres acquire statement', () => {
  const clientFor = (
    statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }>,
  ): LogStorePg.Client => ({
    query: (text, params = []) => {
      statements.push({ text, params });
      return Effect.succeed([{ epoch: '4' }]);
    },
    transaction: (body) => body({ query: () => Effect.succeed([]) }),
    listen: () => Effect.succeed(Stream.empty),
  });

  it('binds expected epoch and head into the atomic epoch bump', async () => {
    const statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }> = [];

    const claim = await Effect.runPromise(
      LogStorePg.make(clientFor(statements)).acquire('stream', 'producer', {
        epoch: 3,
        head: LogOffset.Offset.make('0000000000000000_0000000000000007'),
      }),
    );

    expect(claim.epoch).toBe(4);
    expect(statements).toHaveLength(1);
    expect(statements[0]!.text).toContain('SET epoch = epoch + 1');
    expect(statements[0]!.text).toContain('epoch = $3');
    expect(statements[0]!.text).toContain('last_offset = $4');
    expect(statements[0]!.params).toEqual([
      'stream',
      'producer',
      3,
      '0000000000000000_0000000000000007',
    ]);
  });

  it('leaves the expected predicates disabled for legacy acquire', async () => {
    const statements: Array<{
      readonly text: string;
      readonly params: ReadonlyArray<unknown>;
    }> = [];

    await Effect.runPromise(
      LogStorePg.make(clientFor(statements)).acquire('stream', 'producer'),
    );

    expect(statements).toHaveLength(1);
    expect(statements[0]!.params).toEqual(['stream', 'producer', null, null]);
  });
});
