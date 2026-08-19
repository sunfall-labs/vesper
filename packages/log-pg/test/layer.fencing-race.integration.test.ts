import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, beforeAll, describe, expect, it } from '@effect/vitest';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Deferred,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Redacted,
} from 'effect';

import { VesperPgClient } from '../src/client.js';
import { LogStorePg } from '../src/layer.js';
import {
  createPostgresTestHarness,
  type PostgresTestHarness,
  type ProvisionedTestDatabase,
} from './pg-test-harness.js';

const describeIntegration =
  process.env['RUN_POSTGRES_INTEGRATION'] === '1' ? describe : describe.skip;

// Real two-connection fencing races.
//
// `layer.integration.test.ts` runs the shared `logStoreContract` plus a
// handful of driver-behavior probes, and every fencing case in that contract
// is exercised from one fiber on one connection: acquire, then acquire
// again, then observe the first append fail. That proves the *decision*
// table in `../src/adapter.ts` is right; it proves nothing about the
// `SELECT ... FOR UPDATE` in `../src/layer.ts` that is supposed to make that
// decision safe when two producers reach Postgres at the same instant rather
// than one after the other.
//
// This file races two independent `ManagedRuntime`s — each built from its
// own `VesperPgClient.layer()` call, so each owns its own pool and socket,
// the same shape as the `writer`/`reader` pair in the cross-instance test in
// `layer.integration.test.ts` — through a shared `Deferred` gate, so both
// fibers arrive at their query at once instead of one being scheduled after
// the other resolves.
describeIntegration('LogStore Postgres backend: contended fencing', () => {
  let harness: PostgresTestHarness;
  let database: ProvisionedTestDatabase;
  let writerA: ManagedRuntime.ManagedRuntime<LogStore.Service, unknown>;
  let writerB: ManagedRuntime.ManagedRuntime<LogStore.Service, unknown>;

  beforeAll(async () => {
    harness = await createPostgresTestHarness();
    database = await harness.provisionDatabase({ namePrefix: 'ai_log_race' });
    const connectionLayer = () =>
      LogStorePg.layer().pipe(
        Layer.provide(
          VesperPgClient.layer({
            url: Redacted.make(database.connectionString),
          }),
        ),
        Layer.provide(NodeServices.layer),
      );
    // Two calls, two pools. A single `VesperPgClient.layer()` handed to two
    // `ManagedRuntime.make`s would still be two pools, but calling it twice
    // makes that independence obvious at the call site instead of relying on
    // the reader to know the layer isn't memoized across runtimes.
    writerA = ManagedRuntime.make(connectionLayer());
    writerB = ManagedRuntime.make(connectionLayer());
  }, 180_000);

  afterAll(async () => {
    await writerA?.dispose();
    await writerB?.dispose();
    if (database) await database.cleanup();
    if (harness) await harness.stop();
  }, 120_000);

  /** Run one `LogStore` call to completion on a specific connection. */
  const runOn = <A>(
    runtime: ManagedRuntime.ManagedRuntime<LogStore.Service, unknown>,
    build: (
      log: LogStore.Interface,
    ) => Effect.Effect<A, LogStore.LogStoreError>,
  ): Promise<A> => runtime.runPromise(Effect.flatMap(LogStore.Service, build));

  const text = (value: string): ConversationRecord.Entry => ({
    conversationId: LogVocabulary.ConversationId.make('conversation-1'),
    timestamp: 1_700_000_000_000,
    record: { _tag: 'Text', step: 1, text: value },
  });

  const recordText = (envelope: ConversationRecord.Envelope) =>
    envelope.record._tag === 'Text'
      ? envelope.record.text
      : envelope.record._tag;

  /**
   * Park `effect` on `runtime` behind `gate` and fork it immediately.
   * `Deferred.await` suspends synchronously while the gate is unfulfilled, so
   * calling this for every racing branch before releasing the gate is what
   * guarantees they are all still waiting when release happens — not a
   * `Promise.all` of independently-started work that merely *tends* to
   * overlap. `Effect.result` runs inside the fork, before the fiber can be
   * joined, so a fenced or conflicting outcome comes back as a `Result` to
   * assert on rather than an exception to catch.
   */
  const forkGated = <A>(
    runtime: ManagedRuntime.ManagedRuntime<LogStore.Service, unknown>,
    gate: Deferred.Deferred<void>,
    effect: Effect.Effect<A, LogStore.LogStoreError, LogStore.Service>,
  ) =>
    runtime.runFork(
      Effect.gen(function* () {
        yield* Deferred.await(gate);
        return yield* Effect.result(effect);
      }),
    );

  /** Race one effect per writer connection, released by one shared gate. */
  const race = async <A>(
    effectA: Effect.Effect<A, LogStore.LogStoreError, LogStore.Service>,
    effectB: Effect.Effect<A, LogStore.LogStoreError, LogStore.Service>,
  ) => {
    const gate = await Effect.runPromise(Deferred.make<void>());
    const fiberA = forkGated(writerA, gate, effectA);
    const fiberB = forkGated(writerB, gate, effectB);
    await Effect.runPromise(Deferred.succeed(gate, undefined));
    const [resultA, resultB] = await Promise.all([
      Effect.runPromise(Fiber.join(fiberA)),
      Effect.runPromise(Fiber.join(fiberB)),
    ]);
    return { resultA, resultB };
  };

  const ACQUIRE_RACE_ITERATIONS = 20;

  it(
    'fences the loser when two live connections race acquire, then append',
    { timeout: 120_000 },
    async () => {
      const winners = new Set<'a' | 'b'>();

      for (
        let iteration = 0;
        iteration < ACQUIRE_RACE_ITERATIONS;
        iteration += 1
      ) {
        const path = `race-acquire-${iteration}`;
        const producerA = LogVocabulary.ProducerId.make(
          `writer-a-${iteration}`,
        );
        const producerB = LogVocabulary.ProducerId.make(
          `writer-b-${iteration}`,
        );

        await runOn(writerA, (log) => log.create(path, 'identity'));

        // Legacy (non-CAS) acquire increments the epoch unconditionally, so
        // both connections' acquires succeed — Postgres's row lock decides
        // only *order*, which of the two racing `UPDATE`s applies last, and
        // that is what decides which claim's epoch is current when the
        // append race below runs.
        const acquired = await race(
          Effect.flatMap(LogStore.Service, (log) =>
            log.acquire(path, producerA),
          ),
          Effect.flatMap(LogStore.Service, (log) =>
            log.acquire(path, producerB),
          ),
        );
        if (acquired.resultA._tag !== 'Success') {
          throw new Error(
            `writer A failed to acquire an uncontested stream: ${JSON.stringify(acquired.resultA)}`,
          );
        }
        if (acquired.resultB._tag !== 'Success') {
          throw new Error(
            `writer B failed to acquire an uncontested stream: ${JSON.stringify(acquired.resultB)}`,
          );
        }
        const claimA = acquired.resultA.success;
        const claimB = acquired.resultB.success;
        // Two unconditional epoch increments off the same starting value can
        // never land on the same number; if they did, the row lock failed to
        // serialize the two `UPDATE`s and fencing has no epoch to fence on.
        expect(claimA.epoch).not.toBe(claimB.epoch);

        const aWins = claimA.epoch > claimB.epoch;
        winners.add(aWins ? 'a' : 'b');

        const appendWith = (
          claim: LogStore.ProducerClaim,
          value: string,
        ): Effect.Effect<
          LogOffset.Offset,
          LogStore.LogStoreError,
          LogStore.Service
        > =>
          Effect.flatMap(LogStore.Service, (log) =>
            log.append({
              path,
              producerId: claim.producerId,
              epoch: claim.epoch,
              sequence: LogVocabulary.ProducerSequence.make(0),
              records: [text(value)],
            }),
          );

        // Both attempt to append at once, each still on the connection that
        // acquired its own claim. Only the claim matching the epoch the row
        // now holds may write; the other must be told it is fenced —
        // distinctly from a duplicate write or a silent no-op.
        const appended = await race(
          appendWith(claimA, aWins ? 'winner' : 'loser-should-not-persist'),
          appendWith(claimB, aWins ? 'loser-should-not-persist' : 'winner'),
        );
        const winnerResult = aWins ? appended.resultA : appended.resultB;
        const loserResult = aWins ? appended.resultB : appended.resultA;

        expect(winnerResult._tag).toBe('Success');
        expect(loserResult).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'append', reason: 'fenced' },
        });

        const page = await runOn(writerA, (log) => log.read(path));
        expect(page.records).toHaveLength(1);
        expect(recordText(page.records[0]!)).toBe('winner');
      }

      // Not a correctness assertion on any single iteration — a check that
      // the twenty iterations above actually contended the row rather than
      // one connection deterministically winning every time, which would
      // mean the harness measured scheduling order, not fencing.
      expect(winners.size).toBeGreaterThan(0);
    },
  );

  const INTERLEAVE_RACE_ITERATIONS = 10;

  it(
    'fails the fenced writer under real lock contention while the new epoch appends',
    { timeout: 60_000 },
    async () => {
      for (
        let iteration = 0;
        iteration < INTERLEAVE_RACE_ITERATIONS;
        iteration += 1
      ) {
        const path = `race-interleave-${iteration}`;
        const producerA = LogVocabulary.ProducerId.make(
          `interleave-a-${iteration}`,
        );
        const producerB = LogVocabulary.ProducerId.make(
          `interleave-b-${iteration}`,
        );

        await runOn(writerA, (log) => log.create(path, 'identity'));
        const claimA = await runOn(writerA, (log) =>
          log.acquire(path, producerA),
        );
        const firstOffset = await runOn(writerA, (log) =>
          log.append({
            path,
            producerId: claimA.producerId,
            epoch: claimA.epoch,
            sequence: LogVocabulary.ProducerSequence.make(0),
            records: [text('batch-1')],
          }),
        );

        // B's acquire is sequential, not raced: it is what puts A in the
        // fenced state the concurrent step below is supposed to observe
        // under contention, not what is being raced.
        const claimB = await runOn(writerB, (log) =>
          log.acquire(path, producerB),
        );
        expect(claimB.epoch).toBeGreaterThan(claimA.epoch);

        // A's second batch (stale epoch) and B's first batch (current
        // epoch) enter their transactions at once, on separate connections,
        // both wanting the same row's `FOR UPDATE` lock. A must lose to the
        // epoch check no matter which transaction the lock admits first;
        // this is the scenario a single-fiber fault-injection test cannot
        // reach, because it never has two open transactions contending for
        // that lock simultaneously.
        const raced = await race(
          Effect.flatMap(LogStore.Service, (log) =>
            log.append({
              path,
              producerId: claimA.producerId,
              epoch: claimA.epoch,
              sequence: LogVocabulary.ProducerSequence.make(1),
              records: [text('batch-2-from-fenced-a')],
            }),
          ),
          Effect.flatMap(LogStore.Service, (log) =>
            log.append({
              path,
              producerId: claimB.producerId,
              epoch: claimB.epoch,
              sequence: LogVocabulary.ProducerSequence.make(0),
              records: [text('batch-2-from-b')],
            }),
          ),
        );

        expect(raced.resultA).toMatchObject({
          _tag: 'Failure',
          failure: { operation: 'append', reason: 'fenced' },
        });
        expect(raced.resultB._tag).toBe('Success');

        const page = await runOn(writerA, (log) => log.read(path));
        expect(page.records.map(recordText)).toEqual([
          'batch-1',
          'batch-2-from-b',
        ]);
        // Gapless and ordered: the surviving second record sits immediately
        // after the first, with no hole left by the fenced attempt and
        // nothing out of order.
        expect(page.records[0]?.offset).toBe(firstOffset);
        expect(page.records[0]?.offset).toBe(LogOffset.fromSeq(0n));
        expect(page.records[1]?.offset).toBe(LogOffset.fromSeq(1n));
      }
    },
  );

  const IDEMPOTENT_RACE_ITERATIONS = 20;

  it(
    'converges two connections racing an identical batch on one claim to a single write',
    { timeout: 60_000 },
    async () => {
      // Nothing in `LogStore.ProducerClaim` scopes it to one caller — it is
      // data (`producerId`, `epoch`), not a lease object — so two
      // connections legitimately holding the same claim and racing an
      // identical retry is a real scenario, not a misuse of the interface.
      // The contract's exact-retry case (`log-store-contract.ts`, "returns
      // the original offset for an exact retry") proves this converges
      // sequentially; this proves it converges when both attempts are
      // in-flight against Postgres at once.
      for (
        let iteration = 0;
        iteration < IDEMPOTENT_RACE_ITERATIONS;
        iteration += 1
      ) {
        const path = `race-idempotent-${iteration}`;
        const producer = LogVocabulary.ProducerId.make(
          `idempotent-${iteration}`,
        );

        await runOn(writerA, (log) => log.create(path, 'identity'));
        const claim = await runOn(writerA, (log) =>
          log.acquire(path, producer),
        );

        const appendInput = {
          path,
          producerId: claim.producerId,
          epoch: claim.epoch,
          sequence: LogVocabulary.ProducerSequence.make(0),
          records: [text('same-content'), text('same-content-2')],
        };

        const raced = await race(
          Effect.flatMap(LogStore.Service, (log) => log.append(appendInput)),
          Effect.flatMap(LogStore.Service, (log) => log.append(appendInput)),
        );

        expect(raced.resultA._tag).toBe('Success');
        expect(raced.resultB._tag).toBe('Success');
        if (
          raced.resultA._tag === 'Success' &&
          raced.resultB._tag === 'Success'
        ) {
          // Same offset: the loser landed on the digest-matched retry
          // branch and handed back the winner's offset rather than either
          // erroring or being handed a second, duplicate one.
          expect(raced.resultA.success).toBe(raced.resultB.success);
        }

        const page = await runOn(writerA, (log) => log.read(path));
        expect(page.records).toHaveLength(2);
        expect(page.records.map(recordText)).toEqual([
          'same-content',
          'same-content-2',
        ]);
      }
    },
  );
});
