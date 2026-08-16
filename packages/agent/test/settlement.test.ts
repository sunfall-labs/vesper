import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Deferred, Effect, Fiber, Layer, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { AgentLog } from '../src/log.js';

// Settlement: the durable half of "how did this end".
//
// `Exit` covers the in-process half and does not outlive the process that
// held it. Without a record, a conversation whose pod died and one that is
// still thinking look identical from the outside — records simply stop
// arriving — and it is that distinction the resuming tool dispatch is gated
// on.
//
// What these have to prove: every way a run can end is written down, and the
// one way it cannot be written down leaves the orphan shape a reader is told
// to look for.

const finish = () => ({
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 5, uncached: 5, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

const answer: Response.StreamPartEncoded[] = [
  { type: 'text-start' as const, id: 'a' },
  { type: 'text-delta' as const, id: 'a', delta: 'done' },
  { type: 'text-end' as const, id: 'a' },
  finish(),
];

const answering = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
    streamText: () => Stream.fromIterable(answer),
  }),
);

const failing = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
    streamText: () =>
      Stream.fail(
        AiError.make({
          module: 'test',
          method: 'streamText',
          reason: new AiError.InternalProviderError({
            description: 'the provider fell over',
          }),
        }),
      ),
  }),
);

/** A model that announces it was reached and then never answers. */
const blocking = (reached: Deferred.Deferred<void>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* Deferred.succeed(reached, undefined);
            yield* Effect.never;
            return Stream.fromIterable(answer);
          }),
        ),
    }),
  );

const CONVERSATION = 'settling-conversation';

const agent = Agent.make({
  name: 'test',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

const readAll = Effect.fn('test.readAll')(function* () {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(AgentLog.pathFor(CONVERSATION), { limit: 1000 })
    .pipe(Effect.orDie);
  return page.records;
});

const settlement = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'RunSettled' ? [envelope.record] : [],
  );

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  model: Layer.Layer<LanguageModel.LanguageModel> = answering,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.orDie,
      Effect.provide(model),
      Effect.provide(LogStoreMemory.layer),
      Effect.scoped,
    ),
  );

describe('how a run settles', () => {
  it('records a completed run as a success, with its totals', async () => {
    const written = await run(
      Effect.gen(function* () {
        yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    expect(settlement(written)).toEqual([
      {
        _tag: 'RunSettled',
        outcome: 'success',
        detail: '',
        steps: 1,
        usage: { input: 5, output: 2 },
      },
    ]);
    // Last, so a reader tailing the log knows nothing more is coming.
    expect(written.at(-1)?.record._tag).toBe('RunSettled');
  });

  it('records a failed run, and does not swallow the failure', async () => {
    const observed = await Effect.runPromise(
      Effect.gen(function* () {
        const outcome = yield* agent
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.result);
        return { outcome, written: yield* readAll() };
      }).pipe(
        Effect.orDie,
        Effect.provide(failing),
        Effect.provide(LogStoreMemory.layer),
        Effect.scoped,
      ),
    );

    expect(observed.outcome._tag).toBe('Failure');
    expect(settlement(observed.written)).toMatchObject([
      { outcome: 'failure' },
    ]);
    // The cause is rendered into the record rather than reduced to a flag,
    // because "it failed" is not something anyone can act on later.
    expect(settlement(observed.written)[0]?.detail).toContain(
      'the provider fell over',
    );
  });

  it('records an interrupted run as interrupted', async () => {
    const written = await Effect.runPromise(
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();

        const running = yield* agent
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(
            Effect.orDie,
            Effect.provide(blocking(reached)),
            Effect.forkChild,
          );

        // Waiting on the model rather than sleeping: by the time it reports
        // being reached the session is open and `RunStarted` is written, so
        // the interruption lands on a run that has genuinely begun.
        yield* Deferred.await(reached);
        yield* Fiber.interrupt(running);

        return yield* readAll();
      }).pipe(
        Effect.orDie,
        Effect.provide(LogStoreMemory.layer),
        Effect.scoped,
      ),
    );

    expect(settlement(written)).toMatchObject([{ outcome: 'interrupted' }]);
  });

  it('records an abandoned event stream as interrupted, not a success', async () => {
    const written = await run(
      Effect.gen(function* () {
        // A consumer that takes what it wants and walks away. The run did not
        // finish, and saying "success" here would claim a result nobody got.
        yield* agent
          .recordingTo(CONVERSATION)
          .stream('hi')
          .pipe(Stream.take(2), Stream.runDrain, Effect.orDie);

        return yield* readAll();
      }),
    );

    expect(settlement(written)).toMatchObject([
      {
        outcome: 'interrupted',
        detail: expect.stringContaining('abandoned'),
      },
    ]);
  });
});

describe('orphans', () => {
  it('are a RunStarted with nothing after it', async () => {
    const written = await run(
      Effect.gen(function* () {
        const store = yield* LogStore.Service;
        const path = AgentLog.pathFor(CONVERSATION);
        yield* store.create(path, CONVERSATION).pipe(Effect.orDie);
        const claim = yield* store.acquire(path, 'dead-run').pipe(Effect.orDie);
        yield* store
          .append({
            path,
            producerId: claim.producerId,
            epoch: claim.epoch,
            sequence: claim.nextSequence,
            records: [
              {
                conversationId: CONVERSATION,
                timestamp: 1_700_000_000_000,
                record: { _tag: 'RunStarted', agent: 'test', prompt: [] },
              },
            ],
          })
          .pipe(Effect.orDie);

        return yield* readAll();
      }),
    );

    // Stated as a test because it is the shape every reader of this log is
    // told to look for, and because it is what a settle-time write failure
    // leaves behind — the absence is the signal, so it has to be legible.
    expect(written.map((envelope) => envelope.record._tag)).toEqual([
      'RunStarted',
    ]);
    expect(settlement(written)).toEqual([]);
  });
});
