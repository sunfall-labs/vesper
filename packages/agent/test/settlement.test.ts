import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Ref, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { protocolOf } from '../src/internal.js';
import { AgentLog } from '../src/log.js';
import { RunPolicy } from '../src/run-policy.js';

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

const stallsSettlement = (interrupted: Ref.Ref<boolean>) =>
  Layer.effect(
    LogStore.Service,
    Effect.map(LogStore.Service, (store) =>
      LogStore.Service.of({
        ...store,
        append: (input) =>
          input.records.some(({ record }) => record._tag === 'RunSettled')
            ? Effect.never.pipe(
                Effect.onInterrupt(() => Ref.set(interrupted, true)),
              )
            : store.append(input),
      }),
    ),
  ).pipe(Layer.provide(LogStoreMemory.layer));

const CONVERSATION = LogVocabulary.ConversationId.make('settling-conversation');

const agent = Agent.make({
  name: 'test',
  revision: '1',
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
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(model),
    Effect.provide(LogStoreMemory.layer),
  );

const runInSession = <R>(
  child: Agent.Named<string, R>,
  session: AgentLog.Session,
  input: string,
) =>
  Effect.flatMap(RunPolicy.create(RunPolicy.defaultLimits), (runtime) =>
    protocolOf<R>(child)!.run(runtime, session, input),
  );

describe('how a run settles', () => {
  it.effect('records a completed run as a success, with its totals', () =>
    Effect.gen(function* () {
      const written = yield* run(
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
          resume: {
            formatVersion: 1,
            agent: 'test',
            agentRevision: LogVocabulary.AgentRevision.make('1'),
            usage: { input: 5, output: 2 },
            signalCursor: LogOffset.START,
            completed: {
              outcome: 'success',
              text: 'done',
              steps: 1,
              usage: { input: 5, output: 2 },
            },
            latestTurnUsage: { input: 5, output: 2 },
          },
        },
      ]);
      // Last, so a reader tailing the log knows nothing more is coming.
      expect(written.at(-1)?.record._tag).toBe('RunSettled');
    }),
  );

  it.effect('records a failed run, and does not swallow the failure', () =>
    Effect.gen(function* () {
      const observed = yield* Effect.gen(function* () {
        const outcome = yield* agent
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.result);
        return { outcome, written: yield* readAll() };
      }).pipe(
        Effect.orDie,
        Effect.provide(failing),
        Effect.provide(LogStoreMemory.layer),
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
    }),
  );

  it.effect('records an interrupted run as interrupted', () =>
    Effect.gen(function* () {
      const written = yield* Effect.gen(function* () {
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
      }).pipe(Effect.orDie, Effect.provide(LogStoreMemory.layer));

      expect(settlement(written)).toMatchObject([{ outcome: 'interrupted' }]);
    }),
  );

  it.effect(
    'records an abandoned event stream as interrupted, not a success',
    () =>
      Effect.gen(function* () {
        const written = yield* run(
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
      }),
  );

  it.live(
    'bounds a stalled settlement append and leaves an orphan',
    () =>
      Effect.gen(function* () {
        const interrupted = yield* Ref.make(false);
        const observed = yield* Effect.gen(function* () {
          const session = yield* AgentLog.open(CONVERSATION, {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          });
          const stalled: AgentLog.Session = {
            ...session,
            settlementTimeoutMillis: 10,
          };

          const result = yield* runInSession(agent, stalled, 'hi').pipe(
            Effect.timeout(1_000),
            Effect.orDie,
          );
          return {
            result,
            written: yield* session.recorded,
            interrupted: yield* Ref.get(interrupted),
          };
        }).pipe(
          Effect.provide(answering),
          Effect.provide(stallsSettlement(interrupted)),
        );

        expect(observed.result.text).toBe('done');
        expect(observed.interrupted).toBe(true);
        expect(settlement(observed.written)).toEqual([]);
        expect(observed.written.map(({ record }) => record._tag)).toContain(
          'Completed',
        );
      }),
    10_000,
  );
});

describe('orphans', () => {
  it.effect('are a RunStarted with nothing after it', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          const store = yield* LogStore.Service;
          const path = AgentLog.pathFor(CONVERSATION);
          yield* store.create(path, CONVERSATION).pipe(Effect.orDie);
          const claim = yield* store
            .acquire(path, LogVocabulary.ProducerId.make('dead-run'))
            .pipe(Effect.orDie);
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
                  record: {
                    _tag: 'RunStarted',
                    agent: 'test',
                    formatVersion: 1,
                    agentRevision: LogVocabulary.AgentRevision.make('1'),
                    prompt: [],
                  },
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
    }),
  );
});
