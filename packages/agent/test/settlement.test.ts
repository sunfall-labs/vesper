import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Ref, Stream } from 'effect';
import { AiError, LanguageModel, type Response } from 'effect/ai';
import { TestClock } from 'effect/testing';

import { Conversation } from '../src/conversation.js';
import * as AgentLog from '../src/log.js';
import {
  callsLookup,
  finish,
  lookupAgent,
  plainAgent,
  provider,
  readRecords,
  run,
  says,
  tags,
  testLogLayer,
} from './durability-fixtures.js';

// Settlement is the durable half of "how did this end". `Exit` covers the
// in-process half and does not outlive the process that held it: without a
// record, a conversation whose pod died and one that is still thinking look
// identical from outside, and it is that distinction resuming tool dispatch is
// gated on. Every way a run can end is written down; the one way it cannot be
// leaves the orphan shape (no `RunSettled`) that readers are told to look for.

const ID = 'settling';
const settlements = (records: Effect.Success<ReturnType<typeof readRecords>>) =>
  records.flatMap(({ record }) =>
    record._tag === 'RunSettled' ? [record] : [],
  );

const modelFailing = (description: string) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () =>
        Stream.fail(
          AiError.make({
            module: 'test',
            method: 'streamText',
            reason: new AiError.InternalProviderError({ description }),
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
          Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Effect.never),
          ),
        ),
    }),
  );

describe('how a run settles', () => {
  it.effect('records a completed run as a success, with its totals', () =>
    Effect.gen(function* () {
      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(plainAgent, ID).run('hi');
          return yield* readRecords(ID);
        }),
        provider([says('done')]).layer,
      );

      expect(settlements(written)).toEqual([
        {
          _tag: 'RunSettled',
          outcome: 'success',
          detail: '',
          steps: 1,
          usage: { input: 10, output: 4 },
          resume: {
            formatVersion: 1,
            agent: 'test',
            agentRevision: '1',
            usage: { input: 10, output: 4 },
            signalCursor: LogOffset.START,
            completed: {
              outcome: 'success',
              text: 'done',
              steps: 1,
              usage: { input: 10, output: 4 },
              response: {
                content: [{ role: 'assistant', content: 'done', options: {} }],
              },
            },
            latestTurnUsage: { input: 10, output: 4 },
          },
        },
      ]);
      // Last, so a reader tailing the log knows nothing more is coming.
      expect(written.at(-1)?.record._tag).toBe('RunSettled');
    }),
  );

  it.effect('records a failed run, and does not swallow the failure', () =>
    Effect.gen(function* () {
      const observed = yield* run(
        Effect.gen(function* () {
          const outcome = yield* Conversation.make(plainAgent, ID)
            .run('hi')
            .pipe(Effect.result);
          return { outcome, written: yield* readRecords(ID) };
        }),
        modelFailing('the provider fell over'),
      );

      expect(observed.outcome._tag).toBe('Failure');
      expect(settlements(observed.written)).toMatchObject([
        { outcome: 'failure' },
      ]);
      // The cause is rendered into the record rather than reduced to a flag,
      // because "it failed" is not something anyone can act on later.
      expect(settlements(observed.written)[0]?.detail).toContain(
        'the provider fell over',
      );
    }),
  );

  it.effect('records an interrupted run as interrupted', () =>
    Effect.gen(function* () {
      const reached = yield* Deferred.make<void>();
      const written = yield* run(
        Effect.gen(function* () {
          const running = yield* Conversation.make(plainAgent, ID)
            .run('hi')
            .pipe(Effect.forkChild);
          // Waiting on the model rather than sleeping: by the time it reports
          // being reached `RunStarted` is written, so the interruption lands
          // on a run that has genuinely begun.
          yield* Deferred.await(reached);
          yield* Fiber.interrupt(running);
          return yield* readRecords(ID);
        }),
        blocking(reached),
      );

      expect(settlements(written)).toMatchObject([{ outcome: 'interrupted' }]);
    }),
  );

  it.effect(
    'records an abandoned event stream as interrupted, not a success',
    () =>
      Effect.gen(function* () {
        const written = yield* run(
          Effect.gen(function* () {
            // A consumer that takes what it wants and walks away. The run did
            // not finish, and saying "success" would claim a result nobody got.
            yield* Conversation.make(plainAgent, ID)
              .stream('hi')
              .pipe(Stream.take(2), Stream.runDrain);
            return yield* readRecords(ID);
          }),
          provider([says('done')]).layer,
        );

        expect(settlements(written)).toMatchObject([
          {
            outcome: 'interrupted',
            detail: expect.stringContaining('abandoned'),
          },
        ]);
      }),
  );

  // A store that stalls the settlement append must not hold the caller
  // hostage: the append is abandoned after a bound and the run is left
  // orphaned, which is exactly the shape a crash leaves.
  it.effect('bounds a stalled settlement append and leaves an orphan', () =>
    Effect.gen(function* () {
      const interrupted = yield* Ref.make(false);
      const stalled = yield* Deferred.make<void>();
      const stalls = Layer.effect(
        LogStore.Service,
        Effect.map(LogStore.Service, (store) =>
          LogStore.Service.of({
            ...store,
            append: (input) =>
              input.records.some(({ record }) => record._tag === 'RunSettled')
                ? Deferred.succeed(stalled, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.onInterrupt(() => Ref.set(interrupted, true)),
                  )
                : store.append(input),
          }),
        ),
      ).pipe(Layer.provide(testLogLayer));

      const result = yield* Effect.gen(function* () {
        const running = yield* Conversation.make(plainAgent, ID)
          .run('hi')
          .pipe(Effect.forkChild);
        yield* Deferred.await(stalled);
        yield* TestClock.adjust(AgentLog.SETTLEMENT_TIMEOUT_MILLIS);
        return yield* Fiber.join(running);
      }).pipe(
        Effect.provide(Layer.merge(provider([says('done')]).layer, stalls)),
      );

      expect(result.text).toBe('done');
      expect(yield* Ref.get(interrupted)).toBe(true);
    }).pipe(Effect.provide(testLogLayer)),
  );
});

const truncated = (reason: Response.FinishReason) => [
  ...says('the answer was cut').slice(0, -1),
  finish(reason),
];

// A turn the provider cut off at its output cap is not an answer. The raw
// finish part stays visible to a streaming observer, but the turn must not
// cross the loop's completion interface as a success, and no `Completed`
// record may claim the fragment is complete.
describe('a turn the provider truncated', () => {
  it.effect('is visible in the live event stream, on the raw finish part', () =>
    Effect.gen(function* () {
      const reasons = yield* run(
        plainAgent.stream('hi').pipe(
          // The stream then fails on the incomplete finish; stop at the part.
          Stream.takeUntil(
            (event) => event._tag === 'Part' && event.part.type === 'finish',
          ),
          Stream.flatMap((event) =>
            event._tag === 'Part' && event.part.type === 'finish'
              ? Stream.make(event.part.reason)
              : Stream.empty,
          ),
          Stream.runCollect,
        ),
        provider([truncated('length')]).layer,
      );

      expect(Array.from(reasons)).toEqual(['length']);
    }),
  );

  it.effect.each([
    ['length', 'generation reached its output token limit'],
    ['content-filter', 'generation was stopped by the provider content filter'],
    ['error', 'the provider reported a generation error'],
  ] as const)(
    'fails instead of returning a partial %s answer',
    ([reason, why]) =>
      Effect.gen(function* () {
        const error = yield* plainAgent
          .run('hi')
          .pipe(
            Effect.provide(provider([truncated(reason)]).layer),
            Effect.flip,
          );

        expect(AiError.isAiError(error)).toBe(true);
        expect(error).toMatchObject({
          reason: {
            _tag: 'InvalidOutputError',
            description: `Model output was incomplete because ${why}`,
            metadata: { finishReason: reason },
          },
        });
      }),
  );

  it.effect(
    'settles as failed, without claiming the fragment is complete',
    () =>
      Effect.gen(function* () {
        const observe = (reason: Response.FinishReason) =>
          run(
            Effect.gen(function* () {
              const exit = yield* Conversation.make(plainAgent, ID)
                .run('hi')
                .pipe(Effect.exit);
              return { exit, written: yield* readRecords(ID) };
            }),
            provider([reason === 'stop' ? says('done') : truncated(reason)])
              .layer,
          );
        const cut = yield* observe('length');
        const whole = yield* observe('stop');

        expect(cut.exit._tag).toBe('Failure');
        expect(tags(cut.written)).toEqual([
          'RunStarted',
          'Text',
          'TurnFinished',
          'RunSettled',
        ]);
        expect(settlements(cut.written)).toMatchObject([
          { outcome: 'failure' },
        ]);
        expect(whole.exit._tag).toBe('Success');
        expect(tags(whole.written)).toContain('Completed');
      }),
  );

  // The agent runs a turn's calls only after the response that made them has
  // finished, and a response the provider cut off is not resolved. So nothing
  // about the call is left to guess at: its handler never ran, and a
  // synthesized `execution-interrupted` answer never becomes a durable outcome
  // for a call that might have been entered.
  it.effect('never starts a call from a truncated response', () =>
    Effect.gen(function* () {
      const { agent, ran } = lookupAgent();
      const cut = [...callsLookup().slice(0, -1), finish('length')];
      const observed = yield* run(
        Effect.gen(function* () {
          const exit = yield* Conversation.make(agent, ID)
            .run('look it up')
            .pipe(Effect.exit);
          return { exit, written: yield* readRecords(ID) };
        }),
        provider([cut]).layer,
      );

      expect(observed.exit._tag).toBe('Failure');
      expect(ran.count).toBe(0);
      expect(tags(observed.written)).toContain('StepOpened');
      expect(tags(observed.written)).not.toContain('StepStarted');
      expect(tags(observed.written)).not.toContain('StepSettled');
      expect(observed.written.at(-1)?.record).toMatchObject({
        _tag: 'RunSettled',
        outcome: 'failure',
      });
    }),
  );
});
