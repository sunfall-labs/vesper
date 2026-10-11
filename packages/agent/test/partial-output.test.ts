import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Fiber, Layer, Stream } from 'effect';
import { LanguageModel, type Response } from 'effect/ai';
import { TestClock } from 'effect/testing';

import { Conversation } from '../src/conversation.js';
import { AgentHistory } from '../src/history.js';
import {
  finish,
  plainAgent,
  provider,
  says,
  tags,
  testLogLayer,
} from './durability-fixtures.js';

// Streamed output is committed before its turn settles, so a crash mid-answer
// loses at most one interval of it instead of the whole answer. What has to
// hold: the committed partial is readable as aborted output, the rerun turn is
// never shown it, and a turn that does finish records exactly what it recorded
// before progress existed.

const delta = (text: string): Response.StreamPartEncoded => ({
  type: 'text-delta',
  id: 'a',
  delta: text,
});

/** Drops the settlement append, which is what a killed process never writes. */
const diesBeforeSettling = Layer.effect(
  LogStore.Service,
  Effect.map(LogStore.Service, (store) =>
    LogStore.Service.of({
      ...store,
      append: (input) =>
        input.records.some(({ record }) => record._tag === 'RunSettled')
          ? Effect.fail(
              LogStore.makeError(
                input.path,
                'append',
                'storage',
                'process died',
              ),
            )
          : store.append(input),
    }),
  ),
).pipe(Layer.provide(testLogLayer));

/** A model whose stream is the given parts, with an `Effect` step between. */
const streaming = (parts: Stream.Stream<Response.StreamPartEncoded>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () => parts,
    }),
  );

const afterInterval = (part: Response.StreamPartEncoded) =>
  Stream.fromEffect(Effect.sleep('250 millis').pipe(Effect.as(part)));

const collect = (conversation: {
  readonly records: () => Stream.Stream<
    ConversationRecord.Envelope,
    LogStore.LogStoreError,
    LogStore.Service
  >;
}) =>
  Stream.runCollect(conversation.records()).pipe(
    Effect.map(
      (records): ReadonlyArray<ConversationRecord.Envelope> =>
        Array.from(records),
    ),
  );

const json = (value: unknown): string => JSON.stringify(value);

// A run's budget id is minted fresh per run, so two otherwise identical
// runs differ there and nowhere else.
const strip = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records
    .map(({ record }) =>
      record._tag === 'RunStarted'
        ? { ...record, budget: { ...record.budget, id: '', root: '' } }
        : record,
    )
    .filter((record) => record._tag !== 'PartialOutput');

describe('partial output', () => {
  // Reasoning and `Hel` stream, the interval passes, `lo` and then ` wor`
  // arrive, and the process dies. `Hello` was committed at `lo`; ` wor` landed
  // inside a fresh window and is what a crash loses.
  it.effect(
    'survives a crash mid-answer and stays out of the rerun prompt',
    () =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();
        const paused = yield* Deferred.make<void>();
        const stalled = streaming(
          Stream.fromIterable<Response.StreamPartEncoded>([
            { type: 'reasoning-start', id: 'r' },
            { type: 'reasoning-delta', id: 'r', delta: 'thinking' },
            { type: 'text-start', id: 'a' },
            delta('Hel'),
          ]).pipe(
            Stream.concat(
              Stream.fromEffect(
                Deferred.succeed(paused, undefined).pipe(
                  Effect.andThen(Effect.sleep('250 millis')),
                  Effect.as(delta('lo')),
                ),
              ),
            ),
            Stream.concat(Stream.make(delta(' wor'))),
            Stream.concat(
              Stream.fromEffect(
                Deferred.succeed(reached, undefined).pipe(
                  Effect.andThen(Effect.never),
                ),
              ),
            ),
          ),
        );
        const conversation = Conversation.make(plainAgent, 'crashing');

        const crashed = yield* conversation
          .run('hi')
          .pipe(
            Effect.orDie,
            Effect.provide(Layer.merge(stalled, diesBeforeSettling)),
            Effect.forkChild,
          );
        yield* Deferred.await(paused);
        yield* TestClock.adjust('250 millis');
        yield* Deferred.await(reached);
        yield* Fiber.interrupt(crashed);

        const afterCrash = yield* collect(conversation);
        expect(tags(afterCrash)).toEqual([
          'RunStarted',
          'PartialOutput',
          'PartialOutput',
        ]);
        expect(AgentHistory.abortedOutputFrom(afterCrash)).toMatchObject([
          { text: 'Hello', reasoning: 'thinking' },
        ]);
        // Never part of what a model is shown.
        expect(json(AgentHistory.messagesFrom(afterCrash))).not.toContain(
          'Hello',
        );

        const models = provider([says('fresh answer')]);
        const rerun = yield* conversation
          .run('again')
          .pipe(Effect.orDie, Effect.provide(models.layer));
        expect(rerun.text).toBe('fresh answer');
        expect(json(models.asked)).not.toContain('Hello');
        expect(json(models.asked)).not.toContain('thinking');

        // The crashed attempt stays on record as aborted, and is still not history.
        const afterRerun = yield* collect(conversation);
        expect(AgentHistory.abortedOutputFrom(afterRerun)).toMatchObject([
          { text: 'Hello', reasoning: 'thinking' },
        ]);
        expect(json(AgentHistory.messagesFrom(afterRerun))).not.toContain(
          'Hello',
        );
      }).pipe(Effect.provide(testLogLayer)),
  );

  it.effect('records a finished turn exactly as it did without progress', () =>
    Effect.gen(function* () {
      const withProgress = Conversation.make(plainAgent, 'with-progress');
      const without = Conversation.make(plainAgent, 'without-progress', {
        partialOutput: false,
      });

      const forked = yield* Effect.all([
        withProgress.run('hi').pipe(Effect.orDie, Effect.forkChild),
        without.run('hi').pipe(Effect.orDie, Effect.forkChild),
      ]);
      // The sleeps are registered by fibers that may not have run yet, so keep
      // the clock moving until both runs have finished.
      yield* TestClock.adjust('50 millis').pipe(
        Effect.andThen(Effect.yieldNow),
        Effect.forever,
        Effect.forkChild,
      );
      yield* Effect.forEach(forked, Fiber.join);

      const a = yield* collect(withProgress);
      const b = yield* collect(without);
      expect(tags(a)).toContain('PartialOutput');
      expect(tags(b)).not.toContain('PartialOutput');
      expect(strip(a)).toEqual(strip(b));
      expect(AgentHistory.messagesFrom(a)).toEqual(
        AgentHistory.messagesFrom(b),
      );
      expect(AgentHistory.abortedOutputFrom(a)).toEqual([]);
    }).pipe(
      Effect.provide(
        Layer.merge(
          streaming(
            Stream.fromIterable<Response.StreamPartEncoded>([
              { type: 'text-start', id: 'a' },
              delta('Hel'),
            ]).pipe(
              Stream.concat(afterInterval(delta('lo'))),
              Stream.concat(
                Stream.make<Array<Response.StreamPartEncoded>>(
                  { type: 'text-end', id: 'a' },
                  finish(),
                ),
              ),
            ),
          ),
          testLogLayer,
        ),
      ),
    ),
  );

  it.effect('writes nothing extra for a turn that streams quickly', () =>
    Effect.gen(function* () {
      const conversation = Conversation.make(plainAgent, 'quick');
      yield* conversation.run('hi').pipe(Effect.orDie);
      expect(tags(yield* collect(conversation))).not.toContain('PartialOutput');
    }).pipe(
      Effect.provide(
        Layer.merge(provider([says('quick')]).layer, testLogLayer),
      ),
    ),
  );
});
