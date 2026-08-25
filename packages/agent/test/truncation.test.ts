import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { type Crypto, Effect, Layer, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import * as AgentLog from '../src/log.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

// A turn the provider cut off at the output cap, rather than one the model
// chose to end.
//
// Completion integrity for a turn the provider cut off at its output cap.
//
// The raw finish part remains visible to a streaming observer, but the turn
// must not cross the loop's completion interface as a successful answer. The
// partial text remains an audit fact in a recorded conversation; settlement
// says the run failed and no `Completed` record claims the fragment is an
// answer.

const finish = (reason: Response.FinishReason) => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 7, uncached: 7, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 3 },
  },
});

const ANSWER = 'the answer was cut';

const scripted = (reason: Response.FinishReason) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () =>
        Effect.succeed<Response.PartEncoded[]>([finish(reason)]),
      streamText: () =>
        Stream.fromIterable<Response.StreamPartEncoded>([
          { type: 'text-start', id: 'a' },
          { type: 'text-delta', id: 'a', delta: ANSWER },
          { type: 'text-end', id: 'a' },
          finish(reason),
        ]),
    }),
  );

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

const CONVERSATION = 'truncated-conversation';

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    Crypto.Crypto | LogStore.Service | LanguageModel.LanguageModel
  >,
  reason: Response.FinishReason,
): Effect.Effect<A> =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(Layer.merge(scripted(reason), testLogLayer)),
    Effect.scoped,
  );

/** The recorded conversation and run exit, stripped of storage envelopes. */
const conversationOf = (reason: 'stop' | 'length') =>
  run(
    Effect.gen(function* () {
      const exit = yield* Conversation.make(agent, CONVERSATION)
        .run('hi')
        .pipe(Effect.exit);
      const store = yield* LogStore.Service;
      const page = yield* store
        .read(
          AgentLog.pathFor(LogVocabulary.ConversationId.make(CONVERSATION)),
          { limit: 100 },
        )
        .pipe(Effect.orDie);
      return {
        exit,
        records: page.records.map(
          (envelope: ConversationRecord.Envelope) => envelope.record,
        ),
      };
    }),
    reason,
  );

describe('a turn the provider truncated at the output cap', () => {
  it.effect('is visible in the live event stream, on the raw finish part', () =>
    Effect.gen(function* () {
      const reasons = yield* run(
        agent.stream('hi').pipe(
          Stream.takeUntil(
            (event) => event._tag === 'Part' && event.part.type === 'finish',
          ),
          Stream.filter(
            (event) => event._tag === 'Part' && event.part.type === 'finish',
          ),
          Stream.map((event) =>
            event._tag === 'Part' && event.part.type === 'finish'
              ? event.part.reason
              : 'not-a-part',
          ),
          Stream.runCollect,
          Effect.orDie,
        ),
        'length',
      );

      // The provider's own word for it, unmodified, at the position it arrived.
      // This is the only channel that carries it.
      expect(reasons).toEqual(['length']);
    }),
  );

  it.effect.each([
    [
      'length',
      'Model output was incomplete because generation reached its output token limit',
    ],
    [
      'content-filter',
      'Model output was incomplete because generation was stopped by the provider content filter',
    ],
    [
      'error',
      'Model output was incomplete because the provider reported a generation error',
    ],
  ] as const)(
    'fails instead of returning a partial %s answer',
    ([reason, description]) =>
      Effect.gen(function* () {
        const error = yield* agent
          .run('hi')
          .pipe(Effect.provide(scripted(reason)), Effect.flip);

        expect(AiError.isAiError(error)).toBe(true);
        if (AiError.isAiError(error)) {
          expect(error.reason).toMatchObject({
            _tag: 'InvalidOutputError',
            description,
            metadata: { finishReason: reason },
          });
        }
      }),
  );

  it.effect(
    'records failed settlement without claiming the fragment is complete',
    () =>
      Effect.gen(function* () {
        const truncated = yield* conversationOf('length');
        const complete = yield* conversationOf('stop');

        expect(truncated.exit._tag).toBe('Failure');
        expect(complete.exit._tag).toBe('Success');
        expect(truncated.records.map((record) => record._tag)).toEqual([
          'RunStarted',
          'Text',
          'TurnFinished',
          'RunSettled',
        ]);
        expect(complete.records.map((record) => record._tag)).toEqual([
          'RunStarted',
          'Text',
          'TurnFinished',
          'Completed',
          'RunSettled',
        ]);
        expect(
          truncated.records.find((record) => record._tag === 'RunSettled'),
        ).toMatchObject({ outcome: 'failure' });
      }),
  );
});
