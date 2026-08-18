import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Crypto, Effect, Layer, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';
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
// These are **characterization** tests. They pin what the loop does today with
// `finish.reason: 'length'`, which is: nothing. `observe` reads `usage` off the
// finish part and ignores `reason`; `Stop.State` has no field for it; neither
// `Completed` nor `TurnFinished` carries one; and `partRecords` writes no
// record for a finish part at all. So a truncated turn satisfies the default
// stop condition — it requested no tools — and the run settles as a success
// holding half a sentence.
//
// The one place it *is* visible is the live event stream, because the raw
// finish part travels through as a `Part` event. That asymmetry is the whole
// point of the trio below: a UI watching the stream can see the truncation, and
// a caller using `run`, a stop condition, or a resumed conversation cannot. If
// that is ever fixed, these fail — which is the intent. They are here so the
// gap is a recorded decision rather than an oversight nobody had written down.

const finish = (reason: 'stop' | 'length') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 7, uncached: 7, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 3 },
  },
});

const ANSWER = 'the answer was cut';

const scripted = (reason: 'stop' | 'length') =>
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
  reason: 'stop' | 'length',
): Effect.Effect<A> =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(scripted(reason)),
    Effect.provide(testLogLayer),
    Effect.scoped,
  );

/** The recorded conversation, stripped of what varies between two runs. */
const conversationOf = (reason: 'stop' | 'length') =>
  run(
    Effect.gen(function* () {
      yield* Conversation.make(agent, CONVERSATION)
        .run('hi')
        .pipe(Effect.orDie);
      const store = yield* LogStore.Service;
      const page = yield* store
        .read(
          AgentLog.pathFor(LogVocabulary.ConversationId.make(CONVERSATION)),
          { limit: 100 },
        )
        .pipe(Effect.orDie);
      return page.records.map(
        (envelope: ConversationRecord.Envelope) => envelope.record,
      );
    }),
    reason,
  );

describe('a turn the provider truncated at the output cap', () => {
  it.effect('is visible in the live event stream, on the raw finish part', () =>
    Effect.gen(function* () {
      const reasons = yield* run(
        agent.stream('hi').pipe(
          Stream.filter(
            (event) =>
              event._tag === 'Part' &&
              (event.part as Response.StreamPartEncoded).type === 'finish',
          ),
          Stream.map((event) =>
            event._tag === 'Part'
              ? (event.part as { readonly reason: string }).reason
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

  // Everything below is the gap. A caller that used `run` — which is most of
  // them — gets a `Result` that says the agent answered, and the half sentence
  // it stopped on is the answer.
  it.effect('settles the run as a success, holding the partial answer', () =>
    Effect.gen(function* () {
      const outcome = yield* run(
        Effect.gen(function* () {
          const result = yield* Conversation.make(agent, CONVERSATION)
            .run('hi')
            .pipe(Effect.orDie);

          const store = yield* LogStore.Service;
          const page = yield* store
            .read(
              AgentLog.pathFor(LogVocabulary.ConversationId.make(CONVERSATION)),
              { limit: 100 },
            )
            .pipe(Effect.orDie);

          return {
            result,
            settled: page.records.flatMap((envelope) =>
              envelope.record._tag === 'RunSettled' ? [envelope.record] : [],
            ),
          };
        }),
        'length',
      );

      expect(outcome.result).toMatchObject({ text: ANSWER, steps: 1 });
      expect(outcome.settled).toMatchObject([
        { outcome: 'success', detail: '' },
      ]);
    }),
  );

  // The strongest statement of the gap, and the one that fails first if
  // anybody threads the reason through: two runs that differ only in whether
  // the provider said it had finished produce byte-identical conversations.
  it.effect(
    'records a conversation indistinguishable from a complete answer',
    () =>
      Effect.gen(function* () {
        const truncated = yield* conversationOf('length');
        const complete = yield* conversationOf('stop');

        expect(truncated).toEqual(complete);
        // Not a comparison of a value with itself: both are real recorded runs,
        // and this is the record set they agree on.
        expect(truncated.map((record) => record._tag)).toEqual([
          'RunStarted',
          'Text',
          'TurnFinished',
          'Completed',
          'RunSettled',
        ]);
      }),
  );
});
