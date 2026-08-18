import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Exit, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { AgentHistory } from '../src/history.js';
import { AgentLog } from '../src/log.js';
import * as AgentSignals from '../src/internal/signal-store.js';

// Forking: a second conversation, seeded from a prefix of a first.
//
// `branchFrom` already re-runs a conversation from an earlier record, and it
// costs one marker. What it cannot do is run two variants at once, because a
// branch stays in the ancestor's stream and a stream has exactly one writer.
// Fork buys that and nothing else, so the concurrency block below is the
// reason this file exists; everything after it is the price.
//
// The price is that records are copied, and a copied record's offset-valued
// pointers no longer mean what they said. `log.ts`'s `reseat` is where that is
// decided and the two blocks at the bottom are what make the decision honest:
//
//   - `Compacted.firstKept` points into the conversation's own stream, so it
//     is rewritten through a map from ancestor offset to fork offset;
//   - `SignalReceived.at` points into the conversation's *signal* stream, of
//     which the fork has a different, empty one — so it cannot be rewritten at
//     all, and is reset.
//
// Both tests are built so the two offsets genuinely differ between ancestor
// and fork. That is not incidental: with a prefix copied one-for-one out of an
// unbranched conversation, the nth record lands at the nth offset in both, and
// a test that skipped the rewrite would pass anyway.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const says = (body: string): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id: body },
  { type: 'text-delta' as const, id: body, delta: body },
  { type: 'text-end' as const, id: body },
  finish(),
];

/**
 * A rendezvous that only completes once `participants` fibers have reached it.
 *
 * The whole of the concurrency proof. Two runs that were serialised — by
 * fencing, by a shared claim, by anything — would deadlock here rather than
 * quietly taking turns, so "these two ran at the same time" is a test that
 * hangs when it is false instead of an assertion that cannot fail.
 */
const rendezvous = (participants: number): (() => Promise<void>) => {
  let arrived = 0;
  let release: () => void = () => {};
  const opened = new Promise<void>((resolve) => {
    release = resolve;
  });

  return () => {
    arrived += 1;
    if (arrived >= participants) release();
    return opened;
  };
};

/** A provider that keeps every prompt it was handed and replies from a script. */
const provider = (
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  hold?: () => Promise<void>,
) => {
  const asked: Array<Prompt.Prompt> = [];

  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              asked.push(options.prompt);
              if (hold !== undefined) yield* Effect.promise(hold);
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              return Stream.fromIterable(
                turns[Math.min(index, turns.length - 1)]!,
              );
            }),
          ),
      });
    }),
  );

  return { asked, layer };
};

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(lookup),
}).withHandlers({
  lookup: ({ id }) => Effect.succeed({ status: `shipped:${id}` }),
});

const ANCESTOR = LogVocabulary.ConversationId.make('ancestor-conversation');
const ancestorConversation = Conversation.make(agent, ANCESTOR);

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  models: Layer.Layer<LanguageModel.LanguageModel>,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(models),
    Effect.provide(LogStoreMemory.layer),
    Effect.scoped,
  );

const readPath = Effect.fn('test.readPath')(function* (conversationId: string) {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(AgentLog.pathFor(LogVocabulary.ConversationId.make(conversationId)), {
      limit: 1000,
    })
    .pipe(Effect.orDie);
  return page.records;
});

const readSignals = Effect.fn('test.readSignals')(function* (
  conversationId: string,
) {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(
      AgentSignals.pathFor(LogVocabulary.ConversationId.make(conversationId)),
      { limit: 1000 },
    )
    .pipe(Effect.orDie);
  return page.records;
});

const failsOnceAfter = (
  operation: 'create' | 'acquire' | 'append',
  conversationId: string,
): Layer.Layer<LogStore.Service> =>
  Layer.effect(
    LogStore.Service,
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const failed = yield* Ref.make(false);
      const path = AgentLog.pathFor(
        LogVocabulary.ConversationId.make(conversationId),
      );
      const inject = <A>(effect: Effect.Effect<A, LogStore.LogStoreError>) =>
        Effect.gen(function* () {
          const value = yield* effect;
          if (!(yield* Ref.getAndSet(failed, true))) {
            return yield* Effect.fail(
              new LogStore.LogStoreError({
                path,
                operation,
                reason: 'storage',
                detail: `crashed after ${operation}`,
              }),
            );
          }
          return value;
        });

      return LogStore.Service.of({
        ...store,
        create: (requested, identity) =>
          operation === 'create' && requested === path
            ? inject(store.create(requested, identity))
            : store.create(requested, identity),
        acquire: (requested, producerId, expected) =>
          operation === 'acquire' && requested === path
            ? inject(store.acquire(requested, producerId, expected))
            : store.acquire(requested, producerId, expected),
        append: (input) =>
          operation === 'append' && input.path === path
            ? inject(store.append(input))
            : store.append(input),
      });
    }),
  ).pipe(Layer.provide(LogStoreMemory.layer));

const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map((envelope) => envelope.record._tag);

const textIn = (prompt: Prompt.Prompt): string =>
  JSON.stringify(prompt.content);

const started = (text: string): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  agent: 'test',
  formatVersion: 1,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: Prompt.make(text).content,
});

const said = (text: string): ConversationRecord.Record => ({
  _tag: 'Text',
  step: 1,
  text,
});

const turn: ConversationRecord.Record = {
  _tag: 'TurnFinished',
  step: 1,
  usage: { input: 1, output: 1 },
};

const settled: ConversationRecord.Record = {
  _tag: 'RunSettled',
  outcome: 'success',
  detail: '',
  steps: 1,
  usage: { input: 1, output: 1 },
};

/** Text records only, as plain strings — what the model was shown, in order. */
const textsIn = (prompt: Prompt.Prompt): string => JSON.stringify(prompt);

describe('two forks of one conversation', () => {
  // The capability. `branchFrom` cannot do this at all: both variants would be
  // claiming the ancestor's single stream, and the first would die on its next
  // append. Here each fork is its own stream with its own producer, so both
  // hold a claim at once — which is what the rendezvous inside the provider
  // proves. Neither run can leave `streamText` until the other has entered it.
  it.effect(
    'run at the same time, each in its own stream',
    () => {
      const models = provider([says('fork answer')], rendezvous(2));

      return Effect.gen(function* () {
        const observed = yield* run(
          Effect.gen(function* () {
            // Built by hand rather than by a run, so the ancestor does not have to
            // pass through the provider that is about to block on a rendezvous.
            const session = yield* AgentLog.open(
              LogVocabulary.ConversationId.make(ANCESTOR),
              {
                compatibility: {
                  agent: 'test',
                  revision: LogVocabulary.AgentRevision.make('1'),
                },
              },
            );
            yield* session.append([
              started('what is the status?'),
              said('checking now'),
              turn,
              settled,
            ]);
            const tip = (yield* session.recorded).at(-1)!.offset;

            const [left, right] = yield* Effect.all(
              [
                ancestorConversation
                  .forkFrom(tip, 'fork-left', 'explore the left one')
                  .pipe(Effect.orDie),
                ancestorConversation
                  .forkFrom(tip, 'fork-right', 'explore the right one')
                  .pipe(Effect.orDie),
              ],
              { concurrency: 'unbounded' },
            );

            return {
              left,
              right,
              asked: models.asked,
              leftLog: yield* readPath('fork-left'),
              rightLog: yield* readPath('fork-right'),
            };
          }),
          models.layer,
        );

        // Both finished. Under fencing one of them would have died on an append.
        expect(observed.left.text).toBe('fork answer');
        expect(observed.right.text).toBe('fork answer');

        // Each fork carries the ancestor's prefix and only its own new input. The
        // prompts are matched by content rather than by index because two
        // concurrent runs reach the provider in no fixed order.
        const prompts = observed.asked.map(textIn);
        const forLeft = prompts.find((body) => body.includes('the left one'))!;
        const forRight = prompts.find((body) =>
          body.includes('the right one'),
        )!;

        expect(forLeft).toContain('checking now');
        expect(forLeft).not.toContain('the right one');
        expect(forRight).toContain('checking now');
        expect(forRight).not.toContain('the left one');

        // Each fork is a whole conversation: the copied prefix, then its own run.
        expect(tags(observed.leftLog)).toEqual([
          'RunStarted',
          'Text',
          'TurnFinished',
          'RunSettled',
          'RunStarted',
          'Text',
          'TurnFinished',
          'Completed',
          'RunSettled',
        ]);
        expect(tags(observed.rightLog)).toEqual(tags(observed.leftLog));
      });
    },
    10_000,
  );
});

describe('a fork and the conversation it came from', () => {
  it.effect('reports only fork-billed usage across later resumes', () => {
    const models = provider([
      says('first fork turn'),
      says('second fork turn'),
    ]);
    return run(
      Effect.gen(function* () {
        const session = yield* AgentLog.open(
          LogVocabulary.ConversationId.make(ANCESTOR),
          {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          },
        );
        yield* session.append([
          started('original'),
          said('ancestor answer'),
          { ...turn, usage: { input: 31, output: 17 } },
          { ...settled, usage: { input: 31, output: 17 } },
        ]);
        const tip = (yield* session.recorded).at(-1)!.offset;

        const first = yield* ancestorConversation
          .forkFrom(tip, 'usage-fork', 'fork once')
          .pipe(Effect.orDie);
        const second = yield* Conversation.make(agent, 'usage-fork')
          .resume('fork twice')
          .pipe(Effect.orDie);
        expect(first.usage).toEqual({ input: 10, output: 4 });
        expect(second.usage).toEqual({ input: 20, output: 8 });
      }),
      models.layer,
    );
  });

  it.effect('leaves the ancestor exactly as it was', () => {
    const models = provider([says('fork answer')]);

    return run(
      Effect.gen(function* () {
        const session = yield* AgentLog.open(
          LogVocabulary.ConversationId.make(ANCESTOR),
          {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          },
        );
        yield* session.append([
          started('original'),
          said('first'),
          turn,
          settled,
        ]);

        const before = yield* readPath(ANCESTOR);
        yield* ancestorConversation
          .forkFrom(before.at(-1)!.offset, 'a-fork', 'a new idea')
          .pipe(Effect.orDie);

        const after = yield* readPath(ANCESTOR);

        // No marker, no copy, no note that a fork was taken — reading an ancestor
        // takes no producer claim, so it cannot even fence a run that is live on
        // it. This is the trade against `branchFrom`'s single navigable tail.
        expect(after).toEqual(before);
      }),
      models.layer,
    );
  });

  it.effect('does not receive what the ancestor records afterwards', () => {
    const models = provider([
      says('fork answer'),
      says('ancestor answer'),
      says('later fork answer'),
    ]);

    return run(
      Effect.gen(function* () {
        const session = yield* AgentLog.open(
          LogVocabulary.ConversationId.make(ANCESTOR),
          {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          },
        );
        yield* session.append([
          started('original'),
          said('shared past'),
          turn,
          settled,
        ]);
        const tip = (yield* session.recorded).at(-1)!.offset;

        yield* ancestorConversation
          .forkFrom(tip, 'a-fork', 'the fork question')
          .pipe(Effect.orDie);

        // The ancestor carries on independently, after the fork was taken.
        yield* ancestorConversation
          .resume('the ancestor carries on')
          .pipe(Effect.orDie);

        // And the fork carries on too. Its prompt is the evidence.
        yield* Conversation.make(agent, 'a-fork')
          .resume('and the fork too')
          .pipe(Effect.orDie);

        const forkLog = yield* readPath('a-fork');
        const laterFork = textIn(models.asked.at(-1)!);
        expect(laterFork).toContain('shared past');
        expect(laterFork).toContain('the fork question');
        // The two conversations diverged at the fork and never rejoined.
        expect(laterFork).not.toContain('the ancestor carries on');
        expect(laterFork).not.toContain('ancestor answer');

        expect(
          forkLog.some(
            (envelope) =>
              envelope.record._tag === 'Text' &&
              envelope.record.text === 'ancestor answer',
          ),
        ).toBe(false);
      }),
      models.layer,
    );
  });
});

/**
 * An ancestor whose active path is *shorter* than its log.
 *
 * The abandoned range is what makes the copy's offsets diverge from the
 * ancestor's. Without it the nth record would land at the nth offset in both
 * streams and every pointer would be accidentally right, which is exactly the
 * shape of test that lets the bug through.
 *
 * Lays down, in order: an abandoned run, a marker back to its first record, a
 * kept run, a delivered steer, a compaction that keeps the kept run's answer,
 * and a turn after it.
 */
const branchedAncestor = Effect.fn('test.branchedAncestor')(function* (
  deliveredAt: LogOffset.Offset,
) {
  const session = yield* AgentLog.open(
    LogVocabulary.ConversationId.make(ANCESTOR),
    {
      compatibility: {
        agent: 'test',
        revision: LogVocabulary.AgentRevision.make('1'),
      },
    },
  );

  yield* session.append([started('original'), said('abandoned answer'), turn]);
  const opening = (yield* session.recorded)[0]!;
  yield* session.append([{ _tag: 'BranchedFrom', at: opening.offset }]);

  yield* session.append([started('real question'), said('answer A'), turn]);
  const answerA = (yield* session.recorded).find(
    (envelope) =>
      envelope.record._tag === 'Text' && envelope.record.text === 'answer A',
  )!;

  yield* session.append([
    {
      _tag: 'SignalReceived',
      kind: 'steer',
      text: 'steered mid-run',
      source: 'operator',
      step: 1,
      at: deliveredAt,
    },
    {
      _tag: 'Compacted',
      formatVersion: 1,
      agent: 'test',
      agentRevision: LogVocabulary.AgentRevision.make('1'),
      step: 1,
      summary: 'the story so far',
      firstKept: answerA.offset,
      summarizedMessages: 2,
      keptMessages: 2,
    },
  ]);
  yield* session.append([said('after compaction'), turn, settled]);

  return { session, answerA, tip: (yield* session.recorded).at(-1)!.offset };
});

describe('the offset pointers in a copied prefix', () => {
  // The crux the design spike flagged: "copied records get new offsets, and
  // `Compacted.firstKept` / `SignalReceived.at` would silently corrupt".
  //
  // Mutation-checked. Deleting the `Compacted` case from `reseat` in `log.ts`
  // — copying `firstKept` through unchanged — fails both assertions below: the
  // ancestor's offset resolves, in the fork, onto the compaction record
  // itself, so `keptFrom` keeps nothing verbatim and the kept tail vanishes
  // from the prompt. Nothing errors; the fork simply forgets two messages.
  it.effect('reseats a compaction boundary onto the fork’s own offsets', () => {
    const models = provider([says('fork answer')]);

    return run(
      Effect.gen(function* () {
        const ancestor = yield* branchedAncestor(LogOffset.START);
        const fork = yield* AgentLog.fork(
          LogVocabulary.ConversationId.make(ANCESTOR),
          ancestor.tip,
          LogVocabulary.ConversationId.make('a-fork'),
          {
            agent: 'test',
            revision: LogVocabulary.AgentRevision.make('1'),
          },
        );

        const copied = fork.history.find(
          (envelope) => envelope.record._tag === 'Compacted',
        )!;
        const answerA = fork.history.find(
          (envelope) =>
            envelope.record._tag === 'Text' &&
            envelope.record.text === 'answer A',
        )!;

        if (copied.record._tag !== 'Compacted')
          throw new Error('expected copied compaction');
        const forkBoundary = copied.record.firstKept;

        // The premise: the same record sits at a different offset in the fork. If
        // these were equal the test below would pass without any rewriting.
        expect(answerA.offset).not.toBe(ancestor.answerA.offset);

        // The pointer followed it.
        expect(forkBoundary).toBe(answerA.offset);

        // And it means the same thing: the summary, then the tail the compaction
        // kept verbatim — the answer and the steer that redirected it.
        const body = textsIn(AgentHistory.messagesFrom(fork.history));
        expect(body).toContain('the story so far');
        expect(body).toContain('answer A');
        expect(body).toContain('steered mid-run');
        // Still summarized away, so this is a compaction and not a no-op.
        expect(body).not.toContain('real question');
      }),
      models.layer,
    );
  });

  // The other half, and a different kind of wrongness: this pointer names an
  // offset in the *signal* stream, and the fork has a different one. There is
  // no value to rewrite it to, so it is reset.
  //
  // Mutation-checked. Deleting the `SignalReceived` case from `reseat` parks
  // the fork's delivery cursor at the ancestor's signal offset, which is past
  // where the fork's own first signal is written — and the steer below is
  // never delivered. Silent: no error, an agent that is simply not steered.
  it.effect(
    'restarts the signal cursor, so a signal to the fork is delivered',
    () => {
      const models = provider([says('fork answer')]);

      return run(
        Effect.gen(function* () {
          // Two signals to the ancestor, the second of them delivered, so the
          // recorded cursor is past the offset the fork's first signal will get.
          yield* ancestorConversation
            .send({
              kind: 'steer',
              text: 'first ancestor steer',
              source: 'operator',
            })
            .pipe(Effect.orDie);
          yield* ancestorConversation
            .send({
              kind: 'steer',
              text: 'second ancestor steer',
              source: 'operator',
            })
            .pipe(Effect.orDie);

          const ancestorSignals = yield* readSignals(ANCESTOR);
          const ancestor = yield* branchedAncestor(ancestorSignals[1]!.offset);

          const fork = yield* AgentLog.fork(
            LogVocabulary.ConversationId.make(ANCESTOR),
            ancestor.tip,
            LogVocabulary.ConversationId.make('a-fork'),
            {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          );

          yield* Conversation.make(agent, 'a-fork')
            .send({
              kind: 'steer',
              text: 'a steer for the fork',
              source: 'operator',
            })
            .pipe(Effect.orDie);

          const forkSignals = yield* readSignals('a-fork');
          const copied = fork.history.find(
            (envelope) => envelope.record._tag === 'SignalReceived',
          )!;

          if (copied.record._tag !== 'SignalReceived')
            throw new Error('expected copied signal');
          const copiedAt = copied.record.at;
          const delivered = yield* fork.drainSignals;

          // The premise: carrying the ancestor's cursor over would park the fork
          // past its own first signal, because the fork's signal stream starts again
          // from the beginning.
          expect(
            LogOffset.isAfter(
              ancestorSignals[1]!.offset,
              forkSignals[0]!.offset,
            ),
          ).toBe(true);

          // Reset rather than rewritten — the fork has drained none of its own.
          expect(copiedAt).toBe(LogOffset.START);

          // So the steer actually arrives.
          expect(delivered.map((signal) => signal.text)).toEqual([
            'a steer for the fork',
          ]);

          // And the copied record kept its body: the ancestor's steer is still a
          // user message in the conversation the fork inherited.
          expect(textsIn(AgentHistory.messagesFrom(fork.history))).toContain(
            'steered mid-run',
          );
        }),
        models.layer,
      );
    },
  );
});

describe('forking into an id that is already a conversation', () => {
  // A fork mints a conversation. Appending a second one's prefix into a live
  // conversation would interleave two histories with nothing to say it
  // happened, so the `create` in `AgentLog.fork` does not tolerate the
  // conflict that `open` does.
  it.effect('is a defect, not an append into it', () => {
    const models = provider([says('fork answer')]);

    return run(
      Effect.gen(function* () {
        const session = yield* AgentLog.open(
          LogVocabulary.ConversationId.make(ANCESTOR),
          {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          },
        );
        yield* session.append([
          started('original'),
          said('first'),
          turn,
          settled,
        ]);
        const tip = (yield* session.recorded).at(-1)!.offset;

        const occupied = yield* AgentLog.open(
          LogVocabulary.ConversationId.make('already-here'),
          {
            compatibility: {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          },
        );
        yield* occupied.append([started('a life of its own')]);

        const exit = yield* Effect.exit(
          AgentLog.fork(
            LogVocabulary.ConversationId.make(ANCESTOR),
            tip,
            LogVocabulary.ConversationId.make('already-here'),
            {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          ),
        );

        expect(Exit.isFailure(exit)).toBe(true);
        // Untouched: the fork wrote nothing into the conversation it
        // collided with, rather than half a prefix.
        expect(tags(yield* readPath('already-here'))).toEqual(['RunStarted']);
      }),
      models.layer,
    );
  });
});

describe('fork seeding recovery', () => {
  for (const operation of ['create', 'acquire', 'append'] as const) {
    it.effect(`converges after a crash following ${operation}`, () =>
      Effect.gen(function* () {
        const destination = `retry-${operation}`;
        const layer = failsOnceAfter(operation, destination);
        const result = yield* Effect.gen(function* () {
          const ancestor = yield* AgentLog.open(
            LogVocabulary.ConversationId.make(ANCESTOR),
            {
              compatibility: {
                agent: 'test',
                revision: LogVocabulary.AgentRevision.make('1'),
              },
            },
          );
          yield* ancestor.append([
            started('original'),
            said('copied once'),
            {
              _tag: 'ToolStarted',
              id: LogVocabulary.ToolCallId.make('copied-call'),
              name: 'lookup',
            },
            turn,
          ]);
          const copiedText = (yield* ancestor.recorded)[1]!;
          yield* ancestor.append([
            {
              _tag: 'Compacted',
              formatVersion: 1,
              agent: 'test',
              agentRevision: LogVocabulary.AgentRevision.make('1'),
              step: 1,
              summary: 'seed summary',
              firstKept: copiedText.offset,
              summarizedMessages: 1,
              keptMessages: 1,
            },
            settled,
          ]);
          const tip = (yield* ancestor.recorded).at(-1)!.offset;

          const first = yield* AgentLog.fork(
            LogVocabulary.ConversationId.make(ANCESTOR),
            tip,
            LogVocabulary.ConversationId.make(destination),
            {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          ).pipe(Effect.exit);
          const retried = yield* AgentLog.fork(
            LogVocabulary.ConversationId.make(ANCESTOR),
            tip,
            LogVocabulary.ConversationId.make(destination),
            {
              agent: 'test',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          );
          return { first, records: retried.history };
        }).pipe(Effect.provide(layer), Effect.scoped);

        expect(Exit.isFailure(result.first)).toBe(true);
        // Session history is the retained resume view: the compacted-away
        // RunStarted was copied physically, but is not materialized on open.
        expect(tags(result.records)).toEqual([
          'Text',
          'ToolStarted',
          'TurnFinished',
          'Compacted',
          'RunSettled',
        ]);
      }),
    );
  }
});
