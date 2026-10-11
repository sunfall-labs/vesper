import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Layer, Option, Ref } from 'effect';

import { Conversation } from '../src/conversation.js';
import { AgentHistory } from '../src/history.js';
import * as AgentLog from '../src/log.js';
import {
  callId,
  compatibility,
  lookupStarted,
  openSession,
  plainAgent,
  present,
  provider,
  readRecords,
  run,
  said,
  says,
  settled,
  started,
  tags,
  testLogLayer,
  textIn,
  turn,
} from './durability-fixtures.js';
import { toolCalled, toolStarted } from './tool-fixtures.js';

// Forking: a second conversation, seeded from a prefix of a first.
//
// `branchFrom` re-runs a conversation from an earlier record at the cost of one
// marker. What it cannot do is run two variants at once, because a branch
// stays in the ancestor's stream and a stream has exactly one writer. Fork buys
// that and nothing else, so the concurrency scenario is the reason this file
// exists; the rest is the price.
//
// The price is that records are copied, and a copied record's offset-valued
// pointers no longer mean what they said:
//
//   - `Compacted.firstKept` points into the conversation's own stream, so it
//     is rewritten through a map from ancestor offset to fork offset;
//   - `SignalReceived.at` points into the conversation's *signal* stream, of
//     which the fork has a different, empty one, so it cannot be rewritten at
//     all and is reset.
//
// Both scenarios are built so the offsets genuinely differ between ancestor and
// fork. With a prefix copied one-for-one out of an unbranched conversation the
// nth record lands at the nth offset in both and a test that skipped the
// rewrite would pass anyway.

const ANCESTOR = 'ancestor-conversation';
const ancestorConversation = Conversation.make(plainAgent, ANCESTOR);

/** A rendezvous that only completes once `participants` fibers have reached it. */
const rendezvous = (participants: number): Effect.Effect<void> => {
  const gate = Deferred.makeUnsafe<void>();
  let arrived = 0;
  return Effect.suspend(() => {
    arrived += 1;
    return (
      arrived >= participants ? Deferred.succeed(gate, undefined) : Effect.void
    ).pipe(Effect.andThen(Deferred.await(gate)));
  });
};

/** An ancestor with one finished turn; returns its tip offset. */
const finishedAncestor = (usage = { input: 1, output: 1 }) =>
  Effect.gen(function* () {
    const session = yield* openSession(ANCESTOR);
    yield* session.append([
      started('what is the status?'),
      said('checking now'),
      turn(usage),
      settled(usage),
    ]);
    return {
      session,
      tip: present((yield* session.recorded).at(-1)).offset,
    };
  });

const forkTo = (id: string, tip: LogOffset.Offset, mode?: 'restart') =>
  AgentLog.fork(
    LogVocabulary.ConversationId.make(ANCESTOR),
    tip,
    LogVocabulary.ConversationId.make(id),
    compatibility(),
    mode,
  );

describe('suspended workflow boundaries', () => {
  it.effect('rejects both branch and fork through an owned durable wait', () =>
    Effect.gen(function* () {
      const call = callId('approval-call');
      const source = yield* openSession(ANCESTOR);
      yield* source.append([
        started('approve this'),
        toolCalled('approve', {}, call),
        toolStarted('approve', call),
        {
          _tag: 'StepSuspended',
          id: call,
          name: 'approve',
          wait: 'human-approval',
          token: 'owned-workflow-token',
          request: { action: 'approve this' },
        },
      ]);
      const suspendedAt = present((yield* source.recorded).at(-1)).offset;
      const sourceId = LogVocabulary.ConversationId.make(ANCESTOR);

      const branched = yield* Effect.flip(
        AgentLog.open(sourceId, {
          compatibility: compatibility(),
          branchFrom: suspendedAt,
        }),
      );
      const forked = yield* Effect.flip(forkTo('suspended-fork', suspendedAt));
      expect(branched._tag).toBe('SuspendedConversationError');
      expect(forked._tag).toBe('SuspendedConversationError');

      // Restarting the wait is the explicit way through: the new path gets a
      // fresh dispatch rather than the old workflow's token.
      const restartedBranch = yield* AgentLog.open(sourceId, {
        compatibility: compatibility(),
        branchFrom: suspendedAt,
        pendingWait: 'restart',
      });
      const restartedFork = yield* forkTo(
        'restarted-fork',
        suspendedAt,
        'restart',
      );
      for (const session of [restartedBranch, restartedFork]) {
        const recovery = session.recovery('approve', call);
        expect(Option.isSome(recovery) ? recovery.value._tag : undefined).toBe(
          'Restarting',
        );
      }
    }).pipe(Effect.provide(testLogLayer), Effect.scoped),
  );
});

describe('forks of one conversation', () => {
  // The capability. `branchFrom` cannot do this at all: both variants would be
  // claiming the ancestor's single stream and the first would die on its next
  // append. Here each fork is its own stream with its own producer, so both
  // hold a claim at once, which the rendezvous inside the provider proves:
  // neither run can leave `streamText` until the other has entered it. Under
  // fencing this would hang instead of quietly taking turns.
  it.effect(
    'run at the same time, each in its own stream',
    () => {
      const models = provider([says('fork answer')], rendezvous(2));
      return Effect.gen(function* () {
        const observed = yield* run(
          Effect.gen(function* () {
            // Built by hand so the ancestor does not pass through the provider
            // that is about to block on a rendezvous.
            const { tip } = yield* finishedAncestor();
            const [left, right] = yield* Effect.all(
              [
                ancestorConversation.forkFrom(
                  tip,
                  'fork-left',
                  'explore the left one',
                ),
                ancestorConversation.forkFrom(
                  tip,
                  'fork-right',
                  'explore the right one',
                ),
              ],
              { concurrency: 'unbounded' },
            );
            return {
              left,
              right,
              leftLog: yield* readRecords('fork-left'),
              rightLog: yield* readRecords('fork-right'),
            };
          }),
          models.layer,
        );

        expect(observed.left.text).toBe('fork answer');
        expect(observed.right.text).toBe('fork answer');

        // Prompts are matched by content: two concurrent runs reach the
        // provider in no fixed order. Each carries the ancestor's prefix and
        // only its own new input.
        const prompts = models.asked.map(textIn);
        const forLeft = present(
          prompts.find((body) => body.includes('the left one')),
        );
        const forRight = present(
          prompts.find((body) => body.includes('the right one')),
        );
        expect(forLeft).toContain('checking now');
        expect(forLeft).not.toContain('the right one');
        expect(forRight).toContain('checking now');
        expect(forRight).not.toContain('the left one');

        // Each fork is a whole conversation: the copied prefix, then its run.
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

  // Independence in both directions. Reading an ancestor takes no producer
  // claim, so a fork cannot fence a run that is live on it: no marker, no copy,
  // no note that a fork was taken. That is the trade against `branchFrom`'s
  // single navigable tail.
  it.effect(
    'are independent of the ancestor, and bill only their own usage',
    () => {
      const models = provider([
        says('first fork turn'),
        says('ancestor answer'),
        says('second fork turn'),
      ]);
      return run(
        Effect.gen(function* () {
          const { tip } = yield* finishedAncestor({ input: 31, output: 17 });
          const before = yield* readRecords(ANCESTOR);

          const first = yield* ancestorConversation.forkFrom(
            tip,
            'a-fork',
            'the fork question',
          );
          expect(yield* readRecords(ANCESTOR)).toEqual(before);

          // Each carries on independently after the fork was taken.
          yield* ancestorConversation.run('the ancestor carries on');
          const second = yield* Conversation.make(plainAgent, 'a-fork').run(
            'and the fork too',
          );

          const laterFork = textIn(present(models.asked.at(-1)));
          expect(laterFork).toContain('checking now');
          expect(laterFork).toContain('the fork question');
          expect(laterFork).not.toContain('the ancestor carries on');
          expect(laterFork).not.toContain('ancestor answer');

          // The ancestor's 31/17 is history, not the fork's bill.
          expect(first.usage).toEqual({ input: 10, output: 4 });
          expect(second.usage).toEqual({ input: 20, output: 8 });
        }),
        models.layer,
      );
    },
  );
});

/**
 * An ancestor whose active path is shorter than its log: an abandoned run, a
 * marker back to its first record, a kept run, a compaction that keeps the
 * kept run's answer (and a steer), then a turn after it. The abandoned range is
 * what makes the copy's offsets diverge from the ancestor's.
 */
const branchedAncestor = Effect.gen(function* () {
  const session = yield* openSession(ANCESTOR);
  yield* session.append([
    started('original'),
    said('abandoned answer'),
    turn(),
  ]);
  const opening = present((yield* session.recorded)[0]);
  yield* session.append([
    { _tag: 'BranchedFrom', at: opening.offset },
    started('real question'),
    said('answer A'),
    turn(),
  ]);
  const answerA = present(
    (yield* session.recorded).find(
      ({ record }) => record._tag === 'Text' && record.text === 'answer A',
    ),
  );
  yield* session.append([
    {
      _tag: 'SignalReceived',
      kind: 'steer',
      text: 'steered mid-run',
      source: 'operator',
      step: 1,
      at: LogOffset.START,
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
    said('after compaction'),
    turn(),
    settled(),
  ]);
  return { answerA, tip: present((yield* session.recorded).at(-1)).offset };
});

describe('the offset pointers in a copied prefix', () => {
  // Mutation-checked: copying `Compacted.firstKept` through unchanged resolves
  // the ancestor's offset, in the fork, onto the compaction record itself, so
  // the kept tail vanishes from the prompt. Nothing errors; the fork simply
  // forgets two messages.
  it.effect('reseats a compaction boundary onto the fork’s own offsets', () =>
    run(
      Effect.gen(function* () {
        const ancestor = yield* branchedAncestor;
        const fork = yield* forkTo('a-fork', ancestor.tip);

        const copied = present(
          fork.history.find(({ record }) => record._tag === 'Compacted'),
        );
        const answerA = present(
          fork.history.find(
            ({ record }) =>
              record._tag === 'Text' && record.text === 'answer A',
          ),
        );
        if (copied.record._tag !== 'Compacted') {
          throw new Error('expected copied compaction');
        }

        // The premise: the same record sits at a different offset in the fork.
        expect(answerA.offset).not.toBe(ancestor.answerA.offset);
        // The pointer followed it, and means the same thing: the summary, then
        // the tail the compaction kept verbatim.
        expect(copied.record.firstKept).toBe(answerA.offset);
        const body = textIn(AgentHistory.messagesFrom(fork.history));
        expect(body).toContain('the story so far');
        expect(body).toContain('answer A');
        expect(body).toContain('steered mid-run');
        expect(body).not.toContain('real question');
      }),
      provider([says('unused')]).layer,
    ),
  );

  // A different kind of wrongness: this pointer names an offset in the *signal*
  // stream, and the fork has a different one. There is no value to rewrite it
  // to, so it is reset. Mutation-checked: carrying the ancestor's cursor over
  // parks the fork past where its own first signal is written, so the steer
  // below is never delivered. Silent: an agent that is simply not steered.
  it.effect(
    'restarts the signal cursor, so a signal to the fork is delivered',
    () =>
      run(
        Effect.gen(function* () {
          // Two signals to the ancestor, both delivered, so the recorded cursor
          // is past the offset the fork's first signal will get.
          for (const text of [
            'first ancestor steer',
            'second ancestor steer',
          ]) {
            yield* ancestorConversation.send({
              kind: 'steer',
              text,
              source: 'operator',
            });
          }
          yield* ancestorConversation.run('hi');
          const written = yield* readRecords(ANCESTOR);
          const tip = present(written.at(-1)).offset;
          const delivered = written.flatMap(({ record }) =>
            record._tag === 'SignalReceived' ? [record] : [],
          );
          expect(delivered).toHaveLength(2);
          expect(delivered[1]?.at).not.toBe(LogOffset.START);

          const fork = yield* forkTo('a-fork', tip);
          yield* Conversation.make(plainAgent, 'a-fork').send({
            kind: 'steer',
            text: 'a steer for the fork',
            source: 'operator',
          });

          const copied = fork.history.flatMap(({ record }) =>
            record._tag === 'SignalReceived' ? [record] : [],
          );
          expect(copied.map((signal) => signal.at)).toEqual([
            LogOffset.START,
            LogOffset.START,
          ]);
          expect(
            (yield* fork.drainSignalsBounded(1_000)).signals.map(
              (signal) => signal.text,
            ),
          ).toEqual(['a steer for the fork']);
          // The copied records kept their bodies.
          expect(textIn(AgentHistory.messagesFrom(fork.history))).toContain(
            'first ancestor steer',
          );
        }),
        provider([says('ok')]).layer,
      ),
  );
});

describe('forking into an id that is already a conversation', () => {
  // A fork mints a conversation. Appending a second one's prefix into a live
  // conversation would interleave two histories with nothing to say it
  // happened, so `fork` does not tolerate the conflict that `open` does.
  it.effect('fails rather than appending into it', () =>
    run(
      Effect.gen(function* () {
        const { tip } = yield* finishedAncestor();
        const occupied = yield* openSession('already-here');
        yield* occupied.append([started('a life of its own')]);

        const failure = yield* Effect.exit(forkTo('already-here', tip));

        expect(failure._tag).toBe('Failure');
        // Untouched: no half a prefix was written into the collision.
        expect(tags(yield* readRecords('already-here'))).toEqual([
          'RunStarted',
        ]);
      }),
      provider([says('unused')]).layer,
    ),
  );
});

/** Crashes the destination's storage once, right after `operation` succeeds. */
const failsOnceAfter = (
  operation: 'create' | 'acquire' | 'append',
  conversationId: string,
) =>
  Layer.mergeAll(
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
              return yield* new LogStore.LogStoreError({
                path,
                operation,
                reason: 'storage',
                detail: `crashed after ${operation}`,
              });
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
    ).pipe(Layer.provide(testLogLayer)),
    NodeServices.layer,
  );

describe('fork seeding recovery', () => {
  for (const operation of ['create', 'acquire', 'append'] as const) {
    it.effect(`converges after a crash following ${operation}`, () =>
      Effect.gen(function* () {
        const destination = `retry-${operation}`;
        const result = yield* Effect.gen(function* () {
          const ancestor = yield* openSession(ANCESTOR);
          yield* ancestor.append([
            started('original'),
            said('copied once'),
            lookupStarted('copied-call'),
            turn(),
          ]);
          const copiedText = present((yield* ancestor.recorded)[1]);
          const compaction: ConversationRecord.Record = {
            _tag: 'Compacted',
            formatVersion: 1,
            agent: 'test',
            agentRevision: LogVocabulary.AgentRevision.make('1'),
            step: 1,
            summary: 'seed summary',
            firstKept: copiedText.offset,
            summarizedMessages: 1,
            keptMessages: 1,
          };
          yield* ancestor.append([compaction, settled()]);
          const tip = present((yield* ancestor.recorded).at(-1)).offset;

          const first = yield* Effect.exit(forkTo(destination, tip));
          const retried = yield* forkTo(destination, tip);
          return { first, records: retried.history };
        }).pipe(
          Effect.provide(failsOnceAfter(operation, destination)),
          Effect.scoped,
        );

        expect(result.first._tag).toBe('Failure');
        // Session history is the retained resume view: the compacted-away
        // RunStarted was copied physically but is not materialized on open.
        expect(tags(result.records)).toEqual([
          'Text',
          'StepStarted',
          'TurnFinished',
          'Compacted',
          'RunSettled',
        ]);
      }),
    );
  }
});
