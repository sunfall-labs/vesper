import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Stream } from 'effect';
import { Toolkit, type Response } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { ContextWindow } from '../src/context-window.js';
import { Conversation } from '../src/conversation.js';
import { AgentEvents } from '../src/event.js';
import {
  callsLookup,
  finish,
  lookup,
  lookupAgent,
  lookupCall,
  lookupOutcome,
  plainAgent,
  present,
  provider,
  readRecords,
  rolesOf,
  run,
  said,
  says,
  seed,
  settled,
  started,
  tags,
  textIn,
  turn,
} from './durability-fixtures.js';

// Resumption from the log. The claim the whole design rests on: a
// conversation resumed from records does not re-pay the provider for turns it
// already completed, and does not re-run the tool calls those turns made.
// Checkpointing could offer the first of those, and only by replaying the loop
// from turn one; it never offered the second, because tools run past the
// checkpoint boundary.
//
// The assertions are non-vacuous because the provider counts and keeps the
// prompts it is handed ("was not re-asked" is a number) and the tool handler
// counts its own invocations ("was not re-run" is too).

const ID = 'conversation-1';
const answer = says('do', 'ne');
const later = says('again');

describe('resuming a crashed run', () => {
  // A run is abandoned after its tool call settles (the consumer walks away
  // mid-conversation, which is what a crash looks like from the log's side)
  // and the conversation is then resumed.
  it.effect(
    'continues from what the crashed run left, without re-asking or re-running it',
    () =>
      Effect.gen(function* () {
        const { agent, ran } = lookupAgent();
        const conversation = Conversation.make(agent, ID);
        const models = provider([callsLookup('he', 'l', 'lo'), answer, later]);

        const observed = yield* run(
          Effect.gen(function* () {
            yield* conversation.stream('where is order 42?').pipe(
              Stream.takeUntil(
                (event) =>
                  AgentEvents.isPart(event) &&
                  event.part.type === 'tool-result',
              ),
              Stream.runDrain,
            );
            const afterCrash = tags(yield* readRecords(ID));
            const crashed = { asked: models.asked.length, ran: ran.count };
            const result = yield* conversation.run('and then?');
            return {
              afterCrash,
              crashed,
              result,
              all: tags(yield* readRecords(ID)),
            };
          }),
          models.layer,
        );

        // The crashed run got one turn's worth of records and no `Completed`.
        expect(observed.afterCrash).toEqual([
          'RunStarted',
          'Text',
          'StepOpened',
          'StepStarted',
          'StepSettled',
          'RunSettled',
        ]);
        expect(observed.crashed).toEqual({ asked: 1, ran: 1 });

        // One further provider call, for the turn that had not happened yet.
        // Replay-from-checkpoint recovery would make this two.
        expect(models.asked).toHaveLength(2);
        expect(ran.count).toBe(1);

        // The resumed call was given the conversation, not a fresh start.
        const resumed = present(models.asked[1]);
        expect(rolesOf(resumed)).toEqual([
          'system',
          'user',
          'assistant',
          'tool',
          'user',
        ]);
        for (const fragment of [
          'where is order 42?',
          'hello',
          'call-1',
          'shipped:42',
          'and then?',
        ]) {
          expect(textIn(resumed)).toContain(fragment);
        }
        expect(observed.result.text).toBe('done');

        // The resumed run is an ordinary recorded run appended to the same
        // conversation, not a second history beside it.
        expect(observed.all.slice(6)).toEqual([
          'RunStarted',
          'Text',
          'TurnFinished',
          'Completed',
          'RunSettled',
        ]);
      }),
  );
});

describe('resuming an ordinary conversation', () => {
  it.effect('starts a new conversation when there is nothing to resume', () =>
    Effect.gen(function* () {
      const models = provider([answer]);
      yield* run(
        Conversation.make(plainAgent, 'fresh').run('hello'),
        models.layer,
      );

      const first = present(models.asked[0]);
      expect(rolesOf(first)).toEqual(['system', 'user']);
      expect(textIn(first)).toContain('be terse');
    }),
  );

  it.effect(
    'carries the conversation, and its lifetime usage, into the next turn',
    () =>
      Effect.gen(function* () {
        const conversation = Conversation.make(plainAgent, ID);
        const models = provider([answer, later]);
        const observed = yield* run(
          Effect.gen(function* () {
            const first = yield* conversation.run('first');
            const events = yield* conversation
              .stream('second')
              .pipe(Stream.runCollect);
            return {
              first,
              completed: Array.from(events).find(
                (event) => event._tag === 'Completed',
              ),
            };
          }),
          models.layer,
        );

        const second = present(models.asked[1]);
        expect(rolesOf(second)).toEqual([
          'system',
          'user',
          'assistant',
          'user',
        ]);
        for (const fragment of ['first', 'done', 'second']) {
          expect(textIn(second)).toContain(fragment);
        }
        // `Completed.text` repeats the final turn's `Text`; reading both would
        // say everything twice, which the model would read as a stutter.
        expect(textIn(second).match(/done/g)).toHaveLength(1);
        // A resumed conversation that reset the count would under-report every
        // turn after the first, which is the number anyone asking about cost
        // wants. `stream` must report the same lifetime total `run` does.
        expect(observed.completed).toMatchObject({
          _tag: 'Completed',
          usage: {
            input: observed.first.usage.input * 2,
            output: observed.first.usage.output * 2,
          },
        });
      }),
  );

  it.effect('keeps separate conversations separate', () =>
    Effect.gen(function* () {
      const models = provider([answer, later]);
      yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(plainAgent, 'conv-a').run('first');
          yield* Conversation.make(plainAgent, 'conv-b').run('first');
        }),
        models.layer,
      );

      expect(rolesOf(present(models.asked[1]))).toEqual(['system', 'user']);
    }),
  );

  // A two-step run records cumulative usage on its second turn. Resuming has to
  // anchor the context estimator on the latest turn's own usage, not on the
  // cumulative figure, or the estimate double-counts the first step.
  it.effect(
    'restores the latest turn usage as the resumed estimator anchor',
    () =>
      Effect.gen(function* () {
        const seen: Array<ContextWindow.TurnUsage | undefined> = [];
        const heuristics: ContextWindow.Heuristics = {
          estimate: (_prompt, usage) => {
            seen.push(usage);
            return { tokens: 0, usageTokens: 0, trailingTokens: 0 };
          },
          shouldCompact: () => false,
        };
        const anchored = Agent.make({
          name: 'test',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(lookup),
          compaction: {
            contextWindow: 1_000,
            reserveTokens: 100,
            keepRecentTokens: 10,
            instructions: 'sum',
          },
        }).withHandlers({ lookup: () => Effect.succeed({ status: 'ok' }) });
        const conversation = Conversation.make(anchored, ID);

        yield* run(
          Effect.gen(function* () {
            yield* conversation.run('first');
            seen.length = 0;
            yield* conversation.run('second');
          }).pipe(Effect.provideService(ContextWindow.Service, heuristics)),
          provider([callsLookup(), answer, later]).layer,
        );

        expect(seen).toEqual([{ inputTokens: 10, outputTokens: 4 }]);
      }),
  );

  it.effect('keeps a provider-executed call without a Vesper outcome', () =>
    Effect.gen(function* () {
      const remote: Response.StreamPartEncoded[] = [
        ...says('searched').slice(0, -1),
        {
          type: 'tool-call',
          id: 'provider-call-1',
          name: 'lookup',
          params: { id: '42' },
          providerExecuted: true,
        },
        finish('tool-calls'),
      ];
      const conversation = Conversation.make(plainAgent, ID);
      const models = provider([remote, answer, later]);
      const records = yield* run(
        Effect.gen(function* () {
          yield* conversation.run('search remotely');
          yield* conversation.run('continue');
          return yield* readRecords(ID);
        }),
        models.layer,
      );

      expect(
        records.find(({ record }) => record._tag === 'StepOpened')?.record,
      ).toMatchObject({ providerExecuted: true });
      const assistant = present(models.asked[2]).content.find(
        (message) => message.role === 'assistant',
      );
      expect(
        assistant?.content.find(
          (part) => part.type === 'tool-call' && part.id === 'provider-call-1',
        ),
      ).toMatchObject({ type: 'tool-call', providerExecuted: true });
    }),
  );
});

const signal = (kind: 'steer' | 'cancel', text: string) =>
  ({
    _tag: 'SignalReceived',
    kind,
    text,
    source: 'operator',
    step: 1,
    at: LogOffset.fromSeq(1n),
  }) satisfies ConversationRecord.Record;
// What the model is shown after a resume is rebuilt from nothing but the
// recorded history. Each case writes an exact history, resumes it, and reads
// the prompt the model receives.
describe('rebuilding the prompt from the recorded history', () => {
  const promptAfter = (records: ReadonlyArray<ConversationRecord.Record>) =>
    Effect.gen(function* () {
      const models = provider([later]);
      yield* run(
        Effect.gen(function* () {
          yield* seed(ID, records);
          yield* Conversation.make(plainAgent, ID).run('and then?');
        }),
        models.layer,
      );
      return present(models.asked[0]);
    });

  // The mid-turn crash. An assistant tool call with no matching result is not
  // a degraded prompt (providers reject the request outright), so it is
  // dropped and the model may ask again.
  it.effect(
    'groups a turn into assistant and tool messages and drops an unanswered call',
    () =>
      Effect.gen(function* () {
        const prompt = yield* promptAfter([
          started('hi'),
          said('looking'),
          lookupCall('call-1'),
          lookupOutcome('call-1', 'shipped:42'),
          lookupCall('call-2'),
          turn(),
          settled(),
        ]);

        expect(rolesOf(prompt)).toEqual([
          'system',
          'user',
          'assistant',
          'tool',
          'user',
        ]);
        expect(textIn(prompt)).toContain('call-1');
        expect(textIn(prompt)).not.toContain('call-2');
      }),
  );

  // A turn can finish before every call in it is answered: an approval is
  // decided, or an indeterminate call reconciled, after the turn's
  // `TurnFinished`. The model must still see the call followed by its answer.
  it.effect('keeps a call and an answer recorded after its turn adjacent', () =>
    Effect.gen(function* () {
      const prompt = yield* promptAfter([
        started('look it up'),
        lookupCall(),
        turn(),
        lookupOutcome('call-1', 'found'),
        settled(),
      ]);

      expect(rolesOf(prompt)).toEqual([
        'system',
        'user',
        'assistant',
        'tool',
        'user',
      ]);
      expect(present(prompt.content[3])).toMatchObject({
        role: 'tool',
        content: [{ type: 'tool-result', id: 'call-1' }],
      });
    }),
  );

  // A steer becomes a user message on the next turn, so it belongs after the
  // turn that consumed it, not before the model's own words (where record
  // order alone would put it). A cancel changed no prompt.
  it.effect(
    'replays a steer as the user message it became and ignores a cancel',
    () =>
      Effect.gen(function* () {
        const prompt = yield* promptAfter([
          started('hi'),
          said('working'),
          signal('steer', 'also check the invoice'),
          signal('cancel', 'stop'),
          turn(),
          settled(),
        ]);

        expect(rolesOf(prompt)).toEqual([
          'system',
          'user',
          'assistant',
          'user',
          'user',
        ]);
        // After the model's own words, where record order alone would put
        // it before them.
        expect(
          textIn(prompt).indexOf('also check the invoice'),
        ).toBeGreaterThan(textIn(prompt).indexOf('working'));
        expect(textIn(prompt)).not.toContain('stop');
      }),
  );
});
