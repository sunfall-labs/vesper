import { describe, expect, it } from '@effect/vitest';
import { Effect, Queue, Ref } from 'effect';
import { type Response, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { ScriptedModel } from '../src/testing.js';
import { TurnControl } from '../src/turn-control.js';

const finish = (): Response.FinishPartEncoded => ({
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const textTurn = (
  id: string,
  text: string,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id },
  { type: 'text-delta', id, delta: text },
  { type: 'text-end', id },
  finish(),
];

describe('Agent next-turn control', () => {
  it.effect(
    'can follow up and select the Effect AI model for later turns',
    () =>
      Effect.gen(function* () {
        const primary = ScriptedModel.make([textTurn('one', 'first')]);
        const followUp = ScriptedModel.make([textTurn('two', 'second')]);
        const decisions = yield* Ref.make(0);

        const agent = Agent.make({
          name: 'controlled',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
          nextTurn: () =>
            Effect.gen(function* () {
              const count = yield* Ref.getAndUpdate(
                decisions,
                (current) => current + 1,
              );
              if (count > 0) {
                return TurnControl.keep;
              }
              const service = yield* followUp.service;
              return TurnControl.continueWith('check your answer', {
                model: service,
              });
            }),
        });

        const result = yield* agent
          .run('start')
          .pipe(Effect.provide(primary.layer));
        const primaryRequests = yield* primary.requests;
        const followUpRequests = yield* followUp.requests;

        expect(result.text).toBe('second');
        expect(result.steps).toBe(2);
        expect(primaryRequests).toHaveLength(1);
        expect(followUpRequests).toHaveLength(1);
        expect(followUpRequests[0]?.prompt.content.at(-1)).toMatchObject({
          role: 'user',
          content: [{ type: 'text', text: 'check your answer' }],
        });
      }),
  );

  it.effect('returns Effect AI canonical response messages from run', () =>
    Effect.gen(function* () {
      const service = ScriptedModel.make([
        [
          { type: 'reasoning-start', id: 'r' },
          { type: 'reasoning-delta', id: 'r', delta: 'considered' },
          { type: 'reasoning-end', id: 'r' },
          ...textTurn('t', 'answered'),
        ],
      ]);
      const agent = Agent.make({
        name: 'response',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
      });

      const result = yield* agent
        .run('start')
        .pipe(Effect.provide(service.layer));

      expect(result.response?.content).toHaveLength(1);
      expect(result.response?.content[0]).toMatchObject({
        role: 'assistant',
        content: [
          { type: 'reasoning', text: 'considered' },
          { type: 'text', text: 'answered' },
        ],
      });
    }),
  );

  it.effect('drains follow-ups only when the agent would otherwise stop', () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<string>();
      yield* Queue.offerAll(queue, ['first follow-up', 'second follow-up']);
      const service = ScriptedModel.make([
        textTurn('one', 'initial'),
        textTurn('two', 'after first'),
        textTurn('three', 'after second'),
      ]);
      const agent = Agent.make({
        name: 'follow-ups',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        nextTurn: TurnControl.followUps(queue),
      });

      const result = yield* agent
        .run('start')
        .pipe(Effect.provide(service.layer));
      const requests = yield* service.requests;

      expect(result.steps).toBe(3);
      const lastUserText = requests.map((request) => {
        const message = request.prompt.content.at(-1);
        if (message?.role !== 'user') {
          return undefined;
        }
        return message.content
          .flatMap((part) => (part.type === 'text' ? [part.text] : []))
          .join('');
      });
      expect(lastUserText).toEqual([
        'start',
        'first follow-up',
        'second follow-up',
      ]);
    }),
  );
});
