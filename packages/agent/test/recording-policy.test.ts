import { describe, expect, it } from '@effect/vitest';
import { Context, Effect, Layer, Schema, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interaction } from '../src/interaction.js';
import type { RecordingPolicy } from '../src/recording-policy.js';
import {
  finish,
  plainAgent,
  present,
  provider,
  readRecords,
  run,
  says,
  textIn,
} from './durability-fixtures.js';

// Recording policy filters what is persisted. It must never change what the
// model is shown, and it must reach every record family that can carry a
// secret.

class Redactor extends Context.Service<
  Redactor,
  { readonly replacement: string }
>()('recording-policy-test/Redactor') {}

const Question = Interaction.answer(
  Tool.make('question', {
    description: 'ask the user for information',
    parameters: Schema.Struct({ question: Schema.String }),
    success: Schema.Struct({ answer: Schema.String }),
    failureMode: 'return',
  }),
);

const askingAgent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'ask through the question tool',
  toolkit: Toolkit.make(Question),
}).withHandlers({ question: () => Interaction.unreachable });

const asksQuestion: Response.StreamPartEncoded[] = [
  {
    type: 'tool-call',
    id: 'question-call',
    name: 'question',
    params: { question: 'secret question' },
  },
  finish('tool-calls'),
];

describe('recording policy', () => {
  it.effect('filters persistence without changing the live model prompt', () =>
    Effect.gen(function* () {
      const policy = {
        prompt: () =>
          Effect.map(Redactor, ({ replacement }) => [
            { role: 'user' as const, content: replacement },
          ]),
      } satisfies RecordingPolicy.Policy<Redactor>;
      const models = provider([says('live secret')]);

      const records = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(plainAgent, 'redacted', policy).run(
            'input secret',
          );
          return yield* readRecords('redacted');
        }).pipe(
          Effect.provideService(Redactor, { replacement: '[redacted prompt]' }),
        ),
        models.layer,
      );

      expect(textIn(present(models.asked[0]))).toContain('input secret');
      const persisted = JSON.stringify(records[0]?.record);
      expect(persisted).toContain('[redacted prompt]');
      expect(persisted).not.toContain('input secret');
    }),
  );

  it.effect(
    'covers tool parameters, results, waits and their answers, and delivered signals',
    () =>
      Effect.gen(function* () {
        const policy: RecordingPolicy.Policy = {
          toolParameters: () => Effect.succeed({ redacted: 'params' }),
          toolResult: () => Effect.succeed({ redacted: 'result' }),
          externalRequest: () =>
            Effect.succeed({ redacted: 'external request' }),
          externalResult: () => Effect.succeed({ answer: '[redacted answer]' }),
          signal: (signal) =>
            Effect.succeed({ ...signal, text: '[redacted signal]' }),
          cause: () => Effect.succeed('[redacted cause]'),
        };
        const models = provider([asksQuestion, says('done')]);
        const conversation = Conversation.make(askingAgent, 'covered', policy);

        const written = yield* run(
          Effect.gen(function* () {
            const suspended = yield* conversation.run('ask me');
            expect(suspended.outcome).toBe('suspended');
            yield* conversation.send({
              kind: 'steer',
              text: 'secret steer',
              source: 'operator',
            });
            yield* conversation.resolveInteraction(Question, 'question-call', {
              answer: 'secret answer',
            });
            yield* conversation.run();
            return yield* readRecords('covered');
          }),
          models.layer,
        );

        const of = (tag: string) =>
          written.flatMap(({ record }) =>
            record._tag === tag ? [record] : [],
          );
        expect(of('ToolCall')).toMatchObject([
          { params: { redacted: 'params' } },
        ]);
        expect(of('ToolSuspended')).toMatchObject([
          { request: { redacted: 'external request' } },
        ]);
        expect(of('ToolOutcome')).toMatchObject([
          { result: { redacted: 'result' } },
        ]);
        // The caller's answer is written by `resolveInteraction`, outside any
        // run, and still passes through the conversation's policy.
        expect(of('ToolWaitCompleted')).toMatchObject([
          { result: { answer: '[redacted answer]' } },
        ]);
        expect(of('SignalReceived')).toMatchObject([
          { text: '[redacted signal]' },
        ]);
        const persisted = JSON.stringify(written);
        expect(persisted).not.toContain('secret question');
        expect(persisted).not.toContain('secret answer');
      }),
  );

  it.effect('redacts the rendered cause of a failed run', () =>
    Effect.gen(function* () {
      const failing = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () =>
            Effect.succeed<Response.PartEncoded[]>([finish()]),
          streamText: () =>
            Stream.fail(
              AiError.make({
                module: 'test',
                method: 'streamText',
                reason: new AiError.InternalProviderError({
                  description: 'secret stack',
                }),
              }),
            ),
        }),
      );
      const written = yield* run(
        Effect.gen(function* () {
          yield* Conversation.make(plainAgent, 'failed', {
            cause: () => Effect.succeed('[redacted cause]'),
          })
            .run('hi')
            .pipe(Effect.exit);
          return yield* readRecords('failed');
        }),
        failing,
      );

      expect(written.at(-1)?.record).toMatchObject({
        _tag: 'RunSettled',
        outcome: 'failure',
        detail: '[redacted cause]',
      });
    }),
  );
});
