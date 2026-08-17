import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { describe, expect, it } from '@effect/vitest';
import { Context, Effect, Layer, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { AgentLog } from '../src/log.js';
import { RecordingPolicy } from '../src/recording-policy.js';
import { RecordingPolicyRuntime } from '../src/recording-policy-runtime.js';

const finish: Response.FinishPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

const model = (seen: string[]) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([finish]),
      streamText: (options) => {
        seen.push(JSON.stringify(options.prompt));
        return Stream.make(
          { type: 'text-start', id: 'a' },
          { type: 'text-delta', id: 'a', delta: 'live secret' },
          { type: 'text-end', id: 'a' },
          finish,
        );
      },
    }),
  );

class Redactor extends Context.Service<
  Redactor,
  { readonly replacement: string }
>()('recording-policy-test/Redactor') {}

const policy = {
  prompt: () =>
    Effect.map(Redactor, ({ replacement }) => [
      { role: 'user' as const, content: replacement },
    ]),
  cause: () => Effect.succeed('[redacted cause]'),
} satisfies RecordingPolicy.Policy<Redactor>;

describe('recording policy', () => {
  it.effect('filters persistence without changing the live model prompt', () =>
    Effect.gen(function* () {
      const seen: string[] = [];
      const conversation = 'redacted';
      const agent = Agent.make({
        name: 'redacted',
        revision: '1',
        instructions: 'answer',
        toolkit: Toolkit.make(),
      });

      const records = yield* Effect.gen(function* () {
        yield* agent.recordingTo(conversation, policy).run('input secret');
        const store = yield* LogStore.Service;
        return (yield* store.read(
          AgentLog.pathFor(LogVocabulary.ConversationId.make(conversation)),
          {
            limit: 100,
          },
        )).records;
      }).pipe(
        Effect.provideService(Redactor, { replacement: '[redacted prompt]' }),
        Effect.provide(model(seen)),
        Effect.provide(LogStoreMemory.layer),
      );

      expect(seen[0]).toContain('input secret');
      const started = records.find(
        ({ record }) => record._tag === 'RunStarted',
      );
      expect(JSON.stringify(started?.record)).toContain('[redacted prompt]');
      expect(JSON.stringify(started?.record)).not.toContain('input secret');
    }),
  );

  it.effect(
    'covers tool parameters/results, delivered signals, and rendered causes',
    () =>
      Effect.gen(function* () {
        const filtered = yield* Effect.gen(function* () {
          const context = yield* Effect.context<never>();
          const runtime = RecordingPolicyRuntime.compile(
            {
              toolParameters: () => Effect.succeed({ redacted: 'params' }),
              toolResult: () => Effect.succeed({ redacted: 'result' }),
              signal: (signal) =>
                Effect.succeed({ ...signal, text: '[redacted signal]' }),
              cause: () => Effect.succeed('[redacted cause]'),
            },
            context,
          );
          return yield* Effect.forEach(
            [
              {
                _tag: 'ToolCall' as const,
                step: 1,
                id: LogVocabulary.ToolCallId.make('a'),
                name: 'tool',
                params: { secret: true },
              },
              {
                _tag: 'ToolOutcome' as const,
                step: 1,
                id: LogVocabulary.ToolCallId.make('a'),
                name: 'tool',
                outcome: 'success' as const,
                result: { secret: true },
              },
              {
                _tag: 'ToolStarted' as const,
                id: LogVocabulary.ToolCallId.make('a'),
                name: 'tool',
              },
              {
                _tag: 'SignalReceived' as const,
                kind: 'steer' as const,
                text: 'secret',
                source: 'operator',
                step: 1,
                at: '00000000000000000001' as never,
              },
              {
                _tag: 'RunSettled' as const,
                outcome: 'failure' as const,
                detail: 'secret stack',
                steps: 1,
                usage: { input: 1, output: 1 },
              },
            ],
            runtime.filter,
          );
        });

        expect(filtered).toMatchObject([
          { params: { redacted: 'params' } },
          { result: { redacted: 'result' } },
          { _tag: 'ToolStarted', id: 'a', name: 'tool' },
          { text: '[redacted signal]' },
          { detail: '[redacted cause]' },
        ]);
      }),
  );
});
