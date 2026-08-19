import { describe, expect, it } from '@effect/vitest';
import {
  Context,
  Effect,
  ExecutionPlan,
  Layer,
  Ref,
  Schema,
  Stream,
} from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { ModelPlan } from '../src/model-plan.js';

class RetryGate extends Context.Service<
  RetryGate,
  { readonly enabled: boolean }
>()('ModelPlanTest/RetryGate') {}

const finish = (): Response.FinishPartEncoded => ({
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const providerFailure = () =>
  AiError.make({
    module: 'ModelPlanTest',
    method: 'generate',
    reason: new AiError.InternalProviderError({
      description: 'primary unavailable',
    }),
  });

const succeedingModel = (calls: Ref.Ref<number>, text: string) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () =>
        Ref.update(calls, (count) => count + 1).pipe(
          Effect.as<Response.PartEncoded[]>([{ type: 'text', text }, finish()]),
        ),
      streamText: () =>
        Stream.fromIterable<Response.StreamPartEncoded>([
          { type: 'text-start', id: 'answer' },
          { type: 'text-delta', id: 'answer', delta: text },
          { type: 'text-end', id: 'answer' },
          finish(),
        ]).pipe(Stream.onStart(Ref.update(calls, (count) => count + 1))),
    }),
  );

const failingModel = (calls: Ref.Ref<number>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () =>
        Ref.update(calls, (count) => count + 1).pipe(
          Effect.andThen(Effect.fail(providerFailure())),
        ),
      streamText: () =>
        Stream.fail(providerFailure()).pipe(
          Stream.onStart(Ref.update(calls, (count) => count + 1)),
        ),
    }),
  );

const compileTimeAssertions = () => {
  const calls = Ref.makeUnsafe(0);
  const model = succeedingModel(calls, 'ok');
  const wrongErrorPlan = ExecutionPlan.make({
    provide: model,
    while: (_error: 'not-an-ai-error') => true,
  });
  const noModelPlan = ExecutionPlan.make({ provide: Layer.empty });

  ModelPlan.layer(
    ExecutionPlan.make({
      provide: model,
      while: ModelPlan.when((error) => {
        const exactError: AiError.AiError = error;
        return exactError.isRetryable;
      }),
    }),
  );
  const effectfulPredicate = ModelPlan.layer(
    ExecutionPlan.make({
      provide: model,
      attempts: 2,
      while: ModelPlan.when(() => Effect.as(RetryGate, true)),
    }),
  );
  const requiresRetryGate: Layer.Layer<
    LanguageModel.LanguageModel,
    never,
    RetryGate
  > = effectfulPredicate;
  void requiresRetryGate;

  // @ts-expect-error predicates cannot replace AiError with another input
  ModelPlan.when((_error: 'not-an-ai-error') => true);
  // @ts-expect-error model plans only inspect semantic AI failures
  ModelPlan.layer(wrongErrorPlan);
  // @ts-expect-error every model plan step must provide LanguageModel
  ModelPlan.layer(noModelPlan);
};
void compileTimeAssertions;

describe('ModelPlan', () => {
  it.effect('falls back when a model fails before producing output', () =>
    Effect.gen(function* () {
      const primaryCalls = yield* Ref.make(0);
      const fallbackCalls = yield* Ref.make(0);
      const plan = ExecutionPlan.make(
        { provide: failingModel(primaryCalls) },
        { provide: succeedingModel(fallbackCalls, 'fallback') },
      );

      const response = yield* LanguageModel.generateText({
        prompt: 'answer',
      }).pipe(Effect.provide(ModelPlan.layer(plan)));

      expect(response.text).toBe('fallback');
      expect(yield* Ref.get(primaryCalls)).toBe(1);
      expect(yield* Ref.get(fallbackCalls)).toBe(1);
    }),
  );

  it.effect('falls back when structured object generation fails', () =>
    Effect.gen(function* () {
      const primaryCalls = yield* Ref.make(0);
      const fallbackCalls = yield* Ref.make(0);
      const plan = ExecutionPlan.make(
        { provide: failingModel(primaryCalls) },
        {
          provide: succeedingModel(
            fallbackCalls,
            JSON.stringify({ answer: 'fallback' }),
          ),
        },
      );

      const response = yield* LanguageModel.generateObject({
        prompt: 'answer',
        schema: Schema.Struct({ answer: Schema.String }),
      }).pipe(Effect.provide(ModelPlan.layer(plan)));

      expect(response.value).toEqual({ answer: 'fallback' });
      expect(yield* Ref.get(primaryCalls)).toBe(1);
      expect(yield* Ref.get(fallbackCalls)).toBe(1);
    }),
  );

  it.effect('falls back a stream only before its first emitted part', () =>
    Effect.gen(function* () {
      const primaryCalls = yield* Ref.make(0);
      const fallbackCalls = yield* Ref.make(0);
      const plan = ExecutionPlan.make(
        { provide: failingModel(primaryCalls) },
        { provide: succeedingModel(fallbackCalls, 'fallback') },
      );

      const parts = yield* LanguageModel.streamText({
        prompt: 'answer',
      }).pipe(Stream.runCollect, Effect.provide(ModelPlan.layer(plan)));
      const text = parts
        .filter((part) => part.type === 'text-delta')
        .map((part) => part.delta)
        .join('');

      expect(text).toBe('fallback');
      expect(yield* Ref.get(primaryCalls)).toBe(1);
      expect(yield* Ref.get(fallbackCalls)).toBe(1);
    }),
  );

  it.effect('never splices a fallback into a partially emitted stream', () =>
    Effect.gen(function* () {
      const primaryCalls = yield* Ref.make(0);
      const fallbackCalls = yield* Ref.make(0);
      const partial = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.fail(providerFailure()),
          streamText: () =>
            Stream.fromIterable<Response.StreamPartEncoded>([
              { type: 'text-start', id: 'partial' },
              { type: 'text-delta', id: 'partial', delta: 'visible' },
            ]).pipe(
              Stream.concat(Stream.fail(providerFailure())),
              Stream.onStart(Ref.update(primaryCalls, (count) => count + 1)),
            ),
        }),
      );
      const plan = ExecutionPlan.make(
        { provide: partial },
        { provide: succeedingModel(fallbackCalls, 'must-not-run') },
      );

      const error = yield* LanguageModel.streamText({
        prompt: 'answer',
      }).pipe(
        Stream.runDrain,
        Effect.provide(ModelPlan.layer(plan)),
        Effect.flip,
      );

      expect(AiError.isAiError(error)).toBe(true);
      expect(yield* Ref.get(primaryCalls)).toBe(1);
      expect(yield* Ref.get(fallbackCalls)).toBe(0);
    }),
  );

  it.effect('preserves typed tool failures without trying another model', () =>
    Effect.gen(function* () {
      const primaryCalls = yield* Ref.make(0);
      const fallbackCalls = yield* Ref.make(0);
      const blocked = Tool.make('blocked', {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.Literal('handler-blocked'),
        failureMode: 'error',
      });
      const toolkit = Toolkit.make(blocked);
      const handlers = toolkit.toLayer(
        toolkit.of({
          blocked: () => Effect.fail('handler-blocked'),
        }),
      );
      const aiErrorHandlers = toolkit.toLayer(
        toolkit.of({
          blocked: () => Effect.fail(providerFailure()),
        }),
      );
      const primary = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () =>
            Ref.update(primaryCalls, (count) => count + 1).pipe(
              Effect.as<Response.PartEncoded[]>([
                {
                  type: 'tool-call',
                  id: 'blocked-call',
                  name: 'blocked',
                  params: {},
                },
                finish(),
              ]),
            ),
          streamText: () => Stream.empty,
        }),
      );
      const plan = ExecutionPlan.make(
        { provide: primary },
        { provide: succeedingModel(fallbackCalls, 'must-not-run') },
      );

      const error = yield* LanguageModel.generateText({
        prompt: 'call the tool',
        toolkit,
      }).pipe(
        Effect.provide(Layer.merge(ModelPlan.layer(plan), handlers)),
        Effect.flip,
      );
      const exactError: AiError.AiError | 'handler-blocked' = error;

      const aiError = yield* LanguageModel.generateText({
        prompt: 'call the tool',
        toolkit,
      }).pipe(
        Effect.provide(Layer.merge(ModelPlan.layer(plan), aiErrorHandlers)),
        Effect.flip,
      );

      const streamingPrimaryCalls = yield* Ref.make(0);
      const streamingFallbackCalls = yield* Ref.make(0);
      const streamingPrimary = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([finish()]),
          streamText: () =>
            Stream.fromIterable<Response.StreamPartEncoded>([
              {
                type: 'tool-call',
                id: 'streaming-blocked-call',
                name: 'blocked',
                params: {},
              },
              finish(),
            ]).pipe(
              Stream.onStart(
                Ref.update(streamingPrimaryCalls, (count) => count + 1),
              ),
            ),
        }),
      );
      const streamingPlan = ExecutionPlan.make(
        { provide: streamingPrimary },
        {
          provide: succeedingModel(streamingFallbackCalls, 'must-not-stream'),
        },
      );
      const streamingError = yield* LanguageModel.streamText({
        prompt: 'call the tool',
        toolkit,
      }).pipe(
        Stream.runDrain,
        Effect.provide(
          Layer.merge(ModelPlan.layer(streamingPlan), aiErrorHandlers),
        ),
        Effect.flip,
      );

      expect(exactError).toBe('handler-blocked');
      expect(AiError.isAiError(aiError)).toBe(true);
      expect(yield* Ref.get(primaryCalls)).toBe(2);
      expect(yield* Ref.get(fallbackCalls)).toBe(0);
      expect(AiError.isAiError(streamingError)).toBe(true);
      expect(yield* Ref.get(streamingPrimaryCalls)).toBe(1);
      expect(yield* Ref.get(streamingFallbackCalls)).toBe(0);
    }),
  );
});
