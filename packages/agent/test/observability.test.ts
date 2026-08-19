import { Effect, Metric, Stream } from 'effect';
import { Toolkit, type Response } from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import * as Observability from '../src/internal/observability.js';
import { ScriptedModel } from '../src/testing.js';

describe('agent observability metrics', () => {
  it.effect('records provider cache usage through Agent.stream', () =>
    Effect.gen(function* () {
      const before = {
        calls: yield* Metric.value(Observability.modelCalls),
        input: yield* Metric.value(Observability.modelInputTokens),
        output: yield* Metric.value(Observability.modelOutputTokens),
        uncached: yield* Metric.value(Observability.modelUncachedInputTokens),
        cacheRead: yield* Metric.value(Observability.modelCacheReadTokens),
        cacheWrite: yield* Metric.value(Observability.modelCacheWriteTokens),
      };
      const model = ScriptedModel.make([
        [
          {
            type: 'finish',
            reason: 'stop',
            usage: {
              inputTokens: {
                total: 13,
                uncached: 4,
                cacheRead: 7,
                cacheWrite: 2,
              },
              outputTokens: { total: 5 },
            },
          },
        ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
      ]);
      const agent = Agent.make({
        name: 'observability-test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
      });

      yield* agent
        .stream('hi')
        .pipe(Stream.runDrain, Effect.provide(model.layer));

      expect(
        (yield* Metric.value(Observability.modelCalls)).count -
          before.calls.count,
      ).toBe(1);
      expect(
        (yield* Metric.value(Observability.modelInputTokens)).count -
          before.input.count,
      ).toBe(13);
      expect(
        (yield* Metric.value(Observability.modelOutputTokens)).count -
          before.output.count,
      ).toBe(5);
      expect(
        (yield* Metric.value(Observability.modelUncachedInputTokens)).count -
          before.uncached.count,
      ).toBe(4);
      expect(
        (yield* Metric.value(Observability.modelCacheReadTokens)).count -
          before.cacheRead.count,
      ).toBe(7);
      expect(
        (yield* Metric.value(Observability.modelCacheWriteTokens)).count -
          before.cacheWrite.count,
      ).toBe(2);
    }).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
  );
});
