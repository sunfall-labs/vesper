import { Effect, Metric } from 'effect';
import { describe, expect, it } from '@effect/vitest';

import * as Observability from '../src/internal/observability.js';

describe('agent observability metrics', () => {
  it.effect('records model calls and reported token usage', () =>
    Effect.gen(function* () {
      const beforeCalls = yield* Metric.value(Observability.modelCalls);
      const beforeInput = yield* Metric.value(Observability.modelInputTokens);
      const beforeOutput = yield* Metric.value(Observability.modelOutputTokens);
      const beforeUncached = yield* Metric.value(
        Observability.modelUncachedInputTokens,
      );
      const beforeCacheRead = yield* Metric.value(
        Observability.modelCacheReadTokens,
      );
      const beforeCacheWrite = yield* Metric.value(
        Observability.modelCacheWriteTokens,
      );

      yield* Observability.modelCall;
      yield* Observability.usage({
        inputTokens: {
          total: 13,
          uncached: 4,
          cacheRead: 7,
          cacheWrite: 2,
        },
        outputTokens: { total: 5 },
      });

      expect(
        (yield* Metric.value(Observability.modelCalls)).count -
          beforeCalls.count,
      ).toBe(1);
      expect(
        (yield* Metric.value(Observability.modelInputTokens)).count -
          beforeInput.count,
      ).toBe(13);
      expect(
        (yield* Metric.value(Observability.modelOutputTokens)).count -
          beforeOutput.count,
      ).toBe(5);
      expect(
        (yield* Metric.value(Observability.modelUncachedInputTokens)).count -
          beforeUncached.count,
      ).toBe(4);
      expect(
        (yield* Metric.value(Observability.modelCacheReadTokens)).count -
          beforeCacheRead.count,
      ).toBe(7);
      expect(
        (yield* Metric.value(Observability.modelCacheWriteTokens)).count -
          beforeCacheWrite.count,
      ).toBe(2);
    }),
  );
});
