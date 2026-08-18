import { Effect, Metric } from 'effect';
import { describe, expect, it } from '@effect/vitest';

import * as Observability from '../src/internal/observability.js';

describe('agent observability metrics', () => {
  it.effect('records model calls and reported token usage', () =>
    Effect.gen(function* () {
      const beforeCalls = yield* Metric.value(Observability.modelCalls);
      const beforeInput = yield* Metric.value(Observability.modelInputTokens);
      const beforeOutput = yield* Metric.value(Observability.modelOutputTokens);

      yield* Observability.modelCall;
      yield* Observability.usage({ input: 13, output: 5 });

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
    }),
  );
});
