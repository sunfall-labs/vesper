import { Effect, Metric } from 'effect';
import type { Response } from 'effect/unstable/ai';

/**
 * Agent-level metrics deliberately have no dynamic attributes. Conversation,
 * run, and tool-call identifiers belong on spans; putting them on metrics
 * would create an unbounded time-series cardinality problem.
 */
export const modelCalls = Metric.counter('vesper_agent_model_calls', {
  description: 'Language-model calls, including compaction and retries',
  incremental: true,
});

export const modelInputTokens = Metric.counter(
  'vesper_agent_model_input_tokens',
  {
    description: 'Input tokens reported by language-model calls',
    incremental: true,
  },
);

export const modelOutputTokens = Metric.counter(
  'vesper_agent_model_output_tokens',
  {
    description: 'Output tokens reported by language-model calls',
    incremental: true,
  },
);

export const modelUncachedInputTokens = Metric.counter(
  'vesper_agent_model_uncached_input_tokens',
  {
    description: 'Uncached input tokens reported by language-model calls',
    incremental: true,
  },
);

export const modelCacheReadTokens = Metric.counter(
  'vesper_agent_model_cache_read_tokens',
  {
    description: 'Input tokens read from provider prompt caches',
    incremental: true,
  },
);

export const modelCacheWriteTokens = Metric.counter(
  'vesper_agent_model_cache_write_tokens',
  {
    description: 'Input tokens written to provider prompt caches',
    incremental: true,
  },
);

export const toolCalls = Metric.counter('vesper_agent_tool_calls', {
  description: 'Tool dispatch attempts',
  incremental: true,
});

export const toolFailures = Metric.counter('vesper_agent_tool_failures', {
  description: 'Tool calls that return a failure or fail during execution',
  incremental: true,
});

export const recoveredToolCalls = Metric.counter(
  'vesper_agent_recovered_tool_calls',
  {
    description: 'Tool calls served from durable recovery',
    incremental: true,
  },
);

export const indeterminateToolCalls = Metric.counter(
  'vesper_agent_indeterminate_tool_calls',
  {
    description: 'Tool calls requiring an indeterminate-execution decision',
    incremental: true,
  },
);

export const waitsSuspended = Metric.counter('vesper_agent_waits_suspended', {
  description: 'Tool calls entering an external wait',
  incremental: true,
});

export const waitsCompleted = Metric.counter('vesper_agent_waits_completed', {
  description: 'External waits completed and durably recorded',
  incremental: true,
});

export const waitsRestarted = Metric.counter('vesper_agent_waits_restarted', {
  description: 'External waits re-entered after recovery',
  incremental: true,
});

export const compactions = Metric.counter('vesper_agent_compactions', {
  description: 'History compactions that replaced messages',
  incremental: true,
});

export const one = (metric: Metric.Counter<number>): Effect.Effect<void> =>
  Metric.update(metric, 1);

export const usage = (reported: Response.Usage): Effect.Effect<void> =>
  Effect.all(
    [
      Metric.update(modelInputTokens, reported.inputTokens.total ?? 0),
      Metric.update(modelOutputTokens, reported.outputTokens.total ?? 0),
      Metric.update(
        modelUncachedInputTokens,
        reported.inputTokens.uncached ?? 0,
      ),
      Metric.update(modelCacheReadTokens, reported.inputTokens.cacheRead ?? 0),
      Metric.update(
        modelCacheWriteTokens,
        reported.inputTokens.cacheWrite ?? 0,
      ),
    ],
    { discard: true },
  );

export const modelCall = one(modelCalls);
export const toolCall = one(toolCalls);
export const toolFailure = one(toolFailures);
export const recoveredToolCall = one(recoveredToolCalls);
export const indeterminateToolCall = one(indeterminateToolCalls);
export const waitSuspended = one(waitsSuspended);
export const waitCompleted = one(waitsCompleted);
export const waitRestarted = one(waitsRestarted);
export const compaction = one(compactions);
