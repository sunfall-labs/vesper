import { Effect, Metric } from 'effect';

import type { Stop } from '../stop.js';

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

export const usage = (reported: Stop.Usage): Effect.Effect<void> =>
  Effect.all(
    [
      Metric.update(modelInputTokens, reported.input),
      Metric.update(modelOutputTokens, reported.output),
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
