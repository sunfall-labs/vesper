// The one workload both sides run.
//
// Every constant here is shared so neither side gets a different amount of
// work to do. If a number is only meaningful to one framework it does not
// belong in this file — it belongs in that side's runner, named, and in the
// report's "not a fair comparison" list.

/** System prompt. Identical text on both sides, so prompt assembly is equal. */
export const INSTRUCTIONS = [
  'You are a benchmark agent for an online store.',
  'Look up each order you are asked about before answering.',
  'Answer briefly.',
].join('\n');

export const TOOL_NAME = 'lookup_order';

export const TOOL_DESCRIPTION = 'Look up the fulfilment status of one order.';

export const USER_MESSAGE = 'where are my orders?';

export const FINAL_TEXT = 'All of your orders have shipped.';

/** A fixed payload whose provider chunking is varied by the parts scenario. */
export const PART_OUTPUT_BYTES = 10_000;

export const PART_COUNTS: ReadonlyArray<number> = [1, 100, 10_000];

export const partText = (parts: number): ReadonlyArray<string> => {
  if (PART_OUTPUT_BYTES % parts !== 0) {
    throw new Error(
      `${String(PART_OUTPUT_BYTES)} output bytes cannot be split into ${String(parts)} parts`,
    );
  }
  return Array.from({ length: parts }, () =>
    'x'.repeat(PART_OUTPUT_BYTES / parts),
  );
};

/**
 * How the scripted provider answers, turn by turn.
 *
 * `steps` model calls: the first `steps - 1` each request one tool call, the
 * last returns text and ends the loop. Both sides stop the same way — because
 * the model stopped asking for tools, not because the step ceiling fired. Keep
 * `steps` under 32 (the default `Stop.maxSteps`) so that stays true.
 */
export interface ScriptStep {
  readonly kind: 'tool' | 'text';
  readonly orderId: string;
}

export const script = (steps: number): ReadonlyArray<ScriptStep> =>
  Array.from({ length: steps }, (_, index) => ({
    kind: index < steps - 1 ? ('tool' as const) : ('text' as const),
    orderId: `order_${String(index)}`,
  }));

/** The tool's reply. Same string, same shape, same declared output schema. */
export const toolResult = (orderId: string): { status: string } => ({
  status: `${orderId}: shipped`,
});

// ------------------------------------------------------------------ config

/** Model calls per submission for the per-turn scenario. */
export const TURN_STEPS = 8;

/** Model calls per submission in the multi-message scenarios. */
export const CONVERSATION_STEPS = 3;

/** Messages sent to one conversation in the conversation/memory scenarios. */
export const CONVERSATION_MESSAGES = 30;

export const MEMORY_MESSAGES = 100;

/** Iterations kept, after discarding warmup. */
const smoke = process.env['VESPER_BENCH_SMOKE'] === '1';

export const HEAVY = process.env['VESPER_BENCH_HEAVY'] === '1';

export const ITERATIONS = smoke ? 2 : 30;

/** Iterations run and thrown away before measuring, to let the JIT settle. */
export const WARMUP = smoke ? 1 : 5;

/**
 * Turn counts the `scaling` scenario sweeps within a single submission.
 *
 * The point is the shape, not any one number: a loop whose per-turn cost is
 * flat across this sweep is doing work proportional to the turn, and one whose
 * per-turn cost climbs is doing work proportional to the history so far. That
 * distinction survives hardware changes in a way a headline millisecond does
 * not. Capped at 16 because our default `Stop.maxSteps` is 32.
 */
export const SCALING_TURNS: ReadonlyArray<number> = [1, 2, 4, 8, 16];

export const SCALING_ITERATIONS = smoke ? 2 : 20;

export const PART_ITERATIONS = smoke ? 1 : 10;

export const BACKPRESSURE_ITERATIONS = smoke ? 1 : 5;

export const BACKPRESSURE_PARTS = 100;

export const BACKPRESSURE_DELAY_MS = 1;

/** Default exposes 1k; the 10k fixture is deliberately heavy and opt-in. */
export const HISTORY_RECORD_COUNTS: ReadonlyArray<number> = HEAVY
  ? [1_000, 10_000]
  : [1_000];

export const HISTORY_ITERATIONS = smoke ? 1 : 10;

/**
 * Messages in the `growth` scenario, and how many fresh conversations it
 * averages over.
 *
 * `scaling` holds history at zero and varies turns; this holds turns constant
 * and lets history grow. Together they say which of the two a framework's cost
 * actually tracks — the question the `conversation` row raises but cannot
 * answer on its own.
 */
export const GROWTH_MESSAGES = 40;

export const GROWTH_REPEATS = smoke ? 1 : 8;

export type ScenarioName =
  | 'turn'
  | 'conversation'
  | 'startup'
  | 'memory'
  | 'scaling'
  | 'growth'
  | 'parts'
  | 'history-open'
  | 'backpressure';

export type SideName = 'vesper' | 'vesper+log';

/** One side's answer for one scenario. Both runners produce exactly this. */
export interface ScenarioResult {
  readonly side: SideName;
  readonly scenario: ScenarioName;
  /** Milliseconds per iteration, warmup already discarded. */
  readonly samples: ReadonlyArray<number>;
  /** What one sample covers, e.g. `8 model turns`. For the report's honesty. */
  readonly unit: string;
  /** Divide a sample by this to get a per-turn figure, when meaningful. */
  readonly turnsPerSample?: number;
  /** Heap bytes after a forced GC, for the memory scenario. */
  readonly heapBytes?: number;
  /**
   * Resident set bytes after a forced GC.
   *
   * Reported alongside the heap because a backend that stores conversations
   * outside the JS heap — a native SQLite page cache, a memory-mapped file —
   * never shows up in `heapUsed` at all. The memory backend measured here is
   * a plain JS structure that does, so heap alone would flatter any
   * alternative that keeps its bytes elsewhere.
   */
  readonly rssBytes?: number;
  /** Model calls the scripted provider actually served. A contamination check. */
  readonly modelCalls?: number;
  /** Cold-start milestones, for the startup scenario. */
  readonly constructMs?: number;
  readonly firstPartMs?: number;
  /** Per-turn-count timings, for the scaling scenario. */
  readonly series?: ReadonlyArray<{
    readonly turns: number;
    readonly samples: ReadonlyArray<number>;
  }>;
  /** Per-message timings as one conversation grows, for the growth scenario. */
  readonly growth?: ReadonlyArray<{
    readonly index: number;
    readonly samples: ReadonlyArray<number>;
  }>;
  /** Same output bytes delivered with different provider delta counts. */
  readonly partSeries?: ReadonlyArray<{
    readonly parts: number;
    readonly samples: ReadonlyArray<number>;
  }>;
  /** Time to open a pre-populated conversation through AgentLog.open. */
  readonly historySeries?: ReadonlyArray<{
    readonly mode: 'uncompacted' | 'compacted-fixed-tail' | 'orphan-suffix';
    readonly records: number;
    readonly liveRecords: number;
    readonly samples: ReadonlyArray<number>;
    readonly pages: number;
    readonly recordsRead: number;
  }>;
  /** Text deltas observed by the deliberately slow consumer. */
  readonly consumedParts?: number;
  readonly consumerDelayMs?: number;
}
