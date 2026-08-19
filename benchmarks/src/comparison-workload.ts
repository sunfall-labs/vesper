import { ITERATIONS, WARMUP } from './workload.ts';

export type ComparisonSide = 'vesper+log' | 'flue@2.0.3';
export type ComparisonWorkload =
  | 'one-turn'
  | 'tool-loop'
  | 'startup'
  | 'growth'
  | 'memory'
  | 'concurrency';

export const COMPARISON_ITERATIONS = ITERATIONS;
export const COMPARISON_WARMUP = WARMUP;

export const COMPARISON_GROWTH_MESSAGES = 30;
export const COMPARISON_GROWTH_REPEATS =
  process.env['VESPER_BENCH_SMOKE'] === '1' ? 1 : 7;
export const COMPARISON_MEMORY_MESSAGES = 60;
export const COMPARISON_CONCURRENCY = 16;
export const COMPARISON_CONCURRENCY_ITERATIONS =
  process.env['VESPER_BENCH_SMOKE'] === '1' ? 1 : 10;
export const COMPARISON_CONCURRENCY_WARMUP =
  process.env['VESPER_BENCH_SMOKE'] === '1' ? 0 : 2;
export const COMPARISON_PROCESS_REPEATS =
  process.env['VESPER_BENCH_SMOKE'] === '1' ? 1 : 7;

export const stepsFor = (workload: ComparisonWorkload): number =>
  workload === 'tool-loop' ? 8 : 1;

export interface ComparisonResult {
  readonly side: ComparisonSide;
  readonly workload: ComparisonWorkload;
  readonly samples: ReadonlyArray<number>;
  readonly modelCalls: number;
  readonly callsPerSample?: number;
  readonly growth?: ReadonlyArray<{
    readonly index: number;
    readonly samples: ReadonlyArray<number>;
    readonly modelCalls: number;
    readonly callsPerSample: 1;
  }>;
  readonly heapBytes?: number;
  readonly rssBytes?: number;
  readonly conversationsPerSample?: number;
}

export interface ConformanceResult {
  readonly side: ComparisonSide;
  readonly checks: ReadonlyArray<{
    readonly axis:
      | 'producer fencing'
      | 'indeterminate tool handling'
      | 'revision compatibility'
      | 'durable signal cancellation'
      | 'runtime abort cancellation'
      | 'prompt file-byte recording'
      | 'attachment byte storage';
    readonly status: 'verified' | 'not equivalent' | 'not exercised';
    readonly evidence: string;
  }>;
}
