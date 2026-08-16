// Summary statistics for a set of timed iterations.
//
// Deliberately reports spread, not just a headline. A single mean is how a
// contaminated measurement survives review: the number that gave this project
// a bogus "1m42s startup" reading looked perfectly plausible on its own, and
// only the variance and a cross-check against an independent clock would have
// caught it. Everything here exists so the report cannot omit the spread.

export interface Summary {
  readonly n: number;
  readonly min: number;
  readonly median: number;
  readonly mean: number;
  readonly max: number;
  /** Sample standard deviation (n-1). */
  readonly stddev: number;
  /** stddev / mean, as a percentage. The honesty number. */
  readonly rsdPercent: number;
}

export const summarise = (samples: ReadonlyArray<number>): Summary => {
  if (samples.length === 0) {
    throw new Error('summarise: no samples');
  }

  const sorted = [...samples].sort((a, b) => a - b);
  const n = sorted.length;
  const mean = sorted.reduce((a, b) => a + b, 0) / n;
  const variance =
    n < 2 ? 0 : sorted.reduce((acc, x) => acc + (x - mean) ** 2, 0) / (n - 1);
  const stddev = Math.sqrt(variance);

  const median =
    n % 2 === 1
      ? sorted[(n - 1) / 2]!
      : (sorted[n / 2 - 1]! + sorted[n / 2]!) / 2;

  return {
    n,
    min: sorted[0]!,
    median,
    mean,
    max: sorted[n - 1]!,
    stddev,
    rsdPercent: mean === 0 ? 0 : (stddev / mean) * 100,
  };
};

export const formatMs = (value: number): string =>
  value >= 100
    ? value.toFixed(1)
    : value >= 1
      ? value.toFixed(2)
      : value.toFixed(3);
