import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';

import { LogOffset } from '../src/offset.js';

describe('LogOffset', () => {
  it('formats a sequence as two zero-padded 16-digit components', () => {
    expect(LogOffset.fromSeq(0n)).toBe('0000000000000000_0000000000000000');
    expect(LogOffset.fromSeq(42n)).toBe('0000000000000000_0000000000000042');
  });

  it('carries into the high component past 10^16', () => {
    expect(LogOffset.fromSeq(10n ** 16n)).toBe(
      '0000000000000001_0000000000000000',
    );
  });

  // The whole reason the format is padded decimal. If this stops holding,
  // every ordered read in the package is wrong and nothing else will say so.
  it('orders lexicographically exactly as it orders numerically', () => {
    const seqs = [0n, 1n, 9n, 10n, 99n, 100n, 10n ** 15n, 10n ** 16n];
    const offsets = seqs.map(LogOffset.fromSeq);

    expect([...offsets].sort()).toEqual(offsets);
  });

  it('sorts START before every real offset', () => {
    expect(LogOffset.isAfter(LogOffset.fromSeq(0n), LogOffset.START)).toBe(
      true,
    );
    expect([LogOffset.fromSeq(0n), LogOffset.START].sort()).toEqual([
      LogOffset.START,
      LogOffset.fromSeq(0n),
    ]);
  });

  // A single component reaches 10^16, an order of magnitude past
  // MAX_SAFE_INTEGER. Round-tripping through `bigint` is what keeps that
  // honest; a `parseInt` would pass every test small enough to write.
  it('round-trips a sequence beyond MAX_SAFE_INTEGER', () => {
    const seq = BigInt(Number.MAX_SAFE_INTEGER) * 1000n;
    const back = Effect.runSync(LogOffset.toSeq(LogOffset.fromSeq(seq)));

    expect(back).toBe(seq);
  });

  it('decodes START to -1 so `after` is uniformly exclusive', () => {
    expect(Effect.runSync(LogOffset.toSeq(LogOffset.START))).toBe(-1n);
  });

  it('fails on a malformed offset rather than throwing', () => {
    const outcome = Effect.runSync(
      LogOffset.toSeq(LogOffset.Offset.make('nonsense')).pipe(Effect.result),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure.offset).toBe('nonsense');
    }
  });

  it('refuses to format a sequence it cannot represent', () => {
    expect(() => LogOffset.fromSeq(-1n)).toThrow(RangeError);
    expect(() => LogOffset.fromSeq(LogOffset.MAX_SEQ + 1n)).toThrow(RangeError);
  });
});
