import { describe, expect, it } from '@effect/vitest';
import { Effect } from 'effect';

import { LogOffset } from '../src/offset.js';

describe('LogOffset', () => {
  it.effect('formats a sequence as two zero-padded 16-digit components', () => {
    return Effect.sync(() => {
      expect(LogOffset.fromSeq(0n)).toBe('0000000000000000_0000000000000000');
      expect(LogOffset.fromSeq(42n)).toBe('0000000000000000_0000000000000042');
    });
  });

  it.effect('carries into the high component past 10^16', () => {
    return Effect.sync(() => {
      expect(LogOffset.fromSeq(10n ** 16n)).toBe(
        '0000000000000001_0000000000000000',
      );
    });
  });

  // The whole reason the format is padded decimal. If this stops holding,
  // every ordered read in the package is wrong and nothing else will say so.
  it.effect('orders lexicographically exactly as it orders numerically', () => {
    return Effect.sync(() => {
      const seqs = [0n, 1n, 9n, 10n, 99n, 100n, 10n ** 15n, 10n ** 16n];
      const offsets = seqs.map(LogOffset.fromSeq);

      expect(
        offsets.slice(1).every((offset, index) => {
          const previous = offsets.at(index);
          return previous !== undefined && previous < offset;
        }),
      ).toBe(true);
    });
  });

  it.effect('sorts START before every real offset', () => {
    return Effect.sync(() => {
      expect(LogOffset.isAfter(LogOffset.fromSeq(0n), LogOffset.START)).toBe(
        true,
      );
      expect(LogOffset.START < LogOffset.fromSeq(0n)).toBe(true);
    });
  });

  // A single component reaches 10^16, an order of magnitude past
  // MAX_SAFE_INTEGER. Round-tripping through `bigint` is what keeps that
  // honest; a `parseInt` would pass every test small enough to write.
  it.effect('round-trips a sequence beyond MAX_SAFE_INTEGER', () =>
    Effect.gen(function* () {
      const seq = BigInt(Number.MAX_SAFE_INTEGER) * 1000n;
      const back = yield* LogOffset.toSeq(LogOffset.fromSeq(seq));

      expect(back).toBe(seq);
    }),
  );

  it.effect('decodes START to -1 so `after` is uniformly exclusive', () =>
    Effect.gen(function* () {
      expect(yield* LogOffset.toSeq(LogOffset.START)).toBe(-1n);
    }),
  );

  it.effect('rejects a malformed offset at the decoding boundary', () =>
    Effect.gen(function* () {
      const outcome = yield* LogOffset.decode('nonsense').pipe(Effect.result);

      expect(outcome._tag).toBe('Failure');
    }),
  );

  it.effect('refuses to format a sequence it cannot represent', () => {
    return Effect.sync(() => {
      expect(() => LogOffset.fromSeq(-1n)).toThrow(RangeError);
      expect(() => LogOffset.fromSeq(LogOffset.MAX_SEQ + 1n)).toThrow(
        RangeError,
      );
    });
  });
});
