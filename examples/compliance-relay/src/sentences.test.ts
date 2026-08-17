import { Effect, Stream } from 'effect';
import { describe, expect, it } from '@effect/vitest';

import { bySentence } from './sentences.js';

const collect = (chunks: ReadonlyArray<string>) =>
  Stream.fromIterable(chunks).pipe(
    bySentence,
    Stream.runCollect,
    Effect.map(Array.from),
  );

describe('bySentence', () => {
  it.effect('emits completed sentences across delta boundaries', () =>
    Effect.gen(function* () {
      expect(yield* collect(['First sentence. Sec', 'ond sentence. '])).toEqual(
        ['First sentence.', 'Second sentence.'],
      );
      return Effect.void;
    }),
  );

  it.effect('flushes an unterminated final sentence', () =>
    Effect.gen(function* () {
      expect(yield* collect(['A final sentence without punctuation'])).toEqual([
        'A final sentence without punctuation',
      ]);
      return Effect.void;
    }),
  );

  it.effect('does not emit an empty sentence for an empty stream', () =>
    Effect.gen(function* () {
      expect(yield* collect([])).toEqual([]);
      return Effect.void;
    }),
  );
});
