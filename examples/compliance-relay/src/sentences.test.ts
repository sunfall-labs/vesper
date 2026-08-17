import { Effect, Stream } from 'effect';
import { describe, expect, it } from 'vitest';

import { bySentence } from './sentences.js';

const collect = (chunks: ReadonlyArray<string>) =>
  Effect.runPromise(
    Stream.fromIterable(chunks).pipe(bySentence, Stream.runCollect),
  ).then(Array.from);

describe('bySentence', () => {
  it('emits completed sentences across delta boundaries', async () => {
    await expect(
      collect(['First sentence. Sec', 'ond sentence. ']),
    ).resolves.toEqual(['First sentence.', 'Second sentence.']);
  });

  it('flushes an unterminated final sentence', async () => {
    await expect(
      collect(['A final sentence without punctuation']),
    ).resolves.toEqual(['A final sentence without punctuation']);
  });

  it('does not emit an empty sentence for an empty stream', async () => {
    await expect(collect([])).resolves.toEqual([]);
  });
});
