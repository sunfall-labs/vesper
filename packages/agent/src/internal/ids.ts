import { Crypto, Effect } from 'effect';

import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

// A producer id is written into every batch a run appends, so a time-ordered
// v7 keeps the stores' producer index appending at its tail instead of
// scattering inserts across it.
export const producerId: Effect.Effect<
  LogVocabulary.ProducerId,
  never,
  Crypto.Crypto
> = Effect.flatMap(Crypto.Crypto, (crypto) => crypto.randomUUIDv7()).pipe(
  Effect.orDie,
  Effect.map((uuid) => LogVocabulary.ProducerId.make(uuid)),
);

export const toolCallId: Effect.Effect<
  LogVocabulary.ToolCallId,
  never,
  Crypto.Crypto
> = Effect.flatMap(Crypto.Crypto, (crypto) => crypto.randomUUIDv4()).pipe(
  Effect.orDie,
  Effect.map((uuid) => LogVocabulary.ToolCallId.make(uuid)),
);
