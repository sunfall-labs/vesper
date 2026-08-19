import { Crypto, Effect } from 'effect';

import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

const randomUuid = Effect.fnUntraced(function* () {
  const crypto = yield* Crypto.Crypto;
  return yield* crypto.randomUUIDv4.pipe(Effect.orDie);
});

export const producerId: Effect.Effect<
  LogVocabulary.ProducerId,
  never,
  Crypto.Crypto
> = randomUuid().pipe(
  Effect.map((uuid) => LogVocabulary.ProducerId.make(uuid)),
);

export const toolCallId: Effect.Effect<
  LogVocabulary.ToolCallId,
  never,
  Crypto.Crypto
> = randomUuid().pipe(
  Effect.map((uuid) => LogVocabulary.ToolCallId.make(uuid)),
);
