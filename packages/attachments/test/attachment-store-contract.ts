import { Effect, Schema } from 'effect';
import type { Layer } from 'effect';
import { describe, expect, it } from 'vitest';

import { AttachmentStore } from '../src/attachment-store.js';
import { AttachmentRef } from '../src/ref.js';

// The behaviour every AttachmentStore backend must have, expressed once.
//
// Three of these cases are the package's reason to exist and none of them is
// discoverable from the interface. Identical bytes must land on one address,
// or content addressing is just a hash-shaped key. A miss must be a miss, not
// an integrity failure, because the two ask for different responses. And a
// stored payload that does not match its address must fail loudly rather than
// come back — the failure this design prevents is the silent one, where a
// model receives a damaged document and describes it with complete confidence.
//
// The corruption cases need `overwriteUnsafe` because `put` cannot produce the
// state they test: it derives the address from the bytes, so there is no
// sequence of interface calls that stores a mismatch. It is a required option
// rather than an optional one — a backend that cannot be corrupted on purpose
// cannot demonstrate that it verifies, and this suite would otherwise report a
// pass for the one property it exists to check.

const bytesOf = (text: string): Uint8Array => new TextEncoder().encode(text);

/** An address nothing was ever stored at. */
const ABSENT: AttachmentRef.Ref = AttachmentRef.Ref.make({
  digest: AttachmentRef.Digest.make(
    `${AttachmentRef.DIGEST_PREFIX}:${'0'.repeat(64)}`,
  ),
  mediaType: 'application/octet-stream',
  byteLength: 4,
});

export interface ContractOptions<E> {
  readonly layer: Layer.Layer<AttachmentStore.Service, E>;
  /**
   * Replace the stored payload at `ref` with bytes that do not match it,
   * bypassing `put`.
   *
   * Required. See the note above on why this is not optional.
   */
  readonly overwriteUnsafe: (
    ref: AttachmentRef.Ref,
    replacement: Uint8Array,
  ) => Effect.Effect<void>;
}

export const attachmentStoreContract = <E>(
  name: string,
  options: ContractOptions<E>,
): void => {
  const run = <A>(
    effect: Effect.Effect<A, unknown, AttachmentStore.Service>,
  ): Promise<A> =>
    Effect.runPromise(effect.pipe(Effect.provide(options.layer)));

  describe(`AttachmentStore contract: ${name}`, () => {
    it('round-trips stored bytes', async () => {
      const read = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(bytesOf('hello world'), {
            mediaType: 'text/plain',
          });
          return yield* store.get(ref);
        }),
      );

      expect(new TextDecoder().decode(read)).toBe('hello world');
    });

    it('describes what it stored on the reference', async () => {
      const ref = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          return yield* store.put(bytesOf('hello'), {
            mediaType: 'text/plain',
          });
        }),
      );

      expect(ref.mediaType).toBe('text/plain');
      expect(ref.byteLength).toBe(5);
      expect(ref.digest.startsWith(`${AttachmentRef.DIGEST_PREFIX}:`)).toBe(
        true,
      );
    });

    // Content addressing, stated as the property it actually is: the address
    // is a function of the bytes and of nothing else. Without this, two
    // uploads of the same screenshot are two blobs and a cache never hits.
    it('gives identical bytes an identical reference', async () => {
      const [first, second] = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          return [
            yield* store.put(bytesOf('same'), { mediaType: 'text/plain' }),
            yield* store.put(bytesOf('same'), { mediaType: 'text/plain' }),
          ] as const;
        }),
      );

      expect(second.digest).toBe(first.digest);
    });

    it('gives different bytes different references', async () => {
      const [first, second] = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          return [
            yield* store.put(bytesOf('one'), { mediaType: 'text/plain' }),
            yield* store.put(bytesOf('two'), { mediaType: 'text/plain' }),
          ] as const;
        }),
      );

      expect(second.digest).not.toBe(first.digest);
    });

    // The media type is metadata on the reference, not part of the address.
    // Both references therefore resolve, and the bytes are stored once.
    it('addresses bytes independently of their declared media type', async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const asText = yield* store.put(bytesOf('ambiguous'), {
            mediaType: 'text/plain',
          });
          const asBinary = yield* store.put(bytesOf('ambiguous'), {
            mediaType: 'application/octet-stream',
          });
          return {
            asText,
            asBinary,
            viaText: yield* store.get(asText),
            viaBinary: yield* store.get(asBinary),
          };
        }),
      );

      expect(outcome.asBinary.digest).toBe(outcome.asText.digest);
      expect(new TextDecoder().decode(outcome.viaText)).toBe('ambiguous');
      expect(new TextDecoder().decode(outcome.viaBinary)).toBe('ambiguous');
    });

    // Zero bytes are a real attachment — an empty file, a truncated upload
    // the caller chose to keep — and a store that conflates "empty" with
    // "missing" fails only on the day one shows up.
    it('stores an empty payload as a real attachment', async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(new Uint8Array(0), {
            mediaType: 'application/octet-stream',
          });
          return yield* store.get(ref);
        }),
      );

      expect(outcome.byteLength).toBe(0);
    });

    it('refuses to return a corrupted payload', async () => {
      const original = bytesOf('the real document');
      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(original, { mediaType: 'text/plain' });
          // Shorter, so a `has` that checks even the cheap half of the
          // verification — the declared length — is caught here too.
          yield* options.overwriteUnsafe(ref, bytesOf('short'));

          return yield* store.get(ref).pipe(Effect.result);
        }),
      );

      expect(outcome._tag).toBe('Failure');
    });

    // Attachments are PDFs, screenshots and archives, not text. A backend that
    // round-trips through a string — a JSON column, a `TextDecoder`, a base64
    // step with the wrong alphabet — passes every ASCII case above and then
    // mangles the first real upload. This payload contains a NUL, a lone
    // surrogate's worth of high bytes, and every value in between.
    it('round-trips bytes that are not text', async () => {
      const binary = new Uint8Array(
        Array.from({ length: 256 }, (_, byte) => byte),
      );

      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(binary, {
            mediaType: 'application/octet-stream',
          });
          return { ref, read: yield* store.get(ref) };
        }),
      );

      expect(outcome.ref.byteLength).toBe(256);
      expect(Array.from(outcome.read)).toEqual(Array.from(binary));
    });

    it('fails not-found for a reference that was never stored', async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          return yield* store.get(ABSENT).pipe(Effect.result);
        }),
      );

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toBeInstanceOf(
          AttachmentStore.AttachmentNotFound,
        );
        expect(outcome.failure).toMatchObject({ ref: ABSENT });
      }
    });

    // The case the whole design is for. The replacement is the same length as
    // the original, so nothing but re-hashing can catch it: a store that
    // returns what it read returns bytes that are not the document, and every
    // layer above believes them.
    it('fails integrity rather than returning a corrupted payload', async () => {
      const original = bytesOf('the real document');
      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(original, { mediaType: 'text/plain' });

          const tampered = original.slice();
          const firstByte = original[0] ?? 0;
          tampered[0] = firstByte ^ 0xff;
          yield* options.overwriteUnsafe(ref, tampered);

          return {
            ref,
            result: yield* store.get(ref).pipe(Effect.result),
          };
        }),
      );

      expect(outcome.result._tag).toBe('Failure');
      if (outcome.result._tag === 'Failure') {
        if (
          !Schema.is(AttachmentStore.AttachmentIntegrityError)(
            outcome.result.failure,
          )
        ) {
          throw outcome.result.failure;
        }
        expect(outcome.result.failure).toBeInstanceOf(
          AttachmentStore.AttachmentIntegrityError,
        );
        // Same length, different digest: bit rot or a key collision, and
        // distinguishable from a truncated read by exactly this field.
        expect(outcome.result.failure).toMatchObject({
          actualByteLength: original.byteLength,
        });
        expect(outcome.result.failure.actualDigest).not.toBe(
          outcome.ref.digest,
        );
      }
    });

    it('fails integrity for a truncated payload', async () => {
      const original = bytesOf('the real document');
      const outcome = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(original, { mediaType: 'text/plain' });
          yield* options.overwriteUnsafe(ref, original.slice(0, 4));
          return yield* store.get(ref).pipe(Effect.result);
        }),
      );

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toBeInstanceOf(
          AttachmentStore.AttachmentIntegrityError,
        );
        expect(outcome.failure).toMatchObject({ actualByteLength: 4 });
      }
    });

    // Both directions of the aliasing bug. A store that keeps the caller's
    // array, or hands its own out, develops an integrity failure at some later
    // `get` with no connection to the code that caused it.
    it('does not alias the caller’s array on write', async () => {
      const mutable = bytesOf('stable');
      const read = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(mutable, { mediaType: 'text/plain' });
          mutable[0] = 0x00;
          return yield* store.get(ref);
        }),
      );

      expect(new TextDecoder().decode(read)).toBe('stable');
    });

    it('does not alias its own storage on read', async () => {
      const read = await run(
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          const ref = yield* store.put(bytesOf('stable'), {
            mediaType: 'text/plain',
          });
          const first = yield* store.get(ref);
          first[0] = 0x00;
          return yield* store.get(ref);
        }),
      );

      expect(new TextDecoder().decode(read)).toBe('stable');
    });
  });
};
