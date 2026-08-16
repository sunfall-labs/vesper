import { Crypto, Effect, Layer, MutableHashMap, Option } from 'effect';

import { AttachmentStore } from './attachment-store.js';
import { AttachmentRef } from './ref.js';

// In-process attachment storage.
//
// It hashes on write and verifies on read exactly as a filesystem or object
// store does, even though a `MutableHashMap` cannot plausibly hand back the
// wrong bytes. That is deliberate: the verification is part of the interface's
// meaning, and a memory backend that skipped it would make the contract suite
// pass for the wrong reason and leave the first real backend as the first
// place the check was ever exercised.
//
// Bytes are copied in and copied out. The caller's array stays the caller's to
// mutate, and a store that aliased it would develop an integrity failure some
// arbitrary time later, at a `get` with no relationship to the code that
// caused it.

/** A memory store together with the back door its contract run needs. */
export interface Memory {
  readonly layer: Layer.Layer<AttachmentStore.Service, never, Crypto.Crypto>;
  /**
   * Replace the bytes stored at `ref` with something that does not match it.
   *
   * The only way to reach the state `get`'s verification exists for. Nothing
   * reachable through {@link AttachmentStore.Interface} can produce it —
   * `put` derives the address from the bytes — so without a back door the
   * integrity path would be untested, and an untested integrity check is
   * indistinguishable from no integrity check.
   */
  readonly overwriteUnsafe: (
    ref: AttachmentRef.Ref,
    replacement: Uint8Array,
  ) => Effect.Effect<void>;
}

const service = (blobs: MutableHashMap.MutableHashMap<string, Uint8Array>) =>
  Effect.gen(function* () {
    // Captured once, so `Crypto` is a requirement of the layer rather than of
    // every method — callers wire it at composition and never see it again.
    const crypto = yield* Crypto.Crypto;
    const withCrypto = <A, E>(
      effect: Effect.Effect<A, E, Crypto.Crypto>,
    ): Effect.Effect<A, E> =>
      Effect.provideService(effect, Crypto.Crypto, crypto);

    const put = Effect.fn('AiAttachments.AttachmentStore.put')(function* (
      bytes: Uint8Array,
      options: { readonly mediaType: string },
    ) {
      const ref = yield* withCrypto(AttachmentRef.fromBytes(bytes, options));
      MutableHashMap.set(blobs, ref.digest, bytes.slice());
      return ref;
    });

    const get = Effect.fn('AiAttachments.AttachmentStore.get')(function* (
      ref: AttachmentRef.Ref,
    ) {
      const stored = MutableHashMap.get(blobs, ref.digest);
      if (Option.isNone(stored)) {
        return yield* new AttachmentStore.AttachmentNotFound({ ref });
      }
      yield* withCrypto(AttachmentStore.verified(ref, stored.value));
      return stored.value.slice();
    });

    // `Effect.fn` with a plain function rather than a generator: the lookup is
    // synchronous and has nothing to yield, and an empty generator trips
    // `require-yield`.
    const has = Effect.fn('AiAttachments.AttachmentStore.has')(
      (ref: AttachmentRef.Ref) =>
        Effect.sync(() => MutableHashMap.has(blobs, ref.digest)),
    );

    return AttachmentStore.Service.of({ put, get, has });
  });

/**
 * A store and a handle to corrupt it.
 *
 * Each call gets its own storage, so two contract runs in one file cannot see
 * each other's blobs.
 */
export const make = (): Memory => {
  const blobs = MutableHashMap.empty<string, Uint8Array>();
  return {
    layer: Layer.effect(AttachmentStore.Service, service(blobs)),
    overwriteUnsafe: (ref, replacement) =>
      Effect.sync(() => {
        MutableHashMap.set(blobs, ref.digest, replacement.slice());
      }),
  };
};

/**
 * The ordinary memory store.
 *
 * `Layer.effect` over a suspended constructor rather than a module-level map:
 * every build of this layer gets fresh storage, so two runtimes in one process
 * do not share attachments.
 */
export const layer: Layer.Layer<AttachmentStore.Service, never, Crypto.Crypto> =
  Layer.effect(
    AttachmentStore.Service,
    Effect.suspend(() => service(MutableHashMap.empty<string, Uint8Array>())),
  );

export * as AttachmentStoreMemory from './layer-memory.js';
