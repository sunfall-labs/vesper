import { Context, Effect, Schema } from 'effect';

import { AttachmentRef } from './ref.js';

// Where attachment bytes live.
//
// Three operations, and one of them carries the whole point of the design:
// `get` re-hashes what it read and refuses to hand back bytes that do not
// match the address it was asked for. Content addressing is only worth
// anything if somebody checks, and the failure it guards against — a backend
// quietly returning the wrong or a damaged payload — is invisible everywhere
// else. A truncated S3 read, a partially written file, a cache keyed one
// character short: all of those produce plausible bytes that a model will
// happily describe as if they were the document, and no downstream assertion
// will ever notice. Verification is not a debug aid; it is the reason this
// interface is not a `Map`.
//
// The contract suite in `./attachment-store-contract.ts` holds every backend
// to that. Per `docs/contributing.md` it lives here, in the package that owns
// the interface, rather than in a testkit package that would cycle.

/**
 * The referenced bytes are not in this store.
 *
 * Distinguished from {@link AttachmentIntegrityError} on purpose. A miss is a
 * question about the caller's expectations — wrong store, expired retention,
 * a reference that outlived its blob — and is usually recoverable by asking
 * somewhere else. An integrity failure is a statement about this store, and
 * retrying it against the same backend is pointless.
 */
export class AttachmentNotFound extends Schema.TaggedError<AttachmentNotFound>()(
  '@sunfall/vesper-attachments/AttachmentNotFound',
  {
    ref: AttachmentRef.Ref,
  },
) {}

/**
 * The stored bytes are not the bytes the reference addresses.
 *
 * Loud by construction: the alternative is returning the payload and letting
 * a model reason about corrupted input, which produces a confidently wrong
 * answer with nothing anywhere to say why.
 *
 * Both observed values are reported rather than a boolean, because the two
 * shapes of corruption want different responses: a matching length with a
 * different digest is bit rot or a key collision, while a different length is
 * a truncated or partial read and is the one worth retrying at a lower layer.
 */
export class AttachmentIntegrityError extends Schema.TaggedError<AttachmentIntegrityError>()(
  '@sunfall/vesper-attachments/AttachmentIntegrityError',
  {
    ref: AttachmentRef.Ref,
    actualDigest: AttachmentRef.Digest,
    actualByteLength: Schema.Number,
  },
) {}

/**
 * The backend itself failed.
 *
 * Not producible by the in-memory backend, and declared anyway. A filesystem
 * or object-store backend cannot avoid it, and widening `get`'s error channel
 * after callers exist is a breaking change for every one of them; declaring it
 * now costs a `_tag` nobody matches yet.
 */
export class AttachmentStoreError extends Schema.TaggedError<AttachmentStoreError>()(
  '@sunfall/vesper-attachments/AttachmentStoreError',
  {
    operation: Schema.Literals(['put', 'get', 'has']),
    cause: Schema.Defect(),
  },
) {}

/** Everything `get` can fail with. */
export type GetError =
  | AttachmentNotFound
  | AttachmentIntegrityError
  | AttachmentStoreError;

export interface Interface {
  /**
   * Store bytes, returning their address.
   *
   * Idempotent by construction rather than by effort: the same bytes hash to
   * the same address, so a second `put` is a write of identical content to an
   * identical key. Callers may rely on that and skip their own de-duplication.
   *
   * The store takes ownership of nothing — a backend must copy or persist
   * before returning, because the caller's array remains the caller's to
   * mutate.
   */
  readonly put: (
    bytes: Uint8Array,
    options: { readonly mediaType: string },
  ) => Effect.Effect<AttachmentRef.Ref, AttachmentStoreError>;

  /**
   * Read the bytes at an address, **verified**.
   *
   * Re-hashes what it read and fails {@link AttachmentIntegrityError} on a
   * mismatch. A backend must not skip this because its storage "cannot"
   * corrupt: the check is what makes the address mean something, and the
   * backends where it fires are precisely the ones nobody has written yet.
   */
  readonly get: (ref: AttachmentRef.Ref) => Effect.Effect<Uint8Array, GetError>;

  /**
   * Whether these bytes are present.
   *
   * Presence only — it does not verify, because verifying costs a full read
   * and a caller that wanted one would have called `get`. A `true` here is
   * therefore not a promise that `get` will succeed, and the doc comment is
   * the only place that can say so.
   */
  readonly has: (
    ref: AttachmentRef.Ref,
  ) => Effect.Effect<boolean, AttachmentStoreError>;
}

export class Service extends Context.Service<Service, Interface>()(
  '@sunfall/vesper-attachments/AttachmentStore',
) {}

/**
 * The verification every backend's `get` must apply to what it read.
 *
 * Lives here rather than in each backend so there is one definition of "these
 * are the right bytes" and one place for a reviewer to look. Returns the bytes
 * so it reads as a checkpoint in a pipeline rather than as an assertion whose
 * result can be ignored.
 */
export const verified = Effect.fn('AiAttachments.AttachmentStore.verified')(
  function* (ref: AttachmentRef.Ref, bytes: Uint8Array) {
    const actualDigest = yield* AttachmentRef.digestOf(bytes);
    if (actualDigest !== ref.digest || bytes.byteLength !== ref.byteLength) {
      return yield* new AttachmentIntegrityError({
        ref,
        actualDigest,
        actualByteLength: bytes.byteLength,
      });
    }
    return bytes;
  },
);

export * as AttachmentStore from './attachment-store.js';
