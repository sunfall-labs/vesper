import { Crypto, Effect, Encoding, Schema } from 'effect';

// A content address for one blob of bytes.
//
// The digest *is* the identity. Two attachments with the same bytes are the
// same attachment, whatever they were called or where they came from, so a
// reference is something a log record, a prompt part, or a cache key can carry
// without also carrying a location. Nothing here knows about storage;
// `./attachment-store.ts` owns that, and this module is what the two sides
// agree on.
//
// The media type rides along on the reference but is deliberately **not** part
// of the address. It describes how to interpret the bytes, not which bytes
// they are, and folding it in would mean the same PNG stored once as
// `image/png` and once as `application/octet-stream` occupied two addresses
// and two copies. A caller needing that distinction keeps two references,
// which is exactly what it already has.
//
// `byteLength` is likewise metadata, not identity — a digest already pins the
// length. It is here because quota checks, streaming decisions, and "is this
// small enough to inline" all want it without paying a read.
//
// Hashing goes through Effect's `Crypto` service rather than `node:crypto` or
// a bundled implementation: this package depends on `effect` and nothing else
// (see `docs/contributing.md`), and a service keeps the requirement visible
// on the layer instead of hiding a platform global inside a pure-looking
// function.

/**
 * The hash every address is built from.
 *
 * Named inside the address itself (`sha256:…`) so a second algorithm can be
 * added later without a flag day: an old reference stays parseable and stays
 * verifiable, because it says what it is.
 */
export const DIGEST_ALGORITHM: Crypto.DigestAlgorithm = 'SHA-256';

/** How {@link DIGEST_ALGORITHM} is spelled inside a {@link Digest}. */
export const DIGEST_PREFIX = 'sha256';

/**
 * A hash of some bytes, written `sha256:<64 lowercase hex digits>`.
 *
 * Branded rather than a bare `string` so a conversation id, a media type, or
 * a filename cannot be passed where an address is expected. The brand adds no
 * runtime representation, but the schema validates the wire format before a
 * reference can enter the system. Stored bytes are still re-hashed by `get`.
 */
export const Digest = Schema.String.check(
  Schema.isPattern(/^sha256:[0-9a-f]{64}$/),
).pipe(Schema.brand('@sunfall/vesper-attachments/Digest'));
export type Digest = typeof Digest.Type;

/** A reference to stored bytes. */
export const Ref = Schema.Struct({
  digest: Digest,
  /** How to interpret the bytes. Not part of the address; see above. */
  mediaType: Schema.String,
  /** Length of the referenced bytes. Redundant with the digest, and cheap. */
  byteLength: Schema.Natural,
});

export interface Ref extends Schema.Struct.Type<typeof Ref.fields> {}

/**
 * Hash bytes into a {@link Digest}.
 *
 * A platform digest failure is a defect, not a typed error. SHA-256 over a
 * byte array has no legitimate failure mode; if the platform's crypto is
 * broken then every address this process computes is untrustworthy and no
 * caller has a recovery worth writing. Keeping it out of the error channel is
 * what lets `put` fail only for reasons a caller can act on.
 */
export const digestOf = Effect.fnUntraced(function* (bytes: Uint8Array) {
  const crypto = yield* Crypto.Crypto;
  const hash = yield* crypto.digest(DIGEST_ALGORITHM, bytes).pipe(Effect.orDie);
  return Digest.make(`${DIGEST_PREFIX}:${Encoding.encodeHex(hash)}`);
});

/**
 * Build a reference by hashing the bytes it will point at.
 *
 * This is the only honest way to make one. A reference assembled from a digest
 * somebody else computed is a claim; this is a measurement.
 */
export const fromBytes = Effect.fnUntraced(function* (
  bytes: Uint8Array,
  options: { readonly mediaType: string },
) {
  const digest = yield* digestOf(bytes);
  return Ref.make({
    digest,
    mediaType: options.mediaType,
    byteLength: bytes.byteLength,
  });
});

export * as AttachmentRef from './ref.js';
