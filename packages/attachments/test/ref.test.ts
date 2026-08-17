import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Schema } from 'effect';
import { describe, expect, it } from 'vitest';

import { AttachmentRef } from '../src/ref.js';

// The address format is a wire contract the moment one reference is written
// into a log record or a database row. These tests pin it against published
// SHA-256 vectors rather than against whatever the code currently produces, so
// swapping the algorithm, the encoding, or the prefix is a test failure and
// not a silent re-addressing of every attachment ever stored.

const run = <A>(effect: Effect.Effect<A, never, NodeServices.NodeServices>) =>
  Effect.runPromise(Effect.provide(effect, NodeServices.layer));

const bytesOf = (text: string): Uint8Array => new TextEncoder().encode(text);

describe('AttachmentRef', () => {
  it('hashes with SHA-256, lowercase hex, algorithm-prefixed', async () => {
    const digest = await run(AttachmentRef.digestOf(bytesOf('abc')));

    expect(digest).toBe(
      'sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
    );
  });

  it('hashes the empty payload to the published empty digest', async () => {
    const digest = await run(AttachmentRef.digestOf(new Uint8Array(0)));

    expect(digest).toBe(
      'sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    );
  });

  it('builds a reference by measuring the bytes', async () => {
    const ref = await run(
      AttachmentRef.fromBytes(bytesOf('abc'), { mediaType: 'text/plain' }),
    );

    expect(ref).toEqual({
      digest:
        'sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
      mediaType: 'text/plain',
      byteLength: 3,
    });
  });

  // Every vector above is ASCII text, which is exactly the input a broken
  // implementation survives: anything that decodes the bytes to a string on
  // the way to the hash agrees with a correct one until a byte outside ASCII
  // shows up — and then produces a stable, plausible, *wrong* address for
  // every PDF and screenshot the system will ever store. This is all 256 byte
  // values, hashed against the published digest for that sequence.
  it('hashes arbitrary bytes, not a decoded string', async () => {
    const digest = await run(
      AttachmentRef.digestOf(
        new Uint8Array(Array.from({ length: 256 }, (_, byte) => byte)),
      ),
    );

    expect(digest).toBe(
      'sha256:40aff2e9d2d8922e47afd4648e6967497158785fbd1da870e7110266bf944880',
    );
  });

  // A reference is written into a log record and read back out, so the schema
  // is a wire format. Round-tripping it is what catches an encoded shape that
  // has quietly stopped matching the decoded one — a renamed field, a brand
  // that turns into an object, a number that arrives as a string.
  it('survives being encoded into a record and decoded back', async () => {
    const ref = await run(
      AttachmentRef.fromBytes(bytesOf('abc'), { mediaType: 'image/png' }),
    );

    const wire = JSON.parse(
      JSON.stringify(Schema.encodeSync(AttachmentRef.Ref)(ref)),
    ) as unknown;
    const decoded = Schema.decodeUnknownSync(AttachmentRef.Ref)(wire);

    expect(decoded).toEqual(ref);
    // And the encoded form is pinned, not merely reversible. A schema whose
    // encoding changed — a brand that started encoding to something
    // structured, a length that started encoding to a string — round-trips
    // perfectly within one version and cannot read a single reference written
    // by the one before it.
    expect(wire).toEqual({
      digest:
        'sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
      mediaType: 'image/png',
      byteLength: 3,
    });
  });

  it.each([
    `sha512:${'0'.repeat(64)}`,
    `sha256:${'0'.repeat(63)}`,
    `sha256:${'0'.repeat(65)}`,
    `sha256:${'A'.repeat(64)}`,
    `sha256:${'g'.repeat(64)}`,
  ])('rejects malformed digest %s', (digest) => {
    expect(() =>
      Schema.decodeUnknownSync(AttachmentRef.Ref)({
        digest,
        mediaType: 'application/octet-stream',
        byteLength: 0,
      }),
    ).toThrow();
  });

  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid byteLength %s',
    (byteLength) => {
      expect(() =>
        Schema.decodeUnknownSync(AttachmentRef.Ref)({
          digest: `sha256:${'0'.repeat(64)}`,
          mediaType: 'application/octet-stream',
          byteLength,
        }),
      ).toThrow();
    },
  );

  // The media type is on the reference, not in the address.
  it('addresses the same bytes identically under different media types', async () => {
    const [asText, asBinary] = await run(
      Effect.all([
        AttachmentRef.fromBytes(bytesOf('abc'), { mediaType: 'text/plain' }),
        AttachmentRef.fromBytes(bytesOf('abc'), {
          mediaType: 'application/octet-stream',
        }),
      ]),
    );

    expect(asBinary.digest).toBe(asText.digest);
  });
});
