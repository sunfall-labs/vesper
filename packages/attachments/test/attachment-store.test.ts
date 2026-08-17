import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';

import { AttachmentStore } from '../src/attachment-store.js';
import { AttachmentRef } from '../src/ref.js';

// `AttachmentStore.verified` on its own, rather than through a backend.
//
// It is the one piece of this package every future backend is required to
// call, and the contract suite can only reach it through a backend that has
// already been corrupted on purpose. That leaves one of its two conditions
// unreachable from there: the memory store derives the address from the bytes,
// so it cannot produce a payload whose digest matches a reference while its
// length does not. A real backend can — a reference decoded from an old log
// record, a row whose `byte_length` column was written by a different version
// — and the check that catches it has no other test.

const run = <A, E>(effect: Effect.Effect<A, E, NodeServices.NodeServices>) =>
  Effect.runPromise(Effect.provide(Effect.result(effect), NodeServices.layer));

const bytesOf = (text: string): Uint8Array => new TextEncoder().encode(text);

describe('AttachmentStore.verified', () => {
  it('returns the bytes when they are the bytes the reference addresses', async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const bytes = bytesOf('the real document');
        const ref = yield* AttachmentRef.fromBytes(bytes, {
          mediaType: 'text/plain',
        });
        return yield* AttachmentStore.verified(ref, bytes);
      }),
    );

    expect(outcome._tag).toBe('Success');
    if (outcome._tag === 'Success') {
      expect(new TextDecoder().decode(outcome.success)).toBe(
        'the real document',
      );
    }
  });

  it('fails when the digest does not match', async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const ref = yield* AttachmentRef.fromBytes(bytesOf('the original'), {
          mediaType: 'text/plain',
        });
        return yield* AttachmentStore.verified(ref, bytesOf('a substitute'));
      }),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure).toBeInstanceOf(
        AttachmentStore.AttachmentIntegrityError,
      );
    }
  });

  // The length half of the check, which no backend-driven test can reach. A
  // reference whose declared length disagrees with bytes that hash correctly
  // is a reference that was assembled rather than measured, and handing back
  // its payload would mean a quota check, a streaming decision, or an
  // "is this small enough to inline" test was made against a number that is
  // not the length of what was returned.
  it('fails when the digest matches but the declared length does not', async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const bytes = bytesOf('the real document');
        const measured = yield* AttachmentRef.fromBytes(bytes, {
          mediaType: 'text/plain',
        });
        // Simulate data that bypassed schema decoding (for example a corrupt
        // backend row) so the verifier still defends its own boundary.
        const claimed: AttachmentRef.Ref = {
          ...measured,
          byteLength: measured.byteLength + 1,
        };
        return yield* AttachmentStore.verified(claimed, bytes);
      }),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure).toBeInstanceOf(
        AttachmentStore.AttachmentIntegrityError,
      );
      // The observed length, not the claimed one: the two shapes of corruption
      // are told apart by exactly this field, and reporting the reference's
      // own number back would make every mismatch look like bit rot.
      expect(outcome.failure).toMatchObject({ actualByteLength: 17 });
    }
  });
});
