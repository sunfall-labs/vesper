import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer } from 'effect';

import { AttachmentStore } from '../src/attachment-store.js';
import { make } from '../src/internal/memory-store.js';
import { AttachmentStoreMemory } from '../src/layer-memory.js';
import {
  attachmentStoreContract,
  type ContractOptions as AttachmentStoreContractOptions,
} from './attachment-store-contract.js';

// The memory backend can be corrupted on purpose, so it runs the whole suite
// including the two integrity cases. `Crypto` comes from the Node platform
// layer; the package itself depends on `effect` alone and leaves the choice of
// implementation to whoever wires the runtime.
const memory = make();

attachmentStoreContract('memory', {
  layer: memory.layer.pipe(Layer.provide(NodeServices.layer)),
  overwriteUnsafe: memory.overwriteUnsafe,
});

const _unprovidedAttachmentContract: AttachmentStoreContractOptions<never> = {
  // @ts-expect-error contract helpers must not erase unprovided layer requirements
  layer: memory.layer,
  overwriteUnsafe: memory.overwriteUnsafe,
};

// The contract runs against `make()`, because that is the shape with the back
// door it needs. `layer` is the export everything else wires, and it is a
// different construction — `Effect.suspend` around a fresh map rather than a
// map captured by the caller — so nothing above touches it.
//
// What that leaves untested is the property the suspension is there for. A
// map hoisted to module scope, or a `Layer.effect` over an already-evaluated
// `service(...)`, would pass every contract case and then let two runtimes in
// one process see each other's attachments: a test suite where one file's blob
// resolves in another, and a server where two tenants' stores are one store.

const ordinary = AttachmentStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

describe('the ordinary memory layer', () => {
  it.effect('stores and reads back like any other backend', () =>
    Effect.gen(function* () {
      const store = yield* AttachmentStore.Service;
      const ref = yield* store.put(new TextEncoder().encode('hello'), {
        mediaType: 'text/plain',
      });
      const read = yield* store.get(ref);

      expect(new TextDecoder().decode(read)).toBe('hello');
    }).pipe(Effect.provide(ordinary)),
  );

  it.effect('gives every build its own storage', () =>
    Effect.gen(function* () {
      const ref = yield* Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        return yield* store.put(new TextEncoder().encode('tenant one'), {
          mediaType: 'text/plain',
        });
      }).pipe(Effect.provide(ordinary));

      // A second, independent build of the same layer value.
      const read = yield* Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        return yield* store.get(ref).pipe(Effect.result);
      }).pipe(Effect.provide(ordinary));

      expect(read).toMatchObject({
        _tag: 'Failure',
        failure: { _tag: 'AttachmentNotFound' },
      });
    }),
  );
});
