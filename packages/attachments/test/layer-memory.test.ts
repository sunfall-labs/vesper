import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import { describe, expect, it } from 'vitest';

import { AttachmentStore } from '../src/attachment-store.js';
import { AttachmentStoreContract } from '../src/attachment-store-contract.js';
import { AttachmentStoreMemory } from '../src/layer-memory.js';

// The memory backend can be corrupted on purpose, so it runs the whole suite
// including the two integrity cases. `Crypto` comes from the Node platform
// layer; the package itself depends on `effect` alone and leaves the choice of
// implementation to whoever wires the runtime.
const memory = AttachmentStoreMemory.make();

AttachmentStoreContract.attachmentStoreContract('memory', {
  layer: memory.layer.pipe(Layer.provide(NodeServices.layer)),
  overwriteUnsafe: memory.overwriteUnsafe,
});

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

const inItsOwnRuntime = <A>(
  effect: Effect.Effect<A, unknown, AttachmentStore.Service>,
): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, ordinary) as Effect.Effect<A>);

describe('the ordinary memory layer', () => {
  it('stores and reads back like any other backend', async () => {
    const read = await inItsOwnRuntime(
      Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        const ref = yield* store.put(new TextEncoder().encode('hello'), {
          mediaType: 'text/plain',
        });
        return yield* store.get(ref);
      }),
    );

    expect(new TextDecoder().decode(read)).toBe('hello');
  });

  it('gives every build its own storage', async () => {
    const ref = await inItsOwnRuntime(
      Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        return yield* store.put(new TextEncoder().encode('tenant one'), {
          mediaType: 'text/plain',
        });
      }),
    );

    // A second, independent build of the same layer value.
    const present = await inItsOwnRuntime(
      Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        return yield* store.has(ref);
      }),
    );

    expect(present).toBe(false);
  });
});
