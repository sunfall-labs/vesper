import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer } from 'effect';

import { AttachmentStore } from '../src/attachment-store.js';
import { AttachmentStoreMemory } from '../src/layer-memory.js';

// The shared contract (content addressing, integrity) runs against the
// filesystem backend, the one that can be corrupted on purpose through the
// real disk. The memory backend gets the scenario that matters for it: the
// exported layer must give every build its own storage, or two runtimes in
// one process (two tenants, two test files) would share one store.
const memory = AttachmentStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

describe('the memory attachment layer', () => {
  it.effect('stores, deduplicates, and isolates each build', () =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode('tenant one');
      const ref = yield* Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        const first = yield* store.put(bytes, { mediaType: 'text/plain' });
        const second = yield* store.put(bytes, { mediaType: 'text/plain' });
        expect(second).toEqual(first);
        expect(new TextDecoder().decode(yield* store.get(first))).toBe(
          'tenant one',
        );
        return first;
      }).pipe(Effect.provide(memory));

      const otherBuild = yield* Effect.flatMap(
        AttachmentStore.Service,
        (store) => Effect.result(store.get(ref)),
      ).pipe(Effect.provide(memory));
      expect(otherBuild).toMatchObject({
        _tag: 'Failure',
        failure: { _tag: 'AttachmentNotFound' },
      });
    }),
  );
});
