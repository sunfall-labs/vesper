import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';

import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodePath from '@effect/platform-node/NodePath';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, describe, expect, it } from '@effect/vitest';
import { Effect, Layer } from 'effect';
import * as FileSystem from 'effect/FileSystem';
import * as Path from 'effect/Path';
import * as PlatformError from 'effect/PlatformError';

import { AttachmentStore } from '../src/attachment-store.js';
import { AttachmentStoreFileSystem } from '../src/layer-filesystem.js';
import { AttachmentRef } from '../src/ref.js';
import {
  attachmentStoreContract,
  type ContractOptions as AttachmentStoreContractOptions,
} from './attachment-store-contract.js';

const root = `${tmpdir()}/vesper-attachments-${randomUUID()}`;

const storedPath = (ref: AttachmentRef.Ref) =>
  Effect.map(Path.Path, (path) => {
    const hex = ref.digest.slice(`${AttachmentRef.DIGEST_PREFIX}:`.length);
    return path.join(root, AttachmentRef.DIGEST_PREFIX, hex.slice(0, 2), hex);
  });

const overwriteUnsafe = (
  ref: AttachmentRef.Ref,
  replacement: Uint8Array,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.writeFile(yield* storedPath(ref), replacement);
  }).pipe(Effect.provide(NodeServices.layer), Effect.orDie);

const filesystem = AttachmentStoreFileSystem.layer(root).pipe(
  Layer.provide(NodeServices.layer),
);

attachmentStoreContract('filesystem', {
  layer: filesystem,
  overwriteUnsafe,
});

const _unprovidedAttachmentContract: AttachmentStoreContractOptions<never> = {
  // @ts-expect-error filesystem storage keeps platform requirements visible
  layer: AttachmentStoreFileSystem.layer(root),
  overwriteUnsafe,
};

afterAll(async () => {
  await Effect.runPromise(
    Effect.flatMap(FileSystem.FileSystem, (fs) =>
      fs.remove(root, { recursive: true, force: true }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe('filesystem attachment persistence', () => {
  it.effect('survives rebuilding the layer', () =>
    Effect.gen(function* () {
      const ref = yield* Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        return yield* store.put(new TextEncoder().encode('persistent'), {
          mediaType: 'text/plain',
        });
      }).pipe(Effect.provide(filesystem));

      const bytes = yield* Effect.gen(function* () {
        const store = yield* AttachmentStore.Service;
        return yield* store.get(ref);
      }).pipe(Effect.provide(filesystem));

      expect(new TextDecoder().decode(bytes)).toBe('persistent');
    }),
  );

  it.effect('maps platform failures into AttachmentStoreError', () => {
    const failed = PlatformError.systemError({
      _tag: 'PermissionDenied',
      module: 'FileSystem',
      method: 'readFile',
      pathOrDescriptor: root,
    });
    const failingFileSystem = FileSystem.layerNoop({
      readFile: () => Effect.fail(failed),
    });
    const failingStore = AttachmentStoreFileSystem.layer(root).pipe(
      Layer.provide(
        Layer.mergeAll(failingFileSystem, NodeCrypto.layer, NodePath.layer),
      ),
    );
    const absent = AttachmentRef.Ref.make({
      digest: AttachmentRef.Digest.make(
        `${AttachmentRef.DIGEST_PREFIX}:${'0'.repeat(64)}`,
      ),
      mediaType: 'application/octet-stream',
      byteLength: 1,
    });

    return Effect.gen(function* () {
      const store = yield* AttachmentStore.Service;
      const outcome = yield* store.get(absent).pipe(Effect.result);

      expect(outcome).toMatchObject({
        _tag: 'Failure',
        failure: {
          _tag: 'AttachmentStoreError',
          operation: 'get',
          cause: { _tag: 'PlatformError' },
        },
      });
    }).pipe(Effect.provide(failingStore));
  });
});
