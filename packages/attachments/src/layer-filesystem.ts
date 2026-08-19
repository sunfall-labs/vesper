import { Crypto, Effect, Layer } from 'effect';
import * as FileSystem from 'effect/FileSystem';
import type { PlatformError } from 'effect/PlatformError';
import * as Path from 'effect/Path';

import { AttachmentStore } from './attachment-store.js';
import { AttachmentRef } from './ref.js';

// Durable local attachment storage built entirely on Effect's FileSystem and
// Path interfaces. The application chooses their concrete platform Adapters;
// `@effect/platform-node/NodeServices` is the ordinary Node choice.

const storeError = (
  operation: 'put' | 'get',
  cause: PlatformError,
): AttachmentStore.AttachmentStoreError =>
  new AttachmentStore.AttachmentStoreError({ operation, cause });

const isAlreadyPresent = (error: PlatformError): boolean =>
  error.reason._tag === 'AlreadyExists';

const nativeErrorCode = (error: PlatformError): unknown => {
  const cause = 'cause' in error.reason ? error.reason.cause : undefined;
  if (typeof cause !== 'object' || cause === null || !('code' in cause)) {
    return undefined;
  }
  return cause.code;
};

// POSIX permits opening and syncing a directory. Windows and a few other
// FileSystem adapters do not, even though they still provide atomic rename.
// Keep those adapters usable while retaining failures such as permission
// errors from the actual file write, file sync, or rename operations.
const isUnsupportedDirectorySync = (error: PlatformError): boolean => {
  const code = nativeErrorCode(error);
  return (
    (error.reason.method === 'open' || error.reason.method === 'sync') &&
    (code === 'EISDIR' || code === 'EINVAL' || code === 'ENOTSUP')
  );
};

const blobPath = (
  root: string,
  path: Path.Path,
  ref: AttachmentRef.Ref,
): string => {
  const hex = ref.digest.slice(`${AttachmentRef.DIGEST_PREFIX}:`.length);
  return path.join(root, AttachmentRef.DIGEST_PREFIX, hex.slice(0, 2), hex);
};

const syncFile = (
  fs: FileSystem.FileSystem,
  filePath: string,
): Effect.Effect<void, PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const file = yield* fs.open(filePath, { flag: 'r+' });
      yield* file.sync;
    }),
  );

const syncDirectory = (
  fs: FileSystem.FileSystem,
  directory: string,
): Effect.Effect<void, PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const file = yield* fs.open(directory, { flag: 'r' });
      yield* file.sync;
    }),
  ).pipe(Effect.catchIf(isUnsupportedDirectorySync, () => Effect.void));

const service = (root: string) =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const storageRoot = path.resolve(root);
    const withCrypto = <A, E>(
      effect: Effect.Effect<A, E, Crypto.Crypto>,
    ): Effect.Effect<A, E> =>
      Effect.provideService(effect, Crypto.Crypto, crypto);

    const put = Effect.fn('AttachmentStoreFileSystem.put')(function* (
      bytes: Uint8Array,
      options: { readonly mediaType: string },
    ) {
      const ref = yield* withCrypto(AttachmentRef.fromBytes(bytes, options));
      const target = blobPath(storageRoot, path, ref);
      const directory = path.dirname(target);

      yield* fs.makeDirectory(directory, { recursive: true }).pipe(
        Effect.flatMap(() =>
          fs.makeTempFile({
            directory,
            prefix: '.vesper-attachment-',
            suffix: '.tmp',
          }),
        ),
        Effect.flatMap((temporary) =>
          fs
            .writeFile(temporary, bytes, { mode: 0o600 })
            .pipe(
              Effect.andThen(syncFile(fs, temporary)),
              Effect.andThen(
                fs
                  .rename(temporary, target)
                  .pipe(Effect.catchIf(isAlreadyPresent, () => Effect.void)),
              ),
              Effect.andThen(syncDirectory(fs, directory)),
              Effect.ensuring(
                fs.remove(temporary, { force: true }).pipe(Effect.ignore),
              ),
            ),
        ),
        Effect.mapError((error) => storeError('put', error)),
      );

      return ref;
    });

    const get = Effect.fn('AttachmentStoreFileSystem.get')(function* (
      ref: AttachmentRef.Ref,
    ) {
      const bytes = yield* fs
        .readFile(blobPath(storageRoot, path, ref))
        .pipe(
          Effect.mapError((error) =>
            error.reason._tag === 'NotFound'
              ? new AttachmentStore.AttachmentNotFound({ ref })
              : storeError('get', error),
          ),
        );
      return yield* withCrypto(AttachmentStore.verified(ref, bytes));
    });

    return AttachmentStore.Service.of({ put, get });
  });

/**
 * Content-addressed attachment storage rooted at `directory`.
 *
 * Writes publish through an atomic rename, reads always re-hash their bytes,
 * and the returned layer keeps platform choice visible in its requirement
 * channel.
 */
export const layer = (
  directory: string,
): Layer.Layer<
  AttachmentStore.Service,
  never,
  Crypto.Crypto | FileSystem.FileSystem | Path.Path
> => Layer.effect(AttachmentStore.Service, service(directory));

export * as AttachmentStoreFileSystem from './layer-filesystem.js';
