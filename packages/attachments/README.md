# `@sunfall/vesper-attachments`

Content-addressed blob storage for agent conversations, with content verified
again when it is read.

```bash
npm install @sunfall/vesper-attachments effect@4.0.0-rc.109
```

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-attachments/attachment-store`, `/ref`, `/layer-memory`, and
`/layer-filesystem`.

The filesystem adapter uses Effect's `FileSystem`, `Path`, and `Crypto`
interfaces, so the application retains control of the platform implementation:

```bash
npm install @effect/platform-node@4.0.0-rc.109
```

```ts
import * as NodeServices from '@effect/platform-node/NodeServices';
import { AttachmentStoreFileSystem } from '@sunfall/vesper-attachments/layer-filesystem';
import { Layer } from 'effect';

const attachments = AttachmentStoreFileSystem.layer(
  '/var/lib/vesper/attachments',
).pipe(Layer.provide(NodeServices.layer));
```

Writes are atomically published under their validated SHA-256 address. Reads
always recompute the digest and length before returning bytes. Rebuilding the
layer against the same directory preserves every attachment.

The repository keeps a shared conformance suite beside the store interface so
every built-in backend is held to the same behaviour. It is test
infrastructure, not part of the published package interface.

See the [Vesper repository](https://github.com/sunfall-labs/vesper#readme) for
project status and package documentation.
