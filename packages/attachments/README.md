# `@sunfall/vesper-attachments`

Content-addressed blob storage for agent conversations, with content verified
again when it is read.

```bash
npm install @sunfall/vesper-attachments effect@4.0.0-rc.109
```

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-attachments/attachment-store`, `/ref`, and `/layer-memory`.

`@sunfall/vesper-attachments/attachment-store-contract` is test-only. Backend
authors must install `vitest@^4.1.9` as a dev dependency before importing it.

See the [Vesper repository](https://github.com/sunfall-labs/vesper#readme) for
project status and package documentation.
