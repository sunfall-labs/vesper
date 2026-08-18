# `@sunfall/vesper-attachments`

Content-addressed blob storage for agent conversations, with content verified
again when it is read.

```bash
npm install @sunfall/vesper-attachments effect@4.0.0-rc.109
```

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-attachments/attachment-store`, `/ref`, and `/layer-memory`.

The repository keeps a shared conformance suite beside the store interface so
every built-in backend is held to the same behaviour. It is test
infrastructure, not part of the published package interface.

See the [Vesper repository](https://github.com/sunfall-labs/vesper#readme) for
project status and package documentation.
