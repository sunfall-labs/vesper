# `@sunfall/vesper-workspace`

A filesystem and shell behind one swappable Effect service, plus the six tools
an agent needs to work in that filesystem.

```bash
npm install @sunfall/vesper-workspace effect@4.0.0-rc.109
```

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-workspace/driver`, `/layer-local`, and `/tools`.

`@sunfall/vesper-workspace/workspace-contract` is test-only. Driver authors
must install `vitest@^4.1.9` as a dev dependency before importing it.

See the [workspace guide](https://github.com/sunfall-labs/vesper/blob/main/docs/workspace.md)
for usage and security boundaries.
