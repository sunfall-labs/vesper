# Agent Instructions

## Commands

This repository uses [Nub](https://nubjs.com) as its package manager and task
runner. Use the pinned workspace binary through `nub`; do not invoke `npm`,
`npx`, `node`, `vitest`, `tsgo`, `oxfmt`, or `oxlint` directly for repository
development tasks.

Use the scripts declared in the root `package.json`:

```bash
nub run verify
nub run build
nub run test
nub run typecheck
nub run lint
nub run lint:fix
nub run format
nub run format:check
nub run benchmark
```

Pass test filters after `--` when a focused run is needed:

```bash
nub run test -- packages/agent/test/workflow.test.ts
```

Prefer the nearest existing Nub script over reproducing its implementation.
For example, use `nub run typecheck`, not a direct `tsgo` command; use
`nub run format`, not a direct `oxfmt` command; and use `nub run verify`, not
`node scripts/verify.mjs`.

Direct `npm install` snippets in package README files are documentation for
external consumers, not commands for working in this repository. The one
bootstrap exception is the documented first-time installation command when
`nub` is not available yet.
