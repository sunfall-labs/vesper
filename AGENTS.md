# Agent Instructions

## Commands

This repository uses [Bun](https://bun.sh) as its package manager and task
runner, pinned through `packageManager` in the root `package.json`. Install
with `bun install` and run tasks through `bun run`; do not invoke `npm`, `npx`,
`nub`, `node`, `vitest`, `tsc`, `oxfmt`, or `oxlint` directly for repository
development tasks. The packages target Node.js, so scripts still run their
tools under Node; Bun is the package manager and script runner, not the
runtime under test.

Use the scripts declared in the root `package.json`:

```bash
bun run verify
bun run build
bun run test
bun run typecheck
bun run lint
bun run lint:fix
bun run lint:prune
bun run format
bun run format:check
bun run knip
bun run benchmark
```

Pass test filters directly after the script name when a focused run is needed.
Do not insert `--`; Vitest treats the remaining path as positional data and
runs the full suite:

```bash
bun run test packages/agent/test/workflow.test.ts
```

Prefer the nearest existing script over reproducing its implementation.
For example, use `bun run typecheck`, not a direct `tsc` command; use
`bun run format`, not a direct `oxfmt` command; and use `bun run verify`, not
a hand-written sequence of its build, static, and test gates.

Direct `npm install` snippets in package README files are documentation for
external consumers, not commands for working in this repository.
