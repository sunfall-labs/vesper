# Agent Instructions

## Commands

This repository uses [Nub](https://nubjs.com) as its package manager and task
runner. Use the pinned workspace binary through `nub`; do not invoke `npm`,
`npx`, `node`, `vitest`, `tsc`, `oxfmt`, or `oxlint` directly for repository
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
nub run knip
nub run benchmark
```

Pass test filters directly after the script name when a focused run is needed.
Do not insert `--`; Vitest treats the remaining path as positional data and
runs the full suite:

```bash
nub run test packages/agent/test/workflow.test.ts
```

Prefer the nearest existing Nub script over reproducing its implementation.
For example, use `nub run typecheck`, not a direct `tsc` command; use
`nub run format`, not a direct `oxfmt` command; and use `nub run verify`, not
a hand-written sequence of its build, static, and test gates.

Direct `npm install` snippets in package README files are documentation for
external consumers, not commands for working in this repository. The one
bootstrap exception is the documented first-time installation command when
`nub` is not available yet.
