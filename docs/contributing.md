# Contributing

Rules for changing anything under `packages/`. [`../README.md`](../README.md)
is how the library works, [`../Design.md`](../Design.md) is why it is shaped
this way and what is deliberately missing, [`../CONTEXT.md`](../CONTEXT.md) is
the vocabulary.

## The gate

```bash
pnpm install
pnpm verify
```

`pnpm verify` is what CI runs and what to run before opening a pull request.
The build goes first, because the static gates after it resolve cross-package
imports against built declarations; format, lint, and typecheck then run
concurrently, because they are independent and a contributor waiting on three
serial passes stops running the gate at all. Tests are last.

Every lane is also a script of its own, so a failure is reproducible with one
shorter command — which the gate prints alongside it. A lane's output is
buffered and flushed with a prefix when the lane ends, since three compilers
writing to one terminal at once is unreadable, and lane failures are collected
rather than thrown, since a lint failure and a typecheck failure are usually
one edit apart. Four lanes run at once by default; `pnpm verify:serial`,
`--concurrency=N`, or `VESPER_VERIFY_CONCURRENCY` change that.

| Command                             | What it runs                        |
| ----------------------------------- | ----------------------------------- |
| `pnpm build`                        | `tsgo` per package, `src` to `dist` |
| `pnpm test`                         | `vitest run` over every `test/`     |
| `pnpm typecheck`                    | `tsgo -b` over the project graph    |
| `pnpm typecheck:types`              | tests, benchmarks, and examples     |
| `pnpm lint` / `pnpm lint:fix`       | `oxlint`, warnings denied           |
| `pnpm format` / `pnpm format:check` | `oxfmt`                             |
| `pnpm benchmark`                    | the suite in `benchmarks/`          |

`pnpm example:compliance-relay` and `pnpm example:live-smoke` run the two
programs under `examples/`. Both reach a real provider and need an API key in
the environment; nothing else does, and every test runs against Pi's faux one.

`pnpm typecheck` uses `tsgo -b` rather than a per-package `--noEmit` pass
because the packages are TypeScript project references: `agent` resolves
`@sunfall/vesper-log` through the declarations `log` emits, so a clean clone
has nothing to typecheck against until those exist. `-b` builds what it needs
and is incremental afterwards. The per-package `typecheck` scripts still work
once `dist` is present, and are the faster loop while editing one package.

The pnpm version is pinned by `packageManager`, so
[corepack](https://nodejs.org/api/corepack.html) or a matching install is
enough. CI runs Node 25.

## Module organization

- Put each module in `packages/<module>/`, named `@sunfall/vesper-<module>`.
- Expose implementation files directly with `package.json` subpath exports.
- Do not create `index.ts` barrels, facade packages, or compatibility
  re-exports. A file that is not in `exports` is internal.
- Keep tests in `packages/<module>/test/`, named for what they exercise.
- A backend's contract suite lives in the package that owns the interface —
  `@sunfall/vesper-log/log-store-contract`,
  `@sunfall/vesper-workspace/workspace-contract`,
  `@sunfall/vesper-attachments/attachment-store-contract` — not in a testkit
  of its own. A shared testkit would have to depend on those packages while
  they depend on it to run their contracts, which is a cycle, and one that
  per-package test runs never build the graph to notice.

## Tests

`pnpm test` runs every package's `test/` directory as a single vitest project,
from the repository root. There is no per-package `test` script, and running
vitest from inside a package is not a supported shortcut.

`vitest.config.ts` aliases every `@sunfall/vesper-*` subpath to the **source**
file rather than to `dist`, so a failing test points at the file you would
edit and nothing has to be rebuilt between runs. That alias map mirrors each
package's `exports`, and the same list appears as `paths` in
`tsconfig.base.json`. A new subpath therefore has to be added in three places:
the package's `exports`, the alias map, and `paths`.

Type-level assertions are load-bearing here — several tools pin their service
requirements as assertions that fail at compile rather than at run time — and
`vitest run` does not check them. `pnpm typecheck:types` is what compiles
`packages/*/test`, `benchmarks/`, and `examples/*/src`. A test that only
asserts a type will pass `pnpm test` while failing the gate.

### The Postgres suite

`packages/log/test/layer-pg.integration.test.ts` runs the shared
`log-store-contract` against real Postgres, plus the two things a memory
backend cannot demonstrate: that the schema the test harness creates is the
schema the layer actually queries, and that a `LISTEN` connection dying
reaches the consumer as a failure rather than as a tail that looks healthy and
delivers nothing.

It is skipped unless opted into, because a suite that silently needs a
container runtime is a suite that fails for a contributor who has not been
told:

```bash
RUN_POSTGRES_INTEGRATION=1 pnpm test
```

It provisions and drops a database through testcontainers, so a container
runtime has to be reachable. Docker works as-is; Podman works if `DOCKER_HOST`
points at its socket first. CI runs this as its own job, so "skipped by
default" does not mean "never run".

## Building and publishing

Each package builds with `tsgo -p tsconfig.json` after deleting `dist` and
`.tsbuildinfo`, so a renamed or removed module never survives in the output.
`files` is `["dist", "LICENSE"]` and every `exports` entry names `./dist/*.js`
and `./dist/*.d.ts`: sources are not published, and a module absent from
`exports` is unreachable to a consumer however it got into `dist`.

`pnpm publish:npm` publishes from there. It defaults to the `alpha` dist-tag,
takes `--dry-run` and `--package <name>`, and is idempotent — a version
already on the registry is skipped rather than failed, so re-running a
half-finished release finishes it.

## The layering rule

```
runtime     -> pi, agent, log                (composition — the only one)
agent       -> effect, @sunfall/vesper-log
pi          -> effect, @earendil-works/pi-ai, @earendil-works/pi-agent-core
log         -> effect
workspace   -> effect
attachments -> effect
```

**`@sunfall/vesper-agent` must not depend on `@sunfall/vesper-pi`, and
`@sunfall/vesper-pi` must not depend on `@sunfall/vesper-agent`.** This is the
property that makes the package count worth paying for: the loop targets the
`LanguageModel` service tag from `effect` itself, so provider choice and retry
policy are decided once, at application wiring.

Where the two must agree on something — the context-overflow marker, the shape
of a context-window estimator — each states it independently and
`@sunfall/vesper-runtime` is where a compiler compares them.
`agent/src/context-window.ts` states the estimator's shape,
`pi/src/compaction.ts` produces a value of it without importing the statement,
and the assignment in `runtime.ts` is the check;
`runtime/test/protocol.test.ts` covers the rest. Do not "simplify" this into
an import in either direction. The point is that a second provider adapter can
satisfy the same shape without either package learning about it.

`@sunfall/vesper-runtime` is the single exception, and it exists so the rule
can hold. It is not a barrel: it re-exports nothing and instead decides
defaults and layer order.

`agent -> log` is the one edge below `runtime`, admitted on a narrow argument.
`log` depends on nothing but `effect`, and what it supplies is a data
vocabulary — records, offsets, a store interface — not a provider and not a
durability strategy. Recording is opt-in through `agent.recordingTo(id)` and
`agent.resume(id, input)`, the only places that name `LogStore`; an agent that
calls neither requires no new service and writes nothing. Do not read this as
a precedent for `agent -> workspace`: the test it passed is that the
dependency cannot reintroduce provider knowledge and leaves the ordinary path
requiring exactly what it required before.

## Pi version lockstep

`pi-agent-core@X` requires `pi-ai@^X`. `@sunfall/vesper-pi` depends on both at
0.80.2 and `pnpm-workspace.yaml` pins both there through `overrides`. A
mismatch puts two copies of `pi-ai` in the graph and breaks type identity at
the adapter seam — the one place this repository cannot afford it, and one
whose error points at the call site rather than at the duplication. Do not
bump one without the other, and move both overrides in the same change.

`Compaction.defaultSystem` is transcribed from Pi's unexported
`SUMMARIZATION_SYSTEM_PROMPT`. Nothing can detect drift, so re-read it on any
`pi-agent-core` bump.

## One durability mechanism: the conversation log

A run recording to a conversation appends what happened as it happens, and
`agent.resume(conversationId, input)` rebuilds a `Chat` from those records and
continues from the next turn. A crashed run re-pays the provider for nothing
it completed and re-runs no tool call whose outcome was recorded. Do not add a
second mechanism without beating the argument in
[`../Design.md`](../Design.md) that removed the previous two.

What remains at the provider seam is a **retry**, in `@sunfall/vesper-pi/retry`.
It absorbs a transient failure inside one model call, which is the only
granularity where a 429 costs a wait rather than a re-run of the turn and
everything the turn did.

A durable workflow engine's steps — DBOS's, for instance — are a separate seam
and not a competitor: steps make **effects** exactly-once, the log makes
**conversation state** durable and resumable, and the retry absorbs **provider
blips**.

**A run's log claim is a value, not an ambient.** `AgentLog.Session` is what
child sessions, signal delivery, and resuming tool dispatch all reach through,
and it is threaded lexically: `agent.ts`'s `entryFor(session)` builds the loop
around one, and `Agent.runInSession` hands one across a delegation boundary.
It is deliberately not a `Context.Reference` defaulting to "not recording".
Effect's guidance is not to hide persistence behind a defaulted reference, and
the deleted `Checkpointer.RunId` was this repository's own instance of what
that costs: a reference defaulting to a shared namespace, so a caller who
forgot to scope a run got plausible behaviour, someone else's answers, and no
signal. `Compaction.Policy`'s estimator seam is the deliberate contrast — a
defaulted estimate hides nothing, because the run still works and the reactive
overflow path catches what a cruder guess misses.

## Subagent services

A tool declares the service keys its handler needs as _values_, on
`Tool.make`'s `dependencies` option. Effect's tool model requires this —
`dependencies` takes `Context.Key` values, and services cannot be recovered
from a type. In exchange those keys reach the agent's `Requires`, and because
`Definition.subagents` captures a tuple of `Agent.Named` rather than
`ReadonlyArray<Agent.Any>`, they flow on into every parent's requirement
channel, transitively through delegation chains.

The delegation tool a parent sees is built from a child's _type_, so it cannot
discover the child's keys on its own — that is the ceiling on automatic
propagation, and it is why the tuple matters. Forgetting to declare a
dependency is not a runtime surprise either way: the handler is then required
to be self-contained, and using an undeclared service is a compile error at
the handler.

## Persistence serializability

Everything crossing the log is a `Schema` type, so an unserializable record is
caught at the append rather than months later during a recovery nobody is
watching. Model typed errors that cross that boundary with
`Schema.TaggedErrorClass`; errors that never cross one may use
`Data.TaggedError`.

The three `Schema.Unknown` fields — `RunStarted.prompt`, `ToolCall.params`,
`ToolOutcome.result` — are the exception, held that way because their real
schemas belong to `effect/unstable/ai` and to whatever toolkit is in play.
`ToolOutcome.result` stores the toolkit's **encoding** of a result rather than
the decoded value, which is both what a resuming dispatch can serve back and
what `AgentHistory` puts in front of the model.
