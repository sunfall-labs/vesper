# Contributing

Rules for changing anything under `packages/`. [`../README.md`](../README.md)
is how the library works, [`../Design.md`](../Design.md) is why it is shaped
this way and what is deliberately missing, [`../CONTEXT.md`](../CONTEXT.md) is
the vocabulary.

## The gate

```bash
npx @nubjs/nub@0.7.5 install   # first time; see "Getting nub" below
nub run verify
```

`nub run verify` is what CI runs and what to run before opening a pull request.
The build goes first, because the static gates after it resolve cross-package
imports against built declarations; format, lint, and typecheck then run
concurrently, because they are independent and a contributor waiting on three
serial passes stops running the gate at all. Tests are last.

Every lane is also a script of its own, so a failure is reproducible with one
shorter command — which the gate prints alongside it. A lane's output is
buffered and flushed with a prefix when the lane ends, since three compilers
writing to one terminal at once is unreadable, and lane failures are collected
rather than thrown, since a lint failure and a typecheck failure are usually
one edit apart. Four lanes run at once by default; `nub run verify:serial`,
`--concurrency=N`, or `VESPER_VERIFY_CONCURRENCY` change that.

| Command                                   | What it runs                        |
| ----------------------------------------- | ----------------------------------- |
| `nub run build`                           | `tsgo` per package, `src` to `dist` |
| `nub run test`                            | `vitest run` over every `test/`     |
| `nub run typecheck`                       | `tsgo -b` over the project graph    |
| `nub run typecheck:types`                 | tests, benchmarks, and examples     |
| `nub run lint` / `nub run lint:fix`       | `oxlint`, warnings denied           |
| `nub run format` / `nub run format:check` | `oxfmt`                             |
| `nub run benchmark`                       | the suite in `benchmarks/`          |

`nub run example:compliance-relay` and `nub run example:live-smoke` run the two
programs under `examples/`. Both reach a real provider and need an API key in
the environment; nothing else does, and tests use scripted `LanguageModel`s.

`nub run typecheck` uses `tsgo -b` rather than a per-package `--noEmit` pass
because the packages are TypeScript project references: `agent` resolves
`@sunfall/vesper-log` through the declarations `log` emits, so a clean clone
has nothing to typecheck against until those exist. `-b` builds what it needs
and is incremental afterwards. The per-package `typecheck` scripts still work
once `dist` is present, and are the faster loop while editing one package.

### Getting nub

Nub is pinned as an ordinary devDependency, `@nubjs/nub` at **0.7.5**, so the
lockfile records the exact version and every nested `nub` a script invokes is
that one. Bootstrapping needs one command that does not depend on having nub
yet:

```bash
npx @nubjs/nub@0.7.5 install
```

After that `node_modules/.bin/nub` exists, and `npm install -g @nubjs/nub@0.7.5`
(or [nubjs.com/install.sh](https://nubjs.com/install.sh)) is the convenience of
having `nub` on `PATH` for daily use. CI does exactly this, reading the version
out of `devDependencies` so the bootstrap and the pin cannot drift apart.

The repository declares Nub as its package manager through
`devEngines.packageManager`, keeps workspace configuration in the root
`package.json`, and commits `nub.lock`. The exact executable version is also a
dev dependency so nested scripts use the same Nub that bootstrapped the
workspace. Run `nub pm which` to inspect the active package-manager identity.

Publishable packages use exact versions for dependencies on sibling Vesper
packages rather than `workspace:*`. Nub 0.7.5 preserves the workspace protocol
in packed manifests, which npm consumers reject with `EUNSUPPORTEDPROTOCOL`.
Private examples and benchmarks retain `workspace:*` because they are never
packed.

CI runs Node 22 and 24; publishing uses Node 24.

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

`nub run test` runs every package's `test/` directory as a single vitest project,
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
`vitest run` does not check them. `nub run typecheck:types` is what compiles
`packages/*/test`, `benchmarks/`, and `examples/*/src`. A test that only
asserts a type will pass `nub run test` while failing the gate.

### The Postgres suite

`packages/log/test/layer-pg.integration.test.ts` runs the shared
`log-store-contract` against real Postgres, plus the two things a memory
backend cannot demonstrate: that the schema the test harness creates is the
schema the layer actually queries, and that a `LISTEN` connection dying
reaches the consumer as a failure rather than as a tail that looks healthy and
delivers nothing. Queries and transactions run through Effect SQL's official
`PgClient`; the suite also pins the corrected RC.109 listener readiness,
transaction rollback, timeout isolation, and cross-client notification
semantics.

It is skipped unless opted into, because a suite that silently needs a
container runtime is a suite that fails for a contributor who has not been
told:

```bash
RUN_POSTGRES_INTEGRATION=1 nub run test
```

It provisions and drops a database through testcontainers, so a container
runtime has to be reachable. Docker works as-is; Podman works if `DOCKER_HOST`
points at its socket first. CI runs this as its own job, so "skipped by
default" does not mean "never run".

## Building and publishing

Each package builds with `tsgo -p tsconfig.json` after deleting `dist` and
`.tsbuildinfo`, so a renamed or removed module never survives in the output.
Packages publish `dist` and `LICENSE`; `@sunfall/vesper-log` also publishes its
authoritative `migrations` directory. Every `exports` entry names
`./dist/*.js` and `./dist/*.d.ts`: sources are not published, and a module
absent from `exports` is unreachable to a consumer however it got into `dist`.

`nub run publish:npm` publishes from there. It defaults to the `alpha` dist-tag,
takes `--dry-run` and `--package <name>`, and is idempotent — a version
already on the registry is skipped rather than failed, so re-running a
half-finished release finishes it.

## The layering rule

```
agent       -> effect, @sunfall/vesper-log
log         -> effect
workspace   -> effect
attachments -> effect
```

**No Vesper package depends on a provider SDK.** The loop targets Effect's
`LanguageModel` service, and applications provide it from official packages
such as `@effect/ai-anthropic` or `@effect/ai-openai`. Do not add a Vesper
provider registry or duplicate their prompt, tool, stream, usage, credential,
or error adapters.

The provider-independent context policies live at the seam that consumes them:
`@sunfall/vesper-agent/context-window`. `pure` is the default and
`usageAnchored` is an explicit application wiring choice.

`agent -> log` is the one Vesper-package edge, admitted on a narrow argument.
`log` depends on nothing but `effect`, and what it supplies is a data
vocabulary — records, offsets, a store interface — not a provider and not a
durability strategy. Recording is opt-in through `agent.recordingTo(id)` and
`agent.resume(id, input)`, the only places that name `LogStore`; an agent that
calls neither requires no new service and writes nothing. Do not read this as
a precedent for `agent -> workspace`: the test it passed is that the
dependency cannot reintroduce provider knowledge and leaves the ordinary path
requiring exactly what it required before.

## One durability mechanism: the conversation log

A run recording to a conversation appends what happened as it happens, and
`agent.resume(conversationId, input)` rebuilds a `Chat` from those records and
continues from the next turn. A crashed run re-pays the provider for nothing
it completed and re-runs no tool call whose outcome was recorded. Do not add a
second mechanism without beating the argument in
[`../Design.md`](../Design.md) that removed the previous two.

Provider retries belong in the official client's `HttpClient` transformation,
below streamed output and tool resolution. Wrapping a whole `LanguageModel`
call risks duplicating output or re-running tools.

A durable workflow engine's activities — Effect Workflow's, for instance — are
a separate seam and not a competitor: activities make **effects** replayable,
the log makes
**conversation state** durable and resumable, and provider HTTP policy absorbs
**provider blips**.

**A run's log claim is a value, not an ambient.** `AgentLog.Session` is what
child sessions, signal delivery, and resuming tool dispatch all reach through,
and it is threaded lexically: `agent.ts`'s `entryFor(session)` builds the loop
around one, and an internal symbol protocol hands it across a delegation
boundary without exposing a public session/runtime invocation method.
It is deliberately not a `Context.Reference` defaulting to "not recording".
Effect's guidance is not to hide persistence behind a defaulted reference, and
the deleted `Checkpointer.RunId` was this repository's own instance of what
that costs: a reference defaulting to a shared namespace, so a caller who
forgot to scope a run got plausible behaviour, someone else's answers, and no
signal. `Compaction.Policy`'s estimator seam is the deliberate contrast — a
defaulted estimate hides nothing, because the run still works and the reactive
overflow path catches what a cruder guess misses.

The hard run budget follows the same rule. `RunPolicy.Runtime` is one mutable
ledger scoped to a root run and passed explicitly through generated delegation
handlers into descendant loops. Never recreate it in a child, put it in a
module global, or implement a hard limit as a `StopCondition`: steers may
override stop conditions by design. Count provider calls before invocation,
including compaction retries, and account provider-reported tokens after the
finish part; token enforcement may therefore overshoot by one completed model
call but never starts a later call after exhaustion is known.

Recording filters belong at `AgentLog.Session.append`, after live model/tool
behavior and before schema encoding. Compile an effectful application policy
once against its context, carry the requirement-free runtime in the session,
and inherit it into child sessions. The raw policy must remain explicit; do not
hide persistence filtering behind an ambient default.

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
`Schema.TaggedError`; errors that never cross one may use `Data.TaggedError`.

The three `Schema.Unknown` fields — `RunStarted.prompt`, `ToolCall.params`,
`ToolOutcome.result` — are the exception, held that way because their real
schemas belong to `effect/unstable/ai` and to whatever toolkit is in play.
`ToolOutcome.result` stores the toolkit's **encoding** of a result rather than
the decoded value, which is both what a resuming dispatch can serve back and
what `AgentHistory` puts in front of the model.
