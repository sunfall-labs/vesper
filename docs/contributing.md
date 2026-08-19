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
concurrently with Knip, because they are independent and a contributor waiting
on four serial passes stops running the gate at all. Tests are last.

Every lane is also a script of its own, so a failure is reproducible with one
shorter command. `concurrently` groups and labels each lane's output so four
tools do not write an unreadable stream to the terminal. Use
`nub run verify:serial` when sequential output is more useful.

| Command                                   | What it runs                            |
| ----------------------------------------- | --------------------------------------- |
| `nub run build`                           | `tsdown`, plus package/type validation  |
| `nub run test`                            | `vitest run` over every `test/`         |
| `nub run typecheck`                       | non-emitting package/type checks        |
| `nub run typecheck:types`                 | tests, benchmarks, and examples         |
| `nub run lint` / `nub run lint:fix`       | `oxlint`, warnings denied               |
| `nub run format` / `nub run format:check` | `oxfmt`                                 |
| `nub run knip`                            | unused files, exports, and dependencies |
| `nub run benchmark`                       | the suite in `benchmarks/`              |

`ANTHROPIC_API_KEY=... nub run example:compliance-relay "your prompt"` and
`nub run example:live-smoke` run the two programs under `examples/`. Both reach
a real provider and need an API key in the environment; nothing else does, and
tests use scripted `LanguageModel`s.

`nub run typecheck` checks every package and the type-level test projects
without emitting. tsdown is the only artifact producer; a typecheck can never
overwrite or leave stale files in `dist`. Per-package `typecheck` scripts remain
the faster loop while editing one package.

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
having `nub` on `PATH` for daily use. CI uses the pinned official `setup-nub`
action with the same CLI version and caches against `nub.lock`.

The repository declares Nub as its package manager through
`devEngines.packageManager`, keeps workspace configuration in the root
`package.json`, and commits `nub.lock`. The exact executable version is also a
dev dependency so nested scripts use the same Nub that bootstrapped the
workspace. Run `nub pm which` to inspect the active package-manager identity.

The root workspace catalog owns development-time Effect family versions, so an
RC upgrade is one policy edit for the root, examples, benchmarks, and package
development dependencies. Published runtime and peer ranges remain concrete:
Nub 0.7.5 preserves `catalog:` and `workspace:*` in packed manifests instead of
rewriting them to npm ranges. For the same reason, publishable packages use an
exact version for a sibling Vesper dependency; private examples and benchmarks
can safely retain `workspace:*`.

Node.js 22 or newer is required locally; CI runs Node 22 and 24, and publishing
uses Node 24. See [the release procedure](releasing.md) for versioning and
publishing policy.

## Module organization

- Put each module in `packages/<module>/`, named `@sunfall/vesper-<module>`.
- Expose implementation files directly with `package.json` subpath exports.
- Do not create `index.ts` barrels, facade packages, or compatibility
  re-exports. A file that is not in `exports` is internal.
- Keep tests in `packages/<module>/test/`, named for what they exercise.
- A backend's internal contract suite lives beside the interface it verifies,
  not in a testkit package or the published export map. A shared testkit would
  have to depend on those packages while they depend on it to run their
  contracts, creating a cycle that per-package test runs may not expose.

## Tests

`nub run test` runs every package's `test/` directory as a single vitest project,
from the repository root. There is no per-package `test` script, and running
vitest from inside a package is not a supported shortcut.

`vitest.config.ts` aliases every `@sunfall/vesper-*` subpath to the **source**
file rather than to `dist`, so a failing test points at the file you would
edit and nothing has to be rebuilt between runs. It derives those aliases from
the `paths` in `tsconfig.base.json`, so TypeScript and Vitest share one source
map. Public package subpaths remain explicit in each package's `exports` map;
tsdown validates those built runtime and declaration targets with publint and
Are the types wrong? during every build.

Type-level assertions are load-bearing here — several tools pin their service
requirements as assertions that fail at compile rather than at run time — and
`vitest run` does not check them. `nub run typecheck:types` is what compiles
`packages/*/test`, `benchmarks/`, and `examples/*/src`. A test that only
asserts a type will pass `nub run test` while failing the gate.

### The Postgres suite

`packages/log-pg/test/layer.integration.test.ts` runs the core package's shared
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

One tsdown configuration builds every package in unbundled ESM form, generates
declarations, cleans stale output, keeps dependencies external, and validates
the finished packages with publint and Are the types wrong?. Packages publish
`dist` and `LICENSE`; `@sunfall/vesper-log-pg` also publishes its authoritative
`migrations` directory. Every `exports` entry names `./dist/*.js` and
`./dist/*.d.ts`: sources are not published, and a module absent from `exports`
is unreachable to a consumer however it got into `dist`.

`nub run publish:npm:dry-run` previews the alpha release, and
`nub run publish:npm` publishes it. Nub selects the publishable Vesper packages,
orders their workspace graph, attaches provenance, and skips versions already
on the registry, so re-running a half-finished release finishes it. The release
workflow selects beta, next, or latest when the tag or manual input calls for
another dist-tag.

## The layering rule

```
agent       -> effect, @sunfall/vesper-log, @sunfall/vesper-attachments
log         -> effect
log-pg      -> effect, @sunfall/vesper-log, @effect/sql-pg, pg
workspace   -> effect
attachments -> effect
mcp         -> effect, @sunfall/vesper-agent, @sunfall/vesper-log
```

**No Vesper package depends on a provider SDK.** The loop targets Effect's
`LanguageModel` service, and applications provide it from official packages
such as `@effect/ai-anthropic` or `@effect/ai-openai`. Do not add a Vesper
provider registry or duplicate their prompt, tool, stream, usage, credential,
or error adapters.

The provider-independent context policies live at the seam that consumes them:
`@sunfall/vesper-agent/context-window`. `pure` is the default and
`usageAnchored` is an explicit application wiring choice.

The `agent -> log` and `agent -> attachments` edges are admitted on narrow
arguments. `log` supplies the durable vocabulary and store Interface;
`attachments` supplies content-addressed prompt payloads. Neither knows about
providers, and attachment storage remains optional at runtime. Recording is opt-in through
`Conversation.make(agent, id)` and its bound `conversation.run(input)`, the
only entry points that name `LogStore`; an agent used through neither requires
no new service and writes nothing. Do not read this as
a precedent for `agent -> workspace`: the test it passed is that the
dependency cannot reintroduce provider knowledge and leaves the ordinary path
requiring exactly what it required before.

## One durability mechanism: the conversation log

A run recording to a conversation appends what happened as it happens, and
`conversation.run(input)` rebuilds a `Chat` from those records and
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

The hard run budget follows the same rule. The run-policy runtime is one mutable
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
`Definition.subagents` captures a tuple of branded `Agent.Child` rather than
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
