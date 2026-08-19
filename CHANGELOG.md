# Changelog

Notable changes to Vesper are recorded here. The project is pre-1.0, so every
entry should state compatibility impact and migration guidance when applicable.

## Unreleased

No changes yet.

## 0.1.0-alpha.1 - 2026-08-18

First public alpha of the six-package Vesper framework:

- Effect-native agent loops with typed tools, dynamic run-start tool sources,
  subagents, skills, interception, state, hard run budgets, and observability.
- Durable conversations with resumption, branching, steering, compaction,
  recorded tool outcomes, and keyed human approval waits that re-enter the
  original handler.
- Memory and PostgreSQL append-only log stores, content-addressed attachment
  stores, an explicitly authorized workspace toolkit, and namespaced MCP
  consumption.
- Bounded MCP model-facing metadata/results, log batches, signals, and
  workspace writes, with typed failures at each public boundary.
- Packed-artifact import and declaration checks, Node 22/24 CI, production
  dependency auditing, and provenance-ready npm publishing.

Compatibility: Node.js 22 or newer is required. `effect` and the official
Effect provider packages must resolve to `4.0.0-rc.109`. This is the first
published version, so there is no package migration; adopters of the earlier
repository API should follow [Migrating to Conversation](docs/migrating-to-conversation.md).
The former public `DynamicToolkit.append` helper is no longer exported;
dynamic sources are supplied on `Agent.make({ dynamicTools })` and composed by
the agent at run start.
