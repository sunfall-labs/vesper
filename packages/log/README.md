# `@sunfall/vesper-log`

An append-only conversation log with per-record offsets, producer fencing,
tailing, and in-memory and Postgres backends.

`LogStore.acquire(path, producerId, expected?)` retains unconditional legacy
fencing when `expected` is omitted. Supplying the observed epoch and head makes
validation-to-acquisition atomic: a changed stream fails with `conflict`
without bumping the epoch or fencing its current producer.

`LogStore.readBackwards` pages from an optional exclusive `before` bound. Its
records are newest-first; the cursor is the oldest returned offset and
`upToDate` means the stream beginning was reached. Both bundled backends share
the same validation and contract tests. Postgres uses the existing
`ai_log_records_path_offset_idx` in reverse order, so this operation requires no
DDL beyond `001-initial.sql`.

```bash
npm install @sunfall/vesper-log effect@4.0.0-rc.109
```

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-log/log-store`, `/record`, `/layer-memory`, and `/layer-pg`.

Conversation records expose `FORMAT_VERSION`. New `RunStarted` records and
resume aggregates carry that version plus agent name/revision. Those fields are
optional in the wire schema only so old data can decode far enough for the
agent package to reject it with an actionable compatibility error.

The authoritative Postgres DDL is published as
`@sunfall/vesper-log/migrations/001-initial.sql`. Applications should copy its
statements into their migration system; the layer never migrates at runtime.

`@sunfall/vesper-log/log-store-contract` is a test-only Vitest contract for
third-party backend implementations. Install `vitest@^4.1.9` as a dev
dependency before importing that subpath; ordinary runtime modules do not load
Vitest.

Postgres `changes` subscriptions share one notification session per layer.
Channels are reference-counted, so multiple tails do not consume one pool
connection each. A transaction-local 30-second statement timeout bounds append
SQL and can be configured with
`fromPool(pool, { transactionStatementTimeoutMs })`.

See the [Vesper repository](https://github.com/sunfall-labs/vesper#readme) for
the persistence and resumption model.
