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
`@sunfall/vesper-log/log-store`, `/record`, `/layer-memory`, `/layer-pg`, and
`/pg-client`.

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

The Postgres backend accepts the official `@effect/sql-pg` client. With Effect
4.0.0-rc.109, use Vesper's corrected official-client construction layer because
that release's built-in `PgClient.listen` can miss an opening notification and
does not fail after a listener connection ends:

```ts
import { VesperPgClient } from '@sunfall/vesper-log/pg-client';

const pg = VesperPgClient.layer({ url });
const store = LogStorePg.layer().pipe(Layer.provide(pg));

LogStorePg.make(client, { transactionStatementTimeoutMs: 30_000 });
LogStorePg.layer({ transactionStatementTimeoutMs: 30_000 });
```

`make` accepts a concrete `PgClient.PgClient`. The contextual `layer` requires
that service from an `@effect/sql-pg` layer. The transaction-local statement
timeout defaults to 30 seconds and bounds append SQL. `VesperPgClient.layer`
still provides the official `PgClient.PgClient` and generic `SqlClient`; queries
and transactions remain Effect SQL operations. It replaces only the broken
rc.109 listener implementation with a scoped dedicated connection per
subscription. A standard client remains accepted when its `listen` behavior
satisfies the same lifecycle contract. The corrected listener emits an empty
readiness element only after `LISTEN` succeeds; `LogStorePg.changes` maps that
element to its required opening wake-up. Later empty payload notifications are
also preserved.

See the [Vesper repository](https://github.com/sunfall-labs/vesper#readme) for
the persistence and resumption model.
