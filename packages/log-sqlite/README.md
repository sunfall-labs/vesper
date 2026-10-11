# `@sunfall/vesper-log-sqlite`

The opt-in SQLite adapter for `@sunfall/vesper-log`. It implements the
append-only conversation `LogStore` over Effect's generic `SqlClient`, so the
same store runs on Node, Bun, and a Cloudflare Durable Object. Only the
`SqlClient` layer differs.

| Entry                                         | Runtime                     |
| --------------------------------------------- | --------------------------- |
| `@sunfall/vesper-log-sqlite/layer`            | the store, any `SqlClient`  |
| `@sunfall/vesper-log-sqlite/layer-native`     | `node:sqlite`, `bun:sqlite` |
| `@sunfall/vesper-log-sqlite/layer-cloudflare` | Durable Object SQLite       |

`LogStoreSqlite.migrate()` creates the tables and is safe to run at every
startup. The same DDL is published as
`@sunfall/vesper-log-sqlite/migrations/001-initial.sql`.

```ts
import * as NodeServices from '@effect/platform-node/NodeServices';
import { LogStoreSqlite } from '@sunfall/vesper-log-sqlite/layer';
import { SqliteNative } from '@sunfall/vesper-log-sqlite/layer-native';
import { Layer } from 'effect';

const store = LogStoreSqlite.layer().pipe(
  Layer.provideMerge(SqliteNative.layer('./vesper.db')),
  Layer.provide(NodeServices.layer),
);
```

SQLite has no cross-process notification primitive, so `LogStore.changes`
wakes followers through Effect's in-process `Reactivity` service. An append
invalidates its path after commit. That is correct whenever one process owns
the database, which is the case for a file used by one server and for a
Durable Object.

## Cloudflare Durable Objects

A Durable Object's `ctx.storage.sql` is synchronous SQLite, which is the same
shape as the native layer's driver. `SqliteCloudflare.layer(ctx.storage)`
adapts it to a `SqlClient`; the SQL, schema, and `LogStoreSqlite` are the ones
the other runtimes use. This is a storage adapter, not a second durability
story: the conversation log, producer fencing, and resume semantics are
exactly Vesper's.

```ts
import { DurableObject } from 'cloudflare:workers';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { LogStoreSqlite } from '@sunfall/vesper-log-sqlite/layer';
import { SqliteCloudflare } from '@sunfall/vesper-log-sqlite/layer-cloudflare';
import { Effect, Layer, ManagedRuntime } from 'effect';

import { modelLayer, supportAgent } from './agent.js';

const RESUME_AFTER_MS = 30_000;

export class SupportConversation extends DurableObject {
  // In-memory on purpose: it is false again after an eviction, which is how
  // the alarm below tells an orphaned run from one that is still live.
  private running = false;
  private readonly runtime = ManagedRuntime.make(
    Layer.effectDiscard(LogStoreSqlite.migrate()).pipe(
      Layer.provideMerge(LogStoreSqlite.layer()),
      Layer.provideMerge(SqliteCloudflare.layer(this.ctx.storage)),
      Layer.provideMerge(SqliteCloudflare.cryptoLayer),
      Layer.provideMerge(modelLayer(this.env)),
    ),
  );

  private async run(input?: string): Promise<string> {
    this.running = true;
    try {
      const conversation = Conversation.make(
        supportAgent,
        this.ctx.id.toString(),
      );
      const result = await this.runtime.runPromise(conversation.run(input));
      await this.ctx.storage.deleteAlarm();
      return result.text;
    } finally {
      this.running = false;
    }
  }

  async fetch(request: Request): Promise<Response> {
    // Arm the alarm before the run: if the object is evicted mid-run, the
    // alarm is what brings it back.
    await this.ctx.storage.setAlarm(Date.now() + RESUME_AFTER_MS);
    return new Response(await this.run(await request.text()));
  }

  async alarm(): Promise<void> {
    if (this.running) {
      // Still alive and mid-run: resuming would fence our own live run.
      await this.ctx.storage.setAlarm(Date.now() + RESUME_AFTER_MS);
      return;
    }
    // Continue from durable state alone: no new user message.
    await this.run();
  }
}
```

How Vesper's resume maps onto Durable Object lifecycle:

- A Durable Object can be evicted or restarted at any time. A run that was in
  flight is then an orphan: its log has a `RunStarted` without a `RunSettled`.
  Everything the run appended is already in the object's SQLite storage.
- `conversation.run()` with no input is Vesper's resume. It rebuilds the
  prompt from the log, does not re-ask the provider for turns that completed,
  and does not re-run tool calls whose outcomes were recorded. A tool that had
  started but not finished is never silently re-dispatched; handle it with the
  `onIndeterminateToolCall` interceptor, or the run fails safely and stays
  orphaned (see `docs/conversations.md`).
- Claiming a conversation fences the previous producer. That is why the alarm
  above checks `running` first: an alarm that fires in an object that is still
  executing the run would fence it. The flag is plain memory, so it resets on
  eviction and the alarm resumes.
- Alarms are at-least-once and retried on failure, which suits a resume that
  is idempotent against the log. Clear the alarm once a run settles.
- Keep one conversation per object (key the object by conversation id). The
  object is then the single process the in-memory `changes` wake-ups need.

### How the adapter works

- **Statements** go through `ctx.storage.sql.exec(query, ...bindings)`. Every
  cursor is drained immediately (`toArray()` for rows, `raw()` for positional
  values).
- **Transactions** never use `BEGIN`, `COMMIT`, or `SAVEPOINT` SQL, which a
  Durable Object rejects. `withTransaction` runs its body inside
  `ctx.storage.transactionSync`, so a failure or defect rolls it back and
  nested calls nest. The log store's transaction bodies are made only of
  synchronous SQL; a body that waits on asynchronous work is rolled back and
  dies, because `transactionSync` cannot span an `await`.
- **Crypto**: `SqliteCloudflare.cryptoLayer` provides Effect's `Crypto` for a
  Worker, since `@effect/platform-node` does not run in a Worker. SHA-256 is a
  synchronous pure-TypeScript implementation, because the log store hashes
  inside `transactionSync`, which cannot wait for Web Crypto's asynchronous
  `subtle.digest`. Random bytes use `crypto.getRandomValues`.
- The entry imports no Node module and no `cloudflare:workers`; the Durable
  Object storage types are structural, so the package adds no Cloudflare
  dependency.

### Verification status

The adapter is tested against a stand-in, not workerd. The stand-in
(`test/durable-object-stand-in.ts`) is built on `node:sqlite`, models the
documented `exec` cursor and `transactionSync`, and rejects `BEGIN`, `COMMIT`,
`ROLLBACK`, and `SAVEPOINT` statements and unsupported binding types, as a
Durable Object does. The full shared `LogStore` contract passes against it.
That proves the adapter does not depend on SQL-level transactions; it does not
prove behavior under workerd's input/output gates, storage limits, or
eviction. Run your own Worker tests (for example with
`@cloudflare/vitest-pool-workers`) before relying on it.
