# `@sunfall/vesper-log-pg`

The opt-in PostgreSQL adapter for `@sunfall/vesper-log`. It provides durable,
atomic appends, cross-process wake-ups, producer fencing, and the authoritative
database migration without adding PostgreSQL dependencies to the core package.

```bash
npm install @sunfall/vesper-log-pg \
  @effect/platform-node@4.0.0-rc.109 effect@4.0.0-rc.109
```

The migration is published as
`@sunfall/vesper-log-pg/migrations/001-initial.sql`. Copy its statements into
your migration system; the adapter never migrates at runtime.

```ts
import * as NodeServices from '@effect/platform-node/NodeServices';
import { VesperPgClient } from '@sunfall/vesper-log-pg/client';
import { LogStorePg } from '@sunfall/vesper-log-pg/layer';
import { Layer } from 'effect';

const pg = VesperPgClient.layer({ url });
const store = LogStorePg.layer({
  transactionStatementTimeoutMs: 30_000,
}).pipe(Layer.provide(pg), Layer.provide(NodeServices.layer));
```

`LogStorePg.make` accepts a concrete compatible client and returns the core
`LogStore` interface. `LogStorePg.layer` consumes the official
`@effect/sql-pg` client from the environment. Both require Effect's `Crypto`
service while the adapter is constructed.

`VesperPgClient` delegates queries and transactions to the official client and
corrects only the Effect 4.0.0-rc.109 `LISTEN` connection lifecycle. A standard
client is also accepted when its listener satisfies the same lifecycle
contract.
