# `@sunfall/vesper-log`

The dependency-light core of Vesper's append-only conversation log: durable
record schemas, per-record offsets, producer fencing, tailing, and an in-memory
adapter. The package depends only on Effect; PostgreSQL support is opt-in via
[`@sunfall/vesper-log-pg`](../log-pg).

```bash
npm install @sunfall/vesper-log effect@4.0.0-rc.112
```

Modules are explicit subpaths: `/log-store`, `/record`, `/record-batch`,
`/offset`, `/tail`, `/layer-memory`, `/adapter`, and `/testing`.

`LogStore.acquire(path, producerId, expected?)` fences the prior producer.
Supplying the observed epoch and head makes validation and acquisition atomic;
a changed stream fails with `conflict` without fencing its current producer.

`LogStore.readBackwards` pages newest-first from an optional exclusive bound.
`Tail.from` follows forward from an exclusive offset while preserving failures
from the adapter's change stream.

`RecordBatch.prepare` performs canonical JSON preparation and computes its
SHA-256 fingerprint through Effect's `Crypto` service. In Node applications,
provide `NodeServices.layer` or `NodeCrypto.layer` when constructing an
adapter.

The `/adapter` module centralizes the wire validation, retry identity, producer
fencing, and sequence decisions that storage implementations must share. It is
the narrow seam for custom adapters; persistence and critical-section mechanics
remain owned by each implementation.

The bundled memory adapter is a reference implementation and test double. It
does not survive a restart. Install `@sunfall/vesper-log-pg` when durable,
cross-process storage is required.

## Certifying an adapter

`@sunfall/vesper-log/testing` publishes the same contract suite that
`@sunfall/vesper-log-sqlite` and `@sunfall/vesper-log-pg` run against
themselves, so a custom `LogStore.Service` implementation can be held to the
same behavior without vendoring this package's test tree:

```ts
import { describe } from '@effect/vitest';
import { LogStoreConformance } from '@sunfall/vesper-log/testing';

LogStoreConformance.register('my-backend', myBackendLayer);
```

`register(name, layer, options?)` builds an `@effect/vitest` suite named
`` `LogStore contract: ${name}` `` and provides `layer` fresh for every case:
creation and epoch bumps, compare-and-acquire with its `conflict` outcome,
append atomicity, idempotent retry versus a fingerprint conflict, sequence
gaps, fencing on a stale epoch, per-record offsets with exclusive-bound
paging (forwards and backwards, including mid-batch resume), `meta`, and
in-process `changes` notification. Pass `{ concurrent: false }` when the
layer cannot stand up two independent connections to the same underlying
storage — a single-file SQLite connection, for instance; every case shipped
today runs either way, and the flag exists so a future case that genuinely
needs two connections has somewhere to opt adapters out.

A backend that can fake a `changes` subscription failing on demand — the
bundled memory adapter is the only one that can today — additionally calls
`LogStoreConformance.registerDeadChangeFeed(name, layerWithFailingChanges)`
to certify that a dead feed reaches a tail as a failure rather than a silent
stall.

`@effect/vitest` is an optional peer dependency scoped to this subpath:
installing `@sunfall/vesper-log` for its runtime modules does not require a
test framework, but a project that imports `/testing` needs `@effect/vitest`
in its own dependencies.
