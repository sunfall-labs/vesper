# `@sunfall/vesper-log`

The dependency-light core of Vesper's append-only conversation log: durable
record schemas, per-record offsets, producer fencing, tailing, and an in-memory
adapter. The package depends only on Effect; PostgreSQL support is opt-in via
[`@sunfall/vesper-log-pg`](../log-pg).

```bash
npm install @sunfall/vesper-log effect@4.0.0-rc.109
```

Modules are explicit subpaths: `/log-store`, `/record`, `/record-batch`,
`/offset`, `/tail`, `/layer-memory`, and `/adapter`.

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
