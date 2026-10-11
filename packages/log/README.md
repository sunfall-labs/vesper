# `@sunfall/vesper-log`

The dependency-light core of Vesper's append-only conversation log: durable
record schemas, per-record offsets, producer fencing, tailing, and an in-memory
adapter. The package depends only on Effect; PostgreSQL support is opt-in via
[`@sunfall/vesper-log-pg`](../log-pg).

```bash
npm install @sunfall/vesper-log effect@4.0.3
```

Modules are explicit subpaths: `/log-store`, `/record`, `/record-batch`,
`/offset`, `/tail`, `/layer-memory`, `/adapter`, and `/chain`.

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

## Tamper evidence

Every appended batch is stored with a chained SHA-256 digest that commits to
the digest of the batch before it, so a stream's newest digest (`chainHead` on
`LogStore.meta`) commits to its whole history. Pin that value somewhere the log's
operator cannot rewrite; `LogChain.verify` then recomputes the chain from the
stored batches and reports either `Valid` or the first offset that cannot be
trusted:

```ts
import { LogChain } from '@sunfall/vesper-log/chain';

const result = yield * LogChain.verify(path, { expectedHead: pinned });
// { _tag: 'Valid', head, batches, records }
// | Broken { offset, reason, detail }
```

`reason` is `content_modified` (records or producer coordinates edited, or two
batches swapped), `offset_gap` (a batch or record was removed),
`offset_overlap` (a batch was inserted, repeated, or reordered),
`head_mismatch`, or `malformed` (including a batch with no digest). `LogChain.verifyBatches`
is the same check over batches you already hold, for offline verification of
an export; `LogStore.readBatches` returns them from a store.

Each digest is SHA-256 over a JSON array: a version tag, the previous digest
(a genesis digest committing to the stream path and identity for the first
batch), the batch's first offset, producer id, epoch, and sequence, and the
batch's canonical JSON bytes. The exact definition is in `src/chain.ts`.

What this does and does not give you:

- It is computed under the same lock that orders appends, so it is linear and
  needs no extra coordination. Idempotent retries do not move it.
- Without a pinned head, a party that can rewrite the whole store can
  recompute every digest, and dropping the newest batches leaves a valid
  shorter chain. Pinning `chainHead` (or passing `expectedHead`) is what makes
  the record verifiable by someone who does not trust the runtime or database.
- Storage bookkeeping timestamps and the stream's current epoch and holder are
  not covered; they govern future appends, not history.

Custom `LogStore` adapters must implement `readBatches`, expose `chainHead`,
and compute each batch's digest with `LogChain.forAppend` from the head stored
beside the fencing state.

The bundled memory adapter is a reference implementation and test double. It
does not survive a restart. Install `@sunfall/vesper-log-pg` when durable,
cross-process storage is required.
