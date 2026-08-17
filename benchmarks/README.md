# Benchmarks

`nub run benchmark` is the default Vesper in-memory recording-cost suite. The external
Flue comparison is opt-in:

```bash
nub run --filter @sunfall/vesper-benchmarks bench:compare:flue
```

Set `VESPER_BENCH_SMOKE=1` for a one-sample wiring check. The normal comparison
uses 30 warmed submission samples, seven growing conversations, ten concurrent
batches, and seven fresh processes for startup and retained memory.

Both sides use scripted zero-network providers and assert the exact model-call
count. A side/workload pair always has its own process. Timed conversations use
fresh runtime and storage state on both sides. Cold startup is measured
from parent-side process creation through the first completed, recorded reply;
shutdown is excluded. Memory is the retained `heapUsed` and RSS delta after 60
one-call submissions to one conversation and forced GC.

The report deliberately keeps three kinds of evidence separate:

- Harness timings: one turn, the 8-call/7-tool loop, conversation growth, and
  batches of independent concurrent conversations. Timing boundaries match,
  but provider protocols and adapters do not, so these are not direct claims
  about framework speed.
- Vesper-only structure: pages and records read when opening uncompacted,
  compacted-fixed-tail, and compacted-plus-orphan histories. There is no Flue
  timing row for this.
- Operational conformance: executable probes for producer fencing,
  indeterminate tools, revision compatibility, each framework's distinct
  cancellation mechanism, prompt file-byte recording, and attachment byte
  storage where the public runtime makes a deterministic probe possible.
  `not equivalent` and `not exercised` are reported rather than inferred as
  pass or fail.

The volatile storage engines are intentionally native to each side: Vesper uses
`LogStoreMemory`; Flue 2.0.3 uses its process-local in-memory SQLite runtime.
Flue's benchmark provider directly implements the Provider interface with a
fixed event and chunk count; it does no prompt serialization, token estimation,
cache simulation, or random splitting. Provider protocols, adapters, prompt
projection, and record shapes still differ, so the results describe these
concrete harnesses rather than universal framework performance. Flue timings
include its Provider-to-Pi-event adapter path; no adapter-subtracted direct
framework-speed result is claimed.
