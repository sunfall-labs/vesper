# `@sunfall/vesper-mcp`

Consume MCP servers as scoped, runtime-discovered Vesper tools. Every remote
tool is exposed to the model as `mcp__<server>__<tool>`, with unsupported name
characters replaced by `_`, so tools from different servers remain distinct.

```bash
npm install @sunfall/vesper-mcp @sunfall/vesper-agent effect@4.0.0-rc.109
```

Node.js 22 or newer is required. `getLinearToken()` below is an
application-owned resolver; replace it with your secret-management code and do
not put a raw token in source.

```ts
import { Agent } from '@sunfall/vesper-agent/agent';
import { Mcp } from '@sunfall/vesper-mcp/mcp';
import { Redacted } from 'effect';
import { Toolkit } from 'effect/unstable/ai';

const linear = Mcp.remote({
  name: 'linear',
  url: 'https://mcp.linear.app/mcp',
  auth: () => Redacted.make(getLinearToken()),
  tools: ['search_issues', 'create_issue'],
  optional: true,
});

const agent = Agent.make({
  name: 'support',
  revision: '1',
  instructions: 'Use Linear when it helps answer the request.',
  toolkit: Toolkit.make(),
  dynamicTools: [linear],
});
```

`auth` accepts an Effect `Redacted<string>` or a resolver invoked for each HTTP
request. The raw token is unwrapped only while constructing the authorization
header, reducing accidental exposure through logs and inspection. `headers`,
`requestInit`, a custom `fetch`, and legacy
`transport: 'sse'` are also available. For OAuth or a custom transport, use the
lower-level `Mcp.make({ transport: Mcp.streamableHttp(...) })`.

MCP metadata, schemas, and results are untrusted input. Discovery accepts at
most 128 tools by default, descriptions up to 8 KiB, schemas and arguments up
to 64 KiB, and one model-facing result up to 1 MiB; calls and discovery use a
30-second timeout. Tighten these defaults per source with `limits` and
`timeout` (each has a validated upper bound). These are acceptance and
propagation bounds, not transport-memory limits: the MCP SDK materializes a
response before Vesper can apply `maxResultBytes`. Treat remote descriptions
and schema text as data, and enforce authorization in typed handlers or
`beforeToolCall`.

Every submission discovers a fresh tool snapshot. That snapshot is immutable
across all model turns in the run, and all configured dynamic sources open in
parallel. The current server and tool availability is placed in system context:
an unchanged snapshot stays stable for prompt caching, while a changed or
unavailable server explicitly supersedes earlier availability. `optional: true`
continues with no tools if connection or discovery fails; without it, the run
fails before the first model request.

The optional `tools` allowlist is fail-closed and preserves its declared order.
Without one, callable tools and their JSON schemas are canonicalized so
semantically identical discovery results do not perturb the provider tool
prefix. Unknown, repeated, or MCP task-required allowlist entries fail discovery
before the first model request. Without an allowlist, task-required tools are
omitted because Vesper does not yet implement the MCP task protocol.

MCP schemas use Effect's native `Tool.dynamic`, and remote calls inherit Effect
interruption through an `AbortSignal`. MCP failures are returned through the
ordinary tool-result channel so the model can react. Apply policy spanning the
remote toolkit with `beforeToolCall`. When a destructive operation needs a
durable, independently keyed human decision, expose it through an
application-owned typed tool handler and call `AgentWorkflow.wait` there; the
generated MCP handler deliberately does not introduce a second approval flow.

## Tool drift detection

MCP metadata is trusted the first time a server advertises it — discovery has
no prior definition to compare against. An application that wants to know
when a server later changes a tool's description or schema out from under it
(a "rug pull") can pin the fingerprints it currently trusts and ask Vesper to
check future discoveries against them:

```ts
import { Mcp } from '@sunfall/vesper-mcp/mcp';
import { Effect, Redacted } from 'effect';

// Run once, out of band (a setup script, an admin command) to obtain the
// current fingerprints, keyed by the server's own tool names.
const pins = await Effect.runPromise(
  Mcp.fingerprints({
    name: 'linear',
    url: 'https://mcp.linear.app/mcp',
    auth: () => Redacted.make(getLinearToken()),
  }).pipe(Effect.scoped),
);
// { "search_issues": "3f1c...ab", "create_issue": "9e02...4d" }

const linear = Mcp.remote({
  name: 'linear',
  url: 'https://mcp.linear.app/mcp',
  auth: () => Redacted.make(getLinearToken()),
  toolDrift: { fingerprints: pins },
});
```

A fingerprint is the SHA-256 of the tool's remote name, its rendered
description, and its canonicalized input schema — the same canonicalization
discovery already uses for prompt-cache stability, so key order never affects
it. Only pinned tools are checked: a tool absent from `fingerprints` is
trusted on first discovery, exactly as it is today, so adding `toolDrift`
does not affect servers or tools you have not pinned. Omitting `toolDrift`
entirely leaves discovery unchanged.

When a pinned tool's current fingerprint no longer matches, `onDrift`
decides what happens: `'reject'`, the default, excludes the drifted tool from
the toolkit and logs a `Mcp.ToolDriftError`; `'warn'` logs the same error but
keeps the tool available. Either way the decision is per tool — one server
changing one tool does not take the rest of its catalog down.

**What this does not cover.** Trust-on-first-discovery is inherent: nothing
can detect drift in a tool Vesper has never fingerprinted before. Vesper also
does not persist pins across runs or processes — `Mcp.fingerprints` only
returns a value; storing it, and loading it back into `toolDrift.fingerprints`
on the next run, is the application's job, since only it knows the right
place (a config file, a secrets store, a database row next to the tenant that
uses this server) and the right process (who reviews and approves a genuine
definition change). A minimal file-backed example:

```ts
import { readFile, writeFile } from 'node:fs/promises';

const pinsPath = './linear-tool-pins.json';

const loadPins = async (): Promise<Record<string, string>> => {
  try {
    return JSON.parse(await readFile(pinsPath, 'utf8'));
  } catch {
    return {}; // first run: nothing pinned yet, nothing to drift-check
  }
};

const savePins = (pins: Record<string, string>) =>
  writeFile(pinsPath, JSON.stringify(pins, null, 2));

// After a reviewed, intentional server change, re-run `Mcp.fingerprints` and
// `savePins` the result to accept the new definitions as the trusted baseline.
```

## Reusing connections

Fresh scoped connections are the safe default. To reuse an initialized client
while still refreshing its tools for each run, use `Mcp.cached` and provide one
cache layer around the lifetime that should share connections:

```ts
import { Effect } from 'effect';

const linear = Mcp.cached({
  name: 'linear',
  url: 'https://mcp.linear.app/mcp',
  auth: () => Redacted.make(getLinearToken()),
  optional: true,
});

const program = Effect.gen(function* () {
  yield* agent.run('Find the issue.');
  yield* agent.run('Check it again.');
}).pipe(Effect.provide(Mcp.layerConnectionCache()));
```

The cache is Effect's reference-counted `RcMap`: overlapping runs share the
same client, idle clients close after five minutes by default, and every client
closes when the layer scope closes. Configure the idle lifetime with
`Mcp.layerConnectionCache({ idleTimeToLive: '30 seconds' })`.

For local servers, use `Mcp.make({ transport: Mcp.stdio(...) })`; legacy remote
servers can use `Mcp.sse(...)`. Applications that already own an MCP client can
adapt its scoped lifetime with `Mcp.fromClient(...)`.
