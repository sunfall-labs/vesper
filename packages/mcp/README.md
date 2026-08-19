# `@sunfall/vesper-mcp`

Consume MCP servers as scoped, runtime-discovered Vesper tools. Every remote
tool is exposed to the model as `mcp__<server>__<tool>`, with unsupported name
characters replaced by `_`, so tools from different servers remain distinct.

```bash
npm install @sunfall/vesper-mcp @sunfall/vesper-agent effect@4.0.0-rc.109
```

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
