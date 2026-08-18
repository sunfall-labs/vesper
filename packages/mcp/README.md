# `@sunfall/vesper-mcp`

Expose one Vesper agent as a durable `run_agent` tool on Effect's native MCP
server. Vesper adapts only its own conversation semantics; Effect continues to
own MCP schemas, registration, protocols, stdio, and HTTP transports.

```bash
npm install @sunfall/vesper-mcp @sunfall/vesper-agent effect@4.0.0-rc.109
```

```ts
import { AgentMcp } from '@sunfall/vesper-mcp/agent';
import { Layer } from 'effect';
import { McpProtocol, McpServer } from 'effect/unstable/ai';

const adapter = AgentMcp.make(supportAgent);

const McpLive = adapter.layer.pipe(
  Layer.provide(
    McpServer.layerStdio({
      name: 'support-agent',
      version: '1.0.0',
      protocols: [McpProtocol.v2025_06_18],
    }),
  ),
);
```

`adapter.layer` registers exactly one tool and requires the agent's ordinary
`Agent.Requires`, `LogStore.Service`, `Crypto.Crypto`, and
`McpServer.McpServer` services. Provide `McpLive` with those ordinary Vesper
layers and the platform's stdio adapter. For HTTP, substitute Effect's
`McpServer.layerHttp`. The package does not wrap or re-export either transport,
protocol, or schema.

The tool's declared failure is `RunError`. It carries a closed
`classification`, stable `code`, `retryable` flag, human-readable `message`,
and string-only `details`. Provider, conversation, policy, and durability
failures are reduced to that safe envelope, so Effect's MCP server returns a
typed tool failure without serializing internal causes. Input and successful
output schemas remain Effect's native `Tool` schemas.

This first adapter intentionally exposes no conversation-history resource. A
history endpoint would need application-specific authorization, redaction, and
pagination semantics; `run_agent` keeps the public MCP surface focused on the
durable conversation operation. A resource can be added once those policies
are part of the Vesper contract.
