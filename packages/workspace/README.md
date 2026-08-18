# `@sunfall/vesper-workspace`

A filesystem and shell behind one swappable Effect service, plus the six tools
an agent needs to work in that filesystem. `WorkspaceAgent` is the explicit
adapter from those standalone tools to an agent toolkit; the agent package does
not install workspace access implicitly.

```bash
npm install @sunfall/vesper-workspace effect@4.0.0-rc.109
```

Use the standard toolkit directly, or compose it with application tools:

```ts
import { Agent } from '@sunfall/vesper-agent/agent';
import { WorkspaceAgent } from '@sunfall/vesper-workspace/agent';
import { WorkspaceLocal } from '@sunfall/vesper-workspace/layer-local';
import { WorkspaceTools } from '@sunfall/vesper-workspace/tools';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import { Toolkit } from 'effect/unstable/ai';

const workspace = WorkspaceAgent.standard;
const workspaceWithApplicationTools = WorkspaceAgent.compose(
  Toolkit.make(lookupIssue),
);

const agent = Agent.make({
  // ...
  toolkit: workspaceWithApplicationTools.toolkit,
}).withHandlers(applicationHandlers);

agent
  .run(input)
  .pipe(
    Effect.provide(workspaceWithApplicationTools.layer),
    Effect.provide(WorkspaceTools.rootLayer('/work')),
    Effect.provide(
      WorkspaceLocal.layer.pipe(Layer.provide(NodeServices.layer)),
    ),
  );
```

The layers are explicit: `layer` installs the standard tool handlers and
default shell policy, `rootLayer` selects the visible workspace root, and the
driver layer selects where filesystem and shell operations run. Use
`WorkspaceTools` directly only for advanced lower-level toolkit, handler, root,
or command-policy wiring.

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-workspace/agent`, `/driver`, `/layer-local`, and `/tools`.

The repository keeps a shared conformance suite beside the driver interface so
every built-in driver is held to the same behaviour. It is test
infrastructure, not part of the published package interface.

See the [workspace guide](https://github.com/sunfall-labs/vesper/blob/main/docs/workspace.md)
for usage and security boundaries.
