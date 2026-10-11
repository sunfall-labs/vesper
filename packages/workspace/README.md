# `@sunfall/vesper-workspace`

A filesystem and shell behind one swappable Effect service, plus the coding
tools an agent needs to work in that filesystem. `WorkspaceAgent` is the explicit
adapter from those standalone tools to an agent toolkit; the agent package does
not install workspace access implicitly.

```bash
npm install @sunfall/vesper-workspace effect@4.0.3
```

Use the standard toolkit directly, or compose it with application tools:

```ts
import { Agent } from '@sunfall/vesper-agent/agent';
import { WorkspaceAgent } from '@sunfall/vesper-workspace/agent';
import { WorkspaceLocal } from '@sunfall/vesper-workspace/layer-local';
import { WorkspaceTools } from '@sunfall/vesper-workspace/tools';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import { Toolkit } from 'effect/ai';

const workspace = WorkspaceAgent.standard;
const interactiveWorkspace = WorkspaceTools.makeToolkit({
  apply_patch: true,
  edit_file: true,
  run_shell: true,
  write_file: true,
});
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
`WorkspaceTools.makeToolkit` plus `WorkspaceTools.makeLayer` when an
application needs the same handlers with approval gates; approval is encoded
on the Vesper tool definitions and does not require an adapter layer.

Modules are exposed as explicit subpaths, including
`@sunfall/vesper-workspace/agent`, `/driver`, `/layer-local`, `/shell-sandbox`, and `/tools`.

## Sandboxing shell commands

`WorkspaceLocal.layer` runs `run_shell` commands with the authority of the host
process. `WorkspaceLocal.sandboxed` wraps every command in an OS sandbox, so
the limits hold for code that does not cooperate with the tool-level path
checks:

```ts
WorkspaceLocal.sandboxed({
  mode: 'required', // 'none' | 'preferred' | 'required'
  root: '/work',
  network: false, // default
  readPaths: ['/Users/me/.local/share/mise'], // extra read-only toolchain dirs
}).pipe(Layer.provide(NodeServices.layer));
```

| Mode        | No working backend                                                                 |
| ----------- | ---------------------------------------------------------------------------------- |
| `none`      | Runs unconfined (today's behaviour, stated explicitly).                            |
| `preferred` | Runs unconfined; the result carries `sandbox: 'fallback'` and a warning is logged. |
| `required`  | Refuses: `run_shell` returns `SandboxRequired`; nothing runs.                      |

A confined result carries `sandbox: 'sandbox-exec'` or `'bwrap'`. Availability
is probed once when the layer is built, by running a trivial confined command,
so a host that has the binary but forbids its use (a nested sandbox, disabled
user namespaces) counts as unavailable.

**What is enforced.** Writes only under the workspace root, a private
per-layer temp directory (`TMPDIR`; a private tmpfs on Linux) and any
`writePaths`. Reads only from system directories (`/usr`, `/bin`, `/System`,
`/Library`, `/opt/homebrew`, `/etc`, ...), those same paths and `readPaths`; the
home directory, other users' files and the shared temp directory are not
readable unless the root is inside them. No network unless `network: true`.
The environment is still the layer's isolated one (`PATH` only), so host
secrets in environment variables are not passed in.

**Backends.** macOS uses `sandbox-exec` with a generated SBPL profile
(`deny default`, then a short allowlist). Apple has deprecated the tool, but
it is the only unprivileged option and works on current releases. Linux uses
`bwrap` (bubblewrap): fresh mount, PID, IPC and UTS namespaces, `--unshare-net`
unless networking is allowed, `--die-with-parent`, `--new-session`, system
directories read-only. Other platforms have no backend, so `required` always
refuses there. Paths are never spliced into profile text: SBPL receives them as
`-D` parameters and bwrap as separate argv elements, so a workspace named
`x") (allow file-write* ...` cannot alter the rules.

**What it does not protect against.**

- It confines `run_shell` only. The file tools run in the host process and rely
  on the lexical root checks and `FilesystemPolicy`.
- Anything inside the allowed paths is fully exposed: a command can read and
  destroy the whole workspace, including `.git`, and plant files (hooks, build
  scripts) that run unconfined later.
- File existence and metadata outside the allowed reads is visible on macOS.
- No resource limits: CPU, memory, disk and process counts are bounded only by
  the command timeout.
- Not a defence against kernel or sandbox vulnerabilities, nor a substitute for
  a container or VM when the code is truly hostile. Linux does not mount
  `/home` at all, but `/etc` and `/opt` are readable.
- With `network: true` the whole network is reachable, including loopback
  services and cloud metadata endpoints.
- Under `preferred`, an unavailable backend silently (to the model) degrades
  to unconfined; check `sandbox` in results or use `required`.
- Programs installed outside the system directories (version managers under
  `$HOME`) are invisible until their directory is listed in `readPaths`.

The default stays `none`: the agent layer in `@sunfall/vesper-workspace/agent`
does not choose a driver, so the application opts in where it builds the driver
layer, and switching existing toolchains to confinement would break any command
that needs `$HOME` or the network. New deployments should start with
`required`.

The repository keeps a shared conformance suite beside the driver interface so
every built-in driver is held to the same behaviour. It is test
infrastructure, not part of the published package interface.

See the [workspace guide](https://github.com/sunfall-labs/vesper/blob/main/docs/workspace.md)
for usage and security boundaries.
