# `@sunfall/vesper-workspace`

A filesystem and a shell behind one service, so a local process, a container,
or a remote worker are interchangeable by layer — and, on top of that service,
the six tools an agent needs before it can do anything at all.

```ts
import { WorkspaceDriver } from '@sunfall/vesper-workspace/driver';
import { WorkspaceLocal } from '@sunfall/vesper-workspace/layer-local';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';

const program = Effect.gen(function* () {
  const workspace = yield* WorkspaceDriver.Service;
  const { stdout } = yield* workspace.exec('git status --short', {
    cwd: '/work',
    timeoutMs: 30_000,
  });
  yield* workspace.writeFile('/work/status.txt', stdout);
});

program.pipe(
  Effect.provide(WorkspaceLocal.layer.pipe(Layer.provide(NodeServices.layer))),
);
```

The subpaths are `./agent`, `./driver`, `./layer-local`, `./tools`, `./output`,
`./path`, and `./glob`. Each module also re-exports
itself as a namespace (`WorkspaceAgent`, `WorkspaceDriver`, `WorkspaceLocal`,
`WorkspaceTools`, and so on), which is the form used throughout this file.

## The agent adapter

`WorkspaceAgent` is the explicit adapter between the standalone workspace
package and an agent definition. Nothing in `@sunfall/vesper-agent` installs
workspace access implicitly. Use `standard` for only the six workspace tools,
or `compose` to preserve an application's tools while adding them:

```ts
import { Agent } from '@sunfall/vesper-agent/agent';
import { WorkspaceAgent } from '@sunfall/vesper-workspace/agent';
import { WorkspaceLocal } from '@sunfall/vesper-workspace/layer-local';
import { WorkspaceTools } from '@sunfall/vesper-workspace/tools';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import { Toolkit } from 'effect/unstable/ai';

const workspace = WorkspaceAgent.compose(Toolkit.make(lookupIssue));
// Use WorkspaceAgent.standard instead when there are no application tools.

const agent = Agent.make({
  name: 'worker',
  revision: '1',
  instructions: 'Work in the provided workspace.',
  toolkit: workspace.toolkit,
}).withHandlers({ lookup_issue: lookupIssueHandler });

agent
  .run('inspect issue 42')
  .pipe(
    Effect.provide(workspace.layer),
    Effect.provide(WorkspaceTools.rootLayer('/work')),
    Effect.provide(
      WorkspaceLocal.layer.pipe(Layer.provide(NodeServices.layer)),
    ),
  );
```

The layers remain visible by design: `layer` supplies the standard tool
handlers, a shell-disabled command policy, and symlink-denying filesystem
policy; `rootLayer` selects the workspace root, and the driver layer selects
the execution substrate. Application handlers remain application-owned.
Tool-name collisions are rejected rather than silently overridden. To enable
the host shell, explicitly provide `WorkspaceTools.shellEnabledCommandPolicyLayer`.

## The toolkit

`./tools` is a `Toolkit` of six tools — `read_file`, `write_file`,
`edit_file`, `list_files`, `search_files`, `run_shell`.

`WorkspaceTools` is the advanced lower-level interface for callers that need
direct access to the toolkit, handler layer, root layer, or command policy. For
agent definitions, prefer `WorkspaceAgent.standard` or `WorkspaceAgent.compose`
so that adaptation is explicit without rebuilding this wiring.

They are built on `WorkspaceDriver`, not beside it. Every byte read and every
command run goes through the service, so the layer that decides _where_ the
workspace is decides it for the tools too. No `node:fs` import appears in
`tools.ts`, and that absence is the point.

```ts
run.pipe(
  Effect.provide(WorkspaceTools.layer),
  Effect.provide(WorkspaceTools.rootLayer('/work')),
  Effect.provide(WorkspaceLocal.layer.pipe(Layer.provide(NodeServices.layer))),
);
```

### The requirement is the product

Each tool declares `Root`, `WorkspaceDriver.Service`, and `FilesystemPolicy`
(with `CommandPolicy` additionally required by `run_shell`). That puts those
service keys in `Tool.HandlerServices` and from there into the requirement
channel of any agent holding the toolkit. An application that forgets to wire a
workspace or policy does not get a tool that quietly reads the host filesystem
— it does not compile.

`WorkspaceTools.layer` supplies the handlers and nothing else: `Root`,
`WorkspaceDriver.Service`, and the policy services stay the application's to
provide. `tools.test.ts`
pins both halves as type-level assertions that fail at `tsc` — one pair per
tool (dropping `dependencies` from a single tool would leave a union-shaped
assertion still passing), plus three on the layer's output confirming it
discharges the handlers and neither service.

The alternative — a `makeTools(driver)` factory closing over a driver — is one
line shorter and gives all of that away. The requirement disappears from the
type, and whether the agent is pointed at a container or at the developer's
home directory becomes a runtime fact nobody can see.

### Failures are tags the model can act on

Every tool sets `failureMode: 'return'`, so a failure is encoded into the tool
result and handed back to the model rather than thrown. A mistyped filename
ends a turn, not a run.

Each failure is a `Schema.TaggedError` with the fields needed to retry
differently: `PathOutsideWorkspace`, `FileNotFound`, `NotAFile`,
`NotADirectory`, `BinaryContent`, `EditTargetMissing`, `EditTargetAmbiguous`,
`AccessDenied`, `SymlinkDenied`, `InvalidPattern`, `CommandTimedOut`,
`ShellDisabled`, `WorkspaceUnavailable`. A single `ToolFailed { message }`
would compile and would put the whole diagnosis back into prose.

Two are worth calling out. `EditTargetMissing` is the one that matters most:
an edit tool that writes the file back unchanged and reports success teaches
the model its change landed, and everything the model concludes afterwards is
built on that. `EditTargetAmbiguous` refuses rather than replacing the first
match, because "the first one" is not what was asked for and the intended edit
may be the third.

### What the tools will not do quietly

- **Return a prefix as though it were the whole.** `read_file` reports
  `truncated`, `truncatedBy`, `totalLines`, and `lineCount`; slicing to an
  `offset`/`limit` window counts as truncation even when the window itself fit.
- **Report zero matches for files it never opened.** `search_files` returns
  `filesSearched`, `binaryFilesSkipped`, and `largeFilesSkipped` alongside the
  matches. Files over 2 MB are skipped, as are files that fail the text check.
- **Follow a symlink by default.** The walk lists links as links, and direct
  access to a path containing one returns `SymlinkDenied`. A link to `..` is an
  infinite tree, and a link out of the root defeats lexical containment. Use
  `unrestrictedFilesystemPolicyLayer` only when link-following is intentional.
- **Lose a tree to one unreadable directory.** An `EACCES` deep in the walk is
  recorded in `unreadableDirectories` and the walk continues. The _root_
  directory failing does fail the call — that is the model's mistake, not an
  obstacle in the tree.
- **Walk forever.** 20,000 entries caps a single walk and sets `truncated`.
  `.git` and `node_modules` are not descended and are named in
  `ignoredDirectories`.

`edit_file` uses `split`/`join` rather than `String.replace`, so `$&` and
friends in the replacement text are literal — a model editing a regular
expression or a shell script gets back what it wrote.

`run_shell` is disabled by the default policy and returns `ShellDisabled`; it
must be explicitly enabled with `shellEnabledCommandPolicyLayer`. Once enabled,
it defaults to a 120-second deadline and truncates output from the front so the
end of a long build log survives.

## A driver swap is a composition boundary, not a security boundary

This is worth being blunt about, because "sandbox" — the obvious name for a
package like this, and the one it nearly had — actively implies otherwise.

Swapping the layer decides **where** a tool's file reads and commands land. It
does not confine anything. `exec` on the local driver runs with the full
authority of the host process, and nothing inspects a command before running
it. The seam constrains code that _cooperates_ — a tool written against this
service goes wherever the layer points, and a tool that reaches for `node:fs`
directly is untouched by which driver is wired.

**The toolkit's path containment is lexical plus a symlink check by default.**
Paths are resolved and checked against `Root` before they reach the driver, and
existing symlink components are denied. This stops a model that wandered — not
code that meant to leave. `unrestrictedFilesystemPolicyLayer` restores explicit
link-following behavior, and an enabled `run_shell` still executes a command
string nothing here inspects.

If the requirement is containment of hostile code, that has to come from the
driver's substrate: a container, a VM, a jailed remote worker. What this seam
buys is that moving to one is a layer change rather than a rewrite.

`WorkspacePath` is POSIX-only and deliberately not `node:path`. `node:path` on
Windows treats `\` as a separator and `C:` as a root, so a driver talking to a
Linux container would get its paths reinterpreted by whatever OS the _host_
happens to run.

## Text or binary

`WorkspaceOutput.decodeText` decides, and both checks are needed:

- **A NUL byte** in the leading `BINARY_SNIFF_BYTES` (8000). Executables,
  images, and archives have them; text does not.
- **A strict UTF-8 decode.** A latin-1 or UTF-16 file may carry no NUL at all,
  and a lenient decode hands back a string of `U+FFFD` that the model cannot
  tell from a file of unusual glyphs. Failing is the honest answer, and it is
  why this is not `Buffer.toString('utf8')`.

Sniffing a prefix is the same rule `git` uses and has the same blind spot: a
file that is clean text for its first 8000 bytes and binary afterwards reads as
text.

Truncation is two independent budgets — 2000 lines and 50 KB by default,
whichever runs out first — reported as `truncatedBy`. `head` keeps the
beginning and returns only whole lines; if even the first line is over budget
the content is empty, an honest nothing rather than a fragment the model would
read as a line. `tail` keeps the end, for command output where the error is the
last thing printed, and will return a fragment only when a single line exceeds
the byte budget, flagging it with `partialLine`.

## Globs

`WorkspaceGlob` compiles patterns to regular expressions: `*`, `**`, `**/`,
`?`, and `[abc]` character classes with `[a-z]` ranges and `[!…]` negation.

**Brace alternation (`{ts,tsx}`) is not supported** and `{` is a literal brace.
Two patterns cost one extra call; a half-working `{}` costs a wrong answer
nobody checks. Matching is separated from walking because a glob running
against a _driver_ cannot be one that reads the filesystem itself, which every
published glob library does.

## Driver errors

Four typed failures, not one:

| Error              | Means                                                    |
| ------------------ | -------------------------------------------------------- |
| `PathNotFound`     | the path is not there (`ENOENT`)                         |
| `PermissionDenied` | it is there and the driver may not touch it that way     |
| `CommandTimeout`   | `timeoutMs` expired and the command was terminated       |
| `WorkspaceFailure` | anything else, carrying the driver's own code and defect |

## Exit codes are data

`exec` succeeds for any command that ran to completion, whatever it exited
with. `grep` exits 1 for no match, `git diff --quiet` exits 1 for "there are
changes", `test -f` exits 1 for false, `diff` exits 1 for "files differ" — all
ordinary outcomes an agent needs to _read_. Modelling them as errors would put
`Effect.catchTag` in the middle of ordinary control flow.

Failure is reserved for "could not run it at all": a bad `cwd`, a host that
refused to fork, a deadline that killed it mid-flight. A command that started
and then failed is not that — the shell reports 127 for not-found and 126 for
not-executable, and both arrive as exit codes.

A caller for whom non-zero really is fatal says so at the point where it
matters, which is one line and reads better than a second method would:

```ts
workspace.exec('npm test').pipe(
  Effect.filterOrFail(
    (result) => result.exitCode === 0,
    (result) => new TestsFailed({ stderr: result.stderr }),
  ),
);
```

## Local environment policy

`WorkspaceLocal.layer` supplies only a small executable `PATH` to commands;
host variables and secrets are not inherited. `WorkspaceLocal.unrestricted` is
the explicit opt-in for legacy host-local behavior where `ExecOptions.env` is
merged over the ambient environment. Neither layer is a sandbox: enabled shell
commands still have the local process's filesystem authority. Use a container,
VM, or jailed remote driver when hostile code is in scope.

## Cancellation

Two mechanisms that must agree, and the contract suite checks both:

- **Interruption.** Interrupting the fiber kills the command. On the local
  driver this is `Effect.scoped` over `ChildProcessSpawner.spawn`, which
  acquires the process with `Effect.acquireRelease` and kills the whole
  process group on release — including the grandchildren a shell pipeline
  started, which a bare `child.kill()` would orphan.
- **`timeoutMs`.** A wall-clock deadline. `Effect.timeoutOrElse` interrupts
  the source before running its fallback, so the command is dead before
  `CommandTimeout` is constructed.

_Orphan settlement_ — the eventual result of a command whose caller was
released early — is the concept a remote driver will need, because a driver
that reaches its substrate over a wire cannot always cancel a call already in
flight. Locally the kill is synchronous with the release, so there is no
orphan to settle. It is not something to design in advance of the driver that
needs it.

## The contract

The internal workspace contract expresses the behaviour every built-in driver
must have once. It lives beside `WorkspaceDriver` rather than in a testkit
package, avoiding a dependency cycle without adding test machinery to the
published interface. See [`contributing.md`](contributing.md).

```ts
workspaceContract('local', { layer, root });
```

It runs against a real substrate: files are written, commands are run, and the
cancellation cases are checked by leaving a marker the command would have
written had it survived. A driver can pass everything else while leaking a
process on every timeout, and nothing about its interface would say so.
