import type { Stats } from 'node:fs';
import * as fs from 'node:fs/promises';

import { Config, Duration, Effect, Layer, Predicate, Stream } from 'effect';
import type { PlatformError } from 'effect/PlatformError';
import { ChildProcess, ChildProcessSpawner } from 'effect/process';

import { WorkspaceDriver } from './driver.js';
import { DEFAULT_MAX_BYTES } from './output.js';
import { ShellSandbox } from './shell-sandbox.js';

// The local driver: `node:fs/promises` for files, Effect's
// `ChildProcessSpawner` for commands.
//
// It is the reference implementation and the one the contract suite runs
// against, not a stub — a container or remote driver differs in where the
// bytes live, not in what the operations mean.
//
// The default layer does not inherit the host environment, but it still runs
// local commands with the authority of the host process. See the boundary
// note in `./driver.ts`: a driver layer is not a sandbox. `sandboxed` is the
// opt-in that wraps every command in an OS sandbox (`./shell-sandbox.ts`);
// file operations stay in-process and are confined by the tools' path checks,
// not by the sandbox.
//
// Interruption is the spawner's, not ours. `ChildProcessSpawner.spawn`
// acquires the process with `Effect.acquireRelease` and kills the whole
// process group on release, so `Effect.scoped` is the entire mechanism by
// which an interrupted or timed-out `exec` stops its command — including the
// grandchildren a shell pipeline started, which a bare `child.kill()` would
// orphan. A remote driver has to model *orphan settlement* — the eventual
// result of a command whose caller has already given up — because it cannot
// always cancel a call in flight. Locally the kill is synchronous with the
// release, and `Effect.timeoutOrElse` interrupts the source before running its
// fallback, so a timed-out command is dead before `CommandTimeout` is
// constructed. That concept comes back the moment the driver stops being
// local.

const errnoCode = (error: unknown): string | undefined => {
  if (!Predicate.hasProperty(error, 'code')) {
    return undefined;
  }
  return typeof error.code === 'string' ? error.code : undefined;
};

const classifyWorkspaceFailure = (
  operation: WorkspaceDriver.Operation,
  path: string,
  code: string | undefined,
  cause: unknown,
):
  | WorkspaceDriver.PathNotFound
  | WorkspaceDriver.PermissionDenied
  | WorkspaceDriver.WorkspaceFailure => {
  if (code === 'ENOENT' || code === 'NotFound') {
    return new WorkspaceDriver.PathNotFound({ path, operation });
  }
  if (code === 'EACCES' || code === 'EPERM' || code === 'PermissionDenied') {
    return new WorkspaceDriver.PermissionDenied({ path, operation });
  }
  return new WorkspaceDriver.WorkspaceFailure({
    operation,
    path,
    code: code ?? 'Unknown',
    cause,
  });
};

/**
 * Classify a `node:fs` rejection.
 *
 * Only the two cases a caller can act on are modelled — retarget the path, or
 * give up on permissions. Everything else keeps its `errno` on
 * {@link WorkspaceDriver.WorkspaceFailure} rather than being flattened into a
 * fourth guess at intent.
 */
const fsFailure =
  (operation: WorkspaceDriver.Operation, path: string) =>
  (error: unknown): WorkspaceDriver.FileError =>
    classifyWorkspaceFailure(operation, path, errnoCode(error), error);

class ReadLimitExceeded extends Error {
  readonly code = 'ERR_FILE_READ_LIMIT';
}

const boundedRead = async (
  path: string,
  maxBytes: number,
  signal: AbortSignal,
): Promise<Buffer> => {
  const limit = Math.max(0, Math.trunc(maxBytes));
  const handle = await fs.open(path, 'r');
  try {
    const bytes = Buffer.allocUnsafe(limit + 1);
    let offset = 0;
    while (offset < bytes.length) {
      signal.throwIfAborted();
      const read = await handle.read(
        bytes,
        offset,
        bytes.length - offset,
        null,
      );
      if (read.bytesRead === 0) {
        break;
      }
      offset += read.bytesRead;
    }
    if (offset > limit) {
      throw new ReadLimitExceeded();
    }
    return bytes.subarray(0, offset);
  } finally {
    await handle.close();
  }
};

const readFailure =
  (
    operation: 'readFile' | 'readFileBuffer',
    path: string,
    maxBytes: number | undefined,
  ) =>
  (error: unknown): WorkspaceDriver.FileError =>
    error instanceof ReadLimitExceeded && maxBytes !== undefined
      ? new WorkspaceDriver.FileReadLimitExceeded({
          path,
          operation,
          maxBytes,
        })
      : fsFailure(operation, path)(error);

/** Fixed-size byte ring that keeps draining while retaining only the tail. */
class ByteTail {
  readonly #bytes: Uint8Array;
  #length = 0;
  #start = 0;
  truncated = false;

  constructor(readonly capacity: number) {
    this.#bytes = new Uint8Array(capacity);
  }

  append(chunk: Uint8Array): void {
    if (chunk.length === 0) {
      return;
    }
    if (chunk.length >= this.capacity) {
      const discarded = this.#length > 0 || chunk.length > this.capacity;
      this.#bytes.set(chunk.subarray(chunk.length - this.capacity));
      this.#start = 0;
      this.#length = this.capacity;
      this.truncated = this.truncated || discarded;
      return;
    }

    const overflow = Math.max(0, this.#length + chunk.length - this.capacity);
    if (overflow > 0) {
      this.#start = (this.#start + overflow) % this.capacity;
      this.#length -= overflow;
      this.truncated = true;
    }
    const end = (this.#start + this.#length) % this.capacity;
    const first = Math.min(chunk.length, this.capacity - end);
    this.#bytes.set(chunk.subarray(0, first), end);
    this.#bytes.set(chunk.subarray(first), 0);
    this.#length += chunk.length;
  }

  text(): string {
    const output = new Uint8Array(this.#length);
    const first = Math.min(this.#length, this.capacity - this.#start);
    output.set(this.#bytes.subarray(this.#start, this.#start + first));
    output.set(this.#bytes.subarray(0, this.#length - first), first);
    return new TextDecoder().decode(output);
  }
}

/**
 * Classify a spawner failure — a command that never started.
 *
 * A command that *did* start and then failed is not this: the shell reports
 * that as an exit code (127 for not found, 126 for not executable), which
 * comes back on the {@link WorkspaceDriver.ShellResult} as data. This path is a
 * missing `cwd`, an unreadable one, or a host that refused to fork.
 */
const execFailure =
  (command: string, cwd: string | undefined) =>
  (error: PlatformError): WorkspaceDriver.ExecError => {
    const reason = error.reason;
    const path =
      ('pathOrDescriptor' in reason && reason.pathOrDescriptor !== undefined
        ? String(reason.pathOrDescriptor)
        : cwd) ?? command;

    return classifyWorkspaceFailure('exec', path, reason._tag, error);
  };

const SAFE_PATH = '/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin';

const makeLayer = (
  inheritEnvironment: boolean,
  sandbox?: ShellSandbox.Policy,
): Layer.Layer<
  WorkspaceDriver.Service,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Layer.effect(
    WorkspaceDriver.Service,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

      // Resolved once, at construction: the backend probe runs real
      // processes, and the plan needs the root's real path because both
      // sandboxes match on resolved paths (`/tmp` is `/private/tmp` on macOS).
      const confinement =
        sandbox === undefined || sandbox.mode === 'none'
          ? undefined
          : yield* Effect.gen(function* () {
              // A private temp directory rather than the shared `$TMPDIR`, so
              // a confined command cannot read or clobber other programs'
              // temp files. Removed with the layer.
              const tmp = yield* Config.String('TMPDIR').pipe(
                Config.withDefault('/tmp'),
                Effect.orElseSucceed(() => '/tmp'),
              );
              const tempDir = yield* Effect.acquireRelease(
                Effect.promise(() =>
                  fs
                    .mkdtemp(`${tmp}/vesper-sandbox-`)
                    .then((dir) => fs.realpath(dir)),
                ),
                (dir) =>
                  Effect.promise(() =>
                    fs.rm(dir, { recursive: true, force: true }),
                  ),
              );
              const root = yield* Effect.promise(() =>
                fs.realpath(sandbox.root).catch(() => sandbox.root),
              );
              const plan = ShellSandbox.makePlan(sandbox, { root, tempDir });
              // Probed with the real plan, so the probe cannot pass where a
              // real command's mounts and namespaces would fail.
              const backend = yield* ShellSandbox.detect(
                process.platform,
                spawner,
                plan,
              );
              if (backend === undefined && sandbox.mode === 'preferred') {
                yield* Effect.logWarning(
                  'No shell sandbox is available; commands will run unconfined',
                );
              }
              return { mode: sandbox.mode, backend, plan };
            });

      // `Effect.fn` with plain functions rather than generators wherever the
      // body has nothing to yield: an empty generator trips `require-yield`.
      const readFile = Effect.fn('WorkspaceDriver.readFile')(
        (path: string, options?: WorkspaceDriver.ReadFileOptions) =>
          Effect.tryPromise({
            try: async (signal): Promise<string> => {
              if (options?.maxBytes === undefined) {
                return fs.readFile(path, { encoding: 'utf8', signal });
              }
              return new TextDecoder().decode(
                await boundedRead(path, options.maxBytes, signal),
              );
            },
            catch: readFailure('readFile', path, options?.maxBytes),
          }),
      );

      const readFileBuffer = Effect.fn('WorkspaceDriver.readFileBuffer')(
        (path: string, options?: WorkspaceDriver.ReadFileOptions) =>
          Effect.tryPromise({
            try: (signal): Promise<Buffer> =>
              options?.maxBytes === undefined
                ? fs.readFile(path, { signal })
                : boundedRead(path, options.maxBytes, signal),
            catch: readFailure('readFileBuffer', path, options?.maxBytes),
          }).pipe(
            // A Node `Buffer` is a `Uint8Array` view over a pooled allocation.
            // Copying it out keeps the returned bytes from aliasing a buffer
            // Node may reuse, which is the sort of bug that only shows up under
            // load.
            Effect.map((buffer) => Uint8Array.from(buffer)),
          ),
      );

      const writeFile = Effect.fn('WorkspaceDriver.writeFile')(
        (path: string, content: string | Uint8Array) =>
          Effect.tryPromise({
            try: (signal) => fs.writeFile(path, content, { signal }),
            catch: fsFailure('writeFile', path),
          }),
      );

      const stat = Effect.fn('WorkspaceDriver.stat')((path: string) =>
        Effect.tryPromise({
          // `lstat`, so a symlink reports as a symlink. `stat` would follow it
          // and make `isSymbolicLink` unreachable.
          try: (): Promise<Stats> => fs.lstat(path),
          catch: fsFailure('stat', path),
        }).pipe(
          Effect.map(
            (stats): WorkspaceDriver.FileStat => ({
              isFile: stats.isFile(),
              isDirectory: stats.isDirectory(),
              isSymbolicLink: stats.isSymbolicLink(),
              size: stats.size,
              mtime: stats.mtimeMs,
            }),
          ),
        ),
      );

      const readdir = Effect.fn('WorkspaceDriver.readdir')((path: string) =>
        Effect.tryPromise({
          try: (): Promise<ReadonlyArray<string>> => fs.readdir(path),
          catch: fsFailure('readdir', path),
        }).pipe(Effect.map((entries): ReadonlyArray<string> => entries)),
      );

      const exists = Effect.fn('WorkspaceDriver.exists')((path: string) =>
        Effect.tryPromise({
          try: (): Promise<void> => fs.access(path),
          catch: fsFailure('exists', path),
        }).pipe(
          Effect.as(true),
          // Only absence answers `false`. A permission error still fails, so
          // "cannot tell" never reads as "not there".
          Effect.catchTag('PathNotFound', () => Effect.succeed(false)),
        ),
      );

      const mkdir = Effect.fn('WorkspaceDriver.mkdir')(
        (path: string, options?: WorkspaceDriver.MkdirOptions) =>
          Effect.tryPromise({
            try: (): Promise<string | undefined> =>
              fs.mkdir(path, { recursive: options?.recursive ?? false }),
            catch: fsFailure('mkdir', path),
          }).pipe(Effect.asVoid),
      );

      const rm = Effect.fn('WorkspaceDriver.rm')(
        (path: string, options?: WorkspaceDriver.RmOptions) =>
          Effect.tryPromise({
            try: (): Promise<void> =>
              fs.rm(path, {
                recursive: options?.recursive ?? false,
                force: options?.force ?? false,
              }),
            catch: fsFailure('rm', path),
          }),
      );

      const exec = Effect.fn('WorkspaceDriver.exec')((
        command: string,
        options?: WorkspaceDriver.ExecOptions,
      ): Effect.Effect<
        WorkspaceDriver.ShellResult,
        WorkspaceDriver.ExecError
      > => {
        if (
          confinement?.backend === undefined &&
          confinement?.mode === 'required'
        ) {
          return Effect.fail(
            new WorkspaceDriver.SandboxUnavailable({
              command,
              reason:
                'no working sandbox backend on this host (macOS needs sandbox-exec, Linux needs bwrap with user namespaces)',
            }),
          );
        }
        const backend = confinement?.backend;
        const sandboxLabel: WorkspaceDriver.ShellResult['sandbox'] =
          confinement === undefined ? undefined : (backend ?? 'fallback');
        const environment = inheritEnvironment
          ? options?.env
          : { PATH: SAFE_PATH, ...options?.env };

        const run = Effect.gen(function* () {
          // The sandbox matches and binds resolved paths, so the working
          // directory must be expressed in that same space (`/var` vs
          // `/private/var`, a symlinked workspace root). Unconfined commands
          // keep the caller's cwd untouched.
          const requested = options?.cwd;
          const cwd =
            backend === undefined || requested === undefined
              ? requested
              : yield* Effect.promise(() =>
                  fs.realpath(requested).catch(() => requested),
                );
          const invocation =
            confinement === undefined || backend === undefined
              ? undefined
              : backend === 'sandbox-exec'
                ? ShellSandbox.sandboxExecInvocation(confinement.plan, command)
                : ShellSandbox.bwrapInvocation(confinement.plan, command, cwd);
          const handle = yield* spawner.spawn(
            invocation !== undefined && confinement !== undefined
              ? ChildProcess.make(invocation.file, [...invocation.args], {
                  cwd,
                  // Private temp for both backends: the tmpfs on Linux, the
                  // per-layer directory on macOS.
                  env: {
                    TMPDIR:
                      backend === 'bwrap' ? '/tmp' : confinement.plan.tempDir,
                    ...environment,
                  },
                  extendEnv: inheritEnvironment,
                })
              : ChildProcess.make(command, {
                  // A command string, not an argv: the caller writes shell, so
                  // pipelines and redirection work. It is also why an unknown
                  // command comes back as exit 127 rather than a spawn failure —
                  // the shell started fine, and reporting what it exited with is
                  // the whole point of returning the code as data.
                  shell: true,
                  cwd: options?.cwd,
                  env: environment,
                  // The safe local layer supplies a small executable PATH and no
                  // other host variables. `unrestricted` opts into ambient env
                  // inheritance for callers that explicitly need it.
                  extendEnv: inheritEnvironment,
                }),
          );

          // Concurrently, and that is load-bearing: `exitCode` waits for the
          // process, and a process writing more than a pipe buffer blocks
          // until someone drains it. Awaiting the exit first would deadlock
          // on any command with substantial output.
          const stdout = new ByteTail(DEFAULT_MAX_BYTES);
          const stderr = new ByteTail(DEFAULT_MAX_BYTES);
          const { exitCode } = yield* Effect.all(
            {
              exitCode: handle.exitCode,
              stdout: Stream.runForEach(handle.stdout, (chunk) => {
                return Effect.sync(() => {
                  stdout.append(chunk);
                });
              }),
              stderr: Stream.runForEach(handle.stderr, (chunk) => {
                return Effect.sync(() => {
                  stderr.append(chunk);
                });
              }),
            },
            { concurrency: 'unbounded' },
          );

          return {
            exitCode,
            stdout: stdout.text(),
            stderr: stderr.text(),
            stdoutTruncated: stdout.truncated,
            stderrTruncated: stderr.truncated,
            ...(sandboxLabel === undefined ? {} : { sandbox: sandboxLabel }),
          };
        }).pipe(
          Effect.scoped,
          Effect.mapError(execFailure(command, options?.cwd)),
        );

        const timeoutMs = options?.timeoutMs;
        return timeoutMs === undefined
          ? run
          : run.pipe(
              Effect.timeoutOrElse({
                duration: Duration.millis(timeoutMs),
                orElse: () =>
                  Effect.fail(
                    new WorkspaceDriver.CommandTimeout({ command, timeoutMs }),
                  ),
              }),
            );
      });

      return WorkspaceDriver.Service.of({
        readFile,
        readFileBuffer,
        writeFile,
        stat,
        readdir,
        exists,
        mkdir,
        rm,
        exec,
      });
    }),
  );

/** Local driver with an isolated command environment by default. */
export const layer = makeLayer(false);

/**
 * Local driver whose commands run inside an OS sandbox, per `policy.mode`.
 *
 * Same isolated environment as {@link layer}. With `mode: 'required'` and no
 * working backend, `exec` fails with `SandboxUnavailable` instead of running.
 */
export const sandboxed = (
  policy: ShellSandbox.Policy,
): Layer.Layer<
  WorkspaceDriver.Service,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> => makeLayer(false, policy);

/** Explicit opt-in to the legacy full host-local environment behavior. */
export const unrestricted = makeLayer(true);

export * as WorkspaceLocal from './layer-local.js';
