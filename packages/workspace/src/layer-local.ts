import type { Stats } from 'node:fs';
import * as fs from 'node:fs/promises';

import { Duration, Effect, Layer, Stream } from 'effect';
import type { PlatformError } from 'effect/PlatformError';
import { ChildProcess, ChildProcessSpawner } from 'effect/unstable/process';

import { WorkspaceDriver } from './driver.js';

// The local driver: `node:fs/promises` for files, Effect's
// `ChildProcessSpawner` for commands.
//
// It is the reference implementation and the one the contract suite runs
// against, not a stub — a container or remote driver differs in where the
// bytes live, not in what the operations mean.
//
// It confines nothing. See the boundary note in `./driver.ts`: this runs with
// the authority of the host process, and every path is a host path.
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

const errnoCode = (error: unknown): string | undefined =>
  typeof error === 'object' &&
  error !== null &&
  'code' in error &&
  typeof (error as { code: unknown }).code === 'string'
    ? (error as { code: string }).code
    : undefined;

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
  (error: unknown): WorkspaceDriver.FileError => {
    const code = errnoCode(error);
    if (code === 'ENOENT') {
      return new WorkspaceDriver.PathNotFound({ path, operation });
    }
    if (code === 'EACCES' || code === 'EPERM') {
      return new WorkspaceDriver.PermissionDenied({ path, operation });
    }
    return new WorkspaceDriver.WorkspaceFailure({
      operation,
      path,
      code: code ?? 'Unknown',
      cause: error,
    });
  };

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

    if (reason._tag === 'NotFound') {
      return new WorkspaceDriver.PathNotFound({ path, operation: 'exec' });
    }
    if (reason._tag === 'PermissionDenied') {
      return new WorkspaceDriver.PermissionDenied({ path, operation: 'exec' });
    }
    return new WorkspaceDriver.WorkspaceFailure({
      operation: 'exec',
      path,
      code: reason._tag,
      cause: error,
    });
  };

export const layer: Layer.Layer<
  WorkspaceDriver.Service,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> = Layer.effect(
  WorkspaceDriver.Service,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    // `Effect.fn` with plain functions rather than generators wherever the
    // body has nothing to yield: an empty generator trips `require-yield`.
    const readFile = Effect.fn('AiWorkspace.readFile')((path: string) =>
      Effect.tryPromise({
        try: (signal): Promise<string> =>
          fs.readFile(path, { encoding: 'utf8', signal }),
        catch: fsFailure('readFile', path),
      }),
    );

    const readFileBuffer = Effect.fn('AiWorkspace.readFileBuffer')(
      (path: string) =>
        Effect.tryPromise({
          try: (signal): Promise<Buffer> => fs.readFile(path, { signal }),
          catch: fsFailure('readFileBuffer', path),
        }).pipe(
          // A Node `Buffer` is a `Uint8Array` view over a pooled allocation.
          // Copying it out keeps the returned bytes from aliasing a buffer
          // Node may reuse, which is the sort of bug that only shows up under
          // load.
          Effect.map((buffer) => Uint8Array.from(buffer)),
        ),
    );

    const writeFile = Effect.fn('AiWorkspace.writeFile')(
      (path: string, content: string | Uint8Array) =>
        Effect.tryPromise({
          try: (signal) => fs.writeFile(path, content, { signal }),
          catch: fsFailure('writeFile', path),
        }),
    );

    const stat = Effect.fn('AiWorkspace.stat')((path: string) =>
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

    const readdir = Effect.fn('AiWorkspace.readdir')((path: string) =>
      Effect.tryPromise({
        try: (): Promise<ReadonlyArray<string>> => fs.readdir(path),
        catch: fsFailure('readdir', path),
      }).pipe(Effect.map((entries): ReadonlyArray<string> => entries)),
    );

    const exists = Effect.fn('AiWorkspace.exists')((path: string) =>
      Effect.tryPromise({
        try: (): Promise<void> => fs.access(path),
        catch: fsFailure('exists', path),
      }).pipe(
        Effect.as(true),
        // Only absence answers `false`. A permission error still fails, so
        // "cannot tell" never reads as "not there".
        Effect.catchTag('@sunfall/vesper-workspace/PathNotFound', () =>
          Effect.succeed(false),
        ),
      ),
    );

    const mkdir = Effect.fn('AiWorkspace.mkdir')(
      (path: string, options?: WorkspaceDriver.MkdirOptions) =>
        Effect.tryPromise({
          try: (): Promise<string | undefined> =>
            fs.mkdir(path, { recursive: options?.recursive ?? false }),
          catch: fsFailure('mkdir', path),
        }).pipe(Effect.asVoid),
    );

    const rm = Effect.fn('AiWorkspace.rm')(
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

    const exec = Effect.fn('AiWorkspace.exec')((
      command: string,
      options?: WorkspaceDriver.ExecOptions,
    ): Effect.Effect<
      WorkspaceDriver.ShellResult,
      WorkspaceDriver.ExecError
    > => {
      const run = Effect.gen(function* () {
        const handle = yield* spawner.spawn(
          ChildProcess.make(command, {
            // A command string, not an argv: the caller writes shell, so
            // pipelines and redirection work. It is also why an unknown
            // command comes back as exit 127 rather than a spawn failure —
            // the shell started fine, and reporting what it exited with is
            // the whole point of returning the code as data.
            shell: true,
            cwd: options?.cwd,
            env: options?.env,
            // Merge over the ambient environment. Replacing it would drop
            // `PATH`, and a caller setting one variable would have to
            // reconstruct the rest. It also means the host process's
            // environment reaches every command; see `ExecOptions.env`.
            extendEnv: true,
          }),
        );

        // Concurrently, and that is load-bearing: `exitCode` waits for the
        // process, and a process writing more than a pipe buffer blocks
        // until someone drains it. Awaiting the exit first would deadlock
        // on any command with substantial output.
        const [exitCode, stdout, stderr] = yield* Effect.all(
          [
            handle.exitCode,
            Stream.mkString(Stream.decodeText(handle.stdout)),
            Stream.mkString(Stream.decodeText(handle.stderr)),
          ],
          { concurrency: 'unbounded' },
        );

        return { exitCode: exitCode as number, stdout, stderr };
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

export * as WorkspaceLocal from './layer-local.js';
