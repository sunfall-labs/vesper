import { Context, Effect, Schema } from 'effect';

// The workspace seam: a filesystem and a shell behind one service, so a local
// process, a container, or a remote worker are interchangeable by layer.
//
// **A driver swap is a composition boundary, not a security boundary.** It
// decides *where* a tool's file reads and commands land, and nothing more. It
// constrains code that cooperates: a tool that takes this service and uses it
// stays inside whatever the layer points at, and a tool that reaches for
// `node:fs` directly is unaffected by which driver is wired. Nothing here
// workspacees an untrusted command — `exec` on the local driver runs with the
// full authority of the host process, and paths are not confined to a root.
// If the requirement is containment of hostile code, that has to come from
// the driver's own substrate (a container, a VM, a jailed remote worker), and
// the value of this seam is only that swapping to one is a layer change.
//
// The operation set is the conventional one — read, write, edit, list,
// search, exec — because it is a good shape and because adapters for other
// agent frameworks are already written against it. The differences are
// Effect's:
// every method returns an `Effect` with typed failures instead of a rejecting
// promise, and cancellation is Effect's interruption rather than an
// `AbortSignal` parameter — a caller who wants a deadline uses `timeoutMs` or
// wraps the call, and interrupting the fiber is what stops the work.

/**
 * Which method a failure came from.
 *
 * Carried on every error because the path alone rarely identifies the call:
 * `ENOENT` from `readFile` and `ENOENT` from `rm` want different handling,
 * and a driver that only reported the path would make a caller guess.
 */
export const Operation = Schema.Literals([
  'readFile',
  'readFileBuffer',
  'writeFile',
  'stat',
  'readdir',
  'exists',
  'mkdir',
  'rm',
  'exec',
]);
export type Operation = typeof Operation.Type;

/**
 * File metadata.
 *
 * `isSymbolicLink`, `size`, and `mtime` are optional keys because a remote
 * driver's provider may not expose them. A driver that cannot read one must
 * omit the key rather than invent a placeholder — `size: 0` is
 * indistinguishable from an empty file, and that lie is worse than the gap.
 *
 * `mtime` is epoch milliseconds rather than a `Date`, so the struct survives
 * a JSON boundary without a codec argument at every call site.
 */
export const FileStat = Schema.Struct({
  isFile: Schema.Boolean,
  isDirectory: Schema.Boolean,
  isSymbolicLink: Schema.optionalKey(Schema.Boolean),
  size: Schema.optionalKey(Schema.Number),
  mtime: Schema.optionalKey(Schema.Number),
});
export interface FileStat extends Schema.Struct.Type<typeof FileStat.fields> {}

/**
 * What a finished command produced.
 *
 * A non-zero `exitCode` is data, not a failure. `grep` exits 1 for no match,
 * `git diff --quiet` exits 1 for "there are changes", `test -f` exits 1 for
 * false, `diff` exits 1 for "files differ" — all ordinary outcomes an agent
 * needs to *read*. Modelling them as errors would put `Effect.catchTag` in
 * the middle of ordinary control flow.
 *
 * A caller for whom non-zero really is fatal writes it at the point where it
 * matters:
 *
 * ```ts
 * workspace.exec('npm test').pipe(
 *   Effect.filterOrFail(
 *     (result) => result.exitCode === 0,
 *     (result) => new TestsFailed({ stderr: result.stderr }),
 *   ),
 * )
 * ```
 */
export const ShellResult = Schema.Struct({
  stdout: Schema.String,
  stderr: Schema.String,
  exitCode: Schema.Number,
});
export interface ShellResult extends Schema.Struct.Type<
  typeof ShellResult.fields
> {}

/** The path does not exist. */
export class PathNotFound extends Schema.TaggedErrorClass<PathNotFound>()(
  '@sunfall/vesper-workspace/PathNotFound',
  {
    path: Schema.String,
    operation: Operation,
  },
) {}

/** The path exists but the driver is not allowed to touch it that way. */
export class PermissionDenied extends Schema.TaggedErrorClass<PermissionDenied>()(
  '@sunfall/vesper-workspace/PermissionDenied',
  {
    path: Schema.String,
    operation: Operation,
  },
) {}

/**
 * A command did not finish within `timeoutMs` and was terminated.
 *
 * A failure rather than a result, because a killed command produced no
 * outcome to report: its exit code is the signal that killed it, and its
 * output is however far it happened to get. That is not something a caller
 * can read as data the way a non-zero exit is.
 */
export class CommandTimeout extends Schema.TaggedErrorClass<CommandTimeout>()(
  '@sunfall/vesper-workspace/CommandTimeout',
  {
    command: Schema.String,
    timeoutMs: Schema.Number,
  },
) {}

/**
 * Anything else the substrate refused.
 *
 * The residual case, not the general one: `EEXIST` on `mkdir`, `ENOTDIR`,
 * `EISDIR`, a spawn that never started, a transport fault in a remote driver.
 * `code` is the driver's own classification — an `errno` string for the local
 * driver — so an unmodelled failure is still diagnosable without reading the
 * defect.
 */
export class WorkspaceFailure extends Schema.TaggedErrorClass<WorkspaceFailure>()(
  '@sunfall/vesper-workspace/WorkspaceFailure',
  {
    operation: Operation,
    path: Schema.optionalKey(Schema.String),
    code: Schema.String,
    cause: Schema.Defect(),
  },
) {}

/** Everything a filesystem operation can fail with. */
export type FileError = PathNotFound | PermissionDenied | WorkspaceFailure;

/**
 * Everything {@link Interface.exec} can fail with.
 *
 * All of them mean "the command did not run to completion". A command that
 * ran and exited non-zero is a {@link ShellResult}, not a failure.
 */
export type ExecError =
  | CommandTimeout
  | PathNotFound
  | PermissionDenied
  | WorkspaceFailure;

/** Every typed failure this seam produces. */
export type WorkspaceError = ExecError | FileError;

export interface MkdirOptions {
  /** Create missing parents, and succeed when the directory already exists. */
  readonly recursive?: boolean | undefined;
}

export interface RmOptions {
  readonly recursive?: boolean | undefined;
  /** Treat a missing path as success rather than {@link PathNotFound}. */
  readonly force?: boolean | undefined;
}

export interface ExecOptions {
  readonly cwd?: string | undefined;
  /**
   * Extra environment for the command.
   *
   * Merged over the driver's ambient environment rather than replacing it, so
   * a caller setting one variable does not have to reconstruct `PATH`.
   *
   * **The local driver's ambient environment is the host process's.** Every
   * command therefore inherits whatever the host was started with, secrets
   * included, and this option adds to that rather than narrowing it. It is
   * the environment half of the boundary note at the top of this file: the
   * seam decides where a command runs, not what it can see. A container or
   * remote driver's ambient environment is its own and will not match — code
   * that relies on inheriting a host variable will stop working on the layer
   * swap, which is the right time to find out.
   */
  readonly env?: Record<string, string> | undefined;
  /**
   * Wall-clock deadline in milliseconds. On expiry the command is terminated
   * and the call fails with {@link CommandTimeout}.
   *
   * Independent of interruption. Interrupting the fiber also terminates the
   * command; `timeoutMs` exists because a deadline is the thing an agent
   * actually asks for, and a remote driver can forward it to a provider's
   * native timeout where it cannot observe our fiber at all.
   */
  readonly timeoutMs?: number | undefined;
}

export interface Interface {
  /** Read a file as UTF-8 text. */
  readonly readFile: (path: string) => Effect.Effect<string, FileError>;

  /** Read a file as raw bytes, for content that is not text. */
  readonly readFileBuffer: (
    path: string,
  ) => Effect.Effect<Uint8Array, FileError>;

  /** Write a file, creating or truncating it. Parent directories must exist. */
  readonly writeFile: (
    path: string,
    content: string | Uint8Array,
  ) => Effect.Effect<void, FileError>;

  /**
   * Metadata for a path, without following a final symlink — a link reports
   * as a link rather than as its target.
   */
  readonly stat: (path: string) => Effect.Effect<FileStat, FileError>;

  /** Entry names directly under a directory. Not recursive, not sorted. */
  readonly readdir: (
    path: string,
  ) => Effect.Effect<ReadonlyArray<string>, FileError>;

  /**
   * Whether a path exists.
   *
   * A missing path is `false`, not a failure; anything else that blocks the
   * check still fails, so "cannot tell" never reads as "not there".
   */
  readonly exists: (path: string) => Effect.Effect<boolean, FileError>;

  readonly mkdir: (
    path: string,
    options?: MkdirOptions,
  ) => Effect.Effect<void, FileError>;

  readonly rm: (
    path: string,
    options?: RmOptions,
  ) => Effect.Effect<void, FileError>;

  /**
   * Run a shell command and collect its output.
   *
   * Succeeds for any command that ran to completion, whatever it exited with
   * — see {@link ShellResult}. Failure is reserved for "could not run it at
   * all": a bad `cwd`, a host that refused to fork, or a deadline that killed
   * it mid-flight.
   *
   * The command must be terminated when the call is interrupted or its
   * deadline expires. That is a driver obligation, not a caller's: a driver
   * that returns early while its process keeps running has leaked it.
   */
  readonly exec: (
    command: string,
    options?: ExecOptions,
  ) => Effect.Effect<ShellResult, ExecError>;
}

export class Service extends Context.Service<Service, Interface>()(
  '@sunfall/vesper-workspace/WorkspaceDriver',
) {}

export * as WorkspaceDriver from './driver.js';
