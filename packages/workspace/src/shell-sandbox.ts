import { Effect } from 'effect';
import { ChildProcess, type ChildProcessSpawner } from 'effect/process';

import { WorkspacePath } from './path.js';

// OS-enforced confinement for workspace shell commands.
//
// `driver.ts` is explicit that a driver is not a sandbox, and for a driver
// that runs commands as plain child processes that stays true. This module is
// the opt-in that changes it for the local driver: each command is wrapped in
// a kernel-enforced profile, so the restriction holds for code that does not
// cooperate — the `curl | sh` a model was talked into, not just the tool call
// that stayed inside the root.
//
// Two backends, chosen by the host:
//
// - macOS: `sandbox-exec` with a generated SBPL profile. The tool is
//   deprecated by Apple and has no replacement for unprivileged callers; it
//   is what Chromium, Bazel and Codex-style CLIs ship on. It works today, and
//   `detect` probes it for real rather than trusting that the binary exists.
// - Linux: `bwrap` (bubblewrap), which builds a fresh mount namespace that
//   contains only what is bound into it.
//
// **Paths never become profile text.** The SBPL profile is a constant and each
// path reaches it as a `-D NAME=value` parameter read with `(param "NAME")`.
// A workspace named `x") (allow file-write* (subpath "/` is therefore just a
// string, not a rule: there is no escaping function to get wrong. On Linux
// every path is its own argv element for the same reason.

/**
 * What to do when a sandbox is wanted.
 *
 * - `none`: run unconfined, as the local driver always has. Explicit so that a
 *   layer's blast radius is readable from its construction.
 * - `preferred`: confine when a backend works, otherwise run unconfined and
 *   say so in the result (`sandbox: 'fallback'`) and on the span.
 * - `required`: confine, or refuse. With no working backend every command
 *   fails with `SandboxUnavailable`; nothing silently runs unconfined.
 */
export type Mode = 'none' | 'preferred' | 'required';

export type Backend = 'sandbox-exec' | 'bwrap';

export interface Policy {
  readonly mode: Mode;
  /** Absolute workspace root: the only tree commands may write besides temp. */
  readonly root: string;
  /** Allow network access. Off unless the application says otherwise. */
  readonly network?: boolean | undefined;
  /**
   * Absolute paths commands may read beyond the system directories, the root
   * and the temp directory — a toolchain installed under the home directory,
   * for instance. Read-only.
   */
  readonly readPaths?: ReadonlyArray<string> | undefined;
  /** Absolute paths commands may also write, e.g. a package cache. */
  readonly writePaths?: ReadonlyArray<string> | undefined;
}

/** A policy with every path resolved, ready to turn into an invocation. */
export interface Plan {
  readonly root: string;
  readonly tempDir: string;
  readonly network: boolean;
  readonly readPaths: ReadonlyArray<string>;
  readonly writePaths: ReadonlyArray<string>;
}

export interface Invocation {
  readonly file: string;
  readonly args: ReadonlyArray<string>;
}

export const SANDBOX_EXEC = '/usr/bin/sandbox-exec';

/** The one place a path is validated: absolute, and no NUL byte. */
const checkedPath = (path: string): string => {
  if (!path.startsWith('/') || path.includes('\0')) {
    throw new RangeError(`sandbox paths must be absolute without NUL: ${path}`);
  }
  return WorkspacePath.normalize(path);
};

export const makePlan = (
  policy: Policy,
  roots: { readonly root: string; readonly tempDir: string },
): Plan => ({
  root: checkedPath(roots.root),
  tempDir: checkedPath(roots.tempDir),
  network: policy.network ?? false,
  readPaths: (policy.readPaths ?? []).map(checkedPath),
  writePaths: (policy.writePaths ?? []).map(checkedPath),
});

// -------------------------------------------------------------------- macOS

// What a build tool needs to start: the OS, compilers, package managers
// installed by Homebrew. Deliberately absent: `/Users`, `/private/tmp`, and
// `/private/var/folders` (other apps' temp files). Metadata is allowed
// everywhere because `realpath` and `stat` walk through every ancestor; it
// reveals that a path exists, never what is in it.
const MAC_SYSTEM_READS = [
  '/usr',
  '/bin',
  '/sbin',
  '/System',
  '/Library',
  '/Applications/Xcode.app',
  '/opt/homebrew',
  '/private/etc',
  '/private/var/db',
  '/dev',
] as const;

/**
 * The SBPL profile. A constant apart from the parameter lists: `deny default`
 * and then a short allowlist, so anything not named here — mach services,
 * IOKit, sockets, signals to other processes, writes outside the plan — is
 * refused.
 */
export const sandboxExecProfile = (plan: Plan): string => {
  const reads = [
    ...MAC_SYSTEM_READS.map((path) => `(subpath "${path}")`),
    ...plan.readPaths.map(
      (_, index) => `(subpath (param "READ_${String(index)}"))`,
    ),
  ];
  const writes = plan.writePaths.map(
    (_, index) => `(subpath (param "WRITE_${String(index)}"))`,
  );
  return [
    '(version 1)',
    '(deny default)',
    '(allow process-exec)',
    '(allow process-fork)',
    '(allow signal (target self))',
    '(allow sysctl-read)',
    '(allow file-read-metadata)',
    `(allow file-read* (literal "/") ${reads.join(' ')})`,
    '(allow file-read* file-write* (literal "/dev/null") (literal "/dev/dtracehelper") (literal "/dev/tty"))',
    '(allow file-read* file-write* (subpath (param "ROOT")) (subpath (param "TMP"))' +
      `${writes.length === 0 ? '' : ` ${writes.join(' ')}`})`,
    ...(plan.network ? ['(allow network*)', '(allow mach-lookup)'] : []),
  ].join('\n');
};

/** `sandbox-exec` argv: parameters, the profile inline, then `sh -c`. */
export const sandboxExecInvocation = (
  plan: Plan,
  command: string,
): Invocation => {
  const parameters: Array<readonly [string, string]> = [
    ['ROOT', plan.root],
    ['TMP', plan.tempDir],
    ...plan.readPaths.map((path, index): readonly [string, string] => [
      `READ_${String(index)}`,
      path,
    ]),
    ...plan.writePaths.map((path, index): readonly [string, string] => [
      `WRITE_${String(index)}`,
      path,
    ]),
  ];
  return {
    file: SANDBOX_EXEC,
    args: [
      ...parameters.flatMap(([name, value]) => ['-D', `${name}=${value}`]),
      '-p',
      sandboxExecProfile(plan),
      '/bin/sh',
      '-c',
      command,
    ],
  };
};

// -------------------------------------------------------------------- Linux

const LINUX_SYSTEM_READS = [
  '/usr',
  '/bin',
  '/sbin',
  '/lib',
  '/lib32',
  '/lib64',
  '/etc',
  '/opt',
] as const;

/**
 * `bwrap` argv. The sandbox starts from an empty mount namespace, so the home
 * directory simply is not there unless the root lives under it; the root is
 * bound read-write, system directories read-only, and `/tmp` is a private
 * tmpfs bound *before* the root so a root under `/tmp` stays visible.
 */
export const bwrapInvocation = (
  plan: Plan,
  command: string,
  cwd: string | undefined,
): Invocation => ({
  file: 'bwrap',
  args: [
    '--die-with-parent',
    '--new-session',
    '--unshare-pid',
    '--unshare-ipc',
    '--unshare-uts',
    ...(plan.network ? [] : ['--unshare-net']),
    ...LINUX_SYSTEM_READS.flatMap((path) => ['--ro-bind-try', path, path]),
    ...plan.readPaths.flatMap((path) => ['--ro-bind-try', path, path]),
    '--proc',
    '/proc',
    '--dev',
    '/dev',
    '--tmpfs',
    '/tmp',
    '--bind',
    plan.root,
    plan.root,
    ...plan.writePaths.flatMap((path) => ['--bind-try', path, path]),
    ...(cwd === undefined ? [] : ['--chdir', cwd]),
    '--',
    '/bin/sh',
    '-c',
    command,
  ],
});

// --------------------------------------------------------------- detection

/**
 * The probe for `platform`, or `undefined` where no backend exists.
 *
 * Built from the same invocation builders real commands use, with a trivial
 * command. The probe therefore exercises the exact profile, namespaces and
 * mounts a real command needs and cannot drift from them: a host where
 * `bwrap --ro-bind / /` works but `--proc /proc` or `--unshare-pid` fails
 * (an unprivileged container, for instance) is reported unavailable.
 */
export const probeInvocation = (
  platform: NodeJS.Platform,
  plan: Plan,
): { readonly backend: Backend; readonly invocation: Invocation } | undefined =>
  platform === 'darwin'
    ? {
        backend: 'sandbox-exec',
        invocation: sandboxExecInvocation(plan, 'true'),
      }
    : platform === 'linux'
      ? {
          backend: 'bwrap',
          invocation: bwrapInvocation(plan, 'true', plan.root),
        }
      : undefined;

/**
 * Find a backend that works on this host, or `undefined`.
 *
 * Probed by running a trivial command under the real plan rather than by
 * checking that a binary exists: `sandbox-exec` refuses to nest inside another
 * sandbox, and `bwrap` fails on hosts that disallow unprivileged user
 * namespaces or mounting `/proc`. Either would look installed and then fail
 * every command.
 */
export const detect = (
  platform: NodeJS.Platform,
  spawner: ChildProcessSpawner.ChildProcessSpawner['Service'],
  plan: Plan,
): Effect.Effect<Backend | undefined> =>
  Effect.gen(function* () {
    const probe = probeInvocation(platform, plan);
    if (probe === undefined) {
      return undefined;
    }
    return yield* Effect.gen(function* () {
      const handle = yield* spawner.spawn(
        ChildProcess.make(probe.invocation.file, [...probe.invocation.args], {
          stdin: 'ignore',
          cwd: plan.root,
          env: { TMPDIR: plan.tempDir },
        }),
      );
      return (yield* handle.exitCode) === 0 ? probe.backend : undefined;
    }).pipe(
      Effect.scoped,
      Effect.orElseSucceed((): Backend | undefined => undefined),
    );
  });

export * as ShellSandbox from './shell-sandbox.js';
