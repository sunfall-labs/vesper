import { once } from 'node:events';
import { createServer } from 'node:net';

import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, describe, expect, it } from '@effect/vitest';
import { Effect, Layer, PlatformError, Sink, Stream } from 'effect';
import { ChildProcessSpawner } from 'effect/process';

import { WorkspaceDriver } from '../src/driver.js';
import { sandboxed, unrestricted } from '../src/layer-local.js';
import { ShellSandbox } from '../src/shell-sandbox.js';

const base = `/tmp/ai-sandbox-${String(process.pid)}`;

// Fixtures go through an unconfined local driver rather than `node:fs`, which
// also yields real (symlink-resolved) paths, as the sandbox requires. Paths
// reach the shell through the environment so a hostile name is never parsed.
const unconfined = (command: string, env: Record<string, string>) =>
  Effect.gen(function* () {
    const driver = yield* WorkspaceDriver.Service;
    const result = yield* driver.exec(command, { env });
    return result.stdout.replace(/\n$/, '');
  }).pipe(Effect.provide(unrestricted.pipe(Layer.provide(NodeServices.layer))));

const homeDirectory = unconfined('echo "$HOME"', {});

/** Create `base/<name>` and return its real path; `name` may be hostile. */
const makeDir = (name: string) =>
  unconfined('mkdir -p "$DIR" && cd "$DIR" && pwd -P', {
    DIR: `${base}/${name}`,
  });

afterAll(() => Effect.runPromise(unconfined('rm -rf "$DIR"', { DIR: base })));

const plan = (
  overrides: Partial<ShellSandbox.Plan> = {},
): ShellSandbox.Plan => ({
  root: '/work/root',
  tempDir: '/tmp/vesper-x',
  network: false,
  readPaths: [],
  writePaths: [],
  ...overrides,
});

const exec = (
  policy: ShellSandbox.Policy,
  command: string,
  cwd = policy.root,
) =>
  Effect.gen(function* () {
    const driver = yield* WorkspaceDriver.Service;
    return yield* driver.exec(command, { cwd, timeoutMs: 20_000 });
  }).pipe(
    Effect.provide(sandboxed(policy).pipe(Layer.provide(NodeServices.layer))),
  );

describe('SBPL profile', () => {
  it('never contains a path, however hostile', () => {
    const hostile = [
      '/w/quo"te',
      '/w/back\\slash',
      '/w/paren)(allow file-write* (subpath "/"))',
      '/w/new\nline',
    ];
    const profile = ShellSandbox.sandboxExecProfile(
      plan({ readPaths: hostile, writePaths: hostile, root: hostile[2] ?? '' }),
    );
    for (const path of hostile) {
      expect(profile).not.toContain(path);
    }
    expect(profile).not.toContain('allow file-write* (subpath "/")');
    expect(profile.startsWith('(version 1)\n(deny default)')).toBe(true);
    // One write rule, and it only names parameters.
    expect(profile.match(/file-write\*/g)).toHaveLength(2);
  });

  it('passes hostile paths as single -D arguments', () => {
    const root = '/w/x") (allow default) ("';
    const invocation = ShellSandbox.sandboxExecInvocation(
      plan({ root }),
      'echo hi',
    );
    expect(invocation.file).toBe('/usr/bin/sandbox-exec');
    const index = invocation.args.indexOf(`ROOT=${root}`);
    expect(invocation.args[index - 1]).toBe('-D');
    expect(invocation.args.slice(-3)).toEqual(['/bin/sh', '-c', 'echo hi']);
  });

  it('grants network only when asked', () => {
    expect(ShellSandbox.sandboxExecProfile(plan())).not.toContain('network');
    expect(ShellSandbox.sandboxExecProfile(plan({ network: true }))).toContain(
      '(allow network*)',
    );
  });

  it('rejects relative paths', () => {
    expect(() =>
      ShellSandbox.makePlan(
        { mode: 'required', root: 'rel' },
        { root: 'rel', tempDir: '/tmp/x' },
      ),
    ).toThrow(RangeError);
  });
});

describe('bwrap argv', () => {
  const argv = (overrides: Partial<ShellSandbox.Plan> = {}) =>
    ShellSandbox.bwrapInvocation(plan(overrides), 'echo "$X"', '/work/root/sub')
      .args;

  it('binds the root read-write and keeps the command one element', () => {
    const args = argv();
    const bind = args.indexOf('--bind');
    expect(args.slice(bind, bind + 3)).toEqual([
      '--bind',
      '/work/root',
      '/work/root',
    ]);
    expect(args.slice(-4)).toEqual(['--', '/bin/sh', '-c', 'echo "$X"']);
    expect(args).toContain('--die-with-parent');
    expect(args).toContain('--new-session');
    expect(args.slice(args.indexOf('--chdir'))[1]).toBe('/work/root/sub');
  });

  it('mounts system directories read-only and never the home directory', () => {
    const args = argv();
    expect(args).toContain('--ro-bind-try');
    expect(args).not.toContain('/home');
    expect(args.filter((arg) => arg === '--bind')).toHaveLength(1);
  });

  it('unshares the network unless allowed', () => {
    expect(argv()).toContain('--unshare-net');
    expect(argv({ network: true })).not.toContain('--unshare-net');
  });

  it('mounts /tmp before the root so a root under /tmp survives', () => {
    const args = argv({ root: '/tmp/ws' });
    expect(args.indexOf('--tmpfs')).toBeLessThan(args.indexOf('--bind'));
  });

  it('keeps hostile paths as whole argv elements', () => {
    const root = '/w/a b";rm -rf /;"\n(x)';
    expect(argv({ root })).toContain(root);
  });
});

describe('availability probe', () => {
  const probePlan = plan({
    network: false,
    readPaths: ['/opt/tools'],
    writePaths: ['/cache'],
  });

  it('is the real bwrap plan with a trivial command', () => {
    const probe = ShellSandbox.probeInvocation('linux', probePlan);
    const real = ShellSandbox.bwrapInvocation(
      probePlan,
      'true',
      probePlan.root,
    );
    expect(probe?.backend).toBe('bwrap');
    expect(probe?.invocation).toEqual(real);
    for (const flag of [
      '--unshare-pid',
      '--unshare-net',
      '--proc',
      '--dev',
      '--tmpfs',
      '--bind',
    ]) {
      expect(probe?.invocation.args).toContain(flag);
    }
  });

  it('is the real sandbox-exec profile with a trivial command', () => {
    const probe = ShellSandbox.probeInvocation('darwin', probePlan);
    expect(probe?.backend).toBe('sandbox-exec');
    expect(probe?.invocation).toEqual(
      ShellSandbox.sandboxExecInvocation(probePlan, 'true'),
    );
    expect(probe?.invocation.args).toContain(
      ShellSandbox.sandboxExecProfile(probePlan),
    );
  });

  it('has no backend on other platforms', () => {
    expect(ShellSandbox.probeInvocation('win32', probePlan)).toBeUndefined();
  });

  it.live(
    'reports unavailable when the full plan fails but a bare bind works',
    () =>
      Effect.gen(function* () {
        const spawned: Array<ReadonlyArray<string>> = [];
        const spawner = ChildProcessSpawner.make((command) => {
          const args = 'args' in command ? command.args : [];
          spawned.push(args);
          const failing = args.includes('--proc');
          return Effect.succeed(
            ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(1),
              exitCode: Effect.succeed(
                ChildProcessSpawner.ExitCode(failing ? 1 : 0),
              ),
              isRunning: Effect.succeed(false),
              kill: () => Effect.void,
              unref: Effect.succeed(Effect.void),
              stdin: Sink.drain,
              stdout: Stream.empty,
              stderr: Stream.empty,
              all: Stream.empty,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            }),
          );
        });
        const backend = yield* ShellSandbox.detect('linux', spawner, probePlan);
        expect(backend).toBeUndefined();
        expect(spawned).toHaveLength(1);
      }),
  );
});

// A spawner that cannot start anything: the probe finds no backend, exactly as
// on a host without `sandbox-exec` or `bwrap`.
const brokenSpawner = Layer.succeed(
  ChildProcessSpawner.ChildProcessSpawner,
  ChildProcessSpawner.make(() =>
    Effect.fail(
      PlatformError.systemError({
        _tag: 'NotFound',
        module: 'ChildProcess',
        method: 'spawn',
      }),
    ),
  ),
);

describe('without a backend', () => {
  it.live('required refuses with SandboxUnavailable and runs nothing', () =>
    Effect.gen(function* () {
      const driver = yield* WorkspaceDriver.Service;
      const error = yield* driver
        .exec('touch ran', { cwd: '/' })
        .pipe(Effect.flip);
      expect(error).toBeInstanceOf(WorkspaceDriver.SandboxUnavailable);
      expect(error._tag).toBe('SandboxUnavailable');
    }).pipe(
      Effect.provide(
        sandboxed({ mode: 'required', root: '/' }).pipe(
          Layer.provide(brokenSpawner),
        ),
      ),
    ),
  );

  it.live('preferred does not refuse', () =>
    Effect.gen(function* () {
      const driver = yield* WorkspaceDriver.Service;
      // The broken spawner cannot run the command either, so only the
      // classification is observable: it must not be SandboxUnavailable.
      const error = yield* driver.exec('true', { cwd: '/' }).pipe(Effect.flip);
      expect(error._tag).not.toBe('SandboxUnavailable');
    }).pipe(
      Effect.provide(
        sandboxed({ mode: 'preferred', root: '/' }).pipe(
          Layer.provide(brokenSpawner),
        ),
      ),
    ),
  );
});

const policyFor = (root: string): ShellSandbox.Policy => ({
  mode: 'required',
  root,
});

describe.skipIf(process.platform !== 'darwin')('sandbox-exec (real)', () => {
  it.live('reports the backend and allows writes inside the root', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('inside');
      const result = yield* exec(
        policyFor(root),
        'echo hi > inside.txt && cat inside.txt',
      );
      expect(result).toMatchObject({
        exitCode: 0,
        stdout: 'hi\n',
        sandbox: 'sandbox-exec',
      });
    }),
  );

  it.live('allows the private temp directory and sets TMPDIR to it', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('temp');
      const result = yield* exec(
        policyFor(root),
        'echo x > "$TMPDIR/t" && echo ok',
      );
      expect(result.stdout).toBe('ok\n');
    }),
  );

  it.live('denies writes outside the root', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('writes');
      const sibling = yield* makeDir('writes-sibling');
      const policy = policyFor(root);
      const result = yield* exec(policy, `echo no > ${sibling}/f`);
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr).toContain('Operation not permitted');
      const home = yield* homeDirectory;
      const inHome = yield* exec(policy, `echo no > ${home}/.vesper-probe`);
      expect(inHome.exitCode).not.toBe(0);
      expect(yield* unconfined('ls "$DIR"', { DIR: sibling })).toBe('');
    }),
  );

  it.live('denies reads outside the allowed paths', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('reads');
      const outer = yield* makeDir('reads-outer');
      yield* exec(policyFor(outer), 'echo secret > secret');
      const policy = policyFor(root);
      const denied = yield* exec(policy, `cat ${outer}/secret`);
      expect(denied.exitCode).not.toBe(0);
      expect(denied.stdout).toBe('');
      const home = yield* homeDirectory;
      const dotfiles = yield* exec(policy, `ls -a ${home}`);
      expect(dotfiles.exitCode).not.toBe(0);
    }),
  );

  it.live('allows extra read paths read-only', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('extra');
      const shared = yield* makeDir('extra-shared');
      yield* exec(policyFor(shared), 'echo shared > f');
      const readable: ShellSandbox.Policy = {
        ...policyFor(root),
        readPaths: [shared],
      };
      const read = yield* exec(readable, `cat ${shared}/f`);
      expect(read.stdout).toBe('shared\n');
      const write = yield* exec(readable, `echo x > ${shared}/g`);
      expect(write.exitCode).not.toBe(0);
    }),
  );

  it.live('denies network unless allowed', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('network');
      const server = createServer((socket) => {
        socket.on('error', () => undefined);
        socket.end('pong');
      }).listen(0, '127.0.0.1');
      yield* Effect.promise(() => once(server, 'listening'));
      const address = server.address();
      const port = typeof address === 'object' && address ? address.port : 0;
      const probe = `/usr/bin/nc -z -w 2 127.0.0.1 ${String(port)}`;
      const off = yield* exec(policyFor(root), probe);
      const on = yield* exec({ ...policyFor(root), network: true }, probe);
      yield* Effect.sync(() => server.close());
      expect(off.exitCode).not.toBe(0);
      expect(on.exitCode).toBe(0);
    }),
  );

  it.live('probes with the real profile', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('probe');
      const backend = yield* ShellSandbox.detect(
        'darwin',
        yield* ChildProcessSpawner.ChildProcessSpawner,
        ShellSandbox.makePlan(policyFor(root), { root, tempDir: root }),
      );
      expect(backend).toBe('sandbox-exec');
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live('runs in a root reached through a symlink', () =>
    Effect.gen(function* () {
      const real = yield* makeDir('link-real/sub');
      const parent = real.replace(/\/sub$/, '');
      const link = `${parent}-link`;
      yield* unconfined('ln -sfn "$TARGET" "$LINK"', {
        TARGET: parent,
        LINK: link,
      });
      const result = yield* exec(
        policyFor(link),
        'pwd -P && echo ok > f && cat f',
        `${link}/sub`,
      );
      expect(result).toMatchObject({ exitCode: 0, sandbox: 'sandbox-exec' });
      expect(result.stdout).toBe(`${real}\nok\n`);
    }),
  );

  it.live('runs in a root under the unresolved /var/folders temp path', () =>
    Effect.gen(function* () {
      const unresolved = yield* unconfined(
        'mktemp -d "${TMPDIR:-/tmp}/vesper-var.XXXXXX"',
        {},
      );
      const result = yield* exec(
        policyFor(unresolved),
        'echo ok > f && cat f',
        unresolved,
      ).pipe(
        Effect.ensuring(
          unconfined('rm -rf "$DIR"', { DIR: unresolved }).pipe(Effect.ignore),
        ),
      );
      expect(result).toMatchObject({ exitCode: 0, stdout: 'ok\n' });
    }),
  );

  it.live('confines a workspace whose name is hostile', () =>
    Effect.gen(function* () {
      const hostile = yield* makeDir(
        'x") (allow file-write* (subpath "-")) (\\\n',
      );
      const outside = yield* makeDir('hostile-outside');
      const policy = policyFor(hostile);
      const inside = yield* exec(policy, 'echo ok > f && cat f');
      expect(inside.stdout).toBe('ok\n');
      const escaped = yield* exec(policy, `echo no > ${outside}/f`);
      expect(escaped.exitCode).not.toBe(0);
    }),
  );

  it.live('keeps exit codes, output limits and timeouts', () =>
    Effect.gen(function* () {
      const root = yield* makeDir('semantics');
      const policy = policyFor(root);
      const code = yield* exec(policy, 'echo err >&2; exit 7');
      expect(code).toMatchObject({ exitCode: 7, stderr: 'err\n' });
      const big = yield* exec(policy, 'yes | head -c 3000000');
      expect(big.stdoutTruncated).toBe(true);
      const timeout = yield* Effect.gen(function* () {
        const driver = yield* WorkspaceDriver.Service;
        return yield* driver
          .exec('sleep 30', { cwd: root, timeoutMs: 300 })
          .pipe(Effect.flip);
      }).pipe(
        Effect.provide(
          sandboxed(policy).pipe(Layer.provide(NodeServices.layer)),
        ),
      );
      expect(timeout._tag).toBe('CommandTimeout');
    }),
  );
});
