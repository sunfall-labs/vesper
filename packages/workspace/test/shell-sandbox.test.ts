import { once } from 'node:events';
import { createServer } from 'node:net';

import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, describe, expect, it } from '@effect/vitest';
import { Effect, Layer, PlatformError } from 'effect';
import { ChildProcessSpawner } from 'effect/process';

import { WorkspaceDriver } from '../src/driver.js';
import { sandboxed, unrestricted } from '../src/layer-local.js';
import type { ShellSandbox } from '../src/shell-sandbox.js';

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
