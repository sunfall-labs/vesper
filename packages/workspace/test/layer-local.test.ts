import { chmodSync, mkdtempSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import { afterAll, describe, expect, it } from 'vitest';

import { WorkspaceDriver } from '../src/driver.js';
import { layer as localLayer } from '../src/layer-local.js';
import { workspaceContract } from '../src/workspace-contract.js';

// The local driver against a real temp directory and a real shell. Nothing is
// faked: a stubbed `node:fs` would pass every case here and still get the
// `errno` classification wrong, which is most of what this file is for.

const root = mkdtempSync(join(tmpdir(), 'ai-workspace-'));

const layer = localLayer.pipe(Layer.provide(NodeServices.layer));

afterAll(() => {
  rmSync(root, { recursive: true, force: true });
});

workspaceContract('local', { layer, root });

const run = <A>(
  effect: Effect.Effect<A, unknown, WorkspaceDriver.Service>,
): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(layer)) as Effect.Effect<A>);

describe('local driver specifics', () => {
  it('stat reports a symlink as a symlink, not as its target', async () => {
    const stat = await run(
      Effect.gen(function* () {
        const driver = yield* WorkspaceDriver.Service;
        yield* driver.writeFile(`${root}/link-target`, 'x');
        yield* Effect.sync(() => {
          symlinkSync(`${root}/link-target`, `${root}/link`);
        });
        return yield* driver.stat(`${root}/link`);
      }),
    );

    expect(stat).toMatchObject({ isSymbolicLink: true, isFile: false });
  });

  // The residual case: not every `errno` deserves its own type, but the ones
  // that do not must still arrive classified rather than as a bare defect.
  it('surfaces an unmodelled errno as WorkspaceFailure carrying its code', async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const driver = yield* WorkspaceDriver.Service;
        yield* driver.mkdir(`${root}/twice`);
        return yield* driver.mkdir(`${root}/twice`).pipe(Effect.result);
      }),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure).toBeInstanceOf(WorkspaceDriver.WorkspaceFailure);
      expect(outcome.failure).toMatchObject({
        operation: 'mkdir',
        code: 'EEXIST',
      });
    }
  });

  // Mode bits do not restrain uid 0, so as root the read succeeds and this
  // case cannot be exercised at all.
  //
  // Skipped loudly rather than with `it.skipIf`, which would drop it in a
  // root CI container with no signal — and a silently skipped test reads
  // exactly like a passing one in the summary. That is how a whole error
  // class stops being covered without anyone noticing. Three signals: a
  // warning on stderr, a reporter annotation, and a skip carrying its reason.
  //
  // Dropping privileges instead would need a second uid to drop *to*, which
  // is not something a unit test can arrange portably.
  it('fails with PermissionDenied on an unreadable file', async (context) => {
    if (process.getuid?.() === 0) {
      const reason =
        'running as uid 0: mode bits do not apply, so PermissionDenied is UNVERIFIED in this run';
      console.warn(
        `[@sunfall/vesper-workspace] SKIPPING permission coverage — ${reason}`,
      );
      await context.annotate(reason, 'warning');
      context.skip(reason);
    }

    const outcome = await run(
      Effect.gen(function* () {
        const driver = yield* WorkspaceDriver.Service;
        yield* driver.writeFile(`${root}/secret`, 'x');
        yield* Effect.sync(() => {
          chmodSync(`${root}/secret`, 0o000);
        });
        return yield* driver.readFile(`${root}/secret`).pipe(Effect.result);
      }),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure).toBeInstanceOf(WorkspaceDriver.PermissionDenied);
      expect(outcome.failure).toMatchObject({ operation: 'readFile' });
    }
  });

  // A command that never starts is a different failure from one that starts
  // and exits non-zero, and the spawner is where that distinction is made.
  it('fails with PathNotFound when the cwd does not exist', async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const driver = yield* WorkspaceDriver.Service;
        return yield* driver
          .exec('echo hi', { cwd: `${root}/no-such-directory` })
          .pipe(Effect.result);
      }),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure).toBeInstanceOf(WorkspaceDriver.PathNotFound);
      expect(outcome.failure).toMatchObject({ operation: 'exec' });
    }
  });

  // Under a shell, an unknown command is the shell's exit code, not a spawn
  // failure — worth pinning, because the two are easy to conflate, and only
  // one of them is a failure here.
  it('reports an unknown command as exit 127, not as a failure', async () => {
    const result = await run(
      Effect.gen(function* () {
        const driver = yield* WorkspaceDriver.Service;
        return yield* driver.exec('definitely-not-a-real-command');
      }),
    );

    expect(result.exitCode).toBe(127);
  });

  // Output larger than a pipe buffer. If the driver awaited the exit before
  // draining stdout, this would deadlock rather than fail — the reason the
  // three reads run concurrently.
  it('collects output larger than a pipe buffer', async () => {
    const result = await run(
      Effect.gen(function* () {
        const driver = yield* WorkspaceDriver.Service;
        return yield* driver.exec(
          'i=0; while [ $i -lt 2000 ]; do echo "0123456789012345678901234567890123456789"; i=$((i+1)); done',
        );
      }),
    );

    expect(result.stdout.length).toBeGreaterThan(80_000);
  });
});
