import { Duration, Effect, Fiber, Layer, Schedule } from 'effect';
import { describe, expect, it } from 'vitest';

import { WorkspaceDriver } from './driver.js';

// The behaviour every WorkspaceDriver must have, expressed once.
//
// Pluggable drivers are only real if they are interchangeable, and they are
// only interchangeable if something holds them to the same contract. The
// cases worth reading are the last three: a non-zero exit must come back as a
// readable result rather than a raised error, and a deadline and an
// interruption must each actually terminate the command. A driver can pass
// everything else while leaking a process on every timeout, and nothing about
// its interface would say so.
//
// It runs against a real substrate — files are written, commands are run —
// because the failures being pinned are the substrate's. It assumes a POSIX
// shell and `/`-separated paths, which every driver in view provides.

export interface ContractOptions<E> {
  readonly layer: Layer.Layer<WorkspaceDriver.Service, E>;
  /**
   * An existing, writable directory inside the workspace. Each case creates its
   * own subdirectory under it and never touches a sibling's.
   */
  readonly root: string;
}

let counter = 0;

export const workspaceContract = <E>(
  name: string,
  options: ContractOptions<E>,
): void => {
  const run = <A>(
    effect: Effect.Effect<A, unknown, WorkspaceDriver.Service>,
  ): Promise<A> =>
    Effect.runPromise(effect.pipe(Effect.provide(options.layer)));

  /** A fresh directory for one case, created through the driver under test. */
  const scratch = Effect.fnUntraced(function* () {
    const driver = yield* WorkspaceDriver.Service;
    counter += 1;
    const path = `${options.root}/contract-${String(Date.now())}-${String(counter)}`;
    yield* driver.mkdir(path, { recursive: true });
    return path;
  });

  /**
   * Poll until `check` succeeds. Used only to observe a command that has
   * started, which no interface here reports — every other wait in this suite
   * is a real deadline being tested.
   */
  const waitUntil = (
    check: Effect.Effect<boolean, unknown, WorkspaceDriver.Service>,
  ): Effect.Effect<void, unknown, WorkspaceDriver.Service> =>
    check.pipe(
      Effect.flatMap((ready) =>
        ready ? Effect.void : Effect.fail('pending' as const),
      ),
      Effect.retry(Schedule.spaced(Duration.millis(25))),
      Effect.timeoutOrElse({
        duration: Duration.seconds(10),
        orElse: () => Effect.die(new Error('waitUntil never became true')),
      }),
    );

  describe(`WorkspaceDriver contract: ${name}`, () => {
    it('round-trips text through writeFile and readFile', async () => {
      const text = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(`${dir}/note.txt`, 'hello workspace');
          return yield* driver.readFile(`${dir}/note.txt`);
        }),
      );

      expect(text).toBe('hello workspace');
    });

    it('round-trips bytes through writeFile and readFileBuffer', async () => {
      const bytes = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(
            `${dir}/blob.bin`,
            Uint8Array.from([0, 1, 2, 253, 254, 255]),
          );
          return yield* driver.readFileBuffer(`${dir}/blob.bin`);
        }),
      );

      expect(Array.from(bytes)).toEqual([0, 1, 2, 253, 254, 255]);
    });

    it('reads text written as bytes and bytes written as text', async () => {
      const result = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(`${dir}/a`, 'héllo');
          const bytes = yield* driver.readFileBuffer(`${dir}/a`);
          yield* driver.writeFile(`${dir}/b`, bytes);
          return yield* driver.readFile(`${dir}/b`);
        }),
      );

      expect(result).toBe('héllo');
    });

    it('stat distinguishes a file from a directory', async () => {
      const [file, directory] = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(`${dir}/f`, 'abc');
          return [
            yield* driver.stat(`${dir}/f`),
            yield* driver.stat(dir),
          ] as const;
        }),
      );

      expect(file).toMatchObject({ isFile: true, isDirectory: false });
      expect(directory).toMatchObject({ isFile: false, isDirectory: true });
      // Optional by contract — a driver may omit it, but must not lie.
      if (file.size !== undefined) {
        expect(file.size).toBe(3);
      }
    });

    it('readdir lists the entries of a directory', async () => {
      const entries = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(`${dir}/one`, '1');
          yield* driver.writeFile(`${dir}/two`, '2');
          yield* driver.mkdir(`${dir}/three`);
          return yield* driver.readdir(dir);
        }),
      );

      expect([...entries].sort()).toEqual(['one', 'three', 'two']);
    });

    it('exists reports presence and absence', async () => {
      const [present, absent] = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(`${dir}/here`, '');
          return [
            yield* driver.exists(`${dir}/here`),
            yield* driver.exists(`${dir}/gone`),
          ] as const;
        }),
      );

      expect(present).toBe(true);
      expect(absent).toBe(false);
    });

    it('mkdir recursive creates missing parents and tolerates an existing directory', async () => {
      const exists = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.mkdir(`${dir}/a/b/c`, { recursive: true });
          yield* driver.mkdir(`${dir}/a/b/c`, { recursive: true });
          return yield* driver.exists(`${dir}/a/b/c`);
        }),
      );

      expect(exists).toBe(true);
    });

    it('rm deletes a file and, recursively, a populated directory', async () => {
      const [file, tree] = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(`${dir}/f`, 'x');
          yield* driver.mkdir(`${dir}/tree/inner`, { recursive: true });
          yield* driver.writeFile(`${dir}/tree/inner/f`, 'x');

          yield* driver.rm(`${dir}/f`);
          yield* driver.rm(`${dir}/tree`, { recursive: true });

          return [
            yield* driver.exists(`${dir}/f`),
            yield* driver.exists(`${dir}/tree`),
          ] as const;
        }),
      );

      expect(file).toBe(false);
      expect(tree).toBe(false);
    });

    it('rm force succeeds on a missing path', async () => {
      await expect(
        run(
          Effect.gen(function* () {
            const driver = yield* WorkspaceDriver.Service;
            const dir = yield* scratch();
            yield* driver.rm(`${dir}/never-existed`, { force: true });
          }),
        ),
      ).resolves.toBeUndefined();
    });

    // The typed-error cases. A driver that collapsed these into one generic
    // failure would compile, and every caller wanting to distinguish "wrong
    // path" from "not allowed" would be reduced to matching on a message.
    it('fails with PathNotFound when reading a missing file', async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          return yield* driver.readFile(`${dir}/absent`).pipe(Effect.result);
        }),
      );

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toBeInstanceOf(WorkspaceDriver.PathNotFound);
        expect(outcome.failure).toMatchObject({ operation: 'readFile' });
      }
    });

    it('fails with PathNotFound when removing a missing path without force', async () => {
      const outcome = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          return yield* driver.rm(`${dir}/absent`).pipe(Effect.result);
        }),
      );

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toBeInstanceOf(WorkspaceDriver.PathNotFound);
        expect(outcome.failure).toMatchObject({ operation: 'rm' });
      }
    });

    it('exec captures stdout and stderr separately', async () => {
      const result = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          return yield* driver.exec('echo out; echo err 1>&2');
        }),
      );

      expect(result.stdout.trim()).toBe('out');
      expect(result.stderr.trim()).toBe('err');
      expect(result.exitCode).toBe(0);
    });

    it('exec runs in the requested cwd', async () => {
      const stdout = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();
          yield* driver.writeFile(`${dir}/marker-file`, '');
          const result = yield* driver.exec('ls', { cwd: dir });
          return result.stdout;
        }),
      );

      // Compared by listing rather than by `pwd`: a driver may hand back a
      // resolved path (`/private/var/...` for `/var/...` on macOS) and the
      // test would be asserting path normalization, not cwd.
      expect(stdout).toContain('marker-file');
    });

    it('exec adds env over the ambient environment rather than replacing it', async () => {
      const result = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          return yield* driver.exec(
            'echo "$SANDBOX_CONTRACT_VAR:${PATH:+set}"',
            {
              env: { SANDBOX_CONTRACT_VAR: 'provided' },
            },
          );
        }),
      );

      expect(result.stdout.trim()).toBe('provided:set');
    });

    // A non-zero exit is an outcome to read, not an error to catch: `grep`
    // exits 1 for no match and `git diff --quiet` exits 1 for "there are
    // changes". A driver that raised here would put `Effect.catchTag` in the
    // middle of ordinary control flow, and would have to reconstruct stdout
    // and stderr onto the error to stay usable.
    it('exec returns a non-zero exit as a result, with its output intact', async () => {
      const result = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          return yield* driver.exec('echo partial; echo bad 1>&2; exit 3');
        }),
      );

      expect(result.exitCode).toBe(3);
      expect(result.stdout.trim()).toBe('partial');
      expect(result.stderr.trim()).toBe('bad');
    });

    it('exec fails with CommandTimeout and terminates the command', async () => {
      const [outcome, started, finished] = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();

          const outcome = yield* driver
            .exec(
              `(printf x > '${dir}/child-started'; sleep 2; printf x > '${dir}/child-done') & wait`,
              {
                timeoutMs: 200,
              },
            )
            .pipe(Effect.result);

          // Past when the command would have written `done` had it survived.
          yield* Effect.sleep(Duration.seconds(3));

          return [
            outcome,
            yield* driver.exists(`${dir}/child-started`),
            yield* driver.exists(`${dir}/child-done`),
          ] as const;
        }),
      );

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toBeInstanceOf(WorkspaceDriver.CommandTimeout);
        expect(outcome.failure).toMatchObject({ timeoutMs: 200 });
      }
      expect(started).toBe(true);
      expect(finished).toBe(false);
    });

    it('exec terminates the command when the caller is interrupted', async () => {
      const finished = await run(
        Effect.gen(function* () {
          const driver = yield* WorkspaceDriver.Service;
          const dir = yield* scratch();

          const fiber = yield* driver
            .exec(
              `(printf x > '${dir}/child-started'; sleep 2; printf x > '${dir}/child-done') & wait`,
            )
            .pipe(Effect.forkChild);

          yield* waitUntil(driver.exists(`${dir}/child-started`));
          // `Fiber.interrupt` waits for finalizers, so the kill has happened
          // by the time this returns.
          yield* Fiber.interrupt(fiber);

          yield* Effect.sleep(Duration.seconds(3));

          return yield* driver.exists(`${dir}/child-done`);
        }),
      );

      expect(finished).toBe(false);
    });
  });
};

export * as WorkspaceContract from './workspace-contract.js';
