import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, describe, expect, it } from '@effect/vitest';
import { Effect, Layer, PlatformError, Schema, Stream } from 'effect';
import { ChildProcessSpawner } from 'effect/process';
import { AiError, Tool as ToolNamespace } from 'effect/ai';
import type { Tool, Toolkit } from 'effect/ai';

import { WorkspaceDriver } from '../src/driver.js';
import {
  layer as localLayer,
  sandboxed as sandboxedLayer,
} from '../src/layer-local.js';
import { WorkspaceTools } from '../src/tools.js';

// The toolkit against a real local workspace. Nothing is faked: the failures
// worth pinning here — `EISDIR`, `ENOTDIR`, a killed command, a file that is
// not UTF-8 — are the substrate's, and a stubbed driver would let every one of
// them pass while the real classification was wrong.
//
// Every case goes through `Toolkit.handle` rather than calling a handler
// directly, so parameter validation, the failure mode, and result encoding are
// in the path being tested. A handler asserted in isolation can be correct
// while the tool it is attached to advertises different parameters.

const root = mkdtempSync(join(tmpdir(), 'ai-workspace-tools-'));

afterAll(() => {
  rmSync(root, { recursive: true, force: true });
});

let counter = 0;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/** A fresh directory, used as the workspace root for one case. */
const workspace = (): string => {
  counter += 1;
  const directory = join(root, `case-${String(counter)}`);
  mkdirSync(directory, { recursive: true });
  return directory;
};

type Tools = Toolkit.Tools<typeof WorkspaceTools.toolkit>;

type Outcome =
  | { readonly kind: 'ok'; readonly value: Record<string, unknown> }
  | {
      readonly kind: 'tool-failure';
      readonly tag: string;
      readonly error: Record<string, unknown>;
    }
  | { readonly kind: 'call-error'; readonly error: unknown };

/**
 * Invoke one tool and classify what came back.
 *
 * Three outcomes rather than two, because `failureMode: 'return'` carries both
 * declared handler failures and typed parameter-validation failures on the
 * stream, while framework failures still fail the call itself.
 */
const call = <Name extends keyof Tools>(
  directory: string,
  name: Name,
  params: Tool.Parameters<Tools[Name]>,
  policy: Layer.Layer<WorkspaceTools.CommandPolicy> = WorkspaceTools.shellEnabledCommandPolicyLayer,
  driverLayer: Layer.Layer<WorkspaceDriver.Service> = localLayer.pipe(
    Layer.provide(NodeServices.layer),
  ),
): Effect.Effect<Outcome> =>
  Effect.gen(function* () {
    const kit = yield* WorkspaceTools.toolkit;
    const stream = yield* kit.handle(name, params);
    const chunks = yield* Stream.runCollect(stream);
    const last = chunks.at(-1);
    if (last === undefined) {
      return {
        kind: 'call-error',
        error: new Error('tool returned no result'),
      };
    }
    if (last.isFailure) {
      if (!isRecord(last.result)) {
        return {
          kind: 'call-error',
          error: new Error('tool returned a non-object failure'),
        };
      }
      return {
        kind: 'tool-failure',
        tag: String(
          '_tag' in last.result ? last.result['_tag'] : 'UnknownToolFailure',
        ),
        error: last.result,
      } as const;
    }
    if (!isRecord(last.result)) {
      return {
        kind: 'call-error',
        error: new Error('tool returned a non-object result'),
      };
    }
    return { kind: 'ok', value: last.result } as const;
  }).pipe(
    Effect.provide(WorkspaceTools.layer),
    Effect.provide(
      Layer.mergeAll(
        WorkspaceTools.rootLayer(directory),
        policy,
        WorkspaceTools.defaultFilesystemPolicyLayer,
        driverLayer,
      ),
    ),
    Effect.catchCause((cause) =>
      Effect.succeed({ kind: 'call-error', error: cause } as const),
    ),
  ) as Effect.Effect<Outcome>;

/** Narrow to a successful result, failing the test with the outcome if not. */
const expectOk = (outcome: Outcome): Record<string, unknown> => {
  expect(outcome.kind).toBe('ok');
  return outcome.kind === 'ok' ? outcome.value : {};
};

/** Narrow to a declared tool failure carrying the expected tag. */
const expectFailure = (
  outcome: Outcome,
  tag: string,
): Record<string, unknown> => {
  expect(
    outcome.kind === 'tool-failure' ? outcome.tag : JSON.stringify(outcome),
  ).toBe(tag);
  return outcome.kind === 'tool-failure' ? outcome.error : {};
};

// ------------------------------------------------------------------- typing
//
// The claim this package exists to make: a tool that touches a workspace puts
// the workspace in the requirement channel, so an application that never wired
// one does not compile. These assertions fail at `tsc`, not at runtime.
//
// Spelled per tool rather than over `Tools[keyof Tools]`. The union form is
// vacuous against the mutation that matters — dropping `dependencies` from one
// tool leaves the other five contributing the same services, and the assertion
// still passes.

/** Union membership. The tuple wrapper stops `extends` distributing. */
type Has<Member, Union> = [Member] extends [Union] ? 'yes' : 'no';
/** `0 extends 1 & T` holds precisely when `T` is `any`. */
type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';

type Services<Name extends keyof Tools> = Tool.HandlerServices<Tools[Name]>;

const _readNotAny: IsAny<Services<'read_file'>> = 'not-any';

const _readNeedsDriver: Has<
  WorkspaceDriver.Service,
  Services<'read_file'>
> = 'yes';
const _readNeedsRoot: Has<WorkspaceTools.Root, Services<'read_file'>> = 'yes';
const _readNeedsFilesystemPolicy: Has<
  WorkspaceTools.FilesystemPolicy,
  Services<'read_file'>
> = 'yes';

const _writeNeedsDriver: Has<
  WorkspaceDriver.Service,
  Services<'write_file'>
> = 'yes';
const _writeNeedsRoot: Has<WorkspaceTools.Root, Services<'write_file'>> = 'yes';

const _editNeedsDriver: Has<
  WorkspaceDriver.Service,
  Services<'edit_file'>
> = 'yes';
const _editNeedsRoot: Has<WorkspaceTools.Root, Services<'edit_file'>> = 'yes';

const _listNeedsDriver: Has<
  WorkspaceDriver.Service,
  Services<'list_files'>
> = 'yes';
const _listNeedsRoot: Has<WorkspaceTools.Root, Services<'list_files'>> = 'yes';

const _searchNeedsDriver: Has<
  WorkspaceDriver.Service,
  Services<'search_files'>
> = 'yes';
const _searchNeedsRoot: Has<
  WorkspaceTools.Root,
  Services<'search_files'>
> = 'yes';

const _runNeedsDriver: Has<
  WorkspaceDriver.Service,
  Services<'run_shell'>
> = 'yes';
const _runNeedsRoot: Has<WorkspaceTools.Root, Services<'run_shell'>> = 'yes';
const _runNeedsPolicy: Has<
  WorkspaceTools.CommandPolicy,
  Services<'run_shell'>
> = 'yes';
const _runNeedsFilesystemPolicy: Has<
  WorkspaceTools.FilesystemPolicy,
  Services<'run_shell'>
> = 'yes';

// ...and the handler layer discharges the handlers and *not* the workspace.
// If it provided either, wiring an agent would silently stop requiring one.
type LayerOutput =
  typeof WorkspaceTools.layer extends Layer.Layer<infer Out, infer _E, infer _R>
    ? Out
    : never;

const _layerKeepsDriverRequired: Has<WorkspaceDriver.Service, LayerOutput> =
  'no';
const _layerKeepsRootRequired: Has<WorkspaceTools.Root, LayerOutput> = 'no';
const _layerProvidesHandlers: Has<Tool.HandlersFor<Tools>, LayerOutput> = 'yes';

const read = (directory: string, path: string) =>
  readFileSync(join(directory, path), 'utf8');

/** One workspace holding a scenario's files. */
const seeded = (files: Record<string, string | Buffer>): string => {
  const directory = workspace();
  for (const [path, content] of Object.entries(files)) {
    mkdirSync(join(directory, path, '..'), { recursive: true });
    writeFileSync(join(directory, path), content);
  }
  return directory;
};

const typedEntries = (value: Record<string, unknown>) =>
  Schema.decodeUnknownSync(
    Schema.Array(Schema.Struct({ path: Schema.String, type: Schema.String })),
  )(value['entries']);

const names = (value: Record<string, unknown>, key: string) =>
  Schema.decodeUnknownSync(
    Schema.Array(Schema.Struct({ path: Schema.String })),
  )(value[key]).map((entry) => entry.path);

describe('apply_patch', () => {
  it.live('applies add, update, move, and delete operations', () =>
    Effect.gen(function* () {
      const directory = seeded({
        'src/original.ts': 'const before = 1;\n',
        'src/delete.ts': 'remove me\n',
      });
      const result = expectOk(
        yield* call(directory, 'apply_patch', {
          patchText: [
            '*** Begin Patch',
            '*** Add File: added.txt',
            '+hello',
            '*** Update File: src/original.ts',
            '*** Move to: src/moved.ts',
            '@@',
            '-const before = 1;',
            '+const after = 2;',
            '*** Delete File: src/delete.ts',
            '*** End Patch',
          ].join('\n'),
        }),
      );
      expect(result['filesApplied']).toBe(3);
      expect(read(directory, 'src/moved.ts')).toBe('const after = 2;\n');
      expect(existsSync(join(directory, 'src/original.ts'))).toBe(false);
      expect(existsSync(join(directory, 'src/delete.ts'))).toBe(false);
      expect(read(directory, 'added.txt')).toBe('hello');
    }),
  );
});

describe('read_file', () => {
  it.live('reads, windows, and bounds text honestly', () =>
    Effect.gen(function* () {
      const lines = Array.from(
        { length: 10 },
        (_, i) => `line-${String(i + 1)}`,
      );
      const directory = seeded({
        'note.txt': 'one\ntwo\nthree',
        'utf8.txt': 'héllo 😀 €',
        'lines.txt': lines.join('\n'),
        'big.txt': Array.from({ length: 1000 }, () => 'x'.repeat(200)).join(
          '\n',
        ),
        'huge.txt': 'x'.repeat(2 * 1024 * 1024 + 1),
      });

      expect(
        expectOk(yield* call(directory, 'read_file', { path: 'note.txt' })),
      ).toMatchObject({
        path: 'note.txt',
        content: 'one\ntwo\nthree',
        firstLine: 1,
        lineCount: 3,
        totalLines: 3,
        truncated: false,
        truncatedBy: null,
      });
      expect(
        expectOk(yield* call(directory, 'read_file', { path: 'utf8.txt' }))[
          'content'
        ],
      ).toBe('héllo 😀 €');

      // A window that fits its budget is still not the whole file.
      expect(
        expectOk(
          yield* call(directory, 'read_file', {
            path: 'lines.txt',
            offset: 3,
            limit: 2,
          }),
        ),
      ).toMatchObject({
        content: 'line-3\nline-4',
        firstLine: 3,
        lineCount: 2,
        totalLines: 10,
        truncated: true,
        truncatedBy: 'lines',
      });
      expect(
        expectOk(
          yield* call(directory, 'read_file', {
            path: 'note.txt',
            offset: 100,
            limit: 5,
          }),
        ),
      ).toMatchObject({
        content: '',
        firstLine: 4,
        lineCount: 0,
        totalLines: 3,
        truncated: true,
      });

      const big = expectOk(
        yield* call(directory, 'read_file', { path: 'big.txt' }),
      );
      expect(big).toMatchObject({
        truncated: true,
        truncatedBy: 'bytes',
        totalLines: 1000,
      });
      expect(String(big['content']).length).toBeLessThanOrEqual(51_200);
      expectFailure(
        yield* call(directory, 'read_file', { path: 'huge.txt' }),
        'FileTooLarge',
      );
    }),
  );

  it.live('classifies missing, directory, and binary targets', () =>
    Effect.gen(function* () {
      const directory = seeded({
        'sub/.keep': '',
        'image.png': Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x00, 0x1a]),
        // Latin-1 "café": no NUL, so only strict decoding catches it.
        'latin1.txt': Buffer.from([0x63, 0x61, 0x66, 0xe9]),
      });
      const missing = expectFailure(
        yield* call(directory, 'read_file', { path: 'absent.txt' }),
        'FileNotFound',
      );
      expect(missing['path']).toBe('absent.txt');
      expect(JSON.stringify(missing)).not.toContain(directory);
      expectFailure(
        yield* call(directory, 'read_file', { path: 'sub' }),
        'NotAFile',
      );
      expect(
        expectFailure(
          yield* call(directory, 'read_file', { path: 'image.png' }),
          'BinaryContent',
        )['reason'],
      ).toBe('nul-byte');
      expect(
        expectFailure(
          yield* call(directory, 'read_file', { path: 'latin1.txt' }),
          'BinaryContent',
        )['reason'],
      ).toBe('invalid-utf8');
    }),
  );
});

describe('write_file', () => {
  it.live('creates, replaces, and counts bytes without clobbering', () =>
    Effect.gen(function* () {
      const directory = seeded({ 'f.txt': 'old', 'sub/.keep': '' });
      expect(
        expectOk(
          yield* call(directory, 'write_file', {
            path: 'a/b/c.txt',
            content: '😀é',
          }),
        ),
      ).toMatchObject({ path: 'a/b/c.txt', bytesWritten: 6, created: true });
      expect(read(directory, 'a/b/c.txt')).toBe('😀é');

      expect(
        expectOk(
          yield* call(directory, 'write_file', {
            path: 'f.txt',
            content: 'new',
          }),
        )['created'],
      ).toBe(false);
      expect(read(directory, 'f.txt')).toBe('new');

      expectFailure(
        yield* call(directory, 'write_file', { path: 'sub', content: 'x' }),
        'NotAFile',
      );
      expectFailure(
        yield* call(directory, 'write_file', {
          path: 'too-large.txt',
          content: 'x'.repeat(WorkspaceTools.MAX_WRITABLE_BYTES + 1),
        }),
        'FileTooLarge',
      );
      expect(existsSync(join(directory, 'too-large.txt'))).toBe(false);
    }),
  );

  it.live('does not treat an unrelated stat failure as a missing file', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const statFails = Layer.effect(
        WorkspaceDriver.Service,
        Effect.map(WorkspaceDriver.Service, (driver) => ({
          ...driver,
          stat: (path: string) =>
            Effect.fail(
              new WorkspaceDriver.WorkspaceFailure({
                operation: 'stat',
                path,
                code: 'EIO',
                cause: new Error('probe failed'),
              }),
            ),
        })),
      ).pipe(Layer.provide(localLayer.pipe(Layer.provide(NodeServices.layer))));
      expectFailure(
        yield* call(
          directory,
          'write_file',
          { path: 'new.txt', content: 'must not write' },
          WorkspaceTools.shellEnabledCommandPolicyLayer,
          statFails,
        ),
        'WorkspaceUnavailable',
      );
      expect(existsSync(join(directory, 'new.txt'))).toBe(false);
    }),
  );
});

describe('edit_file', () => {
  it.live('replaces exactly what it is told to and nothing else', () =>
    Effect.gen(function* () {
      const directory = seeded({
        'f.txt': 'const a = 1;\nconst b = 2;\n',
        'many.txt': 'x = 1\ny = 1\nz = 1\n',
        'token.txt': 'TOKEN',
        'blob.bin': Buffer.from([1, 0, 2, 3]),
      });
      expect(
        expectOk(
          yield* call(directory, 'edit_file', {
            path: 'f.txt',
            oldText: 'const b = 2;',
            newText: 'const b = 3;',
          }),
        ),
      ).toMatchObject({ path: 'f.txt', replacements: 1 });
      expect(read(directory, 'f.txt')).toBe('const a = 1;\nconst b = 3;\n');

      const missing = expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: 'not here',
          newText: 'x',
        }),
        'EditTargetMissing',
      );
      expect(JSON.stringify(missing)).not.toContain(directory);
      expect(read(directory, 'f.txt')).toBe('const a = 1;\nconst b = 3;\n');

      expect(
        expectFailure(
          yield* call(directory, 'edit_file', {
            path: 'many.txt',
            oldText: '= 1',
            newText: '= 2',
          }),
          'EditTargetAmbiguous',
        )['occurrences'],
      ).toBe(3);
      expect(read(directory, 'many.txt')).toBe('x = 1\ny = 1\nz = 1\n');
      expect(
        expectOk(
          yield* call(directory, 'edit_file', {
            path: 'many.txt',
            oldText: '= 1',
            newText: '= 2',
            replaceAll: true,
          }),
        )['replacements'],
      ).toBe(3);
      expect(read(directory, 'many.txt')).toBe('x = 2\ny = 2\nz = 2\n');

      // `String.replace` would expand `$&` to the matched text.
      expectOk(
        yield* call(directory, 'edit_file', {
          path: 'token.txt',
          oldText: 'TOKEN',
          newText: 'cost: $& and $1',
        }),
      );
      expect(read(directory, 'token.txt')).toBe('cost: $& and $1');

      expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'blob.bin',
          oldText: 'a',
          newText: 'x',
        }),
        'BinaryContent',
      );
      expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'absent.txt',
          oldText: 'a',
          newText: 'b',
        }),
        'FileNotFound',
      );
      expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: 'const a',
          newText: 'x'.repeat(WorkspaceTools.MAX_WRITABLE_BYTES + 1),
        }),
        'FileTooLarge',
      );
      expect(read(directory, 'f.txt')).toBe('const a = 1;\nconst b = 3;\n');

      // An empty target matches everywhere; the schema refuses it and the
      // refusal comes back to the model as a failed result.
      const empty = yield* call(directory, 'edit_file', {
        path: 'f.txt',
        oldText: '',
        newText: 'x',
      });
      if (empty.kind !== 'tool-failure') {
        throw new Error('expected parameter validation to return a failure');
      }
      const failure = yield* Schema.decodeUnknownEffect(AiError.AiError)(
        empty.error,
      );
      expect(failure.reason).toMatchObject({
        _tag: 'ToolParameterValidationError',
        toolName: 'edit_file',
      });
    }),
  );

  it.live('serializes concurrent edits so one stale update is rejected', () =>
    Effect.gen(function* () {
      const directory = seeded({ 'f.txt': 'TOKEN' });
      const edit = (newText: string) =>
        call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: 'TOKEN',
          newText,
        });
      const outcomes = yield* Effect.all([edit('first'), edit('second')], {
        concurrency: 'unbounded',
      });
      expect(outcomes.filter((outcome) => outcome.kind === 'ok')).toHaveLength(
        1,
      );
      expect(
        outcomes.filter(
          (outcome) =>
            outcome.kind === 'tool-failure' &&
            outcome.tag === 'EditTargetMissing',
        ),
      ).toHaveLength(1);
      expect(['first', 'second']).toContain(read(directory, 'f.txt'));
    }),
  );
});

describe('list_files', () => {
  it.live('walks, filters, caps, and reports what it declined to enter', () =>
    Effect.gen(function* () {
      const directory = seeded({
        'README.md': '',
        'src/a.ts': '',
        'src/deep/b.ts': '',
        'src/deep/c.txt': '',
        'node_modules/pkg/index.js': '',
        'real/inside.txt': '',
      });
      symlinkSync(join(directory, 'real'), join(directory, 'link'));

      const all = expectOk(yield* call(directory, 'list_files', {}));
      expect(new Set(names(all, 'entries'))).toEqual(
        new Set([
          'README.md',
          'node_modules',
          'src',
          'src/a.ts',
          'src/deep',
          'src/deep/b.ts',
          'src/deep/c.txt',
          'real',
          'real/inside.txt',
          'link',
        ]),
      );
      expect(all['ignoredDirectories']).toEqual(['node_modules']);
      // A symlink is listed as one and never walked through.
      expect(
        typedEntries(all).find((entry) => entry.path === 'link')?.type,
      ).toBe('symlink');

      expect(
        new Set(
          names(
            expectOk(
              yield* call(directory, 'list_files', { pattern: '**/*.ts' }),
            ),
            'entries',
          ),
        ),
      ).toEqual(new Set(['src/a.ts', 'src/deep/b.ts']));
      const sub = expectOk(
        yield* call(directory, 'list_files', { path: 'src/deep' }),
      );
      expect(sub['directory']).toBe('src/deep');
      expect(new Set(names(sub, 'entries'))).toEqual(
        new Set(['b.ts', 'c.txt']),
      );
      const capped = expectOk(
        yield* call(directory, 'list_files', { limit: 2 }),
      );
      expect(names(capped, 'entries')).toHaveLength(2);
      expect(capped['truncated']).toBe(true);

      expectFailure(
        yield* call(directory, 'list_files', { pattern: '[z-a]' }),
        'InvalidPattern',
      );
      expectFailure(
        yield* call(directory, 'list_files', { path: 'absent' }),
        'FileNotFound',
      );
      expectFailure(
        yield* call(directory, 'list_files', { path: 'README.md' }),
        'NotADirectory',
      );
    }),
  );
});

describe('search_files', () => {
  it.live('finds lines, narrows, and reports everything it skipped', () =>
    Effect.gen(function* () {
      const directory = seeded({
        'src/a.ts': 'const needle = 1;\nother\n',
        'src/b.ts': 'no match here\n',
        'notes.md': 'needle in prose\n',
        'upper.txt': 'NEEDLE\n',
        'blob.bin': Buffer.from([1, 0, 2, 3]),
        'minified.js': `needle${'x'.repeat(20_000)}`,
        'many.txt': Array.from({ length: 20 }, () => 'needle').join('\n'),
      });

      const found = expectOk(
        yield* call(directory, 'search_files', {
          pattern: 'needle',
          glob: '**/*.md',
        }),
      );
      expect(found['matches']).toEqual([
        { path: 'notes.md', line: 1, text: 'needle in prose' },
      ]);
      expect(
        names(
          expectOk(
            yield* call(directory, 'search_files', {
              pattern: 'needle',
              glob: '**/*.ts',
            }),
          ),
          'matches',
        ),
      ).toEqual(['src/a.ts']);

      const capped = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle', limit: 5 }),
      );
      expect(names(capped, 'matches')).toHaveLength(5);
      expect(capped['truncated']).toBe(true);
      expect(
        expectOk(yield* call(directory, 'search_files', { pattern: 'needle' })),
      ).toMatchObject({ binaryFilesSkipped: 1, longLinesSkipped: 1 });

      const insensitive = expectOk(
        yield* call(directory, 'search_files', {
          pattern: 'needle',
          glob: 'upper.txt',
          ignoreCase: true,
        }),
      );
      expect(names(insensitive, 'matches')).toEqual(['upper.txt']);
      expect(
        expectOk(
          yield* call(directory, 'search_files', {
            pattern: 'needle',
            glob: 'upper.txt',
          }),
        )['matches'],
      ).toEqual([]);

      const bad = expectFailure(
        yield* call(directory, 'search_files', { pattern: '([' }),
        'InvalidPattern',
      );
      expect(bad['pattern']).toBe('([');
      expectFailure(
        yield* call(directory, 'search_files', {
          pattern: 'needle',
          glob: '[z-a]',
        }),
        'InvalidPattern',
      );
    }),
  );

  it.live('rejects catastrophic expressions before reading any file', () =>
    Effect.gen(function* () {
      const directory = seeded({
        'adversarial.txt': `${'a'.repeat(100_000)}!`,
      });
      for (const pattern of ['(a+)+$', '(a|aa)+$']) {
        expectFailure(
          yield* call(directory, 'search_files', { pattern }),
          'InvalidPattern',
        );
      }
    }),
  );

  it.live('enforces and reports the aggregate read budget', () =>
    Effect.gen(function* () {
      const content = 'x'.repeat(2 * 1024 * 1024);
      const directory = seeded(
        Object.fromEntries(
          Array.from({ length: 9 }, (_, i) => [
            `large-${String(i)}.txt`,
            content,
          ]),
        ),
      );
      expect(
        expectOk(yield* call(directory, 'search_files', { pattern: 'needle' })),
      ).toMatchObject({
        filesSearched: 8,
        aggregateBudgetFilesSkipped: 1,
        truncated: true,
      });
    }),
  );

  it.live('reports files that fail during search reads', ({ ...context }) =>
    Effect.gen(function* () {
      if (process.getuid?.() === 0) {
        context.skip('uid 0 can read files regardless of mode bits');
      }
      const directory = seeded({ 'unreadable.txt': 'needle' });
      chmodSync(join(directory, 'unreadable.txt'), 0o000);
      expect(
        expectOk(yield* call(directory, 'search_files', { pattern: 'needle' }))[
          'unreadableFiles'
        ],
      ).toEqual(['unreadable.txt']);
    }),
  );
});

describe('run_shell', () => {
  it.live('runs commands: streams, exit codes, cwd, truncation', () =>
    Effect.gen(function* () {
      const directory = seeded({ 'marker-file': '' });
      expect(
        expectOk(
          yield* call(directory, 'run_shell', {
            command: 'echo out; echo err 1>&2',
          }),
        ),
      ).toMatchObject({ exitCode: 0, stdout: 'out\n', stderr: 'err\n' });
      expect(
        String(
          expectOk(yield* call(directory, 'run_shell', { command: 'ls' }))[
            'stdout'
          ],
        ),
      ).toContain('marker-file');
      // A non-zero exit is a result: `grep` exiting 1 is ordinary control flow.
      expect(
        expectOk(
          yield* call(directory, 'run_shell', {
            command: 'echo partial; echo bad 1>&2; exit 3',
          }),
        ),
      ).toMatchObject({ exitCode: 3, stdout: 'partial\n', stderr: 'bad\n' });

      const loud = expectOk(
        yield* call(directory, 'run_shell', {
          command:
            'i=0; while [ $i -lt 4000 ]; do echo "line-$i 0123456789012345678901234567890123456789"; i=$((i+1)); done; echo FINAL-LINE',
        }),
      );
      expect(loud['stdoutTruncated']).toBe(true);
      expect(String(loud['stdout']).trimEnd().endsWith('FINAL-LINE')).toBe(
        true,
      );
      expect(String(loud['stdout'])).not.toContain('line-0 ');
    }),
  );

  it.live('enforces timeouts, clamped to the application policy', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expect(
        expectFailure(
          yield* call(directory, 'run_shell', {
            command: 'sleep 5',
            timeoutMs: 200,
          }),
          'CommandTimedOut',
        )['timeoutMs'],
      ).toBe(200);
      expect(
        expectFailure(
          yield* call(
            directory,
            'run_shell',
            { command: 'sleep 1', timeoutMs: 60_000 },
            WorkspaceTools.commandPolicyLayer({
              defaultTimeoutMs: 100,
              maxTimeoutMs: 100,
              allowShell: true,
            }),
          ),
          'CommandTimedOut',
        )['timeoutMs'],
      ).toBe(100);
    }),
  );

  it.live('is disabled by default and refuses without a required sandbox', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(
          directory,
          'run_shell',
          { command: 'echo should-not-run' },
          WorkspaceTools.defaultCommandPolicyLayer,
        ),
        'ShellDisabled',
      );
      const noSpawn = Layer.succeed(
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
      expect(
        expectFailure(
          yield* call(
            directory,
            'run_shell',
            { command: 'echo should-not-run' },
            WorkspaceTools.shellEnabledCommandPolicyLayer,
            sandboxedLayer({ mode: 'required', root: directory }).pipe(
              Layer.provide(noSpawn),
            ),
          ),
          'SandboxRequired',
        )['command'],
      ).toBe('echo should-not-run');
    }),
  );

  it.live.skipIf(process.platform !== 'darwin')(
    'runs inside the sandbox and refuses writes outside the root',
    () =>
      Effect.gen(function* () {
        const directory = workspace();
        const sandboxed = sandboxedLayer({
          mode: 'required',
          root: realpathSync(directory),
        }).pipe(Layer.provide(NodeServices.layer));
        const policy = WorkspaceTools.shellEnabledCommandPolicyLayer;
        const ok = expectOk(
          yield* call(
            directory,
            'run_shell',
            { command: 'echo in > f' },
            policy,
            sandboxed,
          ),
        );
        expect(ok['sandbox']).toBe('sandbox-exec');
        const probe = join(tmpdir(), `ai-escape-probe-${String(process.pid)}`);
        const denied = expectOk(
          yield* call(
            directory,
            'run_shell',
            { command: `echo out > ${probe}` },
            policy,
            sandboxed,
          ),
        );
        expect(denied['exitCode']).not.toBe(0);
        expect(existsSync(probe)).toBe(false);
      }),
  );
});

describe('workspace confinement', () => {
  it.live('every tool refuses paths that leave the workspace', () =>
    Effect.gen(function* () {
      const directory = seeded({ 'f.txt': 'secret' });
      writeFileSync(join(root, 'outside.txt'), 'secret');
      symlinkSync(join(root, 'outside.txt'), join(directory, 'link.txt'));

      const escape = expectFailure(
        yield* call(directory, 'read_file', { path: '../outside.txt' }),
        'PathOutsideWorkspace',
      );
      expect(escape['reason']).toBe('escapes-root');
      expect(escape['root']).toBe('.');
      expect(JSON.stringify(escape)).not.toContain(directory);

      expect(
        expectFailure(
          yield* call(directory, 'read_file', { path: 'a\u0000b' }),
          'PathOutsideWorkspace',
        )['reason'],
      ).toBe('nul-byte');

      const refused: ReadonlyArray<Outcome> = [
        yield* call(directory, 'read_file', { path: '/etc/hosts' }),
        yield* call(directory, 'write_file', {
          path: '../escaped.txt',
          content: 'x',
        }),
        yield* call(directory, 'edit_file', {
          path: '../outside.txt',
          oldText: 'secret',
          newText: 'x',
        }),
        yield* call(directory, 'list_files', { path: '..' }),
        yield* call(directory, 'search_files', { pattern: 'x', path: '../..' }),
        yield* call(directory, 'run_shell', { command: 'ls', cwd: '/etc' }),
      ];
      for (const outcome of refused) {
        expectFailure(outcome, 'PathOutsideWorkspace');
      }
      expect(existsSync(join(root, 'escaped.txt'))).toBe(false);
      expect(readFileSync(join(root, 'outside.txt'), 'utf8')).toBe('secret');

      // A symlink inside the workspace pointing out of it is not followed.
      expectFailure(
        yield* call(directory, 'read_file', { path: 'link.txt' }),
        'SymlinkDenied',
      );
      expectFailure(
        yield* call(directory, 'run_shell', { command: 'ls', cwd: 'absent' }),
        'FileNotFound',
      );
    }),
  );
});

// What the model is shown: `Schema.Number` would advertise a string
// alternative ("Infinity" | "NaN"), which one provider took as permission to
// send a string and ended the run on a decode failure.
describe('advertised tool schemas', () => {
  it('offer no string alternative for numbers and require strict mode', () => {
    for (const tool of Object.values(
      WorkspaceTools.toolkit.tools as Record<string, Tool.Any>,
    )) {
      expect(JSON.stringify(ToolNamespace.getJsonSchema(tool))).not.toContain(
        'Infinity',
      );
      expect(ToolNamespace.getStrictMode(tool)).toBe(true);
    }
  });
});
