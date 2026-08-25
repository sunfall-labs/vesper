import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Schema, Stream } from 'effect';
import { Tool as ToolNamespace } from 'effect/unstable/ai';
import type { Tool, Toolkit } from 'effect/unstable/ai';

import { WorkspaceDriver } from '../src/driver.js';
import { layer as localLayer } from '../src/layer-local.js';
import {
  CommandTimedOut,
  EditTargetAmbiguous,
  FileTooLarge,
  WorkspaceTools,
} from '../src/tools.js';

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
 * Three outcomes rather than two, because a *parameter* rejection does not
 * arrive the same way a handler failure does: `handle` fails outright, while a
 * declared failure rides back on the stream with `isFailure` set. Collapsing
 * them would let a test claiming to pin a typed failure pass on a validation
 * error instead.
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
    if (!isRecord(last.result)) {
      return {
        kind: 'call-error',
        error: new Error('tool returned a non-object result'),
      };
    }
    const value = last.result;
    return last.isFailure
      ? ({
          kind: 'tool-failure',
          tag: String(value['_tag']),
          error: value,
        } as const)
      : ({ kind: 'ok', value } as const);
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

const arrayProperty = (
  value: Record<string, unknown>,
  property: string,
): ReadonlyArray<unknown> => {
  const entries = value[property];
  if (!Array.isArray(entries)) {
    throw new Error(`${property} was not an array`);
  }
  return entries;
};

const pathEntries = (
  value: Record<string, unknown>,
): ReadonlyArray<{
  path: string;
}> =>
  Schema.decodeUnknownSync(
    Schema.Array(Schema.Struct({ path: Schema.String })),
  )(value['entries']);

const typedEntries = (
  value: Record<string, unknown>,
): ReadonlyArray<{
  path: string;
  type: string;
}> =>
  Schema.decodeUnknownSync(
    Schema.Array(Schema.Struct({ path: Schema.String, type: Schema.String })),
  )(value['entries']);

const matchEntries = (
  value: Record<string, unknown>,
): ReadonlyArray<{
  path: string;
  line: number;
  text: string;
}> =>
  Schema.decodeUnknownSync(
    Schema.Array(
      Schema.Struct({
        path: Schema.String,
        line: Schema.Finite,
        text: Schema.String,
      }),
    ),
  )(value['matches']);

const populateListTree = (directory: string): void => {
  mkdirSync(join(directory, 'src/deep'), { recursive: true });
  mkdirSync(join(directory, 'node_modules/pkg'), { recursive: true });
  writeFileSync(join(directory, 'README.md'), '');
  writeFileSync(join(directory, 'src/a.ts'), '');
  writeFileSync(join(directory, 'src/deep/b.ts'), '');
  writeFileSync(join(directory, 'src/deep/c.txt'), '');
  writeFileSync(join(directory, 'node_modules/pkg/index.js'), '');
};

const populateSearchTree = (directory: string): void => {
  mkdirSync(join(directory, 'src'), { recursive: true });
  writeFileSync(join(directory, 'src/a.ts'), 'const needle = 1;\nother\n');
  writeFileSync(join(directory, 'src/b.ts'), 'no match here\n');
  writeFileSync(join(directory, 'notes.md'), 'needle in prose\n');
};

const schemaFor = (name: string): Record<string, unknown> => {
  const tools = WorkspaceTools.toolkit.tools as Record<string, Tool.Any>;
  const tool = tools[name];
  if (tool === undefined) {
    throw new Error(`no tool named ${name}`);
  }
  return ToolNamespace.getJsonSchema(tool);
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

it('states its requirements in the type', () => {
  // The assertions above are the test; this exists so a `tsc` regression is
  // reported in the same run as everything else.
  expect(new Set(Object.keys(WorkspaceTools.toolkit.tools))).toEqual(
    new Set([
      'edit_file',
      'apply_patch',
      'list_files',
      'read_file',
      'run_shell',
      'search_files',
      'write_file',
    ]),
  );
});

it('builds the same workspace toolkit with application-owned approval gates', () => {
  const toolkit = WorkspaceTools.makeToolkit({
    apply_patch: true,
    edit_file: true,
    run_shell: true,
    write_file: true,
  });

  expect(toolkit.tools.read_file.needsApproval ?? false).toBe(false);
  expect(toolkit.tools.write_file.needsApproval).toBe(true);
  expect(toolkit.tools.edit_file.needsApproval).toBe(true);
  expect(toolkit.tools.run_shell.needsApproval).toBe(true);
  expect(toolkit.tools.apply_patch.needsApproval).toBe(true);

  // The handlers stay attached to the selected toolkit without an adapter
  // layer or a second set of definitions.
  expect(WorkspaceTools.makeLayer(toolkit)).toBeDefined();
});

describe('apply_patch', () => {
  it.live(
    'applies add, update, move, and delete operations through the driver',
    () =>
      Effect.gen(function* () {
        const directory = workspace();
        mkdirSync(join(directory, 'src'), { recursive: true });
        writeFileSync(
          join(directory, 'src/original.ts'),
          'const before = 1;\n',
        );
        writeFileSync(join(directory, 'src/delete.ts'), 'remove me\n');

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
        expect(readFileSync(join(directory, 'src/moved.ts'), 'utf8')).toBe(
          'const after = 2;\n',
        );
        expect(existsSync(join(directory, 'src/original.ts'))).toBe(false);
        expect(existsSync(join(directory, 'added.txt'))).toBe(true);
      }),
  );
});

// ---------------------------------------------------------------- read_file

describe('read_file', () => {
  it.live('reads a text file', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'note.txt'), 'one\ntwo\nthree');

      const value = expectOk(
        yield* call(directory, 'read_file', { path: 'note.txt' }),
      );

      expect(value).toMatchObject({
        path: 'note.txt',
        content: 'one\ntwo\nthree',
        firstLine: 1,
        lineCount: 3,
        totalLines: 3,
        truncated: false,
        truncatedBy: null,
      });
    }),
  );

  it.live('preserves multi-byte characters exactly', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'utf8.txt'), 'héllo 😀 €');

      const value = expectOk(
        yield* call(directory, 'read_file', { path: 'utf8.txt' }),
      );
      expect(value['content']).toBe('héllo 😀 €');
    }),
  );

  it.live('windows with offset and limit, and admits there is more', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(
        join(directory, 'lines.txt'),
        Array.from(
          { length: 10 },
          (_, index) => `line-${String(index + 1)}`,
        ).join('\n'),
      );

      const value = expectOk(
        yield* call(directory, 'read_file', {
          path: 'lines.txt',
          offset: 3,
          limit: 2,
        }),
      );

      expect(value).toMatchObject({
        content: 'line-3\nline-4',
        firstLine: 3,
        lineCount: 2,
        totalLines: 10,
        // The window fit its own budget, but it is not the whole file, and
        // saying `false` here is exactly the lie this tool must not tell.
        truncated: true,
        truncatedBy: 'lines',
      });
    }),
  );

  it.live('keeps metadata consistent for an offset beyond the end', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'short.txt'), 'one\ntwo');

      const value = expectOk(
        yield* call(directory, 'read_file', {
          path: 'short.txt',
          offset: 100,
          limit: 5,
        }),
      );

      expect(value).toMatchObject({
        content: '',
        firstLine: 3,
        lineCount: 0,
        totalLines: 2,
        truncated: true,
        truncatedBy: 'lines',
      });
    }),
  );

  it.live('refuses to materialize a file beyond the driver read budget', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(
        join(directory, 'huge.txt'),
        'x'.repeat(2 * 1024 * 1024 + 1),
      );

      expectFailure(
        yield* call(directory, 'read_file', { path: 'huge.txt' }),
        'FileTooLarge',
      );
    }),
  );

  it.live(
    'truncates a file too large for a context window and says which limit hit',
    () =>
      Effect.gen(function* () {
        const directory = workspace();
        const line = 'x'.repeat(200);
        writeFileSync(
          join(directory, 'big.txt'),
          Array.from({ length: 1000 }, () => line).join('\n'),
        );

        const value = expectOk(
          yield* call(directory, 'read_file', { path: 'big.txt' }),
        );

        expect(value['truncated']).toBe(true);
        expect(value['truncatedBy']).toBe('bytes');
        expect(value['totalLines']).toBe(1000);
        expect(Number(value['lineCount'])).toBeLessThan(1000);
        expect(String(value['content']).length).toBeLessThanOrEqual(51_200);
      }),
  );

  it.live('fails with FileNotFound on a missing file', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const error = expectFailure(
        yield* call(directory, 'read_file', { path: 'absent.txt' }),
        'FileNotFound',
      );
      expect(error['path']).toBe('absent.txt');
      expect(JSON.stringify(error)).not.toContain(directory);
    }),
  );

  it.live('fails with NotAFile on a directory', () =>
    Effect.gen(function* () {
      const directory = workspace();
      mkdirSync(join(directory, 'sub'));
      expectFailure(
        yield* call(directory, 'read_file', { path: 'sub' }),
        'NotAFile',
      );
    }),
  );

  it.live('refuses a path that traverses out of the workspace', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(root, 'outside.txt'), 'secret');

      const error = expectFailure(
        yield* call(directory, 'read_file', { path: '../outside.txt' }),
        'PathOutsideWorkspace',
      );
      expect(error['reason']).toBe('escapes-root');
      expect(error['root']).toBe('.');
      expect(JSON.stringify(error)).not.toContain(directory);
    }),
  );

  it.live('refuses an absolute path outside the workspace', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'read_file', { path: '/etc/hosts' }),
        'PathOutsideWorkspace',
      );
    }),
  );

  it.live('refuses a path containing a NUL byte', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const error = expectFailure(
        yield* call(directory, 'read_file', { path: 'a\u0000b' }),
        'PathOutsideWorkspace',
      );
      expect(error['reason']).toBe('nul-byte');
    }),
  );

  it.live('fails with BinaryContent on a file with NUL bytes', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(
        join(directory, 'image.png'),
        Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x00, 0x1a, 0x0a]),
      );

      const error = expectFailure(
        yield* call(directory, 'read_file', { path: 'image.png' }),
        'BinaryContent',
      );
      expect(error['reason']).toBe('nul-byte');
    }),
  );

  it.live('fails with BinaryContent on bytes that are not UTF-8', () =>
    Effect.gen(function* () {
      const directory = workspace();
      // Latin-1 "café". No NUL, so only strict decoding catches it — a lenient
      // read would return "caf�" as if it were the file's content.
      writeFileSync(
        join(directory, 'latin1.txt'),
        Buffer.from([0x63, 0x61, 0x66, 0xe9]),
      );

      const error = expectFailure(
        yield* call(directory, 'read_file', { path: 'latin1.txt' }),
        'BinaryContent',
      );
      expect(error['reason']).toBe('invalid-utf8');
    }),
  );
});

// --------------------------------------------------------------- write_file

describe('write_file', () => {
  it.live('creates a file and its missing parents', () =>
    Effect.gen(function* () {
      const directory = workspace();

      const value = expectOk(
        yield* call(directory, 'write_file', {
          path: 'a/b/c.txt',
          content: 'hello',
        }),
      );

      expect(value).toMatchObject({
        path: 'a/b/c.txt',
        bytesWritten: 5,
        created: true,
      });
      expect(readFileSync(join(directory, 'a/b/c.txt'), 'utf8')).toBe('hello');
    }),
  );

  it.live('replaces an existing file and reports that it was not created', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'old');

      const value = expectOk(
        yield* call(directory, 'write_file', {
          path: 'f.txt',
          content: 'new',
        }),
      );

      expect(value['created']).toBe(false);
      expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe('new');
    }),
  );

  it.live('counts bytes rather than characters', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const value = expectOk(
        yield* call(directory, 'write_file', {
          path: 'utf8.txt',
          content: '😀é',
        }),
      );

      expect(value['bytesWritten']).toBe(6);
      expect(readFileSync(join(directory, 'utf8.txt'), 'utf8')).toBe('😀é');
    }),
  );

  it.live('fails with NotAFile rather than clobbering a directory', () =>
    Effect.gen(function* () {
      const directory = workspace();
      mkdirSync(join(directory, 'sub'));

      expectFailure(
        yield* call(directory, 'write_file', { path: 'sub', content: 'x' }),
        'NotAFile',
      );
    }),
  );

  it.live('does not treat an unrelated stat failure as a missing file', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const statFailureLayer = Layer.effect(
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
          statFailureLayer,
        ),
        'WorkspaceUnavailable',
      );
    }),
  );

  it.live(
    'rejects content above the write ceiling without creating a file',
    () =>
      Effect.gen(function* () {
        const directory = workspace();
        const outcome = yield* call(directory, 'write_file', {
          path: 'too-large.txt',
          content: 'x'.repeat(WorkspaceTools.MAX_WRITABLE_BYTES + 1),
        });

        expectFailure(outcome, 'FileTooLarge');
        expect(existsSync(join(directory, 'too-large.txt'))).toBe(false);
      }),
  );

  it.live('refuses to write outside the workspace', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'write_file', {
          path: '../escaped.txt',
          content: 'x',
        }),
        'PathOutsideWorkspace',
      );
    }),
  );
});

// ---------------------------------------------------------------- edit_file

describe('edit_file', () => {
  it.live('replaces a unique target', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'const a = 1;\nconst b = 2;\n');

      const value = expectOk(
        yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: 'const b = 2;',
          newText: 'const b = 3;',
        }),
      );

      expect(value).toMatchObject({ path: 'f.txt', replacements: 1 });
      expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe(
        'const a = 1;\nconst b = 3;\n',
      );
    }),
  );

  it.live('serializes concurrent edits so one stale update is rejected', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'TOKEN');

      const outcomes = yield* Effect.all(
        [
          call(directory, 'edit_file', {
            path: 'f.txt',
            oldText: 'TOKEN',
            newText: 'first',
          }),
          call(directory, 'edit_file', {
            path: 'f.txt',
            oldText: 'TOKEN',
            newText: 'second',
          }),
        ],
        { concurrency: 'unbounded' },
      );

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
      expect(['first', 'second']).toContain(
        readFileSync(join(directory, 'f.txt'), 'utf8'),
      );
    }),
  );

  it.live(
    'rejects an edit whose resulting file exceeds the write ceiling',
    () =>
      Effect.gen(function* () {
        const directory = workspace();
        writeFileSync(join(directory, 'f.txt'), 'TOKEN');

        const outcome = yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: 'TOKEN',
          newText: 'x'.repeat(WorkspaceTools.MAX_WRITABLE_BYTES + 1),
        });

        expectFailure(outcome, 'FileTooLarge');
        expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe('TOKEN');
      }),
  );

  it.live('fails without writing when the target is absent', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'unchanged');

      const error = expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: 'not here',
          newText: 'x',
        }),
        'EditTargetMissing',
      );

      expect(error['target']).toBe('not here');
      expect(error['path']).toBe('f.txt');
      expect(JSON.stringify(error)).not.toContain(directory);
      // The whole point: a model told the edit landed reasons from a file that
      // never changed.
      expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe('unchanged');
    }),
  );

  it.live('refuses an ambiguous target and leaves the file alone', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'x = 1\ny = 1\nz = 1\n');

      const error = expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: '= 1',
          newText: '= 2',
        }),
        'EditTargetAmbiguous',
      );

      expect(error['occurrences']).toBe(3);
      expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe(
        'x = 1\ny = 1\nz = 1\n',
      );
    }),
  );

  it.live('replaces every occurrence when asked', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'x = 1\ny = 1\nz = 1\n');

      const value = expectOk(
        yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: '= 1',
          newText: '= 2',
          replaceAll: true,
        }),
      );

      expect(value['replacements']).toBe(3);
      expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe(
        'x = 2\ny = 2\nz = 2\n',
      );
    }),
  );

  it.live('writes replacement text literally, including `$&`', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'TOKEN');

      expectOk(
        yield* call(directory, 'edit_file', {
          path: 'f.txt',
          oldText: 'TOKEN',
          newText: 'cost: $& and $1',
        }),
      );

      // `String.replace` would expand `$&` to the matched text and `$1` to an
      // empty group, producing "cost: TOKEN and ".
      expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe(
        'cost: $& and $1',
      );
    }),
  );

  it.live('rejects an empty target at parameter validation', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'abc');

      const outcome = yield* call(directory, 'edit_file', {
        path: 'f.txt',
        oldText: '',
        newText: 'x',
      });

      // An empty string matches at every position, so there is no sound answer
      // to give; the schema refuses it before the handler has to invent one.
      expect(outcome.kind).toBe('call-error');
      expect(readFileSync(join(directory, 'f.txt'), 'utf8')).toBe('abc');
    }),
  );

  it.live('fails with FileNotFound on a missing file', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'absent.txt',
          oldText: 'a',
          newText: 'b',
        }),
        'FileNotFound',
      );
    }),
  );

  it.live('fails with BinaryContent rather than corrupting a binary file', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'blob.bin'), Buffer.from([1, 0, 2, 3]));

      expectFailure(
        yield* call(directory, 'edit_file', {
          path: 'blob.bin',
          oldText: 'anything',
          newText: 'x',
        }),
        'BinaryContent',
      );
    }),
  );

  it.live('refuses to edit outside the workspace', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'edit_file', {
          path: '../outside.txt',
          oldText: 'secret',
          newText: 'x',
        }),
        'PathOutsideWorkspace',
      );
    }),
  );
});

// --------------------------------------------------------------- list_files

describe('list_files', () => {
  it.live('walks recursively and types each entry', () =>
    Effect.gen(function* () {
      const directory = workspace();
      populateListTree(directory);

      const value = expectOk(yield* call(directory, 'list_files', {}));
      const entries = typedEntries(value);

      expect(new Set(entries.map((entry) => entry.path))).toEqual(
        new Set([
          'README.md',
          'node_modules',
          'src',
          'src/a.ts',
          'src/deep',
          'src/deep/b.ts',
          'src/deep/c.txt',
        ]),
      );
      expect(entries.find((entry) => entry.path === 'src')?.type).toBe(
        'directory',
      );
      expect(entries.find((entry) => entry.path === 'src/a.ts')?.type).toBe(
        'file',
      );
    }),
  );

  it.live(
    'reports the directories it declined to descend, rather than hiding them',
    () =>
      Effect.gen(function* () {
        const directory = workspace();
        populateListTree(directory);

        const value = expectOk(yield* call(directory, 'list_files', {}));

        expect(value['ignoredDirectories']).toEqual(['node_modules']);
        const entries = pathEntries(value);
        expect(
          entries.some((entry) => entry.path.startsWith('node_modules/')),
        ).toBe(false);
      }),
  );

  it.live('filters by glob', () =>
    Effect.gen(function* () {
      const directory = workspace();
      populateListTree(directory);

      const value = expectOk(
        yield* call(directory, 'list_files', { pattern: '**/*.ts' }),
      );
      const entries = pathEntries(value);

      expect(new Set(entries.map((entry) => entry.path))).toEqual(
        new Set(['src/a.ts', 'src/deep/b.ts']),
      );
    }),
  );

  it.live('returns malformed glob ranges as InvalidPattern', () =>
    Effect.gen(function* () {
      const directory = workspace();
      populateListTree(directory);

      expectFailure(
        yield* call(directory, 'list_files', { pattern: '[z-a]' }),
        'InvalidPattern',
      );
    }),
  );

  it.live('lists a symlink as a symlink and does not walk through it', () =>
    Effect.gen(function* () {
      const directory = workspace();
      mkdirSync(join(directory, 'real'));
      writeFileSync(join(directory, 'real/inside.txt'), '');
      symlinkSync(join(directory, 'real'), join(directory, 'link'));

      const value = expectOk(yield* call(directory, 'list_files', {}));
      const entries = typedEntries(value);

      expect(entries.find((entry) => entry.path === 'link')?.type).toBe(
        'symlink',
      );
      // Descending would loop on a link to `..` and would leave the root on a
      // link pointing out of it.
      expect(entries.some((entry) => entry.path === 'link/inside.txt')).toBe(
        false,
      );
    }),
  );

  it.live('refuses to follow a symlink by default', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'outside.txt'), 'secret');
      symlinkSync(join(directory, 'outside.txt'), join(directory, 'link.txt'));

      expectFailure(
        yield* call(directory, 'read_file', { path: 'link.txt' }),
        'SymlinkDenied',
      );
    }),
  );

  it.live('caps the result at `limit` and says it was capped', () =>
    Effect.gen(function* () {
      const directory = workspace();
      populateListTree(directory);

      const value = expectOk(
        yield* call(directory, 'list_files', { limit: 2 }),
      );

      expect(arrayProperty(value, 'entries')).toHaveLength(2);
      expect(value['truncated']).toBe(true);
    }),
  );

  it.live('starts from a subdirectory when asked', () =>
    Effect.gen(function* () {
      const directory = workspace();
      populateListTree(directory);

      const value = expectOk(
        yield* call(directory, 'list_files', { path: 'src/deep' }),
      );
      const entries = pathEntries(value);

      expect(value['directory']).toBe('src/deep');
      expect(new Set(entries.map((entry) => entry.path))).toEqual(
        new Set(['b.ts', 'c.txt']),
      );
    }),
  );

  it.live('fails with FileNotFound on a missing directory', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'list_files', { path: 'absent' }),
        'FileNotFound',
      );
    }),
  );

  it.live('fails with NotADirectory when pointed at a file', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), '');
      expectFailure(
        yield* call(directory, 'list_files', { path: 'f.txt' }),
        'NotADirectory',
      );
    }),
  );

  it.live('refuses to list outside the workspace', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'list_files', { path: '..' }),
        'PathOutsideWorkspace',
      );
    }),
  );
});

// ------------------------------------------------------------- search_files

describe('search_files', () => {
  it.live('returns one entry per matching line, with line numbers', () =>
    Effect.gen(function* () {
      const directory = workspace();
      populateSearchTree(directory);

      const value = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle' }),
      );
      const matches = matchEntries(value);

      expect(matches).toEqual([
        { path: 'notes.md', line: 1, text: 'needle in prose' },
        { path: 'src/a.ts', line: 1, text: 'const needle = 1;' },
      ]);
      expect(value['filesSearched']).toBe(3);
    }),
  );

  it.live('narrows by glob', () =>
    Effect.gen(function* () {
      const directory = workspace();
      populateSearchTree(directory);

      const value = expectOk(
        yield* call(directory, 'search_files', {
          pattern: 'needle',
          glob: '**/*.ts',
        }),
      );

      expect(matchEntries(value).map((match) => match.path)).toEqual([
        'src/a.ts',
      ]);
    }),
  );

  it.live('honours ignoreCase', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'f.txt'), 'NEEDLE\n');

      const sensitive = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle' }),
      );
      expect(arrayProperty(sensitive, 'matches')).toHaveLength(0);

      const insensitive = expectOk(
        yield* call(directory, 'search_files', {
          pattern: 'needle',
          ignoreCase: true,
        }),
      );
      expect(arrayProperty(insensitive, 'matches')).toHaveLength(1);
    }),
  );

  it.live('reports zero matches distinctly from files it could not read', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'text.txt'), 'nothing relevant\n');
      writeFileSync(join(directory, 'blob.bin'), Buffer.from([1, 0, 2, 3]));

      const value = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle' }),
      );

      expect(value['matches']).toEqual([]);
      expect(value['filesSearched']).toBe(1);
      expect(value['binaryFilesSkipped']).toBe(1);
      expect(value['largeFilesSkipped']).toBe(0);
    }),
  );

  it.live('enforces and reports the aggregate search read budget', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const content = 'x'.repeat(2 * 1024 * 1024);
      for (let index = 0; index < 9; index += 1) {
        writeFileSync(join(directory, `large-${String(index)}.txt`), content);
      }

      const value = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle' }),
      );

      expect(value['filesSearched']).toBe(8);
      expect(value['aggregateBudgetFilesSkipped']).toBe(1);
      expect(value['longLinesSkipped']).toBe(8);
      expect(value['truncated']).toBe(true);
    }),
  );

  it.live('reports files that fail during search reads', ({ ...context }) =>
    Effect.gen(function* () {
      if (process.getuid?.() === 0) {
        context.skip('uid 0 can read files regardless of mode bits');
      }
      const directory = workspace();
      const path = join(directory, 'unreadable.txt');
      writeFileSync(path, 'needle');
      chmodSync(path, 0o000);

      const value = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle' }),
      );

      expect(value['unreadableFiles']).toEqual(['unreadable.txt']);
    }),
  );

  it.live('fails with InvalidPattern on a malformed regular expression', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const error = expectFailure(
        yield* call(directory, 'search_files', { pattern: '([' }),
        'InvalidPattern',
      );
      expect(error['pattern']).toBe('([');
      expect(String(error['reason']).length).toBeGreaterThan(0);
    }),
  );

  it.live.each(['(a+)+$', '(a|aa)+$'])(
    'rejects unsafe expression %s before reading files',
    (pattern) =>
      Effect.gen(function* () {
        const directory = workspace();
        writeFileSync(
          join(directory, 'adversarial.txt'),
          `${'a'.repeat(100_000)}!`,
        );

        expectFailure(
          yield* call(directory, 'search_files', { pattern }),
          'InvalidPattern',
        );
      }),
  );

  it.live('returns malformed search globs as InvalidPattern', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'a.txt'), 'needle');

      expectFailure(
        yield* call(directory, 'search_files', {
          pattern: 'needle',
          glob: '[z-a]',
        }),
        'InvalidPattern',
      );
    }),
  );

  it.live('caps matches at `limit` and says it was capped', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(
        join(directory, 'many.txt'),
        Array.from({ length: 20 }, () => 'needle').join('\n'),
      );

      const value = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle', limit: 5 }),
      );

      expect(arrayProperty(value, 'matches')).toHaveLength(5);
      expect(value['truncated']).toBe(true);
    }),
  );

  it.live('reports lines too large for bounded regex evaluation', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(
        join(directory, 'minified.js'),
        `needle${'x'.repeat(20_000)}`,
      );

      const value = expectOk(
        yield* call(directory, 'search_files', { pattern: 'needle' }),
      );
      expect(value['matches']).toEqual([]);
      expect(value['longLinesSkipped']).toBe(1);
    }),
  );

  it.live('refuses to search outside the workspace', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'search_files', { pattern: 'x', path: '../..' }),
        'PathOutsideWorkspace',
      );
    }),
  );
});

// ---------------------------------------------------------------- run_shell

describe('run_shell', () => {
  it.live('is disabled by the default command policy', () =>
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
    }),
  );

  it.live('runs a command and separates its streams', () =>
    Effect.gen(function* () {
      const directory = workspace();

      const value = expectOk(
        yield* call(directory, 'run_shell', {
          command: 'echo out; echo err 1>&2',
        }),
      );

      expect(value['exitCode']).toBe(0);
      expect(String(value['stdout']).trim()).toBe('out');
      expect(String(value['stderr']).trim()).toBe('err');
      expect(value['stdoutTruncated']).toBe(false);
    }),
  );

  it.live('defaults its working directory to the workspace root', () =>
    Effect.gen(function* () {
      const directory = workspace();
      writeFileSync(join(directory, 'marker-file'), '');

      const value = expectOk(
        yield* call(directory, 'run_shell', { command: 'ls' }),
      );
      expect(String(value['stdout'])).toContain('marker-file');
    }),
  );

  it.live('treats a non-zero exit as a result, with its output intact', () =>
    Effect.gen(function* () {
      const directory = workspace();

      const value = expectOk(
        yield* call(directory, 'run_shell', {
          command: 'echo partial; echo bad 1>&2; exit 3',
        }),
      );

      // A failure here would put `catchTag` in the middle of ordinary control
      // flow: `grep` exits 1 for no match, `test -f` exits 1 for false.
      expect(value['exitCode']).toBe(3);
      expect(String(value['stdout']).trim()).toBe('partial');
      expect(String(value['stderr']).trim()).toBe('bad');
    }),
  );

  it.live(
    'fails with CommandTimedOut at its deadline',
    () =>
      Effect.gen(function* () {
        const directory = workspace();

        const error = expectFailure(
          yield* call(directory, 'run_shell', {
            command: 'sleep 5',
            timeoutMs: 200,
          }),
          'CommandTimedOut',
        );

        expect(error['timeoutMs']).toBe(200);
      }),
    { timeout: 2_000 },
  );

  it.live('clamps a model timeout to the application policy', () =>
    Effect.gen(function* () {
      const directory = workspace();
      const error = expectFailure(
        yield* call(
          directory,
          'run_shell',
          {
            command: 'sleep 1',
            timeoutMs: 60_000,
          },
          WorkspaceTools.commandPolicyLayer({
            defaultTimeoutMs: 100,
            maxTimeoutMs: 100,
            allowShell: true,
          }),
        ),
        'CommandTimedOut',
      );

      expect(error['timeoutMs']).toBe(100);
    }),
  );

  it.live('keeps the end of enormous output and says it was cut', () =>
    Effect.gen(function* () {
      const directory = workspace();

      const value = expectOk(
        yield* call(directory, 'run_shell', {
          command:
            'i=0; while [ $i -lt 4000 ]; do echo "line-$i 0123456789012345678901234567890123456789"; i=$((i+1)); done; echo FINAL-LINE',
        }),
      );

      expect(value['stdoutTruncated']).toBe(true);
      // The end is what a build's error is in, so it is what must survive.
      expect(String(value['stdout']).trimEnd().endsWith('FINAL-LINE')).toBe(
        true,
      );
      expect(String(value['stdout']).includes('line-0 ')).toBe(false);
    }),
  );

  it.live('refuses a working directory outside the workspace', () =>
    Effect.gen(function* () {
      const directory = workspace();
      expectFailure(
        yield* call(directory, 'run_shell', { command: 'ls', cwd: '/etc' }),
        'PathOutsideWorkspace',
      );
    }),
  );

  it.live(
    'fails with FileNotFound when the working directory does not exist',
    () =>
      Effect.gen(function* () {
        const directory = workspace();
        expectFailure(
          yield* call(directory, 'run_shell', { command: 'ls', cwd: 'absent' }),
          'FileNotFound',
        );
      }),
  );
});

// What the model is shown, which is not the same question as what a handler
// accepts — and is the half no in-process test had ever looked at.
//
// `Schema.Number` renders as an `anyOf` whose second branch is a *string* enum
// of `"Infinity" | "-Infinity" | "NaN"`, because that is how Effect encodes the
// non-finite values. Advertising that on a line count tells a model a string is
// legal there. Anthropic ignores it; OpenAI took it, and the resulting tool call
// failed `LanguageModel`'s decode with `Expected number at ["params"]["limit"]`
// — which is an `InvalidOutputError` on the stream, so it ended the run rather
// than coming back as a tool result the model could correct.
describe('the JSON schema these tools advertise', () => {
  const propertyOf = (
    tool: string,
    property: string,
  ): Record<string, unknown> => {
    const properties = schemaFor(tool)['properties'] as Record<
      string,
      Record<string, unknown>
    >;
    const found = properties[property];
    if (found === undefined) {
      throw new Error(`no property ${property} on ${tool}`);
    }
    return found;
  };

  it.each([
    ['read_file', 'offset'],
    ['read_file', 'limit'],
    ['list_files', 'limit'],
    ['search_files', 'limit'],
    ['run_shell', 'timeoutMs'],
  ])('advertises %s.%s as a plain integer', (tool, property) => {
    expect(propertyOf(tool, property)).toEqual({ type: 'integer' });
  });

  it('offers a model no string alternative anywhere in its parameters', () => {
    for (const name of Object.keys(WorkspaceTools.toolkit.tools)) {
      expect(JSON.stringify(schemaFor(name))).not.toContain('Infinity');
    }
  });
});

describe('public numeric failure schemas', () => {
  it.effect('reject non-finite, negative, and fractional values', () =>
    Effect.gen(function* () {
      const cases = [
        {
          schema: FileTooLarge,
          value: {
            _tag: 'FileTooLarge',
            path: 'large.txt',
            maxBytes: Number.NaN,
          },
        },
        {
          schema: EditTargetAmbiguous,
          value: {
            _tag: 'EditTargetAmbiguous',
            path: 'note.txt',
            target: 'needle',
            occurrences: 1.5,
          },
        },
        {
          schema: CommandTimedOut,
          value: {
            _tag: 'CommandTimedOut',
            command: 'sleep 1',
            timeoutMs: -1,
          },
        },
      ];

      for (const { schema, value } of cases) {
        const result = yield* Schema.decodeUnknownEffect(schema)(value).pipe(
          Effect.result,
        );
        expect(result._tag).toBe('Failure');
      }
    }),
  );
});
