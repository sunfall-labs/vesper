import { Effect, Schema } from 'effect';
import { Tool } from 'effect/unstable/ai';

import { WorkspaceDriver } from './driver.js';
import { WorkspaceOutput } from './output.js';
import { WorkspacePath } from './path.js';
import { FilesystemPolicy, Root } from './workspace-context.js';

const MAX_PATCH_BYTES = 2 * 1024 * 1024;

/** A patch was rejected before it could be applied. */
export class PatchRejected extends Schema.TaggedError<PatchRejected>(
  '@sunfall/vesper-workspace/PatchRejected',
)('PatchRejected', { message: Schema.String }) {}

interface Hunk {
  readonly lines: readonly string[];
}

type Operation =
  | {
      readonly type: 'add';
      readonly path: string;
      readonly lines: readonly string[];
    }
  | { readonly type: 'delete'; readonly path: string }
  | {
      readonly type: 'update';
      readonly path: string;
      readonly moveTo: string | null;
      readonly hunks: readonly Hunk[];
    };

const reject = (message: string): PatchRejected =>
  new PatchRejected({ message });

const section = (line: string): boolean =>
  line.startsWith('*** Add File: ') ||
  line.startsWith('*** Update File: ') ||
  line.startsWith('*** Delete File: ') ||
  line === '*** End Patch';

const parse = (
  patch: string,
): Effect.Effect<readonly Operation[], PatchRejected> =>
  Effect.gen(function* () {
    const lines = patch
      .replaceAll('\r\n', '\n')
      .replaceAll('\r', '\n')
      .split('\n');
    if (lines[0] !== '*** Begin Patch') {
      return yield* reject("patch rejected: missing '*** Begin Patch'");
    }

    const operations: Operation[] = [];
    let index = 1;
    while (index < lines.length) {
      const line = lines[index] ?? '';
      if (line === '*** End Patch') {
        break;
      }

      if (line.startsWith('*** Add File: ')) {
        const path = line.slice('*** Add File: '.length).trim();
        if (path.length === 0) {
          return yield* reject('add file path is empty');
        }
        index += 1;
        const added: string[] = [];
        while (index < lines.length && !section(lines[index] ?? '')) {
          const addedLine = lines[index] ?? '';
          if (!addedLine.startsWith('+')) {
            return yield* reject(`invalid add line '${addedLine}'`);
          }
          added.push(addedLine.slice(1));
          index += 1;
        }
        operations.push({ type: 'add', path, lines: added });
        continue;
      }

      if (line.startsWith('*** Delete File: ')) {
        const path = line.slice('*** Delete File: '.length).trim();
        if (path.length === 0) {
          return yield* reject('delete file path is empty');
        }
        operations.push({ type: 'delete', path });
        index += 1;
        continue;
      }

      if (line.startsWith('*** Update File: ')) {
        const path = line.slice('*** Update File: '.length).trim();
        if (path.length === 0) {
          return yield* reject('update file path is empty');
        }
        index += 1;
        let moveTo: string | null = null;
        if ((lines[index] ?? '').startsWith('*** Move to: ')) {
          moveTo = (lines[index] ?? '').slice('*** Move to: '.length).trim();
          if (moveTo.length === 0) {
            return yield* reject('move target path is empty');
          }
          index += 1;
        }

        const hunks: Hunk[] = [];
        while (index < lines.length && !section(lines[index] ?? '')) {
          if (!(lines[index] ?? '').startsWith('@@')) {
            return yield* reject(
              `expected hunk header, got '${lines[index] ?? ''}'`,
            );
          }
          index += 1;
          const hunk: string[] = [];
          while (index < lines.length) {
            const next = lines[index] ?? '';
            if (next.startsWith('@@') || section(next)) {
              break;
            }
            if (next === '\\ No newline at end of file') {
              index += 1;
              continue;
            }
            if (
              !next.startsWith(' ') &&
              !next.startsWith('+') &&
              !next.startsWith('-')
            ) {
              return yield* reject(`invalid hunk line '${next}'`);
            }
            hunk.push(next);
            index += 1;
          }
          if (hunk.length === 0) {
            return yield* reject('update hunk is empty');
          }
          hunks.push({ lines: hunk });
        }
        if (hunks.length === 0) {
          return yield* reject('update has no hunks');
        }
        operations.push({ type: 'update', path, moveTo, hunks });
        continue;
      }

      if (line.trim().length === 0) {
        index += 1;
        continue;
      }
      return yield* reject(`unrecognized patch section '${line}'`);
    }

    if ((lines[index] ?? '') !== '*** End Patch') {
      return yield* reject("patch rejected: missing '*** End Patch'");
    }
    if (operations.length === 0) {
      return yield* reject('patch rejected: empty patch');
    }
    return operations;
  });

const matches = (
  source: readonly string[],
  needle: readonly string[],
  from: number,
): readonly number[] => {
  const found: number[] = [];
  for (let index = from; index <= source.length - needle.length; index += 1) {
    if (needle.every((line, offset) => source[index + offset] === line)) {
      found.push(index);
    }
  }
  return found;
};

const applyHunks = (
  source: string,
  hunks: readonly Hunk[],
): Effect.Effect<string, PatchRejected> =>
  Effect.gen(function* () {
    const lines = source.split('\n');
    let cursor = 0;
    for (const hunk of hunks) {
      const oldLines = hunk.lines
        .filter((line) => !line.startsWith('+'))
        .map((line) => line.slice(1));
      const newLines = hunk.lines
        .filter((line) => !line.startsWith('-'))
        .map((line) => line.slice(1));
      const positions = matches(lines, oldLines, cursor);
      if (positions.length === 0) {
        return yield* reject('hunk context was not found in target file');
      }
      if (positions.length > 1) {
        return yield* reject('hunk context is ambiguous in target file');
      }
      const start = positions[0] ?? 0;
      lines.splice(start, oldLines.length, ...newLines);
      cursor = start + newLines.length;
    }
    const result = lines.join('\n');
    if (WorkspaceOutput.utf8Size(result) > MAX_PATCH_BYTES) {
      return yield* reject(
        `updated file exceeds ${String(MAX_PATCH_BYTES)} bytes`,
      );
    }
    return result;
  });

const resolvePath = (
  input: string,
): Effect.Effect<
  string,
  PatchRejected,
  Root | WorkspaceDriver.Service | FilesystemPolicy
> =>
  Effect.gen(function* () {
    const root = WorkspacePath.normalize((yield* Root).path);
    const resolution = WorkspacePath.resolve(root, input);
    if (!resolution.ok) {
      return yield* reject(`path is outside workspace: ${input}`);
    }
    const driver = yield* WorkspaceDriver.Service;
    if (!(yield* FilesystemPolicy).allowSymlinks) {
      const relative = WorkspacePath.relative(root, resolution.path);
      const segments = relative === '.' ? [] : relative.split('/');
      let current = root;
      for (const segment of segments) {
        current = current === '/' ? `/${segment}` : `${current}/${segment}`;
        const stat = yield* driver.stat(current).pipe(Effect.result);
        if (stat._tag === 'Success' && stat.success.isSymbolicLink === true) {
          return yield* reject(
            `symbolic link traversal is disabled: ${relative}`,
          );
        }
      }
    }
    return resolution.path;
  });

interface Prepared {
  readonly operation: Operation;
  readonly path: string;
  readonly moveTo: string | null;
  readonly content: string | null;
}

const prepare = (
  operation: Operation,
): Effect.Effect<
  Prepared,
  PatchRejected,
  Root | WorkspaceDriver.Service | FilesystemPolicy
> =>
  Effect.gen(function* () {
    const driver = yield* WorkspaceDriver.Service;
    const path = yield* resolvePath(operation.path);
    const exists = yield* driver
      .exists(path)
      .pipe(Effect.mapError(() => reject(`cannot inspect ${operation.path}`)));
    if (operation.type === 'add') {
      if (exists) {
        return yield* reject(`file already exists: ${operation.path}`);
      }
      return {
        operation,
        path,
        moveTo: null,
        content: operation.lines.join('\n'),
      };
    }
    if (!exists) {
      return yield* reject(`file not found: ${operation.path}`);
    }
    if (operation.type === 'delete') {
      return { operation, path, moveTo: null, content: null };
    }

    const original = yield* driver
      .readFile(path, { maxBytes: MAX_PATCH_BYTES })
      .pipe(Effect.mapError(() => reject(`cannot read ${operation.path}`)));
    const content = yield* applyHunks(original, operation.hunks);
    const moveTo =
      operation.moveTo === null ? null : yield* resolvePath(operation.moveTo);
    if (
      moveTo !== null &&
      (yield* driver
        .exists(moveTo)
        .pipe(
          Effect.mapError(() =>
            reject(`cannot inspect move target ${operation.moveTo ?? ''}`),
          ),
        ))
    ) {
      return yield* reject(
        `move target already exists: ${operation.moveTo ?? ''}`,
      );
    }
    return { operation, path, moveTo, content };
  });

export const makeApplyPatchTool = (needsApproval: boolean) =>
  Tool.make('apply_patch', {
    description:
      'Apply a patch envelope with Add/Update/Delete file operations and hunks. Paths are relative to the workspace root.',
    parameters: Schema.Struct({ patchText: Schema.String }),
    success: Schema.Struct({
      filesApplied: Schema.Natural,
      summary: Schema.String,
    }),
    failure: Schema.Struct({ message: Schema.String }),
    failureMode: 'return',
    needsApproval,
    dependencies: [Root, WorkspaceDriver.Service, FilesystemPolicy],
  });

export const ApplyPatchTool = makeApplyPatchTool(false);

export const applyPatchHandler = ({
  patchText,
}: {
  readonly patchText: string;
}) =>
  Effect.gen(function* () {
    const operations = yield* parse(patchText);
    const prepared = yield* Effect.forEach(operations, prepare);
    const driver = yield* WorkspaceDriver.Service;
    const root = WorkspacePath.normalize((yield* Root).path);
    const summaries: string[] = [];
    for (const item of prepared) {
      const parent = item.path.slice(0, item.path.lastIndexOf('/'));
      if (parent !== '' && parent !== root) {
        yield* driver.mkdir(parent, { recursive: true });
      }
      if (item.operation.type === 'delete') {
        yield* driver.rm(item.path);
        summaries.push(`D ${item.operation.path}`);
      } else if (item.moveTo !== null) {
        const moveParent = item.moveTo.slice(0, item.moveTo.lastIndexOf('/'));
        if (moveParent !== '' && moveParent !== root) {
          yield* driver.mkdir(moveParent, { recursive: true });
        }
        yield* driver.writeFile(item.moveTo, item.content ?? '');
        yield* driver.rm(item.path);
        summaries.push(`M ${item.operation.path} -> ${item.moveTo}`);
      } else {
        yield* driver.writeFile(item.path, item.content ?? '');
        summaries.push(
          `${item.operation.type === 'add' ? 'A' : 'M'} ${item.operation.path}`,
        );
      }
    }
    return { filesApplied: operations.length, summary: summaries.join('\n') };
  }).pipe(
    Effect.mapError((error) =>
      Schema.is(PatchRejected)(error)
        ? { message: error.message }
        : { message: String(error) },
    ),
  );
