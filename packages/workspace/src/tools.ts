import { Context, Effect, Layer, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

import { WorkspaceDriver } from './driver.js';
import { WorkspaceGlob } from './glob.js';
import { WorkspaceOutput } from './output.js';
import { WorkspacePath } from './path.js';

// The harness toolkit: six tools an agent needs before it can do anything at
// all — read, write, edit, list, search, run.
//
// They are built on `WorkspaceDriver`, not beside it. Every byte read and
// every command run goes through the service, so the layer that decides
// *where* the workspace is decides it for the tools too, and there is no
// second path around whatever the driver's substrate contains. No `node:fs`
// import appears in this file, and that absence is the point.
//
// ## The requirement is the product
//
// Each tool declares `dependencies: [Root, WorkspaceDriver.Service]`, which
// puts both service keys in `Tool.HandlerServices` and from there into the
// requirement channel of any agent holding this toolkit. An application that
// forgets to wire a workspace does not get a tool that quietly reads the host
// filesystem — it gets a compile error. That is the property this package
// exists for, and `tools.test.ts` pins it as a type-level assertion.
//
// The alternative — a `makeTools(driver)` factory closing over a driver — is
// one line shorter and gives all of that away: the requirement disappears from
// the type, and whether the agent is pointed at a container or at the
// developer's home directory becomes a runtime fact nobody can see.
//
// ## What we took from Pi, and what we did not
//
// Pi's `harness/utils/truncate.ts` is genuinely good, typebox-free logic, and
// `output.ts` reimplements its shape rather than importing it. Two reasons,
// and the second is the decisive one:
//
// 1. `pi-agent-core`'s `exports` map has exactly two entries, `.` and
//    `./node`. There is no subpath for `dist/harness/utils/truncate.js`, so
//    "import `truncateHead`" means importing the package index — which is
//    `export *` over the agent harness, the JSONL session repository, the
//    skills loader (typebox and yaml), the proxy, and `pi-ai`. That is Pi's
//    storage and agent types, which is the case the brief said to write our
//    own for.
// 2. `docs/contributing.md` states the layering as `workspace -> effect`,
//    with `@earendil-works/*` admitted only under `@sunfall/vesper-pi`. Taking a
//    dependency here would be a change to that rule, not an application of it.
//
// `harness/utils/shell-output.ts` was not a candidate either way: it is built
// around Pi's `ExecutionEnv` — `onStdout` chunk callbacks, `createTempFile`,
// `appendFile` — and Pi's `Result` ok/err type. Our driver reports a finished
// `ShellResult`, so the streaming spill-to-tempfile machinery has nothing to
// attach to. Its one piece of portable logic is tail-truncation, which is
// `WorkspaceOutput.tail`.

// ----------------------------------------------------------------- the root

/**
 * The directory the toolkit treats as the workspace.
 *
 * A separate service rather than a constructor argument, for the same reason
 * the driver is: it belongs in the requirement channel. An agent whose tools
 * can reach the filesystem should not compile until someone has said *which*
 * filesystem and *which* directory.
 *
 * **Containment here is lexical.** Paths are resolved and checked against this
 * root before they reach the driver, which stops a model that wandered — not
 * code that meant to leave. A symlink inside the root is followed wherever it
 * points, and `run_shell` executes a command string nothing inspects. See the
 * boundary note in `driver.ts`; this narrows what the tools address, and the
 * driver's substrate is still what confines.
 */
export class Root extends Context.Service<Root, { readonly path: string }>()(
  '@sunfall/vesper-workspace/WorkspaceRoot',
) {}

/** The workspace root as a layer, for wiring. */
export const rootLayer = (path: string): Layer.Layer<Root> =>
  Layer.succeed(Root, { path });

// --------------------------------------------------------------- the limits

/** How long a command may run when the model does not say. */
export const DEFAULT_COMMAND_TIMEOUT_MS = 120_000;

/** Directory names the walk does not descend into, and reports skipping. */
export const IGNORED_DIRECTORIES: ReadonlyArray<string> = [
  '.git',
  'node_modules',
];

/** Entries a single listing or search walk will visit before giving up. */
const MAX_WALK_ENTRIES = 20_000;

/** Characters of a matching line kept by `search_files`. */
const MATCH_LINE_MAX_CHARS = 500;

/**
 * Bytes of a file `search_files` will read.
 *
 * A minified bundle is text by every test in `output.ts` and is worth nothing
 * to grep through. Skipped files are counted in the result rather than
 * dropped, so "no matches" never quietly means "did not look".
 */
const MAX_SEARCHABLE_BYTES = 2 * 1024 * 1024;

// --------------------------------------------------------------- the errors
//
// Modelled with `Schema.TaggedErrorClass` because every one of them is
// encoded into a tool result and handed to a model: the tag is what the model
// reads, and the fields are what it needs to retry differently. A single
// `ToolFailed { message }` would compile and would put the whole diagnosis
// back into prose.

const ToolOperation = Schema.Literals([
  'read',
  'write',
  'list',
  'search',
  'run',
]);

/** The path resolved to somewhere outside the workspace root. */
export class PathOutsideWorkspace extends Schema.TaggedErrorClass<PathOutsideWorkspace>()(
  '@sunfall/vesper-workspace/PathOutsideWorkspace',
  {
    path: Schema.String,
    root: Schema.String,
    reason: Schema.Literals(['escapes-root', 'nul-byte']),
  },
) {}

/** Nothing exists at the path. */
export class FileNotFound extends Schema.TaggedErrorClass<FileNotFound>()(
  '@sunfall/vesper-workspace/FileNotFound',
  { path: Schema.String },
) {}

/** Something exists at the path, but it is not a regular file. */
export class NotAFile extends Schema.TaggedErrorClass<NotAFile>()(
  '@sunfall/vesper-workspace/NotAFile',
  { path: Schema.String },
) {}

/** Something exists at the path, but it is not a directory. */
export class NotADirectory extends Schema.TaggedErrorClass<NotADirectory>()(
  '@sunfall/vesper-workspace/NotADirectory',
  { path: Schema.String },
) {}

/**
 * The file is not UTF-8 text.
 *
 * A failure rather than a lenient decode. `Buffer.toString('utf8')` on a PNG
 * returns a string of replacement characters that looks like content, and a
 * model has no way to tell that from a file of unusual glyphs.
 */
export class BinaryContent extends Schema.TaggedErrorClass<BinaryContent>()(
  '@sunfall/vesper-workspace/BinaryContent',
  {
    path: Schema.String,
    reason: Schema.Literals(['nul-byte', 'invalid-utf8']),
  },
) {}

/**
 * The text to replace is not in the file.
 *
 * The single most important failure here. An edit tool that writes the file
 * back unchanged and reports success teaches the model its change landed, and
 * everything it concludes afterwards is built on that.
 */
export class EditTargetMissing extends Schema.TaggedErrorClass<EditTargetMissing>()(
  '@sunfall/vesper-workspace/EditTargetMissing',
  { path: Schema.String, target: Schema.String },
) {}

/**
 * The text to replace occurs more than once and the caller did not say which.
 *
 * Refusing beats replacing the first: "the first one" is not a thing the model
 * asked for, and the edit it wanted may be the third.
 */
export class EditTargetAmbiguous extends Schema.TaggedErrorClass<EditTargetAmbiguous>()(
  '@sunfall/vesper-workspace/EditTargetAmbiguous',
  {
    path: Schema.String,
    target: Schema.String,
    occurrences: Schema.Number,
  },
) {}

/** The workspace refused the access. */
export class AccessDenied extends Schema.TaggedErrorClass<AccessDenied>()(
  '@sunfall/vesper-workspace/AccessDenied',
  { path: Schema.String, operation: WorkspaceDriver.Operation },
) {}

/** The search pattern is not a valid regular expression. */
export class InvalidPattern extends Schema.TaggedErrorClass<InvalidPattern>()(
  '@sunfall/vesper-workspace/InvalidPattern',
  { pattern: Schema.String, reason: Schema.String },
) {}

/** The command was killed at its deadline. */
export class CommandTimedOut extends Schema.TaggedErrorClass<CommandTimedOut>()(
  '@sunfall/vesper-workspace/CommandTimedOut',
  { command: Schema.String, timeoutMs: Schema.Number },
) {}

/**
 * The workspace failed in a way none of the above describes.
 *
 * Carries the driver's own classification, so an unmodelled `errno` still
 * reaches the model as something it can quote rather than as a defect that
 * kills the run.
 */
export class WorkspaceUnavailable extends Schema.TaggedErrorClass<WorkspaceUnavailable>()(
  '@sunfall/vesper-workspace/WorkspaceUnavailable',
  {
    tool: ToolOperation,
    path: Schema.optionalKey(Schema.String),
    code: Schema.String,
  },
) {}

// ------------------------------------------------------------- the plumbing

/**
 * What reaching a path can go wrong with, whatever the tool was trying to do.
 *
 * Declared as one set and carried by every path-touching tool rather than
 * whittled down per tool. The whittling is not free: `read_file` really can
 * produce `NotADirectory`, because `a/b/c` where `b` is a file is `ENOTDIR`
 * and not `ENOENT`, and a tool declaring only the failures its *happy* shape
 * suggests is how an unhandled case becomes a defect. At most one member is
 * unreachable per tool, and the cost of that is a `catchTag` nobody needs.
 */
const pathFailureSchemas = [
  FileNotFound,
  NotAFile,
  NotADirectory,
  AccessDenied,
  WorkspaceUnavailable,
] as const;

type PathFailure =
  | AccessDenied
  | FileNotFound
  | NotADirectory
  | NotAFile
  | WorkspaceUnavailable;

/**
 * Translate a driver failure into something the model can act on.
 *
 * `EISDIR` and `ENOTDIR` are promoted out of the driver's residual case
 * because they are the two mistakes a model actually makes — reading a
 * directory, listing a file — and both are recoverable with a different call.
 */
const fromFileError =
  (tool: typeof ToolOperation.Type, path: string) =>
  (error: WorkspaceDriver.FileError): PathFailure => {
    if (error._tag === '@sunfall/vesper-workspace/PathNotFound') {
      return new FileNotFound({ path });
    }
    if (error._tag === '@sunfall/vesper-workspace/PermissionDenied') {
      return new AccessDenied({ path, operation: error.operation });
    }
    if (error.code === 'EISDIR') {
      return new NotAFile({ path });
    }
    if (error.code === 'ENOTDIR') {
      return new NotADirectory({ path });
    }
    return new WorkspaceUnavailable({ tool, path, code: error.code });
  };

/** Resolve a model-supplied path against the root, or fail legibly. */
const resolvePath = (
  input: string,
): Effect.Effect<
  { readonly root: string; readonly absolute: string },
  PathOutsideWorkspace,
  Root
> =>
  Effect.gen(function* () {
    const root = yield* Root;
    const normalizedRoot = WorkspacePath.normalize(root.path);
    const resolution = WorkspacePath.resolve(normalizedRoot, input);
    if (!resolution.ok) {
      return yield* Effect.fail(
        new PathOutsideWorkspace({
          path: input,
          root: normalizedRoot,
          reason: resolution.reason,
        }),
      );
    }
    return { root: normalizedRoot, absolute: resolution.path };
  });

/** Read a file as text, refusing anything that is not UTF-8. */
const readText = (
  tool: typeof ToolOperation.Type,
  absolute: string,
): Effect.Effect<
  string,
  BinaryContent | PathFailure,
  WorkspaceDriver.Service
> =>
  Effect.gen(function* () {
    const driver = yield* WorkspaceDriver.Service;
    const bytes = yield* driver
      .readFileBuffer(absolute)
      .pipe(Effect.mapError(fromFileError(tool, absolute)));
    const decoded = WorkspaceOutput.decodeText(bytes);
    return decoded.ok
      ? decoded.text
      : yield* Effect.fail(
          new BinaryContent({ path: absolute, reason: decoded.reason }),
        );
  });

interface WalkEntry {
  readonly path: string;
  readonly type: 'directory' | 'file' | 'symlink';
  readonly size: number | undefined;
}

interface WalkResult {
  readonly entries: ReadonlyArray<WalkEntry>;
  readonly ignoredDirectories: ReadonlyArray<string>;
  readonly unreadableDirectories: ReadonlyArray<string>;
  readonly truncated: boolean;
}

/**
 * Walk a directory tree through the driver.
 *
 * Breadth-first over `readdir` and `stat` rather than a shell `find`, because
 * a remote driver is not obliged to have `find`, and building a command string
 * out of a model-supplied path is a quoting bug waiting to happen.
 *
 * Three things it will not do quietly:
 *
 * - **Descend a symlink.** A link to `..` is an infinite tree and a link out
 *   of the root defeats the containment check. Links are listed, as links.
 * - **Fail on an unreadable subdirectory.** One `EACCES` deep in the tree
 *   should not lose the rest of the walk, so it is recorded and reported.
 *   The *root* directory failing does fail the call — that is the model's
 *   mistake, not an obstacle in the tree.
 * - **Run forever.** {@link MAX_WALK_ENTRIES} caps it, and hitting the cap
 *   sets `truncated`.
 */
const walk = (
  tool: typeof ToolOperation.Type,
  directory: string,
): Effect.Effect<WalkResult, PathFailure, WorkspaceDriver.Service> =>
  Effect.gen(function* () {
    const driver = yield* WorkspaceDriver.Service;
    const entries: Array<WalkEntry> = [];
    const ignoredDirectories: Array<string> = [];
    const unreadableDirectories: Array<string> = [];
    const pending: Array<string> = [''];
    let truncated = false;
    let isRoot = true;

    while (pending.length > 0 && !truncated) {
      const relative = pending.shift() ?? '';
      const absolute = relative === '' ? directory : `${directory}/${relative}`;

      const listing = yield* driver
        .readdir(absolute)
        .pipe(Effect.mapError(fromFileError(tool, absolute)), Effect.result);

      if (listing._tag === 'Failure') {
        if (isRoot) {
          return yield* Effect.fail(listing.failure);
        }
        unreadableDirectories.push(relative);
        continue;
      }
      isRoot = false;

      for (const name of [...listing.success].sort()) {
        if (entries.length >= MAX_WALK_ENTRIES) {
          truncated = true;
          break;
        }

        const childRelative = relative === '' ? name : `${relative}/${name}`;
        const stat = yield* driver
          .stat(`${absolute}/${name}`)
          .pipe(Effect.option);

        // A `stat` that failed is an entry that vanished between the listing
        // and the check, or one we may not look at. Either way it is not
        // something to report a type for.
        if (stat._tag === 'None') {
          continue;
        }

        const type = stat.value.isSymbolicLink
          ? 'symlink'
          : stat.value.isDirectory
            ? 'directory'
            : 'file';

        entries.push({ path: childRelative, type, size: stat.value.size });

        if (type === 'directory') {
          if (IGNORED_DIRECTORIES.includes(name)) {
            ignoredDirectories.push(childRelative);
          } else {
            pending.push(childRelative);
          }
        }
      }
    }

    return { entries, ignoredDirectories, unreadableDirectories, truncated };
  });

// ----------------------------------------------------------------- the tools

const TruncatedBy = Schema.NullOr(Schema.Literals(['lines', 'bytes']));

/**
 * A whole number, for the parameters a model fills in.
 *
 * `Schema.Int` and not `Schema.Number`, and the difference is not stylistic —
 * it is what the model is shown. `Schema.Number` admits the three non-finite
 * values encoded as strings, so `Tool.getJsonSchema` renders it as
 *
 * ```json
 * {"anyOf":[{"type":"number"},
 *           {"type":"string","enum":["Infinity","-Infinity","NaN"]}]}
 * ```
 *
 * which tells the model a string is a legal value for a line count. Anthropic
 * ignores the alternative and sends a number; OpenAI took it, and the tool call
 * then failed `LanguageModel`'s decode with `Expected number at
 * ["params"]["limit"]`. That failure is an `InvalidOutputError` on the stream
 * rather than a tool result the model could correct, so it does not degrade the
 * turn — it kills the run. Found by `@sunfall/vesper-runtime`'s live smoke script the
 * first time this toolkit met a non-Anthropic provider.
 *
 * `Schema.Int` renders as `{"type":"integer"}`, which is both what these
 * parameters actually are — line offsets, item counts, a millisecond timeout —
 * and a schema with nothing in it for a model to misread. The handlers still
 * clamp and truncate, because a model can send `-3` inside a valid integer.
 */
const Count = Schema.Int;

const readFileTool = Tool.make('read_file', {
  description:
    'Read a UTF-8 text file from the workspace. Paths are relative to the ' +
    'workspace root. Long files come back truncated; read the rest by ' +
    'passing `offset`.',
  parameters: Schema.Struct({
    path: Schema.String,
    /** 1-based line to start at. */
    offset: Schema.optionalKey(Count),
    /** Maximum lines to return. */
    limit: Schema.optionalKey(Count),
  }),
  success: Schema.Struct({
    path: Schema.String,
    content: Schema.String,
    firstLine: Schema.Number,
    lineCount: Schema.Number,
    totalLines: Schema.Number,
    truncated: Schema.Boolean,
    truncatedBy: TruncatedBy,
  }),
  failure: Schema.Union([
    PathOutsideWorkspace,
    BinaryContent,
    ...pathFailureSchemas,
  ]),
  // Every one of these is something the model can fix on its next turn — a
  // different path, a different tool, a smaller window. Aborting the run
  // instead would make a mistyped filename fatal.
  failureMode: 'return',
  dependencies: [Root, WorkspaceDriver.Service],
});

const writeFileTool = Tool.make('write_file', {
  description:
    'Write a UTF-8 text file in the workspace, creating it and any missing ' +
    'parent directories, or replacing it entirely if it exists. To change ' +
    'part of an existing file, use edit_file instead.',
  parameters: Schema.Struct({
    path: Schema.String,
    content: Schema.String,
  }),
  success: Schema.Struct({
    path: Schema.String,
    bytesWritten: Schema.Number,
    created: Schema.Boolean,
  }),
  failure: Schema.Union([PathOutsideWorkspace, ...pathFailureSchemas]),
  failureMode: 'return',
  dependencies: [Root, WorkspaceDriver.Service],
});

const editFileTool = Tool.make('edit_file', {
  description:
    'Replace an exact string in a workspace file. Fails without writing if ' +
    'the string is absent, or if it occurs more than once and `replaceAll` ' +
    'is not set. Include enough surrounding text to make it unique.',
  parameters: Schema.Struct({
    path: Schema.String,
    // Non-empty by schema: an empty target matches at every position, so
    // there is no sound answer to give and a parameter error says so before
    // the handler has to invent one.
    oldText: Schema.NonEmptyString,
    newText: Schema.String,
    replaceAll: Schema.optionalKey(Schema.Boolean),
  }),
  success: Schema.Struct({
    path: Schema.String,
    replacements: Schema.Number,
  }),
  failure: Schema.Union([
    PathOutsideWorkspace,
    BinaryContent,
    EditTargetMissing,
    EditTargetAmbiguous,
    ...pathFailureSchemas,
  ]),
  failureMode: 'return',
  dependencies: [Root, WorkspaceDriver.Service],
});

const ListedEntry = Schema.Struct({
  path: Schema.String,
  type: Schema.Literals(['file', 'directory', 'symlink']),
});

const listFilesTool = Tool.make('list_files', {
  description:
    'List workspace entries under a directory, optionally filtered by a ' +
    'glob such as `**/*.ts`. Supports `*`, `**`, `?` and `[abc]`; brace ' +
    'alternation is not supported. `.git` and `node_modules` are not ' +
    'descended into and are reported in `ignoredDirectories`.',
  parameters: Schema.Struct({
    /** Directory to walk. Defaults to the workspace root. */
    path: Schema.optionalKey(Schema.String),
    /** Glob matched against paths relative to `path`. */
    pattern: Schema.optionalKey(Schema.String),
    limit: Schema.optionalKey(Count),
  }),
  success: Schema.Struct({
    directory: Schema.String,
    entries: Schema.Array(ListedEntry),
    truncated: Schema.Boolean,
    ignoredDirectories: Schema.Array(Schema.String),
    unreadableDirectories: Schema.Array(Schema.String),
  }),
  failure: Schema.Union([PathOutsideWorkspace, ...pathFailureSchemas]),
  failureMode: 'return',
  dependencies: [Root, WorkspaceDriver.Service],
});

const SearchMatch = Schema.Struct({
  path: Schema.String,
  line: Schema.Number,
  text: Schema.String,
});

const searchFilesTool = Tool.make('search_files', {
  description:
    'Search workspace file contents with a JavaScript regular expression, ' +
    'returning one entry per matching line. Narrow the files with `glob`. ' +
    'Binary and very large files are skipped and counted, so a zero-match ' +
    'result can be told apart from a search that did not look.',
  parameters: Schema.Struct({
    pattern: Schema.String,
    /** Directory to search. Defaults to the workspace root. */
    path: Schema.optionalKey(Schema.String),
    /** Glob matched against paths relative to `path`. */
    glob: Schema.optionalKey(Schema.String),
    ignoreCase: Schema.optionalKey(Schema.Boolean),
    limit: Schema.optionalKey(Count),
  }),
  success: Schema.Struct({
    directory: Schema.String,
    matches: Schema.Array(SearchMatch),
    truncated: Schema.Boolean,
    filesSearched: Schema.Number,
    binaryFilesSkipped: Schema.Number,
    largeFilesSkipped: Schema.Number,
  }),
  failure: Schema.Union([
    PathOutsideWorkspace,
    InvalidPattern,
    ...pathFailureSchemas,
  ]),
  failureMode: 'return',
  dependencies: [Root, WorkspaceDriver.Service],
});

const runShellTool = Tool.make('run_shell', {
  description:
    'Run a shell command in the workspace and read its output. A non-zero ' +
    'exit is a normal result, reported in `exitCode` with the output ' +
    'intact — it is not an error. Output is truncated from the front, so ' +
    'the end of a long build log survives.',
  parameters: Schema.Struct({
    command: Schema.NonEmptyString,
    /** Working directory. Defaults to the workspace root. */
    cwd: Schema.optionalKey(Schema.String),
    timeoutMs: Schema.optionalKey(Count),
  }),
  success: Schema.Struct({
    command: Schema.String,
    exitCode: Schema.Number,
    stdout: Schema.String,
    stderr: Schema.String,
    stdoutTruncated: Schema.Boolean,
    stderrTruncated: Schema.Boolean,
  }),
  failure: Schema.Union([
    PathOutsideWorkspace,
    CommandTimedOut,
    ...pathFailureSchemas,
  ]),
  failureMode: 'return',
  dependencies: [Root, WorkspaceDriver.Service],
});

/**
 * The six tools, ready to hand to an agent.
 *
 * ```ts
 * const agent = Agent.make({
 *   name: 'coder',
 *   instructions: '…',
 *   toolkit: WorkspaceTools.toolkit,
 * });
 * ```
 *
 * `Root` and `WorkspaceDriver.Service` ride along in the agent's requirement
 * channel from there; {@link layer} supplies the handlers and nothing else.
 */
export const toolkit = Toolkit.make(
  readFileTool,
  writeFileTool,
  editFileTool,
  listFilesTool,
  searchFilesTool,
  runShellTool,
);

// -------------------------------------------------------------- the handlers

const handleRead = Effect.fnUntraced(function* (params: {
  readonly path: string;
  readonly offset?: number;
  readonly limit?: number;
}) {
  const { absolute, root } = yield* resolvePath(params.path);
  const text = yield* readText('read', absolute);

  const allLines = text.split('\n');
  const firstLine = Math.max(1, Math.trunc(params.offset ?? 1));
  const window = allLines.slice(
    firstLine - 1,
    params.limit === undefined
      ? undefined
      : firstLine - 1 + Math.max(0, Math.trunc(params.limit)),
  );

  const truncation = WorkspaceOutput.head(window.join('\n'), {
    maxLines: params.limit,
  });

  return {
    path: WorkspacePath.relative(root, absolute),
    content: truncation.content,
    firstLine,
    lineCount: truncation.outputLines,
    totalLines: allLines.length,
    // Slicing to a window is itself a truncation, and reporting `false`
    // because the *window* fit would tell the model it had seen the file.
    truncated:
      truncation.truncated || firstLine - 1 + window.length < allLines.length,
    truncatedBy: truncation.truncatedBy,
  };
});

const handleWrite = Effect.fnUntraced(function* (params: {
  readonly path: string;
  readonly content: string;
}) {
  const driver = yield* WorkspaceDriver.Service;
  const { absolute, root } = yield* resolvePath(params.path);

  const existing = yield* driver
    .stat(absolute)
    .pipe(Effect.mapError(fromFileError('write', absolute)), Effect.result);

  if (existing._tag === 'Success' && !existing.success.isFile) {
    return yield* Effect.fail(new NotAFile({ path: absolute }));
  }
  // A missing file is the ordinary case for a write; anything else that
  // blocked the `stat` will block the write too, and is reported there.
  const created = existing._tag === 'Failure';

  const parent = absolute.slice(0, absolute.lastIndexOf('/'));
  if (parent !== '' && parent !== root) {
    yield* driver
      .mkdir(parent, { recursive: true })
      .pipe(Effect.mapError(fromFileError('write', parent)));
  }

  yield* driver
    .writeFile(absolute, params.content)
    .pipe(Effect.mapError(fromFileError('write', absolute)));

  return {
    path: WorkspacePath.relative(root, absolute),
    bytesWritten: WorkspaceOutput.utf8Size(params.content),
    created,
  };
});

const handleEdit = Effect.fnUntraced(function* (params: {
  readonly path: string;
  readonly oldText: string;
  readonly newText: string;
  readonly replaceAll?: boolean;
}) {
  const driver = yield* WorkspaceDriver.Service;
  const { absolute, root } = yield* resolvePath(params.path);
  const text = yield* readText('write', absolute);

  const occurrences = text.split(params.oldText).length - 1;
  if (occurrences === 0) {
    return yield* Effect.fail(
      new EditTargetMissing({ path: absolute, target: params.oldText }),
    );
  }
  if (occurrences > 1 && params.replaceAll !== true) {
    return yield* Effect.fail(
      new EditTargetAmbiguous({
        path: absolute,
        target: params.oldText,
        occurrences,
      }),
    );
  }

  // `split`/`join` rather than `replace`: `$&` and friends in `newText` are
  // substitution patterns to `String.replace`, so a model editing a regular
  // expression or a shell script would get text it did not write.
  const updated =
    params.replaceAll === true
      ? text.split(params.oldText).join(params.newText)
      : text.replace(params.oldText, () => params.newText);

  yield* driver
    .writeFile(absolute, updated)
    .pipe(Effect.mapError(fromFileError('write', absolute)));

  return {
    path: WorkspacePath.relative(root, absolute),
    replacements: params.replaceAll === true ? occurrences : 1,
  };
});

const handleList = Effect.fnUntraced(function* (params: {
  readonly path?: string;
  readonly pattern?: string;
  readonly limit?: number;
}) {
  const { absolute, root } = yield* resolvePath(params.path ?? '.');
  const walked = yield* walk('list', absolute);

  const match =
    params.pattern === undefined
      ? () => true
      : (path: string): boolean =>
          WorkspaceGlob.matches(params.pattern ?? '', path);

  const limit = Math.max(0, Math.trunc(params.limit ?? 1000));
  const matched = walked.entries.filter((entry) => match(entry.path));
  const entries = matched.slice(0, limit);

  return {
    directory: WorkspacePath.relative(root, absolute),
    entries: entries.map((entry) => ({ path: entry.path, type: entry.type })),
    truncated: walked.truncated || matched.length > entries.length,
    ignoredDirectories: walked.ignoredDirectories,
    unreadableDirectories: walked.unreadableDirectories,
  };
});

const handleSearch = Effect.fnUntraced(function* (params: {
  readonly pattern: string;
  readonly path?: string;
  readonly glob?: string;
  readonly ignoreCase?: boolean;
  readonly limit?: number;
}) {
  const driver = yield* WorkspaceDriver.Service;
  const { absolute, root } = yield* resolvePath(params.path ?? '.');

  const expression = yield* Effect.try({
    try: () =>
      new RegExp(params.pattern, params.ignoreCase === true ? 'iu' : 'u'),
    catch: (error) =>
      new InvalidPattern({
        pattern: params.pattern,
        reason: error instanceof Error ? error.message : String(error),
      }),
  });

  const walked = yield* walk('search', absolute);
  const limit = Math.max(0, Math.trunc(params.limit ?? 200));

  const candidates = walked.entries.filter(
    (entry) =>
      entry.type === 'file' &&
      (params.glob === undefined ||
        WorkspaceGlob.matches(params.glob, entry.path)),
  );

  const matches: Array<{
    readonly path: string;
    readonly line: number;
    readonly text: string;
  }> = [];
  let filesSearched = 0;
  let binaryFilesSkipped = 0;
  let largeFilesSkipped = 0;
  let truncated = walked.truncated;

  for (const candidate of candidates) {
    if (matches.length >= limit) {
      truncated = true;
      break;
    }
    if (candidate.size !== undefined && candidate.size > MAX_SEARCHABLE_BYTES) {
      largeFilesSkipped += 1;
      continue;
    }

    const bytes = yield* driver
      .readFileBuffer(`${absolute}/${candidate.path}`)
      .pipe(Effect.option);
    if (bytes._tag === 'None') {
      continue;
    }

    const decoded = WorkspaceOutput.decodeText(bytes.value);
    if (!decoded.ok) {
      binaryFilesSkipped += 1;
      continue;
    }

    filesSearched += 1;
    const lines = decoded.text.split('\n');
    for (let index = 0; index < lines.length; index += 1) {
      if (matches.length >= limit) {
        truncated = true;
        break;
      }
      const line = lines[index]!;
      // `lastIndex` is not carried between calls because the expression is
      // built without `g`, so this stays a plain per-line predicate.
      if (expression.test(line)) {
        matches.push({
          path: candidate.path,
          line: index + 1,
          text:
            line.length > MATCH_LINE_MAX_CHARS
              ? `${line.slice(0, MATCH_LINE_MAX_CHARS)}…`
              : line,
        });
      }
    }
  }

  return {
    directory: WorkspacePath.relative(root, absolute),
    matches,
    truncated,
    filesSearched,
    binaryFilesSkipped,
    largeFilesSkipped,
  };
});

const handleRun = Effect.fnUntraced(function* (params: {
  readonly command: string;
  readonly cwd?: string;
  readonly timeoutMs?: number;
}) {
  const driver = yield* WorkspaceDriver.Service;
  const { absolute } = yield* resolvePath(params.cwd ?? '.');
  const timeoutMs = params.timeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS;

  const result = yield* driver
    .exec(params.command, { cwd: absolute, timeoutMs })
    .pipe(
      Effect.mapError((error): CommandTimedOut | PathFailure =>
        error._tag === '@sunfall/vesper-workspace/CommandTimeout'
          ? new CommandTimedOut({ command: params.command, timeoutMs })
          : fromFileError('run', absolute)(error),
      ),
    );

  // From the front: a build prints thousands of lines of progress and then
  // the error, and the error is the part worth spending context on.
  const stdout = WorkspaceOutput.tail(result.stdout);
  const stderr = WorkspaceOutput.tail(result.stderr);

  return {
    command: params.command,
    exitCode: result.exitCode,
    stdout: stdout.content,
    stderr: stderr.content,
    stdoutTruncated: stdout.truncated,
    stderrTruncated: stderr.truncated,
  };
});

/**
 * The handlers, as a layer.
 *
 * Discharges the tool-handler term and nothing else: `Root` and
 * `WorkspaceDriver.Service` are still the application's to provide, which is
 * the whole design.
 *
 * ```ts
 * run.pipe(
 *   Effect.provide(WorkspaceTools.layer),
 *   Effect.provide(WorkspaceTools.rootLayer('/work')),
 *   Effect.provide(WorkspaceLocal.layer.pipe(Layer.provide(NodeServices.layer))),
 * );
 * ```
 */
export const layer: Layer.Layer<
  Tool.HandlersFor<Toolkit.Tools<typeof toolkit>>
> = toolkit.toLayer({
  read_file: handleRead,
  write_file: handleWrite,
  edit_file: handleEdit,
  list_files: handleList,
  search_files: handleSearch,
  run_shell: handleRun,
});

export * as WorkspaceTools from './tools.js';
