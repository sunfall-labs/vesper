// Regenerates every Effect dependency patch in `patches/` from a local Effect
// checkout, so a patch is always the exact built output of a known branch.
//
//   node scripts/regenerate-effect-patches.mjs [--effect <dir>] [--base <ref>]
//
// `--effect` is the Effect monorepo checkout (default
// ~/Developer/Personal/effect-upstream, current branch). `--base` is the git
// ref the branch is diffed against to find the files it touches (default: the
// merge base with `upstream/main`, falling back to `main`). Only those files
// (`src/**` plus the matching `dist/*.js` and `dist/*.d.ts`, no source maps)
// are overlaid on the pinned registry package, so main-branch drift outside
// the touched modules never enters a patch.
//
// For each package in `patchedDependencies` (or the default Effect family when
// none is configured) this builds it in the Effect checkout, extracts the
// pristine catalog version with `nub patch`, overlays the touched files, and
// commits the result with `nub patch-commit`. nub omits the `new file mode`
// header git and bun require for created files, so the script adds it.
//
// This is a repository-maintenance script and deliberately uses only `node:`
// modules.

import { execFileSync } from 'node:child_process';
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { homedir, tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const nub = resolve(root, 'node_modules/.bin/nub');
const defaultPackages = [
  'effect',
  '@effect/ai-openai',
  '@effect/ai-anthropic',
  '@effect/ai-openrouter',
];

/** @param {string} flag @param {string} fallback */
const argument = (flag, fallback) => {
  const index = process.argv.indexOf(flag);
  if (index === -1) {
    return fallback;
  }
  const value = process.argv.at(index + 1);
  if (value === undefined || value.startsWith('--')) {
    throw new Error(`${flag} requires a value`);
  }
  return value;
};

const effectRoot = resolve(
  argument('--effect', join(homedir(), 'Developer/Personal/effect-upstream')),
);

/**
 * @param {string} command
 * @param {ReadonlyArray<string>} args
 * @param {string} cwd
 */
const run = (command, args, cwd) =>
  execFileSync(command, args, { cwd, encoding: 'utf8', stdio: 'pipe' });

/** @param {unknown} value @returns {value is Record<string, unknown>} */
const isObject = (value) =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/** @param {string} path */
const readJson = async (path) => {
  const value = /** @type {unknown} */ (
    JSON.parse(await readFile(path, 'utf8'))
  );
  if (!isObject(value)) {
    throw new Error(`${path} must contain a JSON object`);
  }
  return value;
};

/** @param {string} path */
const sizeOf = async (path) => {
  try {
    return (await stat(path)).size;
  } catch {
    return undefined;
  }
};

// --- Vesper side: which packages, which versions, which patch files ---------

const manifestPath = resolve(root, 'package.json');
const manifest = await readJson(manifestPath);
const workspaces = manifest['workspaces'];
const catalog = isObject(workspaces) ? workspaces['catalog'] : undefined;
if (!isObject(catalog)) {
  throw new Error('package.json must declare workspaces.catalog');
}
const configured = manifest['patchedDependencies'];
if (configured !== undefined && !isObject(configured)) {
  throw new Error('package.json patchedDependencies must be an object');
}
const packageNames =
  configured === undefined
    ? defaultPackages
    : Object.keys(configured).map((selector) => {
        const at = selector.lastIndexOf('@');
        return at <= 0 ? selector : selector.slice(0, at);
      });

/** @type {Map<string, { version: string, previousPatch: string | undefined, previousSize: number | undefined }>} */
const targets = new Map();
for (const name of packageNames) {
  const version = catalog[name];
  if (typeof version !== 'string') {
    throw new Error(`workspaces.catalog has no version for ${name}`);
  }
  const previous = configured?.[`${name}@${version}`];
  const previousPatch =
    typeof previous === 'string' ? resolve(root, previous) : undefined;
  targets.set(name, {
    version,
    previousPatch,
    previousSize:
      previousPatch === undefined ? undefined : await sizeOf(previousPatch),
  });
}

// --- Effect side: locate packages and the files the branch touches -----------

const branch = run('git', ['branch', '--show-current'], effectRoot).trim();
const head = run('git', ['rev-parse', '--short', 'HEAD'], effectRoot).trim();
const base = argument(
  '--base',
  (() => {
    for (const candidate of ['upstream/main', 'main']) {
      try {
        return run('git', ['merge-base', candidate, 'HEAD'], effectRoot).trim();
      } catch {
        // try the next candidate
      }
    }
    throw new Error(
      `Unable to find a base ref in ${effectRoot}; pass --base explicitly`,
    );
  })(),
);
/** @param {string} line */
const report = (line) => {
  process.stdout.write(`${line}\n`);
};

const branchLabel = branch.length === 0 ? 'detached' : branch;
report(
  `Effect checkout ${effectRoot} (${branchLabel} @ ${head}), base ${base}`,
);

/** @param {string} name */
const packageDirectory = async (name) => {
  const candidates = [
    join(effectRoot, 'packages', name),
    join(
      effectRoot,
      'packages',
      name.replace(/^@effect\//, '').replace(/^ai-/, 'ai/'),
    ),
  ];
  for (const candidate of candidates) {
    try {
      const packageJson = await readJson(join(candidate, 'package.json'));
      if (packageJson['name'] === name) {
        return candidate;
      }
    } catch {
      // not this candidate
    }
  }
  throw new Error(
    `Unable to locate package ${name} under ${effectRoot}/packages`,
  );
};

const touched = run('git', ['diff', '--name-only', `${base}..HEAD`], effectRoot)
  .split('\n')
  .filter((line) => line.length > 0);

/** @type {Map<string, { directory: string, sources: string[] }>} */
const plans = new Map();
for (const name of targets.keys()) {
  const directory = await packageDirectory(name);
  const packagePrefix = `${directory.slice(effectRoot.length + 1)}/`;
  const sources = touched
    .filter(
      (file) => file.startsWith(`${packagePrefix}src/`) && file.endsWith('.ts'),
    )
    .map((file) => file.slice(packagePrefix.length));
  if (sources.length === 0) {
    throw new Error(
      `Branch ${branchLabel} touches no source files of ${name}; nothing to patch`,
    );
  }
  plans.set(name, { directory, sources });
}

// --- Build the touched packages in the Effect checkout ----------------------

report(`Building ${[...plans.keys()].join(', ')} in ${effectRoot}`);
run(
  'pnpm',
  [...[...plans.keys()].flatMap((name) => ['--filter', name]), 'build'],
  effectRoot,
);

// --- Start from the pristine registry packages -------------------------------

const previousPatchFiles = [...targets.values()]
  .map(({ previousPatch }) => previousPatch)
  .filter((path) => path !== undefined);
delete manifest['patchedDependencies'];
await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
for (const path of previousPatchFiles) {
  await rm(path, { force: true });
}
report('Installing pristine catalog versions');
run(nub, ['install'], root);

// --- Generate one patch per package -----------------------------------------

const scratch = await mkdtemp(join(tmpdir(), 'vesper-effect-patches-'));

/** @param {string} patchPath */
const addNewFileHeaders = async (patchPath) => {
  const lines = (await readFile(patchPath, 'utf8')).split('\n');
  /** @type {string[]} */
  const output = [];
  let added = 0;
  for (const [index, line] of lines.entries()) {
    output.push(line);
    if (
      line.startsWith('diff --git ') &&
      lines[index + 1] === '--- /dev/null'
    ) {
      output.push('new file mode 100644');
      added += 1;
    }
  }
  if (added > 0) {
    await writeFile(patchPath, output.join('\n'));
  }
  return added;
};

/** @type {Array<{ name: string, patch: string, before: number | undefined, after: number, created: number, files: number }>} */
const summary = [];
for (const [name, { version, previousSize }] of targets) {
  const plan = plans.get(name);
  if (plan === undefined) {
    throw new Error(`No plan for ${name}`);
  }
  const editRoot = join(scratch, name.replaceAll('/', '__'));
  run(nub, ['patch', `${name}@${version}`, '--edit-dir', editRoot], root);
  // nub extracts the writable copy under `<edit-dir>/user`
  const edit =
    (await sizeOf(join(editRoot, 'user', 'package.json'))) === undefined
      ? editRoot
      : join(editRoot, 'user');

  /** @type {string[]} */
  const files = [];
  for (const source of plan.sources) {
    const stem = source.slice('src/'.length, -'.ts'.length);
    files.push(source, `dist/${stem}.js`, `dist/${stem}.d.ts`);
  }
  for (const file of files) {
    const from = join(plan.directory, file);
    if ((await sizeOf(from)) === undefined) {
      throw new Error(`Built file missing in Effect checkout: ${from}`);
    }
    const to = join(edit, file);
    await mkdir(dirname(to), { recursive: true });
    await copyFile(from, to);
  }

  run(nub, ['patch-commit', edit], root);

  const committed = await readJson(manifestPath);
  const patches = committed['patchedDependencies'];
  const relative = isObject(patches)
    ? patches[`${name}@${version}`]
    : undefined;
  if (typeof relative !== 'string') {
    throw new Error(
      `nub patch-commit did not register a patch for ${name}@${version}`,
    );
  }
  const patchPath = resolve(root, relative);
  const created = await addNewFileHeaders(patchPath);
  const after = await sizeOf(patchPath);
  if (after === undefined) {
    throw new Error(`Patch file missing after commit: ${patchPath}`);
  }
  summary.push({
    name,
    patch: relative,
    before: previousSize,
    after,
    created,
    files: files.length,
  });
}

// nub applies the freshly registered patches on install
run(nub, ['install'], root);
await rm(scratch, { recursive: true, force: true });

// --- Report -----------------------------------------------------------------

report('');
report(
  `Regenerated ${String(summary.length)} patches from ${branchLabel} @ ${head}:`,
);
for (const { name, patch, before, after, created, files } of summary) {
  const size =
    before === undefined
      ? `new, ${String(after)} bytes`
      : `${String(before)} -> ${String(after)} bytes`;
  report(
    `  ${name}: ${patch}  (${size}; ${String(files)} files overlaid, ${String(created)} created)`,
  );
}
