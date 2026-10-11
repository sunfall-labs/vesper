// Publishes every public `@sunfall/vesper-*` package.
//
// Bun packs each package, which rewrites `catalog:` and `workspace:`
// specifiers to concrete versions. npm uploads the tarball, because npm is
// what carries provenance and trusted publishing. Versions already on the
// registry are skipped, so rerunning a half-finished release finishes it.
//
// Usage: bun scripts/publish-packages.mjs --tag <dist-tag> [--provenance] [--dry-run]

import { execFileSync } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { parseArgs } from 'node:util';

const { values } = parseArgs({
  options: {
    tag: { type: 'string' },
    provenance: { type: 'boolean', default: false },
    'dry-run': { type: 'boolean', default: false },
  },
});
const tag = values.tag;
if (tag === undefined) {
  throw new Error('--tag is required');
}

const root = resolve(import.meta.dirname, '..');
const packageRoot = resolve(root, 'packages');

/** @typedef {{ name: string, version: string, directory: string, internal: string[] }} Publishable */

/** @param {unknown} value @returns {value is Record<string, unknown>} */
const isObject = (value) =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/** @param {unknown} value @returns {string[]} */
const keysOf = (value) => (isObject(value) ? Object.keys(value) : []);

/** @type {Map<string, Publishable>} */
const publishable = new Map();
for (const entry of await readdir(packageRoot, { withFileTypes: true })) {
  if (!entry.isDirectory()) {
    continue;
  }
  const directory = resolve(packageRoot, entry.name);
  const manifest = /** @type {unknown} */ (
    JSON.parse(await readFile(resolve(directory, 'package.json'), 'utf8'))
  );
  if (!isObject(manifest)) {
    throw new Error(`${entry.name}/package.json must be a JSON object`);
  }
  const name = manifest['name'];
  const version = manifest['version'];
  if (typeof name !== 'string' || typeof version !== 'string') {
    throw new Error(`${entry.name}/package.json needs a name and version`);
  }
  if (manifest['private'] === true || !name.startsWith('@sunfall/vesper-')) {
    continue;
  }
  const internal = [
    ...keysOf(manifest['dependencies']),
    ...keysOf(manifest['peerDependencies']),
    ...keysOf(manifest['optionalDependencies']),
  ].filter((dependency) => dependency.startsWith('@sunfall/vesper-'));
  publishable.set(name, { name, version, directory, internal });
}

// Dependencies first, so a consumer never sees a package whose sibling is
// missing from the registry.
/** @type {Publishable[]} */
const ordered = [];
/** @type {Set<string>} */
const visiting = new Set();
/** @param {Publishable} pkg */
const visit = (pkg) => {
  if (ordered.includes(pkg)) {
    return;
  }
  if (visiting.has(pkg.name)) {
    throw new Error(`Workspace dependency cycle through ${pkg.name}`);
  }
  visiting.add(pkg.name);
  for (const name of pkg.internal) {
    const dependency = publishable.get(name);
    if (dependency !== undefined) {
      visit(dependency);
    }
  }
  visiting.delete(pkg.name);
  ordered.push(pkg);
};
for (const pkg of publishable.values()) {
  visit(pkg);
}

/** @param {Publishable} pkg @returns {boolean} */
const isPublished = (pkg) => {
  try {
    const found = execFileSync(
      'npm',
      ['view', `${pkg.name}@${pkg.version}`, 'version'],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
    );
    return found.trim() === pkg.version;
  } catch {
    return false;
  }
};

const packDirectory = await mkdtemp(join(tmpdir(), 'vesper-publish-'));
try {
  for (const pkg of ordered) {
    if (isPublished(pkg)) {
      process.stdout.write(
        `${pkg.name}@${pkg.version} is already published; skipping.\n`,
      );
      continue;
    }
    const tarball = execFileSync(
      'bun',
      ['pm', 'pack', '--destination', packDirectory, '--quiet'],
      { cwd: pkg.directory, encoding: 'utf8' },
    ).trim();
    const args = ['publish', tarball, '--access', 'public', '--tag', tag];
    if (values.provenance) {
      args.push('--provenance');
    }
    if (values['dry-run']) {
      args.push('--dry-run');
    }
    execFileSync('npm', args, { stdio: 'inherit' });
  }
} finally {
  await rm(packDirectory, { recursive: true, force: true });
}
