import { readdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const packageRoot = resolve(root, 'packages');
/** @typedef {Record<string, unknown>} JsonObject */

/** @param {string} source @param {string} description @returns {JsonObject} */
const parseObject = (source, description) => {
  const value = /** @type {unknown} */ (JSON.parse(source));
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`${description} must be a JSON object`);
  }
  return value;
};

const tsconfig = parseObject(
  await readFile(resolve(root, 'tsconfig.base.json'), 'utf8'),
  'tsconfig.base.json',
);
const compilerOptions = tsconfig['compilerOptions'];
if (
  typeof compilerOptions !== 'object' ||
  compilerOptions === null ||
  Array.isArray(compilerOptions)
) {
  throw new Error('tsconfig.base.json compilerOptions must be an object');
}
const actual = compilerOptions['paths'];
if (typeof actual !== 'object' || actual === null || Array.isArray(actual)) {
  throw new Error('tsconfig.base.json compilerOptions.paths must be an object');
}
const expected = {};

for (const entry of await readdir(packageRoot, { withFileTypes: true })) {
  if (!entry.isDirectory()) {
    continue;
  }
  const manifest = parseObject(
    await readFile(resolve(packageRoot, entry.name, 'package.json'), 'utf8'),
    `${entry.name}/package.json`,
  );
  const name = manifest['name'];
  const exports = manifest['exports'];
  if (
    typeof name !== 'string' ||
    typeof exports !== 'object' ||
    exports === null ||
    Array.isArray(exports)
  ) {
    throw new Error(`${entry.name}/package.json has invalid name or exports`);
  }

  for (const [subpath, target] of Object.entries(exports)) {
    if (typeof target !== 'object' || target === null) {
      continue;
    }
    const specifier = `${name}/${subpath.slice(2)}`;
    expected[specifier] = [
      `./packages/${entry.name}/src/${subpath.slice(2)}.ts`,
    ];
  }
}

const mismatches = [];
for (const [specifier, targets] of Object.entries(expected)) {
  const configured = actual[specifier];
  if (JSON.stringify(configured) !== JSON.stringify(targets)) {
    mismatches.push(
      `${specifier}: expected ${JSON.stringify(targets)}, received ${JSON.stringify(configured)}`,
    );
  }
}

for (const specifier of Object.keys(actual)) {
  if (
    specifier.startsWith('@sunfall/vesper-') &&
    expected[specifier] === undefined
  ) {
    mismatches.push(`${specifier}: no matching package export`);
  }
}

if (mismatches.length > 0) {
  throw new Error(
    `Package exports and source path aliases must match:\n${mismatches.join('\n')}`,
  );
}
