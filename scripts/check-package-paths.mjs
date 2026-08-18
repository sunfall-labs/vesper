import { readdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const packageRoot = resolve(root, 'packages');
const tsconfig = JSON.parse(
  await readFile(resolve(root, 'tsconfig.base.json'), 'utf8'),
);
const actual = tsconfig.compilerOptions.paths;
const expected = {};

for (const entry of await readdir(packageRoot, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const manifest = JSON.parse(
    await readFile(resolve(packageRoot, entry.name, 'package.json'), 'utf8'),
  );

  for (const [subpath, target] of Object.entries(manifest.exports)) {
    if (typeof target !== 'object' || target === null) continue;
    const specifier = `${manifest.name}/${subpath.slice(2)}`;
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
