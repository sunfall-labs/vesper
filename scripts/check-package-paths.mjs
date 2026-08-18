import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const tsconfig = JSON.parse(
  await readFile(resolve(root, 'tsconfig.base.json'), 'utf8'),
);
const paths = tsconfig.compilerOptions.paths;
const packages = ['agent', 'attachments', 'log', 'workspace'];
const missing = [];

for (const directory of packages) {
  const manifest = JSON.parse(
    await readFile(
      resolve(root, 'packages', directory, 'package.json'),
      'utf8',
    ),
  );
  for (const [entry, target] of Object.entries(manifest.exports)) {
    if (typeof target === 'string') continue;
    const specifier = `${manifest.name}/${entry.slice(2)}`;
    if (paths[specifier] === undefined) missing.push(specifier);
  }
}

if (missing.length > 0) {
  throw new Error(
    `Package exports missing source path aliases:\n${missing.join('\n')}`,
  );
}
