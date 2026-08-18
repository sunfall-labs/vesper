import { readdir, readFile } from 'node:fs/promises';
import { relative, resolve } from 'node:path';
import { parseSync, Visitor } from 'oxc-parser';

const root = resolve(import.meta.dirname, '..');
const packages = resolve(root, 'packages');
const failures = [];

const location = (path, source, offset) => {
  const before = source.slice(0, offset);
  const line = before.split('\n').length;
  const lastBreak = before.lastIndexOf('\n');
  return `${relative(root, path)}:${line}:${offset - lastBreak}`;
};

const inspect = async (path) => {
  const source = await readFile(path, 'utf8');
  const result = parseSync(path, source, { lang: 'dts' });
  const errors = result.errors.filter((error) => error.severity === 'Error');

  if (errors.length > 0) {
    throw new Error(
      `Could not inspect ${relative(root, path)}:\n${errors.map((error) => error.message).join('\n')}`,
    );
  }

  new Visitor({
    TSAnyKeyword(node) {
      failures.push(location(path, source, node.start));
    },
  }).visit(result.program);
};

const visitDeclarations = async (directory) => {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      await visitDeclarations(path);
    } else if (entry.name.endsWith('.d.ts')) {
      await inspect(path);
    }
  }
};

for (const entry of await readdir(packages, { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  await visitDeclarations(resolve(packages, entry.name, 'dist'));
}

if (failures.length > 0) {
  throw new Error(
    `Published declarations must not expose explicit any types:\n${failures.join('\n')}`,
  );
}
