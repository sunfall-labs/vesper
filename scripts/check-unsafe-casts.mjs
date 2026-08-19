import { readdir, readFile } from 'node:fs/promises';
import { extname, relative, resolve } from 'node:path';
import { parseSync, Visitor } from 'oxc-parser';

const root = resolve(import.meta.dirname, '..');
const ignored = new Set(['.git', 'dist', 'node_modules']);
const extensions = new Set(['.ts', '.tsx', '.mts', '.cts']);
const forbiddenTypes = new Set([
  'TSAnyKeyword',
  'TSUnknownKeyword',
  'TSNeverKeyword',
]);
/** @type {string[]} */
const failures = [];

/**
 * @param {string} path
 * @param {string} source
 * @param {number} offset
 * @returns {string}
 */
const location = (path, source, offset) => {
  const before = source.slice(0, offset);
  const line = before.split('\n').length;
  const lastBreak = before.lastIndexOf('\n');
  return `${relative(root, path)}:${String(line)}:${String(offset - lastBreak)}`;
};

/** @param {string} path @returns {Promise<void>} */
const inspect = async (path) => {
  const source = await readFile(path, 'utf8');
  const result = parseSync(path, source);
  const errors = result.errors.filter(
    /** @param {import('oxc-parser').OxcError} error */
    (error) => error.severity.toString() === 'Error',
  );

  if (errors.length > 0) {
    throw new Error(
      `Could not inspect ${relative(root, path)}:\n${errors
        .map(
          /** @param {import('oxc-parser').OxcError} error */
          (error) => error.message,
        )
        .join('\n')}`,
    );
  }

  new Visitor({
    TSAsExpression(node) {
      if (
        forbiddenTypes.has(node.typeAnnotation.type) ||
        node.expression.type === 'TSAsExpression'
      ) {
        failures.push(location(path, source, node.start));
      }
    },
  }).visit(result.program);
};

/** @param {string} directory @returns {Promise<void>} */
const visitDirectory = async (directory) => {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (ignored.has(entry.name)) {
      continue;
    }
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      await visitDirectory(path);
    } else if (extensions.has(extname(entry.name))) {
      await inspect(path);
    }
  }
};

await visitDirectory(root);

if (failures.length > 0) {
  throw new Error(
    `Unsafe type assertions are forbidden (any, unknown, never, or double assertions):\n${failures.join('\n')}`,
  );
}
