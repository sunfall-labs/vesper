import { readFile } from 'node:fs/promises';

/** @returns {Promise<Record<string, unknown>>} */
const readManifest = async () => {
  const value = /** @type {unknown} */ (
    JSON.parse(
      await readFile(new URL('../package.json', import.meta.url), 'utf8'),
    )
  );
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('package.json must contain an object');
  }
  return value;
};

const configured = (await readManifest())['patchedDependencies'];
if (
  configured !== undefined &&
  (typeof configured !== 'object' ||
    configured === null ||
    Array.isArray(configured))
) {
  throw new Error('package.json patchedDependencies must contain an object');
}
const patches = configured === undefined ? [] : Object.keys(configured);

if (patches.length > 0) {
  throw new Error(
    `Refusing to publish Vesper with dependency patches: ${patches.join(', ')}. ` +
      'Land the tool-call boundary fix upstream, upgrade to that Effect release, ' +
      'remove the patch, and run the full packed-consumer gate first.',
  );
}
