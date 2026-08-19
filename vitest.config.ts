import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { defineConfig } from 'vitest/config';

// Tests run from the repository root against package **sources**, not against
// built output, so a failing test points at the file you would edit. The
// TypeScript paths are the canonical public source map; Vitest inherits it so
// this config cannot drift into a second list of aliases.
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;
const isStringArray = (value: unknown): value is ReadonlyArray<string> =>
  Array.isArray(value) && value.every((item) => typeof item === 'string');

const parsed: unknown = JSON.parse(
  readFileSync(new URL('./tsconfig.base.json', import.meta.url), 'utf8'),
);
const tsconfig = isRecord(parsed) ? parsed : {};
const compilerOptions = isRecord(tsconfig.compilerOptions)
  ? tsconfig.compilerOptions
  : {};
const paths = isRecord(compilerOptions.paths) ? compilerOptions.paths : {};

const alias = Object.fromEntries(
  Object.entries(paths)
    .filter(
      ([specifier, targets]) =>
        specifier.startsWith('@sunfall/vesper-') && isStringArray(targets),
    )
    .map(([specifier, targets]) => {
      if (!isStringArray(targets)) {
        throw new Error(`Invalid source targets for ${specifier}`);
      }
      const source = targets.at(0);
      if (source === undefined) {
        throw new Error(`Missing source target for ${specifier}`);
      }
      return [specifier, fileURLToPath(new URL(source, import.meta.url))];
    }),
);

export default defineConfig({
  resolve: { alias },
  test: {
    name: 'vesper',
    globals: true,
    environment: 'node',
    include: ['packages/*/test/**/*.test.ts', 'examples/*/src/**/*.test.ts'],
    // The Postgres suite provisions and drops a real database in
    // beforeAll/afterAll. Vitest's 10s hook default is not enough for that
    // under parallel load, and afterAll hooks rarely pass their own timeout.
    hookTimeout: 180_000,
  },
});
