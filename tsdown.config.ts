import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { defineConfig } from 'tsdown';

const packages = [
  'agent',
  'attachments',
  'log',
  'log-pg',
  'log-sqlite',
  'mcp',
  'workspace',
] as const;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;
const parseJson = (source: string): unknown => JSON.parse(source);

const packageEntries = (name: (typeof packages)[number]): string[] => {
  const manifest = parseJson(
    readFileSync(resolve(`packages/${name}/package.json`), 'utf8'),
  );
  if (!isRecord(manifest) || !isRecord(manifest.exports)) {
    throw new Error(`Invalid package manifest exports for ${name}`);
  }
  return Object.entries(manifest.exports).flatMap(([specifier, target]) =>
    typeof target === 'object' && target !== null
      ? [`src/${specifier.slice(2)}.ts`]
      : [],
  );
};

const shared = {
  root: 'src',
  outDir: 'dist',
  clean: true,
  unbundle: true,
  dts: true,
  deps: { neverBundle: true },
  platform: 'node',
  target: 'es2022',
  format: 'esm',
  treeshake: false,
  fixedExtension: false,
  publint: { level: 'error' },
  attw: {
    profile: 'esm-only',
    level: 'error',
    excludeEntrypoints: [/\/migrations\//],
  },
} as const;

export default defineConfig(
  packages.map((name) =>
    Object.assign({}, shared, {
      name,
      cwd: `packages/${name}`,
      // The private benchmark imports the built low-level log by relative
      // path. Keep it as a build entry for complete declarations without
      // publishing a package export that applications can depend on.
      entry:
        name === 'agent'
          ? [...packageEntries(name), 'src/log.ts']
          : packageEntries(name),
    }),
  ),
);
