import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { defineConfig } from 'tsdown';

const packages = ['agent', 'attachments', 'log', 'workspace'] as const;

const packageEntries = (name: (typeof packages)[number]): string[] => {
  const manifest = JSON.parse(
    readFileSync(resolve(`packages/${name}/package.json`), 'utf8'),
  ) as { readonly exports: Record<string, unknown> };
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
      entry: packageEntries(name),
    }),
  ),
);
