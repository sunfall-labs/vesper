import { defineConfig } from 'tsdown';

const packages = ['agent', 'attachments', 'log', 'workspace'];

const shared = {
  entry: 'src/**/*.ts',
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
    Object.assign({}, shared, { name, cwd: `packages/${name}` }),
  ),
);
