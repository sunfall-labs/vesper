import { execFileSync } from 'node:child_process';
import {
  mkdtemp,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const packageRoot = resolve(root, 'packages');
const audit = process.argv.includes('--audit');
const temporaryRoot = await mkdtemp(join(tmpdir(), 'vesper-packed-consumer-'));
const packDirectory = resolve(temporaryRoot, 'packs');
const consumerDirectory = resolve(temporaryRoot, 'consumer');

const run = (args, options = {}) =>
  execFileSync('nub', args, {
    cwd: root,
    encoding: 'utf8',
    stdio: options.capture ? 'pipe' : 'inherit',
  });

const exportedTarget = (target) => {
  const path = typeof target === 'string' ? target : target.default;
  if (typeof path !== 'string') {
    throw new Error('Export map entry has no runtime or asset target');
  }
  return path;
};

try {
  await mkdir(packDirectory);
  await mkdir(consumerDirectory);

  const dependencies = {
    effect: '4.0.0-rc.109',
  };
  const imports = [];
  const assets = [];
  const packages = [];

  for (const entry of await readdir(packageRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;

    const directory = resolve(packageRoot, entry.name);
    const manifest = JSON.parse(
      await readFile(resolve(directory, 'package.json'), 'utf8'),
    );
    if (manifest.private === true) continue;
    const packed = JSON.parse(
      run(
        [
          '--cwd',
          directory,
          'pack',
          '--json',
          '--pack-destination',
          packDirectory,
        ],
        { capture: true },
      ),
    );
    const [artifact] = packed;

    if (packed.length !== 1 || artifact.name !== manifest.name) {
      throw new Error(`Unexpected pack result for ${manifest.name}`);
    }

    dependencies[manifest.name] = `file:${resolve(
      packDirectory,
      artifact.filename,
    )}`;
    packages.push({ name: manifest.name, version: manifest.version });

    for (const [subpath, target] of Object.entries(manifest.exports)) {
      const specifier = `${manifest.name}/${subpath.slice(2)}`;
      if (exportedTarget(target).endsWith('.js')) {
        imports.push(specifier);
      } else {
        assets.push(specifier);
      }
    }
  }

  await writeFile(
    resolve(consumerDirectory, 'package.json'),
    `${JSON.stringify(
      {
        name: 'vesper-packed-consumer-smoke',
        private: true,
        type: 'module',
        scripts: { smoke: 'node smoke.mjs' },
        dependencies,
        overrides: Object.fromEntries(
          Object.entries(dependencies).filter(([name]) =>
            name.startsWith('@sunfall/vesper-'),
          ),
        ),
      },
      null,
      2,
    )}\n`,
  );
  await writeFile(
    resolve(consumerDirectory, 'smoke.mjs'),
    `import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const imports = ${JSON.stringify(imports)};
const assets = ${JSON.stringify(assets)};
const packages = ${JSON.stringify(packages)};

for (const specifier of imports) {
  await import(specifier);
}

for (const specifier of assets) {
  const contents = await readFile(new URL(import.meta.resolve(specifier)), 'utf8');
  if (contents.length === 0)
    throw new Error(\`Empty exported asset: \${specifier}\`);
}

for (const expected of packages) {
  const path = resolve(
    'node_modules',
    ...expected.name.split('/'),
    'package.json',
  );
  const manifest = JSON.parse(await readFile(path, 'utf8'));
  if (
    manifest.name !== expected.name ||
    manifest.version !== expected.version
  ) {
    throw new Error(\`Installed manifest mismatch for \${expected.name}\`);
  }
  const serialized = JSON.stringify({
    dependencies: manifest.dependencies,
    optionalDependencies: manifest.optionalDependencies,
    peerDependencies: manifest.peerDependencies,
  });
  if (serialized.includes('workspace:') || serialized.includes('catalog:')) {
    throw new Error(\`Unpublishable dependency protocol in \${expected.name}\`);
  }
}

console.log(\`Imported \${imports.length} public modules and read \${assets.length} exported assets from \${packages.length} packed packages.\`);
`,
  );
  await writeFile(
    resolve(consumerDirectory, 'smoke.ts'),
    imports
      .map(
        (specifier, index) =>
          `import type * as PublicModule${index} from ${JSON.stringify(specifier)};`,
      )
      .join('\n') + '\n',
  );

  run([
    '--cwd',
    consumerDirectory,
    'install',
    '--prod',
    '--ignore-scripts',
    '--no-frozen-lockfile',
    '--prefer-offline',
  ]);
  run(['--cwd', consumerDirectory, 'run', 'smoke']);
  run([
    '--cwd',
    consumerDirectory,
    'exec',
    resolve(root, 'node_modules/typescript/bin/tsc'),
    '--noEmit',
    '--strict',
    '--skipLibCheck',
    '--target',
    'ES2022',
    '--module',
    'NodeNext',
    '--moduleResolution',
    'NodeNext',
    '--typeRoots',
    resolve(root, 'node_modules/@types'),
    'smoke.ts',
  ]);

  if (audit) {
    run(['--cwd', consumerDirectory, 'audit', '--prod']);
  }
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
