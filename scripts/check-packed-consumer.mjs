import { execFileSync } from 'node:child_process';
import {
  copyFile,
  mkdtemp,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join, relative, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const packageRoot = resolve(root, 'packages');
const audit = process.argv.includes('--audit');
const temporaryRoot = await mkdtemp(join(tmpdir(), 'vesper-packed-consumer-'));
const packDirectory = resolve(temporaryRoot, 'packs');
const consumerDirectory = resolve(temporaryRoot, 'consumer');

/** @typedef {Record<string, unknown>} JsonObject */

/** @param {unknown} value @returns {value is JsonObject} */
const isObject = (value) =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/** @param {string} source @param {string} description @returns {JsonObject} */
const parseObject = (source, description) => {
  const value = /** @type {unknown} */ (JSON.parse(source));
  if (!isObject(value)) {
    throw new Error(`${description} must be a JSON object`);
  }
  return value;
};

/**
 * @param {ReadonlyArray<string>} args
 * @param {{ readonly capture?: boolean, readonly cwd?: string }} [options]
 */
const run = (args, options = {}) =>
  execFileSync('bun', args, {
    cwd: options.cwd ?? root,
    encoding: 'utf8',
    stdio: options.capture ? 'pipe' : 'inherit',
  });

/** @param {unknown} target @returns {string} */
const exportedTarget = (target) => {
  const path =
    typeof target === 'string'
      ? target
      : isObject(target) && typeof target['default'] === 'string'
        ? target['default']
        : undefined;
  if (typeof path !== 'string') {
    throw new Error('Export map entry has no runtime or asset target');
  }
  return path;
};

/**
 * The patched Effect family must carry upstream #9008 in both its source and
 * its built output: `toolCallValidation` on `LanguageModel`, the `deferred`
 * parameters mode on `Response`, and the Anthropic and OpenAI providers
 * returning calls to undeclared tools instead of failing them. A patch that
 * applied to only one of source and build would typecheck against one
 * behavior and run another.
 *
 * @returns {Promise<void>}
 */
const assertEffectPatchSourceContract = async () => {
  /** @type {ReadonlyArray<readonly [string, string, ReadonlyArray<string>]>} */
  const contracts = [
    ['effect', 'src/ai/LanguageModel.ts', ['toolCallValidation']],
    ['effect', 'dist/ai/LanguageModel.js', ['toolCallValidation']],
    ['effect', 'dist/ai/LanguageModel.d.ts', ['toolCallValidation']],
    ['effect', 'dist/ai/Response.d.ts', ['"deferred"']],
    [
      '@effect/ai-anthropic',
      'dist/AnthropicLanguageModel.js',
      ['toolCallValidation'],
    ],
    [
      '@effect/ai-openai',
      'dist/OpenAiLanguageModel.js',
      ['toolCallValidation'],
    ],
  ];
  for (const [pkg, file, markers] of contracts) {
    const source = await readFile(
      resolve(root, 'node_modules', pkg, file),
      'utf8',
    );
    for (const marker of markers) {
      if (!source.includes(marker)) {
        throw new Error(
          `Patched ${pkg} ${file} lost ${marker}; the #9008 backport did not apply`,
        );
      }
    }
  }
};

try {
  await mkdir(packDirectory);
  await mkdir(consumerDirectory);
  const rootManifest = parseObject(
    await readFile(resolve(root, 'package.json'), 'utf8'),
    'root package.json',
  );
  const workspaces = rootManifest['workspaces'];
  const catalog = isObject(workspaces) ? workspaces['catalog'] : undefined;
  const effectVersion = isObject(catalog) ? catalog['effect'] : undefined;
  if (typeof effectVersion !== 'string') {
    throw new Error('root package.json must declare workspaces.catalog.effect');
  }

  const configuredPatches = rootManifest['patchedDependencies'];
  if (configuredPatches !== undefined && !isObject(configuredPatches)) {
    throw new Error('root package.json patchedDependencies must be an object');
  }
  if (
    Object.keys(configuredPatches ?? {}).some(
      (selector) => selector === 'effect' || selector.startsWith('effect@'),
    )
  ) {
    await assertEffectPatchSourceContract();
  }
  /** @type {Record<string, string>} */
  const consumerPatches = {};
  for (const [selector, patchPath] of Object.entries(configuredPatches ?? {})) {
    if (typeof patchPath !== 'string') {
      throw new Error(`Patch path for ${selector} must be a string`);
    }
    const source = resolve(root, patchPath);
    const relativeSource = relative(root, source);
    if (
      relativeSource.startsWith(
        `..${process.platform === 'win32' ? '\\' : '/'}`,
      ) ||
      relativeSource === '..' ||
      isAbsolute(relativeSource)
    ) {
      throw new Error(`Patch path for ${selector} escapes the repository root`);
    }
    const destination = `dependency-${String(Object.keys(consumerPatches).length)}.patch`;
    await copyFile(source, resolve(consumerDirectory, destination));
    consumerPatches[selector] = destination;
  }

  const dependencies = {
    effect: effectVersion,
  };
  for (const selector of Object.keys(configuredPatches ?? {})) {
    const separator = selector.lastIndexOf('@');
    const packageName = separator > 0 ? selector.slice(0, separator) : selector;
    if (packageName === 'effect') {
      continue;
    }
    const version = isObject(catalog) ? catalog[packageName] : undefined;
    if (typeof version !== 'string') {
      throw new Error(
        `Patched dependency ${packageName} must have a catalog version`,
      );
    }
    dependencies[packageName] = version;
  }
  /** @type {string[]} */
  const imports = [];
  /** @type {string[]} */
  const assets = [];
  const packages = [];

  for (const entry of await readdir(packageRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const directory = resolve(packageRoot, entry.name);
    const manifest = parseObject(
      await readFile(resolve(directory, 'package.json'), 'utf8'),
      `${entry.name}/package.json`,
    );
    if (manifest['private'] === true) {
      continue;
    }
    const artifact = run(
      ['pm', 'pack', '--destination', packDirectory, '--quiet'],
      { capture: true, cwd: directory },
    ).trim();
    const name = manifest['name'];
    const version = manifest['version'];
    const exports = manifest['exports'];

    if (
      typeof name !== 'string' ||
      typeof version !== 'string' ||
      !isObject(exports) ||
      !artifact.endsWith('.tgz') ||
      relative(packDirectory, artifact).includes('/')
    ) {
      throw new Error(`Unexpected pack result for ${entry.name}`);
    }

    dependencies[name] = `file:${artifact}`;
    packages.push({ name, version });

    for (const [subpath, target] of Object.entries(exports)) {
      const specifier = `${name}/${subpath.slice(2)}`;
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
        ...(Object.keys(consumerPatches).length === 0
          ? {}
          : { patchedDependencies: consumerPatches }),
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
import { Agent } from '@sunfall/vesper-agent/agent';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import { LanguageModel, Tool, Toolkit } from 'effect/ai';

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

const finish = (reason = 'stop') => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});
const calls = { handler: 0, model: 0 };
const probe = Tool.make('boundary_probe', {
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
}).setNeedsApproval(true);
const agent = Agent.make({
  name: 'packed-boundary-probe',
  revision: '1',
  instructions: 'Use the tool.',
  toolkit: Toolkit.make(probe),
}).withHandlers({
  boundary_probe: () => Effect.sync(() => {
    calls.handler += 1;
    return 'unexpected';
  }),
});
const model = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([]),
    streamText: () => Stream.suspend(() => {
      const turn = calls.model++;
      return Stream.fromIterable(turn === 0
        ? [
            { type: 'tool-call', id: 'bad-params', name: 'boundary_probe', params: { path: null } },
            { type: 'tool-call', id: 'bad-name', name: '__proto__', params: {} },
            finish('tool-calls'),
          ]
        : [
            { type: 'text-start', id: 'answer' },
            { type: 'text-delta', id: 'answer', delta: 'Recovered.' },
            { type: 'text-end', id: 'answer' },
            finish(),
          ]);
    }),
  }),
);
const result = await Effect.runPromise(
  agent.run('probe').pipe(Effect.provide(model), Effect.orDie),
);
if (result.text !== 'Recovered.' || calls.model !== 2 || calls.handler !== 0) {
  throw new Error('Packed Vesper does not recover invalid model tool calls');
}

console.log(\`Imported \${imports.length} public modules and read \${assets.length} exported assets from \${packages.length} packed packages.\`);
`,
  );
  await writeFile(
    resolve(consumerDirectory, 'smoke.ts'),
    imports
      .map(
        (specifier, index) =>
          `import type * as PublicModule${String(index)} from ${JSON.stringify(specifier)};`,
      )
      .join('\n') + '\n',
  );

  run(['install', '--production', '--ignore-scripts', '--linker', 'hoisted'], {
    cwd: consumerDirectory,
  });
  run(['run', 'smoke'], { cwd: consumerDirectory });
  run(
    [
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
    ],
    { cwd: consumerDirectory },
  );

  if (audit) {
    run(['audit'], { cwd: consumerDirectory });
  }
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
