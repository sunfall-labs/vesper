#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packagesDir = join(workspaceRoot, 'packages');
const effectVersion = '4.0.0-rc.109';
const skipBuild = process.argv.slice(2).includes('--skip-build');

const run = (command, args, options = {}) => {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? workspaceRoot,
    encoding: 'utf8',
    stdio: options.capture ? 'pipe' : 'inherit',
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(' ')} failed${
        options.capture ? `:\n${result.stderr || result.stdout}` : '.'
      }`,
    );
  }
  return result.stdout;
};

const publishablePackages = () =>
  readdirSync(packagesDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => join(packagesDir, entry.name))
    .filter((directory) => existsSync(join(directory, 'package.json')))
    .map((directory) => ({
      directory,
      manifest: JSON.parse(
        readFileSync(join(directory, 'package.json'), 'utf8'),
      ),
    }))
    .filter(({ manifest }) =>
      Boolean(manifest.private !== true && manifest.publishConfig?.access),
    );

const exportTargets = (value) => {
  if (typeof value === 'string') return [value];
  if (value === null || typeof value !== 'object') return [];
  return Object.values(value).flatMap(exportTargets);
};

const packageSpecifier = (packageName, subpath) =>
  subpath === '.' ? packageName : `${packageName}/${subpath.slice(2)}`;

const main = () => {
  if (!skipBuild) {
    const nub = process.platform === 'win32' ? 'nub.cmd' : 'nub';
    run(nub, ['run', 'build']);
  }

  const temporaryRoot = mkdtempSync(join(tmpdir(), 'vesper-pack-'));
  const tarballsDir = join(temporaryRoot, 'tarballs');
  const consumerDir = join(temporaryRoot, 'consumer');
  const targets = publishablePackages();

  try {
    mkdirSync(tarballsDir);
    mkdirSync(consumerDir);
    const packed = targets.map((target) => {
      for (const group of [
        'dependencies',
        'optionalDependencies',
        'peerDependencies',
      ]) {
        for (const [name, range] of Object.entries(
          target.manifest[group] ?? {},
        )) {
          if (String(range).startsWith('workspace:')) {
            throw new Error(
              `${target.manifest.name} has unpublished ${group}.${name}=${range}.`,
            );
          }
        }
      }
      const output = run(
        'npm',
        ['pack', '--json', '--pack-destination', tarballsDir, target.directory],
        { capture: true },
      );
      const result = JSON.parse(output)[0];
      if (!result?.filename || !Array.isArray(result.files)) {
        throw new Error(
          `npm pack returned no file manifest for ${target.manifest.name}.`,
        );
      }

      const files = new Set(result.files.map(({ path }) => path));
      for (const exported of exportTargets(target.manifest.exports)) {
        const path = exported.replace(/^\.\//, '');
        if (!files.has(path)) {
          throw new Error(
            `${target.manifest.name} exports missing packed file ${path}.`,
          );
        }
      }
      for (const path of files) {
        if (path.endsWith('.map') || path.startsWith('src/')) {
          throw new Error(
            `${target.manifest.name} unexpectedly packs ${path}.`,
          );
        }
      }
      return { ...target, tarball: join(tarballsDir, result.filename) };
    });

    writeFileSync(
      join(consumerDir, 'package.json'),
      `${JSON.stringify({ private: true, type: 'module' }, null, 2)}\n`,
    );
    run(
      'npm',
      [
        'install',
        '--ignore-scripts',
        '--no-audit',
        '--no-fund',
        ...packed.map(({ tarball }) => tarball),
        `effect@${effectVersion}`,
        '@effect/ai-openai@4.0.0-rc.109',
        '@effect/platform-node@4.0.0-rc.109',
        '@effect/platform-node-shared@4.0.0-rc.109',
        'vitest@^4.1.9',
        'typescript@5.9.3',
        '@types/node@^22',
      ],
      { cwd: consumerDir },
    );

    const imports = packed.flatMap(({ manifest }) =>
      Object.entries(manifest.exports)
        .filter(([, value]) =>
          exportTargets(value).some((path) => path.endsWith('.js')),
        )
        .map(([subpath]) => packageSpecifier(manifest.name, subpath)),
    );
    writeFileSync(
      join(consumerDir, 'exports.test.mjs'),
      `import { Agent } from '@sunfall/vesper-agent/agent';\n` +
        `import { Toolkit } from 'effect/unstable/ai';\n` +
        `import { describe, expect, it } from 'vitest';\n\n` +
        `describe('packed exports', () => {\n` +
        `  it.each(${JSON.stringify(imports, null, 2)})('imports %s', async (specifier) => {\n` +
        `    await expect(import(specifier)).resolves.toBeTypeOf('object');\n` +
        `  });\n\n` +
        `  it('rejects a structural subagent', () => {\n` +
        `    const child = { name: 'forged', revision: '1', run: () => { throw new Error('must not run'); } };\n` +
        `    expect(() => Agent.make({ name: 'parent', revision: '1', instructions: 'Delegate.', toolkit: Toolkit.make(), subagents: [child] })).toThrow('was not created by Agent.make');\n` +
        `  });\n` +
        `});\n`,
    );
    run('npx', ['--no-install', 'vitest', 'run', 'exports.test.mjs'], {
      cwd: consumerDir,
    });

    writeFileSync(
      join(consumerDir, 'consumer.ts'),
      `import { Agent } from '@sunfall/vesper-agent/agent';\n` +
        `import { Subagent } from '@sunfall/vesper-agent/subagent';\n` +
        `import { Toolkit } from 'effect/unstable/ai';\n\n` +
        `const child = Agent.make({ name: 'child', revision: '1', instructions: 'Answer.', toolkit: Toolkit.make() });\n` +
        `// @ts-expect-error packed agents expose only the public one-input run method\n` +
        `child.run({}, {}, 'forged session invocation');\n` +
        `// @ts-expect-error runtime/session delegation handlers are not public\n` +
        `Subagent.handler;\n` +
        `export const agent = Agent.make({ name: 'consumer', revision: '1', instructions: 'Delegate.', toolkit: Toolkit.make(), subagents: [child] });\n`,
    );
    writeFileSync(
      join(consumerDir, 'provider.ts'),
      `import { OpenAiClient, OpenAiLanguageModel } from '@effect/ai-openai';\n` +
        `import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';\n` +
        `import { agent } from './consumer.js';\n` +
        `import { Config, Effect, Layer } from 'effect';\n\n` +
        `const provider = OpenAiLanguageModel.model('gpt-5').pipe(\n` +
        `  Layer.provide(OpenAiClient.layerConfig({ apiKey: Config.redacted('OPENAI_API_KEY') })),\n` +
        `  Layer.provide(NodeHttpClient.layerUndici),\n` +
        `);\n` +
        `export const program = agent.run('hello').pipe(Effect.provide(provider));\n`,
    );
    writeFileSync(
      join(consumerDir, 'tsconfig.json'),
      `${JSON.stringify(
        {
          compilerOptions: {
            target: 'ES2022',
            module: 'NodeNext',
            moduleResolution: 'NodeNext',
            strict: true,
            noEmit: true,
            skipLibCheck: false,
          },
          files: ['consumer.ts'],
        },
        null,
        2,
      )}\n`,
    );
    run('npx', ['--no-install', 'tsc', '-p', 'tsconfig.json'], {
      cwd: consumerDir,
    });
    writeFileSync(
      join(consumerDir, 'tsconfig.provider.json'),
      `${JSON.stringify(
        {
          extends: './tsconfig.json',
          compilerOptions: { skipLibCheck: true },
          files: ['provider.ts'],
        },
        null,
        2,
      )}\n`,
    );
    run('npx', ['--no-install', 'tsc', '-p', 'tsconfig.provider.json'], {
      cwd: consumerDir,
    });
    const installedAgentDist = join(
      consumerDir,
      'node_modules',
      '@sunfall',
      'vesper-agent',
      'dist',
    );
    const agentDeclaration = readFileSync(
      join(installedAgentDist, 'agent.d.ts'),
      'utf8',
    );
    const subagentDeclaration = readFileSync(
      join(installedAgentDist, 'subagent.d.ts'),
      'utf8',
    );
    if (/AgentInternal|internal\.js|run:\s*\(runtime:/.test(agentDeclaration)) {
      throw new Error(
        'Packed agent declaration exposes the internal agent protocol.',
      );
    }
    if (
      /AgentInternal|internal\.js|run-policy\.js|\.\/log\.js|export declare const (?:handler|delegateTo)\b/.test(
        subagentDeclaration,
      )
    ) {
      throw new Error(
        'Packed subagent declaration exposes runtime/session delegation internals.',
      );
    }
    const effectPaths = run('npm', ['ls', 'effect', '--all', '--parseable'], {
      cwd: consumerDir,
      capture: true,
    })
      .trim()
      .split(/\r?\n/)
      .filter((path) => path.endsWith('/node_modules/effect'));
    if (effectPaths.length !== 1) {
      throw new Error(
        `Expected one installed Effect identity, found ${effectPaths.length}: ${effectPaths.join(', ')}`,
      );
    }

    const installedMigration = readFileSync(
      join(
        consumerDir,
        'node_modules',
        '@sunfall',
        'vesper-log',
        'migrations',
        '001-initial.sql',
      ),
      'utf8',
    );
    const authoritativeMigration = readFileSync(
      join(packagesDir, 'log', 'migrations', '001-initial.sql'),
      'utf8',
    );
    if (installedMigration !== authoritativeMigration) {
      throw new Error(
        'Packed log migration differs from the authoritative SQL source.',
      );
    }

    process.stdout.write(
      `Packed-consumer preflight passed for ${targets.length} packages.\n`,
    );
    rmSync(temporaryRoot, { recursive: true, force: true });
  } catch (error) {
    process.stderr.write(`Packed artifacts retained at ${temporaryRoot}\n`);
    throw error;
  }
};

try {
  main();
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : error}\n`);
  process.exitCode = 1;
}
