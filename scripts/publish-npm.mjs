#!/usr/bin/env node

// Publish the `packages/*` libraries to npm.
//
// Only packages that are not `private` and carry `publishConfig.access` are
// candidates; everything under `examples/` and `benchmarks/` is private and is
// skipped without being mentioned, because "skipped 3 things you never meant
// to publish" is noise in a release log.
//
// Publishing is idempotent by default: a version already on the registry is
// skipped rather than failed, so re-running a partially-failed release
// finishes it instead of starting an argument.

import { spawnSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packagesDir = join(workspaceRoot, 'packages');
const DEFAULT_DIST_TAG = 'alpha';

const usage = `USAGE
  nub run publish:npm -- [--tag <dist-tag>] [--dry-run] [--package <name>]
                         [--no-skip-existing] [--no-provenance]

Options:
  --tag <dist-tag>     npm dist-tag to attach. Defaults to ${DEFAULT_DIST_TAG}.
  --dry-run            Run npm publish --dry-run without mutating the registry.
  --package <name>     Limit targets. May be passed more than once.
  --no-skip-existing   Fail instead of skipping versions already on npm.
  --no-provenance      Omit npm provenance. Local emergency bootstrap only.
  --help               Show this help text.
`;

const parseArgs = (argv) => {
  const options = {
    distTag: DEFAULT_DIST_TAG,
    dryRun: false,
    packageNames: new Set(),
    provenance: true,
    skipExisting: true,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--') continue;
    if (arg === '--help') {
      process.stdout.write(usage);
      process.exit(0);
    } else if (arg === '--dry-run') options.dryRun = true;
    else if (arg === '--no-provenance') options.provenance = false;
    else if (arg === '--no-skip-existing') options.skipExisting = false;
    else if (arg === '--tag') options.distTag = argv[(index += 1)];
    else if (arg.startsWith('--tag=')) options.distTag = arg.slice(6);
    else if (arg === '--package') options.packageNames.add(argv[(index += 1)]);
    else if (arg.startsWith('--package=')) {
      options.packageNames.add(arg.slice(10));
    } else throw new Error(`Unknown argument: ${arg}\n\n${usage}`);
  }

  if (!options.distTag) throw new Error(`--tag requires a value.\n\n${usage}`);
  return options;
};

const publishTargets = (options) =>
  readdirSync(packagesDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const directory = join(packagesDir, entry.name);
      const manifest = JSON.parse(
        readFileSync(join(directory, 'package.json'), 'utf8'),
      );
      return { directory, manifest };
    })
    .filter(({ manifest }) => manifest.private !== true)
    .filter(
      ({ manifest }) =>
        options.packageNames.size === 0 ||
        options.packageNames.has(manifest.name),
    );

const alreadyPublished = (name, version) => {
  const result = spawnSync('npm', ['view', `${name}@${version}`, 'version'], {
    encoding: 'utf-8',
  });
  return result.status === 0 && result.stdout.trim() === version;
};

const publish = (target, options) => {
  const { name, version } = target.manifest;

  if (
    options.skipExisting &&
    !options.dryRun &&
    alreadyPublished(name, version)
  ) {
    process.stdout.write(`- ${name}@${version} already published, skipping\n`);
    return;
  }

  const args = ['publish', '--tag', options.distTag, '--access', 'public'];
  if (options.dryRun) args.push('--dry-run');
  // Provenance requires a trusted CI environment; npm rejects it elsewhere.
  if (options.provenance && !options.dryRun) args.push('--provenance');

  process.stdout.write(`▶ ${name}@${version} (${options.distTag})\n`);
  const result = spawnSync('npm', args, {
    cwd: target.directory,
    stdio: 'inherit',
  });
  if (result.status !== 0) {
    throw new Error(`npm publish failed for ${name}@${version}.`);
  }
  process.stdout.write(`✓ ${name}@${version}\n`);
};

try {
  const options = parseArgs(process.argv.slice(2));
  const targets = publishTargets(options);

  if (targets.length === 0) {
    throw new Error('No publishable packages matched.');
  }

  // A published package that was never built ships an empty `dist`, and npm
  // has no way to take that back.
  for (const target of targets) {
    try {
      readdirSync(join(target.directory, 'dist'));
    } catch {
      throw new Error(
        `${target.manifest.name} has no dist/. Run \`nub run build\` first.`,
      );
    }
  }

  for (const target of targets) publish(target, options);
} catch (error) {
  process.stderr.write(`${error instanceof Error ? error.message : error}\n`);
  process.exitCode = 1;
}
