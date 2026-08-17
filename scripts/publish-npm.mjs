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
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const packagesDir = join(workspaceRoot, 'packages');
const DEFAULT_DIST_TAG = 'alpha';

const usage = `USAGE
  nub run publish:npm -- [--tag <dist-tag>] [--dry-run] [--package <name>]
                         [--release-tag <vX.Y.Z>] [--no-skip-existing]
                         [--no-provenance]

Options:
  --tag <dist-tag>     npm dist-tag to attach. Defaults to ${DEFAULT_DIST_TAG}.
  --dry-run            Run npm publish --dry-run without mutating the registry.
  --package <name>     Limit targets. May be passed more than once.
  --release-tag <tag>  Require every package version to match this v-prefixed tag.
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
    releaseTag:
      process.env['GITHUB_REF_TYPE'] === 'tag'
        ? process.env['GITHUB_REF_NAME']
        : undefined,
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
    } else if (arg === '--release-tag') {
      options.releaseTag = argv[(index += 1)];
    } else if (arg.startsWith('--release-tag=')) {
      options.releaseTag = arg.slice(14);
    } else throw new Error(`Unknown argument: ${arg}\n\n${usage}`);
  }

  if (!options.distTag) throw new Error(`--tag requires a value.\n\n${usage}`);
  return options;
};

const publishTargets = () =>
  readdirSync(packagesDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .filter((entry) =>
      existsSync(join(packagesDir, entry.name, 'package.json')),
    )
    .map((entry) => {
      const directory = join(packagesDir, entry.name);
      const manifest = JSON.parse(
        readFileSync(join(directory, 'package.json'), 'utf8'),
      );
      return { directory, manifest };
    })
    .filter(({ manifest }) => manifest.private !== true)
    .filter(({ manifest }) => manifest.publishConfig?.access);

const inDependencyOrder = (targets) => {
  const byName = new Map(
    targets.map((target) => [target.manifest.name, target]),
  );
  const ordered = [];
  const visited = new Set();

  const visit = (target) => {
    if (visited.has(target.manifest.name)) return;
    visited.add(target.manifest.name);
    const dependencies = {
      ...target.manifest.dependencies,
      ...target.manifest.optionalDependencies,
      ...target.manifest.peerDependencies,
    };
    for (const name of Object.keys(dependencies)) {
      const dependency = byName.get(name);
      if (dependency) visit(dependency);
    }
    ordered.push(target);
  };

  for (const target of targets) visit(target);
  return ordered;
};

const validateManifest = (target) => {
  const dependencyGroups = [
    'dependencies',
    'optionalDependencies',
    'peerDependencies',
  ];
  for (const group of dependencyGroups) {
    for (const [name, range] of Object.entries(target.manifest[group] ?? {})) {
      if (String(range).startsWith('workspace:')) {
        throw new Error(
          `${target.manifest.name} has ${group}.${name}=${range}; npm consumers cannot install workspace protocols.`,
        );
      }
    }
  }
};

const alreadyPublished = (name, version) => {
  const result = spawnSync('npm', ['view', `${name}@${version}`, 'version'], {
    encoding: 'utf-8',
  });
  return result.status === 0 && result.stdout.trim() === version;
};

const validateReleaseTag = (targets, releaseTag) => {
  if (releaseTag === undefined) return;
  if (!releaseTag.startsWith('v')) {
    throw new Error(`Release tag must be v-prefixed, got ${releaseTag}.`);
  }
  const expectedVersion = releaseTag.slice(1);
  const mismatches = targets.filter(
    ({ manifest }) => manifest.version !== expectedVersion,
  );
  if (mismatches.length > 0) {
    throw new Error(
      `Release tag ${releaseTag} does not match: ${mismatches
        .map(({ manifest }) => `${manifest.name}@${manifest.version}`)
        .join(', ')}.`,
    );
  }
};

const validateSiblingDependencies = (allTargets, selectedTargets) => {
  const allByName = new Map(
    allTargets.map((target) => [target.manifest.name, target]),
  );
  const selectedNames = new Set(
    selectedTargets.map(({ manifest }) => manifest.name),
  );
  for (const target of selectedTargets) {
    for (const group of [
      'dependencies',
      'optionalDependencies',
      'peerDependencies',
    ]) {
      for (const [name, range] of Object.entries(
        target.manifest[group] ?? {},
      )) {
        const sibling = allByName.get(name);
        if (sibling === undefined) continue;
        if (range !== sibling.manifest.version) {
          throw new Error(
            `${target.manifest.name} must use exact sibling ${name}@${sibling.manifest.version}, got ${range}.`,
          );
        }
        if (
          !selectedNames.has(name) &&
          !alreadyPublished(name, sibling.manifest.version)
        ) {
          throw new Error(
            `${target.manifest.name} requires ${name}@${range}; select ${name} in this run or publish it first. ` +
              'For recovery, rerun with both packages or publish the missing sibling before retrying this package.',
          );
        }
      }
    }
  }
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
  const allTargets = publishTargets();
  const targets = inDependencyOrder(
    allTargets.filter(
      ({ manifest }) =>
        options.packageNames.size === 0 ||
        options.packageNames.has(manifest.name),
    ),
  );

  if (targets.length === 0) {
    throw new Error('No publishable packages matched.');
  }

  validateReleaseTag(allTargets, options.releaseTag);
  validateSiblingDependencies(allTargets, targets);

  // A published package that was never built ships an empty `dist`, and npm
  // has no way to take that back.
  for (const target of targets) {
    validateManifest(target);
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
