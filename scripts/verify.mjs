#!/usr/bin/env node

// The whole gate, in the order that fails cheapest first.
//
// `nub run verify` is what CI runs and what a contributor runs before opening
// a pull request. Everything it does is also runnable on its own — `nub run
// build`, `nub run lint`, and so on — so a failure here is always reproducible
// with one shorter command, which is printed alongside it.
//
// The build comes first because it is the thing most likely to be broken and
// the cheapest to read when it is: the packages are TypeScript project
// references, so a package that does not compile takes its dependents with it
// and reports the same failure three more times under a lane prefix. Format,
// lint, and typecheck then run concurrently: they are independent, and a
// contributor waiting on three serial passes tends to stop running the gate at
// all.

import { spawn } from 'node:child_process';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));
// `node_modules/.bin` is on PATH for anything nub itself started, so this
// resolves to the version pinned in devDependencies rather than to whichever
// nub the contributor happens to have globally.
const nubCommand = process.platform === 'win32' ? 'nub.cmd' : 'nub';

const parseConcurrency = (argv, env) => {
  const flag = argv.find(
    (arg) => arg === '--concurrency' || arg.startsWith('--concurrency='),
  );
  const raw =
    flag === undefined
      ? env['VESPER_VERIFY_CONCURRENCY']
      : flag.includes('=')
        ? flag.slice(flag.indexOf('=') + 1)
        : argv[argv.indexOf(flag) + 1];

  if (raw === undefined || String(raw).trim() === '') return 4;

  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(
      `--concurrency must be a positive integer, got ${JSON.stringify(raw)}.`,
    );
  }
  return parsed;
};

const formatDuration = (startedAt) => {
  const seconds = (Date.now() - startedAt) / 1000;
  return seconds < 60
    ? `${seconds.toFixed(1)}s`
    : `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
};

/**
 * Run one package script through nub, buffering its output.
 *
 * Buffered rather than streamed because lanes run concurrently: interleaved
 * compiler output from three passes at once is unreadable, and the whole point
 * of a gate is that its failure is legible. The buffer is flushed with a lane
 * prefix when the lane ends, pass or fail.
 */
const run = (label, script) =>
  new Promise((resolve, reject) => {
    const startedAt = Date.now();
    process.stdout.write(`▶ ${label}\n`);

    const args = ['run', script];
    const child = spawn(nubCommand, args, {
      cwd: workspaceRoot,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    let output = '';
    child.stdout.on('data', (chunk) => {
      output += chunk;
    });
    child.stderr.on('data', (chunk) => {
      output += chunk;
    });

    const flush = () => {
      for (const line of output.split(/\r?\n/)) {
        if (line.length > 0) process.stdout.write(`[${label}] ${line}\n`);
      }
    };

    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        process.stdout.write(`✓ ${label} (${formatDuration(startedAt)})\n`);
        resolve();
        return;
      }
      flush();
      reject(
        new Error(
          `${label} failed with exit code ${code}. ` +
            `Reproduce with: ${nubCommand} ${args.join(' ')}`,
        ),
      );
    });
  });

/** Run `tasks` with at most `concurrency` in flight, and report every failure. */
const runAll = async (label, tasks, concurrency) => {
  process.stdout.write(`\n== ${label} ==\n`);

  const queue = [...tasks];
  const failures = [];
  const worker = async () => {
    for (let next = queue.shift(); next !== undefined; next = queue.shift()) {
      try {
        await run(next.label, next.script);
      } catch (error) {
        // Collected rather than thrown: a lint failure and a typecheck failure
        // are usually one edit apart, and stopping at the first hides the
        // second until the next round trip.
        failures.push(error);
      }
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(concurrency, tasks.length) }, worker),
  );

  if (failures.length > 0) {
    throw new AggregateError(failures, `${label} failed.`);
  }
};

const main = async () => {
  const concurrency = parseConcurrency(process.argv.slice(2), process.env);
  process.stdout.write(
    `Vesper verify running with lane concurrency ${concurrency}.\n`,
  );

  await run('package builds', 'build');

  await runAll(
    'source static gates',
    [
      { label: 'workspace format check', script: 'format:check' },
      { label: 'workspace lint', script: 'lint' },
      { label: 'workspace typecheck', script: 'typecheck' },
    ],
    concurrency,
  );

  process.stdout.write('\n== packed consumer ==\n');
  await run('packed-consumer preflight', 'preflight:pack:built');

  process.stdout.write('\n== tests ==\n');
  await run('workspace tests', 'test');
};

try {
  await main();
} catch (error) {
  const messages =
    error instanceof AggregateError
      ? error.errors.map((cause) => cause.message)
      : [error instanceof Error ? error.message : String(error)];
  for (const message of messages) process.stderr.write(`${message}\n`);
  process.exitCode = 1;
}
