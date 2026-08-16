// Benchmark entry point. Not a test: nothing here runs under `nub run test`
// or `nub run verify`, and it is invoked deliberately with `nub run benchmark`.
//
// Each (side, scenario) pair runs in its own child process. That is not
// tidiness — it is the measurement. Two configurations loaded into one process
// share a JIT, a heap and a module graph, and whichever ran second would be
// measured against warm code and a dirtied heap. Separate processes cost a
// second and remove the whole class of contamination. It is also why this is
// not a `vitest bench` suite: startup and resident memory are properties of a
// process, and an in-process harness cannot observe either.
//
// The two sides are the same agent with recording off and on — `vesper` and
// `vesper+log` — so every row answers "what does durability cost?".

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import { formatMs, summarise } from './stats.js';
import type { ScenarioName, ScenarioResult, SideName } from './workload.js';

const SELF = fileURLToPath(import.meta.url);

/**
 * Fresh processes sampled for the once-per-process scenarios.
 *
 * Startup and heap are single-shot by nature — you only start once — so the
 * only way to get a spread is to start repeatedly. A single reading here is
 * exactly the shape of measurement that once had this project reporting a
 * 1m42s startup for something that takes well under a second.
 */
const MEMORY_REPEATS = 5;
const STARTUP_REPEATS = 5;

const PLAN: ReadonlyArray<readonly [SideName, ScenarioName]> = [
  ['vesper', 'turn'],
  ['vesper+log', 'turn'],
  ['vesper+log', 'conversation'],
  ['vesper', 'scaling'],
  ['vesper+log', 'scaling'],
  ['vesper+log', 'growth'],
  ['vesper', 'startup'],
  ['vesper+log', 'memory'],
];

// --------------------------------------------------------------- child mode

const child = async (side: SideName, scenario: ScenarioName): Promise<void> => {
  const result: ScenarioResult = await (
    await import('./vesper.js')
  ).run(scenario, side === 'vesper+log');

  process.stdout.write(`##BENCH##${JSON.stringify(result)}\n`);
};

// -------------------------------------------------------------- parent mode

const runChild = (
  side: SideName,
  scenario: ScenarioName,
): Promise<ScenarioResult> =>
  new Promise((resolve, reject) => {
    const proc = spawn(
      process.execPath,
      ['--expose-gc', SELF, '--child', side, scenario],
      { stdio: ['ignore', 'pipe', 'inherit'] },
    );

    let out = '';
    proc.stdout.on('data', (chunk: Buffer) => {
      out += chunk.toString();
    });

    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`${side}/${scenario} exited with ${code}`));
        return;
      }
      const marker = out.lastIndexOf('##BENCH##');
      if (marker === -1) {
        reject(new Error(`${side}/${scenario} produced no result`));
        return;
      }
      resolve(JSON.parse(out.slice(marker + 9).trim()) as ScenarioResult);
    });
  });

/**
 * One line of report output.
 *
 * `process.stdout.write` rather than `console.log` because this is a
 * report, not a debug aid: the repository lints `no-console` outside
 * tests and scripts, and the benchmark's modules stay under `src/` so that both
 * typecheckers keep seeing them.
 */
const line = (text = ''): void => {
  process.stdout.write(`${text}\n`);
};

const mib = (bytes: number): string => (bytes / 1024 / 1024).toFixed(1);

const report = (results: ReadonlyArray<ScenarioResult>): void => {
  const byScenario = new Map<ScenarioName, ScenarioResult[]>();
  for (const result of results) {
    const list = byScenario.get(result.scenario) ?? [];
    list.push(result);
    byScenario.set(result.scenario, list);
  }

  for (const [scenario, list] of byScenario) {
    line(`\n## ${scenario}`);
    line(`   ${list[0]!.unit}`);

    if (scenario === 'memory') {
      // One heap reading is an anecdote: a forced GC is advisory and the
      // number moves. Report the spread across repeated fresh processes, and
      // let the reader decide whether a difference is real.
      const bySide = new Map<SideName, ScenarioResult[]>();
      for (const r of list) {
        const seen = bySide.get(r.side) ?? [];
        seen.push(r);
        bySide.set(r.side, seen);
      }
      line(
        `   ${'side'.padEnd(9)} ${'heap'.padStart(9)} ${'(min-max)'.padStart(
          15,
        )} ${'rss'.padStart(9)} ${'(min-max)'.padStart(15)}   (MiB)`,
      );
      for (const [side, seen] of bySide) {
        const heap = summarise(seen.map((r) => r.heapBytes ?? 0));
        const rss = summarise(seen.map((r) => r.rssBytes ?? 0));
        const range = (s: { min: number; max: number }) =>
          `(${mib(s.min)}-${mib(s.max)})`;
        line(
          `   ${side.padEnd(9)} ${mib(heap.median).padStart(9)} ${range(
            heap,
          ).padStart(15)} ${mib(rss.median).padStart(9)} ${range(rss).padStart(
            15,
          )}   n=${heap.n}`,
        );
      }
      continue;
    }

    if (scenario === 'growth') {
      const picks = [1, 5, 10, 20, 30, 40];
      line(
        `   ${'side'.padEnd(9)} ` +
          picks.map((i) => `msg ${i}`.padStart(9)).join(' ') +
          '   (ms, median)',
      );
      for (const r of list) {
        const cells = picks.map((i) => {
          const point = (r.growth ?? []).find((g) => g.index === i);
          return (
            point === undefined
              ? '-'
              : formatMs(summarise(point.samples).median)
          ).padStart(9);
        });
        line(`   ${r.side.padEnd(9)} ${cells.join(' ')}`);
      }
      for (const r of list) {
        const first = (r.growth ?? []).find((g) => g.index === 1);
        const last = (r.growth ?? []).at(-1);
        if (first === undefined || last === undefined) continue;
        const a = summarise(first.samples).median;
        const b = summarise(last.samples).median;
        line(
          `   ${r.side.padEnd(9)} message ${last.index} costs ${(b / a).toFixed(
            1,
          )}x message 1` + ` (n=${first.samples.length} conversations)`,
        );
      }
      continue;
    }

    if (scenario === 'scaling') {
      const turns = list[0]?.series?.map((s) => s.turns) ?? [];
      line(
        `   ${'side'.padEnd(9)} ` +
          turns.map((k) => `K=${k}`.padStart(9)).join(' ') +
          '   (ms per turn, median)',
      );
      for (const r of list) {
        const cells = (r.series ?? []).map((s) =>
          formatMs(summarise(s.samples).median / s.turns).padStart(9),
        );
        line(`   ${r.side.padEnd(9)} ${cells.join(' ')}`);
      }
      line(
        `   flat across K = cost scales with the turn;` +
          ` rising = cost scales with the history` +
          ` (n=${list[0]?.series?.[0]?.samples.length ?? 0} per cell)`,
      );
      continue;
    }

    if (scenario === 'startup') {
      const bySide = new Map<SideName, ScenarioResult[]>();
      for (const r of list) {
        const seen = bySide.get(r.side) ?? [];
        seen.push(r);
        bySide.set(r.side, seen);
      }
      line(
        `   ${'side'.padEnd(9)} ${'construct'.padStart(10)} ${'(min-max)'.padStart(
          15,
        )} ${'first part'.padStart(11)} ${'(min-max)'.padStart(15)}   ms`,
      );
      for (const [side, seen] of bySide) {
        const construct = summarise(seen.map((r) => r.constructMs ?? 0));
        const first = summarise(seen.map((r) => r.firstPartMs ?? 0));
        const range = (s: { min: number; max: number }) =>
          `(${formatMs(s.min)}-${formatMs(s.max)})`;
        line(
          `   ${side.padEnd(9)} ${formatMs(construct.median).padStart(10)} ${range(
            construct,
          ).padStart(15)} ${formatMs(first.median).padStart(11)} ${range(
            first,
          ).padStart(15)}   n=${construct.n}`,
        );
      }
      continue;
    }

    line(
      `   ${'side'.padEnd(9)} ${'median'.padStart(9)} ${'mean'.padStart(9)} ${'min'.padStart(
        9,
      )} ${'max'.padStart(9)} ${'sd'.padStart(8)} ${'rsd'.padStart(6)} ${'per turn'.padStart(
        9,
      )}`,
    );
    for (const r of list) {
      const s = summarise(r.samples);
      const perTurn =
        r.turnsPerSample === undefined
          ? ''
          : formatMs(s.median / r.turnsPerSample);
      line(
        `   ${r.side.padEnd(9)} ${formatMs(s.median).padStart(9)} ${formatMs(
          s.mean,
        ).padStart(
          9,
        )} ${formatMs(s.min).padStart(9)} ${formatMs(s.max).padStart(9)} ${formatMs(
          s.stddev,
        ).padStart(
          8,
        )} ${`${s.rsdPercent.toFixed(1)}%`.padStart(6)} ${perTurn.padStart(9)}`,
      );
    }
    line(
      `   n=${list[0]!.samples.length} kept per side` +
        `; model calls: ${list.map((r) => `${r.side}=${r.modelCalls}`).join(', ')}`,
    );
  }
};

const parent = async (): Promise<void> => {
  const results: ScenarioResult[] = [];

  for (const [side, scenario] of PLAN) {
    // Heap readings are noisy, so the memory scenario is sampled across
    // several fresh processes rather than trusted once.
    const repeats =
      scenario === 'memory'
        ? MEMORY_REPEATS
        : scenario === 'startup'
          ? STARTUP_REPEATS
          : 1;
    for (let i = 0; i < repeats; i++) {
      process.stderr.write(
        `running ${side}/${scenario}${repeats > 1 ? ` (${i + 1}/${repeats})` : ''}...\n`,
      );
      results.push(await runChild(side, scenario));
    }
  }

  line(`node ${process.version} on ${process.platform}/${process.arch}`);
  report(results);
};

const argv = process.argv.slice(2);
if (argv[0] === '--child') {
  await child(argv[1] as SideName, argv[2] as ScenarioName);
} else {
  await parent();
}
