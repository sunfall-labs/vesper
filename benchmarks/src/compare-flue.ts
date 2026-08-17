// Opt-in external comparison. Each side/workload runs in its own process.

import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

import {
  COMPARISON_PROCESS_REPEATS,
  type ComparisonResult,
  type ComparisonSide,
  type ComparisonWorkload,
  type ConformanceResult,
} from './comparison-workload.ts';
import { formatMs, summarise } from './stats.ts';
import type { ScenarioResult } from './workload.ts';

const SELF = fileURLToPath(import.meta.url);
const SIDES: ReadonlyArray<ComparisonSide> = ['vesper+log', 'flue@2.0.3'];
const TIMED: ReadonlyArray<ComparisonWorkload> = [
  'one-turn',
  'tool-loop',
  'growth',
  'concurrency',
];

const validate = (result: ComparisonResult): void => {
  if (result.workload === 'growth') {
    const points = result.growth ?? [];
    if (points.length === 0)
      throw new Error(`${result.side}/growth has no points`);
    for (const point of points) {
      const expected = point.samples.length * point.callsPerSample;
      if (point.modelCalls !== expected) {
        throw new Error(
          `${result.side}/growth point ${point.index} reported ${point.modelCalls} calls for ${expected} expected`,
        );
      }
    }
    const expected = points.reduce((sum, point) => sum + point.modelCalls, 0);
    if (result.modelCalls !== expected) {
      throw new Error(
        `${result.side}/growth reported ${result.modelCalls} calls for ${expected} point calls`,
      );
    }
    return;
  }
  const sampleCount =
    result.workload === 'memory' || result.workload === 'startup'
      ? 1
      : result.samples.length;
  if (result.callsPerSample === undefined) {
    throw new Error(`${result.side}/${result.workload} omitted callsPerSample`);
  }
  const expected = sampleCount * result.callsPerSample;
  if (result.modelCalls !== expected) {
    throw new Error(
      `${result.side}/${result.workload} reported ${result.modelCalls} calls for ${expected} expected`,
    );
  }
};

const child = async (
  side: ComparisonSide,
  workload: ComparisonWorkload,
): Promise<void> => {
  const result =
    side === 'vesper+log'
      ? await (await import('./vesper.ts')).runComparison(workload)
      : await (await import('./flue.ts')).run(workload);
  validate(result);
  process.stdout.write(`##BENCH##${JSON.stringify(result)}\n`);
};

const conformanceChild = async (side: ComparisonSide): Promise<void> => {
  const result =
    side === 'vesper+log'
      ? await (await import('./vesper.ts')).runConformance()
      : await (await import('./flue.ts')).runConformance();
  process.stdout.write(`##BENCH##${JSON.stringify(result)}\n`);
};

const historyChild = async (): Promise<void> => {
  const result = await (await import('./vesper.ts')).run('history-open', true);
  process.stdout.write(`##BENCH##${JSON.stringify(result)}\n`);
};

const runChild = <T>(
  args: ReadonlyArray<string>,
  label: string,
  startup = false,
): Promise<T> =>
  new Promise((resolve, reject) => {
    const env = startup
      ? {
          ...process.env,
          VESPER_BENCH_PROCESS_T0_NS: process.hrtime.bigint().toString(),
        }
      : process.env;
    const proc = spawn(process.execPath, ['--expose-gc', SELF, ...args], {
      stdio: ['ignore', 'pipe', 'inherit'],
      env,
    });
    let out = '';
    proc.stdout.on('data', (chunk: Buffer) => {
      out += chunk.toString();
    });
    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`${label} exited with ${code}`));
        return;
      }
      const marker = out.lastIndexOf('##BENCH##');
      if (marker === -1) {
        reject(new Error(`${label} produced no result`));
        return;
      }
      resolve(JSON.parse(out.slice(marker + 9).trim()) as T);
    });
  });

const line = (text = ''): void => {
  process.stdout.write(`${text}\n`);
};

const mib = (bytes: number): string => (bytes / 1024 / 1024).toFixed(1);

const timingTable = (results: ReadonlyArray<ComparisonResult>): void => {
  line(
    'completed submission latency and independent-conversation batch throughput:',
  );
  line(
    `${'workload'.padEnd(12)} ${'side'.padEnd(12)} ${'median'.padStart(9)} ${'mean'.padStart(9)} ${'min'.padStart(9)} ${'max'.padStart(9)} ${'calls'.padStart(7)}`,
  );
  for (const workload of ['one-turn', 'tool-loop', 'concurrency'] as const) {
    for (const result of results.filter(
      (entry) => entry.workload === workload,
    )) {
      const summary = summarise(result.samples);
      line(
        `${workload.padEnd(12)} ${result.side.padEnd(12)} ${formatMs(summary.median).padStart(9)} ${formatMs(summary.mean).padStart(9)} ${formatMs(summary.min).padStart(9)} ${formatMs(summary.max).padStart(9)} ${String(result.modelCalls).padStart(7)}`,
      );
    }
  }
  line(
    'concurrency is one batch of 16 fresh, independent conversations; lower batch ms means higher throughput.',
  );
};

const growthTable = (results: ReadonlyArray<ComparisonResult>): void => {
  line();
  line(
    'steady conversation growth (one model call per submission, one recorded in-memory conversation):',
  );
  const picks = [1, 5, 10, 20, 30];
  line(
    `${'side'.padEnd(12)} ${picks.map((index) => `msg ${index}`.padStart(9)).join(' ')}   medians`,
  );
  for (const result of results.filter((entry) => entry.workload === 'growth')) {
    const cells = picks.map((index) => {
      const point = result.growth?.find((entry) => entry.index === index);
      return formatMs(summarise(point?.samples ?? []).median).padStart(9);
    });
    line(`${result.side.padEnd(12)} ${cells.join(' ')}`);
  }
};

const processTable = (results: ReadonlyArray<ComparisonResult>): void => {
  line();
  line('fresh-process measurements:');
  line(
    `${'axis'.padEnd(12)} ${'side'.padEnd(12)} ${'median'.padStart(10)} ${'min'.padStart(10)} ${'max'.padStart(10)}   n`,
  );
  for (const side of SIDES) {
    const startup = results.filter(
      (entry) => entry.side === side && entry.workload === 'startup',
    );
    const summary = summarise(startup.flatMap((entry) => entry.samples));
    line(
      `${'cold complete'.padEnd(12)} ${side.padEnd(12)} ${formatMs(summary.median).padStart(10)} ${formatMs(summary.min).padStart(10)} ${formatMs(summary.max).padStart(10)}   ${summary.n}`,
    );
    const memory = results.filter(
      (entry) => entry.side === side && entry.workload === 'memory',
    );
    const heap = summarise(memory.map((entry) => entry.heapBytes ?? 0));
    const rss = summarise(memory.map((entry) => entry.rssBytes ?? 0));
    line(
      `${'heap MiB'.padEnd(12)} ${side.padEnd(12)} ${mib(heap.median).padStart(10)} ${mib(heap.min).padStart(10)} ${mib(heap.max).padStart(10)}   ${heap.n}`,
    );
    line(
      `${'rss MiB'.padEnd(12)} ${side.padEnd(12)} ${mib(rss.median).padStart(10)} ${mib(rss.min).padStart(10)} ${mib(rss.max).padStart(10)}   ${rss.n}`,
    );
  }
};

const structuralTable = (history: ScenarioResult): void => {
  line();
  line(
    'Vesper-only compacted long-history open structure (not a Flue comparison):',
  );
  line(
    `${'mode'.padEnd(22)} ${'lifetime'.padStart(9)} ${'live'.padStart(6)} ${'pages'.padStart(7)} ${'records read'.padStart(13)}`,
  );
  for (const point of history.historySeries ?? []) {
    line(
      `${point.mode.padEnd(22)} ${String(point.records).padStart(9)} ${String(point.liveRecords).padStart(6)} ${String(point.pages).padStart(7)} ${String(point.recordsRead).padStart(13)}`,
    );
  }
};

const conformanceTable = (results: ReadonlyArray<ConformanceResult>): void => {
  line();
  line('recovery/operational conformance (not timings):');
  line(
    `${'axis'.padEnd(29)} ${'side'.padEnd(12)} ${'status'.padEnd(15)} evidence`,
  );
  for (const axis of results[0]?.checks.map((entry) => entry.axis) ?? []) {
    for (const result of results) {
      const check = result.checks.find((entry) => entry.axis === axis);
      if (check !== undefined) {
        line(
          `${axis.padEnd(29)} ${result.side.padEnd(12)} ${check.status.padEnd(15)} ${check.evidence}`,
        );
      }
    }
  }
};

const parent = async (): Promise<void> => {
  const results: ComparisonResult[] = [];
  for (const workload of TIMED) {
    for (const side of SIDES) {
      process.stderr.write(`running ${side}/${workload}...\n`);
      results.push(
        await runChild(['--child', side, workload], `${side}/${workload}`),
      );
    }
  }
  for (const [workloadIndex, workload] of (
    ['startup', 'memory'] as const
  ).entries()) {
    for (let repeat = 0; repeat < COMPARISON_PROCESS_REPEATS; repeat++) {
      const order =
        (repeat + workloadIndex) % 2 === 0 ? SIDES : [...SIDES].reverse();
      for (const side of order) {
        process.stderr.write(
          `running ${side}/${workload} (${repeat + 1}/${COMPARISON_PROCESS_REPEATS})...\n`,
        );
        results.push(
          await runChild(
            ['--child', side, workload],
            `${side}/${workload}`,
            workload === 'startup',
          ),
        );
      }
    }
  }
  process.stderr.write('running Vesper compacted-history structure...\n');
  const history = await runChild<ScenarioResult>(
    ['--history'],
    'Vesper/history-open',
  );
  const conformance: ConformanceResult[] = [];
  for (const side of SIDES) {
    process.stderr.write(`running ${side}/conformance...\n`);
    conformance.push(
      await runChild(['--conformance', side], `${side}/conformance`),
    );
  }

  line(`node ${process.version} on ${process.platform}/${process.arch}`);
  line('opt-in external comparison: @flue/runtime@2.0.3');
  line(
    'zero-network deterministic scripted providers; exact model-call counts asserted; every side/workload isolated by process',
  );
  line(
    'one-turn = 1 call/no tool; tool-loop = 8 calls/7 tools; both timed from submission through completed final reply',
  );
  timingTable(results);
  growthTable(results);
  processTable(results);
  structuralTable(history);
  conformanceTable(conformance);
  line();
  line(
    'caveats: provider event protocols, native adapters, prompt projection, record shapes, and storage engines differ; these are concrete harness timings, not direct framework-speed rankings. Flue uses volatile process-local SQLite and Vesper uses volatile LogStoreMemory.',
  );
  line(
    'Flue timings include its Provider-to-Pi-event adapter path; Vesper timings include Effect LanguageModel stream handling. No adapter-subtracted framework-speed metric is claimed.',
  );
  line(
    'cold complete includes process creation, module loading, runtime/database startup, one model call, and in-memory recording/settlement, but excludes shutdown. Side order alternates across repeats.',
  );
  line(
    'memory is retained delta after 60 one-call submissions and forced GC; RSS is allocator-sensitive and may be zero or negative at this scale.',
  );
  line(
    'Flue interrupted-tool behavior is documented by Flue but intentionally remains "not exercised" here because this harness does not fake a process crash.',
  );
};

const argv = process.argv.slice(2);
if (argv[0] === '--child') {
  await child(argv[1] as ComparisonSide, argv[2] as ComparisonWorkload);
} else if (argv[0] === '--conformance') {
  await conformanceChild(argv[1] as ComparisonSide);
} else if (argv[0] === '--history') {
  await historyChild();
} else {
  await parent();
}
