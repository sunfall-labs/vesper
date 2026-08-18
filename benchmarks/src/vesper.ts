// `@sunfall/vesper-agent` against a zero-latency scripted Effect language model.
//
// Everything is the documented default configuration. In particular the stop
// condition is left unset (so `Stop.defaultCondition` applies) and compaction
// is left on. Nothing here is tuned for the benchmark. A number produced by a
// configuration nobody ships is not a number about the library.

import { Agent } from '@sunfall/vesper-agent/agent';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { AgentLog } from '@sunfall/vesper-agent/log';
import { AgentHistory } from '@sunfall/vesper-agent/history';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Deferred,
  Effect,
  Fiber,
  Layer,
  ManagedRuntime,
  Option,
  Stream,
} from 'effect';
import { Schema } from 'effect';
import { AgentEvents } from '@sunfall/vesper-agent/event';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import {
  CONVERSATION_MESSAGES,
  CONVERSATION_STEPS,
  BACKPRESSURE_DELAY_MS,
  BACKPRESSURE_ITERATIONS,
  BACKPRESSURE_PARTS,
  FINAL_TEXT,
  HISTORY_ITERATIONS,
  HISTORY_RECORD_COUNTS,
  INSTRUCTIONS,
  ITERATIONS,
  GROWTH_MESSAGES,
  GROWTH_REPEATS,
  MEMORY_MESSAGES,
  PART_COUNTS,
  PART_ITERATIONS,
  partText,
  SCALING_ITERATIONS,
  SCALING_TURNS,
  type ScenarioName,
  type ScenarioResult,
  script,
  TOOL_DESCRIPTION,
  TOOL_NAME,
  toolResult,
  TURN_STEPS,
  USER_MESSAGE,
  WARMUP,
} from './workload.ts';
import {
  COMPARISON_ITERATIONS,
  COMPARISON_CONCURRENCY,
  COMPARISON_CONCURRENCY_ITERATIONS,
  COMPARISON_CONCURRENCY_WARMUP,
  COMPARISON_GROWTH_MESSAGES,
  COMPARISON_GROWTH_REPEATS,
  COMPARISON_MEMORY_MESSAGES,
  COMPARISON_WARMUP,
  type ConformanceResult,
  type ComparisonResult,
  type ComparisonWorkload,
  stepsFor,
} from './comparison-workload.ts';
import { Prompt } from 'effect/unstable/ai';

// ------------------------------------------------------------------- agent

const lookupOrder = Tool.make(TOOL_NAME, {
  description: TOOL_DESCRIPTION,
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const benchAgent = Agent.make({
  name: 'bench',
  revision: '1',
  instructions: INSTRUCTIONS,
  toolkit: Toolkit.make(lookupOrder),
}).withHandlers({
  lookup_order: ({ orderId }) => Effect.succeed(toolResult(orderId)),
});

// ---------------------------------------------------------------- provider

/**
 * A context window large enough that no run ever compacts.
 *
 * Compaction is on by default and triggers off the model's declared window.
 * Left at a typical provider default, longer scenarios would cross the threshold partway
 * through and the benchmark would be measuring summarisation, not the loop.
 */
export const FAUX_CONTEXT_WINDOW = 100_000_000;

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const makeProvider = () => ({
  responses: [] as ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  state: { responseIndex: 0, totalCalls: 0 },
  setResponses(
    responses: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  ) {
    this.responses = responses;
    this.state.responseIndex = 0;
  },
});

type Handle = ReturnType<typeof makeProvider>;

const loadScript = (handle: Handle, steps: number): void => {
  handle.setResponses(
    script(steps).map((step) =>
      step.kind === 'tool'
        ? [
            {
              type: 'tool-call' as const,
              id: `call-${step.orderId}`,
              name: TOOL_NAME,
              params: { orderId: step.orderId },
            },
            finish('tool-calls'),
          ]
        : [
            { type: 'text-start' as const, id: 'answer' },
            {
              type: 'text-delta' as const,
              id: 'answer',
              delta: FINAL_TEXT,
            },
            { type: 'text-end' as const, id: 'answer' },
            finish(),
          ],
    ),
  );
};

interface HistoryReads {
  pages: number;
  records: number;
}

const countedLogLayer = (reads: HistoryReads) =>
  Layer.effect(
    LogStore.Service,
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      return LogStore.Service.of({
        ...store,
        readBackwards: (path, options) =>
          store.readBackwards(path, options).pipe(
            Effect.tap((page) =>
              Effect.sync(() => {
                reads.pages += 1;
                reads.records += page.records.length;
              }),
            ),
          ),
      });
    }),
  ).pipe(Layer.provide(LogStoreMemory.layer));

const layerFor = (handle: Handle, historyReads?: HistoryReads) =>
  Layer.mergeAll(
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: () => {
          const index = handle.state.responseIndex++;
          handle.state.totalCalls++;
          return Stream.fromIterable(
            handle.responses[Math.min(index, handle.responses.length - 1)]!,
          );
        },
      }),
    ),
    historyReads === undefined
      ? LogStoreMemory.layer
      : countedLogLayer(historyReads),
  );

const assertEqual = (
  actual: number,
  expected: number,
  detail: string,
): void => {
  if (actual !== expected) {
    throw new Error(`${detail}: expected ${expected}, observed ${actual}`);
  }
};

const readyRuntime = async (historyReads?: HistoryReads) => {
  const handle = makeProvider();
  const runtime = ManagedRuntime.make(layerFor(handle, historyReads));
  await runtime.runPromise(Effect.void);
  return { handle, runtime };
};

const callsFor = async (
  handle: Handle,
  expected: number,
  submission: () => Promise<unknown>,
): Promise<void> => {
  const before = handle.state.totalCalls;
  await submission();
  assertEqual(
    handle.state.totalCalls - before,
    expected,
    'model calls in submission',
  );
};

// ---------------------------------------------------------------- scenarios

/**
 * One submission of `steps` model turns.
 *
 * `recording` picks `Conversation.make(agent, id).run(...)` over
 * `agent.run(...)` — the only difference between the two rows in the report.
 */
const runTurn = async (recording: boolean): Promise<ScenarioResult> => {
  const samples: number[] = [];
  let modelCalls = 0;

  const once = async (index: number, measured: boolean): Promise<void> => {
    const { handle, runtime } = await readyRuntime();
    loadScript(handle, TURN_STEPS);
    const before = handle.state.totalCalls;
    const t0 = performance.now();
    const execute = recording
      ? () =>
          runtime.runPromise(
            Conversation.make(benchAgent, `bench-${index}`)
              .run(USER_MESSAGE)
              .pipe(Effect.orDie) as Effect.Effect<unknown>,
          )
      : () =>
          runtime.runPromise(
            benchAgent
              .run(USER_MESSAGE)
              .pipe(Effect.orDie) as Effect.Effect<unknown>,
          );
    await callsFor(handle, TURN_STEPS, execute);
    const elapsed = performance.now() - t0;
    if (measured) {
      samples.push(elapsed);
      modelCalls += handle.state.totalCalls - before;
    }
    await runtime.dispose();
  };

  for (let i = 0; i < WARMUP; i++) await once(i, false);
  for (let i = 0; i < ITERATIONS; i++) await once(WARMUP + i, true);
  assertEqual(modelCalls, ITERATIONS * TURN_STEPS, 'turn scenario model calls');

  return {
    side: recording ? 'vesper+log' : 'vesper',
    scenario: 'turn',
    samples,
    unit: `${TURN_STEPS} model turns, ${TURN_STEPS - 1} tool calls`,
    turnsPerSample: TURN_STEPS,
    modelCalls,
  };
};

/** Recorded comparison workload used only by the opt-in Flue report. */
export const runComparison = async (
  workload: ComparisonWorkload,
): Promise<ComparisonResult> => {
  if (workload === 'startup') return runComparisonStartup();
  if (workload === 'growth') return runComparisonGrowth();
  if (workload === 'memory') return runComparisonMemory();
  if (workload === 'concurrency') return runComparisonConcurrency();

  const steps = stepsFor(workload);
  const samples: number[] = [];
  let modelCalls = 0;

  for (let i = 0; i < COMPARISON_WARMUP + COMPARISON_ITERATIONS; i++) {
    const { handle, runtime } = await readyRuntime();
    loadScript(handle, steps);
    const agent = Conversation.make(benchAgent, `${workload}-${i}`);
    const before = handle.state.totalCalls;
    const t0 = performance.now();
    await callsFor(handle, steps, () =>
      runtime.runPromise(
        Effect.orDie(agent.run(USER_MESSAGE)) as Effect.Effect<unknown>,
      ),
    );
    const elapsed = performance.now() - t0;
    const calls = handle.state.totalCalls - before;
    if (i >= COMPARISON_WARMUP) {
      samples.push(elapsed);
      modelCalls += calls;
    }
    await runtime.dispose();
  }

  assertEqual(
    modelCalls,
    COMPARISON_ITERATIONS * steps,
    `${workload} measured model calls`,
  );
  return {
    side: 'vesper+log',
    workload,
    samples,
    modelCalls,
    callsPerSample: steps,
  };
};

const runComparisonStartup = async (): Promise<ComparisonResult> => {
  const { handle, runtime } = await readyRuntime();
  loadScript(handle, 1);
  await callsFor(handle, 1, () =>
    runtime.runPromise(
      Effect.orDie(
        Conversation.make(benchAgent, 'startup').run(USER_MESSAGE),
      ) as Effect.Effect<unknown>,
    ),
  );
  const started = process.env.VESPER_BENCH_PROCESS_T0_NS;
  if (started === undefined) {
    throw new Error('startup comparison needs VESPER_BENCH_PROCESS_T0_NS');
  }
  const elapsed = Number(process.hrtime.bigint() - BigInt(started)) / 1_000_000;
  await runtime.dispose();
  return {
    side: 'vesper+log',
    workload: 'startup',
    samples: [elapsed],
    modelCalls: handle.state.totalCalls,
    callsPerSample: 1,
  };
};

const runComparisonGrowth = async (): Promise<ComparisonResult> => {
  const growth = Array.from(
    { length: COMPARISON_GROWTH_MESSAGES },
    (_, index) => ({
      index: index + 1,
      samples: [] as number[],
      modelCalls: 0,
      callsPerSample: 1 as const,
    }),
  );
  let modelCalls = 0;
  for (let repeat = 0; repeat <= COMPARISON_GROWTH_REPEATS; repeat++) {
    const { handle, runtime } = await readyRuntime();
    const conversation = Conversation.make(
      benchAgent,
      `comparison-growth-${repeat}`,
    );
    for (let message = 0; message < COMPARISON_GROWTH_MESSAGES; message++) {
      loadScript(handle, 1);
      const before = handle.state.totalCalls;
      const t0 = performance.now();
      await callsFor(handle, 1, () =>
        runtime.runPromise(
          Effect.orDie(
            conversation.resume(`${USER_MESSAGE} (${message})`),
          ) as Effect.Effect<unknown>,
        ),
      );
      const calls = handle.state.totalCalls - before;
      assertEqual(calls, 1, `comparison growth point ${message + 1} calls`);
      if (repeat > 0) {
        growth[message]!.samples.push(performance.now() - t0);
        growth[message]!.modelCalls += calls;
        modelCalls += calls;
      }
    }
    await runtime.dispose();
  }
  assertEqual(
    modelCalls,
    COMPARISON_GROWTH_REPEATS * COMPARISON_GROWTH_MESSAGES,
    'comparison growth model calls',
  );
  return {
    side: 'vesper+log',
    workload: 'growth',
    samples: [],
    growth,
    modelCalls,
  };
};

const collect = (): void => {
  const gc = (globalThis as { gc?: () => void }).gc;
  if (gc === undefined) throw new Error('memory comparison needs --expose-gc');
  gc();
  gc();
};

const runComparisonMemory = async (): Promise<ComparisonResult> => {
  const { handle, runtime } = await readyRuntime();
  const conversation = Conversation.make(benchAgent, 'comparison-memory');
  collect();
  const baseline = process.memoryUsage();
  for (let message = 0; message < COMPARISON_MEMORY_MESSAGES; message++) {
    loadScript(handle, 1);
    await callsFor(handle, 1, () =>
      runtime.runPromise(
        Effect.orDie(
          conversation.resume(`${USER_MESSAGE} (${message})`),
        ) as Effect.Effect<unknown>,
      ),
    );
  }
  handle.setResponses([]);
  collect();
  const after = process.memoryUsage();
  assertEqual(
    handle.state.totalCalls,
    COMPARISON_MEMORY_MESSAGES,
    'comparison memory model calls',
  );
  await runtime.dispose();
  return {
    side: 'vesper+log',
    workload: 'memory',
    samples: [],
    heapBytes: after.heapUsed - baseline.heapUsed,
    rssBytes: after.rss - baseline.rss,
    modelCalls: handle.state.totalCalls,
    callsPerSample: COMPARISON_MEMORY_MESSAGES,
  };
};

const runComparisonConcurrency = async (): Promise<ComparisonResult> => {
  const samples: number[] = [];
  let modelCalls = 0;
  for (
    let repeat = 0;
    repeat < COMPARISON_CONCURRENCY_WARMUP + COMPARISON_CONCURRENCY_ITERATIONS;
    repeat++
  ) {
    const { handle, runtime } = await readyRuntime();
    loadScript(handle, 1);
    const before = handle.state.totalCalls;
    const t0 = performance.now();
    await Promise.all(
      Array.from({ length: COMPARISON_CONCURRENCY }, (_, index) =>
        runtime.runPromise(
          Effect.orDie(
            Conversation.make(
              benchAgent,
              `comparison-concurrent-${repeat}-${index}`,
            ).run(USER_MESSAGE),
          ) as Effect.Effect<unknown>,
        ),
      ),
    );
    const calls = handle.state.totalCalls - before;
    assertEqual(
      calls,
      COMPARISON_CONCURRENCY,
      'comparison concurrency model calls',
    );
    if (repeat >= COMPARISON_CONCURRENCY_WARMUP) {
      samples.push(performance.now() - t0);
      modelCalls += calls;
    }
    await runtime.dispose();
  }
  return {
    side: 'vesper+log',
    workload: 'concurrency',
    samples,
    modelCalls,
    callsPerSample: COMPARISON_CONCURRENCY,
    conversationsPerSample: COMPARISON_CONCURRENCY,
  };
};

export const runConformance = async (): Promise<ConformanceResult> => {
  const { handle, runtime } = await readyRuntime();
  let cancellationCalls = 0;
  const result = await runtime.runPromise(
    Effect.gen(function* () {
      const store = yield* LogStore.Service;
      const fencingConversation =
        LogVocabulary.ConversationId.make('comparison-fencing');
      yield* store.create(fencingConversation, fencingConversation);
      const first = yield* store.acquire(
        fencingConversation,
        LogVocabulary.ProducerId.make('first'),
      );
      yield* store.acquire(
        fencingConversation,
        LogVocabulary.ProducerId.make('replacement'),
      );
      const fenced = yield* store
        .append({
          path: fencingConversation,
          producerId: first.producerId,
          epoch: first.epoch,
          sequence: first.nextSequence,
          records: [
            {
              conversationId: fencingConversation,
              timestamp: Date.now(),
              record: { _tag: 'Text', step: 1, text: 'stale writer' },
            },
          ],
        })
        .pipe(Effect.result);
      if (fenced._tag !== 'Failure' || fenced.failure.reason !== 'fenced') {
        throw new Error('stale Vesper producer was not fenced');
      }

      const recoveryId = LogVocabulary.ConversationId.make(
        'comparison-recovery',
      );
      const recoverySeed = yield* AgentLog.open(recoveryId, {
        compatibility: {
          agent: 'bench',
          revision: LogVocabulary.AgentRevision.make('1'),
        },
      });
      yield* recoverySeed.append([
        {
          _tag: 'RunStarted',
          agent: 'bench',
          agentRevision: LogVocabulary.AgentRevision.make('1'),
          formatVersion: 1,
          prompt: [],
        },
        {
          _tag: 'ToolStarted',
          id: LogVocabulary.ToolCallId.make('uncertain'),
          name: TOOL_NAME,
        },
      ]);
      const recovered = yield* AgentLog.open(recoveryId, {
        compatibility: {
          agent: 'bench',
          revision: LogVocabulary.AgentRevision.make('1'),
        },
      });
      const uncertain = recovered.recovery(
        TOOL_NAME,
        LogVocabulary.ToolCallId.make('uncertain'),
      );
      if (
        !Option.isSome(uncertain) ||
        uncertain.value._tag !== 'Indeterminate'
      ) {
        throw new Error('unfinished Vesper tool was not indeterminate');
      }

      const revisionId = LogVocabulary.ConversationId.make(
        'comparison-revision',
      );
      const old = yield* AgentLog.open(revisionId, {
        compatibility: {
          agent: 'bench',
          revision: LogVocabulary.AgentRevision.make('old'),
        },
      });
      yield* old.append([
        {
          _tag: 'RunStarted',
          agent: 'bench',
          agentRevision: LogVocabulary.AgentRevision.make('old'),
          formatVersion: 1,
          prompt: [],
        },
      ]);
      const mismatch = yield* AgentLog.open(revisionId, {
        compatibility: {
          agent: 'bench',
          revision: LogVocabulary.AgentRevision.make('1'),
        },
      }).pipe(Effect.result);
      if (mismatch._tag !== 'Failure') {
        throw new Error('Vesper accepted mismatched revision history');
      }

      const bytes = new Uint8Array([0, 1, 255]);
      const file = yield* AgentLog.open(
        LogVocabulary.ConversationId.make('comparison-file-bytes'),
        {
          compatibility: {
            agent: 'bench',
            revision: LogVocabulary.AgentRevision.make('1'),
          },
        },
      );
      yield* AgentLog.start(file, {
        agent: 'bench',
        revision: LogVocabulary.AgentRevision.make('1'),
        input: [
          {
            role: 'user',
            content: [
              Prompt.makePart('file', {
                mediaType: 'application/octet-stream',
                fileName: 'input.bin',
                data: bytes,
              }),
            ],
          },
        ],
      });
      const rebuilt = AgentHistory.messagesFrom(yield* file.recorded).content;
      const rebuiltData = (
        rebuilt[0] as { content: ReadonlyArray<{ data?: unknown }> }
      ).content[0]?.data;
      if (
        !(rebuiltData instanceof Uint8Array) ||
        rebuiltData.join(',') !== '0,1,255'
      ) {
        throw new Error('Vesper file bytes did not round-trip');
      }

      const entered = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<void>();
      const blocking = Layer.succeed(
        LanguageModel.LanguageModel,
        yield* LanguageModel.make({
          generateText: () =>
            Effect.succeed<Response.PartEncoded[]>([finish()]),
          streamText: () => {
            cancellationCalls++;
            return Stream.unwrap(
              Deferred.succeed(entered, undefined).pipe(
                Effect.as(
                  Stream.never.pipe(
                    Stream.ensuring(Deferred.succeed(stopped, undefined)),
                  ),
                ),
              ),
            );
          },
        }),
      );
      const cancellation = Conversation.make(benchAgent, 'comparison-cancel');
      const running = yield* Effect.forkChild(
        cancellation.run(USER_MESSAGE).pipe(Effect.provide(blocking)),
      );
      yield* Deferred.await(entered);
      yield* cancellation.send({
        kind: 'cancel',
        text: 'benchmark in-flight cancellation probe',
        source: 'comparison',
      });
      yield* Fiber.join(running);
      yield* Deferred.await(stopped);
      const cancelled = yield* AgentLog.open(
        LogVocabulary.ConversationId.make('comparison-cancel'),
        {
          compatibility: {
            agent: 'bench',
            revision: LogVocabulary.AgentRevision.make('1'),
          },
        },
      );
      if (
        !(yield* cancelled.recorded).some(
          (entry) =>
            entry.record._tag === 'RunSettled' &&
            entry.record.outcome === 'cancelled',
        )
      ) {
        throw new Error('Vesper cancellation did not settle as cancelled');
      }
    }),
  );
  void result;
  assertEqual(cancellationCalls, 1, 'in-flight cancellation model calls');
  assertEqual(handle.state.totalCalls, 0, 'unexpected default provider calls');
  await runtime.dispose();
  return {
    side: 'vesper+log',
    checks: [
      {
        axis: 'producer fencing',
        status: 'verified',
        evidence: 'stale epoch append rejected as fenced',
      },
      {
        axis: 'indeterminate tool handling',
        status: 'verified',
        evidence: 'ToolStarted without ToolOutcome opens as Indeterminate',
      },
      {
        axis: 'revision compatibility',
        status: 'verified',
        evidence: 'mismatched revision rejected before a model call',
      },
      {
        axis: 'durable signal cancellation',
        status: 'verified',
        evidence:
          'provider stream entered before cancel signal; stream was interrupted and cancellation settlement recorded',
      },
      {
        axis: 'runtime abort cancellation',
        status: 'not equivalent',
        evidence:
          'Vesper exposes conversation signal cancellation rather than a Flue-style handle.abort API',
      },
      {
        axis: 'prompt file-byte recording',
        status: 'verified',
        evidence:
          'prompt bytes 0x00 0x01 0xff recorded in LogStoreMemory and rebuilt byte-for-byte',
      },
      {
        axis: 'attachment byte storage',
        status: 'not exercised',
        evidence:
          'separate attachment package is outside the Vesper agent-log harness',
      },
    ],
  };
};

/**
 * A growing conversation: `CONVERSATION_MESSAGES` messages to one
 * conversation, each running `CONVERSATION_STEPS` model turns.
 *
 * Recording is not optional here. Continuing a conversation means having one,
 * and `conversation.resume` rebuilds the prompt from the log. One sample is
 * the whole conversation rather than a single message, because the cost being
 * measured is how the loop behaves as history accumulates.
 */
const runConversation = async (): Promise<ScenarioResult> => {
  const samples: number[] = [];
  let modelCalls = 0;

  const once = async (index: number, measured: boolean): Promise<void> => {
    const { handle, runtime } = await readyRuntime();
    const id = `conv-${index}`;
    const conversation = Conversation.make(benchAgent, id);
    const before = handle.state.totalCalls;
    const t0 = performance.now();
    let turns = 0;
    for (let m = 0; m < CONVERSATION_MESSAGES; m++) {
      loadScript(handle, CONVERSATION_STEPS);
      await callsFor(handle, CONVERSATION_STEPS, () =>
        runtime.runPromise(
          Effect.orDie(
            conversation.resume(`${USER_MESSAGE} (${m})`),
          ) as Effect.Effect<unknown>,
        ),
      );
      turns += CONVERSATION_STEPS;
    }
    assertEqual(
      turns,
      CONVERSATION_MESSAGES * CONVERSATION_STEPS,
      'conversation turns',
    );
    const elapsed = performance.now() - t0;
    if (measured) {
      samples.push(elapsed);
      modelCalls += handle.state.totalCalls - before;
    }
    await runtime.dispose();
  };

  for (let i = 0; i < WARMUP; i++) await once(i, false);
  for (let i = 0; i < ITERATIONS; i++) await once(WARMUP + i, true);
  assertEqual(
    modelCalls,
    ITERATIONS * CONVERSATION_MESSAGES * CONVERSATION_STEPS,
    'conversation scenario model calls',
  );

  return {
    side: 'vesper+log',
    scenario: 'conversation',
    samples,
    unit: `${CONVERSATION_MESSAGES} messages x ${CONVERSATION_STEPS} turns to one conversation`,
    turnsPerSample: CONVERSATION_MESSAGES * CONVERSATION_STEPS,
    modelCalls,
  };
};

/**
 * Cold start: construct the runtime, then time the first streamed content
 * part.
 *
 * Measured once, in a process that has done no other work, because that is
 * what "cold" means. `constructMs` covers building the layer and forcing the
 * model service; `firstPartMs` runs from the `stream` call to the first
 * `Part` event carrying model output.
 */
const runStartup = async (): Promise<ScenarioResult> => {
  const handle = makeProvider();
  loadScript(handle, 1);

  const t0 = performance.now();
  const runtime = ManagedRuntime.make(layerFor(handle));
  // ManagedRuntime builds lazily; force it, or "construct" measures nothing.
  await runtime.runPromise(Effect.void);
  const constructMs = performance.now() - t0;

  const t1 = performance.now();
  await runtime.runPromise(
    benchAgent
      .stream(USER_MESSAGE)
      .pipe(
        Stream.filter(AgentEvents.isPart),
        Stream.take(1),
        Stream.runDrain,
        Effect.orDie,
      ) as Effect.Effect<unknown>,
  );
  const firstPartMs = performance.now() - t1;

  assertEqual(handle.state.totalCalls, 1, 'startup model calls');

  await runtime.dispose();

  return {
    side: 'vesper',
    scenario: 'startup',
    samples: [constructMs + firstPartMs],
    unit: 'cold construct + first streamed content part',
    constructMs,
    firstPartMs,
    modelCalls: handle.state.totalCalls,
  };
};

/** Heap retained by one long conversation, after a forced GC. */
const runMemory = async (): Promise<ScenarioResult> => {
  const handle = makeProvider();
  const runtime = ManagedRuntime.make(layerFor(handle));
  const id = 'memory-conversation';
  const conversation = Conversation.make(benchAgent, id);

  await runtime.runPromise(Effect.void);

  const collect = (): void => {
    const gc = (globalThis as { gc?: () => void }).gc;
    if (gc === undefined) {
      throw new Error('memory scenario needs node --expose-gc');
    }
    gc();
    gc();
  };

  collect();
  const baseline = process.memoryUsage();

  for (let m = 0; m < MEMORY_MESSAGES; m++) {
    loadScript(handle, CONVERSATION_STEPS);
    await callsFor(handle, CONVERSATION_STEPS, () =>
      runtime.runPromise(
        Effect.orDie(
          conversation.resume(`${USER_MESSAGE} (${m})`),
        ) as Effect.Effect<unknown>,
      ),
    );
  }

  assertEqual(
    handle.state.totalCalls,
    MEMORY_MESSAGES * CONVERSATION_STEPS,
    'memory scenario model calls',
  );

  // Do not count the provider's final scripted response as retained log data.
  handle.setResponses([]);
  collect();
  const after = process.memoryUsage();
  const heapBytes = after.heapUsed - baseline.heapUsed;
  const rssBytes = after.rss - baseline.rss;

  await runtime.dispose();

  return {
    side: 'vesper+log',
    scenario: 'memory',
    samples: [],
    unit: `heap retained after ${MEMORY_MESSAGES} messages x ${CONVERSATION_STEPS} turns`,
    heapBytes,
    rssBytes,
    modelCalls: handle.state.totalCalls,
  };
};

/**
 * How the cost of one submission grows with the turns inside it.
 *
 * Each measurement is a fresh conversation, so the only thing varying is how
 * many turns the loop runs before the model stops asking for tools. Flat
 * per-turn cost means the loop pays for the turn; rising per-turn cost means
 * it pays for the history.
 */
const runScaling = async (recording: boolean): Promise<ScenarioResult> => {
  const series: Array<{ turns: number; samples: number[] }> = [];
  let modelCalls = 0;

  for (const turns of SCALING_TURNS) {
    const samples: number[] = [];
    for (let i = 0; i < SCALING_ITERATIONS + WARMUP; i++) {
      const { handle, runtime } = await readyRuntime();
      loadScript(handle, turns);
      const t0 = performance.now();
      const execute = recording
        ? () =>
            runtime.runPromise(
              Conversation.make(benchAgent, `scale-${turns}-${i}`)
                .run(USER_MESSAGE)
                .pipe(Effect.orDie) as Effect.Effect<unknown>,
            )
        : () =>
            runtime.runPromise(
              benchAgent
                .run(USER_MESSAGE)
                .pipe(Effect.orDie) as Effect.Effect<unknown>,
            );
      await callsFor(handle, turns, execute);
      const elapsed = performance.now() - t0;
      if (i >= WARMUP) {
        samples.push(elapsed);
        modelCalls += handle.state.totalCalls;
      }
      await runtime.dispose();
    }
    series.push({ turns, samples });
  }

  const expectedCalls =
    SCALING_ITERATIONS * SCALING_TURNS.reduce((sum, turns) => sum + turns, 0);
  assertEqual(modelCalls, expectedCalls, 'scaling scenario model calls');

  return {
    side: recording ? 'vesper+log' : 'vesper',
    scenario: 'scaling',
    samples: [],
    unit: 'one submission of K turns, fresh conversation each time',
    series,
    modelCalls,
  };
};

/**
 * What each successive message in one conversation costs.
 *
 * Turns per message are fixed, so anything the curve does is the cost of
 * carrying the history — rebuilding the prompt from the log, and whatever else
 * the framework does per submission that is proportional to what came before.
 */
const runGrowth = async (): Promise<ScenarioResult> => {
  const perIndex: number[][] = Array.from(
    { length: GROWTH_MESSAGES },
    () => [],
  );
  let modelCalls = 0;

  for (let repeat = 0; repeat < GROWTH_REPEATS + 1; repeat++) {
    const { handle, runtime } = await readyRuntime();
    const id = `growth-${repeat}`;
    const conversation = Conversation.make(benchAgent, id);
    for (let m = 0; m < GROWTH_MESSAGES; m++) {
      loadScript(handle, CONVERSATION_STEPS);
      const t0 = performance.now();
      await callsFor(handle, CONVERSATION_STEPS, () =>
        runtime.runPromise(
          Effect.orDie(
            conversation.resume(`${USER_MESSAGE} (${m})`),
          ) as Effect.Effect<unknown>,
        ),
      );
      const elapsed = performance.now() - t0;
      // The first whole conversation is warmup and is thrown away.
      if (repeat > 0) perIndex[m]!.push(elapsed);
    }
    if (repeat > 0) modelCalls += handle.state.totalCalls;
    await runtime.dispose();
  }

  assertEqual(
    modelCalls,
    GROWTH_REPEATS * GROWTH_MESSAGES * CONVERSATION_STEPS,
    'growth scenario model calls',
  );

  return {
    side: 'vesper+log',
    scenario: 'growth',
    samples: [],
    unit: `message N of one conversation, ${CONVERSATION_STEPS} turns per message`,
    growth: perIndex.map((samples, index) => ({ index: index + 1, samples })),
    modelCalls,
  };
};

const loadParts = (handle: Handle, parts: number): void => {
  handle.setResponses([
    [
      { type: 'text-start' as const, id: 'answer' },
      ...partText(parts).map((delta) => ({
        type: 'text-delta' as const,
        id: 'answer',
        delta,
      })),
      { type: 'text-end' as const, id: 'answer' },
      finish(),
    ],
  ]);
};

/** Fixed output bytes split across provider delta counts. */
const runParts = async (recording: boolean): Promise<ScenarioResult> => {
  const partSeries: Array<{ parts: number; samples: number[] }> = [];
  let modelCalls = 0;

  for (const parts of PART_COUNTS) {
    // The 10k-delta cell is intentionally opt-in; it dominates default runtime.
    if (parts === 10_000 && process.env.VESPER_BENCH_HEAVY !== '1') continue;
    const samples: number[] = [];
    for (let i = 0; i < PART_ITERATIONS + WARMUP; i++) {
      const { handle, runtime } = await readyRuntime();
      loadParts(handle, parts);
      let observedParts = 0;
      let observedBytes = 0;
      const t0 = performance.now();
      const drain = <E, R>(
        events: Stream.Stream<AgentEvents.ObservedEvent<any>, E, R>,
      ) =>
        runtime.runPromise(
          events.pipe(
            Stream.tap((event) =>
              Effect.sync(() => {
                if (event._tag === 'Part' && event.part.type === 'text-delta') {
                  observedParts++;
                  observedBytes += event.part.delta.length;
                }
              }),
            ),
            Stream.runDrain,
            Effect.orDie,
          ) as Effect.Effect<unknown>,
        );
      await callsFor(handle, 1, () =>
        recording
          ? drain(
              Conversation.make(benchAgent, `parts-${parts}-${i}`).stream(
                USER_MESSAGE,
              ),
            )
          : drain(benchAgent.stream(USER_MESSAGE)),
      );
      const elapsed = performance.now() - t0;
      assertEqual(observedParts, parts, `parts scenario deltas at ${parts}`);
      assertEqual(observedBytes, 10_000, `parts scenario bytes at ${parts}`);
      if (i >= WARMUP) {
        samples.push(elapsed);
        modelCalls += handle.state.totalCalls;
      }
      await runtime.dispose();
    }
    partSeries.push({ parts, samples });
  }

  assertEqual(
    modelCalls,
    PART_ITERATIONS * partSeries.length,
    'parts scenario model calls',
  );
  return {
    side: recording ? 'vesper+log' : 'vesper',
    scenario: 'parts',
    samples: [],
    unit: '10,000 output bytes split across N text deltas',
    partSeries,
    modelCalls,
  };
};

/** AgentLog.open over synthetic histories populated through Session.append. */
const runHistoryOpen = async (): Promise<ScenarioResult> => {
  const historySeries: NonNullable<ScenarioResult['historySeries']>[number][] =
    [];

  for (const records of HISTORY_RECORD_COUNTS) {
    for (const mode of [
      'uncompacted',
      'compacted-fixed-tail',
      'orphan-suffix',
    ] as const) {
      const reads = { pages: 0, records: 0 };
      const { runtime } = await readyRuntime(reads);
      const id = LogVocabulary.ConversationId.make(
        `history-${mode}-${records}`,
      );
      const session = await runtime.runPromise(
        AgentLog.open(id, {
          compatibility: {
            agent: 'bench',
            revision: LogVocabulary.AgentRevision.make('1'),
          },
        }),
      );
      const oldCount = mode === 'uncompacted' ? records : records - 10;
      await runtime.runPromise(
        session.append(
          Array.from({ length: oldCount }, (_, step) => ({
            _tag: 'Text' as const,
            step,
            text: `fixture-${step}`,
          })),
        ),
      );
      if (mode !== 'uncompacted') {
        await runtime.runPromise(
          session.append(
            Array.from({ length: 10 }, (_, index) => ({
              _tag: 'Text' as const,
              step: oldCount + index,
              text: `kept-${index}`,
            })),
          ),
        );
        const beforeCompaction = await runtime.runPromise(session.recorded);
        await runtime.runPromise(
          session.append([
            {
              _tag: 'Compacted',
              formatVersion: 1,
              agent: 'bench',
              agentRevision: LogVocabulary.AgentRevision.make('1'),
              step: records,
              summary: 'fixed summary',
              firstKept: beforeCompaction[oldCount]!.offset,
              summarizedMessages: oldCount,
              keptMessages: 10,
            },
            ...Array.from({ length: 5 }, (_, index) => ({
              _tag: 'Text' as const,
              step: records + index,
              text: `tail-${index}`,
            })),
          ]),
        );
      }
      await runtime.runPromise(
        session.append([
          {
            _tag: 'RunSettled',
            outcome: 'success',
            detail: '',
            steps: records,
            usage: { input: records, output: records },
          },
        ]),
      );
      if (mode === 'orphan-suffix') {
        await runtime.runPromise(
          session.append([
            {
              _tag: 'RunStarted',
              agent: 'bench',
              formatVersion: 1,
              agentRevision: LogVocabulary.AgentRevision.make('1'),
              prompt: [],
            },
            {
              _tag: 'ToolStarted',
              id: LogVocabulary.ToolCallId.make('orphan'),
              name: TOOL_NAME,
            },
          ]),
        );
      }

      reads.pages = 0;
      reads.records = 0;
      const samples: number[] = [];
      let retained = 0;
      for (let i = 0; i < HISTORY_ITERATIONS + WARMUP; i++) {
        const t0 = performance.now();
        const opened = await runtime.runPromise(
          AgentLog.open(id, {
            compatibility: {
              agent: 'bench',
              revision: LogVocabulary.AgentRevision.make('1'),
            },
          }),
        );
        const elapsed = performance.now() - t0;
        retained = opened.history.length;
        if (i >= WARMUP) samples.push(elapsed);
      }
      const opens = HISTORY_ITERATIONS + WARMUP;
      historySeries.push({
        mode,
        records,
        liveRecords: retained,
        samples,
        pages: reads.pages / opens,
        recordsRead: reads.records / opens,
      });
      await runtime.dispose();
    }
  }

  return {
    side: 'vesper+log',
    scenario: 'history-open',
    samples: [],
    unit: 'AgentLog.open by lifetime and retained suffix shape',
    historySeries,
    modelCalls: 0,
  };
};

/** A delayed downstream pull must directly extend stream completion time. */
const runBackpressure = async (recording: boolean): Promise<ScenarioResult> => {
  const samples: number[] = [];
  let modelCalls = 0;
  let consumedParts = 0;

  for (let i = 0; i < BACKPRESSURE_ITERATIONS + WARMUP; i++) {
    const { handle, runtime } = await readyRuntime();
    loadParts(handle, BACKPRESSURE_PARTS);
    let observed = 0;
    const t0 = performance.now();
    const drain = <E, R>(
      events: Stream.Stream<AgentEvents.ObservedEvent<any>, E, R>,
    ) =>
      runtime.runPromise(
        events.pipe(
          Stream.tap((event) =>
            event._tag === 'Part' && event.part.type === 'text-delta'
              ? Effect.sleep(`${BACKPRESSURE_DELAY_MS} millis`).pipe(
                  Effect.tap(() => Effect.sync(() => observed++)),
                )
              : Effect.void,
          ),
          Stream.runDrain,
          Effect.orDie,
        ) as Effect.Effect<unknown>,
      );
    await callsFor(handle, 1, () =>
      recording
        ? drain(
            Conversation.make(benchAgent, `backpressure-${i}`).stream(
              USER_MESSAGE,
            ),
          )
        : drain(benchAgent.stream(USER_MESSAGE)),
    );
    const elapsed = performance.now() - t0;
    assertEqual(observed, BACKPRESSURE_PARTS, 'backpressure consumed deltas');
    if (i >= WARMUP) {
      const minimum = BACKPRESSURE_PARTS * BACKPRESSURE_DELAY_MS * 0.75;
      if (elapsed < minimum) {
        throw new Error(
          `consumer delay did not backpressure stream: expected at least ${minimum}ms, observed ${elapsed}ms`,
        );
      }
      samples.push(elapsed);
      consumedParts += observed;
      modelCalls += handle.state.totalCalls;
    }
    await runtime.dispose();
  }

  assertEqual(modelCalls, BACKPRESSURE_ITERATIONS, 'backpressure model calls');
  assertEqual(
    consumedParts,
    BACKPRESSURE_ITERATIONS * BACKPRESSURE_PARTS,
    'backpressure measured deltas',
  );
  return {
    side: recording ? 'vesper+log' : 'vesper',
    scenario: 'backpressure',
    samples,
    unit: `${BACKPRESSURE_PARTS} deltas with ${BACKPRESSURE_DELAY_MS}ms downstream delay each`,
    turnsPerSample: 1,
    modelCalls,
    consumedParts,
    consumerDelayMs: BACKPRESSURE_DELAY_MS,
  };
};

export const run = async (
  scenario: ScenarioName,
  recording: boolean,
): Promise<ScenarioResult> => {
  switch (scenario) {
    case 'turn':
      return runTurn(recording);
    case 'scaling':
      return runScaling(recording);
    case 'growth':
      return runGrowth();
    case 'parts':
      return runParts(recording);
    case 'history-open':
      return runHistoryOpen();
    case 'backpressure':
      return runBackpressure(recording);
    case 'conversation':
      return runConversation();
    case 'startup':
      return runStartup();
    case 'memory':
      return runMemory();
  }
};
