import {
  AgentRunError,
  defineTool,
  init,
  useModel,
  useTool,
} from '@flue/runtime';
import { sqlite, start } from '@flue/runtime/node';
import {
  InMemoryAttachmentStore,
  createAttachmentRef,
} from '@flue/runtime/adapter';
import {
  createAssistantMessageEventStream,
  type AssistantMessage,
  type Model,
  type Provider,
  type StreamOptions,
  type ToolCall,
} from '@earendil-works/pi-ai';
import * as v from 'valibot';

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
import {
  FINAL_TEXT,
  INSTRUCTIONS,
  TOOL_DESCRIPTION,
  TOOL_NAME,
  USER_MESSAGE,
  script,
  toolResult,
} from './workload.ts';

const PROVIDER_ID = 'benchmark-scripted';
const MODEL_ID = 'benchmark';
const usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
const model: Model<string> = {
  id: MODEL_ID,
  name: MODEL_ID,
  api: PROVIDER_ID,
  provider: PROVIDER_ID,
  baseUrl: 'http://localhost.invalid',
  reasoning: false,
  input: ['text'],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 100_000_000,
  maxTokens: 1_024,
};

interface ScriptedProvider {
  readonly provider: Provider;
  readonly state: { callCount: number };
  setResponses(
    responses: ReadonlyArray<ReturnType<typeof script>[number]>,
  ): void;
  blockUntilAbort(onEntered: () => void): void;
}

const assistantMessage = (
  content: AssistantMessage['content'],
  stopReason: AssistantMessage['stopReason'],
): AssistantMessage => ({
  role: 'assistant',
  content,
  api: PROVIDER_ID,
  provider: PROVIDER_ID,
  model: MODEL_ID,
  usage,
  stopReason,
  timestamp: 0,
});

/** Minimal deterministic implementation of the Provider interface Flue accepts. */
const makeScriptedProvider = (): ScriptedProvider => {
  let responses: ReadonlyArray<ReturnType<typeof script>[number]> = [];
  let responseIndex = 0;
  let blocking: (() => void) | undefined;
  const state = { callCount: 0 };

  const stream = (
    _model: Model<string>,
    _context: unknown,
    options?: StreamOptions,
  ) => {
    const output = createAssistantMessageEventStream();
    const step = responses[responseIndex++];
    state.callCount++;

    queueMicrotask(() => {
      if (blocking !== undefined) {
        const entered = blocking;
        blocking = undefined;
        const partial = assistantMessage([], 'pending');
        output.push({ type: 'start', partial });
        entered();
        const abort = () => {
          const aborted = {
            ...partial,
            stopReason: 'aborted' as const,
            errorMessage: 'Request was aborted',
          };
          output.push({ type: 'error', reason: 'aborted', error: aborted });
          output.end(aborted);
        };
        if (options?.signal?.aborted === true) {
          abort();
        } else {
          options?.signal?.addEventListener('abort', abort, { once: true });
        }
        return;
      }
      if (step === undefined) {
        const failed = {
          ...assistantMessage([], 'error'),
          errorMessage: 'No scripted response',
        };
        output.push({ type: 'error', reason: 'error', error: failed });
        output.end(failed);
        return;
      }

      const toolCall: ToolCall | undefined =
        step.kind === 'tool'
          ? {
              type: 'toolCall',
              id: `call-${step.orderId}`,
              name: TOOL_NAME,
              arguments: { orderId: step.orderId },
            }
          : undefined;
      const content: AssistantMessage['content'] =
        toolCall === undefined
          ? [{ type: 'text', text: FINAL_TEXT }]
          : [toolCall];
      const reason =
        step.kind === 'tool' ? ('toolUse' as const) : ('stop' as const);
      const message = assistantMessage(content, reason);
      const partial = assistantMessage([], 'pending');
      output.push({ type: 'start', partial });
      if (step.kind === 'tool') {
        output.push({ type: 'toolcall_start', contentIndex: 0, partial });
        output.push({
          type: 'toolcall_delta',
          contentIndex: 0,
          delta: `{"orderId":"${step.orderId}"}`,
          partial,
        });
        if (toolCall === undefined) {
          throw new Error('scripted tool response did not create a tool call');
        }
        output.push({
          type: 'toolcall_end',
          contentIndex: 0,
          toolCall,
          partial: message,
        });
      } else {
        output.push({ type: 'text_start', contentIndex: 0, partial });
        output.push({
          type: 'text_delta',
          contentIndex: 0,
          delta: FINAL_TEXT,
          partial: message,
        });
        output.push({
          type: 'text_end',
          contentIndex: 0,
          content: FINAL_TEXT,
          partial: message,
        });
      }
      output.push({ type: 'done', reason, message });
      output.end(message);
    });
    return output;
  };

  return {
    provider: {
      id: PROVIDER_ID,
      name: PROVIDER_ID,
      auth: {
        apiKey: {
          name: PROVIDER_ID,
          resolve: () => Promise.resolve({ auth: {} }),
        },
      },
      getModels: () => [model],
      stream,
      streamSimple: stream,
    } as Provider,
    state,
    setResponses(next) {
      responses = next;
      responseIndex = 0;
    },
    blockUntilAbort(onEntered) {
      responses = [];
      responseIndex = 0;
      blocking = onEntered;
    },
  };
};

const lookupOrder = defineTool({
  name: TOOL_NAME,
  description: TOOL_DESCRIPTION,
  input: v.object({ orderId: v.string() }),
  output: v.object({ status: v.string() }),
  durable: true,
  run: async ({ data, step }) => ({
    output: await step.do('lookup', () =>
      Promise.resolve(toolResult(data.orderId)),
    ),
  }),
});

function BenchmarkAgent(): string {
  useModel(`${PROVIDER_ID}/${MODEL_ID}`, { compaction: false });
  useTool(lookupOrder);
  return INSTRUCTIONS;
}

const loadScript = (provider: ScriptedProvider, steps: number): void => {
  provider.setResponses(script(steps));
};

const assertEqual = (
  actual: number,
  expected: number,
  detail: string,
): void => {
  if (actual !== expected) {
    throw new Error(
      `${detail}: expected ${String(expected)}, observed ${String(actual)}`,
    );
  }
};

export const run = async (
  workload: ComparisonWorkload,
): Promise<ComparisonResult> => {
  if (workload === 'startup') {
    return runStartup();
  }
  if (workload === 'growth') {
    return runGrowth();
  }
  if (workload === 'memory') {
    return runMemory();
  }
  if (workload === 'concurrency') {
    return runConcurrency();
  }

  const steps = stepsFor(workload);
  const samples: number[] = [];
  let modelCalls = 0;

  for (let i = 0; i < COMPARISON_WARMUP + COMPARISON_ITERATIONS; i++) {
    const setup = await startRuntime();
    await using _flue = setup.runtime;
    loadScript(setup.provider, steps);
    const agent = init(BenchmarkAgent, { id: `${workload}-${String(i)}` });
    const t0 = performance.now();
    const receipt = await agent.dispatch(USER_MESSAGE);
    const reply = await agent.read(receipt);
    const elapsed = performance.now() - t0;
    const calls = setup.provider.state.callCount;

    assertEqual(calls, steps, `${workload} sample ${String(i)} model calls`);
    if (reply.text !== FINAL_TEXT) {
      throw new Error(
        `${workload} sample ${String(i)} returned ${JSON.stringify(reply.text)}`,
      );
    }
    if (i >= COMPARISON_WARMUP) {
      samples.push(elapsed);
      modelCalls += calls;
    }
  }

  assertEqual(
    modelCalls,
    COMPARISON_ITERATIONS * steps,
    `${workload} measured model calls`,
  );

  return {
    side: 'flue@2.0.3',
    workload,
    samples,
    modelCalls,
    callsPerSample: steps,
  };
};

const startRuntime = async () => {
  const provider = makeScriptedProvider();
  const runtime = await start({
    agents: [BenchmarkAgent],
    db: sqlite(':memory:'),
    providers: [provider.provider],
  });
  return { provider, runtime };
};

const dispatchOne = async (
  id: string,
  message = USER_MESSAGE,
): Promise<void> => {
  const agent = init(BenchmarkAgent, { id });
  const receipt = await agent.dispatch(message);
  const reply = await agent.read(receipt);
  if (reply.text !== FINAL_TEXT) {
    throw new Error(`${id} returned ${JSON.stringify(reply.text)}`);
  }
};

const runStartup = async (): Promise<ComparisonResult> => {
  const setup = await startRuntime();
  await using _flue = setup.runtime;
  loadScript(setup.provider, 1);
  await dispatchOne('startup');
  const started = process.env['VESPER_BENCH_PROCESS_T0_NS'];
  if (started === undefined) {
    throw new Error('startup comparison needs VESPER_BENCH_PROCESS_T0_NS');
  }
  const elapsed = Number(process.hrtime.bigint() - BigInt(started)) / 1_000_000;
  return {
    side: 'flue@2.0.3',
    workload: 'startup',
    samples: [elapsed],
    modelCalls: setup.provider.state.callCount,
    callsPerSample: 1,
  };
};

const runGrowth = async (): Promise<ComparisonResult> => {
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
    const setup = await startRuntime();
    await using _flue = setup.runtime;
    for (let message = 0; message < COMPARISON_GROWTH_MESSAGES; message++) {
      loadScript(setup.provider, 1);
      const before = setup.provider.state.callCount;
      const t0 = performance.now();
      await dispatchOne(
        `comparison-growth-${String(repeat)}`,
        `${USER_MESSAGE} (${String(message)})`,
      );
      const calls = setup.provider.state.callCount - before;
      assertEqual(
        calls,
        1,
        `comparison growth point ${String(message + 1)} calls`,
      );
      if (repeat > 0) {
        const point = growth[message];
        if (point === undefined) {
          throw new Error(`missing growth point ${String(message)}`);
        }
        point.samples.push(performance.now() - t0);
        point.modelCalls += calls;
        modelCalls += calls;
      }
    }
  }
  assertEqual(
    modelCalls,
    COMPARISON_GROWTH_REPEATS * COMPARISON_GROWTH_MESSAGES,
    'comparison growth model calls',
  );
  return {
    side: 'flue@2.0.3',
    workload: 'growth',
    samples: [],
    growth,
    modelCalls,
  };
};

const isGc = (value: unknown): value is () => void =>
  typeof value === 'function';

const collect = (): void => {
  const maybeGc: unknown = Reflect.get(globalThis, 'gc');
  if (!isGc(maybeGc)) {
    throw new Error('memory comparison needs --expose-gc');
  }
  maybeGc();
  maybeGc();
};

const runMemory = async (): Promise<ComparisonResult> => {
  const setup = await startRuntime();
  await using _flue = setup.runtime;
  collect();
  const baseline = process.memoryUsage();
  for (let message = 0; message < COMPARISON_MEMORY_MESSAGES; message++) {
    loadScript(setup.provider, 1);
    await dispatchOne(
      'comparison-memory',
      `${USER_MESSAGE} (${String(message)})`,
    );
  }
  setup.provider.setResponses([]);
  collect();
  const after = process.memoryUsage();
  assertEqual(
    setup.provider.state.callCount,
    COMPARISON_MEMORY_MESSAGES,
    'comparison memory model calls',
  );
  return {
    side: 'flue@2.0.3',
    workload: 'memory',
    samples: [],
    heapBytes: after.heapUsed - baseline.heapUsed,
    rssBytes: after.rss - baseline.rss,
    modelCalls: setup.provider.state.callCount,
    callsPerSample: COMPARISON_MEMORY_MESSAGES,
  };
};

const runConcurrency = async (): Promise<ComparisonResult> => {
  const samples: number[] = [];
  let modelCalls = 0;
  for (
    let repeat = 0;
    repeat < COMPARISON_CONCURRENCY_WARMUP + COMPARISON_CONCURRENCY_ITERATIONS;
    repeat++
  ) {
    const setup = await startRuntime();
    await using _flue = setup.runtime;
    setup.provider.setResponses(
      Array.from({ length: COMPARISON_CONCURRENCY }, () => {
        const [step] = script(1);
        if (step === undefined) {
          throw new Error('script did not produce a step');
        }
        return step;
      }),
    );
    const t0 = performance.now();
    await Promise.all(
      Array.from({ length: COMPARISON_CONCURRENCY }, (_, index) =>
        dispatchOne(`comparison-concurrent-${String(repeat)}-${String(index)}`),
      ),
    );
    const calls = setup.provider.state.callCount;
    assertEqual(
      calls,
      COMPARISON_CONCURRENCY,
      'comparison concurrency model calls',
    );
    if (repeat >= COMPARISON_CONCURRENCY_WARMUP) {
      samples.push(performance.now() - t0);
      modelCalls += calls;
    }
  }
  return {
    side: 'flue@2.0.3',
    workload: 'concurrency',
    samples,
    modelCalls,
    callsPerSample: COMPARISON_CONCURRENCY,
    conversationsPerSample: COMPARISON_CONCURRENCY,
  };
};

export const runConformance = async (): Promise<ConformanceResult> => {
  const bytes = new Uint8Array([0, 1, 255]);
  const attachment = await createAttachmentRef({
    id: 'comparison-bytes',
    mimeType: 'image/png',
    bytes,
  });
  const attachments = new InMemoryAttachmentStore();
  await attachments.put({
    streamPath: 'comparison/file-bytes',
    conversationId: 'file-bytes',
    attachment,
    bytes,
  });
  const stored = await attachments.get({
    streamPath: 'comparison/file-bytes',
    conversationId: 'file-bytes',
    attachmentId: attachment.id,
  });
  if (stored?.bytes.join(',') !== '0,1,255') {
    throw new Error('Flue attachment bytes did not round-trip');
  }

  const setup = await startRuntime();
  await using _flue = setup.runtime;
  let markEntered!: () => void;
  const entered = new Promise<void>((resolve) => {
    markEntered = resolve;
  });
  setup.provider.blockUntilAbort(markEntered);
  const agent = init(BenchmarkAgent, { id: 'comparison-cancel' });
  const receipt = await agent.dispatch(USER_MESSAGE);
  const read = agent.read(receipt);
  await entered;
  await agent.abort();
  let aborted = false;
  try {
    await read;
  } catch (error) {
    aborted = error instanceof AgentRunError && error.outcome === 'aborted';
  }
  if (!aborted) {
    throw new Error('Flue in-flight abort did not settle as aborted');
  }
  assertEqual(
    setup.provider.state.callCount,
    1,
    'Flue in-flight cancellation model calls',
  );

  return {
    side: 'flue@2.0.3',
    checks: [
      {
        axis: 'producer fencing',
        status: 'not equivalent',
        evidence: 'no API-equivalent epoch/sequence producer probe is exposed',
      },
      {
        axis: 'indeterminate tool handling',
        status: 'not exercised',
        evidence:
          'deterministic process-crash recovery is outside this in-process harness',
      },
      {
        axis: 'revision compatibility',
        status: 'not equivalent',
        evidence:
          'agent registration exposes a durable name but no application revision parameter',
      },
      {
        axis: 'durable signal cancellation',
        status: 'not equivalent',
        evidence:
          'Flue exposes instance abort intent rather than Vesper signal-stream cancellation',
      },
      {
        axis: 'runtime abort cancellation',
        status: 'verified',
        evidence:
          'provider entered its stream before handle.abort; live read rejected with outcome aborted',
      },
      {
        axis: 'prompt file-byte recording',
        status: 'not equivalent',
        evidence:
          'attachment storage does not establish prompt-history byte reconstruction',
      },
      {
        axis: 'attachment byte storage',
        status: 'verified',
        evidence:
          'exported attachment store round-tripped 0x00 0x01 0xff byte-for-byte',
      },
    ],
  };
};
