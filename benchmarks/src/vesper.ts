// `@sunfall/vesper-agent` driven through `@sunfall/vesper-runtime`'s
// composition layer, against pi-ai's faux provider with `tokensPerSecond: 0`.
//
// Everything is the documented default configuration. In particular the stop
// condition is left unset (so `Stop.defaultCondition` applies), compaction is
// left on, and the retry wrapper `AiRuntime.model` installs by default stays
// installed. Nothing here is tuned for the benchmark. A number produced by a
// configuration nobody ships is not a number about the library.

import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from '@earendil-works/pi-ai/providers/faux';
import { Agent } from '@sunfall/vesper-agent/agent';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { CredentialStore } from '@sunfall/vesper-pi/credentials';
import { PiRegistry } from '@sunfall/vesper-pi/registry';
import { AiRuntime } from '@sunfall/vesper-runtime/runtime';
import { Effect, Layer, ManagedRuntime, Stream } from 'effect';
import { Schema } from 'effect';
import { AgentEvents } from '@sunfall/vesper-agent/event';
import { Tool, Toolkit } from 'effect/unstable/ai';

import {
  CONVERSATION_MESSAGES,
  CONVERSATION_STEPS,
  INSTRUCTIONS,
  ITERATIONS,
  GROWTH_MESSAGES,
  GROWTH_REPEATS,
  MEMORY_MESSAGES,
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
} from './workload.js';

// ------------------------------------------------------------------- agent

const lookupOrder = Tool.make(TOOL_NAME, {
  description: TOOL_DESCRIPTION,
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const benchAgent = Agent.make({
  name: 'bench',
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
 * Left at the faux default, longer scenarios would cross the threshold partway
 * through and the benchmark would be measuring summarisation, not the loop.
 */
export const FAUX_CONTEXT_WINDOW = 100_000_000;

const makeProvider = () =>
  fauxProvider({
    provider: 'faux',
    models: [
      {
        id: 'faux-1',
        contextWindow: FAUX_CONTEXT_WINDOW,
        maxTokens: 64_000,
      },
    ],
    tokensPerSecond: 0,
  });

type Handle = ReturnType<typeof makeProvider>;

const loadScript = (handle: Handle, steps: number): void => {
  handle.setResponses(
    script(steps).map((step) =>
      step.kind === 'tool'
        ? fauxAssistantMessage(
            [fauxToolCall(TOOL_NAME, { orderId: step.orderId })],
            { stopReason: 'toolUse' },
          )
        : fauxAssistantMessage('All of your orders have shipped.'),
    ),
  );
};

const layerFor = (handle: Handle) =>
  Layer.mergeAll(
    AiRuntime.model({ provider: 'faux', model: 'faux-1' }).pipe(
      Layer.provide(
        PiRegistry.layer({
          register: (models) =>
            Effect.sync(() => models.setProvider(handle.provider)),
        }),
      ),
      Layer.provide(CredentialStore.layerMemory),
    ),
    LogStoreMemory.layer,
  );

// ---------------------------------------------------------------- scenarios

/**
 * One submission of `steps` model turns.
 *
 * `recording` picks `agent.recordingTo(id).run(...)` over `agent.run(...)` —
 * the only difference between the two rows in the report.
 */
const runTurn = async (recording: boolean): Promise<ScenarioResult> => {
  const handle = makeProvider();
  const runtime = ManagedRuntime.make(layerFor(handle));
  const samples: number[] = [];
  let conversation = 0;

  const once = async (): Promise<void> => {
    loadScript(handle, TURN_STEPS);
    const agent = recording
      ? benchAgent.recordingTo(`bench-${conversation++}`)
      : benchAgent;
    await runtime.runPromise(
      Effect.orDie(agent.run(USER_MESSAGE)) as Effect.Effect<unknown>,
    );
  };

  for (let i = 0; i < WARMUP; i++) await once();

  const before = handle.state.callCount;
  for (let i = 0; i < ITERATIONS; i++) {
    const t0 = performance.now();
    await once();
    samples.push(performance.now() - t0);
  }
  const modelCalls = handle.state.callCount - before;

  await runtime.dispose();

  return {
    side: recording ? 'vesper+log' : 'vesper',
    scenario: 'turn',
    samples,
    unit: `${TURN_STEPS} model turns, ${TURN_STEPS - 1} tool calls`,
    turnsPerSample: TURN_STEPS,
    modelCalls,
  };
};

/**
 * A growing conversation: `CONVERSATION_MESSAGES` messages to one
 * conversation, each running `CONVERSATION_STEPS` model turns.
 *
 * Recording is not optional here. Continuing a conversation means having one,
 * and `agent.resume` rebuilds the prompt from the log. One sample is the whole
 * conversation rather than a single message, because the cost being measured
 * is how the loop behaves as history accumulates.
 */
const runConversation = async (): Promise<ScenarioResult> => {
  const handle = makeProvider();
  const runtime = ManagedRuntime.make(layerFor(handle));
  const samples: number[] = [];
  let conversation = 0;

  const once = async (messages: number): Promise<void> => {
    const id = `conv-${conversation++}`;
    for (let m = 0; m < messages; m++) {
      loadScript(handle, CONVERSATION_STEPS);
      await runtime.runPromise(
        Effect.orDie(
          benchAgent.resume(id, `${USER_MESSAGE} (${m})`),
        ) as Effect.Effect<unknown>,
      );
    }
  };

  for (let i = 0; i < WARMUP; i++) await once(CONVERSATION_MESSAGES);

  const before = handle.state.callCount;
  for (let i = 0; i < ITERATIONS; i++) {
    const t0 = performance.now();
    await once(CONVERSATION_MESSAGES);
    samples.push(performance.now() - t0);
  }
  const modelCalls = handle.state.callCount - before;

  await runtime.dispose();

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

  await runtime.dispose();

  return {
    side: 'vesper',
    scenario: 'startup',
    samples: [constructMs + firstPartMs],
    unit: 'cold construct + first streamed content part',
    constructMs,
    firstPartMs,
    modelCalls: handle.state.callCount,
  };
};

/** Heap retained by one long conversation, after a forced GC. */
const runMemory = async (): Promise<ScenarioResult> => {
  const handle = makeProvider();
  const runtime = ManagedRuntime.make(layerFor(handle));
  const id = 'memory-conversation';

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
    await runtime.runPromise(
      Effect.orDie(
        benchAgent.resume(id, `${USER_MESSAGE} (${m})`),
      ) as Effect.Effect<unknown>,
    );
  }

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
    modelCalls: handle.state.callCount,
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
  const handle = makeProvider();
  const runtime = ManagedRuntime.make(layerFor(handle));
  const series: Array<{ turns: number; samples: number[] }> = [];
  let conversation = 0;

  for (const turns of SCALING_TURNS) {
    const samples: number[] = [];
    for (let i = 0; i < SCALING_ITERATIONS + WARMUP; i++) {
      loadScript(handle, turns);
      const agent = recording
        ? benchAgent.recordingTo(`scale-${conversation++}`)
        : benchAgent;
      const t0 = performance.now();
      await runtime.runPromise(
        Effect.orDie(agent.run(USER_MESSAGE)) as Effect.Effect<unknown>,
      );
      const elapsed = performance.now() - t0;
      if (i >= WARMUP) samples.push(elapsed);
    }
    series.push({ turns, samples });
  }

  await runtime.dispose();

  return {
    side: recording ? 'vesper+log' : 'vesper',
    scenario: 'scaling',
    samples: [],
    unit: 'one submission of K turns, fresh conversation each time',
    series,
    modelCalls: handle.state.callCount,
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
  const handle = makeProvider();
  const runtime = ManagedRuntime.make(layerFor(handle));
  const perIndex: number[][] = Array.from(
    { length: GROWTH_MESSAGES },
    () => [],
  );

  for (let repeat = 0; repeat < GROWTH_REPEATS + 1; repeat++) {
    const id = `growth-${repeat}`;
    for (let m = 0; m < GROWTH_MESSAGES; m++) {
      loadScript(handle, CONVERSATION_STEPS);
      const t0 = performance.now();
      await runtime.runPromise(
        Effect.orDie(
          benchAgent.resume(id, `${USER_MESSAGE} (${m})`),
        ) as Effect.Effect<unknown>,
      );
      const elapsed = performance.now() - t0;
      // The first whole conversation is warmup and is thrown away.
      if (repeat > 0) perIndex[m]!.push(elapsed);
    }
  }

  await runtime.dispose();

  return {
    side: 'vesper+log',
    scenario: 'growth',
    samples: [],
    unit: `message N of one conversation, ${CONVERSATION_STEPS} turns per message`,
    growth: perIndex.map((samples, index) => ({ index: index + 1, samples })),
    modelCalls: handle.state.callCount,
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
    case 'conversation':
      return runConversation();
    case 'startup':
      return runStartup();
    case 'memory':
      return runMemory();
  }
};
