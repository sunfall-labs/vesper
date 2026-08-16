import { Agent } from '@sunfall/vesper-agent/agent';
import { ContextWindow } from '@sunfall/vesper-agent/context-window';
import { PiCompaction } from '@sunfall/vesper-pi/compaction';
import { CredentialStore } from '@sunfall/vesper-pi/credentials';
import { PiRegistry } from '@sunfall/vesper-pi/registry';
import {
  fauxAssistantMessage,
  fauxProvider,
  type FauxProviderHandle,
} from '@earendil-works/pi-ai/providers/faux';
import {
  Config,
  ConfigProvider,
  Duration,
  Effect,
  Layer,
  Stream,
} from 'effect';
import { LanguageModel, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { AiRuntime } from '../src/runtime.js';

// End to end through the composition layer, against Pi's own faux provider.
//
// What this package has to justify is now smaller than it was: it assembles a
// Pi model, decides whether provider blips are retried, and gets out of the
// way. The checkpoint-scoping assertions that used to live here went with
// `@sunfall/vesper-durable` — a resumed conversation is `agent.resume`'s job, and
// `@sunfall/vesper-agent` proves that against records rather than against a
// checkpoint namespace.

const agent = Agent.make({
  name: 'test',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

const withProvider = <A, E>(
  program: (
    handle: FauxProviderHandle,
  ) => Effect.Effect<A, E, LanguageModel.LanguageModel>,
  options: Partial<AiRuntime.Options> = {},
) => {
  const handle = fauxProvider({
    provider: 'faux',
    models: [{ id: 'faux-1' }],
    tokensPerSecond: 0,
  });
  handle.setResponses(
    Array.from({ length: 8 }, (_, index) =>
      fauxAssistantMessage(`answer-${index}`),
    ),
  );

  const live = AiRuntime.model({
    provider: 'faux',
    model: 'faux-1',
    ...options,
  }).pipe(
    Layer.provide(
      PiRegistry.layer({
        register: (models) =>
          Effect.sync(() => models.setProvider(handle.provider)),
      }),
    ),
    Layer.provide(CredentialStore.layerMemory),
  );

  return Effect.runPromise(
    Effect.gen(function* () {
      const value = yield* program(handle);
      return { value, providerCalls: handle.state.callCount };
    }).pipe(Effect.provide(live), Effect.orDie) as Effect.Effect<{
      value: A;
      providerCalls: number;
    }>,
  );
};

describe('AiRuntime', () => {
  // The layer is the whole wiring: no run id, no namespace, nothing a caller
  // has to remember. `forRun` used to be required here and its absence used to
  // be a silent correctness bug.
  it('runs an agent with nothing but the layer', async () => {
    const { value, providerCalls } = await withProvider(() => agent.run('hi'));

    expect(value.text).toBe('answer-0');
    expect(providerCalls).toBe(1);
  });

  // Every run reaches the provider. Nothing caches a model call any more, and
  // that is the intended change: identical prompts from unrelated
  // conversations used to serve each other's answers.
  it('reaches the provider once per run', async () => {
    const { value, providerCalls } = await withProvider(() =>
      Effect.gen(function* () {
        const first = yield* agent.run('hi');
        const second = yield* agent.run('hi');
        return [first.text, second.text];
      }),
    );

    expect(providerCalls).toBe(2);
    expect(value).toEqual(['answer-0', 'answer-1']);
  });

  // Wiring the model has to be enough to get the provider-usage estimator
  // too. A `Context.Reference` cannot report that it was never overridden, so
  // a caller who assembled the stack the documented way and still got the
  // character-count fallback would have no signal anywhere that compaction was
  // firing on a guess.
  it('installs Pi context-window heuristics alongside the model', async () => {
    const { value } = await withProvider(() => ContextWindow.Service);

    expect(value).toBe(PiCompaction.heuristics);
    expect(value).not.toBe(ContextWindow.pure);
  });

  it('streams through the same wiring', async () => {
    const { value } = await withProvider(() =>
      agent.stream('hi').pipe(
        Stream.map((event) => event._tag),
        Stream.runCollect,
      ),
    );

    expect(value[0]).toBe('TurnStarted');
    expect(value[value.length - 1]).toBe('Completed');
  });

  it('runs with retrying turned off', async () => {
    const { value } = await withProvider(() => agent.run('hi'), {
      retry: false,
    });

    expect(value.text).toBe('answer-0');
  });

  it('accepts a retry policy', async () => {
    const { value } = await withProvider(() => agent.run('hi'), {
      retry: { maxAttempts: 5, baseDelay: Duration.millis(1) },
    });

    expect(value.text).toBe('answer-0');
  });

  // The config-backed variant. Production reads the provider; tests pass
  // values directly, which is the point of shipping both.
  it('reads its model from configuration', async () => {
    const handle = fauxProvider({
      provider: 'faux',
      models: [{ id: 'faux-1' }],
      tokensPerSecond: 0,
    });
    handle.setResponses([fauxAssistantMessage('from-config')]);

    const live = AiRuntime.modelConfig({
      provider: Config.string('AI_PROVIDER'),
      model: Config.string('AI_MODEL'),
    }).pipe(
      Layer.provide(
        PiRegistry.layer({
          register: (models) =>
            Effect.sync(() => models.setProvider(handle.provider)),
        }),
      ),
      Layer.provide(CredentialStore.layerMemory),
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            AI_PROVIDER: 'faux',
            AI_MODEL: 'faux-1',
          }),
        ),
      ),
    );

    const text = await Effect.runPromise(
      agent
        .run('hi')
        .pipe(
          Effect.provide(live),
          Effect.orDie,
        ) as Effect.Effect<Agent.Result>,
    );

    expect(text.text).toBe('from-config');
  });

  it('fails loudly when required configuration is absent', async () => {
    const live = AiRuntime.modelConfig({
      provider: Config.string('AI_PROVIDER'),
      model: Config.string('AI_MODEL'),
    }).pipe(
      Layer.provide(PiRegistry.layer()),
      Layer.provide(CredentialStore.layerMemory),
      Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
    );

    const outcome = await Effect.runPromise(
      agent
        .run('hi')
        .pipe(Effect.provide(live), Effect.result) as unknown as Effect.Effect<{
        readonly _tag: string;
      }>,
    );

    expect(outcome._tag).toBe('Failure');
  });
});
