import {
  fauxAssistantMessage,
  fauxProvider,
  type FauxProviderHandle,
} from '@earendil-works/pi-ai/providers/faux';
import { Cause, Effect, Exit, Layer, Redacted } from 'effect';
import { LanguageModel } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { CredentialStore } from '../src/credentials.js';
import { PiModel } from '../src/model.js';
import { PiProvider } from '../src/provider.js';
import { PiRegistry } from '../src/registry.js';

// Two failures found by running this package against a real OpenAI model:
// a catalog miss that reported nothing, and no supported way to reach a model
// the pinned `pi-ai` predates.
//
// Pi's faux provider stands in for OpenAI. What matters here is the shape —
// a provider whose `getModels()` is a fixed catalog that does not contain the
// id being asked for — and that is the same whether the catalog is Pi's real
// OpenAI list or three faux entries.

const faux = (
  ...models: ReadonlyArray<string>
): FauxProviderHandle['provider'] =>
  fauxProvider({
    provider: 'faux',
    api: 'faux',
    models: models.map((id) => ({ id })),
    tokensPerSecond: 0,
  }).provider;

const registryOf = (
  ...providers: ReadonlyArray<FauxProviderHandle['provider']>
): Layer.Layer<PiRegistry.Service> =>
  PiRegistry.layer({
    register: (models) =>
      Effect.sync(() => {
        for (const provider of providers) models.setProvider(provider);
      }),
  }).pipe(Layer.provide(CredentialStore.layerMemory));

const resolving = <A>(
  registry: Layer.Layer<PiRegistry.Service>,
  use: (
    service: PiRegistry.Interface,
  ) => Effect.Effect<A, PiRegistry.ModelNotFound>,
): Promise<Exit.Exit<A, PiRegistry.ModelNotFound>> =>
  Effect.runPromise(
    Effect.flatMap(PiRegistry.Service, use).pipe(
      Effect.provide(registry),
      Effect.exit,
    ),
  );

/**
 * A model this repo runs in production that `pi-ai@0.80.2` has never heard of.
 *
 * The numbers are a real production configuration's, not invented.
 * `api` is the faux api rather than `openai-responses` because the provider
 * standing in for OpenAI here is the faux one.
 */
const LUNA: PiProvider.ModelSpec = {
  id: 'gpt-5.6-luna',
  name: 'GPT-5.6 Luna',
  api: 'faux',
  baseUrl: 'http://localhost:0',
  reasoning: true,
  input: ['text'],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 1_050_000,
  maxTokens: 128_000,
};

describe('ModelNotFound', () => {
  // The reported defect: `@sunfall/vesper-pi/ModelNotFound:` and nothing else.
  it('names the model, the provider, and what the provider does offer', async () => {
    const exit = await resolving(
      registryOf(faux('faux-1', 'faux-2')),
      (registry) => registry.resolve('faux', 'gpt-5.6-luna'),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;

    expect((error as Error).message).toBe(
      'Model "gpt-5.6-luna" not found in provider "faux", which offers 2 models: faux-1, faux-2. ' +
        'Add it with PiProvider.withModels when the pinned pi-ai catalog predates the model.',
    );
  });

  // A different fix, so a different message: nothing to add the model to.
  it('says so when the provider itself was never registered', async () => {
    const exit = await resolving(registryOf(faux('faux-1')), (registry) =>
      registry.resolve('openai', 'gpt-5.6-luna'),
    );

    const error = Exit.isFailure(exit)
      ? (Cause.squash(exit.cause) as PiRegistry.ModelNotFound)
      : undefined;

    expect(error?.message).toBe(
      'Model "gpt-5.6-luna" not found: no provider "openai" is registered.',
    );
    expect(error?.available).toBeUndefined();
  });

  // A real OpenAI catalog is dozens of ids long. The count is stated in full
  // so a trimmed list is never mistaken for the whole one.
  it('caps the catalog listing but still reports its size', async () => {
    const ids = Array.from(
      { length: 12 },
      (_, index) => `faux-${String(index)}`,
    );

    const exit = await resolving(registryOf(faux(...ids)), (registry) =>
      registry.resolve('faux', 'absent'),
    );
    const message = Exit.isFailure(exit)
      ? (Cause.squash(exit.cause) as Error).message
      : '';

    expect(message).toContain('offers 12 models: faux-0,');
    expect(message).toContain('faux-7, and 4 more.');
    expect(message).not.toContain('faux-8');
  });

  it('reports the fields alongside the message, not only inside it', async () => {
    const exit = await resolving(registryOf(faux('faux-1')), (registry) =>
      registry.resolve('faux', 'gpt-5.6-luna'),
    );
    const error = Exit.isFailure(exit)
      ? (Cause.squash(exit.cause) as PiRegistry.ModelNotFound)
      : undefined;

    expect(error?.provider).toBe('faux');
    expect(error?.modelId).toBe('gpt-5.6-luna');
    expect(error?.available).toStrictEqual(['faux-1']);
  });

  // How the live run actually met this: `PiModel.hooks` calls `Effect.orDie`,
  // so the miss arrives as a defect, and a defect is read as rendered text.
  it('renders both names through the defect PiModel.hooks raises', async () => {
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        yield* LanguageModel.LanguageModel;
      }).pipe(
        Effect.provide(
          PiModel.model('faux', 'gpt-5.6-luna').pipe(
            Layer.provide(registryOf(faux('faux-1'))),
          ),
        ),
        Effect.exit,
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    const rendered = Exit.isFailure(exit) ? Cause.pretty(exit.cause) : '';

    expect(rendered).toContain('gpt-5.6-luna');
    expect(rendered).toContain('faux');
  });
});

describe('PiProvider.withModels', () => {
  it('makes a model absent from the catalog resolvable', async () => {
    const exit = await resolving(
      registryOf(PiProvider.withModels(faux('faux-1'), [LUNA])),
      (registry) => registry.resolve('faux', 'gpt-5.6-luna'),
    );

    expect(Exit.isSuccess(exit)).toBe(true);
    const model = Exit.isSuccess(exit) ? exit.value : undefined;

    expect(model?.id).toBe('gpt-5.6-luna');
    expect(model?.contextWindow).toBe(1_050_000);
    expect(model?.maxTokens).toBe(128_000);
  });

  // `Models.stream` finds the owning provider by `model.provider`, so an id
  // the caller had to repeat is an id the caller can get wrong — and wrong
  // there fails on the first request, not at wiring time.
  it('stamps the provider id rather than asking for it again', async () => {
    const exit = await resolving(
      registryOf(PiProvider.withModels(faux('faux-1'), [LUNA])),
      (registry) => registry.resolve('faux', 'gpt-5.6-luna'),
    );

    expect(Exit.isSuccess(exit) ? exit.value.provider : undefined).toBe('faux');
  });

  it('leaves the catalog it extends untouched', () => {
    const base = faux('faux-1');

    PiProvider.withModels(base, [LUNA]);

    expect(base.getModels().map((model) => model.id)).toStrictEqual(['faux-1']);
  });

  // Pi's `getModel` is a `find`, so an appended correction would lose to the
  // stale entry it was meant to correct.
  it('replaces a stale catalog entry of the same id', async () => {
    const provider = PiProvider.withModels(faux('faux-1', 'faux-2'), [
      { ...LUNA, id: 'faux-1', contextWindow: 999_000 },
    ]);

    const exit = await resolving(registryOf(provider), (registry) =>
      registry.resolve('faux', 'faux-1'),
    );

    expect(Exit.isSuccess(exit) ? exit.value.contextWindow : undefined).toBe(
      999_000,
    );
    expect(provider.getModels().map((model) => model.id)).toStrictEqual([
      'faux-2',
      'faux-1',
    ]);
  });

  // Resolving is half of it. This is the other half: the added record has to
  // survive dispatch and reach the provider's own `stream`.
  it('streams through the assembled LanguageModel', async () => {
    const handle = fauxProvider({
      provider: 'faux',
      api: 'faux',
      models: [{ id: 'faux-1' }],
      tokensPerSecond: 0,
    });
    handle.setResponses([fauxAssistantMessage('luna answered')]);

    const text = await Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* LanguageModel.generateText({ prompt: 'hi' });
        return response.text;
      }).pipe(
        Effect.provide(
          PiModel.model('faux', 'gpt-5.6-luna').pipe(
            Layer.provide(
              registryOf(PiProvider.withModels(handle.provider, [LUNA])),
            ),
          ),
        ),
      ) as Effect.Effect<string>,
    );

    expect(text).toBe('luna answered');
  });

  // The capability has to reach the wiring people actually write, which is
  // the convenience layer rather than `PiRegistry.layer` by hand.
  it('composes with PiProvider.layer', async () => {
    const exit = await Effect.runPromise(
      Effect.flatMap(PiRegistry.Service, (registry) =>
        registry.resolve('faux', 'gpt-5.6-luna'),
      ).pipe(
        Effect.provide(
          PiProvider.layer({
            provider: PiProvider.withModels(faux('faux-1'), [LUNA]),
            apiKey: Redacted.make('k'),
          }),
        ),
        Effect.exit,
      ),
    );

    expect(Exit.isSuccess(exit) ? exit.value.id : undefined).toBe(
      'gpt-5.6-luna',
    );
  });
});
