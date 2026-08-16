import type {
  Api,
  Model as PiModelRecord,
  Provider,
} from '@earendil-works/pi-ai';
import { Config, Effect, Layer, Option, Redacted } from 'effect';

import { CredentialStore } from './credentials.js';
import { PiRegistry } from './registry.js';

// One provider, wired.
//
// Registering a provider and seeding its credential are two steps that are
// never useful apart, and doing them by hand is four layers deep with a
// nested `Layer.provide` — enough ceremony that the interesting part of an
// application gets lost in it. Nothing here is new capability; it is
// `PiRegistry.layer` and `CredentialStore` composed the one way that is
// almost always wanted.
//
// The key is a `Redacted<string>`, never a bare string. That is not
// decoration: a plain string ends up in a log line or an error message
// eventually, and `Redacted` makes reaching the value an explicit act.
// `layerConfig` goes further and never materialises it in application code
// at all.

export interface Options {
  /** A Pi provider, e.g. `anthropicProvider()`. */
  readonly provider: Provider;
  readonly apiKey: Redacted.Redacted<string>;
}

/**
 * Register one provider and seed its API key.
 *
 * Provides both services, so a caller merges this and is done:
 *
 * ```ts
 * const infrastructure = PiProvider.layerConfig({
 *   provider: anthropicProvider(),
 *   apiKey: Config.redacted('ANTHROPIC_API_KEY'),
 * });
 * ```
 *
 * Registration happens during layer acquisition, inherited from
 * `PiRegistry.layer` — no caller can obtain a registry whose providers are
 * still being added, which is the failure that shows up as "No API provider
 * registered" on the first request after a deploy.
 */
export const layer = (
  options: Options,
): Layer.Layer<PiRegistry.Service | CredentialStore.Service> => {
  const credentials = CredentialStore.layerMemory;

  const seeded = Layer.effectDiscard(
    Effect.gen(function* () {
      const store = yield* CredentialStore.Service;
      yield* store.modify(options.provider.id, () =>
        Effect.succeed(
          Option.some({
            type: 'api_key' as const,
            key: Redacted.value(options.apiKey),
          }),
        ),
      );
    }).pipe(
      // A store that cannot accept a credential while the layer is being
      // built is a defect, not something a caller recovers from — the
      // application simply cannot start. Dying here keeps
      // `CredentialStoreError` out of the layer's error channel, where every
      // consumer would otherwise have to handle a case that only arises at
      // wiring time.
      Effect.orDie,
    ),
  ).pipe(Layer.provide(credentials));

  const registry = PiRegistry.layer({
    register: (models) =>
      Effect.sync(() => {
        models.setProvider(options.provider);
      }),
  }).pipe(Layer.provide(credentials));

  return Layer.mergeAll(registry, credentials).pipe(Layer.provide(seeded));
};

/**
 * The same, with the key read from configuration.
 *
 * The pair — concrete `layer(options)` alongside `layerConfig(wrapped)` —
 * is the shape Effect libraries use, and the reason to prefer this one is
 * that the secret never becomes a value in application code: it is resolved
 * by the `ConfigProvider` and handed straight to Pi.
 */
export const layerConfig = (options: {
  readonly provider: Provider;
  readonly apiKey: Config.Config<Redacted.Redacted<string>>;
}): Layer.Layer<
  PiRegistry.Service | CredentialStore.Service,
  Config.ConfigError
> =>
  Layer.unwrap(
    Effect.map(options.apiKey, (apiKey) =>
      layer({ provider: options.provider, apiKey }),
    ),
  );

/**
 * A model to add to a provider's catalog.
 *
 * Pi's own `Model` record minus `provider`, which is not the caller's to
 * choose: {@link withModels} stamps it from the provider being extended.
 * Getting it wrong is otherwise a trap that survives registration and
 * lookup and only fires on the first request — `Models.stream` resolves the
 * owning provider by `model.provider`, so a mismatched id throws Pi's
 * "Unknown provider" from inside the stream rather than at wiring time.
 *
 * Every other field stays required. Nothing here knows a model's real price,
 * context window, or API shape, and defaulting them would put invented
 * numbers into cost reporting and into the overflow arithmetic the agent loop
 * compacts against.
 */
export type ModelSpec<TApi extends Api = Api> = Omit<
  PiModelRecord<TApi>,
  'provider'
>;

/**
 * The same provider, with extra models in its catalog.
 *
 * The catalog a pinned `pi-ai` ships is a snapshot, and models ship faster
 * than the pin moves — `pi-ai@0.80.2` stops at `gpt-5.5-pro` while later
 * models are already serving production traffic elsewhere. Without this, such
 * a model is simply unreachable
 * through `@sunfall/vesper-pi`: `PiRegistry.resolve` misses and `PiModel.hooks`
 * turns the miss into a defect, however well the provider's API would have
 * handled the request. Lagging is the normal case, not the edge case.
 *
 * A `Provider` is a plain interface whose `getModels()` *is* the catalog, so
 * extending one is a wrapper — no fork, no patched module, no mutation of
 * Pi's own objects. It is a plain function returning a `Provider`, which is
 * what lets it sit wherever a provider already goes:
 *
 * ```ts
 * const infrastructure = PiProvider.layerConfig({
 *   provider: PiProvider.withModels(openaiProvider(), [
 *     {
 *       id: 'gpt-5.6-luna',
 *       name: 'GPT-5.6 Luna',
 *       api: 'openai-responses',
 *       baseUrl: 'https://api.openai.com/v1',
 *       reasoning: true,
 *       input: ['text', 'image'],
 *       cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
 *       contextWindow: 1_050_000,
 *       maxTokens: 128_000,
 *     },
 *   ]),
 *   apiKey: Config.redacted('OPENAI_API_KEY'),
 * });
 * ```
 *
 * and equally inside `PiRegistry.layer({ register })` for wiring that does
 * not go through {@link layer} at all.
 *
 * An added model replaces a catalog entry with the same id rather than
 * queueing behind it. Pi's `getModel` is a `find`, so appending a correction
 * would leave the stale entry winning — and correcting a stale entry (a
 * context window that grew, a price that changed) is the same need as adding
 * a missing one.
 *
 * The base catalog is re-read on every call, so a dynamic provider's
 * `refreshModels()` still shows through the wrapper.
 */
export const withModels = <TApi extends Api>(
  provider: Provider<TApi>,
  models: ReadonlyArray<ModelSpec<TApi>>,
): Provider<TApi> => {
  const added = models.map(
    (model): PiModelRecord<TApi> => ({ ...model, provider: provider.id }),
  );
  const replaced = new Set(added.map((model) => model.id));

  return {
    ...provider,
    getModels: () => [
      ...provider.getModels().filter((model) => !replaced.has(model.id)),
      ...added,
    ],
  };
};

export * as PiProvider from './provider.js';
