import {
  createModels,
  type Api,
  type Model as PiModelRecord,
  type Models,
  type MutableModels,
} from '@earendil-works/pi-ai';
import { Context, Effect, Layer, Option, Schema } from 'effect';

import { CredentialStore } from './credentials.js';

// Pi's `createModels()` builds a mutable registry of providers, then resolves
// auth and dispatches each request to the provider owning the model. Holding
// it behind a Layer rather than a module-scope singleton is the point of this
// file.
//
// Registering providers lazily — on first use — is a live production failure
// mode: the first request after a deploy races registration and fails with
// "No API provider registered for api: …". A Layer makes that
// unrepresentable, because no caller can obtain a registry that has not
// finished being built.

/**
 * The registry has no such model.
 *
 * Carries a rendered `message` for the same reason `AiError` does: this one
 * is reported through `Effect.orDie` (see `PiModel.hooks`), and a defect is
 * read as text by whoever finds it in a log. Tag-and-fields alone printed as
 * a bare `@sunfall/vesper-pi/ModelNotFound:` — true, and useless.
 *
 * The two ways to miss have different fixes, so they are different values of
 * {@link available} rather than one undifferentiated "not found": an
 * unregistered provider means the wiring never registered it, while a
 * registered provider that does not list the id means the pinned `pi-ai`
 * catalog predates the model — repaired with `PiProvider.withModels`, not by
 * changing the wiring.
 */
export class ModelNotFound extends Schema.TaggedErrorClass<ModelNotFound>()(
  '@sunfall/vesper-pi/ModelNotFound',
  {
    provider: Schema.String,
    modelId: Schema.String,
    /**
     * The model ids the provider offers, or absent when no provider under
     * that id is registered at all.
     */
    available: Schema.optionalKey(Schema.Array(Schema.String)),
  },
) {
  override get message(): string {
    if (this.available === undefined) {
      return `Model "${this.modelId}" not found: no provider "${this.provider}" is registered.`;
    }
    return (
      `Model "${this.modelId}" not found in provider "${this.provider}", which offers ${listing(this.available)}. ` +
      'Add it with PiProvider.withModels when the pinned pi-ai catalog predates the model.'
    );
  }
}

/**
 * A catalog rendered for a human, not dumped.
 *
 * Capped because an OpenAI catalog is dozens of ids long and a defect message
 * that scrolls is one nobody reads to the end of. The count is stated in full
 * either way, so "which offers 0 models" stays distinguishable from a list
 * that was merely trimmed.
 */
const LISTED_MODELS = 8;

const listing = (ids: ReadonlyArray<string>): string => {
  if (ids.length === 0) return 'no models';
  const shown = ids.slice(0, LISTED_MODELS).join(', ');
  return ids.length <= LISTED_MODELS
    ? `${String(ids.length)} models: ${shown}`
    : `${String(ids.length)} models: ${shown}, and ${String(ids.length - LISTED_MODELS)} more`;
};

export interface Interface {
  /** Resolve a model record, failing when the provider does not offer it. */
  readonly resolve: (
    provider: string,
    modelId: string,
  ) => Effect.Effect<PiModelRecord<Api>, ModelNotFound>;

  /**
   * The underlying Pi registry. Exposed because `Models.stream` is the
   * dispatch entry point `model.ts` needs; callers outside this package
   * should use `resolve` and the `LanguageModel` service instead.
   */
  readonly models: Models;
}

export class Service extends Context.Service<Service, Interface>()(
  '@sunfall/vesper-pi/Registry',
) {}

export interface LayerOptions {
  /**
   * Register providers into the freshly created registry. Runs once, during
   * layer acquisition, so every provider is present before the first
   * request can be issued.
   */
  readonly register?: (models: MutableModels) => Effect.Effect<void>;
}

export const layer = (
  options: LayerOptions = {},
): Layer.Layer<Service, never, CredentialStore.Service> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const credentials = yield* CredentialStore.toPiCredentialStore;
      const models = createModels({ credentials });

      if (options.register !== undefined) {
        yield* options.register(models);
      }

      const resolve = Effect.fn('AiPi.Registry.resolve')(function* (
        provider: string,
        modelId: string,
      ) {
        const found = Option.fromNullishOr(models.getModel(provider, modelId));
        if (Option.isNone(found)) {
          // `models.getModels(id)` rather than `provider.getModels()`: Pi
          // swallows a throwing provider there, and building the failure is
          // the worst place to acquire a new way to fail.
          return yield* Effect.fail(
            models.getProvider(provider) === undefined
              ? new ModelNotFound({ provider, modelId })
              : new ModelNotFound({
                  provider,
                  modelId,
                  available: models
                    .getModels(provider)
                    .map((model) => model.id),
                }),
          );
        }
        return found.value;
      });

      return Service.of({ resolve, models });
    }),
  );

export * as PiRegistry from './registry.js';
