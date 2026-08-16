import { fauxProvider } from '@earendil-works/pi-ai/providers/faux';
import {
  Config,
  ConfigProvider,
  Effect,
  Layer,
  Option,
  Redacted,
} from 'effect';
import { describe, expect, it } from 'vitest';

import { CredentialStore } from '../src/credentials.js';
import { PiProvider } from '../src/provider.js';
import { PiRegistry } from '../src/registry.js';

// The convenience layer exists to remove ceremony, so what is worth testing
// is that it removes it *correctly*: both services present, the credential
// actually seeded under the provider's own id, and the key never required as
// a bare string.

const handle = () =>
  fauxProvider({
    provider: 'faux',
    models: [{ id: 'faux-1' }],
    tokensPerSecond: 0,
  });

describe('PiProvider.layer', () => {
  it('provides the registry with the provider already registered', async () => {
    const provider = handle().provider;

    const found = await Effect.runPromise(
      Effect.gen(function* () {
        const registry = yield* PiRegistry.Service;
        return registry.models.getProvider('faux')?.id;
      }).pipe(
        Effect.provide(
          PiProvider.layer({ provider, apiKey: Redacted.make('k') }),
        ),
      ),
    );

    expect(found).toBe('faux');
  });

  // The half most easily got wrong by hand: registering the provider but
  // forgetting to seed its key, which fails only on the first real request.
  it('seeds the credential under the provider id', async () => {
    const provider = handle().provider;

    const credential = await Effect.runPromise(
      Effect.gen(function* () {
        const store = yield* CredentialStore.Service;
        return yield* store.read('faux');
      }).pipe(
        Effect.provide(
          PiProvider.layer({ provider, apiKey: Redacted.make('secret-key') }),
        ),
        Effect.orDie,
      ),
    );

    expect(Option.isSome(credential)).toBe(true);
    expect(Option.getOrThrow(credential)).toStrictEqual({
      type: 'api_key',
      key: 'secret-key',
    });
  });

  it('reads the key from configuration without it becoming a value', async () => {
    const provider = handle().provider;

    const credential = await Effect.runPromise(
      Effect.gen(function* () {
        const store = yield* CredentialStore.Service;
        return yield* store.read('faux');
      }).pipe(
        Effect.provide(
          PiProvider.layerConfig({
            provider,
            apiKey: Config.redacted('FAUX_API_KEY'),
          }).pipe(
            Layer.provide(
              ConfigProvider.layer(
                ConfigProvider.fromUnknown({ FAUX_API_KEY: 'from-config' }),
              ),
            ),
          ),
        ),
        Effect.orDie,
      ),
    );

    expect(Option.getOrThrow(credential)).toStrictEqual({
      type: 'api_key',
      key: 'from-config',
    });
  });

  it('fails when the configured key is absent', async () => {
    const provider = handle().provider;

    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        yield* PiRegistry.Service;
      }).pipe(
        Effect.provide(
          PiProvider.layerConfig({
            provider,
            apiKey: Config.redacted('FAUX_API_KEY'),
          }).pipe(
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
          ),
        ),
        Effect.result,
      ),
    );

    expect(outcome._tag).toBe('Failure');
  });
});
