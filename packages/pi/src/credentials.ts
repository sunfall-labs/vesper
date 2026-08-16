import type {
  Credential,
  CredentialStore as PiCredentialStore,
} from '@earendil-works/pi-ai';
import {
  Context,
  Effect,
  Layer,
  MutableHashMap,
  Option,
  PartitionedSemaphore,
  Schema,
} from 'effect';

// Pi's `CredentialStore` exists to make one guarantee: `modify` is a
// serialized read-modify-write per provider, so two concurrent requests
// cannot both refresh a rotating OAuth token and race each other into an
// invalid state.
//
// That guarantee is a `PartitionedSemaphore` keyed by provider id — mutual
// exclusion per key, concurrency across keys — so refreshing an Anthropic
// token never blocks an OpenAI request. Modelling the store as a service
// means it is chosen at runtime wiring (in-memory for tests, database-backed
// in production) rather than threaded through every call site.

export class CredentialStoreError extends Schema.TaggedErrorClass<CredentialStoreError>()(
  '@sunfall/vesper-pi/CredentialStoreError',
  {
    providerId: Schema.String,
    operation: Schema.Literals(['read', 'modify', 'delete']),
  },
) {}

export interface Interface {
  readonly read: (
    providerId: string,
  ) => Effect.Effect<Option.Option<Credential>, CredentialStoreError>;

  /**
   * Serialized read-modify-write. `update` receives the current credential
   * and returns the replacement, or `Option.none()` to leave it untouched.
   */
  readonly modify: (
    providerId: string,
    update: (
      current: Option.Option<Credential>,
    ) => Effect.Effect<Option.Option<Credential>, CredentialStoreError>,
  ) => Effect.Effect<Option.Option<Credential>, CredentialStoreError>;

  readonly delete: (
    providerId: string,
  ) => Effect.Effect<void, CredentialStoreError>;
}

export class Service extends Context.Service<Service, Interface>()(
  '@sunfall/vesper-pi/CredentialStore',
) {}

/**
 * Adapt the Effect service to the Promise-shaped interface Pi consumes.
 *
 * Pi wraps a rejection here into its own `ModelsError` with code "auth", so
 * failures stay distinguishable from "provider not configured".
 */
export const toPiCredentialStore: Effect.Effect<
  PiCredentialStore,
  never,
  Service
> = Effect.gen(function* () {
  const store = yield* Service;
  const context = yield* Effect.context<never>();
  const run = Effect.runPromiseWith(context);

  return {
    read: (providerId) =>
      run(store.read(providerId).pipe(Effect.map(Option.getOrUndefined))),

    modify: (providerId, fn) =>
      run(
        store
          .modify(providerId, (current) =>
            Effect.tryPromise({
              try: () => fn(Option.getOrUndefined(current)),
              catch: () =>
                new CredentialStoreError({ providerId, operation: 'modify' }),
            }).pipe(Effect.map(Option.fromNullishOr)),
          )
          .pipe(Effect.map(Option.getOrUndefined)),
      ),

    delete: (providerId) => run(store.delete(providerId)),
  };
});

/**
 * In-memory store. Correct for tests and single-process development; a
 * restart loses every credential, so production wiring should provide a
 * persistent implementation of the same interface.
 */
export const layerMemory: Layer.Layer<Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const credentials = MutableHashMap.empty<string, Credential>();
    const lock = yield* PartitionedSemaphore.make<string>({ permits: 1 });

    // `Effect.fn` with a plain function rather than a generator: the lookup is
    // synchronous and has nothing to yield, and an empty generator trips
    // `require-yield`.
    const read = Effect.fn('AiPi.CredentialStore.read')((providerId: string) =>
      Effect.sync(() => MutableHashMap.get(credentials, providerId)),
    );

    const modify = Effect.fn('AiPi.CredentialStore.modify')(function* (
      providerId: string,
      update: (
        current: Option.Option<Credential>,
      ) => Effect.Effect<Option.Option<Credential>, CredentialStoreError>,
    ) {
      return yield* lock.withPermit(providerId)(
        Effect.gen(function* () {
          const current = MutableHashMap.get(credentials, providerId);
          const next = yield* update(current);
          if (Option.isSome(next)) {
            MutableHashMap.set(credentials, providerId, next.value);
            return next;
          }
          return current;
        }),
      );
    });

    const remove = Effect.fn('AiPi.CredentialStore.delete')(function* (
      providerId: string,
    ) {
      yield* lock.withPermit(providerId)(
        Effect.sync(() => {
          MutableHashMap.remove(credentials, providerId);
        }),
      );
    });

    return Service.of({ read, modify, delete: remove });
  }),
);

export * as CredentialStore from './credentials.js';
