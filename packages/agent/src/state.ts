import { RecordBatch } from '@sunfall/vesper-log/record-batch';
import { Cache, Context, Effect, Layer, Ref, Schema, Semaphore } from 'effect';

import { Session, StateCleanup } from './internal/protocol.js';
import type { AgentLog } from './log.js';
import { ResumeProjection } from './resume-projection.js';

export interface Handle<A> {
  readonly get: Effect.Effect<A, Error>;
  readonly set: (value: A) => Effect.Effect<void, Error>;
  readonly update: (f: (value: A) => A) => Effect.Effect<A, Error>;
  readonly modify: <B>(
    f: (value: A) => readonly [B, A],
  ) => Effect.Effect<B, Error>;
}

export interface Definition<A> extends Context.Service<
  Definition<A>,
  Handle<A>
> {
  readonly id: string;
  readonly version: string;
  readonly schema: Schema.Schema<A>;
  readonly initial: () => A;
}

export interface Options<A> {
  readonly id: string;
  readonly version: string;
  readonly schema: Schema.Schema<A>;
  readonly initial: A | (() => A);
}

export class Error extends Schema.TaggedError<Error>()(
  '@sunfall/vesper-agent/AgentStateError',
  {
    reason: Schema.Literals([
      'invalid-definition',
      'no-session',
      'incompatible',
      'decode',
      'encode',
      'not-json-safe',
    ]),
    message: Schema.String,
    stateId: Schema.optional(Schema.String),
    stateVersion: Schema.optional(Schema.String),
    persistedId: Schema.optional(Schema.String),
    persistedVersion: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.String),
  },
) {}

export const error = Error;

export const make = <A>(options: Options<A>): Definition<A> => {
  if (options.id.trim() === '') {
    throw new Error({
      reason: 'invalid-definition',
      message: 'State id must be non-empty',
    });
  }
  if (options.version.trim() === '')
    throw new Error({
      reason: 'invalid-definition',
      message: 'State version must be non-empty',
      stateId: options.id,
    });
  const tag = Context.Service<Definition<A>, Handle<A>>(
    `@sunfall/vesper-agent/state/${options.id}`,
  );
  const initial: () => A =
    typeof options.initial === 'function'
      ? (options.initial as () => A)
      : () => options.initial as A;
  return Object.assign(tag, {
    id: options.id,
    version: options.version,
    schema: options.schema,
    initial,
  });
};

export const dependencies = <
  A,
  const Rest extends ReadonlyArray<Context.Key<any, any>>,
>(
  state: Definition<A>,
  ...rest: Rest
): [Definition<A>, ...Rest] => [state, ...rest];

const decode = <A>(schema: Schema.Schema<A>, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value) as Effect.Effect<
    A,
    Schema.SchemaError
  >;

const encode = <A>(schema: Schema.Schema<A>, value: A) =>
  Schema.encodeEffect(schema)(value) as Effect.Effect<
    unknown,
    Schema.SchemaError
  >;

export const open = <A>(
  definition: Definition<A>,
  session: AgentLog.Session | undefined,
): Effect.Effect<Handle<A>, Error> =>
  Effect.gen(function* () {
    const persisted =
      session === undefined
        ? undefined
        : ResumeProjection.stateFrom(session.stateHistory);
    if (
      persisted !== undefined &&
      (persisted.id !== definition.id ||
        persisted.version !== definition.version)
    ) {
      return yield* Effect.fail(
        new Error({
          reason: 'incompatible',
          message: 'Persisted state schema does not match the agent definition',
          stateId: definition.id,
          stateVersion: definition.version,
          persistedId: persisted.id,
          persistedVersion: persisted.version,
        }),
      );
    }
    const initial =
      persisted === undefined
        ? definition.initial()
        : yield* decode(definition.schema, persisted.value).pipe(
            Effect.mapError(
              (cause) =>
                new Error({
                  reason: 'decode',
                  message: 'State checkpoint does not decode',
                  stateId: definition.id,
                  stateVersion: definition.version,
                  cause: String(cause),
                }),
            ),
          );
    const current = yield* Ref.make(initial);
    const lock = yield* Semaphore.make(1);

    const persist = (next: A) =>
      Effect.gen(function* () {
        if (session !== undefined) {
          const encoded = yield* encode(definition.schema, next).pipe(
            Effect.mapError(
              (cause) =>
                new Error({
                  reason: 'encode',
                  message: 'State checkpoint does not encode',
                  stateId: definition.id,
                  stateVersion: definition.version,
                  cause: String(cause),
                }),
            ),
          );
          const value = yield* RecordBatch.prepareUnknown(encoded).pipe(
            Effect.mapError(
              (cause) =>
                new Error({
                  reason: 'not-json-safe',
                  message: 'State checkpoint is not JSON-safe',
                  stateId: definition.id,
                  stateVersion: definition.version,
                  cause: cause.detail,
                }),
            ),
          );
          yield* session.append([
            {
              _tag: 'StateCheckpoint',
              id: definition.id,
              version: definition.version,
              value,
            },
          ]);
        }
        yield* Ref.set(current, next);
      });

    const modify: Handle<A>['modify'] = (f) =>
      lock.withPermits(1)(
        Effect.gen(function* () {
          const [result, next] = f(yield* Ref.get(current));
          yield* persist(next);
          return result;
        }),
      );

    return {
      get: Ref.get(current),
      set: (value) => lock.withPermits(1)(persist(value)),
      update: (f) =>
        modify((value) => {
          const next = f(value);
          return [next, next] as const;
        }),
      modify,
    };
  });

export const layerEphemeral = <A>(definition: Definition<A>) =>
  Layer.effect(definition, open(definition, undefined));

export const layerRecorded = <A>(definition: Definition<A>) =>
  Layer.effect(
    definition,
    Effect.gen(function* () {
      const cache = yield* Cache.make({
        capacity: Number.MAX_SAFE_INTEGER,
        lookup: (session: AgentLog.Session) => open(definition, session),
      });
      const release = (session: AgentLog.Session) =>
        Cache.invalidate(cache, session);
      const resolve = Effect.gen(function* () {
        const session = yield* Session;
        if (session === undefined) {
          return yield* Effect.fail(
            new Error({
              reason: 'no-session',
              message: 'Recorded state requires a recorded agent session',
              stateId: definition.id,
              stateVersion: definition.version,
            }),
          );
        }
        const cleanup = yield* StateCleanup;
        cleanup?.add(release);
        return yield* Cache.get(cache, session);
      });
      const modify: Handle<A>['modify'] = (f) =>
        Effect.flatMap(resolve, (handle) => handle.modify(f));
      return {
        get: Effect.flatMap(resolve, (handle) => handle.get),
        set: (value) => Effect.flatMap(resolve, (handle) => handle.set(value)),
        update: (f) => Effect.flatMap(resolve, (handle) => handle.update(f)),
        modify,
      };
    }),
  );

export * as AgentState from './state.js';
