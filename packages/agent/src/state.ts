import { RecordBatch } from '@sunfall/vesper-log/record-batch';
import { Context, Effect, Layer, Schema, SynchronizedRef } from 'effect';

import type { AgentLog } from './log.js';
import { ResumeProjection } from './resume-projection.js';

export interface Handle<A, EncodeR = never> {
  readonly get: Effect.Effect<A, Error>;
  readonly set: (value: A) => Effect.Effect<void, Error, EncodeR>;
  readonly update: (f: (value: A) => A) => Effect.Effect<A, Error, EncodeR>;
  readonly modify: <B>(
    f: (value: A) => readonly [B, A],
  ) => Effect.Effect<B, Error, EncodeR>;
}

export interface Definition<
  A,
  Encoded = A,
  DecodeR = never,
  EncodeR = never,
> extends Context.Service<
  Definition<A, Encoded, DecodeR, EncodeR>,
  Handle<A, EncodeR>
> {
  readonly id: string;
  readonly version: string;
  readonly schema: Schema.Codec<A, Encoded, DecodeR, EncodeR>;
  readonly initial: () => A;
}

export interface Options<A, Encoded = A, DecodeR = never, EncodeR = never> {
  readonly id: string;
  readonly version: string;
  readonly schema: Schema.Codec<A, Encoded, DecodeR, EncodeR>;
  readonly initial: A | (() => A);
}

/** Any state definition, used when an agent's state is not known statically. */
// The key is covariant, while `Definition` itself is invariant in its
// identifier and handle shape. Constraining against the key keeps generic
// agents assignable without erasing their concrete codec below.
// oxlint-disable-next-line no-explicit-any
export type AnyDefinition = Context.Key<any, any> & {
  readonly id: string;
  readonly version: string;
  // oxlint-disable-next-line no-explicit-any
  readonly schema: Schema.Codec<any, any, any, any>;
  // oxlint-disable-next-line no-explicit-any
  readonly initial: () => any;
};

/** Services needed to decode and encode one state definition's checkpoints. */
export type DecodingServices<D extends AnyDefinition> =
  Schema.Codec.DecodingServices<D['schema']>;
export type EncodingServices<D extends AnyDefinition> =
  Schema.Codec.EncodingServices<D['schema']>;
export type Services<D extends AnyDefinition> =
  | DecodingServices<D>
  | EncodingServices<D>;

// `id` is the durable identity used in checkpoints, not a Context identity.
// Two definitions may intentionally share an id while carrying different
// schemas or lifetimes, so each definition gets a process-local key as well.
let nextContextKey = 0;

export class StateDefinitionError extends Schema.TaggedError<StateDefinitionError>(
  '@sunfall/vesper-agent/StateDefinitionError',
)('StateDefinitionError', {
  message: Schema.String,
  stateId: Schema.optionalKey(Schema.String),
}) {}

export class StateCompatibilityError extends Schema.TaggedError<StateCompatibilityError>(
  '@sunfall/vesper-agent/StateCompatibilityError',
)('StateCompatibilityError', {
  message: Schema.String,
  stateId: Schema.String,
  stateVersion: Schema.String,
  persistedId: Schema.String,
  persistedVersion: Schema.String,
}) {}

export class StateDecodeError extends Schema.TaggedError<StateDecodeError>(
  '@sunfall/vesper-agent/StateDecodeError',
)('StateDecodeError', {
  message: Schema.String,
  stateId: Schema.String,
  stateVersion: Schema.String,
  cause: Schema.String,
}) {}

export class StateEncodeError extends Schema.TaggedError<StateEncodeError>(
  '@sunfall/vesper-agent/StateEncodeError',
)('StateEncodeError', {
  message: Schema.String,
  stateId: Schema.String,
  stateVersion: Schema.String,
  cause: Schema.String,
}) {}

export class StateJsonError extends Schema.TaggedError<StateJsonError>(
  '@sunfall/vesper-agent/StateJsonError',
)('StateJsonError', {
  message: Schema.String,
  stateId: Schema.String,
  stateVersion: Schema.String,
  cause: Schema.String,
}) {}

/** Every recoverable state failure, usable directly as a tool failure schema. */
export const Error = Schema.Union([
  StateDefinitionError,
  StateCompatibilityError,
  StateDecodeError,
  StateEncodeError,
  StateJsonError,
]);
export type Error = typeof Error.Type;

export const error = Error;

export const make = <A, Encoded = A, DecodeR = never, EncodeR = never>(
  options: Options<A, Encoded, DecodeR, EncodeR>,
): Definition<A, Encoded, DecodeR, EncodeR> => {
  if (options.id.trim() === '') {
    throw new StateDefinitionError({
      message: 'State id must be non-empty',
    });
  }
  if (options.version.trim() === '')
    throw new StateDefinitionError({
      message: 'State version must be non-empty',
      stateId: options.id,
    });
  const tag = Context.Service<
    Definition<A, Encoded, DecodeR, EncodeR>,
    Handle<A, EncodeR>
  >(`@sunfall/vesper-agent/state/${options.id}/${nextContextKey++}`);
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
  D extends AnyDefinition,
  const Rest extends ReadonlyArray<Context.Key<any, any>>,
>(
  state: D,
  ...rest: Rest
): [D, ...Rest] => [state, ...rest];

const decode = <A, Encoded, DecodeR, EncodeR>(
  schema: Schema.Codec<A, Encoded, DecodeR, EncodeR>,
  value: unknown,
): Effect.Effect<A, Schema.SchemaError, DecodeR> =>
  Schema.decodeUnknownEffect(schema)(value);

const encode = <A, Encoded, DecodeR, EncodeR>(
  schema: Schema.Codec<A, Encoded, DecodeR, EncodeR>,
  value: A,
): Effect.Effect<Encoded, Schema.SchemaError, EncodeR> =>
  Schema.encodeEffect(schema)(value);

export const open = Effect.fn('AgentState.open')(function* <
  A,
  Encoded,
  DecodeR,
  EncodeR,
>(
  definition: {
    readonly id: string;
    readonly version: string;
    readonly schema: Schema.Codec<A, Encoded, DecodeR, EncodeR>;
    readonly initial: () => A;
  },
  session: AgentLog.Session | undefined,
) {
  yield* Effect.annotateCurrentSpan({
    'vesper.state.id': definition.id,
    'vesper.state.version': definition.version,
    'vesper.state.recorded': session !== undefined,
  });

  const persisted =
    session === undefined
      ? undefined
      : ResumeProjection.stateFrom(session.stateHistory);
  if (
    persisted !== undefined &&
    (persisted.id !== definition.id || persisted.version !== definition.version)
  ) {
    return yield* Effect.fail(
      new StateCompatibilityError({
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
              new StateDecodeError({
                message: 'State checkpoint does not decode',
                stateId: definition.id,
                stateVersion: definition.version,
                cause: String(cause),
              }),
          ),
        );
  const current = yield* SynchronizedRef.make(initial);

  const persist = (next: A) =>
    Effect.gen(function* () {
      if (session !== undefined) {
        const encoded = yield* encode(definition.schema, next).pipe(
          Effect.mapError(
            (cause) =>
              new StateEncodeError({
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
              new StateJsonError({
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
      return next;
    });

  const modify: Handle<A, EncodeR>['modify'] = (f) =>
    SynchronizedRef.modifyEffect(current, (value) => {
      const [result, next] = f(value);
      return Effect.map(
        persist(next),
        (persisted) => [result, persisted] as const,
      );
    });

  const set: Handle<A, EncodeR>['set'] = (value) =>
    SynchronizedRef.updateEffect(current, () => persist(value));

  const update: Handle<A, EncodeR>['update'] = (f) =>
    modify((value) => {
      const next = f(value);
      return [next, next] as const;
    });

  return {
    get: SynchronizedRef.get(current),
    set,
    update,
    modify,
  };
});

export const layerEphemeral = <D extends AnyDefinition>(definition: D) =>
  Layer.effect(definition, open(definition, undefined));

export * as AgentState from './state.js';
