import { Effect, Schema } from 'effect';

import { LogOffset } from './offset.js';
import { Entry, Envelope } from './record.js';

/** Build the persisted envelope for an entry once its offset is known. */
export const envelope = (offset: LogOffset.Offset, entry: Entry): Envelope => ({
  offset,
  conversationId: entry.conversationId,
  timestamp: entry.timestamp,
  record: entry.record,
});

/** Codecs shared by persistence adapters and their contract suite. */
export const decodeEnvelope = Schema.decodeUnknownEffect(Envelope);
export const decodeEntry = Schema.decodeUnknownEffect(Entry);
export const encodeEnvelope = Schema.encodeEffect(Envelope);
export const encodeEntry = Schema.encodeEffect(Entry);

/** A record that cannot be turned into its persisted form. */
export class EncodeError extends Schema.TaggedError<EncodeError>()(
  '@sunfall/vesper-log/RecordEncodeError',
  {
    detail: Schema.String,
  },
) {}

type JsonPrimitive = null | boolean | number | string;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

const jsonFailure = (path: string, detail: string): EncodeError =>
  new EncodeError({ detail: `${path} ${detail}` });

/** Validate and canonically clone one value exactly as JSON can represent it. */
const jsonClone = (
  value: unknown,
  path: string,
  ancestors: ReadonlySet<object>,
): JsonValue => {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean'
  ) {
    return value;
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw jsonFailure(path, 'is not finite');
    if (Object.is(value, -0)) throw jsonFailure(path, 'is negative zero');
    return value;
  }
  if (typeof value !== 'object') {
    throw jsonFailure(path, `has unsupported type ${typeof value}`);
  }
  if (ancestors.has(value)) throw jsonFailure(path, 'contains a cycle');

  const nextAncestors = new Set(ancestors).add(value);
  if (Array.isArray(value)) {
    const ownKeys = Reflect.ownKeys(value);
    let indexes = 0;
    for (const key of ownKeys) {
      if (key === 'length') continue;
      if (
        typeof key !== 'string' ||
        !/^(0|[1-9]\d*)$/.test(key) ||
        Number(key) >= value.length
      ) {
        throw jsonFailure(path, `has non-JSON array property ${String(key)}`);
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
      if (!('value' in descriptor)) {
        throw jsonFailure(`${path}[${key}]`, 'is an accessor property');
      }
      indexes += 1;
    }
    if (indexes !== value.length) throw jsonFailure(path, 'is sparse');

    const clone: JsonValue[] = [];
    for (let index = 0; index < value.length; index += 1) {
      clone.push(jsonClone(value[index], `${path}[${index}]`, nextAncestors));
    }
    return clone;
  }

  const prototype = Object.getPrototypeOf(value) as object | null;
  if (prototype !== Object.prototype && prototype !== null) {
    throw jsonFailure(path, 'is not a plain object');
  }
  const stringKeys: string[] = [];
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key === 'symbol') {
      throw jsonFailure(path, `has symbol property ${String(key)}`);
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
    if (!descriptor.enumerable) {
      throw jsonFailure(`${path}.${key}`, 'is non-enumerable');
    }
    if (!('value' in descriptor)) {
      throw jsonFailure(`${path}.${key}`, 'is an accessor property');
    }
    stringKeys.push(key);
  }

  const clone = Object.create(null) as { [key: string]: JsonValue };
  for (const key of stringKeys.sort()) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
    clone[key] = jsonClone(
      (descriptor as PropertyDescriptor & { value: unknown }).value,
      `${path}.${key}`,
      nextAncestors,
    );
  }
  return clone;
};

/** Validate and clone an opaque value exactly as conversation storage will. */
export const prepareUnknown = (
  value: unknown,
): Effect.Effect<unknown, EncodeError> =>
  Effect.try({
    try: () => jsonClone(value, '$', new Set()),
    catch: (cause) =>
      cause instanceof EncodeError
        ? cause
        : new EncodeError({
            detail: `value is not JSON-safe: ${String(cause)}`,
          }),
  });

const sha256 = (material: string): Effect.Effect<string, EncodeError> =>
  Effect.tryPromise({
    try: async () => {
      const bytes = new TextEncoder().encode(material);
      const digest = await crypto.subtle.digest('SHA-256', bytes);
      return Array.from(new Uint8Array(digest), (byte) =>
        byte.toString(16).padStart(2, '0'),
      ).join('');
    },
    catch: (cause) =>
      new EncodeError({
        detail: `cannot fingerprint records: ${String(cause)}`,
      }),
  });

export interface PreparedBatch {
  /** Canonical JSON clones decoded back through the public entry schema. */
  readonly entries: ReadonlyArray<Entry>;
  /** Exact canonical bytes fingerprinted and suitable for JSON persistence. */
  readonly encoded: string;
  /** SHA-256 of the canonical encoded batch bytes. */
  readonly fingerprint: string;
}

/** Prepare the one representation every backend stores and fingerprints. */
export const prepare = (
  entries: ReadonlyArray<Entry>,
): Effect.Effect<PreparedBatch, EncodeError> =>
  Effect.gen(function* () {
    const encoded = yield* Effect.forEach(entries, (entry) =>
      encodeEntry(entry),
    ).pipe(
      Effect.mapError(
        (error) =>
          new EncodeError({
            detail: `records do not encode: ${String(error)}`,
          }),
      ),
    );

    const canonical = yield* Effect.try({
      try: () => jsonClone(encoded, '$', new Set()),
      catch: (cause) =>
        cause instanceof EncodeError
          ? cause
          : new EncodeError({
              detail: `records are not JSON-safe: ${String(cause)}`,
            }),
    });
    const material = JSON.stringify(canonical);
    const persisted = JSON.parse(material) as ReadonlyArray<unknown>;
    const normalized = yield* Effect.forEach(persisted, (entry) =>
      decodeEntry(entry),
    ).pipe(
      Effect.mapError(
        (error) =>
          new EncodeError({
            detail: `canonical records do not decode: ${String(error)}`,
          }),
      ),
    );
    return {
      entries: normalized,
      encoded: material,
      fingerprint: yield* sha256(material),
    };
  });

export * as RecordBatch from './record-batch.js';
