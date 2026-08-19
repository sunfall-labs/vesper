import { Crypto, Effect, Encoding, Schema } from 'effect';

import type { LogOffset } from './offset.js';
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
export class EncodeError extends Schema.TaggedError<EncodeError>(
  '@sunfall/vesper-log/EncodeError',
)('EncodeError', {
  detail: Schema.String,
}) {}

/** Maximum number of entries accepted by one append batch. */
export const MAX_RECORDS = 16_384;
/** Maximum nesting accepted while cloning JSON values. */
export const MAX_JSON_DEPTH = 64;
/** Maximum JSON values and properties visited while cloning. */
export const MAX_JSON_NODES = MAX_RECORDS * 32;
/** Maximum aggregate string characters in one cloned value or batch. */
export const MAX_STRING_CHARS = 4 * 1024 * 1024;
/** Maximum characters in the canonical JSON batch representation. */
export const MAX_BATCH_JSON_CHARS = 16 * 1024 * 1024;

interface JsonBudget {
  nodes: number;
  stringChars: number;
}

const newJsonBudget = (): JsonBudget => ({ nodes: 0, stringChars: 0 });

type JsonPrimitive = null | boolean | number | string;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

const newJsonObject = (): { [key: string]: JsonValue } => {
  const object: { [key: string]: JsonValue } = {};
  Object.setPrototypeOf(object, null);
  return object;
};

const jsonFailure = (path: string, detail: string): EncodeError =>
  new EncodeError({ detail: `${path} ${detail}` });

/** Validate and canonically clone one value exactly as JSON can represent it. */
const jsonClone = (
  value: unknown,
  path: string,
  ancestors: ReadonlySet<object>,
  budget: JsonBudget,
  depth: number,
): JsonValue => {
  if (depth > MAX_JSON_DEPTH) {
    throw jsonFailure(
      path,
      `exceeds maximum JSON depth of ${String(MAX_JSON_DEPTH)}`,
    );
  }
  budget.nodes += 1;
  if (budget.nodes > MAX_JSON_NODES) {
    throw jsonFailure(path, 'contains too many JSON values');
  }
  if (value === null || typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    budget.stringChars += value.length;
    if (value.length > MAX_STRING_CHARS) {
      throw jsonFailure(
        path,
        `exceeds maximum string length of ${String(MAX_STRING_CHARS)} characters`,
      );
    }
    if (budget.stringChars > MAX_STRING_CHARS) {
      throw jsonFailure(
        path,
        `exceeds maximum aggregate string length of ${String(MAX_STRING_CHARS)} characters`,
      );
    }
    return value;
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw jsonFailure(path, 'is not finite');
    }
    if (Object.is(value, -0)) {
      throw jsonFailure(path, 'is negative zero');
    }
    return value;
  }
  if (typeof value !== 'object') {
    throw jsonFailure(path, `has unsupported type ${typeof value}`);
  }
  if (ancestors.has(value)) {
    throw jsonFailure(path, 'contains a cycle');
  }

  const nextAncestors = new Set(ancestors).add(value);
  if (Array.isArray(value)) {
    const ownKeys = Reflect.ownKeys(value);
    if (budget.nodes + ownKeys.length > MAX_JSON_NODES) {
      throw jsonFailure(path, 'contains too many JSON values or properties');
    }
    let indexes = 0;
    for (const key of ownKeys) {
      if (key === 'length') {
        continue;
      }
      if (
        typeof key !== 'string' ||
        !/^(0|[1-9]\d*)$/.test(key) ||
        Number(key) >= value.length
      ) {
        throw jsonFailure(path, `has non-JSON array property ${String(key)}`);
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined) {
        throw jsonFailure(`${path}[${key}]`, 'is not an own property');
      }
      if (!('value' in descriptor)) {
        throw jsonFailure(`${path}[${key}]`, 'is an accessor property');
      }
      indexes += 1;
    }
    if (indexes !== value.length) {
      throw jsonFailure(path, 'is sparse');
    }

    const clone: JsonValue[] = [];
    for (let index = 0; index < value.length; index += 1) {
      clone.push(
        jsonClone(
          value[index],
          `${path}[${String(index)}]`,
          nextAncestors,
          budget,
          depth + 1,
        ),
      );
    }
    return clone;
  }

  const prototype = Reflect.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw jsonFailure(path, 'is not a plain object');
  }
  const stringKeys: string[] = [];
  const ownKeys = Reflect.ownKeys(value);
  if (budget.nodes + ownKeys.length > MAX_JSON_NODES) {
    throw jsonFailure(path, 'contains too many JSON values or properties');
  }
  for (const key of ownKeys) {
    if (typeof key === 'symbol') {
      throw jsonFailure(path, `has symbol property ${String(key)}`);
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) {
      throw jsonFailure(`${path}.${key}`, 'is not an own property');
    }
    if (descriptor.enumerable !== true) {
      throw jsonFailure(`${path}.${key}`, 'is non-enumerable');
    }
    if (!('value' in descriptor)) {
      throw jsonFailure(`${path}.${key}`, 'is an accessor property');
    }
    stringKeys.push(key);
  }

  // This array is local and disposable. Native sorting keeps canonicalization
  // O(n log n); insertion sorting is prohibitively expensive near the JSON
  // node budget.
  stringKeys.sort();
  const clone = newJsonObject();
  for (const key of stringKeys) {
    budget.stringChars += key.length;
    if (budget.stringChars > MAX_STRING_CHARS) {
      throw jsonFailure(
        path,
        `exceeds maximum aggregate string length of ${String(MAX_STRING_CHARS)} characters`,
      );
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) {
      throw jsonFailure(`${path}.${key}`, 'is not an own property');
    }
    if (!('value' in descriptor)) {
      throw jsonFailure(`${path}.${key}`, 'is an accessor property');
    }
    clone[key] = jsonClone(
      descriptor.value,
      `${path}.${key}`,
      nextAncestors,
      budget,
      depth + 1,
    );
  }
  return clone;
};

/** Validate and clone an opaque value exactly as conversation storage will. */
export const prepareUnknown = (
  value: unknown,
): Effect.Effect<unknown, EncodeError> =>
  Effect.try({
    try: () => jsonClone(value, '$', new Set(), newJsonBudget(), 0),
    catch: (cause) =>
      Schema.is(EncodeError)(cause)
        ? cause
        : new EncodeError({
            detail: `value is not JSON-safe: ${String(cause)}`,
          }),
  });

const sha256 = (
  material: string,
): Effect.Effect<string, EncodeError, Crypto.Crypto> =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const digest = yield* crypto
      .digest('SHA-256', new TextEncoder().encode(material))
      .pipe(
        Effect.mapError(
          (cause) =>
            new EncodeError({
              detail: `cannot fingerprint records: ${String(cause)}`,
            }),
        ),
      );
    return Encoding.encodeHex(digest);
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
): Effect.Effect<PreparedBatch, EncodeError, Crypto.Crypto> =>
  Effect.gen(function* () {
    if (entries.length > MAX_RECORDS) {
      return yield* new EncodeError({
        detail: `batch contains ${String(entries.length)} records; maximum is ${String(MAX_RECORDS)}`,
      });
    }
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
      try: () => jsonClone(encoded, '$', new Set(), newJsonBudget(), 0),
      catch: (cause) =>
        Schema.is(EncodeError)(cause)
          ? cause
          : new EncodeError({
              detail: `records are not JSON-safe: ${String(cause)}`,
            }),
    });
    if (!Array.isArray(canonical)) {
      return yield* new EncodeError({
        detail: 'canonical records are not an array',
      });
    }
    const material = yield* Schema.encodeEffect(
      Schema.fromJsonString(Schema.Array(Schema.Unknown)),
    )(canonical).pipe(
      Effect.mapError(
        (error) =>
          new EncodeError({
            detail: `canonical records cannot encode: ${String(error)}`,
          }),
      ),
    );
    if (material.length > MAX_BATCH_JSON_CHARS) {
      return yield* new EncodeError({
        detail: `encoded batch is ${String(material.length)} characters; maximum is ${String(MAX_BATCH_JSON_CHARS)}`,
      });
    }
    const persisted = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Array(Schema.Unknown)),
    )(material).pipe(
      Effect.mapError(
        (error) =>
          new EncodeError({
            detail: `canonical records are not JSON: ${String(error)}`,
          }),
      ),
    );
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
