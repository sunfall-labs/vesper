import { Effect, Order, Schema } from 'effect';

// Where a reader is in a log.
//
// The format is borrowed from Durable Streams (ElectricSQL's public HTTP
// protocol, github.com/durable-streams/durable-streams): two zero-padded
// 16-digit decimal components joined by `_`, with `-1` as the sentinel for
// "from the beginning". We adopt the *offset format only* — none of the wire
// protocol, forks, sub-offsets, retention, or `Stream-Closed`.
//
// Borrowing it costs nothing and buys two things. Interop, if a DS-speaking
// reader ever shows up. And, more immediately, the property the padding
// exists for: fixed-width zero-padded decimals compare correctly as bytes,
// so ordering an offset is a string comparison in JavaScript, in SQL, in a
// URL query, and in a sorted key store, with no parsing and no agreement
// about integer width. `'-1'` sorts before every real offset because `'-'`
// is 0x2D and `'0'` is 0x30 — that is why the sentinel is spelled that way
// rather than as `0`.
//
// Sequences are `bigint` everywhere inside this package and become strings
// only here, at the boundary. A single 16-digit component reaches 10^16,
// which is an order of magnitude past `Number.MAX_SAFE_INTEGER` — parsing
// one into a JS number would be silently lossy on a log large enough to
// matter and perfectly fine on every log small enough to test.
//
// The module namespace is `LogOffset` rather than `Offset` because the
// primary type is already called `Offset` and a file cannot export both
// under one name. Same shape as `AgentEvents.Event`.

/**
 * A position in a log.
 *
 * Branded rather than a bare `string` so a conversation id, a path, or a
 * producer id cannot be passed where a position is expected. The brand adds
 * no runtime check; {@link fromSeq} and {@link toSeq} are the validated
 * constructors.
 */
export const Offset = Schema.String.pipe(
  Schema.brand('@sunfall/vesper-log/Offset'),
);
export type Offset = typeof Offset.Type;

export class OffsetError extends Schema.TaggedError<OffsetError>()(
  '@sunfall/vesper-log/OffsetError',
  {
    offset: Schema.String,
  },
) {}

/** Digits in one component. Fixed by the Durable Streams format. */
export const COMPONENT_DIGITS = 16;

/** One more than the largest value a single component can hold. */
const COMPONENT_SPAN = 10n ** BigInt(COMPONENT_DIGITS);

/** The largest sequence the two components can express. */
export const MAX_SEQ = COMPONENT_SPAN * COMPONENT_SPAN - 1n;

/**
 * Read from the beginning.
 *
 * Also what {@link toSeq} maps to `-1n`, which is what makes `after` an
 * exclusive bound uniformly: the first record is sequence `0`, and
 * "everything after -1" is everything.
 */
export const START: Offset = Offset.make('-1');

const pad = (value: bigint): string =>
  value.toString().padStart(COMPONENT_DIGITS, '0');

/**
 * Format a sequence number as an offset.
 *
 * The sequence is split across the two components, high half first, so the
 * pair is really one 32-digit number written in two halves. DS calls the
 * high component a read sequence and uses it for forks; we have no forks, so
 * ours is `0` for every sequence below 10^16 — which is to say, always.
 * Should forks arrive, `0` is the natural generation zero and existing
 * offsets keep meaning what they meant.
 *
 * Throws on a negative or absurd sequence. Both are defects — a store
 * handing this function a sequence it did not derive from its own counter is
 * broken in a way no caller can recover from — and this is the same bargain
 * `Schema.make` strikes for trusted construction.
 */
export const fromSeq = (seq: bigint): Offset => {
  if (seq < 0n || seq > MAX_SEQ) {
    throw new RangeError(`Offset sequence out of range: ${seq}`);
  }
  return Offset.make(
    `${pad(seq / COMPONENT_SPAN)}_${pad(seq % COMPONENT_SPAN)}`,
  );
};

const PATTERN = new RegExp(
  `^(\\d{${COMPONENT_DIGITS}})_(\\d{${COMPONENT_DIGITS}})$`,
);

/**
 * Parse an offset back into a sequence number.
 *
 * Fails rather than throws: offsets arrive from readers, from URLs, and from
 * stored cursors, so a malformed one is an ordinary boundary failure and not
 * a defect. {@link START} decodes to `-1n`.
 */
export const toSeq = (offset: Offset): Effect.Effect<bigint, OffsetError> =>
  Effect.suspend(() => {
    if (offset === START) return Effect.succeed(-1n);

    const match = PATTERN.exec(offset);
    if (match === null) {
      return Effect.fail(new OffsetError({ offset }));
    }
    return Effect.succeed(
      BigInt(match[1]!) * COMPONENT_SPAN + BigInt(match[2]!),
    );
  });

/**
 * Ordering on offsets.
 *
 * Plain string comparison, correct by construction rather than by
 * implementation: fixed-width zero-padded decimals order lexicographically
 * exactly as they order numerically. Nothing here parses, so nothing here
 * can be wrong about integer width.
 */
export const order: Order.Order<Offset> = Order.String;

/** True when `self` is strictly after `that`. */
export const isAfter: (self: Offset, that: Offset) => boolean =
  Order.isGreaterThan(order);

export * as LogOffset from './offset.js';
