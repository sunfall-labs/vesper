import { Context, Effect, Option, Schema, Stream } from 'effect';

import type { LogOffset } from './offset.js';
import type { ConversationRecord } from './record.js';

// The append-only log every conversation is written to.
//
// Two primitives and nothing else: `read`, which pages history, and
// `changes`, which says "something arrived". Resumable tailing is derived
// from those in `./tail.ts` rather than being a third method, so a backend
// implements two things and gets tailing that is tested once. The contract
// suite in `./log-store-contract.ts` holds every backend to the same
// behaviour; per `docs/contributing.md` it lives here, in the package that
// owns the interface, not in a testkit package that would cycle.
//
// ## Offsets are per record, not per batch
//
// The obvious design assigns one offset per append batch, and that makes
// mid-batch resumption impossible: a reader that crashed after processing two
// records of a five-record batch can only restart before the batch or after
// it, so it either re-processes or skips. Here every record gets its own sequence
// number while the batch is still written atomically. The cost is a row per
// record instead of a row per batch; the return is that a reader can resume
// anywhere, which is the entire point of having offsets.
//
// ## Producer fencing
//
// The standard mechanism, unchanged. `acquire` bumps the stream's epoch
// and resets the producer sequence; `append` carries both. An epoch that is
// not the current one is a producer that has been superseded — `fenced`. A
// sequence that repeats the last applied one is a retry, and returns the
// offset the original got, so a producer that crashed between writing and
// hearing the answer converges instead of duplicating. A sequence beyond the
// next expected one means writes were lost — `gap` — and the store refuses
// rather than leaving a hole nobody will notice until a replay reads it.
//
// Note the two counters. The producer sequence resets on every `acquire`;
// the stream's own record sequence never does. Conflating them would make
// offsets non-monotonic across a producer handover, which would silently
// break every ordering guarantee above.

/**
 * Everything that can go wrong in a log store.
 *
 * `reason` is a closed union rather than a message because callers act on
 * these: `fenced` means stop, this producer lost; `gap` means recover from
 * the log rather than retry; `offset_gone` means the reader has fallen off
 * the retained window and must restart from a snapshot.
 */
export class LogStoreError extends Schema.TaggedErrorClass<LogStoreError>()(
  '@sunfall/vesper-log/LogStoreError',
  {
    path: Schema.String,
    operation: Schema.Literals([
      'create',
      'acquire',
      'append',
      'read',
      'meta',
      'changes',
    ]),
    reason: Schema.Literals([
      /** No stream at this path. */
      'not_found',
      /** A stream already exists at this path. */
      'conflict',
      /** The producer's epoch is not the current one; it has been superseded. */
      'fenced',
      /** The producer's sequence skipped ahead; writes were lost. */
      'gap',
      /**
       * The requested offset predates what the backend still retains.
       * Nothing implements retention yet — the reason exists so the Postgres
       * backend has somewhere truthful to land when it does, rather than
       * reporting a trimmed offset as `not_found`.
       */
      'offset_gone',
      /** An append with no records. */
      'empty',
      /**
       * A record cannot be turned into its persisted form. Not retryable —
       * the same payload will fail the same way — which is why it is not
       * folded into `storage`.
       */
      'encoding',
      /** The backend itself failed. */
      'storage',
    ]),
    /** Human-readable context. Never matched on. */
    detail: Schema.String,
  },
) {}

/** What a stream is, apart from its contents. */
export interface StreamMeta {
  readonly path: string;
  /**
   * Who the stream belongs to — a conversation id, an agent identity.
   * Opaque to the store; it exists so a reader can tell whose log it opened
   * without reading a record.
   */
  readonly identity: string;
  /** Bumped by every {@link Interface.acquire}. */
  readonly epoch: number;
  /** The producer holding the current epoch, if any. */
  readonly producerId: Option.Option<string>;
  /** Offset of the last record written, or {@link LogOffset.START} if none. */
  readonly head: LogOffset.Offset;
  readonly records: number;
}

/** A producer's right to write to a stream, for as long as its epoch holds. */
export interface ProducerClaim {
  readonly path: string;
  readonly producerId: string;
  readonly epoch: number;
  /** The sequence the next {@link Interface.append} must carry. Always 0 here. */
  readonly nextSequence: number;
}

export interface AppendInput {
  readonly path: string;
  readonly producerId: string;
  readonly epoch: number;
  /**
   * The producer's batch counter, starting at
   * {@link ProducerClaim.nextSequence} and incrementing by one per
   * successful append. Not an offset — offsets are the store's to assign.
   */
  readonly sequence: number;
  readonly records: ReadonlyArray<ConversationRecord.Entry>;
}

export interface ReadOptions {
  /** Exclusive lower bound. Defaults to {@link LogOffset.START}. */
  readonly after?: LogOffset.Offset;
  /** Defaults to {@link DEFAULT_READ_LIMIT}. */
  readonly limit?: number;
}

export interface Page {
  readonly records: ReadonlyArray<ConversationRecord.Envelope>;
  /**
   * Where to resume: pass this as the next read's `after`.
   *
   * Named `cursor` on purpose, and not `nextOffset`. Implementations that
   * use that name generally do not hold the next offset in it — they hold the
   * last one written. Anyone who trusts the name reads the last record twice,
   * or, having corrected for it once, skips one. A cursor is a position, and this one
   * is exclusive-after like every other `after` in this interface.
   *
   * Equal to the supplied `after` when the page is empty.
   */
  readonly cursor: LogOffset.Offset;
  /**
   * True when nothing exists beyond {@link cursor} at the moment of the read.
   *
   * A backend must not report `false` unless there really is another record
   * to fetch; `./tail.ts` pages until this is `true` and would spin forever
   * on a backend that lies.
   */
  readonly upToDate: boolean;
}

export const DEFAULT_READ_LIMIT = 256;

export interface Interface {
  /**
   * Create an empty stream.
   *
   * Fails `conflict` when the path is taken. Creation is explicit rather
   * than implicit on first append so that a typo in a conversation id
   * surfaces as `not_found` instead of quietly starting a second, empty
   * history alongside the real one.
   */
  readonly create: (
    path: string,
    identity: string,
  ) => Effect.Effect<StreamMeta, LogStoreError>;

  /**
   * Claim the right to write, fencing off whoever held it before.
   *
   * Bumps the epoch and resets the producer sequence, so the previous
   * holder's next append fails `fenced` rather than interleaving with this
   * one's. Safe to call on a stream nobody is writing to.
   */
  readonly acquire: (
    path: string,
    producerId: string,
  ) => Effect.Effect<ProducerClaim, LogStoreError>;

  /**
   * Append a batch atomically, returning the offset of the **last** record
   * written.
   *
   * All-or-nothing: a rejected append leaves the log exactly as it was, so a
   * fenced or out-of-sequence producer cannot half-write a turn.
   *
   * An exact retry — same producer, same epoch, same sequence, and the same
   * records — succeeds without writing and returns the original last offset.
   * Sameness is decided by a digest of the encoded batch, not by a proxy
   * like record count: reusing a sequence for different content is not a
   * retry, and answering it with the earlier offset would drop the new
   * records silently. It fails `conflict`.
   */
  readonly append: (
    input: AppendInput,
  ) => Effect.Effect<LogOffset.Offset, LogStoreError>;

  /** Page history from an exclusive lower bound. */
  readonly read: (
    path: string,
    options?: ReadOptions,
  ) => Effect.Effect<Page, LogStoreError>;

  readonly meta: (
    path: string,
  ) => Effect.Effect<Option.Option<StreamMeta>, LogStoreError>;

  /**
   * Wake-ups. No payload, deliberately.
   *
   * A signal carrying records would be a second delivery path with its own
   * ordering and its own gaps, and a reader would have to reconcile it
   * against `read`. Carrying nothing, it can only ever be a hint to read
   * again, which is unfalsifiable: a spurious wake-up costs an empty read
   * and a coalesced one costs nothing at all.
   *
   * A `Stream` rather than a subscribe-returning-unsubscriber, because
   * `Scope` already handles teardown and a stream composes with the paging
   * in `./tail.ts` instead of needing a bridge.
   *
   * **A backend must emit one wake-up as soon as its subscription is
   * established, before any other element.** That opening tick is what makes
   * "catch up, then follow" implementable at all. Without it a reader has to
   * subscribe and read as two separate steps, and whichever order it picks
   * has a window — read first and an append landing before the subscription
   * is lost; subscribe first and the reader cannot tell when the
   * subscription actually took effect, because forking a fiber that will
   * subscribe is not the same as having subscribed. Receiving the tick is
   * proof, and it costs one empty read. Wake-ups may otherwise be coalesced
   * or spurious.
   *
   * **A change feed that dies must fail, not go quiet.** Postgres
   * `LISTEN`/`NOTIFY` over a dropped connection is the case this is for: a
   * feed that silently stops delivering leaves a tail that looks healthy and
   * is not, which is indistinguishable from a conversation where nothing is
   * happening. `Tail.from` propagates the failure rather than swallowing it,
   * so the consumer can reconnect or give up. Errors on this channel are
   * about the feed itself; a reader that has fallen behind still learns that
   * from `read`.
   */
  readonly changes: (path: string) => Stream.Stream<void, LogStoreError>;
}

export class Service extends Context.Service<Service, Interface>()(
  '@sunfall/vesper-log/LogStore',
) {}

export * as LogStore from './log-store.js';
