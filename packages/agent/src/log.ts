import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Cause, Clock, Effect, Exit, Option, Ref, Stream } from 'effect';
import { Prompt, type Response, type Tool } from 'effect/unstable/ai';

import { AgentBranch } from './branch.js';
import type { AgentEvents } from './event.js';
import { AgentHistory } from './history.js';
import { AgentSignals } from './signal.js';
import type { Stop } from './stop.js';

// The sink that turns a run into records, and the handle it writes through.
//
// The loop already emits everything the log needs, so this is a sink and not
// new bookkeeping: `AgentEvents.Event` in, the same events out, records
// written on the way past. It is reached by `agent.recordingTo(id)` and by a
// parent delegating into `agent.runInSession(session, input)` — an agent that
// does neither never opens a {@link Session} and never touches this file.
//
// ## Two ordering properties, both load-bearing
//
// Records are appended with `Stream.tap`, so the append completes *before*
// the event it describes reaches the consumer. That is the same rule
// `@sunfall/vesper-durable` states as "deltas stream live, but `finish` is withheld
// until the checkpoint is durable": a consumer must never act on something
// the durable record does not yet contain. Here the consumer is whatever runs
// tool calls in reaction to a turn, or a UI that will resume from an offset.
//
// And a batch is one append, so the text that preceded a tool call and the
// tool call itself land atomically or not at all. A reader cannot observe a
// `ToolCall` whose preceding assistant text was lost.
//
// ## Where text is cut
//
// Coalescing boundaries are **semantic**: one `Text` record per contiguous
// run of assistant text within a turn, flushed when the model does something
// other than talk — a tool call, a tool result, a signal, the end of a turn,
// the end of the run.
//
// The alternatives are a fixed character count or sentence detection, and
// both are worse for the same reason. The log exists to rebuild a
// conversation, and what it rebuilds a `Prompt` message from is exactly "what
// the model said before it did something else". A 512-character cut splits
// that in a place with no meaning, mid-word and mid-JSON, and makes the row
// count a function of a constant nobody can justify. Sentence detection is
// worse still: it is locale- and content-sensitive, it mangles code blocks,
// and it buys nothing a reader wants — nobody resumes a conversation at a
// sentence.
//
// The cost of the semantic boundary is that one very long uninterrupted
// answer is one large row. That is bounded by the model's max output tokens,
// which is the same bound the provider already imposes on a single turn.
//
// ## One producer per run, shared by everything the run writes
//
// A {@link Session} is that producer. The sink writes through it, the loop
// writes signal deliveries through it, delegation opens children through it.
// They have to be one producer and not several: the store's fencing gives a
// stream exactly one writer, so a second `acquire` for the same conversation
// would fence the first, and a run that recorded its signals through a
// separate claim would kill its own event sink on the next append.

/**
 * Where a conversation's stream lives.
 *
 * One function, exported, because the writer and {@link Agent.streamFrom}
 * have to agree and a convention that lives in two places is a convention
 * that eventually differs. The prefix leaves room for streams that are not
 * conversations — `signals/<id>`, and a per-agent identity stream if one ever
 * lands — without a collision that would only surface as one log interleaved
 * into another.
 */
export const pathFor = (conversationId: string): string =>
  `conversations/${conversationId}`;

/**
 * A conversation id for a child, derived rather than minted.
 *
 * Deterministic on purpose. A random id would give a recovered run a *second*
 * child conversation for the same delegation, leaving the first orphaned with
 * no reference to it from anywhere; deriving it means the retry lands on the
 * same stream, so the child's own log — and its own resumption — carries on
 * rather than starting again beside itself. Tool call ids are unique within a
 * conversation, which is what makes this collision-free.
 */
export const childIdFor = (
  parentConversationId: string,
  toolCallId: string,
): string => `${parentConversationId}/${toolCallId}`;

/** How a tool call ended, as a previous run recorded it. */
export interface Settled {
  readonly outcome: 'success' | 'failure';
  /** The encoded result, in the form the provider was shown it. */
  readonly result: unknown;
}

/** A signal this run has taken delivery of. */
export interface Delivered {
  readonly kind: 'steer' | 'cancel';
  readonly text: string;
  readonly source: string;
  /** The signal's offset in the signal stream. */
  readonly at: string;
}

/** Where a run picks a conversation up, when not at its end. */
export interface OpenOptions {
  /**
   * Continue from this record instead of from the conversation's last one.
   *
   * The offset is **inclusive**: the record it names is the last one the new
   * run inherits, and everything after it becomes an abandoned branch. The
   * marker is written as part of claiming — see {@link open} — which is what
   * lets every reader below stay unaware that branching exists.
   */
  readonly branchFrom: LogOffset.Offset;
}

/**
 * What {@link openWith} may write between claiming a stream and reading it.
 *
 * Both fields describe the same manoeuvre from two sides: put something into
 * the stream *before* the history read, so that everything derived from that
 * read — the prompt, the recovery index, the signal cursor — describes the
 * conversation the caller meant rather than the one the records literally
 * came from. `branchFrom` writes one marker into an existing conversation;
 * `seed` writes a copied prefix into a new one. See {@link open} for why the
 * ordering is the whole trick, and {@link fork} for the copy.
 *
 * Internal, and structurally a supertype of {@link OpenOptions}, so the public
 * `open` passes its own options straight through.
 */
interface ClaimOptions {
  readonly branchFrom?: LogOffset.Offset;
  readonly seed?: ReadonlyArray<ConversationRecord.Envelope>;
}

export interface ChildOptions {
  /** The delegation tool call the child answers. */
  readonly toolCallId: string;
  /** The child agent's name. */
  readonly agent: string;
  /** The child's delegation depth; 1 for a top-level agent's child. */
  readonly depth: number;
}

/**
 * One run's claim on a conversation, and everything it can do with it.
 *
 * Passed by value rather than read from the context. It could have been a
 * `Context.Reference` defaulting to "not recording", and that shape is
 * rejected deliberately: `Checkpointer.RunId` is what a defaulted reference
 * for persistence looks like after it has gone wrong, and Effect's own
 * guidance is not to put persistence behind one. Handing the session down the
 * call chain means the loop, the sink, and delegation are all looking at the
 * same object because they were given it, not because a lookup happened to
 * find something.
 *
 * Nothing here fails. The store's errors become defects — see {@link orDie} —
 * for the reason `@sunfall/vesper-durable` gives for a checkpoint write: this is
 * infrastructure, not something the model did, and a run whose result exists
 * while its history does not is the exact divergence the log removes.
 */
export interface Session {
  readonly conversationId: string;

  /**
   * Everything the conversation contained when this run claimed it.
   *
   * Read once at {@link open}, which already pays for the full scan to build
   * the recovery index and the signal cursor, and held rather than re-read:
   * a second read would race this run's own appends and hand back a history
   * that includes what this run has already written. `AgentHistory` turns it
   * into a `Prompt`, which is how a run continues a conversation instead of
   * restarting it.
   *
   * The **whole log**, including any branch this conversation has abandoned.
   * It is deliberately not pre-filtered to the active path: two of the folds
   * over it must see what physically happened rather than what the
   * conversation now says, and `branch.ts` holds the table of which are which.
   * A caller building a prompt goes through `AgentHistory`, which filters.
   *
   * A run that claimed the conversation at an earlier point — `branchFrom` —
   * finds its own `BranchedFrom` marker as the last entry here. The marker is
   * written during {@link open} precisely so that it is: it has to be in this
   * array for the prompt and the recovery index to describe the branch rather
   * than the conversation it came from.
   */
  readonly history: ReadonlyArray<ConversationRecord.Envelope>;

  /**
   * Everything the conversation contains **now**, including what this run has
   * already written.
   *
   * The deliberate opposite of {@link history}, and the reason both exist. A
   * reader that must not see this run's own records reads `history`; a writer
   * that needs to point at a record *it just wrote* reads this. Only
   * compaction does — the `Compacted` record names the record its summary
   * stops replacing, and that record is usually one this run appended.
   *
   * A fresh scan every call, which is affordable because compaction is the
   * only caller and a conversation compacts once in a very long while. The
   * alternative — a ledger of offsets kept alongside the appends — would mean
   * inferring each record's offset from the last one in its batch, and that
   * is store-internal arithmetic this side has no business knowing.
   */
  readonly recorded: Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>>;

  /** Append a batch. An empty batch is a no-op, not an empty write. */
  readonly append: (
    records: ReadonlyArray<ConversationRecord.Record>,
  ) => Effect.Effect<void>;

  /**
   * The outcome an **unsettled earlier run** already recorded for this call.
   *
   * Synchronous, because it is a lookup in an index built when the session
   * opened, and that timing is the safety property: everything in it predates
   * anything this run writes, so a run can never serve itself its own result.
   *
   * Empty unless the conversation's last `RunStarted` has no `RunSettled`
   * after it. A run that ended — successfully, in failure, or cancelled — has
   * nothing to recover, so its tool outcomes are not offered to a later one.
   */
  readonly settled: (
    name: string,
    toolCallId: string,
  ) => Option.Option<Settled>;

  /**
   * Signals that have arrived since the last drain.
   *
   * One page per call. A backlog larger than a page is delivered across
   * successive turn boundaries rather than in one burst, which is fine —
   * nothing here promises to deliver a backlog atomically — and it keeps the
   * drain a single bounded read.
   */
  readonly drainSignals: Effect.Effect<ReadonlyArray<Delivered>>;

  /**
   * Record a delegation and open the child's own conversation.
   *
   * Writes the same `ChildSession` record into both logs: into this one as
   * "I delegated here", into the child's as its opening statement of whose
   * child it is. See the record's own documentation for why one record type
   * rather than two conventions.
   */
  readonly child: (options: ChildOptions) => Effect.Effect<Session>;
}

/**
 * Claim a conversation and read back what a previous run left.
 *
 * `create` tolerates `conflict` — a second run on an existing conversation is
 * the normal case, not an error — and `acquire` then fences whoever wrote
 * last. Fencing is the intended behaviour: a conversation has one writer, and
 * a second concurrent run should fail on its next append rather than
 * interleave two runs into one history.
 *
 * The history read is a full scan of the conversation. That is O(records) per
 * run, which is the cost of the two things it produces — the unsettled-run
 * index and the signal delivery cursor — and it is the point at which
 * `@sunfall/vesper-store`'s snapshot stops being merely a cache. Nothing here is
 * tuned for a very long conversation yet.
 *
 * ## Branching happens here, before anything is read
 *
 * `options.branchFrom` claims the conversation at an earlier record than its
 * last, which is what "edit an earlier message and re-run" is from the log's
 * side. The `BranchedFrom` marker is appended **between the claim and the
 * history read**, and that ordering is the whole trick: everything derived
 * below — the prompt a caller rebuilds from `history`, the recovery index, the
 * signal cursor — is derived from a log that already contains the marker, so
 * each of them describes the branch without a single one of them knowing that
 * branching exists.
 *
 * Doing it any later would be wrong in a way that is quiet. A marker appended
 * after `unsettledOutcomes` ran would leave the crashed run the user branched
 * *away from* still holding the recovery index, and the new run would be
 * served tool results answering calls that are no longer in its prompt.
 */
export const open = Effect.fn('AgentLog.open')(function* (
  conversationId: string,
  options?: OpenOptions,
) {
  const store = yield* LogStore.Service;
  return yield* openWith(store, conversationId, options);
});

/**
 * Start a **new** conversation from a prefix of an existing one.
 *
 * {@link open}'s `branchFrom` moves where one conversation continues from;
 * this makes a second conversation that begins where the first had reached at
 * `at`. The difference that matters is the stream: a branch stays in the
 * ancestor's stream and therefore inherits its single writer, so two branches
 * are sequential by construction. A fork is its own stream with its own
 * producer claim, so two forks of one conversation run **concurrently**,
 * which is the only reason this exists.
 *
 * The ancestor is **read, never claimed**. Reading takes no producer, so
 * forking cannot fence a run that is live on the conversation being forked
 * from, and the ancestor gets no record of having been forked — its own
 * history is exactly what it was.
 *
 * ## Records are copied, and their pointers are reseated
 *
 * The alternative was to seed the fork from a rebuilt prompt — one
 * `RunStarted` whose prompt is `messagesFrom(activePath(prefix))` — which is
 * cheaper and needs no pointer rewriting at all. It is rejected because of
 * what it does to the fork's *later* life, not to its first turn. `fold`
 * attributes every message from a `RunStarted` to that one record's offset,
 * so a seeded prefix collapses to a single position; `boundaryFor` would then
 * resolve any compaction boundary that fell inside it back to that one offset
 * and `keptFrom` would keep the entire ancestor history verbatim beside the
 * summary. The fork would be unable to compact its own inheritance, and would
 * re-trigger compaction immediately — precisely the bug
 * `ConversationRecord.Compacted` was given `firstKept` to fix. Copying keeps
 * one record per record, so a fork compacts, branches, and resumes exactly
 * like any other conversation.
 *
 * The price is the pointer trap, and it has two halves that are **not** the
 * same problem:
 *
 *   - `Compacted.firstKept` points into *this* stream. The copy gives every
 *     record a new offset, so the pointer is rewritten through a map from
 *     ancestor offset to fork offset — see {@link seedInto}, which flushes
 *     before each such record so the map is populated when it is read.
 *   - `SignalReceived.at` points into the conversation's **signal** stream,
 *     and the fork has a different one — empty, and starting from low offsets
 *     again. There is no offset in it that corresponds, so this cannot be
 *     rewritten; it is reset to {@link LogOffset.START}. That is the correct
 *     value rather than a fallback: `deliveredThrough` reads it as "how far
 *     this conversation has drained its own signal stream", and the fork has
 *     drained none of its own. Carrying the ancestor's number over would park
 *     the fork's cursor past every offset a genuine signal to the fork would
 *     be written at, and those signals would be **silently never delivered**.
 *     The record's body is kept, because a `steer` is a user message in the
 *     rebuilt conversation and dropping it would change what the model sees.
 *
 * `ChildSession` keeps the ancestor's ids deliberately. They name a child
 * conversation that really exists, over there; rewriting them to the fork
 * would name one that does not exist anywhere.
 *
 * Forking into an id that already holds a conversation is a defect, not a
 * merge: the `create` below does not tolerate the conflict that {@link open}
 * does.
 */
export const fork = Effect.fn('AgentLog.fork')(function* (
  conversationId: string,
  at: LogOffset.Offset,
  forkConversationId: string,
) {
  const store = yield* LogStore.Service;

  yield* orDie(
    store.create(pathFor(forkConversationId), forkConversationId),
  ).pipe(Effect.asVoid);

  const ancestor = yield* readAll(store, pathFor(conversationId));
  // Cut first, then fold: this is exactly the sequence `activePath` would
  // return if a `BranchedFrom { at }` had been appended to the ancestor, which
  // is what makes a fork inherit the same prefix `branchFrom` would have.
  const prefix = AgentBranch.activePath(
    ancestor.filter((envelope) => !LogOffset.isAfter(envelope.offset, at)),
  );

  return yield* openWith(store, forkConversationId, { seed: prefix });
});

const openWith = (
  store: LogStore.Interface,
  conversationId: string,
  options?: ClaimOptions,
): Effect.Effect<Session> =>
  Effect.gen(function* () {
    const path = pathFor(conversationId);

    yield* store.create(path, conversationId).pipe(
      Effect.asVoid,
      Effect.catchIf(
        (error) => error.reason === 'conflict',
        () => Effect.void,
      ),
      orDie,
    );

    const claim = yield* orDie(store.acquire(path, crypto.randomUUID()));
    const sequence = yield* Ref.make(claim.nextSequence);

    const append: Session['append'] = (records) =>
      records.length === 0
        ? Effect.void
        : Effect.gen(function* () {
            const timestamp = yield* Clock.currentTimeMillis;
            const next = yield* Ref.get(sequence);

            yield* orDie(
              store.append({
                path,
                producerId: claim.producerId,
                epoch: claim.epoch,
                sequence: next,
                records: records.map((record) => ({
                  conversationId,
                  timestamp,
                  record,
                })),
              }),
            );

            // Advanced only on success, so a caller that retries a failed
            // append reuses the sequence — which the store answers
            // idempotently when the batch digest matches, and rejects when it
            // does not.
            yield* Ref.set(sequence, next + 1);
          });

    if (options?.branchFrom !== undefined) {
      yield* append([{ _tag: 'BranchedFrom', at: options.branchFrom }]);
    }
    if (options?.seed !== undefined) {
      yield* seedInto(options.seed, append, readAll(store, path));
    }

    const history = yield* readAll(store, path);
    // Scoped to the active path: a run this conversation branched away from
    // recorded tool outcomes for calls that are no longer in anyone's prompt,
    // and serving those back would answer questions the resumed run never
    // asked. The signal cursor immediately below is the opposite case, and
    // `branch.ts` says why.
    const recoverable = unsettledOutcomes(AgentBranch.activePath(history));
    const signalCursor = yield* Ref.make(deliveredThrough(history));

    const drainSignals = Effect.gen(function* () {
      const after = yield* Ref.get(signalCursor);
      const signalPath = AgentSignals.pathFor(conversationId);

      const page = yield* store.read(signalPath, { after }).pipe(
        // No stream at all is the ordinary case: nobody has ever signalled
        // this conversation. It is not an empty page — the store
        // distinguishes those deliberately — so it is caught here rather than
        // by creating the stream from the reading side, which would leave an
        // empty signal stream behind every run that was never steered.
        Effect.catchIf(
          (error) => error.reason === 'not_found',
          () =>
            Effect.succeed({
              records: [],
              cursor: after,
              upToDate: true,
            } satisfies LogStore.Page),
        ),
        orDie,
      );

      yield* Ref.set(signalCursor, page.cursor);

      return page.records.flatMap(
        (envelope): ReadonlyArray<Delivered> =>
          envelope.record._tag === 'Signal'
            ? [
                {
                  kind: envelope.record.kind,
                  text: envelope.record.text,
                  source: envelope.record.source,
                  at: envelope.offset,
                },
              ]
            : [],
      );
    });

    const child = (options: ChildOptions): Effect.Effect<Session> =>
      Effect.gen(function* () {
        const childConversationId = childIdFor(
          conversationId,
          options.toolCallId,
        );
        const reference: ConversationRecord.Record = {
          _tag: 'ChildSession',
          toolCallId: options.toolCallId,
          agent: options.agent,
          parentConversationId: conversationId,
          childConversationId,
          depth: options.depth,
        };

        yield* append([reference]);
        const session = yield* openWith(store, childConversationId);
        yield* session.append([reference]);
        return session;
      }).pipe(Effect.withSpan('AgentLog.Session.child'));

    return {
      conversationId,
      history,
      recorded: readAll(store, path),
      append,
      settled: (name, toolCallId) =>
        Option.fromUndefinedOr(recoverable.get(settledKey(name, toolCallId))),
      drainSignals,
      child,
    } satisfies Session;
  });

const readAll = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>> =>
  Effect.gen(function* () {
    const all: ConversationRecord.Envelope[] = [];
    let cursor = LogOffset.START;
    let done = false;

    while (!done) {
      const page = yield* orDie(store.read(path, { after: cursor }));
      all.push(...page.records);
      cursor = page.cursor;
      done = page.upToDate;
    }

    return all;
  });

/**
 * Copy an ancestor's prefix into a freshly claimed stream.
 *
 * Writes in as few batches as the pointers allow. A run of records with no
 * offset-valued pointer is one append; a `Compacted` forces a flush before it,
 * because it is rewritten through a map that only holds offsets already
 * written. Compaction is rare, so this is one append for almost every fork.
 *
 * The index alignment is what makes the map cheap and is why this runs inside
 * {@link openWith}'s claim: the stream was created empty by {@link fork} and
 * this producer is the only writer, so the nth envelope read back is the nth
 * record copied.
 */
const seedInto = (
  prefix: ReadonlyArray<ConversationRecord.Envelope>,
  append: Session['append'],
  recorded: Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const reseated = new Map<LogOffset.Offset, LogOffset.Offset>();
    const sources: Array<LogOffset.Offset> = [];
    let pending: Array<ConversationRecord.Record> = [];

    const flush = Effect.gen(function* () {
      if (pending.length === 0) return;
      yield* append(pending);
      pending = [];

      const written = yield* recorded;
      written.forEach((envelope, index) => {
        const source = sources[index];
        if (source !== undefined) reseated.set(source, envelope.offset);
      });
    });

    for (const { offset, record } of prefix) {
      // Flushed *before* any record whose pointer is rewritten through the
      // map, so that the offsets it may name are in it. `Compacted` is the
      // only such case today, and {@link reseat} is the tripwire: adding a
      // record that points into this stream stops it compiling, which is what
      // brings whoever adds it back to this line.
      if (record._tag === 'Compacted') yield* flush;

      pending.push(reseat(record, reseated));
      sources.push(offset);
    }

    yield* flush;
  });

/**
 * A copied record, with its offset-valued pointers made to mean the same thing
 * in the stream it is being copied into.
 *
 * Exhaustive on purpose, with no `default` that passes an unknown case
 * through. Every offset in this union is a pointer into some stream, and a
 * copy moves records between streams — so a new record case carrying one has
 * to decide here what it means over there. The `never` binding below is what
 * makes that decision unavoidable rather than a silent corruption of a field
 * nobody remembered. {@link fork} states the two live cases and why they are
 * handled differently.
 */
const reseat = (
  record: ConversationRecord.Record,
  reseated: ReadonlyMap<LogOffset.Offset, LogOffset.Offset>,
): ConversationRecord.Record => {
  switch (record._tag) {
    case 'Compacted':
      return {
        ...record,
        // A miss falls back to START, which is the same tolerance
        // `AgentHistory`'s `keptFrom` already shows a `firstKept` naming a
        // record that is not there: keep the summary, keep nothing verbatim
        // before it. START itself misses and maps to itself, correctly — no
        // record is ever written at the sentinel.
        firstKept: reseated.get(record.firstKept) ?? LogOffset.START,
      };

    case 'SignalReceived':
      // Not a rewrite — there is no offset in the fork's signal stream that
      // corresponds to one in the ancestor's. See {@link fork}.
      return { ...record, at: LogOffset.START };

    case 'RunStarted':
    case 'Text':
    case 'ToolCall':
    case 'ToolOutcome':
    case 'TurnFinished':
    case 'BranchedFrom':
    case 'Completed':
    case 'ChildSession':
    case 'Signal':
    case 'RunSettled':
      return record;

    default: {
      const unreachable: never = record;
      return unreachable;
    }
  }
};

/**
 * Map key for a tool outcome, keyed by tool name and call id together.
 *
 * The separator is U+001F UNIT SEPARATOR, the ASCII control character whose
 * defined meaning is exactly this: joining fields into one datum. It is used
 * rather than a printable character because it cannot occur in either
 * component, so no pair of distinct (name, id) can collide by containing the
 * separator itself.
 *
 * It is deliberately **not** U+0000. A NUL anywhere in a source file makes
 * every byte-oriented tool classify the file as binary: `file(1)` reports it
 * as `data`, and `grep` and `diff` silently produce no output for the whole
 * file — no error and no "binary file matches" line. A search for a symbol
 * that is plainly present comes back empty, which reads as absence.
 */
const settledKey = (name: string, toolCallId: string): string =>
  `${name}\u001f${toolCallId}`;

/**
 * Tool outcomes belonging to a run that started and never settled.
 *
 * The gate on resuming dispatch, and the reason `RunSettled` exists. Both
 * `RunStarted` and `RunSettled` clear the map, so what survives the scan is
 * exactly the outcomes recorded after the last `RunStarted` when no
 * `RunSettled` followed it — a crash. A conversation whose last run finished
 * yields an empty map, and dispatch behaves as it always did.
 *
 * Given the **active path**, not the log. A crashed run the conversation has
 * since branched away from is not a run this one is recovering; its outcomes
 * belong to tool calls the new prompt does not contain, and offering them
 * would hand the model results for questions it never asked. The caller
 * filters rather than this function, so that the two full-log folds nearby are
 * visibly a different decision and not an omission.
 */
const unsettledOutcomes = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyMap<string, Settled> => {
  const outcomes = new Map<string, Settled>();
  let running = false;

  for (const { record } of history) {
    switch (record._tag) {
      case 'RunStarted':
        outcomes.clear();
        running = true;
        break;
      case 'RunSettled':
        outcomes.clear();
        running = false;
        break;
      case 'ToolOutcome':
        if (running) {
          outcomes.set(settledKey(record.name, record.id), {
            outcome: record.outcome,
            result: record.result,
          });
        }
        break;
      default:
        break;
    }
  }

  return outcomes;
};

/**
 * The furthest signal offset this conversation has recorded taking.
 *
 * The **whole log**, and this is the row of `branch.ts`'s table that is worth
 * being careful about. A `SignalReceived` says a steer was delivered to a
 * running agent, which then acted on it — a fact about the world, not a claim
 * the conversation can withdraw by branching. If this were scoped to the
 * active path, branching away from the turn that took a steer would rewind the
 * cursor past it, and the next run would drain that steer from the signal
 * stream a second time and inject an instruction the agent has already
 * followed. Delivery is at-least-once by design; this keeps "at least" from
 * quietly becoming "every time anyone edits an earlier message".
 */
const deliveredThrough = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): LogOffset.Offset => {
  let at = LogOffset.START;
  for (const { record } of history) {
    if (record._tag === 'SignalReceived' && LogOffset.isAfter(record.at, at)) {
      at = record.at;
    }
  }
  return at;
};

export interface Options {
  /** Agent name, written into `RunStarted`. */
  readonly agent: string;
  /** The run's input, written into `RunStarted` as prompt messages. */
  readonly input: Prompt.RawInput;
}

/**
 * Record a run's events into the session's conversation.
 *
 * The returned stream emits exactly what it was given, and — unlike the
 * version this replaces — does not change its requirement channel either: the
 * session already holds the store. A log write that fails becomes a
 * **defect**, not a failure, which is the same decision `@sunfall/vesper-durable`
 * makes for a checkpoint write and for the same reason: this is
 * infrastructure, not something the model did, and no retry policy written
 * for provider errors should absorb it. Continuing past a failed append would
 * produce a run whose result exists and whose history does not.
 *
 * The one exception is the settlement record, which is written from a
 * finalizer and cannot report anything to anyone. See {@link settle}.
 */
export const record = <Tools extends Record<string, Tool.Any>, E, R>(
  session: Session,
  options: Options,
  events: Stream.Stream<AgentEvents.Event<Tools>, E, R>,
): Stream.Stream<AgentEvents.Event<Tools>, E, R> =>
  Stream.unwrap(
    Effect.gen(function* () {
      yield* session.append([
        {
          _tag: 'RunStarted',
          agent: options.agent,
          // Messages rather than the raw input: `RawInput` is a string, a
          // `Prompt`, or an array of messages, and a reader should not have
          // to re-implement that normalisation to find out what was asked.
          prompt: Prompt.make(options.input).content,
        },
      ]);

      const pending: Pending = {
        step: 0,
        text: '',
        steps: 0,
        usage: { input: 0, output: 0 },
        completed: false,
        cancelled: false,
      };

      return Stream.tap(events, (event) =>
        // Compaction is the one event whose record cannot be built from the
        // event alone: it names a position in the log, and only the log knows
        // positions. Everything else is a pure function of the event and what
        // the sink has seen.
        event._tag === 'Compacted'
          ? compaction(session, pending, event)
          : session.append(recordsFor(pending, event)),
      ).pipe(Stream.onExit((exit) => settle(session, pending, exit)));
    }),
  );

/**
 * Write down that history was replaced, and what it was replaced by.
 *
 * Two appends rather than one, in this order and for this reason: any text the
 * model had produced and not yet flushed is part of the history compaction
 * just summarized, so it has to be *in* the log before the boundary is
 * resolved against it. Resolving first would point `firstKept` at a log that
 * is one record short of the conversation the loop compacted.
 *
 * `keptMessages` arrives as a count because that is all the loop can supply —
 * compaction runs against `Chat`'s in-memory history, which carries no record
 * identity — and `AgentHistory.boundaryFor` is what turns it back into a
 * position. It has to be that function and not a private one here: the
 * boundary is only meaningful in terms of the messages `messagesFrom`
 * rebuilds, so the writer and the reader have to be reading the same
 * definition.
 */
const compaction = (
  session: Session,
  pending: Pending,
  event: Extract<AgentEvents.Lifecycle, { readonly _tag: 'Compacted' }>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* session.append(flush(pending));

    const recorded = yield* session.recorded;

    yield* session.append([
      {
        _tag: 'Compacted',
        step: event.step,
        summary: event.summary,
        firstKept: AgentHistory.boundaryFor(recorded, event.keptMessages),
        summarizedMessages: event.summarizedMessages,
        keptMessages: event.keptMessages,
      },
    ]);
  });

/**
 * Write down how the run ended, including the ways that end no stream.
 *
 * Uninterruptible, because the interesting case is an interrupted run and a
 * finalizer that is itself interrupted records nothing about it.
 *
 * Failures here are swallowed after being logged, which is the opposite of
 * every other write in this file and is the only defensible option. There is
 * no one left to fail to: the stream has ended, its consumer has its value or
 * its error, and turning a settle-time store failure into a defect would
 * replace whatever actually went wrong with a complaint about the log. What
 * it leaves behind is a `RunStarted` with no `RunSettled` — which is exactly
 * the orphan shape a reader is told to look for, and which the resuming
 * dispatch treats conservatively by re-offering that run's tool outcomes.
 */
const settle = (
  session: Session,
  pending: Pending,
  exit: Exit.Exit<unknown, unknown>,
): Effect.Effect<void> =>
  Effect.suspend(() => {
    const settlement: ConversationRecord.Record = {
      _tag: 'RunSettled',
      ...outcomeOf(pending, exit),
      steps: pending.steps,
      usage: pending.usage,
    };

    return session.append([settlement]).pipe(
      Effect.catchCause((cause) =>
        Effect.logError(
          `Conversation ${session.conversationId} could not record how its run settled`,
          cause,
        ),
      ),
      Effect.uninterruptible,
    );
  });

const outcomeOf = (
  pending: Pending,
  exit: Exit.Exit<unknown, unknown>,
): { readonly outcome: SettledOutcome; readonly detail: string } => {
  if (Exit.isFailure(exit)) {
    return Cause.hasInterrupts(exit.cause)
      ? { outcome: 'interrupted', detail: 'the run was interrupted' }
      : { outcome: 'failure', detail: Cause.pretty(exit.cause) };
  }
  if (pending.cancelled) {
    return { outcome: 'cancelled', detail: 'a cancel signal ended the run' };
  }
  if (pending.completed) {
    return { outcome: 'success', detail: '' };
  }
  // The stream ended cleanly without a `Completed` event, which the loop
  // never does on its own: a consumer took a fixed number of events and
  // walked away. The run did not finish, and saying so is what stops the
  // record from claiming a success nobody got.
  return {
    outcome: 'interrupted',
    detail: 'the event stream was abandoned before the run completed',
  };
};

type SettledOutcome = ConversationRecord.RecordOf<'RunSettled'>['outcome'];

/** What the sink has seen so far: text awaiting a flush, and how it ended. */
interface Pending {
  step: number;
  text: string;
  steps: number;
  usage: Stop.Usage;
  completed: boolean;
  cancelled: boolean;
}

const flush = (pending: Pending): ReadonlyArray<ConversationRecord.Record> => {
  if (pending.text === '') return [];
  const record: ConversationRecord.Record = {
    _tag: 'Text',
    step: pending.step,
    text: pending.text,
  };
  pending.text = '';
  return [record];
};

/**
 * What one event contributes to the log, coalescing text as it goes.
 *
 * Returns an empty array for the events that are not worth a row — turn
 * starts, text framing, reasoning, provider metadata — and `Stream.tap` skips
 * the append for those, so the common case (a text delta) costs a string
 * concatenation and nothing else.
 */
const recordsFor = <Tools extends Record<string, Tool.Any>>(
  pending: Pending,
  event: AgentEvents.Event<Tools>,
): ReadonlyArray<ConversationRecord.Record> => {
  switch (event._tag) {
    case 'TurnStarted':
      return [];
    case 'TurnFinished':
      pending.steps = event.step;
      pending.usage = event.usage;
      return [
        ...flush(pending),
        { _tag: 'TurnFinished', step: event.step, usage: event.usage },
      ];
    case 'Signalled':
      if (event.kind === 'cancel') pending.cancelled = true;
      return [
        // Flushed first, so a rebuilt prompt has the model's own words before
        // the instruction that redirected it rather than after.
        ...flush(pending),
        {
          _tag: 'SignalReceived',
          kind: event.kind,
          text: event.text,
          source: event.source,
          step: event.step,
          at: LogOffset.Offset.make(event.at),
        },
      ];
    case 'Completed':
      pending.completed = true;
      pending.steps = event.steps;
      pending.usage = event.usage;
      return [
        ...flush(pending),
        {
          _tag: 'Completed',
          text: event.text,
          steps: event.steps,
          usage: event.usage,
        },
      ];
    case 'Compacted':
      // Written by {@link compaction}, which needs the log's own offsets and
      // therefore cannot be a pure function of the event. Listed here so the
      // match stays exhaustive rather than defaulted — a new lifecycle case
      // should fail this switch, which is the whole reason it has no
      // `default`.
      return [];
    case 'Part':
      return partRecords(pending, event.step, event.part);
  }
};

// Read through the encoded shape, exactly as `agent.ts`'s `observe` does. The
// stream carries decoded parts, whose *field names* are identical for
// everything matched below except one: a decoded tool result carries both the
// handler's value (`result`) and the toolkit's encoding of it
// (`encodedResult`), and it is the latter that is written down. That is the
// form `Prompt` puts in front of the model, so it is the only one a resuming
// dispatch can serve back without changing what the model sees — and it is
// already JSON, so a success type holding a `Date` no longer fails the append
// on its way to storage.
const partRecords = <Tools extends Record<string, Tool.Any>>(
  pending: Pending,
  step: number,
  part: Response.StreamPart<Tools>,
): ReadonlyArray<ConversationRecord.Record> => {
  const encoded = part as Response.StreamPartEncoded;
  switch (encoded.type) {
    case 'text-delta':
      pending.step = step;
      pending.text += encoded.delta;
      return [];
    case 'tool-call':
      return [
        ...flush(pending),
        {
          _tag: 'ToolCall',
          step,
          id: encoded.id,
          name: encoded.name,
          params: encoded.params,
        },
      ];
    case 'tool-result':
      return [
        ...flush(pending),
        {
          _tag: 'ToolOutcome',
          step,
          id: encoded.id,
          name: encoded.name,
          outcome: encoded.isFailure ? 'failure' : 'success',
          result: (part as Response.ToolResultPart<string, unknown, unknown>)
            .encodedResult,
        },
      ];
    default:
      return [];
  }
};

const orDie = <A, R>(
  effect: Effect.Effect<A, LogStore.LogStoreError, R>,
): Effect.Effect<A, never, R> =>
  Effect.catchTag(effect, '@sunfall/vesper-log/LogStoreError', (error) =>
    Effect.die(
      new Error(
        `Conversation log ${error.operation} failed (${error.reason}) for ${error.path}: ${error.detail}` +
          (error.reason === 'encoding'
            ? ' — a tool parameter or result did not survive JSON encoding.'
            : ''),
        { cause: error },
      ),
    ),
  );

export * as AgentLog from './log.js';
