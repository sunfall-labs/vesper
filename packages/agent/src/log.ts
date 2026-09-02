import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import {
  type ConversationRecord,
  FORMAT_VERSION,
} from '@sunfall/vesper-log/record';
import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, type Option, type Stream } from 'effect';
import type { Crypto } from 'effect';
import { Prompt } from 'effect/unstable/ai';

import { AgentBranch } from './branch.js';
import type {
  CompatibilityError,
  DurabilityError,
  SuspendedConversationError,
} from './conversation-error.js';
import { AgentHistory as AgentHistoryRuntime } from './internal/history.js';
import {
  compatibilityError,
  validateCompatibility,
  validateCompatibilityInput,
} from './internal/compatibility.js';
import { pathFor } from './internal/conversation-stream.js';
import { forkIdentity } from './internal/fork-seed.js';
import { readAll } from './internal/resume-read.js';
import {
  openWith,
  type SessionTypeId,
  validateSuspendedBoundary,
} from './internal/session-open.js';

export {
  CompatibilityError,
  DurabilityError,
  SuspendedConversationError,
} from './conversation-error.js';
import type * as RecoveryState from './recovery.js';
import type { Stop } from './stop.js';
import type { RecordingPolicyRuntime } from './recording-policy-runtime.js';
import * as RecordingSink from './recording-sink.js';

// The public `AgentLog` surface: stream addressing, the `Session` value, and
// the `open`/`fork` claims. The machinery lives in `internal/` —
// `conversation-stream` (addressing), `resume-read` (bounded history reads),
// `compatibility` (format and revision validation), `fork-seed` (fork
// identity and prefix copying), and `session-open` (the claim itself) — and
// is re-exported here so `AgentLog.*` remains the one spelling consumers use.

// Stream addressing is defined beside the claim machinery that uses it and
// re-exported here, so the writer and `Conversation.follow` still agree on
// one definition.
export {
  childIdFor,
  follow,
  pathFor,
  snapshot,
} from './internal/conversation-stream.js';

// Recovery types live in the private recovery module and are re-exported below
// so AgentLog's public type shape remains stable without exposing the Module.
/** How a tool call ended, as a previous run recorded it. */
export type Settled = RecoveryState.Settled;
/** What an orphaned run durably established about a tool call. */
export type Recovery = RecoveryState.Recovery;
/** An orphaned handler start and the provider call that originally caused it. */
export type IndeterminateToolCall = RecoveryState.IndeterminateToolCall;
/** A deliberately suspended call and its external wait identity. */
export type SuspendedToolCall = RecoveryState.SuspendedToolCall;
/** What an external actor durably decided for one wait's token. */
export type CompletedWait = RecoveryState.CompletedWait;

/** A signal this run has taken delivery of. */
export interface Delivered {
  readonly kind: 'steer' | 'cancel';
  readonly text: string;
  readonly source: string;
  /** The signal's offset in the signal stream. */
  readonly at: LogOffset.Offset;
}

export interface SignalDrain {
  readonly signals: ReadonlyArray<Delivered>;
  /** More signals remain after this bounded page. */
  readonly backlog: boolean;
}

/** Where a run picks a conversation up, when not at its end. */
export interface OpenOptions {
  /** The agent definition that will continue this conversation. */
  readonly compatibility: Compatibility;
  /**
   * Continue from this record instead of from the conversation's last one.
   *
   * The offset is **inclusive**: the record it names is the last one the new
   * run inherits, and everything after it becomes an abandoned branch. The
   * marker is written as part of claiming — see {@link open} — which is what
   * lets every reader below stay unaware that branching exists.
   */
  readonly branchFrom?: LogOffset.Offset;
  /** Explicitly re-enter a suspended provider call with a fresh wait token. */
  readonly pendingWait?: 'restart';
}

/**
 * Durable identity required before a definition may continue history.
 *
 * `digest` is optional here, deliberately: `Compatibility` is also the shape
 * a caller builds by hand against the lower-level `open`/`fork` entry points
 * (this module's own tests do), and those callers may have no compiled
 * `Agent.Instance` to read a digest from. Every `Agent`-driven call site —
 * `agent.ts`'s `stream`/`run` protocol, `Conversation`, subagent delegation —
 * always supplies the real `Agent.digest`. When `digest` is absent here, the
 * open-time comparison in `internal/compatibility.ts` skips the digest check
 * entirely rather than treating the absence as a mismatch.
 */
export interface Compatibility {
  readonly agent: string;
  readonly revision: LogVocabulary.AgentRevision;
  readonly digest?: LogVocabulary.AgentDefinitionDigest | undefined;
}

/** Ensure a claimed session is handed only to the definition that claimed it. */
export const assertCompatible = (
  session: Session,
  expected: Compatibility,
): Effect.Effect<void, CompatibilityError> =>
  session.compatibility.agent === expected.agent &&
  session.compatibility.revision === expected.revision &&
  // Absent on either side compares equal: a caller without a digest (see
  // `Compatibility`'s doc comment) never fences itself out of its own claim.
  (session.compatibility.digest === expected.digest ||
    session.compatibility.digest === undefined ||
    expected.digest === undefined)
    ? Effect.void
    : Effect.fail(
        compatibilityError(
          expected,
          {
            formatVersion: FORMAT_VERSION,
            agent: session.compatibility.agent,
            agentRevision: session.compatibility.revision,
            ...(session.compatibility.digest === undefined
              ? {}
              : { agentDigest: session.compatibility.digest }),
          },
          `session was claimed for agent "${session.compatibility.agent}" revision "${session.compatibility.revision}"`,
        ),
      );

export interface ChildOptions {
  /** The delegation tool call the child answers. */
  readonly toolCallId: LogVocabulary.ToolCallId;
  /** The child agent's name. */
  readonly agent: string;
  readonly revision: LogVocabulary.AgentRevision;
  readonly digest?: LogVocabulary.AgentDefinitionDigest | undefined;
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
 * Appends expose {@link DurabilityError} as an ordinary typed failure, so
 * callers can distinguish durable infrastructure from model and application
 * failures. Reads retain the historic defect boundary because they back
 * synchronous views. Attachment persistence is opt-in through
 * `AttachmentStore.Service`; without it, prompts retain the inline transport.
 */
export interface Session {
  /** @internal Prevents structural fabrication outside this module. */
  readonly [SessionTypeId]: typeof SessionTypeId;
  readonly conversationId: LogVocabulary.ConversationId;
  readonly compatibility: Compatibility;

  /** Usage copied from an ancestor when this conversation was forked. */
  readonly inheritedUsage: Stop.Usage;

  /** Cumulative physical-log usage through the point this session opened. */
  readonly usage: Stop.Usage;

  /** Provider usage for the latest uncompacted completed turn. */
  readonly latestTurnUsage: Stop.Usage | undefined;

  /** Latest completed result on the active path, including anchored history. */
  readonly completed: ReturnType<typeof AgentHistoryRuntime.completedFrom>;

  /** How long teardown waits for this session's settlement append. */
  readonly settlementTimeoutMillis: number;

  /**
   * The records required to rebuild the live prompt when this run claimed it.
   *
   * Read once at {@link open} and held rather than re-read:
   * a second read would race this run's own appends and hand back a history
   * that includes what this run has already written. `AgentHistory` turns it
   * into a `Prompt`, which is how a run continues a conversation instead of
   * restarting it.
   *
   * Compacted conversations retain only the active live tail from the latest
   * compaction boundary. Cumulative physical facts are exposed separately by
   * {@link usage} and the session's durable signal cursor.
   *
   * A run claimed with `branchFrom` follows that marker while paging and does
   * not retain the marker or the abandoned physical range in this array.
   */
  /** @internal Raw recovery snapshot used to rebuild the prompt. */
  readonly history: ReadonlyArray<ConversationRecord.Envelope>;

  /** @internal Bounded aggregate suffix used to restore recorded state. */
  readonly stateHistory: ReadonlyArray<ConversationRecord.Envelope>;

  /**
   * The active records required to rebuild the conversation **now**, including
   * what this run has already written.
   *
   * The deliberate opposite of {@link history}, and the reason both exist. A
   * reader that must not see this run's own records reads `history`; a writer
   * that needs to point at a record *it just wrote* reads this. Only
   * compaction does — the `Compacted` record names the record its summary
   * stops replacing, and that record is usually one this run appended.
   *
   * A backwards active-path read that stops at the latest compaction boundary.
   * Uncompacted history remains proportional to the prompt it must rebuild.
   */
  /** @internal Raw active-path persistence view. */
  readonly recorded: Effect.Effect<ReadonlyArray<ConversationRecord.Envelope>>;

  /** @internal Append plumbing for the run recorder. */
  readonly append: (
    records: ReadonlyArray<ConversationRecord.Record>,
    timeoutMillis?: number,
  ) => Effect.Effect<void, DurabilityError>;

  /**
   * The state an **unsettled earlier run** recorded for this call: either a
   * completed outcome or a handler start with no outcome.
   *
   * Synchronous, because it is a lookup in an index built when the session
   * opened, and that timing is the safety property: everything in it predates
   * anything this run writes, so a run can never recover its own result.
   *
   * Empty unless the conversation's last `RunStarted` has no `RunSettled`
   * after it. A run that ended — successfully, in failure, or cancelled — has
   * nothing to recover, so its tool state is not offered to a later one.
   */
  /** @internal Recovery index for tool dispatch. */
  readonly recovery: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
  ) => Option.Option<Recovery>;

  /** @internal Orphaned calls in their original durable ToolCall order. */
  readonly pendingToolCalls: ReadonlyArray<IndeterminateToolCall>;

  /** @internal Indeterminate calls requiring an application decision. */
  readonly indeterminateToolCalls: ReadonlyArray<IndeterminateToolCall>;

  /** @internal Deliberately suspended calls, safe for workflow replay. */
  readonly suspendedToolCalls: ReadonlyArray<SuspendedToolCall>;

  /** @internal Corrupt recovery state that cannot be safely reconciled. */
  readonly recoveryCorruption: string | undefined;

  /** @internal Whether an external wait result is already durably audited. */
  readonly hasCompletedWait: (token: string) => boolean;

  /** @internal The durable decision recorded for one wait's token, if any. */
  readonly completedWait: (
    token: string,
  ) => Option.Option<RecoveryState.CompletedWait>;

  /** @internal Why an open tool call prevents this run from settling. */
  readonly pendingToolState: Effect.Effect<RecoveryState.PendingToolState>;

  /** @internal Whether a durable handler start still lacks an outcome. */
  readonly hasPendingToolCalls: Effect.Effect<boolean>;

  /** @internal Register work released after an outcome is durable. */
  readonly onToolSettled: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
    effect: Effect.Effect<void>,
  ) => void;

  /**
   * Signals that have arrived since the last drain.
   *
   * One page per call. A backlog larger than a page is delivered across
   * successive turn boundaries rather than in one burst, which is fine —
   * nothing here promises to deliver a backlog atomically — and it keeps the
   * drain a single bounded read.
   */
  /** @internal Bounded authoritative signal drain. */
  readonly drainSignalsBounded: (limit: number) => Effect.Effect<SignalDrain>;

  /**
   * Hint-only views of the next bounded signal page.
   *
   * Each change-feed tick re-reads from the authoritative delivery cursor but
   * never advances it. The opening tick closes the subscribe/read race. Only
   * {@link drainSignalsBounded} acknowledges records at a turn boundary.
   */
  /** @internal Hint-only signal watcher. */
  readonly signalPages: (
    limit: number,
  ) => Stream.Stream<SignalDrain, LogStore.LogStoreError>;

  /**
   * Record a delegation and open the child's own conversation.
   *
   * Writes the same `ChildSession` record into both logs: into this one as
   * "I delegated here", into the child's as its opening statement of whose
   * child it is. See the record's own documentation for why one record type
   * rather than two conventions.
   */
  /** @internal Open a recorded child session for delegation. */
  readonly child: (
    options: ChildOptions,
  ) => Effect.Effect<
    Session,
    | CompatibilityError
    | SuspendedConversationError
    | LogStore.LogStoreError
    | DurabilityError,
    Crypto.Crypto
  >;
}

/**
 * Claim a conversation and read back what a previous run left.
 *
 * `create` tolerates `conflict` — a second run on an existing conversation is
 * the normal case, not an error. Existing history is compatibility-validated
 * before `acquire`, because an incompatible reader has no right to fence its
 * writer. A compatible open then acquires normally, preserving the single
 * writer race: a second concurrent run fails on its next append rather than
 * interleaving two runs into one history.
 *
 * History is read backwards in two bounded views. Physical cumulative state
 * stops at the newest durable resume aggregate; prompt state follows branch
 * jumps and stops at the latest compaction's kept boundary. An uncompacted
 * prompt, a fork prefix, and compatible history without a resume aggregate
 * still read every record they require.
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
 * after the recovery fold ran would leave the crashed run the user branched
 * *away from* still holding the recovery index, and the new run would be
 * served tool results answering calls that are no longer in its prompt.
 */
export const open: (
  conversationId: LogVocabulary.ConversationId,
  options: OpenOptions,
) => Effect.Effect<
  Session,
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | DurabilityError,
  LogStore.Service | Crypto.Crypto
> = Effect.fn('AgentLog.open')(function* (conversationId, options) {
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
 * so a seeded prefix collapses to a single position; `compactionBoundary` would then
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
 *     ancestor offset to fork offset — see `internal/fork-seed.ts`'s
 *     `seedInto`, which flushes before each such record so the map is
 *     populated when it is read.
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
 * Fork provenance lives in the destination stream's identity. That makes an
 * empty or partially seeded destination distinguishable from an unrelated
 * conversation after a crash, without introducing a second persistence
 * mechanism. A retry with the same source and boundary resumes its copy;
 * every other non-empty destination is rejected.
 */
export const fork: (
  conversationId: LogVocabulary.ConversationId,
  at: LogOffset.Offset,
  forkConversationId: LogVocabulary.ConversationId,
  compatibility: Compatibility,
  pendingWait?: 'restart',
) => Effect.Effect<
  Session,
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | DurabilityError,
  LogStore.Service | Crypto.Crypto
> = Effect.fn('AgentLog.fork')(
  function* (
    conversationId,
    at,
    forkConversationId,
    compatibility,
    pendingWait,
  ) {
    yield* validateCompatibilityInput(compatibility);
    const store = yield* LogStore.Service;

    const ancestor = yield* readAll(store, pathFor(conversationId));
    // Cut first, then fold: this is exactly the sequence `activePath` would
    // return if a `BranchedFrom { at }` had been appended to the ancestor, which
    // is what makes a fork inherit the same prefix `branchFrom` would have.
    const prefix = AgentBranch.activePath(
      ancestor.filter((envelope) => !LogOffset.isAfter(envelope.offset, at)),
    );
    yield* validateCompatibility(prefix, compatibility);
    yield* validateSuspendedBoundary(conversationId, prefix, pendingWait);
    const identity = forkIdentity({
      sourceConversationId: conversationId,
      at,
      records: prefix.length,
      inheritedUsage: AgentHistoryRuntime.usageFrom(prefix),
    });

    return yield* openWith(store, forkConversationId, {
      seed: prefix,
      identity,
      compatibility,
      ...(pendingWait === undefined ? {} : { pendingWait }),
    });
  },
);

/** Attach a compiled persistence policy without changing the run's live values. */
export const withRecordingPolicy = (
  session: Session,
  recordingPolicy: RecordingPolicyRuntime.Runtime,
): Session => {
  const append: Session['append'] = (records) =>
    Effect.forEach(records, recordingPolicy.filter).pipe(
      Effect.flatMap(session.append),
    );
  return {
    ...session,
    append,
    child: (options) =>
      Effect.map(session.child(options), (child) =>
        withRecordingPolicy(child, recordingPolicy),
      ),
  };
};

export interface Options {
  /** Agent name, written into `RunStarted`. */
  readonly agent: string;
  readonly revision: LogVocabulary.AgentRevision;
  readonly digest?: LogVocabulary.AgentDefinitionDigest | undefined;
  /** The run's input, written into `RunStarted` as prompt messages. */
  readonly input: Prompt.RawInput;
}

/** Persist the effective input before any event from the run can escape. */
export const start = (
  session: Session,
  options: Options,
): Effect.Effect<void, DurabilityError> =>
  session.append([
    {
      _tag: 'RunStarted',
      agent: options.agent,
      agentRevision: options.revision,
      ...(options.digest === undefined ? {} : { agentDigest: options.digest }),
      formatVersion: FORMAT_VERSION,
      // `beforeTurn` has already run when this is called. Persisting here is
      // what makes reconstruction use the same input the provider saw.
      prompt: Prompt.make(options.input).content,
    },
  ]);

export const record = RecordingSink.record;
export const SETTLEMENT_TIMEOUT_MILLIS =
  RecordingSink.SETTLEMENT_TIMEOUT_MILLIS;
