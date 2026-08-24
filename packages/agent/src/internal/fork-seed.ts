import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Schema } from 'effect';

import type { DurabilityError } from '../conversation-error.js';
import type { Session } from '../log.js';
import type { Stop } from '../stop.js';

// Fork identity and prefix seeding: the durable identity string that marks a
// stream as a fork of another, and the copy that seeds a freshly claimed
// stream with an ancestor's prefix while reseating offset-valued pointers.

const FORK_IDENTITY_PREFIX = '@sunfall/vesper-agent/fork/v1:';

export interface ForkIdentity {
  readonly sourceConversationId: LogVocabulary.ConversationId;
  readonly at: LogOffset.Offset;
  readonly records: number;
  readonly inheritedUsage: Stop.Usage;
}

export const forkIdentity = (identity: ForkIdentity): string =>
  `${FORK_IDENTITY_PREFIX}${Schema.encodeSync(
    Schema.fromJsonString(ForkIdentitySchema),
  )([
    identity.sourceConversationId,
    identity.at,
    identity.records,
    identity.inheritedUsage.input,
    identity.inheritedUsage.output,
  ])}`;

const ForkIdentitySchema = Schema.Tuple([
  LogVocabulary.ConversationId,
  LogOffset.Offset,
  Schema.Natural,
  Schema.Natural,
  Schema.Natural,
]);

export const parseForkIdentity = (
  identity: string,
): ForkIdentity | undefined => {
  if (!identity.startsWith(FORK_IDENTITY_PREFIX)) {
    return undefined;
  }
  try {
    const value = Schema.decodeSync(Schema.fromJsonString(ForkIdentitySchema))(
      identity.slice(FORK_IDENTITY_PREFIX.length),
    );
    return {
      sourceConversationId: value[0],
      at: value[1],
      records: value[2],
      inheritedUsage: { input: value[3], output: value[4] },
    };
  } catch {
    return undefined;
  }
};

const ConversationRecordJson = Schema.fromJsonString(ConversationRecord.Record);
const encodeConversationRecordSync = Schema.encodeSync(ConversationRecordJson);
const encodeConversationRecord = Schema.encodeEffect(ConversationRecordJson);

export const ensureChildReference = (
  conversationId: LogVocabulary.ConversationId,
  history: ReadonlyArray<ConversationRecord.Envelope>,
  reference: ConversationRecord.RecordOf<'ChildSession'>,
  append: Session['append'],
): Effect.Effect<void, DurabilityError> => {
  const parentSide = conversationId === reference.parentConversationId;
  const links = history.flatMap(({ record }) =>
    record._tag === 'ChildSession' &&
    (parentSide
      ? record.parentConversationId === conversationId &&
        record.toolCallId === reference.toolCallId
      : record.childConversationId === conversationId)
      ? [record]
      : [],
  );
  const encodedReference = encodeConversationRecordSync(reference);
  if (
    links.some(
      (link) => encodeConversationRecordSync(link) === encodedReference,
    )
  ) {
    return Effect.void;
  }
  if (links.length > 0) {
    return Effect.die(
      new Error(
        `Conversation ${conversationId} has a conflicting child-session link for tool call ${reference.toolCallId}`,
      ),
    );
  }
  return append([reference]);
};

/**
 * Copy an ancestor's prefix into a freshly claimed stream.
 *
 * Writes in as few batches as the pointers allow. A run of records with no
 * offset-valued pointer is one append; a `Compacted` forces a flush before it,
 * because it is rewritten through a map that only holds offsets already
 * written. Compaction is rare, so this is one append for almost every fork.
 *
 * The index alignment is what makes the map cheap and is why this runs inside
 * `openWith`'s claim: the stream was created empty by `fork` and
 * this producer is the only writer, so the nth envelope read back is the nth
 * record copied.
 */
export const seedInto = (
  prefix: ReadonlyArray<ConversationRecord.Envelope>,
  append: Session['append'],
  recorded: Effect.Effect<
    ReadonlyArray<ConversationRecord.Envelope>,
    LogStore.LogStoreError
  >,
): Effect.Effect<void, LogStore.LogStoreError | DurabilityError> =>
  Effect.gen(function* () {
    const reseated = new Map<LogOffset.Offset, LogOffset.Offset>();
    let written = yield* recorded;

    for (
      let index = 0;
      index < Math.min(written.length, prefix.length);
      index += 1
    ) {
      const source = prefix[index];
      const destination = written[index];
      if (source === undefined || destination === undefined) {
        throw new Error(
          `Fork destination seed is missing record ${String(index)}`,
        );
      }
      const expected = reseat(source.record, reseated);
      const destinationEncoded = yield* encodeConversationRecord(
        destination.record,
      ).pipe(Effect.orDie);
      const expectedEncoded = yield* encodeConversationRecord(expected).pipe(
        Effect.orDie,
      );
      if (destinationEncoded !== expectedEncoded) {
        return yield* Effect.die(
          new Error(`Fork destination seed differs at record ${String(index)}`),
        );
      }
      reseated.set(source.offset, destination.offset);
    }

    // A matching fork identity may be opened again after its independent run
    // has appended beyond the copied prefix. Validate the entire seed above,
    // then leave those genuine destination records untouched.
    if (written.length >= prefix.length) {
      return;
    }

    let copied = written.length;
    let pending: Array<ConversationRecord.Record> = [];

    const flush = Effect.gen(function* () {
      if (pending.length === 0) {
        return;
      }
      yield* append(pending);
      pending = [];

      written = yield* recorded;
      while (copied < written.length) {
        const source = prefix[copied];
        const destination = written[copied];
        if (source === undefined || destination === undefined) {
          throw new Error(
            `Fork destination seed is missing record ${String(copied)}`,
          );
        }
        reseated.set(source.offset, destination.offset);
        copied += 1;
      }
    });

    for (let index = written.length; index < prefix.length; index += 1) {
      const source = prefix[index];
      if (source === undefined) {
        throw new Error(`Fork source seed is missing record ${String(index)}`);
      }
      const { record } = source;
      // Flushed *before* any record whose pointer is rewritten through the
      // map, so that the offsets it may name are in it. `Compacted` is the
      // only such case today, and {@link reseat} is the tripwire: adding a
      // record that points into this stream stops it compiling, which is what
      // brings whoever adds it back to this line.
      if (record._tag === 'Compacted') {
        yield* flush;
      }

      pending.push(reseat(record, reseated));
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
 * nobody remembered. `fork` states the two live cases and why they are
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
      // corresponds to one in the ancestor's. See `fork`.
      return { ...record, at: LogOffset.START };

    case 'RunSettled':
      return record.resume === undefined
        ? record
        : {
            ...record,
            resume: { ...record.resume, signalCursor: LogOffset.START },
          };

    case 'RunStarted':
    case 'Text':
    case 'ToolCall':
    case 'ToolStarted':
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
    case 'ToolOutcome':
    case 'StateCheckpoint':
    case 'CodeStateCheckpoint':
    case 'TurnFinished':
    case 'BranchedFrom':
    case 'Completed':
    case 'ChildSession':
    case 'Signal':
      return record;

    default: {
      const unreachable: never = record;
      return unreachable;
    }
  }
};
