import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Tail } from '@sunfall/vesper-log/tail';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Stream } from 'effect';

// Stream addressing for conversations: where a conversation's records live,
// how they are read back, and how a child's stream is named. `log.ts`
// re-exports all four, so `AgentLog.pathFor` remains the one public spelling.

/**
 * Where a conversation's stream lives.
 *
 * One function, exported, because the writer and `Conversation.follow`
 * have to agree and a convention that lives in two places is a convention
 * that eventually differs. The prefix leaves room for streams that are not
 * conversations — `signals/<id>`, and a per-agent identity stream if one ever
 * lands — without a collision that would only surface as one log interleaved
 * into another.
 */
export const pathFor = (conversationId: LogVocabulary.ConversationId): string =>
  `conversations/${conversationId}`;

/** @internal Replay existing records and follow future appends. */
export const follow = (
  conversationId: LogVocabulary.ConversationId,
  after: LogOffset.Offset = LogOffset.START,
): Stream.Stream<
  ConversationRecord.Envelope,
  LogStore.LogStoreError,
  LogStore.Service
> => Tail.from(pathFor(conversationId), after);

/** @internal Read a finite snapshot of the records currently stored. */
export const snapshot = (
  conversationId: LogVocabulary.ConversationId,
  after: LogOffset.Offset = LogOffset.START,
): Stream.Stream<
  ConversationRecord.Envelope,
  LogStore.LogStoreError,
  LogStore.Service
> =>
  Stream.unwrap(
    Effect.map(LogStore.Service, (store) => {
      const path = pathFor(conversationId);
      const page = (
        cursor: LogOffset.Offset,
      ): Stream.Stream<ConversationRecord.Envelope, LogStore.LogStoreError> =>
        Stream.unwrap(
          Effect.map(store.read(path, { after: cursor }), (read) =>
            Stream.concat(
              Stream.fromIterable(read.records),
              read.upToDate ? Stream.empty : page(read.cursor),
            ),
          ),
        );
      return page(after);
    }),
  );

/**
 * A conversation id for a child, derived rather than minted.
 *
 * Deterministic on purpose. A random id would give a recovered run a *second*
 * child conversation for the same delegation, leaving the first orphaned with
 * no reference to it from anywhere; deriving it means the retry lands on the
 * same stream, so the child's own log — and its own resumption — carries on
 * rather than starting again beside itself. Tool call ids are unique within a
 * conversation. Both inputs are arbitrary strings, so separators alone are
 * not sufficient: `a/b` + `c` and `a` + `b/c` used to name the same child.
 * The parent's UTF-16 length makes the boundary unambiguous without assuming
 * anything about provider ids, including their Unicode normalization.
 */
export const childIdFor = (
  parentConversationId: LogVocabulary.ConversationId,
  toolCallId: LogVocabulary.ToolCallId,
): LogVocabulary.ConversationId =>
  LogVocabulary.ConversationId.make(
    `child-v1:${String(parentConversationId.length)}:${parentConversationId}${toolCallId}`,
  );
