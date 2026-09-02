import { AttachmentStore } from '@sunfall/vesper-attachments/attachment-store';
import {
  type ConversationRecord,
  FORMAT_VERSION,
} from '@sunfall/vesper-log/record';
import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect } from 'effect';

import { AgentBranch } from '../branch.js';
import { CompatibilityError } from '../conversation-error.js';
import { AgentHistory } from '../history.js';
import type { Compatibility } from '../log.js';
import { PromptTransport } from '../prompt-transport.js';

// Conversation-format and revision validation: every retained durable
// definition identity is checked against the definition asking to continue
// the history, before a model or tool call and before the producer is fenced.

export interface PersistedCompatibility {
  readonly formatVersion?: number | undefined;
  readonly agent?: string | undefined;
  readonly agentRevision?: LogVocabulary.AgentRevision | undefined;
  /**
   * Absent on a record written before this field existed. Accepted as
   * compatible, deliberately: an unrevisioned digest gap in older history is
   * not itself grounds to reject a resume — only a same-revision digest that
   * actively disagrees is.
   */
  readonly agentDigest?: LogVocabulary.AgentDefinitionDigest | undefined;
}

/** Validate every retained durable definition identity against one authority. */
export const validateCompatibility = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
  expected: Compatibility,
): Effect.Effect<void, CompatibilityError> => {
  const identities: PersistedCompatibility[] = [];
  for (const { record } of history) {
    if (record._tag === 'RunStarted') {
      identities.push({
        formatVersion: record.formatVersion,
        agent: record.agent,
        agentRevision: record.agentRevision,
        agentDigest: record.agentDigest,
      });
    } else if (record._tag === 'Compacted') {
      identities.push(record);
    } else if (record._tag === 'RunSettled' && record.resume !== undefined) {
      identities.push(record.resume);
    }
  }

  // A newly-created stream, or a child stream containing only links/signals,
  // has no definition identity to compare yet. Any actual conversation state
  // without one is legacy history and must not be adopted silently.
  if (identities.length === 0) {
    const hasConversationState = history.some(
      ({ record }) =>
        record._tag !== 'ChildSession' && record._tag !== 'Signal',
    );
    return hasConversationState
      ? Effect.fail(
          compatibilityError(
            expected,
            {},
            'history predates explicit compatibility metadata',
          ),
        )
      : Effect.void;
  }

  for (const persisted of identities) {
    const problem =
      persisted.formatVersion === undefined ||
      persisted.agentRevision === undefined ||
      persisted.agent === undefined
        ? 'history predates explicit compatibility metadata'
        : persisted.formatVersion !== FORMAT_VERSION
          ? `conversation format ${String(persisted.formatVersion)} is unsupported; this release supports format ${String(FORMAT_VERSION)}`
          : persisted.agent !== expected.agent
            ? `history contains contradictory agent "${persisted.agent}", not "${expected.agent}"`
            : persisted.agentRevision !== expected.revision
              ? `history contains contradictory revision "${persisted.agentRevision}", not "${expected.revision}"`
              : // Same revision from here on. A digest absent on either side is
                // accepted as compatible — see `PersistedCompatibility.agentDigest`
                // and `Compatibility.digest` — so this only fires when both
                // sides know a digest and they disagree: the one case a
                // revision bump alone cannot catch a coding agent forgetting.
                expected.digest !== undefined &&
                  persisted.agentDigest !== undefined &&
                  persisted.agentDigest !== expected.digest
                ? `history was recorded under revision "${expected.revision}" with definition digest "${persisted.agentDigest}", but this definition's digest is "${expected.digest}"; bump revision when the agent definition changes`
                : undefined;
    if (problem !== undefined) {
      return Effect.fail(compatibilityError(expected, persisted, problem));
    }
  }
  return Effect.void;
};

export const validateCompatibilityInput = (
  compatibility: Compatibility,
): Effect.Effect<void, CompatibilityError> => {
  const problem =
    compatibility.agent.trim() === ''
      ? 'agent name must be non-empty'
      : compatibility.revision.trim() === ''
        ? 'revision must be non-empty'
        : undefined;
  return problem === undefined
    ? Effect.void
    : Effect.fail(compatibilityError(compatibility, {}, problem));
};

/** Validate untrusted durable prompts before they reach the synchronous fold. */
export const hydrateHistory = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
  attachmentStore: AttachmentStore.Interface | undefined,
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  PromptTransport.DecodeError | AttachmentStore.GetError
> =>
  attachmentStore === undefined
    ? Effect.succeed(history)
    : Effect.forEach(history, (envelope) =>
        envelope.record._tag !== 'RunStarted'
          ? Effect.succeed(envelope)
          : PromptTransport.decodeWithAttachments(envelope.record.prompt).pipe(
              Effect.provideService(AttachmentStore.Service, attachmentStore),
              Effect.map((prompt) => ({
                // Envelope is decoded data, not a class instance. The public
                // interface intentionally merges with its schema value.
                // oxlint-disable-next-line typescript/no-misused-spread
                ...envelope,
                record: {
                  ...envelope.record,
                  // Keep hydrated bytes in the in-memory history. The durable
                  // log remains content-addressed; this view is what the
                  // synchronous prompt rebuild hands to the model.
                  prompt,
                },
              })),
            ),
      );

export const validatePromptHistory = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
  expected: Compatibility,
  attachmentStore?: AttachmentStore.Interface,
): Effect.Effect<void, CompatibilityError> =>
  Effect.gen(function* () {
    for (const { record } of AgentBranch.activePath(history)) {
      if (record._tag === 'RunStarted') {
        const decode =
          attachmentStore === undefined
            ? PromptTransport.decodeMessages(record.prompt)
            : PromptTransport.decodeMessagesWithAttachments(record.prompt).pipe(
                Effect.provideService(AttachmentStore.Service, attachmentStore),
              );
        yield* decode;
      }
    }
    yield* Effect.try({
      try: () => AgentHistory.messagesFrom(history),
      catch: (cause) =>
        new PromptTransport.DecodeError({
          message: `Malformed persisted prompt messages: ${
            cause instanceof Error ? cause.message : String(cause)
          }`,
        }),
    });
  }).pipe(
    Effect.asVoid,
    Effect.mapError((error) =>
      compatibilityError(
        expected,
        {},
        `malformed persisted prompt: ${error.message}`,
      ),
    ),
  );

/** An actionable error for incompatible durable history. */
export const compatibilityError = (
  expected: Compatibility,
  persisted: PersistedCompatibility,
  problem: string,
): CompatibilityError =>
  new CompatibilityError({
    message:
      `Cannot resume durable conversation: ${problem}. ` +
      'Use the matching agent definition or explicitly migrate the history and its compatibility metadata.',
    expectedAgent: expected.agent,
    expectedRevision: expected.revision,
    ...(expected.digest === undefined
      ? {}
      : { expectedDigest: expected.digest }),
    ...(persisted.formatVersion === undefined
      ? {}
      : { persistedFormat: persisted.formatVersion }),
    ...(persisted.agent === undefined
      ? {}
      : { persistedAgent: persisted.agent }),
    ...(persisted.agentRevision === undefined
      ? {}
      : { persistedRevision: persisted.agentRevision }),
    ...(persisted.agentDigest === undefined
      ? {}
      : { persistedDigest: persisted.agentDigest }),
  });
