import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Schema } from 'effect';

/** A durable conversation cannot be opened by this agent definition. */
export class CompatibilityError extends Schema.TaggedError<CompatibilityError>(
  '@sunfall/vesper-agent/CompatibilityError',
)('CompatibilityError', {
  message: Schema.String,
  expectedAgent: Schema.String,
  expectedRevision: Schema.String,
  persistedFormat: Schema.optionalKey(Schema.Natural),
  persistedAgent: Schema.optionalKey(Schema.String),
  persistedRevision: Schema.optionalKey(LogVocabulary.AgentRevision),
}) {}

/** A branch or fork boundary would detach a wait from its workflow owner. */
export class SuspendedConversationError extends Schema.TaggedError<SuspendedConversationError>(
  '@sunfall/vesper-agent/SuspendedConversationError',
)('SuspendedConversationError', {
  message: Schema.String,
  conversationId: LogVocabulary.ConversationId,
  toolCallId: LogVocabulary.ToolCallId,
  wait: Schema.String,
}) {}
