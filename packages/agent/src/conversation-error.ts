import {
  AgentRevision,
  ConversationId,
  ToolCallId,
} from '@sunfall/vesper-log/vocabulary';
import { Schema } from 'effect';

const CompatibilityErrorFields: {
  readonly message: typeof Schema.String;
  readonly expectedAgent: typeof Schema.String;
  readonly expectedRevision: typeof Schema.String;
  readonly persistedFormat: ReturnType<
    typeof Schema.optionalKey<Schema.Natural>
  >;
  readonly persistedAgent: ReturnType<typeof Schema.optionalKey<Schema.String>>;
  readonly persistedRevision: Schema.optionalKey<typeof AgentRevision>;
} = {
  message: Schema.String,
  expectedAgent: Schema.String,
  expectedRevision: Schema.String,
  persistedFormat: Schema.optionalKey(Schema.Natural),
  persistedAgent: Schema.optionalKey(Schema.String),
  persistedRevision: Schema.optionalKey(AgentRevision),
};

const SuspendedConversationErrorFields: {
  readonly message: typeof Schema.String;
  readonly conversationId: typeof ConversationId;
  readonly toolCallId: typeof ToolCallId;
  readonly wait: typeof Schema.String;
} = {
  message: Schema.String,
  conversationId: ConversationId,
  toolCallId: ToolCallId,
  wait: Schema.String,
};

/** A durable conversation cannot be opened by this agent definition. */
export class CompatibilityError extends Schema.TaggedError<CompatibilityError>(
  '@sunfall/vesper-agent/CompatibilityError',
)('CompatibilityError', CompatibilityErrorFields) {}

/** A branch or fork boundary would detach a wait from its workflow owner. */
export class SuspendedConversationError extends Schema.TaggedError<SuspendedConversationError>(
  '@sunfall/vesper-agent/SuspendedConversationError',
)('SuspendedConversationError', SuspendedConversationErrorFields) {}

/** A conversation checkpoint could not be made durable. */
export class DurabilityError extends Schema.TaggedError<DurabilityError>(
  '@sunfall/vesper-agent/DurabilityError',
)('DurabilityError', {
  source: Schema.Literals(['log', 'attachment', 'timeout']),
  operation: Schema.String,
  reason: Schema.String,
  detail: Schema.String,
  cause: Schema.Defect(),
}) {}
