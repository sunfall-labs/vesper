import {
  AgentDefinitionDigest,
  AgentRevision,
  ConversationId,
  ToolCallId,
} from '@sunfall/vesper-log/vocabulary';
import { Schema } from 'effect';

const CompatibilityErrorFields: {
  readonly message: typeof Schema.String;
  readonly expectedAgent: typeof Schema.String;
  readonly expectedRevision: typeof Schema.String;
  readonly expectedDigest: Schema.optionalKey<typeof AgentDefinitionDigest>;
  readonly persistedFormat: ReturnType<
    typeof Schema.optionalKey<Schema.Natural>
  >;
  readonly persistedAgent: ReturnType<typeof Schema.optionalKey<Schema.String>>;
  readonly persistedRevision: Schema.optionalKey<typeof AgentRevision>;
  readonly persistedDigest: Schema.optionalKey<typeof AgentDefinitionDigest>;
} = {
  message: Schema.String,
  expectedAgent: Schema.String,
  expectedRevision: Schema.String,
  expectedDigest: Schema.optionalKey(AgentDefinitionDigest),
  persistedFormat: Schema.optionalKey(Schema.Natural),
  persistedAgent: Schema.optionalKey(Schema.String),
  persistedRevision: Schema.optionalKey(AgentRevision),
  persistedDigest: Schema.optionalKey(AgentDefinitionDigest),
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

const ApprovalResolutionErrorFields: {
  readonly message: typeof Schema.String;
  readonly conversationId: typeof ConversationId;
  readonly toolCallId: typeof ToolCallId;
  readonly reason: Schema.Literals<['not_found', 'already_resolved']>;
} = {
  message: Schema.String,
  conversationId: ConversationId,
  toolCallId: ToolCallId,
  reason: Schema.Literals(['not_found', 'already_resolved']),
};

/**
 * A `Conversation.resolveApproval` call could not be applied.
 *
 * `not_found` covers both an unknown tool call id and one that was never
 * durably suspended on a `needsApproval` gate; both are indistinguishable to
 * a caller and neither has a decision to make idempotent. `already_resolved`
 * is the double-resolve case: a typed conflict rather than a silent no-op,
 * so a second, differing decision cannot be mistaken for having taken
 * effect.
 */
export class ApprovalResolutionError extends Schema.TaggedError<ApprovalResolutionError>(
  '@sunfall/vesper-agent/ApprovalResolutionError',
)('ApprovalResolutionError', ApprovalResolutionErrorFields) {}

/** An externally answered interaction could not be completed. */
const InteractionResolutionErrorFields: {
  readonly message: typeof Schema.String;
  readonly conversationId: typeof ConversationId;
  readonly toolCallId: typeof ToolCallId;
  readonly interaction: typeof Schema.String;
  readonly reason: Schema.Literals<['not_found', 'already_resolved']>;
} = {
  message: Schema.String,
  conversationId: ConversationId,
  toolCallId: ToolCallId,
  interaction: Schema.String,
  reason: Schema.Literals(['not_found', 'already_resolved']),
};

export class InteractionResolutionError extends Schema.TaggedError<InteractionResolutionError>(
  '@sunfall/vesper-agent/InteractionResolutionError',
)('InteractionResolutionError', InteractionResolutionErrorFields) {}

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
