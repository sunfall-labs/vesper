import { Schema } from 'effect';

export const ConversationId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand('@sunfall/vesper-log/ConversationId'),
);
export type ConversationId = typeof ConversationId.Type;

export const ToolCallId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand('@sunfall/vesper-log/ToolCallId'),
);
export type ToolCallId = typeof ToolCallId.Type;

export const AgentRevision = Schema.String.check(
  Schema.isPattern(/^\S(?:.*\S)?$/),
).pipe(Schema.brand('@sunfall/vesper-log/AgentRevision'));
export type AgentRevision = typeof AgentRevision.Type;

export const ProducerId = Schema.String.check(Schema.isMinLength(1)).pipe(
  Schema.brand('@sunfall/vesper-log/ProducerId'),
);
export type ProducerId = typeof ProducerId.Type;

export const Epoch = Schema.Natural.pipe(
  Schema.brand('@sunfall/vesper-log/Epoch'),
);
export type Epoch = typeof Epoch.Type;

export const ProducerSequence = Schema.Natural.pipe(
  Schema.brand('@sunfall/vesper-log/ProducerSequence'),
);
export type ProducerSequence = typeof ProducerSequence.Type;

export * as LogVocabulary from './vocabulary.js';
