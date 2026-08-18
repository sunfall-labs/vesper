import type { Agent } from '../packages/agent/dist/agent.js';
import type {
  CompatibilityError,
  SuspendedConversationError,
} from '../packages/agent/dist/conversation.js';
import type { AgentWorkflow } from '../packages/agent/dist/workflow.js';
import type { AgentMcp } from '../packages/mcp/dist/agent.js';
import type { LogVocabulary } from '../packages/log/dist/vocabulary.js';
import type { Tool } from 'effect/unstable/ai';

type Equal<Left, Right> =
  (<Type>() => Type extends Left ? 1 : 2) extends <Type>() => Type extends Right
    ? 1
    : 2
    ? true
    : false;
type Expect<Type extends true> = Type;
type IsAny<Type> = 0 extends 1 & Type ? true : false;

type RunAgentSuccess = Tool.Success<typeof AgentMcp.RunAgent>;

type RunAgentSuccessIsTyped = Expect<Equal<IsAny<RunAgentSuccess>, false>>;
type RunAgentSuccessIsResult = Expect<Equal<RunAgentSuccess, Agent.Result>>;
type SuspendedConversationIdIsBranded = Expect<
  Equal<
    SuspendedConversationError['conversationId'],
    LogVocabulary.ConversationId
  >
>;
type SuspendedToolCallIdIsBranded = Expect<
  Equal<SuspendedConversationError['toolCallId'], LogVocabulary.ToolCallId>
>;
type WaitConversationIdIsBranded = Expect<
  Equal<
    AgentWorkflow.WaitStateError['conversationId'],
    LogVocabulary.ConversationId
  >
>;
type PersistedRevisionIsBranded = Expect<
  Equal<
    CompatibilityError['persistedRevision'],
    LogVocabulary.AgentRevision | undefined
  >
>;

export type PublishedTypeAssertions =
  | RunAgentSuccessIsTyped
  | RunAgentSuccessIsResult
  | SuspendedConversationIdIsBranded
  | SuspendedToolCallIdIsBranded
  | WaitConversationIdIsBranded
  | PersistedRevisionIsBranded;
