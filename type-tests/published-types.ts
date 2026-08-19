import type {
  CompatibilityError,
  SuspendedConversationError,
} from '../packages/agent/dist/conversation.js';
import type { DynamicToolkit } from '../packages/agent/dist/dynamic-toolkit.js';
import type { AgentWorkflow } from '../packages/agent/dist/workflow.js';
import type { Mcp } from '../packages/mcp/dist/mcp.js';
import type { LogVocabulary } from '../packages/log/dist/vocabulary.js';
import type { Redacted } from 'effect';

type Equal<Left, Right> =
  (<Type>() => Type extends Left ? 1 : 2) extends <Type>() => Type extends Right
    ? 1
    : 2
    ? true
    : false;
type Expect<Type extends true> = Type;
type IsAny<Type> = 0 extends 1 & Type ? true : false;

type McpSource = ReturnType<typeof Mcp.fromClient>;
type McpRemoteSource = ReturnType<typeof Mcp.remote>;
type McpCachedSource = ReturnType<typeof Mcp.cached>;

type McpSourceIsTyped = Expect<Equal<IsAny<McpSource>, false>>;
type McpRemoteSourceIsTyped = Expect<Equal<IsAny<McpRemoteSource>, false>>;
type McpCachedRequiresCache = Expect<
  McpCachedSource extends DynamicToolkit.Source<infer _Tools, infer Requires>
    ? Mcp.ConnectionCache extends Requires
      ? true
      : false
    : false
>;
type McpAuthRejectsPlainString = Expect<
  Equal<string extends Mcp.Auth ? true : false, false>
>;
type McpAuthAcceptsRedacted = Expect<
  Equal<Redacted.Redacted<string> extends Mcp.Auth ? true : false, true>
>;
type McpNamesArePrefixed = Expect<
  Equal<keyof Mcp.Tools, `mcp__${string}__${string}`>
>;
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

export type PublishedTypeAssertions = [
  McpSourceIsTyped,
  McpRemoteSourceIsTyped,
  McpCachedRequiresCache,
  McpAuthRejectsPlainString,
  McpAuthAcceptsRedacted,
  McpNamesArePrefixed,
  SuspendedConversationIdIsBranded,
  SuspendedToolCallIdIsBranded,
  WaitConversationIdIsBranded,
  PersistedRevisionIsBranded,
];
