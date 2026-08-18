import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Prompt } from 'effect/unstable/ai';

import { AgentBranch } from './branch.js';
import { rebuild } from './internal/history.js';

/**
 * Rebuild the active durable conversation as an Effect AI prompt.
 *
 * The agent's system message is not included: it belongs to the current agent
 * definition, not durable history. Unanswered tool calls are omitted, the
 * latest compaction is applied, and abandoned branches are skipped.
 */
export const messagesFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): Prompt.Prompt =>
  Prompt.fromMessages(
    rebuild(AgentBranch.activePath(records)).map((placed) => placed.message),
  );

export * as AgentHistory from './history.js';
