import type { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Prompt } from 'effect/ai';

import { AgentBranch } from './branch.js';
import { rebuild } from './internal/history.js';

export { usageFrom } from './internal/history.js';

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

/** Output a run streamed and never finished: no turn ever settled it. */
export interface AbortedOutput {
  /** Offset of the first progress record in the aborted group. */
  readonly offset: LogOffset.Offset;
  readonly step: number;
  readonly text: string;
  readonly reasoning: string;
}

/**
 * Records a tool handler or state writer can append while a response is still
 * streaming. They flush no text, so they say nothing about the group before
 * them.
 */
const NON_SUPERSEDING = new Set<ConversationRecord.Record['_tag']>([
  'StepStarted',
  'StepResolved',
  'StepRestarted',
  'StateCheckpoint',
  'CodeStateCheckpoint',
  'ChildSession',
]);

/**
 * Output that was committed while streaming but never settled, in log order.
 *
 * A group of contiguous `PartialOutput` records is aborted when nothing that
 * settles a turn follows it: the log ends, a later run starts, or the run
 * settles. When a `Text` (or tool call, turn end, signal, ...) follows
 * instead, that record carries the whole turn text and supersedes the group.
 *
 * This is the read-side view for UIs and audits. It is deliberately not part
 * of {@link messagesFrom}: the model never said this, so the rerun turn must
 * not see it.
 */
export const abortedOutputFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<AbortedOutput> => {
  const aborted: Array<AbortedOutput> = [];
  let group:
    | {
        offset: LogOffset.Offset;
        step: number;
        text: string;
        reasoning: string;
      }
    | undefined;
  const close = (): void => {
    if (group !== undefined) {
      aborted.push(group);
      group = undefined;
    }
  };
  for (const { offset, record } of AgentBranch.activePath(records)) {
    if (record._tag === 'PartialOutput') {
      group ??= { offset, step: record.step, text: '', reasoning: '' };
      if (record.kind === 'text') {
        group.text += record.text;
      } else {
        group.reasoning += record.text;
      }
      continue;
    }
    if (record._tag === 'RunStarted' || record._tag === 'RunSettled') {
      close();
    } else if (!NON_SUPERSEDING.has(record._tag)) {
      group = undefined;
    }
  }
  close();
  return aborted;
};

export * as AgentHistory from './history.js';
