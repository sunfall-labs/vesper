import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';

// The conversation as a tree, folded out of a log that is a line.
//
// A conversation stream is append-only and totally ordered, and for most of
// its life that is also its shape: each record continues the one before it.
// Editing an earlier message and re-running breaks that — the run after the
// edit continues from the middle, and everything the first attempt wrote after
// that point is still in the log but is no longer part of the conversation.
//
// `ConversationRecord.BranchedFrom` is how the log says so, and this file is
// the only thing that reads it. Everything else in the family keeps treating
// records as a line; what changes is *which* line it is handed.
//
// ## Why a marker and not a parent pointer
//
// The obvious encoding — a `parentId` on every record — is what a log with no
// ordering primitive has to do. It is also almost entirely redundant: a
// conversation appends at its head, so the parent is the previous record on
// every record except the first one after a branch. We have offsets, which are
// a total order, so the redundant part can simply be left out and the
// exceptions written down. One row per branch, and the same information.
//
// The full tree comes back from the same markers without extra storage: a
// record's parent is the record before it in offset order, unless that record
// is a `BranchedFrom { at }`, in which case it is the record at `at`. This
// file exposes only {@link activePath}, because that is the question every
// caller in this package actually asks; a "render every branch" reader would
// be a forward fold over the same markers and needs nothing new in the log.
//
// ## Which folds get the path, and which must not — the load-bearing table
//
// Branching splits every fold over a conversation's records into two kinds,
// and getting a row wrong is silent in both directions. The tempting refactor
// — filter to the active path once, where the records are read, and let
// everything downstream see only the path — is **wrong**, because two of these
// rows are about what physically happened rather than about what the
// conversation now says.
//
// | fold                     | where                | scope       | why |
// | ------------------------ | -------------------- | ----------- | --- |
// | `messagesFrom`           | `history.ts`         | active path | it is the prompt; the abandoned branch is what the user edited away |
// | `boundaryFor`            | `history.ts`         | active path | it must resolve against the same messages `messagesFrom` builds, or a compaction points `firstKept` at a record the reader cannot see |
// | `rebuild` / `lastCompaction` / `keptFrom` / `fold` | `history.ts` | active path | private to the two above; they receive an already-filtered array and never filter themselves |
// | `usageFrom`              | `history.ts`         | **full log**| tokens spent on a branch that was abandoned were still spent. Scoping this to the path makes a branched conversation under-report its own cost, and the number never comes back |
// | `Recovery.fold`          | `recovery.ts`        | active path | crash recovery serves a dead run's tool state back to its successor. If the user branched *away* from the crashed run, those recoveries answer tool calls that are no longer in the prompt — the successor would be handed results or indeterminate calls for questions it never asked |
// | `deliveredThrough`       | `log.ts`             | **full log**| the signal-delivery cursor. A steer is delivered at most once, and the record of taking it may sit on a branch that was later abandoned. Scoping this to the path rewinds the cursor and **re-delivers a steer the agent already acted on** — the one failure this cursor exists to prevent |
// | `activePath`             | here                 | **full log**| it is the filter; it has to see what it is filtering |
//
// The two full-log rows are not an oversight to be tidied up later. Resume
// aggregates preserve them across compacted-away history; active-path folds
// consume only the records required to rebuild the live prompt.

/**
 * The records that are still part of the conversation, in order.
 *
 * A backwards walk from the head. Ordinary records are kept; a
 * `BranchedFrom { at }` is dropped and the walk resumes from the last record
 * at or before `at`, so everything in between — the branch that was abandoned
 * — is skipped. Reversed at the end, because callers fold forwards.
 *
 * Total and pure, with no failure mode. Three properties are deliberate:
 *
 *   - **It cannot loop.** The cursor strictly decreases on every step,
 *     including the jump, whatever `at` says. A marker naming an offset at or
 *     after its own record degenerates into an ordinary step backwards rather
 *     than a cycle. That matters because these records come out of a database
 *     and a tail that hangs on one bad row is worse than a wrong answer.
 *   - **It is linear.** The cursor never moves forwards, so the total work is
 *     bounded by the number of records however many markers there are.
 *   - **`at` need not name a record that is here.** The walk resumes at the
 *     last record before it, which is the same tolerance
 *     `AgentHistory`'s `keptFrom` shows a `firstKept` pointing into a trimmed
 *     log. A branch point in a range that was itself abandoned is fine and
 *     meaningful: it is how a reader returns to a branch it left.
 *
 * Handing it records that contain no marker returns them unchanged, which is
 * what makes every existing caller correct without a special case.
 */
export const activePath = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<ConversationRecord.Envelope> => {
  const path: Array<ConversationRecord.Envelope> = [];

  let index = records.length - 1;
  while (index >= 0) {
    const envelope = records[index]!;

    if (envelope.record._tag === 'BranchedFrom') {
      // Start below the marker rather than at it, which is what makes the
      // cursor decrease even when `at` names this record or a later one.
      let next = index - 1;
      const { at } = envelope.record;
      while (next >= 0 && LogOffset.isAfter(records[next]!.offset, at)) {
        next -= 1;
      }
      index = next;
      continue;
    }

    path.push(envelope);
    index -= 1;
  }

  return path.reverse();
};

export * as AgentBranch from './branch.js';
