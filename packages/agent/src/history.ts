import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Prompt } from 'effect/unstable/ai';

import { AgentBranch } from './branch.js';
import { Compaction } from './compaction.js';
import type { Stop } from './stop.js';

// Records back into a runnable conversation.
//
// `Agent.streamFrom` is the reader half of the log and yields records; this is
// the writer half, and it is what makes the log a durability mechanism rather
// than an audit trail. Without it a crashed run can only restart from turn one
// — which is the gap `@sunfall/vesper-durable`'s checkpointer existed to cover, by
// re-running the loop and serving each model call from storage. Rebuilding the
// prompt reaches the same place without the replay, and without re-executing
// the tool calls those turns made.
//
// ## The shape it rebuilds is `Chat`'s own
//
// One turn is an assistant message carrying that turn's text and tool calls,
// followed by a tool message carrying their results. That is exactly what
// `Prompt.fromResponseParts` produces from a provider response, which is what
// `Chat` appends to history per turn — so a rebuilt history is
// indistinguishable from one the loop accumulated in process, rather than a
// second encoding of the same conversation that providers might read
// differently.
//
// ## A tool call with no outcome is dropped
//
// A run that died between issuing a tool call and recording its result leaves
// a call nothing answered. Sending that to a provider is not a degraded
// prompt, it is a rejected request — Anthropic and OpenAI both refuse an
// assistant tool call with no matching result. So an unanswered call is
// dropped and the model is free to ask again. The tool may or may not have
// actually run before the crash; nothing in the log can say, and re-asking is
// the same at-least-once bargain the rest of this family makes.
//
// ## Compaction is honoured, not ignored
//
// `Compacted` is the one record that replaces history rather than adding to
// it, and it is the only one that can make a rebuild *longer* than the run it
// rebuilds. This used to ignore it — the record carried counts and nothing
// else, so there was nothing to honour — and the consequence was not merely
// "more context than the crashed run had": a resumed conversation was handed
// back the very messages compaction had already decided did not fit, so it
// overflowed and compacted again on its first turn, paying for a summary the
// log was already holding.
//
// The rule:
//
//   - **The latest compaction wins.** A long conversation compacts repeatedly,
//     and each summary already subsumes the one before it — the earlier
//     summary was an ordinary user message in the history the later one
//     summarized. Reading anything but the last would replay superseded text.
//   - **Everything before `firstKept` is replaced** by the summary, rendered
//     as a user message by `Compaction.summaryMessage` — the same function
//     that put it into the live `Chat`, so the rebuilt conversation and the
//     compacted one are worded identically rather than equivalently.
//   - **Everything after is rebuilt as usual**, including the records between
//     `firstKept` and the compaction itself, which are the tail compaction
//     kept verbatim.
//
// A `firstKept` of `LogOffset.START` means nothing before it survived; a
// `firstKept` naming a record that is not here — a trimmed log — falls back to
// the same thing rather than guessing. Both keep the summary, which is the
// part that cannot be recovered from anywhere else.
//
// ## Branches are followed, not flattened
//
// A conversation is a line until someone edits an earlier message and re-runs,
// at which point the log holds both what happened and what happens instead.
// `AgentBranch.activePath` is what picks between them, and the two exported
// functions here are the reason it exists: what a model is shown is the path,
// never the log. It is applied *inside* each of them rather than by their
// callers, so that no caller can be handed a `Session.history` and forget —
// and so that {@link boundaryFor} and {@link messagesFrom} cannot be given
// different views of the same conversation, which would put a compaction
// boundary on a record the reader cannot see.
//
// {@link usageFrom} deliberately does **not** apply it. `branch.ts` carries
// the full table of which folds are path-scoped and which are not, and why
// each is the way it is; that table is the thing to read before adding a fold
// here.
//
// ## What is deliberately not reconstructed
//
// `Completed` is skipped: its `text` is the final turn's text, which the
// `Text` records already carry. Reading both would say everything twice.

/**
 * Rebuild the conversation a run would continue, from its records.
 *
 * The agent's system message is **not** included: it belongs to the agent, not
 * to the conversation, and an agent whose instructions changed since the
 * crashed run should resume under the instructions it has now. Callers seed it
 * themselves — `Agent.resume` does.
 *
 * Pure, and total for records this family wrote. A `RunStarted` whose stored
 * prompt is not a prompt throws, which becomes a defect at the call site: that
 * is corruption, and it is the same call `log.ts` makes for a store failure.
 */
export const messagesFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): Prompt.Prompt =>
  Prompt.fromMessages(
    rebuild(AgentBranch.activePath(records)).map((placed) => placed.message),
  );

/**
 * Where in the log to point a new compaction's `firstKept`.
 *
 * The other half of the reconstruction rule, and it lives here because it has
 * to agree with {@link messagesFrom} exactly — the boundary is only meaningful
 * in terms of the messages this file rebuilds. Compaction reports how many
 * messages it kept, because that is all it can know: it runs against `Chat`'s
 * in-memory history, which carries no record identity. This turns that count
 * into the position of the record the kept tail starts at.
 *
 * The two message sequences line up because they are the same conversation
 * accumulated twice — `Chat`'s history is what the loop appended turn by turn,
 * and this is what the records of those same turns rebuild. That equivalence
 * is not a new assumption; it is the one `Agent.resume` has always rested on,
 * because it is what makes a rebuilt history a continuation rather than a
 * different conversation.
 *
 * Returns {@link LogOffset.START} when nothing survives — a kept tail of zero,
 * or an empty log.
 */
export const boundaryFor = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
  keptMessages: number,
): LogOffset.Offset => {
  if (keptMessages <= 0) return LogOffset.START;
  // The same view {@link messagesFrom} rebuilds, for the reason stated above:
  // a boundary resolved against the whole log could name a record on an
  // abandoned branch, which the reader would then fail to find and fall back
  // from — silently keeping nothing where the compaction meant to keep a tail.
  const built = rebuild(AgentBranch.activePath(records));
  if (built.length === 0) return LogOffset.START;
  // Clamped rather than failed: a tail longer than the rebuilt history means
  // keep all of it, which is what the caller asked for.
  return built[Math.max(0, built.length - keptMessages)]!.offset;
};

/** A rebuilt message, and the record that started it. */
interface Placed {
  readonly offset: LogOffset.Offset;
  readonly message: Prompt.Message;
}

/**
 * Apply the compaction rule, then rebuild what is left.
 *
 * Split from {@link fold} rather than folded into it because the two answer
 * different questions — *which* records still count, and what those records
 * say — and because a compaction inside the kept range must contribute
 * nothing rather than be applied a second time.
 *
 * **Takes an active path, not a log.** Both callers filter first, and that is
 * the whole of what branching costs this file: "the latest compaction wins"
 * has to mean the latest one *on this path*, or a compaction the user branched
 * away from would replace a history it never summarized. Nothing below
 * changes — the rule was always about the sequence it was given.
 */
const rebuild = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<Placed> => {
  const latest = lastCompaction(records);
  if (latest === undefined) return fold(records);

  const compaction = records[latest]!;
  const record = compaction.record as ConversationRecord.RecordOf<'Compacted'>;

  return [
    // The summary stands where the compaction record does, so a later
    // compaction whose kept tail reaches back this far can point at it — and
    // dropping it then is correct, because that later summary summarized this
    // one along with everything else.
    {
      offset: compaction.offset,
      message: Compaction.summaryMessage(record.summary),
    },
    ...fold([
      ...records.slice(keptFrom(records, latest, record.firstKept), latest),
      ...records.slice(latest + 1),
    ]),
  ];
};

const lastCompaction = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): number | undefined => {
  for (let index = records.length - 1; index >= 0; index -= 1) {
    if (records[index]!.record._tag === 'Compacted') return index;
  }
  return undefined;
};

/**
 * The index the kept tail starts at.
 *
 * Falls back to "nothing before the compaction survived" when the pointer
 * names no record that is here. That is the conservative direction: a rebuild
 * that keeps too little is short some verbatim context the summary already
 * describes, and one that kept too much would reintroduce the messages
 * compaction removed — which is the bug this whole rule exists to fix.
 */
const keptFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
  compaction: number,
  firstKept: LogOffset.Offset,
): number => {
  if (firstKept === LogOffset.START) return compaction;
  const index = records.findIndex(
    (envelope) => !LogOffset.isAfter(firstKept, envelope.offset),
  );
  return index === -1 || index > compaction ? compaction : index;
};

/**
 * Records into messages, with no compaction rule applied.
 *
 * Everything the accumulation below does is per-turn bookkeeping, which is why
 * it is a plain fold: the only cross-turn decision in this file is which
 * records reach it, and {@link rebuild} has already made that one.
 */
const fold = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<Placed> => {
  const messages: Array<Placed> = [];
  let assistant: Array<Prompt.AssistantMessagePart> = [];
  let results: Array<Prompt.ToolResultPart> = [];
  // Where each accumulating message began. A message is attributed to the
  // record that opened it, which is what makes a boundary pointing at it mean
  // "this message and everything after".
  let assistantAt = LogOffset.START;
  let resultsAt = LogOffset.START;

  /**
   * Close the turn being accumulated.
   *
   * The two filters are one rule seen from both ends: a tool call and its
   * result travel together or neither travels. An unanswered call would be
   * rejected by the provider; an orphaned result refers to a call the model
   * never sees and cannot be attributed.
   */
  const flush = (): void => {
    const answered = new Set(results.map((result) => result.id));
    const asked = new Set(
      assistant.flatMap((part) => (part.type === 'tool-call' ? [part.id] : [])),
    );

    const content = assistant.filter(
      (part) => part.type !== 'tool-call' || answered.has(part.id),
    );
    const answers = results.filter((result) => asked.has(result.id));

    if (content.length > 0) {
      messages.push({
        offset: assistantAt,
        message: Prompt.makeMessage('assistant', { content }),
      });
    }
    if (answers.length > 0) {
      messages.push({
        offset: resultsAt,
        message: Prompt.makeMessage('tool', { content: answers }),
      });
    }

    assistant = [];
    results = [];
    assistantAt = LogOffset.START;
    resultsAt = LogOffset.START;
  };

  for (const { offset, record } of records) {
    switch (record._tag) {
      case 'RunStarted':
        // A previous run's input is a user message in this conversation, and
        // `RunStarted.prompt` already holds it normalised to messages.
        flush();
        messages.push(
          ...Prompt.make(record.prompt as Prompt.RawInput).content.map(
            (message) => ({ offset, message }),
          ),
        );
        break;

      case 'Text':
        if (assistant.length === 0) assistantAt = offset;
        assistant.push(Prompt.makePart('text', { text: record.text }));
        break;

      case 'ToolCall':
        if (assistant.length === 0) assistantAt = offset;
        assistant.push(
          Prompt.makePart('tool-call', {
            id: record.id,
            name: record.name,
            params: record.params,
            providerExecuted: false,
          }),
        );
        break;

      case 'ToolOutcome':
        if (results.length === 0) resultsAt = offset;
        results.push(
          Prompt.makePart('tool-result', {
            id: record.id,
            name: record.name,
            isFailure: record.outcome === 'failure',
            // Already the toolkit's encoding — the form `Prompt` puts in front
            // of the model — which is why `ToolOutcome.result` stores that and
            // not the decoded value.
            result: record.result,
          }),
        );
        break;

      case 'TurnFinished':
        flush();
        break;

      case 'SignalReceived':
        // A steer became a user message on the next turn, so it lands after
        // the turn that consumed it. Flushing first is what puts the model's
        // own words before the instruction that redirected it. A cancel
        // changed no prompt; it ended a run.
        flush();
        if (record.kind === 'steer') {
          messages.push({
            offset,
            message: Prompt.makeMessage('user', {
              content: [Prompt.makePart('text', { text: record.text })],
            }),
          });
        }
        break;

      // Everything below contributes no message. `Compacted` is handled by
      // {@link rebuild}, which has already decided which records reach here —
      // a compaction that survived that decision is an earlier one the latest
      // superseded, and replaying its summary would say a superseded thing
      // twice. `Completed` repeats the final turn's text; `ChildSession`
      // describes a delegation the parent already recorded as an ordinary tool
      // call; `RunSettled` is bookkeeping; and `Signal` does not live in this
      // stream at all. `BranchedFrom` is the same situation as `Compacted`
      // seen once more — `AgentBranch.activePath` has already used it to
      // decide which records reach here, so no marker ever survives to this
      // switch and one that did would say nothing about the conversation.
      case 'Compacted':
      case 'BranchedFrom':
      case 'Completed':
      case 'ChildSession':
      case 'RunSettled':
      case 'Signal':
        break;
    }
  }

  // The last turn of a crashed run has no `TurnFinished` — that is what makes
  // it crashed. Closing here is what keeps its work.
  flush();

  return messages;
};

/**
 * What this conversation has cost across every run in it.
 *
 * `TurnFinished`, `Completed` and `RunSettled` all carry usage cumulative
 * *within* one run, so a conversation's total is the sum over runs of the last
 * figure each reported. A resumed conversation that restarted its counter
 * would under-report every turn after the first, which is the number anyone
 * asking about cost actually wants.
 *
 * **The whole log, not the active path**, and the one function in this file
 * that is deliberately not branch-scoped. Tokens spent on a branch that was
 * later abandoned were spent; a provider billed for them and no edit gives
 * them back. Filtering here would make a conversation's cost fall when its
 * user changed their mind, which is the opposite of what the number is for.
 * `branch.ts` holds the full table of this split.
 */
export const usageFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): Stop.Usage => {
  let total: Stop.Usage = { input: 0, output: 0 };
  let run: Stop.Usage = { input: 0, output: 0 };

  for (const { record } of records) {
    switch (record._tag) {
      case 'RunStarted':
        // Only non-zero for a run that reported usage and never settled, which
        // takes a lost finalizer. Banking it is cheaper than losing it.
        total = add(total, run);
        run = { input: 0, output: 0 };
        break;
      case 'TurnFinished':
      case 'Completed':
        run = record.usage;
        break;
      case 'RunSettled':
        total = add(total, record.usage);
        run = { input: 0, output: 0 };
        break;
      default:
        break;
    }
  }

  return add(total, run);
};

const add = (left: Stop.Usage, right: Stop.Usage): Stop.Usage => ({
  input: left.input + right.input,
  output: left.output + right.output,
});

export * as AgentHistory from './history.js';
