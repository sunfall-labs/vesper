import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect } from 'effect';

import { pathFor } from './internal/conversation-stream.js';
import { readAll } from './internal/resume-read.js';
import type { Limit } from './run-policy.js';

// Budget compliance, recomputed from the durable log alone.
//
// A run's hard budget is enforced in memory (`run-policy-runtime.ts`); this is
// the reader that checks, afterwards and without the process, that the
// ceilings were in force and were honored. It trusts nothing but records:
//
//   - the ceilings come from `RunStarted.budget`, which every recorded run
//     carries; a budget that names no ceiling for an audited limit leaves that
//     limit `unprovable`, never `within`;
//   - consumption is recomputed from `TurnFinished` / `Completed` /
//     `RunSettled` usage and from `ChildSession` links, the same records the
//     runtime's counters were fed from;
//   - a budget is shared by a root run and every descendant loop, so runs are
//     grouped by `budget.id` across the conversations supplied. A child
//     conversation that is linked but absent is an evidence gap, because its
//     spend is exactly what a ceiling check must not silently drop.
//
// **What "respected" means.** Turns and delegations are checked and refused
// *before* they happen, so recorded work never exceeds those ceilings. Token
// ceilings are checked *after* a turn's usage is known (see `Limits`), so a
// run that is stopped by one may legitimately end above it. The bound the
// runtime guarantees is: when the ceiling is crossed, each conversation of the
// budget has at most one turn in flight, and each such turn's usage lands
// above the ceiling once before its run fails. So a token overshoot is
// accepted as `stopped` only if it is no larger than the sum, over the budget's
// conversations, of the largest single-turn usage recorded in each, *and* a
// `RunSettled.stoppedBy` names that limit. Anything else above a ceiling is
// `violated`.
//
// **What is not covered.** Limits whose consumption the log does not carry
// (model calls, wall clock, signal sizes) are listed in `unaudited`. Usage a
// provider never reported, and a turn in flight when the process died, are
// not in any log; a run left unsettled is therefore a gap, not a pass. This
// proves the log is consistent with the ceiling, not that the log is genuine —
// that needs a tamper-evident log underneath.

/** Limits whose consumption this audit recomputes from records. */
export const AUDITED_LIMITS = [
  'turns',
  'delegated_tasks',
  'input_tokens',
  'output_tokens',
] as const satisfies ReadonlyArray<Limit>;
export type AuditedLimit = (typeof AUDITED_LIMITS)[number];

const UNAUDITED_LIMITS: ReadonlyArray<Limit> = [
  'model_calls',
  'deadline',
  'signal_bytes',
  'signals_per_boundary',
  'steered_bytes',
];

const ceilingKey: Readonly<Record<AuditedLimit, string>> = {
  turns: 'maxTurns',
  delegated_tasks: 'maxDelegatedTasks',
  input_tokens: 'maxInputTokens',
  output_tokens: 'maxOutputTokens',
};

export type LimitStatus =
  /** Recomputed use is at or under the recorded ceiling. */
  | 'within'
  /** Over the ceiling by no more than the documented bound, and the run was stopped for it. */
  | 'stopped'
  /** Over the ceiling beyond the bound, or not shown to have been stopped. */
  | 'violated'
  /** The recorded budget names no ceiling for this limit. */
  | 'unprovable';

export interface LimitAudit {
  readonly limit: AuditedLimit;
  /** Recorded ceiling; undefined when the budget names none for this limit. */
  readonly ceiling: number | undefined;
  /** Consumption recomputed from records. */
  readonly used: number;
  /** `used - ceiling` when positive, else 0. */
  readonly overshoot: number;
  /** Largest overshoot the enforcement model allows for this limit. */
  readonly overshootBound: number;
  readonly status: LimitStatus;
}

export type EvidenceGap =
  /** The root run's conversation was not supplied or holds no run of this budget. */
  | { readonly kind: 'root-log-missing'; readonly conversationId: string }
  /** A `ChildSession` names a conversation with no run under this budget. */
  | { readonly kind: 'child-log-missing'; readonly conversationId: string }
  /** A run of this budget never settled; its in-flight turn is unaccounted for. */
  | { readonly kind: 'run-unsettled'; readonly conversationId: string }
  /** Runs of one budget recorded different ceilings or roots. */
  | { readonly kind: 'inconsistent-budget'; readonly conversationId: string }
  /** A recorded stop reports more token use than the log accounts for. */
  | {
      readonly kind: 'unlogged-usage';
      readonly limit: AuditedLimit;
      readonly claimed: number;
      readonly recomputed: number;
    };

export type Verdict = 'compliant' | 'breached' | 'unprovable';

export interface RunAudit {
  readonly budgetId: string;
  /** The root run's conversation. */
  readonly conversationId: string;
  /** Conversations whose runs drew from this budget, root first. */
  readonly conversations: ReadonlyArray<string>;
  /** How the root run settled; undefined if it never did. */
  readonly outcome:
    | ConversationRecord.RecordOf<'RunSettled'>['outcome']
    | undefined;
  /** Limits a `RunSettled.stoppedBy` reports as the cause of a stop. */
  readonly stoppedBy: ReadonlyArray<string>;
  readonly limits: ReadonlyArray<LimitAudit>;
  /** Limits the log does not carry consumption for; never reported compliant. */
  readonly unaudited: ReadonlyArray<Limit>;
  readonly gaps: ReadonlyArray<EvidenceGap>;
  readonly verdict: Verdict;
}

export interface Audit {
  readonly runs: ReadonlyArray<RunAudit>;
  /** `compliant` only if there is at least one run and every run is. */
  readonly verdict: Verdict;
}

interface Usage {
  readonly input: number;
  readonly output: number;
}

interface Segment {
  readonly conversationId: string;
  readonly budget: ConversationRecord.RunBudget;
  turns: number;
  /** Cumulative usage after each recorded point, in order. */
  readonly cumulative: Usage[];
  readonly children: Set<string>;
  settled: ConversationRecord.RecordOf<'RunSettled'> | undefined;
}

const segmentsOf = (
  envelopes: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<Segment> => {
  const segments: Segment[] = [];
  let current: Segment | undefined;
  for (const { conversationId, record } of envelopes) {
    if (record._tag === 'RunStarted') {
      current = {
        conversationId,
        budget: record.budget,
        turns: 0,
        cumulative: [],
        children: new Set(),
        settled: undefined,
      };
      segments.push(current);
    } else if (current === undefined) {
      continue;
    } else if (record._tag === 'TurnFinished') {
      current.turns += 1;
      current.cumulative.push(record.usage);
    } else if (record._tag === 'Completed') {
      current.cumulative.push(record.usage);
    } else if (record._tag === 'RunSettled') {
      current.settled = record;
      current.cumulative.push(record.usage);
    } else if (
      record._tag === 'ChildSession' &&
      record.parentConversationId === conversationId
    ) {
      current.children.add(record.childConversationId);
    }
  }
  return segments;
};

/** Total and largest single step of one usage dimension within a segment. */
const spend = (
  segment: Segment,
  field: keyof Usage,
): { readonly total: number; readonly largestStep: number } => {
  let previous = 0;
  let largestStep = 0;
  for (const point of segment.cumulative) {
    // Usage within a run only grows; a smaller figure is not a refund.
    const next = Math.max(previous, point[field]);
    largestStep = Math.max(largestStep, next - previous);
    previous = next;
  }
  return { total: previous, largestStep };
};

type Stop = NonNullable<ConversationRecord.RecordOf<'RunSettled'>['stoppedBy']>;

const stopsOf = (segment: Segment): ReadonlyArray<Stop> =>
  segment.settled?.stoppedBy === undefined ? [] : [segment.settled.stoppedBy];

const sameBudget = (
  a: ConversationRecord.RunBudget,
  b: ConversationRecord.RunBudget,
): boolean =>
  a.root === b.root &&
  Object.keys(a.limits).length === Object.keys(b.limits).length &&
  Object.entries(a.limits).every(([name, value]) => b.limits[name] === value);

const verdictOf = (
  limits: ReadonlyArray<LimitAudit>,
  gaps: ReadonlyArray<EvidenceGap>,
): Verdict =>
  limits.some((limit) => limit.status === 'violated')
    ? 'breached'
    : gaps.length > 0 || limits.some((limit) => limit.status === 'unprovable')
      ? 'unprovable'
      : 'compliant';

const auditBudget = (
  budget: ConversationRecord.RunBudget,
  segments: ReadonlyArray<Segment>,
): RunAudit => {
  const gaps: EvidenceGap[] = [];
  const rootSegments = segments.filter(
    (segment) => segment.conversationId === budget.root,
  );
  const rootSettled = rootSegments.at(-1)?.settled;
  if (rootSegments.length === 0) {
    gaps.push({ kind: 'root-log-missing', conversationId: budget.root });
  }
  for (const segment of segments) {
    if (!sameBudget(segment.budget, budget)) {
      gaps.push({
        kind: 'inconsistent-budget',
        conversationId: segment.conversationId,
      });
    }
    if (segment.settled === undefined) {
      gaps.push({
        kind: 'run-unsettled',
        conversationId: segment.conversationId,
      });
    }
  }
  const present = new Set(segments.map((segment) => segment.conversationId));
  const children = new Set(
    segments.flatMap((segment) => [...segment.children]),
  );
  for (const child of children) {
    if (!present.has(child)) {
      gaps.push({ kind: 'child-log-missing', conversationId: child });
    }
  }

  const stops = segments.flatMap(stopsOf);
  const conversations = [
    budget.root,
    ...[...present].filter((conversation) => conversation !== budget.root),
  ];
  const tokenUse = (field: keyof Usage) => {
    let total = 0;
    let bound = 0;
    for (const segment of segments) {
      const part = spend(segment, field);
      total += part.total;
      bound += part.largestStep;
    }
    return { used: total, bound };
  };

  const limits = AUDITED_LIMITS.map((limit): LimitAudit => {
    const measured =
      limit === 'turns'
        ? { used: segments.reduce((sum, s) => sum + s.turns, 0), bound: 0 }
        : limit === 'delegated_tasks'
          ? { used: children.size, bound: 0 }
          : tokenUse(limit === 'input_tokens' ? 'input' : 'output');
    const ceiling = budget.limits[ceilingKey[limit]];
    const overshoot =
      ceiling === undefined ? 0 : Math.max(0, measured.used - ceiling);
    const stopped = stops.some((stop) => stop.limit === limit);
    const status: LimitStatus =
      ceiling === undefined
        ? 'unprovable'
        : overshoot === 0
          ? 'within'
          : stopped && overshoot <= measured.bound
            ? 'stopped'
            : 'violated';
    if (limit === 'input_tokens' || limit === 'output_tokens') {
      const claimed = Math.max(
        0,
        ...stops.filter((stop) => stop.limit === limit).map((s) => s.used),
      );
      if (claimed > measured.used) {
        gaps.push({
          kind: 'unlogged-usage',
          limit,
          claimed,
          recomputed: measured.used,
        });
      }
    }
    return {
      limit,
      ceiling,
      used: measured.used,
      overshoot,
      overshootBound: measured.bound,
      status,
    };
  });

  return {
    budgetId: budget.id,
    conversationId: budget.root,
    conversations,
    outcome: rootSettled?.outcome,
    stoppedBy: [...new Set(stops.map((stop) => stop.limit))],
    limits,
    unaudited: UNAUDITED_LIMITS,
    gaps,
    verdict: verdictOf(limits, gaps),
  };
};

/**
 * Recompute budget consumption from conversation records and judge it against
 * the ceilings those records name.
 *
 * `envelopes` is every record to consider, from the root conversation and from
 * every child conversation whose spend should be counted, each conversation's
 * records in log order. Pass fewer conversations and the missing ones surface
 * as gaps, never as compliance. Pure and total: it reads nothing but its input.
 */
export const audit = (
  envelopes: ReadonlyArray<ConversationRecord.Envelope>,
): Audit => {
  const byConversation = new Map<string, ConversationRecord.Envelope[]>();
  for (const envelope of envelopes) {
    const list = byConversation.get(envelope.conversationId);
    if (list === undefined) {
      byConversation.set(envelope.conversationId, [envelope]);
    } else {
      list.push(envelope);
    }
  }
  const segments = [...byConversation.values()].flatMap(segmentsOf);

  const groups = new Map<
    string,
    { budget: ConversationRecord.RunBudget; segments: Segment[] }
  >();
  const runs: RunAudit[] = [];
  for (const segment of segments) {
    const group = groups.get(segment.budget.id);
    if (group === undefined) {
      groups.set(segment.budget.id, {
        budget: segment.budget,
        segments: [segment],
      });
    } else {
      group.segments.push(segment);
    }
  }
  for (const { budget, segments: group } of groups.values()) {
    runs.push(auditBudget(budget, group));
  }

  return {
    runs,
    verdict:
      runs.length === 0
        ? 'unprovable'
        : runs.some((run) => run.verdict === 'breached')
          ? 'breached'
          : runs.every((run) => run.verdict === 'compliant')
            ? 'compliant'
            : 'unprovable',
  };
};

/**
 * Read a conversation and every descendant it links to, then {@link audit}.
 *
 * Follows `ChildSession` records transitively. A linked child whose stream
 * does not exist is left out, so the audit reports it as a missing child
 * instead of failing.
 */
export const auditStored: (
  conversationId: LogVocabulary.ConversationId,
) => Effect.Effect<Audit, LogStore.LogStoreError, LogStore.Service> = Effect.fn(
  'RunPolicy.auditStored',
)(function* (conversationId: LogVocabulary.ConversationId) {
  const store = yield* LogStore.Service;
  const collected: ConversationRecord.Envelope[] = [];
  const seen = new Set<string>();
  const pending: LogVocabulary.ConversationId[] = [conversationId];
  for (let next = pending.pop(); next !== undefined; next = pending.pop()) {
    if (seen.has(next)) {
      continue;
    }
    seen.add(next);
    const records = yield* readAll(store, pathFor(next)).pipe(
      Effect.catchIf(
        (error) => error.reason === 'not_found' && next !== conversationId,
        () => Effect.succeed([]),
      ),
    );
    collected.push(...records);
    for (const { record } of records) {
      if (
        record._tag === 'ChildSession' &&
        record.parentConversationId === next
      ) {
        pending.push(record.childConversationId);
      }
    }
  }
  return audit(collected);
});
