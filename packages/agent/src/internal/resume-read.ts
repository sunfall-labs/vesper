import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import {
  type ConversationRecord,
  FORMAT_VERSION,
} from '@sunfall/vesper-log/record';
import { Effect } from 'effect';

import type { Compatibility } from '../log.js';
import type { Stop } from '../stop.js';
import { AgentHistory as AgentHistoryRuntime } from './history.js';

// Resumption, in CONTEXT.md's sense: the bounded reads that rebuild a
// conversation's cumulative state and live prompt from its records, and the
// resume aggregate a settling run writes so the next open stays bounded.

export const readAll = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  LogStore.LogStoreError
> =>
  Effect.gen(function* () {
    const all: ConversationRecord.Envelope[] = [];
    let cursor = LogOffset.START;
    let done = false;

    while (!done) {
      const page = yield* store.read(path, { after: cursor });
      all.push(...page.records);
      cursor = page.cursor;
      done = page.upToDate;
    }

    return all;
  });

export interface OpenState {
  readonly history: ReadonlyArray<ConversationRecord.Envelope>;
  readonly aggregateSuffix: ReadonlyArray<ConversationRecord.Envelope>;
  readonly usage: Stop.Usage;
  readonly signalCursor: LogOffset.Offset;
}

const oldestFirst = (
  newestFirst: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<ConversationRecord.Envelope> =>
  newestFirst.reduceRight<Array<ConversationRecord.Envelope>>(
    (ordered, envelope) => {
      ordered.push(envelope);
      return ordered;
    },
    [],
  );

/** Read only the physical suffix needed to resume cumulative state. */
export const loadOpenState = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<OpenState, LogStore.LogStoreError> =>
  Effect.gen(function* () {
    const aggregateSuffix = yield* readAggregateSuffix(store, path);
    return {
      history: yield* readResumeHistory(store, path),
      aggregateSuffix,
      usage: AgentHistoryRuntime.usageFrom(aggregateSuffix),
      signalCursor: deliveredThrough(aggregateSuffix),
    };
  });

/** Read through the newest aggregate, or the full physical log when none exists. */
export const readAggregateSuffix = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  LogStore.LogStoreError
> =>
  Effect.gen(function* () {
    const newest: ConversationRecord.Envelope[] = [];
    let before: LogOffset.Offset | undefined;
    let done = false;
    while (!done) {
      const page = yield* store.readBackwards(path, {
        ...(before === undefined ? {} : { before }),
        limit: RESUME_READ_LIMIT,
      });
      for (const envelope of page.records) {
        newest.push(envelope);
        if (
          envelope.record._tag === 'RunSettled' &&
          envelope.record.resume !== undefined
        ) {
          done = true;
          break;
        }
      }
      if (done || page.upToDate) {
        break;
      }
      before = page.cursor;
    }
    return oldestFirst(newest);
  });

export const mergeByOffset = (
  left: ReadonlyArray<ConversationRecord.Envelope>,
  right: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<ConversationRecord.Envelope> => {
  const retained = new Map(
    left.map((envelope) => [envelope.offset, envelope] as const),
  );
  for (const envelope of right) {
    retained.set(envelope.offset, envelope);
  }
  const ordered = [...retained.values()];
  ordered.sort((candidate, current) =>
    LogOffset.isAfter(candidate.offset, current.offset)
      ? 1
      : LogOffset.isAfter(current.offset, candidate.offset)
        ? -1
        : 0,
  );
  return ordered;
};

/**
 * Walk the active path backwards, jumping over abandoned branches and stopping
 * once the latest compaction's kept boundary has been retained.
 */
export const readResumeHistory = (
  store: LogStore.Interface,
  path: string,
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  LogStore.LogStoreError
> =>
  Effect.gen(function* () {
    const newest: ConversationRecord.Envelope[] = [];
    let before: LogOffset.Offset | undefined;
    let boundary: LogOffset.Offset | undefined;
    let done = false;

    while (!done) {
      const page = yield* store.readBackwards(path, {
        ...(before === undefined ? {} : { before }),
        limit: RESUME_READ_LIMIT,
      });
      let jumped = false;
      for (const envelope of page.records) {
        if (envelope.record._tag === 'BranchedFrom') {
          if (
            envelope.record.at === LogOffset.START ||
            !LogOffset.isAfter(envelope.offset, envelope.record.at)
          ) {
            if (envelope.record.at === LogOffset.START) {
              done = true;
            }
            continue;
          }
          before = yield* offsetAfter(envelope.record.at);
          jumped = true;
          break;
        }

        newest.push(envelope);
        if (
          boundary !== undefined &&
          !LogOffset.isAfter(envelope.offset, boundary)
        ) {
          done = true;
          break;
        }
        if (boundary === undefined && envelope.record._tag === 'Compacted') {
          boundary = envelope.record.firstKept;
          if (boundary === LogOffset.START) {
            done = true;
            break;
          }
        }
      }
      if (done) {
        break;
      }
      if (jumped) {
        continue;
      }
      if (page.upToDate) {
        break;
      }
      before = page.cursor;
    }
    return oldestFirst(newest);
  });

const RESUME_READ_LIMIT = 32;

const offsetAfter = (
  offset: LogOffset.Offset,
): Effect.Effect<LogOffset.Offset> =>
  LogOffset.toSeq(offset).pipe(
    Effect.map((sequence) => LogOffset.fromSeq(sequence + 1n)),
    Effect.orDie,
  );

export const resumeState = (
  compatibility: Compatibility,
  usage: Stop.Usage,
  signalCursor: LogOffset.Offset,
  completed: ReturnType<typeof AgentHistoryRuntime.completedFrom>,
  latestTurnUsage: Stop.Usage | undefined,
  state: ConversationRecord.RecordOf<'StateCheckpoint'> | undefined,
  codeState: ConversationRecord.RecordOf<'CodeStateCheckpoint'> | undefined,
) => ({
  formatVersion: FORMAT_VERSION,
  agent: compatibility.agent,
  agentRevision: compatibility.revision,
  ...(compatibility.digest === undefined
    ? {}
    : { agentDigest: compatibility.digest }),
  usage,
  signalCursor,
  ...(completed === undefined ? {} : { completed }),
  ...(latestTurnUsage === undefined ? {} : { latestTurnUsage }),
  ...(state === undefined
    ? {}
    : { state: { id: state.id, version: state.version, value: state.value } }),
  ...(codeState === undefined ? {} : { codeState: codeState.state }),
});

export const addUsage = (left: Stop.Usage, right: Stop.Usage): Stop.Usage => ({
  input: left.input + right.input,
  output: left.output + right.output,
});

/**
 * The furthest signal offset this conversation has recorded taking.
 *
 * The **whole log**, and this is the row of `branch.ts`'s table that is worth
 * being careful about. A `SignalReceived` says a steer was delivered to a
 * running agent, which then acted on it — a fact about the world, not a claim
 * the conversation can withdraw by branching. If this were scoped to the
 * active path, branching away from the turn that took a steer would rewind the
 * cursor past it, and the next run would drain that steer from the signal
 * stream a second time and inject an instruction the agent has already
 * followed. Delivery is at-least-once by design; this keeps "at least" from
 * quietly becoming "every time anyone edits an earlier message".
 */
const deliveredThrough = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): LogOffset.Offset => {
  let at = LogOffset.START;
  for (const { record } of history) {
    if (record._tag === 'SignalReceived' && LogOffset.isAfter(record.at, at)) {
      at = record.at;
    }
    if (
      record._tag === 'RunSettled' &&
      record.resume !== undefined &&
      LogOffset.isAfter(record.resume.signalCursor, at)
    ) {
      at = record.resume.signalCursor;
    }
  }
  return at;
};
