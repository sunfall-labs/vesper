import { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Option, Ref } from 'effect';

import { AgentBranch } from './branch.js';

/** How a tool call ended, as a previous run recorded it. */
export interface Settled {
  readonly outcome: 'success' | 'failure';
  /** The encoded result, in the form the provider was shown. */
  readonly result: unknown;
}

/** What an orphaned run durably established about a tool call. */
export type Recovery =
  | { readonly _tag: 'Indeterminate' }
  | ({ readonly _tag: 'Settled' } & Settled);

/** An orphaned handler start and the provider call that originally caused it. */
export interface IndeterminateToolCall {
  readonly step: number;
  readonly name: string;
  readonly toolCallId: LogVocabulary.ToolCallId;
  readonly params: unknown;
}

export interface Snapshot {
  readonly recoveries: Map<string, Recovery>;
  readonly indeterminate: ReadonlyArray<IndeterminateToolCall>;
  readonly corruption: string | undefined;
}

/**
 * Fold the active orphan suffix into the state dispatch needs to recover it.
 *
 * Keeping the fold independent of AgentLog means live bookkeeping and
 * recovery reconstruction share one key and one state model without making
 * this module depend on the Session it eventually serves.
 */
export const fold = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): Snapshot => {
  const recoveries = new Map<string, Recovery>();
  const calls = new Map<string, IndeterminateToolCall>();
  const starts = new Map<
    string,
    { readonly name: string; readonly id: string }
  >();
  const order: string[] = [];
  let running = false;

  for (const { record } of AgentBranch.activePath(history)) {
    switch (record._tag) {
      case 'RunStarted':
        running = true;
        break;
      case 'RunSettled':
        recoveries.clear();
        calls.clear();
        starts.clear();
        order.length = 0;
        running = false;
        break;
      case 'ToolCall':
        if (running) {
          const key = settledKey(record.name, record.id);
          if (!calls.has(key)) order.push(key);
          calls.set(key, {
            step: record.step,
            name: record.name,
            toolCallId: record.id,
            params: record.params,
          });
        }
        break;
      case 'ToolStarted':
        if (running) {
          const key = settledKey(record.name, record.id);
          recoveries.set(key, { _tag: 'Indeterminate' });
          starts.set(key, { name: record.name, id: record.id });
        }
        break;
      case 'ToolOutcome':
        if (running) {
          recoveries.set(settledKey(record.name, record.id), {
            _tag: 'Settled',
            outcome: record.outcome,
            result: record.result,
          });
        }
        break;
      default:
        break;
    }
  }

  // Dispatch commits before entering the handler, while ToolCall arrives via
  // the provider event stream. Diagnose corruption only after the complete
  // orphan suffix is folded.
  const unmatched = [...starts].find(
    ([key]) => recoveries.get(key)?._tag === 'Indeterminate' && !calls.has(key),
  )?.[1];

  return {
    recoveries,
    corruption:
      unmatched === undefined
        ? undefined
        : `Cannot recover indeterminate tool ${unmatched.name} (${unmatched.id}): ` +
          'durable ToolStarted has no matching ToolCall',
    indeterminate: order.flatMap((key) =>
      recoveries.get(key)?._tag === 'Indeterminate' && calls.has(key)
        ? [calls.get(key)!]
        : [],
    ),
  };
};

const settledKey = (
  name: string,
  toolCallId: LogVocabulary.ToolCallId,
): string => `${name}\u001f${toolCallId}`;

export interface Tracker {
  readonly recovery: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
  ) => Option.Option<Recovery>;
  readonly indeterminateToolCalls: ReadonlyArray<IndeterminateToolCall>;
  readonly recoveryCorruption: string | undefined;
  readonly hasPendingToolCalls: Effect.Effect<boolean>;
  readonly onToolSettled: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
    effect: Effect.Effect<void>,
  ) => void;
  /** Apply records after their append is durable. */
  readonly track: (
    records: ReadonlyArray<ConversationRecord.Record>,
  ) => Effect.Effect<void>;
}

/**
 * Live state for one claimed session's tool recovery.
 *
 * The same state shape serves the recovery snapshot built at open time and the
 * records appended by the current run. Keeping callbacks here ensures a
 * cancellation waiter is released only after its ToolOutcome is durable.
 */
export const make = (snapshot: Snapshot): Effect.Effect<Tracker> =>
  Effect.gen(function* () {
    const recoveries = new Map(snapshot.recoveries);
    const pending = yield* Ref.make(
      new Set(
        [...recoveries].flatMap(([key, recovery]) =>
          recovery._tag === 'Indeterminate' ? [key] : [],
        ),
      ),
    );
    const callbacks = new Map<string, Array<Effect.Effect<void>>>();

    const track = (records: ReadonlyArray<ConversationRecord.Record>) =>
      Effect.gen(function* () {
        for (const record of records) {
          if (record._tag === 'ToolStarted') {
            recoveries.set(settledKey(record.name, record.id), {
              _tag: 'Indeterminate',
            });
          } else if (record._tag === 'ToolOutcome') {
            recoveries.set(settledKey(record.name, record.id), {
              _tag: 'Settled',
              outcome: record.outcome,
              result: record.result,
            });
          }
        }
        yield* Ref.update(pending, (current) => {
          const next = new Set(current);
          for (const record of records) {
            if (record._tag === 'ToolStarted') {
              next.add(settledKey(record.name, record.id));
            } else if (record._tag === 'ToolOutcome') {
              next.delete(settledKey(record.name, record.id));
            }
          }
          return next;
        });

        for (const record of records) {
          if (record._tag !== 'ToolOutcome') continue;
          const key = settledKey(record.name, record.id);
          const waiting = callbacks.get(key) ?? [];
          callbacks.delete(key);
          yield* Effect.forEach(waiting, (callback) => callback, {
            discard: true,
          });
        }
      });

    return {
      recovery: (name, toolCallId) =>
        Option.fromUndefinedOr(recoveries.get(settledKey(name, toolCallId))),
      indeterminateToolCalls: snapshot.indeterminate,
      recoveryCorruption: snapshot.corruption,
      hasPendingToolCalls: Effect.map(
        Ref.get(pending),
        (current) => current.size > 0,
      ),
      onToolSettled: (name, toolCallId, effect) => {
        const key = settledKey(name, toolCallId);
        callbacks.set(key, [...(callbacks.get(key) ?? []), effect]);
      },
      track,
    } satisfies Tracker;
  });
