import { ConversationRecord } from '@sunfall/vesper-log/record';
import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Option, Ref } from 'effect';

import { AgentBranch } from './branch.js';

/**
 * The reserved `ToolSuspended.wait` name for a tool's own `needsApproval`
 * gate, as opposed to an application-defined `AgentWorkflow.wait`.
 *
 * A durable approval never runs `AgentWorkflow.wait`'s handler-resumption
 * replay: the call is suspended by `effect/unstable/ai`'s `LanguageModel`
 * *before* any handler is entered, so its `ToolSuspended` has no matching
 * `ToolStarted` — the one shape {@link fold} otherwise treats as corruption.
 * Resuming it means either genuinely dispatching the handler for the first
 * time (approved) or settling a refusal without ever entering it (denied);
 * see `dispatch.ts`'s `resolveIndeterminate` for where that decision is
 * made. Declared here, and re-exported from `dispatch.ts`, because this is
 * the fold that has to know about it.
 */
export const APPROVAL_WAIT = '@sunfall/vesper-agent/approval';

/** How a tool call ended, as a previous run recorded it. */
export interface Settled {
  readonly outcome: 'success' | 'failure';
  /** The encoded result, in the form the provider was shown. */
  readonly result: unknown;
}

/** What an external actor durably decided for one wait's token. */
export interface CompletedWait {
  readonly outcome: 'success' | 'failure';
  readonly result: unknown;
}

/** What an orphaned run durably established about a tool call. */
export type Recovery =
  | { readonly _tag: 'Indeterminate' }
  | { readonly _tag: 'Restarting' }
  | {
      readonly _tag: 'Suspended';
      readonly wait: string;
      readonly token: string;
      readonly request: unknown;
    }
  | ({ readonly _tag: 'Settled' } & Settled);

type SuspendedRecovery = Extract<Recovery, { readonly _tag: 'Suspended' }>;

/** An orphaned handler start and the provider call that originally caused it. */
export interface IndeterminateToolCall {
  readonly step: number;
  readonly name: string;
  readonly toolCallId: LogVocabulary.ToolCallId;
  readonly params: unknown;
}

/** A deliberately suspended call and the external wait that owns its replay. */
export interface SuspendedToolCall extends IndeterminateToolCall {
  readonly wait: string;
  readonly token: string;
  readonly request: unknown;
}

export interface Snapshot {
  readonly recoveries: Map<string, Recovery>;
  readonly completedWaitOutcomes: ReadonlyMap<string, CompletedWait>;
  readonly pending: ReadonlyArray<IndeterminateToolCall>;
  readonly indeterminate: ReadonlyArray<IndeterminateToolCall>;
  readonly suspended: ReadonlyArray<SuspendedToolCall>;
  readonly corruption: string | undefined;
}

/** Why a run cannot be settled while one of its tool calls is still open. */
export type PendingToolState = 'none' | 'suspended' | 'indeterminate';

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
  const completedWaitOutcomes = new Map<string, CompletedWait>();
  let running = false;

  const foldRecord = ConversationRecord.Record.match({
    RunStarted: () => {
      running = true;
    },
    RunSettled: () => {
      recoveries.clear();
      calls.clear();
      starts.clear();
      completedWaitOutcomes.clear();
      order.length = 0;
      running = false;
    },
    ToolCall: (record) => {
      if (running) {
        const key = settledKey(record.name, record.id);
        if (!calls.has(key)) {
          order.push(key);
        }
        calls.set(key, {
          step: record.step,
          name: record.name,
          toolCallId: record.id,
          params: record.params,
        });
      }
    },
    ToolStarted: (record) => {
      if (running) {
        const key = settledKey(record.name, record.id);
        recoveries.set(key, { _tag: 'Indeterminate' });
        starts.set(key, { name: record.name, id: record.id });
      }
    },
    ToolSuspended: (record) => {
      if (running) {
        recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Suspended',
          wait: record.wait,
          token: record.token,
          request: record.request,
        });
      }
    },
    ToolWaitCompleted: (record) => {
      if (running) {
        completedWaitOutcomes.set(record.token, {
          outcome: record.outcome,
          result: record.result,
        });
      }
    },
    ToolWaitRestarted: (record) => {
      if (running) {
        recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Restarting',
        });
      }
    },
    ToolOutcome: (record) => {
      if (running) {
        recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Settled',
          outcome: record.outcome,
          result: record.result,
        });
      }
    },
    Text: () => {},
    ToolResumed: () => {},
    TurnFinished: () => {},
    Compacted: () => {},
    BranchedFrom: () => {},
    Completed: () => {},
    StateCheckpoint: () => {},
    CodeStateCheckpoint: () => {},
    ChildSession: () => {},
    Signal: () => {},
    SignalReceived: () => {},
  });
  for (const { record } of AgentBranch.activePath(history)) {
    foldRecord(record);
  }

  // Dispatch commits before entering the handler, while ToolCall arrives via
  // the provider event stream. Diagnose corruption only after the complete
  // orphan suffix is folded.
  const unmatched = [...starts].find(([key]) => {
    const recovery = recoveries.get(key);
    return recovery?._tag !== 'Settled' && !calls.has(key);
  })?.[1];
  const unmatchedSuspension = [...recoveries].flatMap(
    ([key, recovery]): ReadonlyArray<SuspendedRecovery> =>
      recovery._tag === 'Suspended' &&
      recovery.wait !== APPROVAL_WAIT &&
      !starts.has(key)
        ? [recovery]
        : [],
  )[0];

  return {
    recoveries,
    completedWaitOutcomes,
    corruption:
      unmatchedSuspension !== undefined
        ? `Cannot recover suspended wait ${unmatchedSuspension.wait}: ` +
          'durable ToolSuspended has no matching ToolStarted'
        : unmatched === undefined
          ? undefined
          : `Cannot recover indeterminate tool ${unmatched.name} (${unmatched.id}): ` +
            'durable ToolStarted has no matching ToolCall',
    pending: order.flatMap((key) => {
      const recovery = recoveries.get(key);
      const call = calls.get(key);
      return recovery?._tag !== 'Settled' && call !== undefined ? [call] : [];
    }),
    indeterminate: order.flatMap((key) => {
      const recovery = recoveries.get(key);
      const call = calls.get(key);
      return recovery?._tag === 'Indeterminate' && call !== undefined
        ? [call]
        : [];
    }),
    suspended: order.flatMap((key): ReadonlyArray<SuspendedToolCall> => {
      const recovery = recoveries.get(key);
      const call = calls.get(key);
      return recovery?._tag === 'Suspended' && call !== undefined
        ? [
            {
              ...call,
              wait: recovery.wait,
              token: recovery.token,
              request: recovery.request,
            },
          ]
        : [];
    }),
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
  readonly pendingToolCalls: ReadonlyArray<IndeterminateToolCall>;
  readonly indeterminateToolCalls: ReadonlyArray<IndeterminateToolCall>;
  readonly suspendedToolCalls: ReadonlyArray<SuspendedToolCall>;
  readonly recoveryCorruption: string | undefined;
  /** Whether this wait result is already present in the conversation audit. */
  readonly hasCompletedWait: (token: string) => boolean;
  /** The durable decision recorded for one wait's token, if any. */
  readonly completedWait: (token: string) => Option.Option<CompletedWait>;
  /** Distinguish an intentional durable wait from an unsafe orphan. */
  readonly pendingToolState: Effect.Effect<PendingToolState>;
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
          recovery._tag === 'Indeterminate' ||
          recovery._tag === 'Suspended' ||
          recovery._tag === 'Restarting'
            ? [key]
            : [],
        ),
      ),
    );
    const callbacks = new Map<string, Array<Effect.Effect<void>>>();
    const completedWaitOutcomes = new Map(snapshot.completedWaitOutcomes);

    // These matchers close over the tracker state, not an individual append,
    // so build them once per session rather than once per tracked batch.
    const trackRecord = ConversationRecord.Record.match({
      ToolStarted: (record) => {
        recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Indeterminate',
        });
      },
      ToolSuspended: (record) => {
        recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Suspended',
          wait: record.wait,
          token: record.token,
          request: record.request,
        });
      },
      ToolWaitCompleted: (record) => {
        completedWaitOutcomes.set(record.token, {
          outcome: record.outcome,
          result: record.result,
        });
      },
      ToolWaitRestarted: (record) => {
        recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Restarting',
        });
      },
      ToolOutcome: (record) => {
        recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Settled',
          outcome: record.outcome,
          result: record.result,
        });
      },
      RunStarted: () => {},
      Text: () => {},
      ToolCall: () => {},
      ToolResumed: () => {},
      TurnFinished: () => {},
      Compacted: () => {},
      BranchedFrom: () => {},
      Completed: () => {},
      StateCheckpoint: () => {},
      CodeStateCheckpoint: () => {},
      ChildSession: () => {},
      Signal: () => {},
      SignalReceived: () => {},
      RunSettled: () => {},
    });

    const updatePending = (
      next: Set<string>,
      record: ConversationRecord.Record,
    ): void => {
      switch (record._tag) {
        case 'ToolStarted':
        case 'ToolSuspended':
        case 'ToolWaitRestarted':
          next.add(settledKey(record.name, record.id));
          return;
        case 'ToolOutcome':
          next.delete(settledKey(record.name, record.id));
          return;
        case 'RunStarted':
        case 'Text':
        case 'ToolCall':
        case 'ToolResumed':
        case 'ToolWaitCompleted':
        case 'TurnFinished':
        case 'Compacted':
        case 'BranchedFrom':
        case 'Completed':
        case 'StateCheckpoint':
        case 'CodeStateCheckpoint':
        case 'ChildSession':
        case 'Signal':
        case 'SignalReceived':
        case 'RunSettled':
          return;
        default: {
          const exhaustive: never = record;
          return exhaustive;
        }
      }
    };

    const notifyRecord = ConversationRecord.Record.match({
      ToolOutcome: (record) => {
        const key = settledKey(record.name, record.id);
        const waiting = callbacks.get(key) ?? [];
        callbacks.delete(key);
        return Effect.forEach(waiting, (callback) => callback, {
          discard: true,
        });
      },
      RunStarted: () => Effect.void,
      Text: () => Effect.void,
      ToolCall: () => Effect.void,
      ToolStarted: () => Effect.void,
      ToolSuspended: () => Effect.void,
      ToolResumed: () => Effect.void,
      ToolWaitCompleted: () => Effect.void,
      ToolWaitRestarted: () => Effect.void,
      TurnFinished: () => Effect.void,
      Compacted: () => Effect.void,
      BranchedFrom: () => Effect.void,
      Completed: () => Effect.void,
      StateCheckpoint: () => Effect.void,
      CodeStateCheckpoint: () => Effect.void,
      ChildSession: () => Effect.void,
      Signal: () => Effect.void,
      SignalReceived: () => Effect.void,
      RunSettled: () => Effect.void,
    });

    const track = (records: ReadonlyArray<ConversationRecord.Record>) =>
      Effect.gen(function* () {
        for (const record of records) {
          trackRecord(record);
        }
        yield* Ref.update(pending, (current) => {
          const next = new Set(current);
          for (const record of records) {
            updatePending(next, record);
          }
          return next;
        });

        for (const record of records) {
          yield* notifyRecord(record);
        }
      });

    return {
      recovery: (name, toolCallId) =>
        Option.fromUndefinedOr(recoveries.get(settledKey(name, toolCallId))),
      pendingToolCalls: snapshot.pending,
      indeterminateToolCalls: snapshot.indeterminate,
      suspendedToolCalls: snapshot.suspended,
      recoveryCorruption: snapshot.corruption,
      hasCompletedWait: (token) => completedWaitOutcomes.has(token),
      completedWait: (token) =>
        Option.fromNullishOr(completedWaitOutcomes.get(token)),
      pendingToolState: Effect.map(Ref.get(pending), (current) => {
        if (current.size === 0) {
          return 'none';
        }
        for (const key of current) {
          if (recoveries.get(key)?._tag !== 'Suspended') {
            return 'indeterminate';
          }
        }
        return 'suspended';
      }),
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
