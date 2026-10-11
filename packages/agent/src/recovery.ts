import { ConversationRecord } from '@sunfall/vesper-log/record';
import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Option } from 'effect';

import { AgentBranch } from './branch.js';

/**
 * The reserved `StepSuspended.wait` name for a native pre-handler interaction,
 * as opposed to an application-defined `AgentWorkflow.wait`.
 *
 * A native interaction never runs `AgentWorkflow.wait`'s handler-resumption
 * replay: the call is suspended *before* any handler is entered, so its
 * `StepSuspended` has no earlier `StepStarted` — the one shape {@link fold}
 * otherwise treats as corruption.
 * Resuming it means either dispatching the handler for the first time
 * (approval) or settling the tool directly (answer or denial);
 * see `dispatch.ts`'s `resolveIndeterminate` for where that decision is
 * made. Declared here, and re-exported from `dispatch.ts`, because this is
 * the fold that has to know about it.
 */
export const INTERACTION_WAIT = '@sunfall/vesper-agent/interaction';

/** One durable wait, followed across every run that can still see it. */
export interface Wait {
  readonly offset: ConversationRecord.Envelope['offset'];
  readonly record: ConversationRecord.RecordOf<'StepSuspended'>;
  /** A decision has been recorded for this token. */
  readonly resolved: boolean;
  /**
   * Nothing has closed the wait since: it was not restarted on this path, its
   * call did not settle, and its run did not end.
   */
  readonly open: boolean;
}

/**
 * Every wait the given records suspend, in suspension order.
 *
 * {@link fold} is scoped to the run a resume must finish, and forgets a wait
 * once that run settles. Approval and workflow services answer a different
 * question — whether a token was ever decided, whether it can still be — so
 * they read the lifecycle of each wait across runs here, from the same step
 * records. Pass the active path.
 */
export const waits = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): ReadonlyArray<Wait> => {
  const byToken = new Map<string, Wait>();
  const close = (keep: (wait: Wait) => boolean) => {
    for (const [token, wait] of byToken) {
      if (!keep(wait)) {
        byToken.set(token, { ...wait, open: false });
      }
    }
  };
  for (const { offset, record } of history) {
    switch (record._tag) {
      case 'StepSuspended':
        byToken.set(record.token, {
          offset,
          record,
          resolved: false,
          open: true,
        });
        break;
      case 'StepResolved': {
        const wait = byToken.get(record.token);
        if (wait !== undefined) {
          byToken.set(record.token, { ...wait, resolved: true });
        }
        break;
      }
      case 'StepRestarted':
        close((wait) => wait.record.token !== record.priorToken);
        break;
      case 'StepSettled':
        if (record.parent === undefined) {
          close((wait) => wait.record.id !== record.id);
        }
        break;
      case 'RunSettled':
      case 'Completed':
        close(() => false);
        break;
      case 'RunStarted':
      case 'Text':
      case 'PartialOutput':
      case 'StepOpened':
      case 'StepStarted':
      case 'TurnFinished':
      case 'Compacted':
      case 'BranchedFrom':
      case 'StateCheckpoint':
      case 'CodeStateCheckpoint':
      case 'ChildSession':
      case 'Signal':
      case 'SignalReceived':
        break;
    }
  }
  return [...byToken.values()];
};

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
  | {
      readonly _tag: 'Indeterminate';
      /** `StepStarted.replay`: whether re-running was known safe at start. */
      readonly replay: 'safe' | 'unsafe';
      /** The wait whose resolution this start resumed, if any. */
      readonly resumes?: string;
    }
  | { readonly _tag: 'Restarting' }
  | {
      readonly _tag: 'Suspended';
      readonly wait: string;
      readonly token: string;
      readonly request: unknown;
      readonly interaction?: {
        readonly name: string;
        readonly mode: 'dispatch' | 'answer';
      };
    }
  | ({ readonly _tag: 'Settled' } & Settled);

type SuspendedRecovery = Extract<Recovery, { readonly _tag: 'Suspended' }>;

/** An orphaned handler start and the provider call that originally caused it. */
export interface IndeterminateToolCall {
  readonly step: number;
  readonly name: string;
  readonly toolCallId: LogVocabulary.ToolCallId;
  readonly params: unknown;
  /** `StepOpened.inputDigest`: the call's identity beside its name and id. */
  readonly inputDigest: string;
}

/** A deliberately suspended call and the external wait that owns its replay. */
export interface SuspendedToolCall extends IndeterminateToolCall {
  readonly wait: string;
  readonly token: string;
  readonly request: unknown;
}

/**
 * What an orphaned run durably established about one code-mode nested call,
 * keyed by its parent `exec` call and the call's key within it.
 */
export interface NestedRecovery {
  readonly name: string;
  readonly inputDigest: string;
  /** Whether the handler was entered, and if so how safely it re-runs. */
  readonly started?: 'safe' | 'unsafe';
  readonly settled?: Settled;
}

export interface Snapshot {
  /** The orphaned run's calls, by {@link settledKey}, in provider order. */
  readonly calls: ReadonlyMap<string, IndeterminateToolCall>;
  /** What the orphaned run established about each of them. */
  readonly recoveries: ReadonlyMap<string, Recovery>;
  /** Nested calls of the orphaned run, by {@link nestedKey}. */
  readonly nested: ReadonlyMap<string, NestedRecovery>;
  readonly completedWaitOutcomes: ReadonlyMap<string, CompletedWait>;
  readonly pending: ReadonlyArray<IndeterminateToolCall>;
  readonly indeterminate: ReadonlyArray<IndeterminateToolCall>;
  readonly suspended: ReadonlyArray<SuspendedToolCall>;
  readonly corruption: string | undefined;
}

/** Why a run cannot be settled while one of its tool calls is still open. */
export type PendingToolState = 'none' | 'suspended' | 'indeterminate';

const settledKey = (
  name: string,
  toolCallId: LogVocabulary.ToolCallId,
): string => `${name}\u001f${toolCallId}`;

const nestedKey = (parent: string, key: string): string =>
  `${parent}\u001f${key}`;

/**
 * The one state machine over a run's durable step records.
 *
 * Both readers use it: {@link fold} applies the orphaned run's records when a
 * session opens, and {@link make}'s tracker applies each batch the current run
 * appends. One `apply` means the two cannot disagree about what a record
 * means — the property the open-time and live folds used to keep by hand.
 *
 * State is scoped to the newest run: `RunSettled` clears it, because a settled
 * run left nothing to recover.
 */
class StepFold {
  private running = false;
  readonly calls = new Map<string, IndeterminateToolCall>();
  readonly recoveries = new Map<string, Recovery>();
  readonly starts = new Map<
    string,
    { readonly name: string; readonly id: string }
  >();
  readonly order: string[] = [];
  readonly waits = new Map<string, CompletedWait>();
  readonly nested = new Map<string, NestedRecovery>();

  readonly apply = ConversationRecord.Record.match({
    RunStarted: () => {
      this.running = true;
    },
    RunSettled: () => {
      this.calls.clear();
      this.recoveries.clear();
      this.starts.clear();
      this.order.length = 0;
      this.waits.clear();
      this.nested.clear();
      this.running = false;
    },
    StepOpened: (record) => {
      if (!this.running) {
        return;
      }
      if (record.parent !== undefined) {
        this.nested.set(nestedKey(record.parent, record.id), {
          name: record.name,
          inputDigest: record.inputDigest,
        });
        return;
      }
      const key = settledKey(record.name, record.id);
      if (!this.calls.has(key)) {
        this.order.push(key);
      }
      this.calls.set(key, {
        step: record.step ?? 0,
        name: record.name,
        toolCallId: record.id,
        params: record.params,
        inputDigest: record.inputDigest,
      });
    },
    StepStarted: (record) => {
      if (!this.running) {
        return;
      }
      if (record.parent !== undefined) {
        const key = nestedKey(record.parent, record.id);
        const call = this.nested.get(key);
        if (call !== undefined) {
          this.nested.set(key, { ...call, started: record.replay });
        }
        return;
      }
      const key = settledKey(record.name, record.id);
      this.recoveries.set(key, {
        _tag: 'Indeterminate',
        replay: record.replay,
        ...(record.resumes === undefined ? {} : { resumes: record.resumes }),
      });
      this.starts.set(key, { name: record.name, id: record.id });
    },
    StepSuspended: (record) => {
      if (!this.running) {
        return;
      }
      this.recoveries.set(settledKey(record.name, record.id), {
        _tag: 'Suspended',
        wait: record.wait,
        token: record.token,
        request: record.request,
        ...(record.interaction === undefined
          ? {}
          : { interaction: record.interaction }),
      });
    },
    StepResolved: (record) => {
      if (this.running) {
        this.waits.set(record.token, {
          outcome: record.outcome,
          result: record.result,
        });
      }
    },
    StepRestarted: (record) => {
      if (this.running) {
        this.recoveries.set(settledKey(record.name, record.id), {
          _tag: 'Restarting',
        });
      }
    },
    StepSettled: (record) => {
      if (!this.running) {
        return;
      }
      const settled = { outcome: record.outcome, result: record.result };
      if (record.parent !== undefined) {
        const key = nestedKey(record.parent, record.id);
        const call = this.nested.get(key);
        if (call !== undefined) {
          this.nested.set(key, { ...call, settled });
        }
        return;
      }
      this.recoveries.set(settledKey(record.name, record.id), {
        _tag: 'Settled',
        ...settled,
      });
    },
    Text: () => {},
    PartialOutput: () => {},
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

  /** Why the run cannot settle while a call is open, from current state. */
  pendingState(): PendingToolState {
    let state: PendingToolState = 'none';
    for (const recovery of this.recoveries.values()) {
      if (recovery._tag === 'Settled') {
        continue;
      }
      if (recovery._tag !== 'Suspended') {
        return 'indeterminate';
      }
      state = 'suspended';
    }
    return state;
  }

  /** An immutable view of the state as it stands. */
  snapshot(): Snapshot {
    // Diagnose corruption only after the complete orphan suffix is folded.
    const unmatched = [...this.starts].find(([key]) => {
      const recovery = this.recoveries.get(key);
      return recovery?._tag !== 'Settled' && !this.calls.has(key);
    })?.[1];
    const unmatchedSuspension = [...this.recoveries].flatMap(
      ([key, recovery]): ReadonlyArray<SuspendedRecovery> =>
        recovery._tag === 'Suspended' &&
        recovery.wait !== INTERACTION_WAIT &&
        !this.starts.has(key)
          ? [recovery]
          : [],
    )[0];
    const listed = (keep: (recovery: Recovery | undefined) => boolean) =>
      this.order.flatMap((key) => {
        const call = this.calls.get(key);
        return call !== undefined && keep(this.recoveries.get(key))
          ? [call]
          : [];
      });

    return {
      calls: new Map(this.calls),
      recoveries: new Map(this.recoveries),
      nested: new Map(this.nested),
      completedWaitOutcomes: new Map(this.waits),
      corruption:
        unmatchedSuspension !== undefined
          ? `Cannot recover suspended wait ${unmatchedSuspension.wait}: ` +
            'durable StepSuspended has no earlier StepStarted'
          : unmatched === undefined
            ? undefined
            : `Cannot recover indeterminate tool ${unmatched.name} (${unmatched.id}): ` +
              'durable StepStarted has no StepOpened',
      pending: listed((recovery) => recovery?._tag !== 'Settled'),
      indeterminate: listed((recovery) => recovery?._tag === 'Indeterminate'),
      suspended: this.order.flatMap((key): ReadonlyArray<SuspendedToolCall> => {
        const recovery = this.recoveries.get(key);
        const call = this.calls.get(key);
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
  }
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
  const steps = new StepFold();
  for (const { record } of AgentBranch.activePath(history)) {
    steps.apply(record);
  }
  return steps.snapshot();
};

export interface Tracker {
  /**
   * What the orphaned run recorded for one nested call. Like {@link prior},
   * it reads only what predates this run, so a run never serves itself.
   */
  readonly nestedRecovery: (
    parent: LogVocabulary.ToolCallId,
    key: string,
  ) => Option.Option<NestedRecovery>;
  /** The live state of one call, including what this run has recorded. */
  readonly recovery: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
  ) => Option.Option<Recovery>;
  /**
   * The state of a call the orphaned run made, when this is that same call.
   *
   * Only a call the previous run recorded, with the same name, id, and
   * parameters, is the call being recovered. Providers reuse ids across turns
   * (`call_0` on every turn is common), so an id alone does not identify a
   * call: a new call that happens to share one must run, not be answered with
   * another call's result. Identity comes from the open-time snapshot; the
   * state is live, because recovery may already have settled the call.
   */
  readonly prior: (
    name: string,
    toolCallId: LogVocabulary.ToolCallId,
    inputDigest: string,
  ) => Option.Option<Recovery>;
  /** The orphaned run's open calls, as of open. */
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
 * The orphaned run's records are folded once, at open, into {@link StepFold};
 * the current run's appends go through the same fold. The open-time
 * {@link Snapshot} is kept beside it as the one source of "what did the
 * previous run do", so serving from the log cannot reach a call this run made.
 * Callbacks here ensure a cancellation waiter is released only after its
 * StepSettled is durable.
 */
export const make = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
): Effect.Effect<Tracker> =>
  Effect.sync(() => {
    const steps = new StepFold();
    for (const { record } of AgentBranch.activePath(history)) {
      steps.apply(record);
    }
    const opened = steps.snapshot();
    const callbacks = new Map<string, Array<Effect.Effect<void>>>();

    const notify = (record: ConversationRecord.Record): Effect.Effect<void> => {
      if (record._tag !== 'StepSettled' || record.parent !== undefined) {
        return Effect.void;
      }
      const key = settledKey(record.name, record.id);
      const waiting = callbacks.get(key) ?? [];
      callbacks.delete(key);
      return Effect.forEach(waiting, (callback) => callback, {
        discard: true,
      });
    };

    const track = (records: ReadonlyArray<ConversationRecord.Record>) =>
      Effect.suspend(() => {
        for (const record of records) {
          steps.apply(record);
        }
        return Effect.forEach(records, notify, { discard: true });
      });

    return {
      nestedRecovery: (parent, key) =>
        Option.fromUndefinedOr(opened.nested.get(nestedKey(parent, key))),
      recovery: (name, toolCallId) =>
        Option.fromUndefinedOr(
          steps.recoveries.get(settledKey(name, toolCallId)),
        ),
      prior: (name, toolCallId, inputDigest) => {
        const key = settledKey(name, toolCallId);
        return opened.calls.get(key)?.inputDigest === inputDigest
          ? Option.fromUndefinedOr(steps.recoveries.get(key))
          : Option.none();
      },
      pendingToolCalls: opened.pending,
      indeterminateToolCalls: opened.indeterminate,
      suspendedToolCalls: opened.suspended,
      recoveryCorruption: opened.corruption,
      hasCompletedWait: (token) => steps.waits.has(token),
      completedWait: (token) => Option.fromNullishOr(steps.waits.get(token)),
      pendingToolState: Effect.sync(() => steps.pendingState()),
      hasPendingToolCalls: Effect.sync(() => steps.pendingState() !== 'none'),
      onToolSettled: (name, toolCallId, effect) => {
        const key = settledKey(name, toolCallId);
        callbacks.set(key, [...(callbacks.get(key) ?? []), effect]);
      },
      track,
    } satisfies Tracker;
  });
