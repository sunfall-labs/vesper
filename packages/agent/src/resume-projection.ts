import type { ConversationRecord } from '@sunfall/vesper-log/record';

import { AgentBranch } from './branch.js';
import type { Stop } from './stop.js';

export interface Completed {
  readonly text: string;
  readonly outcome: 'success' | 'cancelled';
  readonly steps: number;
  readonly usage: Stop.Usage;
}

export interface Active {
  readonly completed: Completed | undefined;
  readonly latestTurnUsage: Stop.Usage | undefined;
  readonly previousTurnUsage: Stop.Usage;
  readonly compactedSinceTurn: boolean;
}

export const empty = (): Active => ({
  completed: undefined,
  latestTurnUsage: undefined,
  previousTurnUsage: { input: 0, output: 0 },
  compactedSinceTurn: false,
});

/** Fold one active-path record into the bounded state written at settlement. */
export const update = (
  current: Active,
  record: ConversationRecord.Record,
): Active => {
  switch (record._tag) {
    case 'RunStarted':
      return {
        ...current,
        completed: undefined,
        previousTurnUsage: { input: 0, output: 0 },
      };
    case 'Completed':
      return {
        ...current,
        completed: { ...record, outcome: record.outcome ?? 'success' },
      };
    case 'Compacted':
      return { ...current, compactedSinceTurn: true };
    case 'TurnFinished':
      return {
        ...current,
        latestTurnUsage: current.compactedSinceTurn
          ? undefined
          : {
              input: record.usage.input - current.previousTurnUsage.input,
              output: record.usage.output - current.previousTurnUsage.output,
            },
        previousTurnUsage: record.usage,
        compactedSinceTurn: false,
      };
    case 'RunSettled':
      return record.resume === undefined
        ? current
        : {
            ...current,
            completed:
              record.resume.completed === undefined
                ? undefined
                : {
                    ...record.resume.completed,
                    outcome: record.resume.completed.outcome ?? 'success',
                  },
            latestTurnUsage: record.resume.latestTurnUsage,
          };
    case 'Text':
    case 'ToolCall':
    case 'ToolStarted':
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
    case 'ToolOutcome':
    case 'BranchedFrom':
    case 'StateCheckpoint':
    case 'CodeStateCheckpoint':
    case 'ChildSession':
    case 'Signal':
    case 'SignalReceived':
      return current;
    default: {
      const _exhaustive: never = record;
      return _exhaustive;
    }
  }
};

/** Project the branch-aware conversation facts used to continue a run. */
export const activeFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): Active =>
  AgentBranch.activePath(records).reduce(
    (current, { record }) => update(current, record),
    empty(),
  );

export type State = ConversationRecord.RecordOf<'StateCheckpoint'> | undefined;

/** Fold one record into the durable State checkpoint projection. */
export const updateState = (
  current: State,
  record: ConversationRecord.Record,
): State => {
  switch (record._tag) {
    case 'StateCheckpoint':
      return record;
    case 'RunSettled':
      return record.resume?.state !== undefined
        ? { _tag: 'StateCheckpoint', ...record.resume.state }
        : current;
    case 'RunStarted':
    case 'Text':
    case 'ToolCall':
    case 'ToolStarted':
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
    case 'ToolOutcome':
    case 'TurnFinished':
    case 'Compacted':
    case 'BranchedFrom':
    case 'Completed':
    case 'ChildSession':
    case 'Signal':
    case 'SignalReceived':
    case 'CodeStateCheckpoint':
      return current;
    default: {
      const _exhaustive: never = record;
      return _exhaustive;
    }
  }
};

/** Select the latest durable State checkpoint on the active branch. */
export const stateFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): State =>
  AgentBranch.activePath(records).reduce<State>(
    (current, { record }) => updateState(current, record),
    undefined,
  );

export type CodeState =
  | ConversationRecord.RecordOf<'CodeStateCheckpoint'>
  | undefined;

/** Fold one record into the durable code scratch-state projection. */
export const updateCodeState = (
  current: CodeState,
  record: ConversationRecord.Record,
): CodeState => {
  switch (record._tag) {
    case 'CodeStateCheckpoint':
      return record;
    case 'RunSettled':
      return record.resume?.codeState === undefined
        ? current
        : { _tag: 'CodeStateCheckpoint', state: record.resume.codeState };
    case 'RunStarted':
    case 'Text':
    case 'ToolCall':
    case 'ToolStarted':
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
    case 'ToolOutcome':
    case 'TurnFinished':
    case 'Compacted':
    case 'BranchedFrom':
    case 'Completed':
    case 'StateCheckpoint':
    case 'ChildSession':
    case 'Signal':
    case 'SignalReceived':
      return current;
    default: {
      const _exhaustive: never = record;
      return _exhaustive;
    }
  }
};

/** Select the latest durable code scratch checkpoint on the active branch. */
export const codeStateFrom = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): CodeState =>
  AgentBranch.activePath(records).reduce<CodeState>(
    (current, { record }) => updateCodeState(current, record),
    undefined,
  );

export * as ResumeProjection from './resume-projection.js';
