import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Schema } from 'effect';
import { Prompt } from 'effect/ai';

import { AgentBranch } from './branch.js';
import type { Stop } from './stop.js';

export interface Completed {
  readonly text: string;
  readonly outcome: 'success' | 'cancelled';
  readonly steps: number;
  readonly usage: Stop.Usage;
  readonly response?: Prompt.PromptEncoded;
}

const decodeResponse = (response: unknown): Prompt.PromptEncoded =>
  Schema.decodeUnknownSync(Schema.toEncoded(Prompt.Prompt))(response);

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
        completed: {
          text: record.text,
          steps: record.steps,
          usage: record.usage,
          outcome: record.outcome,
          ...(record.response === undefined
            ? {}
            : { response: decodeResponse(record.response) }),
        },
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
                    text: record.resume.completed.text,
                    steps: record.resume.completed.steps,
                    usage: record.resume.completed.usage,
                    outcome: record.resume.completed.outcome,
                    ...(record.resume.completed.response === undefined
                      ? {}
                      : {
                          response: decodeResponse(
                            record.resume.completed.response,
                          ),
                        }),
                  },
            latestTurnUsage: record.resume.latestTurnUsage,
          };
    case 'Text':
    case 'PartialOutput':
    case 'StepOpened':
    case 'StepStarted':
    case 'StepSuspended':
    case 'StepResolved':
    case 'StepRestarted':
    case 'StepSettled':
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
    case 'PartialOutput':
    case 'StepOpened':
    case 'StepStarted':
    case 'StepSuspended':
    case 'StepResolved':
    case 'StepRestarted':
    case 'StepSettled':
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

type Checkpoint = ConversationRecord.RecordOf<'CodeStateCheckpoint'>;

export interface CodeState {
  /** The latest checkpoint in effect. */
  readonly committed: Checkpoint | undefined;
  /** Checkpoints whose `exec` step has not settled, by that step's id. */
  readonly pending: ReadonlyMap<string, Checkpoint>;
}

export const emptyCodeState: CodeState = {
  committed: undefined,
  pending: new Map(),
};

/** Fold one record into the durable code scratch-state projection. */
export const updateCodeState = (
  current: CodeState,
  record: ConversationRecord.Record,
): CodeState => {
  switch (record._tag) {
    case 'CodeStateCheckpoint':
      return record.producer === undefined
        ? { ...current, committed: record }
        : {
            ...current,
            pending: new Map(current.pending).set(record.producer, record),
          };
    case 'StepSettled': {
      const held =
        record.parent === undefined
          ? current.pending.get(record.id)
          : undefined;
      if (held === undefined) {
        return current;
      }
      const pending = new Map(current.pending);
      pending.delete(record.id);
      return { committed: held, pending };
    }
    // A new run abandons what a crashed one left pending: a retried `exec`
    // commits again, and one answered without running commits nothing.
    case 'RunStarted':
      return current.pending.size === 0
        ? current
        : { ...current, pending: new Map() };
    case 'RunSettled':
      return {
        committed:
          record.resume?.codeState === undefined
            ? current.committed
            : { _tag: 'CodeStateCheckpoint', state: record.resume.codeState },
        pending: new Map(),
      };
    case 'Text':
    case 'PartialOutput':
    case 'StepOpened':
    case 'StepStarted':
    case 'StepSuspended':
    case 'StepResolved':
    case 'StepRestarted':
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
    emptyCodeState,
  );

export * as ResumeProjection from './resume-projection.js';
