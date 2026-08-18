import type { Stop } from '../stop.js';
import { AgentEvents } from '../event.js';

export const turnStarted = (step: number): AgentEvents.Lifecycle =>
  AgentEvents.Lifecycle.cases.TurnStarted.make({ step });

export const turnFinished = (
  step: number,
  usage: Stop.Usage,
): AgentEvents.Lifecycle =>
  AgentEvents.Lifecycle.cases.TurnFinished.make({ step, usage });

export const completed = (
  text: string,
  steps: number,
  usage: Stop.Usage,
  outcome: 'success' | 'cancelled',
): AgentEvents.Lifecycle =>
  AgentEvents.Lifecycle.cases.Completed.make({ outcome, text, steps, usage });

export const signalled = (
  step: number,
  signal: {
    readonly kind: 'steer' | 'cancel';
    readonly text: string;
    readonly source: string;
    readonly at: string;
  },
): AgentEvents.Lifecycle =>
  AgentEvents.Lifecycle.cases.Signalled.make({ step, ...signal });

export const signalRejected = (
  step: number,
  signal: {
    readonly kind: 'steer' | 'cancel';
    readonly text: string;
    readonly source: string;
    readonly at: string;
  },
  exhaustion: {
    readonly limit: 'signal_bytes' | 'signals_per_boundary' | 'steered_bytes';
    readonly used: number;
    readonly maximum: number;
  },
): AgentEvents.Lifecycle =>
  AgentEvents.Lifecycle.cases.SignalRejected.make({
    step,
    ...signal,
    reason: exhaustion.limit,
    used: exhaustion.used,
    maximum: exhaustion.maximum,
  });

export const signalBacklog = (
  step: number,
  maximum: number,
): AgentEvents.Lifecycle =>
  AgentEvents.Lifecycle.cases.SignalBacklog.make({ step, maximum });

export const compacted = (
  step: number,
  summarized: {
    readonly summary: string;
    readonly summarizedMessages: number;
    readonly keptMessages: number;
  },
): AgentEvents.Lifecycle =>
  AgentEvents.Lifecycle.cases.Compacted.make({ step, ...summarized });

export * as AgentEventRuntime from './event.js';
