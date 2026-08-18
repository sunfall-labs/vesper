import type { LogStore } from '@sunfall/vesper-log/log-store';
import type { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Crypto, Effect, Stream } from 'effect';
import type { Prompt } from 'effect/unstable/ai';

import { Agent } from './agent.js';
import {
  CompatibilityError,
  SuspendedConversationError,
} from './conversation-error.js';
import type { AgentEvents } from './event.js';
import { foldToResult } from './internal/fold-to-result.js';
import * as AgentLog from './log.js';
import { RecordingPolicy } from './recording-policy.js';
import { append as appendSignal } from './internal/signal-store.js';
import { protocolOf } from './internal/protocol.js';

/** Out-of-band input addressed to a durable conversation. */
export interface Signal {
  readonly kind: 'steer' | 'cancel';
  /** Steering text, or a cancellation reason. */
  readonly text: string;
  /** User or service that sent the signal. */
  readonly source: string;
}

/** How a new conversation path treats a tool currently awaiting external input. */
export interface PathOptions {
  /** Re-enter the recorded provider call and issue a fresh wait token. */
  readonly pendingWait?: 'restart';
}

/** Durable external-wait records exposed to approval and worker services. */
export type WaitRecord =
  | ConversationRecord.RecordOf<'ToolSuspended'>
  | ConversationRecord.RecordOf<'ToolResumed'>
  | ConversationRecord.RecordOf<'ToolWaitCompleted'>
  | ConversationRecord.RecordOf<'ToolWaitRestarted'>;

/** A conversation envelope statically narrowed to an external-wait record. */
export type WaitEnvelope = Omit<ConversationRecord.Envelope, 'record'> & {
  readonly record: WaitRecord;
};

/** Narrow an ordinary conversation record to the durable wait lifecycle. */
export const isWaitEnvelope = (
  envelope: ConversationRecord.Envelope,
): envelope is WaitEnvelope => {
  switch (envelope.record._tag) {
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
      return true;
    default:
      return false;
  }
};

type ConcreteAgent = Agent.Any;

/** Recoverable failures from continuing a durable conversation. */
export type Error<A extends ConcreteAgent> =
  | Agent.Error<A>
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | AgentLog.DurabilityError;

export interface Instance<
  A extends ConcreteAgent,
  Requires = Agent.Requires<A>,
> {
  /** Stable durable identity shared by runs, records, and signals. */
  readonly id: LogVocabulary.ConversationId;
  /** Continue this conversation, starting it when no durable history exists. */
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<
    Agent.Result,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /** Stream the same durable continuation that {@link run} folds. */
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<Agent.Tools<A>>,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /**
   * Re-root this conversation after `at` and continue it.
   *
   * Later records remain auditable but leave the active prompt. Branches share
   * this conversation's stream and are therefore sequential.
   */
  readonly branchFrom: (
    at: LogOffset.Offset,
    input: Prompt.RawInput,
    options?: PathOptions,
  ) => Effect.Effect<
    Agent.Result,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /**
   * Copy the active prefix through `at` into a new conversation and run it.
   *
   * The destination has an independent stream, so sibling forks may run
   * concurrently without fencing each other or their source.
   */
  readonly forkFrom: (
    at: LogOffset.Offset,
    targetConversationId: string,
    input: Prompt.RawInput,
    options?: PathOptions,
  ) => Effect.Effect<
    Agent.Result,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /** Read a finite snapshot of records currently stored after `after`. */
  readonly records: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<
    ConversationRecord.Envelope,
    LogStore.LogStoreError,
    LogStore.Service
  >;
  /** Replay records after `after`, then follow future appends until interrupted. */
  readonly follow: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<
    ConversationRecord.Envelope,
    LogStore.LogStoreError,
    LogStore.Service
  >;
  /** Read a finite, typed snapshot of external wait lifecycle records. */
  readonly waits: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<WaitEnvelope, LogStore.LogStoreError, LogStore.Service>;
  /** Replay and follow typed external wait lifecycle records. */
  readonly followWaits: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<WaitEnvelope, LogStore.LogStoreError, LogStore.Service>;
  /** Persist an out-of-band steer or cancellation for the next run boundary. */
  readonly send: (
    signal: Signal,
  ) => Effect.Effect<
    void,
    LogStore.LogStoreError,
    LogStore.Service | Crypto.Crypto
  >;
}

const bind = <A extends ConcreteAgent, PolicyRequires = never>(
  agent: A,
  conversationId: string,
  policy?: RecordingPolicy.Policy<PolicyRequires>,
): Instance<A, Agent.Requires<A> | PolicyRequires> => {
  const id = LogVocabulary.ConversationId.make(conversationId);
  const protocol = protocolOf<
    Agent.Requires<A>,
    Agent.Error<A>,
    Agent.Tools<A>
  >(agent);
  if (protocol === undefined) {
    throw new Error('Conversation agent was not created by Agent.make');
  }
  const streamFrom = (
    input: Prompt.RawInput,
    options?: {
      readonly branchFrom?: LogOffset.Offset;
      readonly forkConversationId?: LogVocabulary.ConversationId;
      readonly pendingWait?: 'restart';
    },
  ) =>
    protocol.stream(
      id,
      input,
      policy === undefined ? options : { ...options, policy },
    );
  return {
    id,
    run: (input) => foldToResult(streamFrom(input)),
    stream: (input) => streamFrom(input),
    branchFrom: (at, input, options) =>
      foldToResult(
        streamFrom(input, {
          branchFrom: at,
          ...(options?.pendingWait === undefined
            ? {}
            : { pendingWait: options.pendingWait }),
        }),
      ),
    forkFrom: (at, targetConversationId, input, options) =>
      foldToResult(
        streamFrom(input, {
          branchFrom: at,
          forkConversationId:
            LogVocabulary.ConversationId.make(targetConversationId),
          ...(options?.pendingWait === undefined
            ? {}
            : { pendingWait: options.pendingWait }),
        }),
      ),
    records: (after) => AgentLog.snapshot(id, after),
    follow: (after) => AgentLog.follow(id, after),
    waits: (after) =>
      AgentLog.snapshot(id, after).pipe(Stream.filter(isWaitEnvelope)),
    followWaits: (after) =>
      AgentLog.follow(id, after).pipe(Stream.filter(isWaitEnvelope)),
    send: (signal) => appendSignal(id, signal),
  };
};

/** Bind an agent to one durable conversation identity. */
export function make<A extends ConcreteAgent>(
  agent: A,
  conversationId: string,
): Instance<A>;
/** Bind an agent with one recording policy applied across every durable run. */
export function make<A extends ConcreteAgent, const P extends object>(
  agent: A,
  conversationId: string,
  policy: P & RecordingPolicy.Policy<RecordingPolicy.Services<P>>,
): Instance<A, Agent.Requires<A> | RecordingPolicy.Services<P>>;
export function make<A extends ConcreteAgent, const P extends object>(
  agent: A,
  conversationId: string,
  policy?: P & RecordingPolicy.Policy<RecordingPolicy.Services<P>>,
): Instance<A, Agent.Requires<A> | RecordingPolicy.Services<P>> {
  return bind(agent, conversationId, policy);
}

export { CompatibilityError, SuspendedConversationError };
export { DurabilityError } from './log.js';

export * as Conversation from './conversation.js';
