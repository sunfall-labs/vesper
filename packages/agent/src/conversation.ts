import type { LogStore } from '@sunfall/vesper-log/log-store';
import type { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Stream } from 'effect';
import type { Prompt } from 'effect/unstable/ai';
import type { Tool } from 'effect/unstable/ai';

import { Agent } from './agent.js';
import type { AgentEvents } from './event.js';
import { protocolOf } from './internal/protocol.js';
import { AgentLog } from './log.js';
import { RecordingPolicy } from './recording-policy.js';
import { append as appendSignal } from './internal/signal-store.js';

/** Out-of-band input addressed to a durable conversation. */
export interface Signal {
  readonly kind: 'steer' | 'cancel';
  /** Steering text, or a cancellation reason. */
  readonly text: string;
  /** User or service that sent the signal. */
  readonly source: string;
}

export interface Instance<
  Requires,
  Error,
  Tools extends Record<string, Tool.Any>,
> {
  /** Stable durable identity shared by runs, records, and signals. */
  readonly id: string;
  /** Start a recorded run and fold its event stream into the final result. */
  readonly run: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Agent.Result, Error, Requires | LogStore.Service>;
  /** Start a recorded run and observe its model and lifecycle events live. */
  readonly stream: (
    input: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<Tools>,
    Error,
    Requires | LogStore.Service
  >;
  /** Continue from the active end of this conversation's durable history. */
  readonly resume: (
    input: Prompt.RawInput,
  ) => Effect.Effect<Agent.Result, Error, Requires | LogStore.Service>;
  /**
   * Re-root this conversation after `at` and continue it.
   *
   * Later records remain auditable but leave the active prompt. Branches share
   * this conversation's stream and are therefore sequential.
   */
  readonly branchFrom: (
    at: LogOffset.Offset,
    input: Prompt.RawInput,
  ) => Effect.Effect<Agent.Result, Error, Requires | LogStore.Service>;
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
  ) => Effect.Effect<Agent.Result, Error, Requires | LogStore.Service>;
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
  /** Persist an out-of-band steer or cancellation for the next run boundary. */
  readonly send: (
    signal: Signal,
  ) => Effect.Effect<void, LogStore.LogStoreError, LogStore.Service>;
}

type ConcreteAgent = Agent.Any;

const bind = <A extends ConcreteAgent>(
  agent: A,
  conversationId: string,
  policy?: RecordingPolicy.Policy<never>,
): Instance<
  Agent.Requires<A>,
  Agent.Error<A> | AgentLog.CompatibilityError,
  Agent.Tools<A>
> => {
  const protocol = protocolOf<Agent.Requires<A>, Agent.Tools<A>>(agent);
  if (protocol === undefined) {
    throw new Error('Conversation agent was not created by Agent.make');
  }
  const stream = (input: Prompt.RawInput) =>
    protocol.record(conversationId, input, policy) as Instance<
      Agent.Requires<A>,
      Agent.Error<A> | AgentLog.CompatibilityError,
      Agent.Tools<A>
    >['stream'] extends (input: Prompt.RawInput) => infer S
      ? S
      : never;
  const continueFrom = (
    input: Prompt.RawInput,
    options?: {
      readonly branchFrom?: LogOffset.Offset;
      readonly forkConversationId?: string;
    },
  ) =>
    protocol.continue(
      conversationId,
      input,
      policy === undefined ? options : { ...options, policy },
    ) as Effect.Effect<
      Agent.Result,
      Agent.Error<A> | AgentLog.CompatibilityError,
      Agent.Requires<A> | LogStore.Service
    >;
  return {
    id: conversationId,
    run: (input) =>
      stream(input).pipe(
        Stream.runFold(
          (): Agent.Result | undefined => undefined,
          (result, event) =>
            event._tag === 'Completed'
              ? {
                  outcome: event.outcome,
                  text: event.text,
                  steps: event.steps,
                  usage: event.usage,
                }
              : result,
        ),
        Effect.flatMap((result) =>
          result === undefined
            ? Effect.die(new Error('Agent stream ended without completing'))
            : Effect.succeed(result),
        ),
      ),
    stream,
    resume: (input) => continueFrom(input),
    branchFrom: (at, input) => continueFrom(input, { branchFrom: at }),
    forkFrom: (at, targetConversationId, input) =>
      continueFrom(input, {
        branchFrom: at,
        forkConversationId: targetConversationId,
      }),
    records: (after) =>
      AgentLog.snapshot(
        LogVocabulary.ConversationId.make(conversationId),
        after,
      ),
    follow: (after) =>
      AgentLog.follow(LogVocabulary.ConversationId.make(conversationId), after),
    send: (signal) => appendSignal(conversationId, signal),
  };
};

/** Bind an agent to one durable conversation identity. */
export const make = <A extends ConcreteAgent>(
  agent: A,
  conversationId: string,
): Instance<
  Agent.Requires<A>,
  Agent.Error<A> | AgentLog.CompatibilityError,
  Agent.Tools<A>
> => bind(agent, conversationId);

/** Bind an agent with one recording policy applied across every durable run. */
export const recording = <A extends ConcreteAgent, const P extends object>(
  agent: A,
  conversationId: string,
  policy: P & RecordingPolicy.Policy<RecordingPolicy.Services<P>>,
): Instance<
  Agent.Requires<A> | RecordingPolicy.Services<P>,
  Agent.Error<A> | AgentLog.CompatibilityError,
  Agent.Tools<A>
> =>
  bind(
    agent,
    conversationId,
    policy as RecordingPolicy.Policy<never>,
  ) as Instance<
    Agent.Requires<A> | RecordingPolicy.Services<P>,
    Agent.Error<A> | AgentLog.CompatibilityError,
    Agent.Tools<A>
  >;

export * as Conversation from './conversation.js';
