import type { LogStore } from '@sunfall/vesper-log/log-store';
import type { LogOffset } from '@sunfall/vesper-log/offset';
import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { type Crypto, type Effect, type Stream } from 'effect';
import type { Prompt, Tool } from 'effect/unstable/ai';

import type { Agent } from '../agent.js';
import type {
  CompatibilityError,
  SuspendedConversationError,
} from '../conversation-error.js';
import type { AgentEvents } from '../event.js';
import type * as AgentLog from '../log.js';
import type { RecordingPolicy } from '../recording-policy.js';
import type { RunPolicyRuntime } from '../run-policy-runtime.js';

type ProtocolError<RunError> =
  | RunError
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | AgentLog.DurabilityError;

export interface AgentProtocol<
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any> = Record<string, Tool.Any>,
> {
  /** Continue one durable conversation, starting it when no history exists. */
  readonly stream: <PolicyRequires = never>(
    conversationId: LogVocabulary.ConversationId,
    input: Prompt.RawInput,
    options?: {
      readonly branchFrom?: LogOffset.Offset;
      readonly forkConversationId?: LogVocabulary.ConversationId;
      readonly pendingWait?: 'restart';
      readonly policy?: RecordingPolicy.Policy<PolicyRequires>;
    },
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<Tools>,
    ProtocolError<RunError>,
    Requires | PolicyRequires | LogStore.Service | Crypto.Crypto
  >;
  readonly run: (
    runtime: RunPolicyRuntime.Runtime,
    session: AgentLog.Session | undefined,
    input: Prompt.RawInput,
  ) => Effect.Effect<Agent.Result, ProtocolError<RunError>, Requires>;
}

/** Internal symbol carrying durable invocation without widening Agent's Interface. */
const ProtocolTypeId: unique symbol = Symbol.for(
  '@sunfall/vesper-agent/internal/AgentProtocol',
);

export interface ProtocolCarrier<
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any>,
> {
  readonly [ProtocolTypeId]: AgentProtocol<Requires, RunError, Tools>;
}

/** Attach the internal protocol without adding enumerable public fields. */
export const register = <
  A extends object,
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any>,
>(
  agent: A,
  protocol: AgentProtocol<Requires, RunError, Tools>,
): A => {
  Object.defineProperty(agent, ProtocolTypeId, { value: protocol });
  return agent;
};

export const protocolOf = <
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any> = Record<string, Tool.Any>,
>(
  agent: object,
): AgentProtocol<Requires, RunError, Tools> | undefined =>
  hasProtocol<Requires, RunError, Tools>(agent)
    ? agent[ProtocolTypeId]
    : undefined;

export const hasProtocol = <
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any> = Record<string, Tool.Any>,
>(
  agent: object,
): agent is ProtocolCarrier<Requires, RunError, Tools> => {
  if (!(ProtocolTypeId in agent)) {
    return false;
  }
  const protocol = agent[ProtocolTypeId];
  return (
    typeof protocol === 'object' &&
    protocol !== null &&
    'stream' in protocol &&
    typeof protocol.stream === 'function' &&
    'run' in protocol &&
    typeof protocol.run === 'function'
  );
};
