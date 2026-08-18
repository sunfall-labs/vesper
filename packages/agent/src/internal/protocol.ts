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
import type { AgentLog } from '../log.js';
import type { RecordingPolicy } from '../recording-policy.js';
import type { RunPolicyRuntime } from '../run-policy-runtime.js';

type ProtocolError<RunError> =
  | RunError
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError;

export interface AgentProtocol<
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any>,
> {
  /** Continue one durable conversation, starting it when no history exists. */
  readonly stream: (
    conversationId: LogVocabulary.ConversationId,
    input: Prompt.RawInput,
    options?: {
      readonly branchFrom?: LogOffset.Offset;
      readonly forkConversationId?: LogVocabulary.ConversationId;
      readonly pendingWait?: 'restart';
      readonly policy?: RecordingPolicy.Policy<never>;
    },
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<Tools>,
    ProtocolError<RunError>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  readonly run: (
    runtime: RunPolicyRuntime.Runtime,
    session: AgentLog.Session | undefined,
    input: Prompt.RawInput,
  ) => Effect.Effect<Agent.Result, ProtocolError<RunError>, Requires>;
}

type ErasedProtocol = AgentProtocol<
  unknown,
  Agent.RunFailure | CompatibilityError,
  never
>;

const protocols = new WeakMap<object, ErasedProtocol>();

export const register = <
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any>,
>(
  agent: object,
  protocol: AgentProtocol<Requires, RunError, Tools>,
): void => {
  protocols.set(agent, protocol as unknown as ErasedProtocol);
};

export const protocolOf = <
  Requires,
  RunError extends Agent.RunFailure | CompatibilityError,
  Tools extends Record<string, Tool.Any> = Record<string, Tool.Any>,
>(
  agent: object,
): AgentProtocol<Requires, RunError, Tools> | undefined =>
  protocols.get(agent) as unknown as
    | AgentProtocol<Requires, RunError, Tools>
    | undefined;
