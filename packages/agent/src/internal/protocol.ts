import { Context, type Effect, type Stream } from 'effect';
import type { AiError, Prompt, Tool } from 'effect/unstable/ai';

import type { Agent } from '../agent.js';
import type { AgentEvents } from '../event.js';
import type { AgentLog } from '../log.js';
import type { RecordingPolicy } from '../recording-policy.js';
import type { RunPolicyRuntime } from '../run-policy-runtime.js';

export const Session = Context.Reference<AgentLog.Session | undefined>(
  '@sunfall/vesper-agent/Session',
  { defaultValue: () => undefined },
);

export const StateCleanup = Context.Reference<
  Set<(session: AgentLog.Session) => Effect.Effect<void>> | undefined
>('@sunfall/vesper-agent/StateCleanup', { defaultValue: () => undefined });

export interface AgentProtocol<
  Requires,
  Tools extends Record<string, Tool.Any>,
> {
  readonly record: (
    conversationId: string,
    input: Prompt.RawInput,
    policy?: RecordingPolicy.Policy<never>,
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<Tools>,
    AiError.AiError | AgentLog.CompatibilityError,
    Requires | import('@sunfall/vesper-log/log-store').LogStore.Service
  >;
  readonly run: (
    runtime: RunPolicyRuntime.Runtime,
    session: AgentLog.Session | undefined,
    input: Prompt.RawInput,
  ) => Effect.Effect<
    Agent.Result,
    AiError.AiError | AgentLog.CompatibilityError,
    Requires
  >;
  readonly continue: (
    conversationId: string,
    input: Prompt.RawInput,
    options?: {
      readonly branchFrom?: import('@sunfall/vesper-log/offset').LogOffset.Offset;
      readonly forkConversationId?: string;
      readonly policy?: RecordingPolicy.Policy<never>;
    },
  ) => Effect.Effect<
    Agent.Result,
    AiError.AiError | AgentLog.CompatibilityError,
    Requires
  >;
}

const protocols = new WeakMap<object, AgentProtocol<unknown, never>>();

export const register = <Requires, Tools extends Record<string, Tool.Any>>(
  agent: object,
  protocol: AgentProtocol<Requires, Tools>,
): void => {
  protocols.set(agent, protocol as unknown as AgentProtocol<unknown, never>);
};

export const protocolOf = <
  Requires,
  Tools extends Record<string, Tool.Any> = Record<string, Tool.Any>,
>(
  agent: object,
): AgentProtocol<Requires, Tools> | undefined =>
  protocols.get(agent) as unknown as AgentProtocol<Requires, Tools> | undefined;

export const hasProtocol = (agent: object): boolean => protocols.has(agent);
