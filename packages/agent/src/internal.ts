import { Context, type Effect } from 'effect';
import type { AiError, Prompt } from 'effect/unstable/ai';

import type { AgentLog } from './log.js';
import type { RunPolicyRuntime } from './run-policy-runtime.js';
import type { Agent } from './agent.js';

export const Session = Context.Reference<AgentLog.Session | undefined>(
  '@sunfall/vesper-agent/Session',
  { defaultValue: () => undefined },
);

export const StateCleanup = Context.Reference<
  Set<(session: AgentLog.Session) => Effect.Effect<void>> | undefined
>('@sunfall/vesper-agent/StateCleanup', { defaultValue: () => undefined });

interface AgentProtocol<Requires> {
  readonly run: (
    runtime: RunPolicyRuntime.Runtime,
    session: AgentLog.Session | undefined,
    input: Prompt.RawInput,
  ) => Effect.Effect<
    Agent.Result,
    AiError.AiError | AgentLog.CompatibilityError,
    Requires
  >;
}

const protocols = new WeakMap<object, AgentProtocol<unknown>>();

export const register = <Requires>(
  agent: object,
  protocol: AgentProtocol<Requires>,
): void => {
  protocols.set(agent, protocol as unknown as AgentProtocol<unknown>);
};

export const protocolOf = <Requires>(
  agent: object,
): AgentProtocol<Requires> | undefined =>
  protocols.get(agent) as AgentProtocol<Requires> | undefined;

export const hasProtocol = (agent: object): boolean => protocols.has(agent);
