import { Effect } from 'effect';
import { AiError, Toolkit } from 'effect/unstable/ai';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

import type { Agent } from './agent.js';
import { protocolOf } from './internal.js';
import type { AgentLog } from './log.js';
import { RunPolicy } from './run-policy.js';
import { RunPolicyRuntime } from './run-policy-runtime.js';
import {
  Depth,
  MAX_DEPTH,
  type Services,
  tool,
  toolName,
  type Tools,
  type ToolTuple,
} from './subagent.js';

interface CallContext {
  readonly toolCallId?: LogVocabulary.ToolCallId | undefined;
}

export const handler =
  <Name extends string, R>(
    child: Agent.Named<Name, R>,
    session?: AgentLog.Session,
    runtime?: RunPolicyRuntime.Runtime,
  ) =>
  (input: { readonly prompt: string }, call?: CallContext) =>
    Effect.gen(function* () {
      const depth = yield* Depth;
      const maxDepth = runtime?.limits.maxDelegationDepth ?? MAX_DEPTH;
      if (depth >= maxDepth) {
        return yield* Effect.fail({
          refused:
            `Delegation depth ${maxDepth} reached; complete this task ` +
            'directly instead of delegating further.',
        });
      }

      const protocol = protocolOf<R>(child);
      if (protocol === undefined) {
        return yield* Effect.die(
          new Error(`Subagent "${child.name}" was not created by Agent.make`),
        );
      }
      const active =
        runtime ?? (yield* RunPolicyRuntime.create(RunPolicy.defaultLimits));
      const run = (childSession: AgentLog.Session | undefined) =>
        protocol.run(active, childSession, input.prompt).pipe(
          Effect.catchTag('@sunfall/vesper-agent/CompatibilityError', (error) =>
            Effect.fail(
              new AiError.AiError({
                module: 'Subagent',
                method: 'delegate',
                reason: new AiError.InvalidRequestError({
                  description: error.message,
                }),
              }),
            ),
          ),
        );
      const delegated =
        session === undefined
          ? run(undefined)
          : Effect.flatMap(
              session.child({
                toolCallId:
                  call?.toolCallId ??
                  LogVocabulary.ToolCallId.make(crypto.randomUUID()),
                agent: child.name,
                revision: child.revision,
                depth: depth + 1,
              }),
              run,
            );
      const result = yield* active.delegation(delegated).pipe(
        Effect.catchTag('@sunfall/vesper-agent/CompatibilityError', (error) =>
          Effect.fail(
            new AiError.AiError({
              module: 'Subagent',
              method: 'delegate',
              reason: new AiError.InvalidRequestError({
                description: error.message,
              }),
            }),
          ),
        ),
        Effect.provideService(Depth, depth + 1),
      );
      return { result: result.text, steps: result.steps };
    }).pipe(Effect.withSpan(`Agent.delegate.${child.name}`));

type DelegationHandler = (
  input: { readonly prompt: string },
  call: CallContext,
) => Effect.Effect<
  { readonly result: string; readonly steps: number },
  AiError.AiError | { readonly refused: string },
  unknown
>;

export const delegateTo = <const Children extends ReadonlyArray<Agent.Named>>(
  ...children: Children
) => {
  const tools = children.map((child) => tool(child)) as ToolTuple<Children>;
  const kit = Toolkit.make(...tools) as unknown as Toolkit.Toolkit<
    Tools<Children>
  >;
  const layer = (
    session: AgentLog.Session | undefined,
    runtime?: RunPolicyRuntime.Runtime,
  ) =>
    kit.toLayer(
      Effect.gen(function* () {
        const context = yield* Effect.context<Services<Children>>();
        const handlers: Record<string, DelegationHandler> = Object.fromEntries(
          children.map((child) => [
            toolName(child.name),
            (input: { readonly prompt: string }, call: CallContext) =>
              handler(
                child,
                session,
                runtime,
              )(
                input,
                call.toolCallId === undefined
                  ? call
                  : {
                      toolCallId: LogVocabulary.ToolCallId.make(
                        call.toolCallId,
                      ),
                    },
              ).pipe(Effect.provide(context)),
          ]),
        );
        return handlers as never;
      }),
    );
  return { toolkit: kit, layer };
};

export * as SubagentRuntime from './subagent-runtime.js';
