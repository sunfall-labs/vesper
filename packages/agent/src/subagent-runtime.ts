import { Effect } from 'effect';
import { AiError, Toolkit } from 'effect/unstable/ai';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

import type { Agent } from './agent.js';
import * as AgentIds from './internal/ids.js';
import { protocolOf } from './internal/protocol.js';
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
      yield* Effect.annotateCurrentSpan({
        'vesper.agent.child.name': child.name,
        'vesper.agent.child.revision': child.revision,
        'vesper.agent.delegation.depth': depth,
      });

      const protocol = protocolOf<
        R,
        Agent.Error<typeof child>,
        Agent.Tools<typeof child>
      >(child);
      if (protocol === undefined) {
        return yield* Effect.die(
          new Error(`Subagent "${child.name}" was not created by Agent.make`),
        );
      }
      const active =
        runtime ?? (yield* RunPolicyRuntime.create(RunPolicy.defaultLimits));
      const run = (childSession: AgentLog.Session | undefined) =>
        protocol.run(active, childSession, input.prompt);
      const delegated =
        session === undefined
          ? run(undefined)
          : Effect.flatMap(
              session.child({
                toolCallId: call?.toolCallId ?? (yield* AgentIds.toolCallId),
                agent: child.name,
                revision: child.revision,
                depth: depth + 1,
              }),
              run,
            );
      const result = yield* active.delegation(delegated).pipe(
        Effect.catchTag('LogStoreError', (error) =>
          Effect.fail(
            new AiError.AiError({
              module: 'Subagent',
              method: 'delegate',
              reason: new AiError.UnknownError({
                description: error.detail,
                metadata: {
                  path: error.path,
                  operation: error.operation,
                  reason: error.reason,
                },
              }),
            }),
          ),
        ),
        Effect.catchTag('CompatibilityError', (error) =>
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
        Effect.catchTag('SuspendedConversationError', (error) =>
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
    }).pipe(Effect.withSpan('Agent.delegate'));

type DelegationHandler = (
  input: { readonly prompt: string },
  call: CallContext,
) => Effect.Effect<
  { readonly result: string; readonly steps: number },
  AiError.AiError | RunPolicy.RunPolicyExhausted | { readonly refused: string },
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
