import { Effect, type Layer } from 'effect';
import { AiError, Toolkit, type Tool } from 'effect/unstable/ai';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

import type { Agent } from './agent.js';
import * as AgentIds from './internal/ids.js';
import { protocolOf } from './internal/protocol.js';
import type * as AgentLog from './log.js';
import { RunPolicy } from './run-policy.js';
import { RunPolicyRuntime } from './run-policy-runtime.js';
import {
  Depth,
  MAX_DEPTH,
  type Services,
  toolName,
  runtimeTool,
  type Tools,
} from './subagent.js';

interface CallContext {
  readonly toolCallId?: string | undefined;
}

export const handler =
  <Name extends string, R>(
    child: Agent.Child<Name, R>,
    session?: AgentLog.Session,
    runtime?: RunPolicyRuntime.Runtime,
  ) =>
  (input: { readonly prompt: string }, call?: CallContext) =>
    Effect.gen(function* () {
      const protocol = protocolOf<
        R,
        Agent.RunFailure,
        Record<string, Tool.Any>
      >(child);
      if (protocol === undefined) {
        return yield* Effect.die(
          new Error('Subagent was not created by Agent.make'),
        );
      }
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

      const active =
        runtime ?? (yield* RunPolicyRuntime.create(RunPolicy.defaultLimits));
      const run = (childSession: AgentLog.Session | undefined) => {
        const childRun = protocol.run(active, childSession, input.prompt);
        return childSession === undefined
          ? childRun
          : Effect.annotateCurrentSpan({
              'vesper.child.conversation.id': childSession.conversationId,
            }).pipe(Effect.andThen(childRun));
      };
      const delegated =
        session === undefined
          ? run(undefined)
          : Effect.flatMap(
              session.child({
                toolCallId:
                  call?.toolCallId === undefined
                    ? yield* AgentIds.toolCallId
                    : LogVocabulary.ToolCallId.make(call.toolCallId),
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
        Effect.catchTag('DurabilityError', (error) =>
          Effect.fail(
            new AiError.AiError({
              module: 'Subagent',
              method: 'delegate',
              reason: new AiError.UnknownError({
                description: error.detail,
                metadata: {
                  tag: error._tag,
                  source: error.source,
                  operation: error.operation,
                  reason: error.reason,
                },
              }),
            }),
          ),
        ),
        Effect.provideService(Depth, depth + 1),
      );
      return { result: result.text, steps: result.steps };
    }).pipe(
      Effect.withSpan('Agent.delegate', {
        attributes: {
          'vesper.agent.child.name': child.name,
          'vesper.agent.child.revision': child.revision,
          ...(session === undefined
            ? {}
            : { 'vesper.conversation.id': session.conversationId }),
        },
      }),
    );

type DelegationHandler = (
  input: { readonly prompt: string },
  call: CallContext,
) => Effect.Effect<
  { readonly result: string; readonly steps: number },
  AiError.AiError | RunPolicy.RunPolicyExhausted | { readonly refused: string },
  never
>;

type DelegateResult<Children extends ReadonlyArray<Agent.Child>> = {
  readonly toolkit: Toolkit.Any;
  readonly layer: (
    session: AgentLog.Session | undefined,
    runtime?: RunPolicyRuntime.Runtime,
  ) => Layer.Layer<
    Tool.HandlersFor<Tools<Children>>,
    never,
    Services<Children>
  >;
};

export function delegateTo<const Children extends ReadonlyArray<Agent.Child>>(
  ...children: Children
): DelegateResult<Children>;
export function delegateTo(
  ...children: ReadonlyArray<Agent.Child>
): DelegateResult<ReadonlyArray<Agent.Child>> {
  const tools = children.map((child) => runtimeTool(child));
  const kit = Toolkit.make(...tools);
  const layer = (
    session: AgentLog.Session | undefined,
    runtime?: RunPolicyRuntime.Runtime,
  ) =>
    kit.toLayer(
      Effect.gen(function* () {
        const context = yield* Effect.context<unknown>();
        const handlers: Record<string, DelegationHandler> = Object.fromEntries(
          children.map((child) => [
            toolName(child.name),
            (input: { readonly prompt: string }, call: CallContext) =>
              handler(
                child,
                session,
                runtime,
              )(input, call).pipe(Effect.provideContext(context)),
          ]),
        );
        return handlers;
      }),
    );
  return { toolkit: kit, layer };
}

export * as SubagentRuntime from './subagent-runtime.js';
