import { Effect, Ref } from 'effect';
import { AiError, Prompt, type Chat, type Tool } from 'effect/unstable/ai';

import { CodeMode } from '../code-mode.js';
import { DynamicToolkit } from '../dynamic-toolkit.js';
import type * as AgentLog from '../log.js';
import type { RunPolicy } from '../run-policy.js';
import { RunPolicyRuntime } from '../run-policy-runtime.js';
import type { ReEnter, Wiring } from './loop.js';

// The three `streamIn` preconditions that resolve run-scoped state before
// any turn can start: opening code mode's execution state, opening a
// dynamic toolkit's sources, and creating the root run-policy runtime. All
// three re-enter `entryFor` with an updated `Wiring` once their concern is
// resolved — `ReEnter` (see `loop.ts`) types that re-entry once instead of
// asserting the resolved `Stream` shape at each call site.

/**
 * `dynamicContextFor` and `replaceSystemInstructions` live here rather than
 * in `loop.ts` because both exist only to serve the dynamic-toolkit
 * bootstrap below (and, for `dynamicContextFor`, the one other place a run
 * needs the same resource-context string — `loop.ts`'s own
 * `runInstructions`, which imports it back from here).
 */
export const dynamicContextFor = (
  instructions: string,
  toolkit: unknown,
): string => {
  const context = DynamicToolkit.resourceContext(toolkit);
  return context === '' ? instructions : `${instructions}\n\n${context}`;
};

const replaceSystemInstructions = (
  chat: Chat.Service,
  instructions: string,
): Effect.Effect<void> =>
  Ref.update(chat.history, (history) => {
    const rest =
      history.content[0]?.role === 'system'
        ? history.content.slice(1)
        : history.content;
    return Prompt.fromMessages([
      Prompt.makeMessage('system', { content: instructions }),
      ...rest,
    ]);
  });

/**
 * Open code mode's execution state and re-enter with it wired in. Entered
 * when `CodeMode.isEnabled(definition.codeMode)` and no state has been
 * opened for this run yet — `loop.ts`'s `streamIn` names both halves of
 * that precondition itself.
 */
export const bootstrapCodeMode = <
  ModelTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorR,
>(params: {
  readonly session: AgentLog.Session | undefined;
  readonly wiring: Wiring<InterceptorR, DynamicTools>;
  readonly entryFor: ReEnter<
    ModelTools,
    DynamicTools,
    BaseRequires,
    InterceptorR
  >;
  readonly chat: Chat.Service;
  readonly input: Prompt.RawInput;
}) =>
  Effect.gen(function* () {
    const codeState = yield* CodeMode.openState(params.session).pipe(
      Effect.mapError(
        (error) =>
          new AiError.AiError({
            module: 'CodeMode',
            method: 'openState',
            reason: new AiError.InvalidRequestError({
              description: error.message,
            }),
          }),
      ),
    );
    return params
      .entryFor({ ...params.wiring, codeState })
      .streamIn(params.chat, params.input);
  });

/**
 * Open the dynamic toolkit's sources, splice their resource context into
 * the system instructions, and re-enter with the opened toolkit wired in.
 * Entered once per run, the first time `streamIn` sees dynamic sources
 * configured and no dynamic toolkit open yet.
 */
export const bootstrapDynamicToolkit = <
  ModelTools extends Record<string, Tool.Any>,
  DynamicSources extends ReadonlyArray<DynamicToolkit.Any>,
  BaseRequires,
  InterceptorR,
>(params: {
  readonly dynamicTools: DynamicSources;
  readonly instructions: string;
  readonly wiring: Wiring<InterceptorR, DynamicToolkit.Tools<DynamicSources>>;
  readonly entryFor: ReEnter<
    ModelTools,
    DynamicToolkit.Tools<DynamicSources>,
    BaseRequires,
    InterceptorR
  >;
  readonly chat: Chat.Service;
  readonly input: Prompt.RawInput;
}) =>
  Effect.gen(function* () {
    const dynamicToolkit = yield* DynamicToolkit.open(params.dynamicTools);
    const nextWiring = { ...params.wiring, dynamicToolkit };
    yield* replaceSystemInstructions(
      params.chat,
      dynamicContextFor(params.instructions, dynamicToolkit),
    );
    return params.entryFor(nextWiring).streamIn(params.chat, params.input);
  });

/**
 * Create the root run-policy runtime and re-enter with it wired in. Entered
 * once per run, before the first turn and before recovery — the ledger it
 * creates is the one every descendant loop shares, per CONTEXT.md's "Hard
 * run budget".
 */
export const bootstrapRuntime = <
  ModelTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorR,
>(params: {
  readonly runPolicy: RunPolicy.Limits;
  readonly wiring: Wiring<InterceptorR, DynamicTools>;
  readonly entryFor: ReEnter<
    ModelTools,
    DynamicTools,
    BaseRequires,
    InterceptorR
  >;
  readonly chat: Chat.Service;
  readonly input: Prompt.RawInput;
}) =>
  Effect.gen(function* () {
    const root = yield* RunPolicyRuntime.create(params.runPolicy);
    return params
      .entryFor({ ...params.wiring, runtime: root })
      .streamIn(params.chat, params.input);
  });
