import { Effect, Option, Stream } from 'effect';
import { Prompt } from 'effect/unstable/ai';

import type { ToolDispatch } from '../dispatch.js';
import type * as AgentLog from '../log.js';
import type { RunPolicy } from '../run-policy.js';
import { RunPolicyRuntime } from '../run-policy-runtime.js';

// Steering intake and the responsive-cancel watcher: the two pieces of
// signal handling shared between an ordinary turn boundary (`turn.ts`) and
// the pre-turn-1 recovery boundary (`recovery-run.ts`). Split out because
// both callers drain the same `signals/<conversationId>` stream against the
// same run-policy limits and race the same cancel watcher against their own
// work — duplicating either invites the two copies to drift.

/**
 * What a batch of steering instructions becomes for the next turn.
 *
 * Plain user messages. A steer is somebody talking to the agent
 * out-of-band, and the model already knows what to do with a user turn;
 * inventing a bespoke role would mean every provider adapter had to learn
 * one. Several steers delivered together are joined rather than sent as
 * several messages, so the turn count does not depend on how a sender
 * happened to batch them.
 */
export const steeringInput = (
  steers: ReadonlyArray<{ readonly text: string }>,
): Prompt.RawInput =>
  steers.length === 0
    ? Prompt.empty
    : Prompt.make([
        {
          role: 'user',
          content: steers.map((steer) => steer.text).join('\n\n'),
        },
      ]);

/**
 * A hint-only watcher that preempts an in-flight provider stream for a
 * valid, reachable cancel signal — see CONTEXT.md's "Signal". Raced against
 * the work it guards with `Stream.interruptWhen`; a session-less run has
 * nothing to watch, so callers only build this when a session exists.
 *
 * `failureMessage` is the only thing that varies between the ordinary
 * turn-boundary watcher and the pre-turn-1 recovery watcher — everything
 * else, including the `vesper.event` tag, is identical between the two call
 * sites in the code this was split from.
 */
export const watchForCancel = (params: {
  readonly session: AgentLog.Session;
  readonly runtime: RunPolicyRuntime.Runtime | undefined;
  readonly runPolicy: RunPolicy.Limits;
  readonly arbitration: ToolDispatch.TurnArbitration;
  readonly failureMessage: string;
}) =>
  params.session
    .signalPages(
      params.runtime?.limits.maxSignalsPerBoundary ??
        params.runPolicy.maxSignalsPerBoundary,
    )
    .pipe(
      Stream.flatMap((page) =>
        Stream.fromIterable(
          page.signals.map((signal, index) => ({ signal, index })),
        ),
      ),
      Stream.filter(({ signal }) => signal.kind === 'cancel'),
      Stream.filter(({ signal, index }) =>
        RunPolicyRuntime.acceptsCancel(
          params.runtime?.limits ?? params.runPolicy,
          signal.text,
          index,
        ),
      ),
      Stream.tap(() => params.arbitration.cancel),
      Stream.runHead,
      Effect.flatMap((cancel) =>
        Option.isSome(cancel) ? Effect.void : Effect.never,
      ),
      Effect.catch((error) =>
        Effect.logError(params.failureMessage, error).pipe(
          Effect.annotateLogs({
            'vesper.component': 'agent',
            'vesper.event': 'responsive_cancel_watcher_failure',
          }),
          Effect.andThen(Effect.never),
        ),
      ),
    );
