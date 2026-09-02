import { Effect, Option, Ref, Stream } from 'effect';
import { Prompt, type Chat, type Tool, type Toolkit } from 'effect/unstable/ai';

import { ToolDispatch } from '../dispatch.js';
import type { AgentEvents } from '../event.js';
import { AgentHistory } from '../history.js';
import type { Interception } from '../interception.js';
import type * as AgentLog from '../log.js';
import { RunPolicyRuntime } from '../run-policy-runtime.js';
import { AgentEventRuntime } from './event.js';
import type { ReEnter, Wiring } from './loop.js';
import { steeringInput, watchForCancel } from './signals-run.js';

// The recovery branch entered when a session has pending tool calls from a
// crashed or resumed run — CONTEXT.md's "Indeterminate tool call" — plus
// the settle-from-history path it funnels into once recovery and any still-
// pending approvals are resolved. Both stay together because the settle
// path only ever runs as recovery's own continuation: nothing else reaches
// it, and it is what lets the ordinary turn path (`turn.ts`) assume
// `chat.history` already reflects every recorded record when it starts.

/**
 * Run once, entered only when `session.hasPendingToolCalls` — see
 * `loop.ts`'s `streamIn`, which checks that precondition itself and calls
 * this only once it holds. Drains signals at this pre-turn-1 boundary,
 * resolves every indeterminate tool call it can, races that resolution
 * against a cancel, and — once nothing is left pending — replays the
 * session's recorded history into `chat` and re-enters `entryFor` to start
 * the ordinary turn path from a caught-up conversation.
 */
export const runRecovery = <
  ModelTools extends Record<string, Tool.Any>,
  RunTools extends Record<string, Tool.Any>,
  DynamicTools extends Record<string, Tool.Any>,
  BaseRequires,
  InterceptorR,
  RunToolkitR = unknown,
>(params: {
  readonly agentName: string;
  readonly session: AgentLog.Session;
  readonly interceptor: Interception.Interceptor<InterceptorR> | undefined;
  readonly runtime: RunPolicyRuntime.Runtime;
  readonly delegationToolNames: ReadonlySet<string>;
  readonly runToolkit: Effect.Effect<
    Toolkit.WithHandler<RunTools>,
    never,
    RunToolkitR
  >;
  readonly runInstructions: string;
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
    const {
      agentName,
      session,
      interceptor,
      runtime,
      delegationToolNames,
      runToolkit,
      runInstructions,
      wiring,
      entryFor,
      chat,
      input,
    } = params;

    const drained = yield* session.drainSignalsBounded(
      runtime.limits.maxSignalsPerBoundary,
    );
    const decisions = yield* Effect.forEach(drained.signals, (signal, index) =>
      Effect.map(
        runtime.signal(signal.kind, signal.text, index),
        (decision) => ({ signal, ...decision }),
      ),
    );
    const delivered = decisions.flatMap((decision) =>
      decision.accepted ? [decision.signal] : [],
    );
    const steers = delivered.filter((signal) => signal.kind === 'steer');
    const effective =
      steers.length === 0
        ? input
        : Prompt.concat(Prompt.make(input), Prompt.make(steeringInput(steers)));
    const announced = Stream.fromIterable(
      decisions
        .map((decision) =>
          decision.accepted
            ? AgentEventRuntime.signalled(0, decision.signal)
            : AgentEventRuntime.signalRejected(
                0,
                decision.signal,
                decision.exhaustion,
              ),
        )
        .concat(
          drained.backlog
            ? [
                AgentEventRuntime.signalBacklog(
                  0,
                  runtime.limits.maxSignalsPerBoundary,
                ),
              ]
            : [],
        ),
    );
    if (delivered.some((signal) => signal.kind === 'cancel')) {
      if (wiring.startRun !== undefined) {
        yield* wiring.startRun(effective);
      }
      return Stream.concat(
        announced,
        Stream.make(
          AgentEventRuntime.completed(
            '',
            0,
            wiring.initialUsage ?? { input: 0, output: 0 },
            'cancelled',
          ),
        ),
      );
    }

    const arbitration = yield* ToolDispatch.makeTurnArbitration;
    const cancelDuringRecovery = watchForCancel({
      session,
      runtime,
      runPolicy: runtime.limits,
      arbitration,
      failureMessage:
        'Agent recovery cancel watcher failed; cancellation remains available at the next boundary',
    });

    return Stream.concat(
      announced,
      Stream.unwrap(
        Effect.gen(function* () {
          const remaining = yield* runtime.remainingMillis;
          const recovery = ToolDispatch.resolveIndeterminate(runToolkit, {
            agent: agentName,
            session,
            interceptor,
            runtime,
            unmeteredToolNames: delegationToolNames,
            arbitration,
          }).pipe(
            Effect.timeoutOrElse({
              duration: remaining,
              orElse: () =>
                Effect.fail(
                  RunPolicyRuntime.error({
                    limit: 'deadline',
                    used: runtime.limits.wallClockMillis,
                    maximum: runtime.limits.wallClockMillis,
                  }),
                ),
            }),
          );
          const recoveryOutcome = yield* Effect.raceFirst(
            Effect.map(recovery, (results) => ({
              _tag: 'Recovered' as const,
              results,
            })),
            Effect.as(cancelDuringRecovery, {
              _tag: 'Cancelled' as const,
            }),
          );
          if (recoveryOutcome._tag === 'Cancelled') {
            const after = yield* session.drainSignalsBounded(
              runtime.limits.maxSignalsPerBoundary,
            );
            const afterDecisions = yield* Effect.forEach(
              after.signals,
              (signal, index) =>
                Effect.map(
                  runtime.signal(signal.kind, signal.text, index),
                  (decision) => ({ signal, ...decision }),
                ),
            );
            const afterSteers = afterDecisions
              .filter(
                (decision) =>
                  decision.accepted && decision.signal.kind === 'steer',
              )
              .map((decision) => decision.signal);
            const cancelledInput =
              afterSteers.length === 0
                ? effective
                : Prompt.concat(
                    Prompt.make(effective),
                    Prompt.make(steeringInput(afterSteers)),
                  );
            if (wiring.startRun !== undefined) {
              yield* wiring.startRun(cancelledInput);
            }
            return Stream.concat(
              Stream.fromIterable(
                afterDecisions.map((decision) =>
                  decision.accepted
                    ? AgentEventRuntime.signalled(0, decision.signal)
                    : AgentEventRuntime.signalRejected(
                        0,
                        decision.signal,
                        decision.exhaustion,
                      ),
                ),
              ),
              Stream.make(
                AgentEventRuntime.completed(
                  '',
                  0,
                  wiring.initialUsage ?? { input: 0, output: 0 },
                  'cancelled',
                ),
              ),
            );
          }
          const recovered = recoveryOutcome.results;

          // `resolveIndeterminate` settles every durable approval this
          // session already has a decision for, but it does not — and
          // must not — invent one for a call still waiting on
          // `Conversation.resolveApproval`. Re-check by identity
          // rather than trusting `suspendedToolCalls`, which is the
          // snapshot from when this session opened: the recovery
          // index it is read through here is the same one
          // `resolveIndeterminate` just updated.
          const approvalWaits = session.suspendedToolCalls.filter(
            (call) => call.wait === ToolDispatch.INTERACTION_WAIT,
          );
          // The toolkit is resolved once, and only when an approval
          // wait exists at all: resolution may include dynamic
          // sources whose work is real (an MCP discovery
          // round-trip), and almost every run has nothing suspended.
          const stillPendingInteractions: AgentEvents.PendingInteraction[] =
            approvalWaits.length === 0
              ? []
              : yield* Effect.gen(function* () {
                  const approvalToolkit = Effect.succeed(yield* runToolkit);
                  const batches = yield* Effect.forEach(
                    approvalWaits,
                    (call) => {
                      const current = session.recovery(
                        call.name,
                        call.toolCallId,
                      );
                      if (
                        Option.isNone(current) ||
                        current.value._tag !== 'Suspended'
                      ) {
                        return Effect.succeed<
                          ReadonlyArray<AgentEvents.PendingInteraction>
                        >([]);
                      }
                      const pendingRecovery = current.value;
                      // Re-decoded against the tool's current
                      // parameter schema rather than surfaced from
                      // the durable encoded form: a caller
                      // re-reading this pending approval sees the
                      // same typed value the first suspension did,
                      // not the toolkit's wire encoding of it.
                      return Effect.map(
                        ToolDispatch.decodeSuspendedRequest(
                          approvalToolkit,
                          call.name,
                          call.request,
                        ),
                        (
                          decodedInput,
                        ): ReadonlyArray<AgentEvents.PendingInteraction> => [
                          {
                            toolCallId: call.toolCallId,
                            toolName: call.name,
                            kind:
                              pendingRecovery.interaction?.name ?? 'approval',
                            request: decodedInput,
                          },
                        ],
                      );
                    },
                  );
                  return batches.flat();
                });
          if (stillPendingInteractions.length > 0) {
            return Stream.concat(
              Stream.fromIterable(recovered),
              Stream.make(
                AgentEventRuntime.suspended(
                  0,
                  // No model call happened on this path — the run was
                  // refused before turn 1 — so there is no partial text
                  // to preserve.
                  '',
                  wiring.initialUsage ?? { input: 0, output: 0 },
                  stillPendingInteractions,
                ),
              ),
            );
          }

          yield* Ref.set(
            chat.history,
            Prompt.concat(
              Prompt.make([{ role: 'system', content: runInstructions }]),
              AgentHistory.messagesFrom(yield* session.recorded),
            ),
          );
          return Stream.concat(
            Stream.fromIterable(recovered),
            entryFor({
              session: wiring.session,
              interceptor: wiring.interceptor,
              runtime,
              ...(wiring.dynamicToolkit === undefined
                ? {}
                : { dynamicToolkit: wiring.dynamicToolkit }),
              ...(wiring.startRun === undefined
                ? {}
                : { startRun: wiring.startRun }),
              ...(wiring.initialUsage === undefined
                ? {}
                : { initialUsage: wiring.initialUsage }),
              ...(wiring.lastTurn === undefined
                ? {}
                : { lastTurn: wiring.lastTurn }),
              ...(wiring.codeState === undefined
                ? {}
                : { codeState: wiring.codeState }),
            }).streamIn(chat, effective),
          );
        }),
      ),
    );
  });
