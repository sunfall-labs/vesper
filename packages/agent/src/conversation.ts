import type { LogStore } from '@sunfall/vesper-log/log-store';
import type { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import {
  Array as Arr,
  Effect,
  Option,
  Schema,
  Stream,
  type Crypto,
} from 'effect';
import { type Prompt, Tool } from 'effect/ai';

import type { Agent } from './agent.js';
import {
  ApprovalResolutionError,
  CompatibilityError,
  InteractionResolutionError,
  SignalRequestConflictError,
  SuspendedConversationError,
} from './conversation-error.js';
import { ToolDispatch } from './dispatch.js';
import type { AgentEvents } from './event.js';
import { Interaction } from './interaction.js';
import { foldToResult } from './internal/fold-to-result.js';
import * as AgentLog from './log.js';
import type { RecordingPolicy } from './recording-policy.js';
import { RecordingPolicyRuntime } from './recording-policy-runtime.js';
import {
  append as appendSignal,
  Signal as SignalSchema,
  type Signal as SignalType,
} from './internal/signal-store.js';
import { protocolOf } from './internal/protocol.js';
import * as RecoveryState from './recovery.js';

/** Out-of-band input addressed to a durable conversation. */
export const Signal = SignalSchema;
export type Signal = SignalType;

/** How a new conversation path treats a tool currently awaiting external input. */
export interface PathOptions {
  /** Re-enter the recorded provider call and issue a fresh wait token. */
  readonly pendingWait?: 'restart';
}

/** Durable external-wait records exposed to approval and worker services. */
export type WaitRecord =
  | ConversationRecord.RecordOf<'StepSuspended'>
  | ConversationRecord.RecordOf<'StepResolved'>
  | ConversationRecord.RecordOf<'StepRestarted'>;

/** A conversation envelope statically narrowed to an external-wait record. */
export type WaitEnvelope = Omit<ConversationRecord.Envelope, 'record'> & {
  readonly record: WaitRecord;
};

/** Narrow an ordinary conversation record to the durable wait lifecycle. */
export const isWaitEnvelope = (
  envelope: ConversationRecord.Envelope,
): envelope is WaitEnvelope => {
  switch (envelope.record._tag) {
    case 'StepSuspended':
    case 'StepResolved':
    case 'StepRestarted':
      return true;
    case 'BranchedFrom':
    case 'ChildSession':
    case 'CodeStateCheckpoint':
    case 'Compacted':
    case 'Completed':
    case 'RunSettled':
    case 'RunStarted':
    case 'Signal':
    case 'SignalReceived':
    case 'StateCheckpoint':
    case 'Text':
    case 'PartialOutput':
    case 'StepOpened':
    case 'StepSettled':
    case 'StepStarted':
    case 'TurnFinished':
      return false;
    default:
      return false;
  }
};

type ConcreteAgent = Agent.Any;

/**
 * The latest native interaction suspension of one call that `accepts` takes,
 * with whether it was already decided.
 */
const nativeInteraction = (
  history: ReadonlyArray<ConversationRecord.Envelope>,
  toolCallId: LogVocabulary.ToolCallId,
  accepts: (record: ConversationRecord.RecordOf<'StepSuspended'>) => boolean,
): RecoveryState.Wait | undefined =>
  Option.getOrUndefined(
    Arr.findLast(
      RecoveryState.waits(history),
      ({ record }) =>
        record.id === toolCallId &&
        record.wait === ToolDispatch.INTERACTION_WAIT &&
        accepts(record),
    ),
  );

/**
 * Overloaded so only a keyed send carries the conflict error: unkeyed callers
 * keep the narrow error channel they had before keys existed.
 */
export interface Send {
  (
    signal: Signal & { readonly requestId: string },
  ): Effect.Effect<
    void,
    LogStore.LogStoreError | SignalRequestConflictError,
    LogStore.Service | Crypto.Crypto
  >;
  (
    signal: Omit<Signal, 'requestId'> & { readonly requestId?: never },
  ): Effect.Effect<
    void,
    LogStore.LogStoreError,
    LogStore.Service | Crypto.Crypto
  >;
  (
    signal: Signal,
  ): Effect.Effect<
    void,
    LogStore.LogStoreError | SignalRequestConflictError,
    LogStore.Service | Crypto.Crypto
  >;
}

/** Recoverable failures from continuing a durable conversation. */
export type Error<A extends ConcreteAgent> =
  | Agent.Error<A>
  | CompatibilityError
  | SuspendedConversationError
  | LogStore.LogStoreError
  | AgentLog.DurabilityError;

export interface Instance<
  A extends ConcreteAgent,
  Requires = Agent.Requires<A>,
  PolicyRequires = never,
> {
  /** Stable durable identity shared by runs, records, and signals. */
  readonly id: LogVocabulary.ConversationId;
  /**
   * Continue this conversation, starting it when no durable history exists.
   *
   * Omitting `input` continues from durable state alone without appending a
   * user message — the shape a run suspended on a tool approval resumes with
   * after {@link resolveApproval}, where the decision, not a new prompt, is
   * what there is to act on.
   */
  readonly run: (
    input?: Prompt.RawInput,
  ) => Effect.Effect<
    Agent.Result,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /** Stream the same durable continuation that {@link run} folds. */
  readonly stream: (
    input?: Prompt.RawInput,
  ) => Stream.Stream<
    AgentEvents.ObservedEvent<Agent.Tools<A>>,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /**
   * Re-root this conversation after `at` and continue it.
   *
   * Later records remain auditable but leave the active prompt. Branches share
   * this conversation's stream and are therefore sequential.
   */
  readonly branchFrom: (
    at: LogOffset.Offset,
    input: Prompt.RawInput,
    options?: PathOptions,
  ) => Effect.Effect<
    Agent.Result,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /**
   * Copy the active prefix through `at` into a new conversation and run it.
   *
   * The destination has an independent stream, so sibling forks may run
   * concurrently without fencing each other or their source.
   */
  readonly forkFrom: (
    at: LogOffset.Offset,
    targetConversationId: string,
    input: Prompt.RawInput,
    options?: PathOptions,
  ) => Effect.Effect<
    Agent.Result,
    Error<A>,
    Requires | LogStore.Service | Crypto.Crypto
  >;
  /** Read a finite snapshot of records currently stored after `after`. */
  readonly records: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<
    ConversationRecord.Envelope,
    LogStore.LogStoreError,
    LogStore.Service
  >;
  /** Replay records after `after`, then follow future appends until interrupted. */
  readonly follow: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<
    ConversationRecord.Envelope,
    LogStore.LogStoreError,
    LogStore.Service
  >;
  /** Read a finite, typed snapshot of external wait lifecycle records. */
  readonly waits: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<WaitEnvelope, LogStore.LogStoreError, LogStore.Service>;
  /** Replay and follow typed external wait lifecycle records. */
  readonly followWaits: (
    after?: LogOffset.Offset,
  ) => Stream.Stream<WaitEnvelope, LogStore.LogStoreError, LogStore.Service>;
  /**
   * Persist an out-of-band steer or cancellation for the next run boundary.
   *
   * Payloads are limited to 256 KiB. Submission is durable and delivery is
   * at-least-once. Set `requestId` to make submission idempotent within this
   * conversation: retrying after an ambiguous transport failure with the same
   * key and payload acknowledges the original instead of appending again, and
   * the same key with a different payload fails with
   * {@link SignalRequestConflictError}. Without a key every call appends.
   */
  readonly send: Send;
  /**
   * Durably decide one tool call this conversation suspended on a
   * `needsApproval` gate.
   *
   * Records the decision and returns; it does not itself dispatch the tool
   * or resolve the run. The next `run` (or `stream`) picks it up: approved
   * dispatches the handler for the first time, denied settles a
   * refusal-style tool result without ever entering it. Call it again for
   * the same `toolCallId` and it fails with {@link ApprovalResolutionError}
   * rather than silently applying — or discarding — a second decision.
   */
  readonly resolveApproval: (
    toolCallId: string,
    decision: 'approve' | 'deny',
    reason?: string,
  ) => Effect.Effect<
    void,
    | ApprovalResolutionError
    | CompatibilityError
    | SuspendedConversationError
    | AgentLog.DurabilityError
    | LogStore.LogStoreError,
    LogStore.Service | Crypto.Crypto | PolicyRequires
  >;
  /**
   * Validate, encode, and durably complete a pre-dispatch interaction with
   * the tool's typed success value.
   */
  readonly resolveInteraction: <
    Name extends string,
    C extends Interaction.ToolConfig,
    ToolRequires,
  >(
    tool: Tool.Tool<Name, C, ToolRequires> & Interaction.Answer,
    toolCallId: string,
    result: C['success']['Type'],
  ) => Effect.Effect<
    void,
    | Schema.SchemaError
    | InteractionResolutionError
    | CompatibilityError
    | SuspendedConversationError
    | AgentLog.DurabilityError
    | LogStore.LogStoreError,
    | LogStore.Service
    | Crypto.Crypto
    | C['success']['EncodingServices']
    | PolicyRequires
  >;
}

const bind = <A extends ConcreteAgent, PolicyRequires = never>(
  agent: A,
  conversationId: string,
  policy?: RecordingPolicy.Policy<PolicyRequires>,
): Instance<A, Agent.Requires<A> | PolicyRequires, PolicyRequires> => {
  const id = LogVocabulary.ConversationId.make(conversationId);
  // Resolutions record the caller's answer, so they pass through the same
  // recording policy every run of this conversation does: an answer must be
  // redacted on its way into the log exactly as a tool result would be.
  const openRecorded = Effect.flatMap(
    AgentLog.open(id, {
      compatibility: { agent: agent.name, revision: agent.revision },
    }),
    (session) =>
      policy === undefined
        ? Effect.succeed(session)
        : Effect.map(Effect.context<PolicyRequires>(), (context) =>
            AgentLog.withRecordingPolicy(
              session,
              RecordingPolicyRuntime.compile(policy, context),
            ),
          ),
  );
  const protocol = protocolOf<
    Agent.Requires<A>,
    Agent.Error<A>,
    Agent.Tools<A>
  >(agent);
  if (protocol === undefined) {
    throw new Error('Conversation agent was not created by Agent.make');
  }
  const streamFrom = (
    input: Prompt.RawInput,
    options?: {
      readonly branchFrom?: LogOffset.Offset;
      readonly forkConversationId?: LogVocabulary.ConversationId;
      readonly pendingWait?: 'restart';
    },
  ) =>
    protocol.stream(
      id,
      input,
      policy === undefined ? options : { ...options, policy },
    );
  // Overload signatures only: the keyed/unkeyed split is a static refinement
  // of one runtime behaviour, since only a keyed append can raise a conflict.
  function send(
    signal: Signal & { readonly requestId: string },
  ): ReturnType<Send>;
  function send(
    signal: Omit<Signal, 'requestId'> & { readonly requestId?: never },
  ): Effect.Effect<
    void,
    LogStore.LogStoreError,
    LogStore.Service | Crypto.Crypto
  >;
  function send(signal: Signal): ReturnType<Send>;
  function send(signal: Signal) {
    return appendSignal(id, signal);
  }
  return {
    id,
    run: (input) => foldToResult(streamFrom(input ?? [])),
    stream: (input) => streamFrom(input ?? []),
    branchFrom: (at, input, options) =>
      foldToResult(
        streamFrom(input, {
          branchFrom: at,
          ...(options?.pendingWait === undefined
            ? {}
            : { pendingWait: options.pendingWait }),
        }),
      ),
    forkFrom: (at, targetConversationId, input, options) =>
      foldToResult(
        streamFrom(input, {
          branchFrom: at,
          forkConversationId:
            LogVocabulary.ConversationId.make(targetConversationId),
          ...(options?.pendingWait === undefined
            ? {}
            : { pendingWait: options.pendingWait }),
        }),
      ),
    records: (after) => AgentLog.snapshot(id, after),
    follow: (after) => AgentLog.follow(id, after),
    waits: (after) =>
      AgentLog.snapshot(id, after).pipe(Stream.filter(isWaitEnvelope)),
    followWaits: (after) =>
      AgentLog.follow(id, after).pipe(Stream.filter(isWaitEnvelope)),
    send,
    resolveApproval: (toolCallId, decision, reason) =>
      Effect.gen(function* () {
        const normalizedId = LogVocabulary.ToolCallId.make(toolCallId);
        const session = yield* openRecorded;
        // The lookup reads each wait's lifecycle across `session.history` —
        // the resume view that spans runs back to the latest compaction
        // boundary — rather than the recovery snapshot
        // (`suspendedToolCalls`/`hasCompletedWait`). The
        // snapshot indexes only what the *next run* must resolve, so it
        // empties once the owning run settles: reading it here misreported a
        // resolved-then-dispatched approval as `not_found`, and — worse —
        // could let a later conflicting decision through, because the
        // completion had fallen out of the snapshot along with the
        // suspension. The history keeps both records for as long as the
        // suspension itself is visible, so the two answers cannot go blind
        // independently.
        const suspension = nativeInteraction(
          session.history,
          normalizedId,
          (record) => record.interaction?.mode !== 'answer',
        );
        if (suspension === undefined) {
          return yield* new ApprovalResolutionError({
            message: `No tool call ${normalizedId} is durably waiting for approval in conversation ${id}`,
            conversationId: id,
            toolCallId: normalizedId,
            reason: 'not_found',
          });
        }
        if (suspension.resolved) {
          return yield* new ApprovalResolutionError({
            message: `Tool call ${normalizedId} in conversation ${id} was already resolved`,
            conversationId: id,
            toolCallId: normalizedId,
            reason: 'already_resolved',
          });
        }
        // A denial's result is what `dispatch.ts`'s `resolveIndeterminate`
        // copies straight into the refusal `StepSettled` the model is shown,
        // without ever entering the handler. It is Effect's own
        // `Tool.ExecutionFailure`, the shape `LanguageModel` gives an in-band
        // denial, so it decodes through every tool's failure schema and reads
        // the same to the model whichever approval path refused the call.
        // Unused on approval; what runs the tool for real is the handler
        // dispatch that follows, not this record.
        const result =
          decision === 'approve'
            ? null
            : yield* Schema.encodeUnknownEffect(Tool.ExecutionFailure)({
                type: 'execution-denied',
                reason:
                  reason === undefined
                    ? `Tool call ${normalizedId} was denied approval`
                    : `Tool call ${normalizedId} was denied approval: ${reason}`,
              }).pipe(Effect.orDie);
        return yield* session.append([
          {
            _tag: 'StepResolved',
            id: normalizedId,
            name: suspension.record.name,
            wait: ToolDispatch.INTERACTION_WAIT,
            token: suspension.record.token,
            outcome: decision === 'approve' ? 'success' : 'failure',
            result,
          },
        ]);
      }),
    resolveInteraction: (tool, toolCallId, result) =>
      Effect.gen(function* () {
        const interaction = Interaction.metadata(tool);
        if (
          interaction._tag === 'None' ||
          interaction.value.mode !== 'answer'
        ) {
          return yield* Effect.die(
            new Error(`Tool "${tool.name}" is not an answer interaction`),
          );
        }
        const encodedResult = yield* Schema.encodeUnknownEffect(
          tool.successSchema,
        )(result);
        const normalizedId = LogVocabulary.ToolCallId.make(toolCallId);
        const session = yield* openRecorded;
        const suspension = nativeInteraction(
          session.history,
          normalizedId,
          (record) =>
            record.interaction?.mode === 'answer' &&
            record.interaction.name === interaction.value.name,
        );
        if (suspension === undefined) {
          return yield* new InteractionResolutionError({
            message: `No tool call ${normalizedId} is durably waiting for interaction "${interaction.value.name}" in conversation ${id}`,
            conversationId: id,
            toolCallId: normalizedId,
            interaction: interaction.value.name,
            reason: 'not_found',
          });
        }
        if (suspension.resolved) {
          return yield* new InteractionResolutionError({
            message: `Tool call ${normalizedId} in conversation ${id} was already resolved`,
            conversationId: id,
            toolCallId: normalizedId,
            interaction: interaction.value.name,
            reason: 'already_resolved',
          });
        }
        return yield* session.append([
          {
            _tag: 'StepResolved',
            id: normalizedId,
            name: suspension.record.name,
            wait: ToolDispatch.INTERACTION_WAIT,
            token: suspension.record.token,
            outcome: 'success',
            result: encodedResult,
          },
        ]);
      }),
  };
};

/** Bind an agent to one durable conversation identity. */
export function make<A extends ConcreteAgent>(
  agent: A,
  conversationId: string,
): Instance<A>;
/** Bind an agent with one recording policy applied across every durable run. */
export function make<A extends ConcreteAgent, const P extends object>(
  agent: A,
  conversationId: string,
  policy: P & RecordingPolicy.Policy<RecordingPolicy.Services<P>>,
): Instance<
  A,
  Agent.Requires<A> | RecordingPolicy.Services<P>,
  RecordingPolicy.Services<P>
>;
export function make<A extends ConcreteAgent, const P extends object>(
  agent: A,
  conversationId: string,
  policy?: P & RecordingPolicy.Policy<RecordingPolicy.Services<P>>,
): Instance<
  A,
  Agent.Requires<A> | RecordingPolicy.Services<P>,
  RecordingPolicy.Services<P>
> {
  return bind(agent, conversationId, policy);
}

export {
  ApprovalResolutionError,
  CompatibilityError,
  InteractionResolutionError,
  SignalRequestConflictError,
  SuspendedConversationError,
};
export { DurabilityError } from './conversation-error.js';

export * as Conversation from './conversation.js';
