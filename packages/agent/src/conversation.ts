import type { LogStore } from '@sunfall/vesper-log/log-store';
import type { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Schema, Stream, type Crypto } from 'effect';
import { AiError, type Prompt } from 'effect/unstable/ai';

import type { Agent } from './agent.js';
import {
  ApprovalResolutionError,
  CompatibilityError,
  SuspendedConversationError,
} from './conversation-error.js';
import { ToolDispatch } from './dispatch.js';
import type { AgentEvents } from './event.js';
import { foldToResult } from './internal/fold-to-result.js';
import * as AgentLog from './log.js';
import type { RecordingPolicy } from './recording-policy.js';
import {
  append as appendSignal,
  Signal as SignalSchema,
  type Signal as SignalType,
} from './internal/signal-store.js';
import { protocolOf } from './internal/protocol.js';

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
  | ConversationRecord.RecordOf<'ToolSuspended'>
  | ConversationRecord.RecordOf<'ToolResumed'>
  | ConversationRecord.RecordOf<'ToolWaitCompleted'>
  | ConversationRecord.RecordOf<'ToolWaitRestarted'>;

/** A conversation envelope statically narrowed to an external-wait record. */
export type WaitEnvelope = Omit<ConversationRecord.Envelope, 'record'> & {
  readonly record: WaitRecord;
};

/** Narrow an ordinary conversation record to the durable wait lifecycle. */
export const isWaitEnvelope = (
  envelope: ConversationRecord.Envelope,
): envelope is WaitEnvelope => {
  switch (envelope.record._tag) {
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
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
    case 'ToolCall':
    case 'ToolOutcome':
    case 'ToolStarted':
    case 'TurnFinished':
      return false;
    default:
      return false;
  }
};

type ConcreteAgent = Agent.Any;

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
   * at-least-once, but `send` has no idempotency key: retrying after an
   * ambiguous transport failure may append the same logical signal twice.
   */
  readonly send: (
    signal: Signal,
  ) => Effect.Effect<
    void,
    LogStore.LogStoreError,
    LogStore.Service | Crypto.Crypto
  >;
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
    LogStore.Service | Crypto.Crypto
  >;
}

const bind = <A extends ConcreteAgent, PolicyRequires = never>(
  agent: A,
  conversationId: string,
  policy?: RecordingPolicy.Policy<PolicyRequires>,
): Instance<A, Agent.Requires<A> | PolicyRequires> => {
  const id = LogVocabulary.ConversationId.make(conversationId);
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
    send: (signal) => appendSignal(id, signal),
    resolveApproval: (toolCallId, decision, reason) =>
      Effect.gen(function* () {
        const normalizedId = LogVocabulary.ToolCallId.make(toolCallId);
        const session = yield* AgentLog.open(id, {
          compatibility: { agent: agent.name, revision: agent.revision },
        });
        // Both lookups scan `session.history` — the resume view that spans
        // runs back to the latest compaction boundary — rather than the
        // recovery snapshot (`suspendedToolCalls`/`hasCompletedWait`). The
        // snapshot indexes only what the *next run* must resolve, so it
        // empties once the owning run settles: reading it here misreported a
        // resolved-then-dispatched approval as `not_found`, and — worse —
        // could let a later conflicting decision through, because the
        // completion had fallen out of the snapshot along with the
        // suspension. The history keeps both records for as long as the
        // suspension itself is visible, so the two answers cannot go blind
        // independently.
        let suspended:
          | { readonly name: string; readonly token: string }
          | undefined;
        let resolved = false;
        for (const envelope of session.history) {
          const record = envelope.record;
          if (
            record._tag === 'ToolSuspended' &&
            record.id === normalizedId &&
            record.wait === ToolDispatch.APPROVAL_WAIT
          ) {
            suspended = { name: record.name, token: record.token };
            resolved = false;
          } else if (
            suspended !== undefined &&
            record._tag === 'ToolWaitCompleted' &&
            record.token === suspended.token
          ) {
            resolved = true;
          }
        }
        if (suspended === undefined) {
          return yield* new ApprovalResolutionError({
            message: `No tool call ${normalizedId} is durably waiting for approval in conversation ${id}`,
            conversationId: id,
            toolCallId: normalizedId,
            reason: 'not_found',
          });
        }
        if (resolved) {
          return yield* new ApprovalResolutionError({
            message: `Tool call ${normalizedId} in conversation ${id} was already resolved`,
            conversationId: id,
            toolCallId: normalizedId,
            reason: 'already_resolved',
          });
        }
        // A denial's result is what `dispatch.ts`'s `resolveIndeterminate`
        // copies straight into the refusal `ToolOutcome` the model is shown,
        // without ever entering the handler — the same encoded `AiError`
        // shape `failureMode: 'return'` already uses for a framework-level
        // failure, so it decodes through any tool's result schema, not only
        // one that declared its own failure type. Encoded through `Schema`
        // rather than built by hand: `AiError`'s wire shape is not one to
        // keep in sync by inspection. Unused on approval; what runs the tool
        // for real is the handler dispatch that follows, not this record.
        const result =
          decision === 'approve'
            ? null
            : yield* Schema.encodeUnknownEffect(AiError.AiError)(
                new AiError.AiError({
                  module: 'Conversation',
                  method: 'resolveApproval',
                  reason: new AiError.UnknownError({
                    description:
                      reason === undefined
                        ? `Tool call ${normalizedId} was denied approval`
                        : `Tool call ${normalizedId} was denied approval: ${reason}`,
                    metadata: { toolCallId: normalizedId },
                  }),
                }),
              ).pipe(Effect.orDie);
        return yield* session.append([
          {
            _tag: 'ToolWaitCompleted',
            id: normalizedId,
            name: suspended.name,
            wait: ToolDispatch.APPROVAL_WAIT,
            token: suspended.token,
            outcome: decision === 'approve' ? 'success' : 'failure',
            result,
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
): Instance<A, Agent.Requires<A> | RecordingPolicy.Services<P>>;
export function make<A extends ConcreteAgent, const P extends object>(
  agent: A,
  conversationId: string,
  policy?: P & RecordingPolicy.Policy<RecordingPolicy.Services<P>>,
): Instance<A, Agent.Requires<A> | RecordingPolicy.Services<P>> {
  return bind(agent, conversationId, policy);
}

export {
  ApprovalResolutionError,
  CompatibilityError,
  SuspendedConversationError,
};
export { DurabilityError } from './conversation-error.js';

export * as Conversation from './conversation.js';
