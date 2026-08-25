import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import {
  ConversationId as ConversationIdSchema,
  LogVocabulary,
} from '@sunfall/vesper-log/vocabulary';
import type { Cause, Crypto, Schedule, SchemaIssue } from 'effect';
import { Effect, Exit, Layer, Option, Schema, Stream } from 'effect';
import { AiError, type Prompt, type Tool } from 'effect/unstable/ai';
import {
  Activity,
  DurableDeferred,
  Workflow,
  WorkflowEngine,
} from 'effect/unstable/workflow';
import type { WorkflowInstance } from 'effect/unstable/workflow/WorkflowEngine';

import { Agent } from './agent.js';
import { AgentBranch } from './branch.js';
import { Conversation } from './conversation.js';
import * as ToolExecution from './internal/tool-execution.js';

/** Durable request fields shared by execution, cancellation, and resumption. */
export const RequestFields = {
  conversationId: Schema.String,
  input: Schema.String,
} as const;

type ReservedRequestField = keyof typeof RequestFields;
type ReservedFree<Fields extends Schema.Struct.Fields> =
  Extract<keyof Fields, ReservedRequestField> extends never ? Fields : never;

type RequestSchema<
  Fields extends Schema.Struct.Fields,
  Input extends Schema.Constraint,
> = Schema.Struct<
  {
    readonly conversationId: typeof Schema.String;
    readonly input: Input;
  } & Fields
>;

/** Define a workflow request with Vesper's identity and plain-text input. */
export function request<const Fields extends Schema.Struct.Fields>(
  fields: ReservedFree<Fields>,
): RequestSchema<Fields, typeof Schema.String>;

/** Define a workflow request whose application input has its own schema. */
export function request<
  const Fields extends Schema.Struct.Fields,
  Input extends Schema.Constraint,
>(fields: ReservedFree<Fields>, input: Input): RequestSchema<Fields, Input>;

export function request(
  fields: Schema.Struct.Fields,
  input: Schema.Constraint = Schema.String,
) {
  return Schema.Struct({
    conversationId: Schema.String,
    input,
    ...fields,
  });
}

export interface Request<Input = string> {
  readonly conversationId: string;
  readonly input: Input;
}

const IdentityTypeId: unique symbol = Symbol.for(
  '@sunfall/vesper-agent/AgentWorkflow/Identity',
);

/** One exact workflow execution and its corresponding Vesper conversation. */
export interface Identity {
  readonly [IdentityTypeId]: typeof IdentityTypeId;
  readonly workflow: 'run' | 'path';
  readonly executionId: string;
  readonly conversationId: LogVocabulary.ConversationId;
}

type WorkflowPayload<Payload extends Workflow.AnyStructSchema> =
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload;

/** Recoverable failures shared by workflow-backed branch and fork execution. */
export type PathFailure<Error extends Schema.Top> =
  | Error['Type']
  | Schema.SchemaError
  | SchemaIssue.Issue
  | PathError;

/** Services shared by workflow-backed branch and fork execution. */
export type PathRequirements<
  Payload extends Workflow.AnyStructSchema,
  Error extends Schema.Top,
> =
  | WorkflowEngine.WorkflowEngine
  | Payload['EncodingServices']
  | (typeof Agent.Result)['DecodingServices']
  | Error['DecodingServices'];

/** Start or await a new workflow-owned conversation path. */
export interface PathOperation<
  Payload extends Workflow.AnyStructSchema,
  Error extends Schema.Top,
> {
  (
    source: Identity,
    at: LogOffset.Offset,
    payload: Payload['~type.make.in'],
    options: { readonly discard: true },
  ): Effect.Effect<
    Identity,
    PathFailure<Error>,
    PathRequirements<Payload, Error>
  >;
  (
    source: Identity,
    at: LogOffset.Offset,
    payload: Payload['~type.make.in'],
    options?: { readonly discard?: false },
  ): Effect.Effect<
    Agent.Result,
    PathFailure<Error>,
    PathRequirements<Payload, Error>
  >;
}

/** A durable Effect workflow bound to one Vesper agent definition. */
export interface Binding<
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Error extends Schema.Top,
  Requires,
> {
  readonly workflow: Workflow.Workflow<
    Tag,
    Payload,
    typeof Agent.Result,
    Error
  >;
  /** Register the workflow handler. The application still chooses its engine. */
  readonly layer: Layer.Layer<never, never, Requires>;
  /** Derive both durable identities from one validated workflow payload. */
  readonly identify: (
    payload: Payload['~type.make.in'],
  ) => Effect.Effect<Identity, SchemaIssue.Issue>;
  /** Persist Vesper cancellation intent, then terminally interrupt the workflow. */
  readonly cancel: (
    identity: Identity,
    signal: CancelSignal,
  ) => Effect.Effect<
    void,
    LogStore.LogStoreError,
    WorkflowEngine.WorkflowEngine | LogStore.Service | Crypto.Crypto
  >;
  /** Supersede a suspended source execution and restart its wait on a branch. */
  readonly branchFrom: PathOperation<Payload, Error>;
  /** Keep the source execution and restart its wait independently in a fork. */
  readonly forkFrom: PathOperation<Payload, Error>;
}

export interface CancelSignal {
  readonly text: string;
  readonly source: string;
}

interface OptionsBase<
  Tag extends string,
  Payload extends Workflow.AnyStructSchema & Schema.Schema<Request<unknown>>,
  Error extends Schema.Top,
  AgentError,
> {
  readonly tag: Tag;
  readonly payload: Payload;
  readonly idempotencyKey: (payload: Payload['Type']) => string;
  readonly error: Error;
  readonly mapError: (error: AgentError) => Error['Type'];
  readonly suspendedRetrySchedule?: Schedule.Schedule<unknown> | undefined;
}

/** Options for the ordinary plain-text workflow request. */
export interface Options<
  Tag extends string,
  Payload extends Workflow.AnyStructSchema & Schema.Schema<Request>,
  Error extends Schema.Top,
  AgentError,
> extends OptionsBase<Tag, Payload, Error, AgentError> {
  /** Override the default plain-text input projection. */
  readonly input?: (payload: Payload['Type']) => Prompt.RawInput;
}

/** Options for schema-typed application input projected into Effect Prompt. */
export interface InputOptions<
  Tag extends string,
  Payload extends Workflow.AnyStructSchema & Schema.Schema<Request<unknown>>,
  Error extends Schema.Top,
  AgentError,
> extends OptionsBase<Tag, Payload, Error, AgentError> {
  readonly input: (payload: Payload['Type']) => Prompt.RawInput;
}

/** A named, replayable effect inside an agent workflow. */
export interface Step<
  Input,
  Success extends Schema.Constraint,
  Error extends Schema.Constraint,
  Requires,
> {
  (input: Input): Activity.Activity<Success, Error, Requires>;
  readonly stepName: string;
  readonly success: Success;
  readonly error: Error;
}

/** Options for one durable activity exposed as an ordinary function. */
export interface StepOptions<
  Input,
  Success extends Schema.Constraint,
  Error extends Schema.Constraint,
  Requires,
> {
  readonly name: string;
  /** Stable identity for this logical call within one workflow execution. */
  readonly key: (input: Input) => string;
  readonly success: Success;
  readonly error: Error;
  readonly execute: (
    input: Input,
  ) => Effect.Effect<Success['Type'], Error['Type'], Requires>;
  readonly interruptRetryPolicy?:
    | Schedule.Schedule<unknown, Cause.Cause<unknown>>
    | undefined;
}

/**
 * Mark a tool as requiring Effect Workflow's durable execution context.
 *
 * Use this for handlers that call {@link step} or {@link wait}. The added
 * dependencies stay visible in the agent's requirement type and are supplied
 * by {@link make}; the same handler cannot accidentally run durably through a
 * plain agent invocation.
 */
export const durable = <
  Name extends string,
  Config extends {
    readonly parameters: Schema.Constraint;
    readonly success: Schema.Constraint;
    readonly failure: Schema.Constraint;
    readonly failureMode: Tool.FailureMode;
  },
  Requirements,
>(
  tool: Tool.Tool<Name, Config, Requirements>,
) =>
  tool
    .addDependency(WorkflowEngine.WorkflowEngine)
    .addDependency(WorkflowEngine.WorkflowInstance)
    .addDependency(ToolExecution.Current);

/** A malformed token, or one addressed to a different wait definition. */
export class WaitTokenError extends Schema.TaggedError<WaitTokenError>(
  '@sunfall/vesper-agent/AgentWorkflow/WaitTokenError',
)('WaitTokenError', { message: Schema.String }) {}

/** Ambiguous durable state for one independently keyed external wait. */
const WaitStateErrorFields: {
  readonly message: typeof Schema.String;
  readonly conversationId: typeof ConversationIdSchema;
  readonly wait: typeof Schema.String;
  readonly key: typeof Schema.String;
} = {
  message: Schema.String,
  conversationId: ConversationIdSchema,
  wait: Schema.String,
  key: Schema.String,
};

export class WaitStateError extends Schema.TaggedError<WaitStateError>(
  '@sunfall/vesper-agent/AgentWorkflow/WaitStateError',
)('WaitStateError', WaitStateErrorFields) {}

/** Invalid source/target identity for a workflow branch or fork. */
export class PathError extends Schema.TaggedError<PathError>(
  '@sunfall/vesper-agent/AgentWorkflow/PathError',
)('PathError', { message: Schema.String }) {}

const changesPending = (record: ConversationRecord.Record): boolean => {
  switch (record._tag) {
    case 'ToolSuspended':
    case 'ToolResumed':
    case 'ToolWaitCompleted':
    case 'ToolWaitRestarted':
    case 'ToolOutcome':
    case 'RunSettled':
    case 'Completed':
    case 'BranchedFrom':
      return true;
    case 'ChildSession':
    case 'CodeStateCheckpoint':
    case 'Compacted':
    case 'RunStarted':
    case 'Signal':
    case 'SignalReceived':
    case 'StateCheckpoint':
    case 'Text':
    case 'ToolCall':
    case 'ToolStarted':
    case 'TurnFinished':
      return false;
    default:
      return false;
  }
};

interface PendingWaitFields<RequestValue> {
  readonly conversationId: LogVocabulary.ConversationId;
  readonly offset: LogOffset.Offset;
  readonly toolCallId: LogVocabulary.ToolCallId;
  readonly toolName: string;
  readonly key: string;
  readonly token: string;
  readonly request: RequestValue;
}

/** One independently keyed external wait that can still be completed. */
export interface PendingWait<
  RequestValue,
  Success extends Schema.Constraint,
  Error extends Schema.Constraint,
> extends PendingWaitFields<RequestValue> {
  readonly complete: (
    value: Success['Type'],
  ) => Effect.Effect<
    void,
    Schema.SchemaError | WaitTokenError,
    WorkflowEngine.WorkflowEngine | Success['EncodingServices']
  >;
  readonly fail: (
    error: Error['Type'],
  ) => Effect.Effect<
    void,
    Schema.SchemaError | WaitTokenError,
    WorkflowEngine.WorkflowEngine | Error['EncodingServices']
  >;
}

/** A named durable wait used from inside a recorded workflow tool handler. */
export interface Wait<
  WaitRequest extends Schema.Constraint,
  Success extends Schema.Constraint,
  Error extends Schema.Constraint,
> {
  (
    request: WaitRequest['Type'],
  ): Effect.Effect<
    Success['Type'],
    Error['Type'] | AiError.AiError,
    | WorkflowEngine.WorkflowEngine
    | WorkflowInstance
    | ToolExecution.Current
    | WaitRequest['EncodingServices']
    | Success['EncodingServices']
    | Error['EncodingServices']
    | Success['DecodingServices']
    | Error['DecodingServices']
  >;
  readonly waitName: string;
  readonly request: WaitRequest;
  readonly success: Success;
  readonly error: Error;
  /** Wait for the independently keyed request that needs an external result. */
  readonly awaitPending: <A extends Agent.Any, Requires>(
    conversation: Conversation.Instance<A, Requires>,
    key: string,
  ) => Effect.Effect<
    PendingWait<WaitRequest['Type'], Success, Error>,
    LogStore.LogStoreError | Schema.SchemaError | WaitStateError,
    LogStore.Service | WaitRequest['DecodingServices']
  >;
  /** Complete a serialized pending wait by its exact durable token. */
  readonly complete: (
    token: string,
    value: Success['Type'],
  ) => Effect.Effect<
    void,
    Schema.SchemaError | WaitTokenError,
    WorkflowEngine.WorkflowEngine | Success['EncodingServices']
  >;
  readonly fail: (
    token: string,
    error: Error['Type'],
  ) => Effect.Effect<
    void,
    Schema.SchemaError | WaitTokenError,
    WorkflowEngine.WorkflowEngine | Error['EncodingServices']
  >;
}

/** Options for one externally completed durable handler wait. */
export interface WaitOptions<
  WaitRequest extends Schema.Constraint,
  Success extends Schema.Constraint,
  Error extends Schema.Constraint,
> {
  readonly name: string;
  /** Stable identity for this logical wait within one workflow execution. */
  readonly key: (request: WaitRequest['Type']) => string;
  readonly request: WaitRequest;
  readonly success: Success;
  readonly error: Error;
}

/**
 * Define a durable external wait for use inside a tool handler.
 *
 * The request is written to the conversation as `ToolSuspended`; completing
 * its token wakes the owning Effect Workflow. On replay, Vesper re-enters only
 * the deliberately suspended handler and Effect Workflow restores completed
 * activities before returning the external result here.
 */
export const wait = <
  WaitRequest extends Schema.Constraint,
  Success extends Schema.Constraint,
  Error extends Schema.Constraint,
>(
  options: WaitOptions<WaitRequest, Success, Error>,
): Wait<WaitRequest, Success, Error> => {
  if (options.name.length === 0) {
    throw new Error('AgentWorkflow.wait requires a non-empty name');
  }
  const error = options.error;
  const prefix = `${options.name}/`;
  const keyFor = (requestValue: WaitRequest['Type']) => {
    const key = options.key(requestValue);
    if (key.length === 0) {
      throw new Error(
        `AgentWorkflow wait "${options.name}" produced an empty key`,
      );
    }
    return key;
  };
  const deferredFor = (key: string) =>
    DurableDeferred.make(`${prefix}${encodeURIComponent(key)}`, {
      success: options.success,
      error,
    });
  const encodeFailure =
    (stage: string, value: unknown) => (issue: Schema.SchemaError) =>
      new AiError.AiError({
        module: 'AgentWorkflow',
        method: `${options.name}.${stage}`,
        reason: new AiError.ToolResultEncodingError({
          toolName: options.name,
          toolResult: value,
          description: issue.message,
        }),
      });
  const fromToken = (token: string) =>
    Effect.gen(function* () {
      const parsed = yield* Schema.decodeUnknownEffect(
        DurableDeferred.TokenParsed.FromString,
      )(token);
      if (!parsed.deferredName.startsWith(prefix)) {
        return yield* new WaitTokenError({
          message: `Token is for wait "${parsed.deferredName}", not "${options.name}"`,
        });
      }
      return {
        token: parsed.asToken,
        deferred: DurableDeferred.make(parsed.deferredName, {
          success: options.success,
          error,
        }),
      };
    });

  const run = (requestValue: WaitRequest['Type']) => {
    const key = keyFor(requestValue);
    return Effect.gen(function* () {
      const execution = yield* ToolExecution.Current;
      const deferred = deferredFor(key);
      const token = yield* DurableDeferred.token(deferred);
      const engine = yield* WorkflowEngine.WorkflowEngine;
      const existing = yield* engine.deferredResult(deferred);
      if (Option.isSome(existing)) {
        if (!execution.session.hasCompletedWait(token)) {
          const result = yield* Schema.encodeUnknownEffect(
            Schema.toCodecJson(deferred.exitSchema),
          )(existing.value).pipe(
            Effect.mapError(encodeFailure('resume', existing.value)),
          );
          yield* execution.session.append([
            {
              _tag: 'ToolWaitCompleted',
              id: execution.toolCallId,
              name: execution.name,
              wait: options.name,
              token,
              outcome: Exit.isSuccess(existing.value) ? 'success' : 'failure',
              result,
            },
          ]);
        }
        return yield* existing.value;
      }

      const encoded = yield* Schema.encodeUnknownEffect(options.request)(
        requestValue,
      ).pipe(Effect.mapError(encodeFailure('request', requestValue)));
      yield* execution.session.append([
        {
          _tag: 'ToolSuspended',
          id: execution.toolCallId,
          name: execution.name,
          wait: options.name,
          token,
          request: encoded,
        },
      ]);
      return yield* DurableDeferred.await(deferred);
    });
  };

  type SuspendedEnvelope = Conversation.WaitEnvelope & {
    readonly record: Extract<
      Conversation.WaitRecord,
      { readonly _tag: 'ToolSuspended' }
    >;
  };

  const projectPending = <A extends Agent.Any, Requires>(
    conversation: Conversation.Instance<A, Requires>,
    stored: ReadonlyArray<ConversationRecord.Envelope>,
  ): Effect.Effect<
    ReadonlyArray<PendingWaitFields<WaitRequest['Type']>>,
    Schema.SchemaError,
    WaitRequest['DecodingServices']
  > => {
    const actionable = new Map<string, SuspendedEnvelope>();

    for (const envelope of AgentBranch.activePath(stored)) {
      const { record } = envelope;
      switch (record._tag) {
        case 'ToolSuspended':
          if (record.wait === options.name) {
            actionable.set(record.token, {
              // Envelope is a schema-decoded data object. Its declaration-
              // mergeable public interface shares a name with the schema
              // value, so the type-aware rule mistakes it for a class.
              // oxlint-disable-next-line typescript/no-misused-spread
              ...envelope,
              record,
            });
          }
          break;
        case 'ToolResumed':
        case 'ToolWaitCompleted':
          actionable.delete(record.token);
          break;
        case 'ToolWaitRestarted':
          actionable.delete(record.priorToken);
          break;
        case 'ToolOutcome':
          for (const [token, suspended] of actionable) {
            if (suspended.record.id === record.id) {
              actionable.delete(token);
            }
          }
          break;
        case 'RunSettled':
        case 'Completed':
          actionable.clear();
          break;
        case 'BranchedFrom':
        case 'ChildSession':
        case 'CodeStateCheckpoint':
        case 'Compacted':
        case 'RunStarted':
        case 'Signal':
        case 'SignalReceived':
        case 'StateCheckpoint':
        case 'Text':
        case 'ToolCall':
        case 'ToolStarted':
        case 'TurnFinished':
          break;
        default:
          break;
      }
    }

    return Effect.forEach(actionable.values(), (envelope) =>
      Schema.decodeUnknownEffect(options.request)(envelope.record.request).pipe(
        Effect.map(
          (decodedRequest): PendingWaitFields<WaitRequest['Type']> => ({
            conversationId: conversation.id,
            offset: envelope.offset,
            toolCallId: envelope.record.id,
            toolName: envelope.record.name,
            key: keyFor(decodedRequest),
            token: envelope.record.token,
            request: decodedRequest,
          }),
        ),
      ),
    );
  };

  const snapshot = <A extends Agent.Any, Requires>(
    conversation: Conversation.Instance<A, Requires>,
  ) =>
    conversation.records().pipe(
      Stream.runCollect,
      // `execute(..., { discard: true })` may return before the workflow has
      // created its conversation stream. A pending wait is a follower, so an
      // absent stream is its empty initial state rather than a failed lookup.
      Effect.catchIf(
        (cause) => cause.reason === 'not_found',
        () => Effect.succeed<ReadonlyArray<ConversationRecord.Envelope>>([]),
      ),
      Effect.flatMap((stored) =>
        Effect.map(
          projectPending(conversation, Array.from(stored)),
          (items) => ({
            items,
            cursor: stored.at(-1)?.offset ?? LogOffset.START,
          }),
        ),
      ),
    );

  const complete = (token: string, value: Success['Type']) =>
    Effect.flatMap(fromToken(token), ({ deferred, token: deferredToken }) =>
      Schema.encodeUnknownEffect(Schema.toCodecJson(options.success))(
        value,
      ).pipe(
        Effect.asVoid,
        Effect.andThen(
          DurableDeferred.succeed(deferred, { token: deferredToken, value }),
        ),
      ),
    );

  const fail = (token: string, failure: Error['Type']) =>
    Effect.flatMap(fromToken(token), ({ deferred, token: deferredToken }) =>
      Schema.encodeUnknownEffect(Schema.toCodecJson(options.error))(
        failure,
      ).pipe(
        Effect.asVoid,
        Effect.andThen(
          DurableDeferred.fail(deferred, {
            token: deferredToken,
            error: failure,
          }),
        ),
      ),
    );

  const selectPending = (
    items: ReadonlyArray<PendingWaitFields<WaitRequest['Type']>>,
    conversationId: LogVocabulary.ConversationId,
    key: string,
  ): Effect.Effect<
    Option.Option<PendingWaitFields<WaitRequest['Type']>>,
    WaitStateError
  > => {
    const matching = items.filter((pending) => pending.key === key);
    if (matching.length > 1) {
      return Effect.fail(
        new WaitStateError({
          message:
            `Wait "${options.name}" has ${String(matching.length)} active tokens ` +
            `for key "${key}" in conversation ${conversationId}`,
          conversationId,
          wait: options.name,
          key,
        }),
      );
    }
    return Effect.succeed(Option.fromNullishOr(matching[0]));
  };

  const bindPending = (
    pending: PendingWaitFields<WaitRequest['Type']>,
  ): PendingWait<WaitRequest['Type'], Success, Error> => ({
    ...pending,
    complete: (value) => complete(pending.token, value),
    fail: (failure) => fail(pending.token, failure),
  });

  const awaitPending = <A extends Agent.Any, Requires>(
    conversation: Conversation.Instance<A, Requires>,
    key: string,
  ): Effect.Effect<
    PendingWait<WaitRequest['Type'], Success, Error>,
    LogStore.LogStoreError | Schema.SchemaError | WaitStateError,
    LogStore.Service | WaitRequest['DecodingServices']
  > => {
    if (key.length === 0) {
      throw new Error(
        `AgentWorkflow wait "${options.name}" received an empty key`,
      );
    }
    return Effect.gen(function* () {
      const initial = yield* snapshot(conversation);
      const existing = yield* selectPending(
        initial.items,
        conversation.id,
        key,
      );
      if (Option.isSome(existing)) {
        return bindPending(existing.value);
      }

      const found = yield* conversation.follow(initial.cursor).pipe(
        Stream.filter(({ record }) => changesPending(record)),
        // A log append is atomic but its records have individual offsets.
        // Re-reading observes the whole append before selecting this key.
        Stream.mapEffect(() => snapshot(conversation)),
        Stream.mapEffect(({ items }) =>
          selectPending(items, conversation.id, key),
        ),
        Stream.filter(Option.isSome),
        Stream.map((pending) => pending.value),
        Stream.runHead,
      );
      if (Option.isNone(found)) {
        return yield* Effect.never;
      }
      return bindPending(found.value);
    });
  };

  return Object.assign(run, {
    waitName: options.name,
    request: options.request,
    success: options.success,
    error,
    awaitPending,
    complete,
    fail,
  });
};

type WorkflowAgentError<A extends Agent.Any> = Conversation.Error<A>;
type WorkflowAgentRequirements<A extends Agent.Any> = Exclude<
  Agent.Requires<A>,
  WorkflowEngine.WorkflowEngine | WorkflowInstance | ToolExecution.Current
>;

type BindingRequirements<
  A extends Agent.Any,
  Payload extends Workflow.AnyStructSchema,
  Error extends Schema.Top,
> =
  | WorkflowEngine.WorkflowEngine
  | WorkflowAgentRequirements<A>
  | LogStore.Service
  | WorkflowPayload<Payload>['DecodingServices' | 'EncodingServices']
  | (typeof Agent.Result)['EncodingServices']
  | Error['EncodingServices'];

type BoundWorkflow<
  A extends Agent.Any,
  Tag extends string,
  Payload extends Workflow.AnyStructSchema,
  Error extends Schema.Top,
> = Binding<
  Tag,
  WorkflowPayload<Payload>,
  Error,
  BindingRequirements<A, Payload, Error>
>;

/**
 * Define one replayable workflow step for use inside an agent tool handler.
 *
 * The returned function creates an Effect `Activity` when called. Its stable
 * name is the durable identity within one workflow execution; use distinct
 * names for distinct logical calls.
 */
export const step = <
  Input,
  Success extends Schema.Constraint,
  Error extends Schema.Constraint,
  Requires,
>(
  options: StepOptions<Input, Success, Error, Requires>,
): Step<Input, Success, Error, Requires> => {
  const run = (input: Input) => {
    const key = options.key(input);
    if (key.length === 0) {
      throw new Error(
        `AgentWorkflow step "${options.name}" produced an empty key`,
      );
    }
    return Activity.make({
      name: `${options.name}/${encodeURIComponent(key)}`,
      success: options.success,
      error: options.error,
      execute: Effect.suspend(() => options.execute(input)),
      ...(options.interruptRetryPolicy === undefined
        ? {}
        : { interruptRetryPolicy: options.interruptRetryPolicy }),
    });
  };

  return Object.assign(run, {
    stepName: options.name,
    success: options.success,
    error: options.error,
  });
};

/** Stable idempotency key for an external effect performed by a step. */
export const idempotencyKey = Activity.idempotencyKey;

/**
 * Bind an agent to Effect's durable workflow runtime.
 *
 * Vesper remains authoritative for conversation history and tool recovery;
 * the supplied WorkflowEngine remains authoritative for execution and wakeup.
 */
const bind = <
  A extends Agent.Any,
  const Tag extends string,
  const Payload extends Workflow.AnyStructSchema &
    Schema.Schema<Request<unknown>>,
  Error extends Schema.Top,
>(
  agent: A,
  options: OptionsBase<Tag, Payload, Error, WorkflowAgentError<A>>,
  inputFor: (payload: Payload['Type']) => Prompt.RawInput,
): BoundWorkflow<A, Tag, Payload, Error> => {
  const workflow = Workflow.make(options.tag, {
    payload: options.payload,
    idempotencyKey: options.idempotencyKey,
    success: Agent.Result,
    error: options.error,
    ...(options.suspendedRetrySchedule === undefined
      ? {}
      : { suspendedRetrySchedule: options.suspendedRetrySchedule }),
  });

  const PathPayload = Schema.Struct({
    mode: Schema.Literals(['branch', 'fork']),
    sourceConversationId: LogVocabulary.ConversationId,
    sourceExecutionId: Schema.String,
    at: LogOffset.Offset,
    targetExecutionId: Schema.String,
    encodedPayload: Schema.Unknown,
  });
  const pathWorkflow = Workflow.make(`${options.tag}/RestartPath`, {
    payload: PathPayload,
    idempotencyKey: ({
      at,
      mode,
      sourceConversationId,
      sourceExecutionId,
      targetExecutionId,
    }) =>
      `${mode}/${sourceConversationId}/${sourceExecutionId}/${at}/${targetExecutionId}`,
    success: Agent.Result,
    error: Schema.Union([options.error, PathError]),
    ...(options.suspendedRetrySchedule === undefined
      ? {}
      : { suspendedRetrySchedule: options.suspendedRetrySchedule }),
  });

  const layer = Layer.merge(
    workflow.toLayer((payload) =>
      Conversation.make(agent, payload.conversationId)
        .run(inputFor(payload))
        .pipe(Effect.mapError(options.mapError)),
    ),
    pathWorkflow.toLayer(({ at, encodedPayload, mode, sourceConversationId }) =>
      Effect.gen(function* () {
        const payload = yield* Schema.decodeUnknownEffect(
          workflow.payloadSchema,
        )(encodedPayload).pipe(
          Effect.mapError(
            (error) =>
              new PathError({
                message: `Cannot decode restarted workflow payload: ${String(error)}`,
              }),
          ),
        );
        return yield* (
          mode === 'branch'
            ? Conversation.make(agent, sourceConversationId).branchFrom(
                at,
                inputFor(payload),
                { pendingWait: 'restart' },
              )
            : Conversation.make(agent, sourceConversationId).forkFrom(
                at,
                payload.conversationId,
                inputFor(payload),
                { pendingWait: 'restart' },
              )
        ).pipe(Effect.mapError(options.mapError));
      }),
    ),
  );

  const identify = (
    payload: (typeof workflow.payloadSchema)['~type.make.in'],
  ) =>
    Effect.gen(function* () {
      // Validate at the public boundary so malformed caller input stays a
      // recoverable schema failure. Workflow.executionId validates internally
      // as well, because its public interface accepts raw payloads; passing
      // this validated value is the strongest guarantee available here.
      const validated = yield* workflow.payloadSchema.makeEffect(payload);
      const conversationId = yield* LogVocabulary.ConversationId.makeEffect(
        validated.conversationId,
      );
      const executionId = yield* workflow.executionId(validated);
      return {
        [IdentityTypeId]: IdentityTypeId,
        workflow: 'run',
        executionId,
        conversationId,
      } satisfies Identity;
    });

  const ownerOf = (identity: Identity) =>
    identity.workflow === 'run' ? workflow : pathWorkflow;

  const cancel = (identity: Identity, signal: CancelSignal) =>
    Effect.gen(function* () {
      yield* Conversation.make(agent, identity.conversationId).send({
        kind: 'cancel',
        text: signal.text,
        source: signal.source,
      });
      yield* ownerOf(identity).interrupt(identity.executionId);
    });

  const validatePathPayload = (
    source: Identity,
    payload: Payload['~type.make.in'],
    mode: 'branch' | 'fork',
  ) =>
    Effect.gen(function* () {
      const validated = yield* workflow.payloadSchema.makeEffect(payload);
      const target = yield* LogVocabulary.ConversationId.makeEffect(
        validated.conversationId,
      );
      if (mode === 'branch' && target !== source.conversationId) {
        return yield* new PathError({
          message:
            `Branch payload targets conversation ${target}; expected source ` +
            source.conversationId,
        });
      }
      if (mode === 'fork' && target === source.conversationId) {
        return yield* new PathError({
          message: `Fork payload must target a new conversation, not ${target}`,
        });
      }
      const encodedPayload = yield* Schema.encodeUnknownEffect(
        workflow.payloadSchema,
      )(validated);
      const targetExecutionId = yield* workflow.executionId(validated);
      return { encodedPayload, target, targetExecutionId };
    });

  type PathInput = (typeof workflow.payloadSchema)['~type.make.in'];
  type BoundPathFailure = PathFailure<Error>;
  type BoundPathRequires = PathRequirements<
    typeof workflow.payloadSchema,
    Error
  >;

  function branchFrom(
    source: Identity,
    at: LogOffset.Offset,
    payload: PathInput,
    executeOptions: { readonly discard: true },
  ): Effect.Effect<Identity, BoundPathFailure, BoundPathRequires>;
  function branchFrom(
    source: Identity,
    at: LogOffset.Offset,
    payload: PathInput,
    executeOptions?: { readonly discard?: false },
  ): Effect.Effect<Agent.Result, BoundPathFailure, BoundPathRequires>;
  function branchFrom(
    source: Identity,
    at: LogOffset.Offset,
    payload: PathInput,
    executeOptions?: { readonly discard?: boolean },
  ): Effect.Effect<
    Identity | Agent.Result,
    BoundPathFailure,
    BoundPathRequires
  > {
    return Effect.gen(function* () {
      const validated = yield* validatePathPayload(source, payload, 'branch');
      yield* ownerOf(source).interrupt(source.executionId);
      const pathPayload = {
        mode: 'branch' as const,
        sourceConversationId: source.conversationId,
        sourceExecutionId: source.executionId,
        at,
        targetExecutionId: validated.targetExecutionId,
        encodedPayload: validated.encodedPayload,
      };
      if (executeOptions?.discard === true) {
        const executionId = yield* pathWorkflow.execute(pathPayload, {
          discard: true,
        });
        return {
          [IdentityTypeId]: IdentityTypeId,
          workflow: 'path',
          executionId,
          conversationId: validated.target,
        } satisfies Identity;
      }
      return yield* pathWorkflow.execute(pathPayload);
    });
  }

  function forkFrom(
    source: Identity,
    at: LogOffset.Offset,
    payload: PathInput,
    executeOptions: { readonly discard: true },
  ): Effect.Effect<Identity, BoundPathFailure, BoundPathRequires>;
  function forkFrom(
    source: Identity,
    at: LogOffset.Offset,
    payload: PathInput,
    executeOptions?: { readonly discard?: false },
  ): Effect.Effect<Agent.Result, BoundPathFailure, BoundPathRequires>;
  function forkFrom(
    source: Identity,
    at: LogOffset.Offset,
    payload: PathInput,
    executeOptions?: { readonly discard?: boolean },
  ): Effect.Effect<
    Identity | Agent.Result,
    BoundPathFailure,
    BoundPathRequires
  > {
    return Effect.gen(function* () {
      const validated = yield* validatePathPayload(source, payload, 'fork');
      const pathPayload = {
        mode: 'fork' as const,
        sourceConversationId: source.conversationId,
        sourceExecutionId: source.executionId,
        at,
        targetExecutionId: validated.targetExecutionId,
        encodedPayload: validated.encodedPayload,
      };
      if (executeOptions?.discard === true) {
        const executionId = yield* pathWorkflow.execute(pathPayload, {
          discard: true,
        });
        return {
          [IdentityTypeId]: IdentityTypeId,
          workflow: 'path',
          executionId,
          conversationId: validated.target,
        } satisfies Identity;
      }
      return yield* pathWorkflow.execute(pathPayload);
    });
  }

  return {
    workflow,
    layer,
    identify,
    cancel,
    branchFrom,
    forkFrom,
  } satisfies BoundWorkflow<A, Tag, Payload, Error>;
};

/** Bind the ordinary plain-text request to Effect's durable workflow runtime. */
export const make = <
  A extends Agent.Any,
  const Tag extends string,
  const Payload extends Workflow.AnyStructSchema & Schema.Schema<Request>,
  Error extends Schema.Top,
>(
  agent: A,
  options: Options<Tag, Payload, Error, WorkflowAgentError<A>>,
): BoundWorkflow<A, Tag, Payload, Error> =>
  bind(
    agent,
    options,
    options.input ?? ((payload: Payload['Type']) => payload.input),
  );

/** Bind schema-typed application input through an explicit prompt projection. */
export const makeWithInput = <
  A extends Agent.Any,
  const Tag extends string,
  const Payload extends Workflow.AnyStructSchema &
    Schema.Schema<Request<unknown>>,
  Error extends Schema.Top,
>(
  agent: A,
  options: InputOptions<Tag, Payload, Error, WorkflowAgentError<A>>,
): BoundWorkflow<A, Tag, Payload, Error> => bind(agent, options, options.input);

/** @since 0.1.0 */
export * as AgentWorkflow from './workflow.js';
