import type { Layer, Schedule } from 'effect';
import { Effect, Schema } from 'effect';
import type { Prompt } from 'effect/unstable/ai';
import {
  Activity,
  Workflow,
  type WorkflowEngine,
} from 'effect/unstable/workflow';

import { Agent } from './agent.js';
import { AgentSignals } from './signal.js';
import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';

/** Durable request fields shared by execution, cancellation, and resumption. */
export const RequestFields = {
  conversationId: Schema.String,
  input: Schema.String,
} as const;

/** Define a workflow request schema with Vesper's required identity fields. */
export const request = <const Fields extends Schema.Struct.Fields>(
  fields: Fields,
) =>
  Schema.Struct({
    ...RequestFields,
    ...fields,
  });

export interface Request {
  readonly conversationId: string;
  readonly input: string;
}

type WorkflowPayload<Payload extends Workflow.AnyStructSchema> =
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload;

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
  readonly ids: (payload: Payload['~type.make.in']) => Effect.Effect<{
    readonly executionId: string;
    readonly conversationId: LogVocabulary.ConversationId;
  }>;
  /** Persist Vesper cancellation intent, then terminally interrupt the workflow. */
  readonly cancel: (
    payload: Payload['~type.make.in'],
    signal: CancelSignal,
  ) => Effect.Effect<
    void,
    LogStore.LogStoreError,
    WorkflowEngine.WorkflowEngine | LogStore.Service
  >;
}

export interface CancelSignal {
  readonly text: string;
  readonly source: string;
}

/** Options for projecting an application workflow payload into an agent run. */
export interface Options<
  Tag extends string,
  Payload extends Workflow.AnyStructSchema & Schema.Schema<Request>,
  Error extends Schema.Top,
  AgentError,
> {
  readonly tag: Tag;
  readonly payload: Payload;
  readonly idempotencyKey: (payload: Payload['Type']) => string;
  readonly input?: (payload: Payload['Type']) => Prompt.RawInput;
  readonly error: Error;
  readonly mapError: (error: AgentError) => Error['Type'];
  readonly suspendedRetrySchedule?: Schedule.Schedule<any, unknown> | undefined;
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
    | Schedule.Schedule<any, import('effect').Cause.Cause<unknown>>
    | undefined;
}

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
export const make = <
  A extends Agent.Any,
  const Tag extends string,
  const Payload extends Workflow.AnyStructSchema & Schema.Schema<Request>,
  Error extends Schema.Top,
>(
  agent: A,
  options: Options<Tag, Payload, Error, Agent.Error<A>>,
): Binding<
  Tag,
  WorkflowPayload<Payload>,
  Error,
  | WorkflowEngine.WorkflowEngine
  | Agent.Requires<A>
  | LogStore.Service
  | WorkflowPayload<Payload>['DecodingServices' | 'EncodingServices']
  | (typeof Agent.Result)['EncodingServices']
  | Error['EncodingServices']
> => {
  const workflow = Workflow.make(options.tag, {
    payload: options.payload,
    idempotencyKey: options.idempotencyKey,
    success: Agent.Result,
    error: options.error,
    ...(options.suspendedRetrySchedule === undefined
      ? {}
      : { suspendedRetrySchedule: options.suspendedRetrySchedule }),
  });

  const layer = workflow.toLayer((payload) =>
    agent
      .resume(payload.conversationId, options.input?.(payload) ?? payload.input)
      .pipe(Effect.mapError(options.mapError)),
  );

  const ids = (payload: (typeof workflow.payloadSchema)['~type.make.in']) =>
    Effect.map(
      workflow.payloadSchema.makeEffect(payload).pipe(Effect.orDie),
      (validated) => ({
        conversationId: LogVocabulary.ConversationId.make(
          validated.conversationId,
        ),
      }),
    ).pipe(
      Effect.flatMap(({ conversationId }) =>
        Effect.map(workflow.executionId(payload), (executionId) => ({
          executionId,
          conversationId,
        })),
      ),
    );

  const cancel = (
    payload: (typeof workflow.payloadSchema)['~type.make.in'],
    signal: CancelSignal,
  ) =>
    Effect.gen(function* () {
      const identity = yield* ids(payload);
      yield* AgentSignals.send(identity.conversationId, {
        kind: 'cancel',
        text: signal.text,
        source: signal.source,
      });
      yield* workflow.interrupt(identity.executionId);
    });

  return { workflow, layer, ids, cancel } satisfies Binding<
    Tag,
    WorkflowPayload<Payload>,
    Error,
    | WorkflowEngine.WorkflowEngine
    | Agent.Requires<A>
    | LogStore.Service
    | WorkflowPayload<Payload>['DecodingServices' | 'EncodingServices']
    | (typeof Agent.Result)['EncodingServices']
    | Error['EncodingServices']
  >;
};

/** @since 0.1.0 */
export * as AgentWorkflow from './workflow.js';
