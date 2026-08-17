import type { Layer, Schedule, Schema } from 'effect';
import { Effect } from 'effect';
import type { Prompt } from 'effect/unstable/ai';
import {
  Activity,
  Workflow,
  type WorkflowEngine,
} from 'effect/unstable/workflow';

import { Agent } from './agent.js';
import type { LogStore } from '@sunfall/vesper-log/log-store';

type PayloadType<
  Payload extends Schema.Struct.Fields | Workflow.AnyStructSchema,
> = (Payload extends Schema.Struct.Fields
  ? Schema.Struct<Payload>
  : Payload)['Type'];

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
}

/** Options for projecting an application workflow payload into an agent run. */
export interface Options<
  Tag extends string,
  Payload extends Schema.Struct.Fields | Workflow.AnyStructSchema,
  Error extends Schema.Top,
  AgentError,
> {
  readonly tag: Tag;
  readonly payload: Payload;
  readonly idempotencyKey: (payload: PayloadType<Payload>) => string;
  readonly conversationId: (payload: PayloadType<Payload>) => string;
  readonly input: (payload: PayloadType<Payload>) => Prompt.RawInput;
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
  const Payload extends Schema.Struct.Fields | Workflow.AnyStructSchema,
  Error extends Schema.Top,
>(
  agent: A,
  options: Options<Tag, Payload, Error, Agent.Error<A>>,
): Binding<
  Tag,
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload,
  Error,
  | WorkflowEngine.WorkflowEngine
  | Agent.Requires<A>
  | LogStore.Service
  | (Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload)[
      | 'DecodingServices'
      | 'EncodingServices']
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
      .resume(options.conversationId(payload), options.input(payload))
      .pipe(Effect.mapError(options.mapError)),
  );

  return { workflow, layer } as Binding<
    Tag,
    Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload,
    Error,
    | WorkflowEngine.WorkflowEngine
    | Agent.Requires<A>
    | LogStore.Service
    | (Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload)[
        | 'DecodingServices'
        | 'EncodingServices']
    | (typeof Agent.Result)['EncodingServices']
    | Error['EncodingServices']
  >;
};

/** @since 0.1.0 */
export * as AgentWorkflow from './workflow.js';
