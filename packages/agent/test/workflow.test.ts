import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { Context, Effect, Layer, Schema, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';
import { Workflow, WorkflowEngine } from 'effect/unstable/workflow';
import type { WorkflowInstance } from 'effect/unstable/workflow/WorkflowEngine';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Interception } from '../src/interception.js';
import { AgentWorkflow } from '../src/workflow.js';

type LayerR<T> = T extends Layer.Layer<infer _A, infer _E, infer R> ? R : never;
type EffR<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;
type Has<M, U> = [M] extends [U] ? 'yes' : 'no';
type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';

class AgentDependency extends Context.Service<AgentDependency, {}>()(
  'workflow-test/AgentDependency',
) {}

const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 2, uncached: 2, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

const ModelLive = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([finish]),
    streamText: () =>
      Stream.fromIterable<Response.StreamPartEncoded>([
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: 'hello' },
        { type: 'text-end', id: 'answer' },
        finish,
      ]),
  }),
);

class WorkflowFailure extends Schema.TaggedError<WorkflowFailure>()(
  'WorkflowFailure',
  { message: Schema.String },
) {}

const durableLookup = AgentWorkflow.step({
  name: 'lookup-order',
  key: (orderId: string) => orderId,
  success: Schema.String,
  error: WorkflowFailure,
  execute: (orderId: string) => Effect.succeed(`order:${orderId}`),
});

const _stepRequiresWorkflow: Has<
  WorkflowInstance | WorkflowEngine.WorkflowEngine,
  EffR<ReturnType<typeof durableLookup>>
> = 'yes';

const agent = Agent.make({
  name: 'workflow-test',
  revision: '1',
  instructions: 'Answer briefly.',
  toolkit: Toolkit.make(),
});

const WorkflowRequest = AgentWorkflow.request({
  submissionId: Schema.String,
});

const RequiringWorkflowRequest = AgentWorkflow.request({ id: Schema.String });

const requiringAgent = Agent.make({
  name: 'requiring-workflow-test',
  revision: '1',
  instructions: 'Answer briefly.',
  toolkit: Toolkit.make(),
}).intercepting({
  beforeTurn: () => Effect.as(AgentDependency, Interception.proceed),
});

const binding = AgentWorkflow.make(agent, {
  tag: 'WorkflowTest',
  payload: WorkflowRequest,
  idempotencyKey: ({ submissionId }) => submissionId,
  error: WorkflowFailure,
  mapError: (error) => new WorkflowFailure({ message: String(error) }),
});

const requiringBinding = AgentWorkflow.make(requiringAgent, {
  tag: 'RequiringWorkflowTest',
  payload: RequiringWorkflowRequest,
  idempotencyKey: ({ id }) => id,
  error: WorkflowFailure,
  mapError: (error) => new WorkflowFailure({ message: String(error) }),
});

const _layerNotAny: IsAny<LayerR<typeof requiringBinding.layer>> = 'not-any';
const _keepsAgentRequirements: Has<
  AgentDependency,
  LayerR<typeof requiringBinding.layer>
> = 'yes';
const _requiresWorkflowEngine: Has<
  WorkflowEngine.WorkflowEngine,
  LayerR<typeof requiringBinding.layer>
> = 'yes';

const AppLive = binding.layer.pipe(
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provide(ModelLive),
  Layer.provide(LogStoreMemory.layer),
);

describe('AgentWorkflow', () => {
  it.effect(
    'derives workflow and conversation identities from one payload',
    () =>
      Effect.gen(function* () {
        const payload = {
          submissionId: 'identity-submission',
          conversationId: 'identity-conversation',
          input: 'hello',
        };
        const ids = yield* binding.identify(payload);
        const executionId = yield* binding.workflow.executionId(payload);

        expect(ids).toMatchObject({
          executionId,
          conversationId: 'identity-conversation',
        });
      }),
  );

  it.effect('runs a recorded agent through Effect Workflow', () =>
    Effect.gen(function* () {
      const result = yield* binding.workflow
        .execute({
          submissionId: 'submission-1',
          conversationId: 'conversation-1',
          input: 'say hello',
        })
        .pipe(Effect.provide(AppLive));

      expect(result.outcome).toBe('success');
      expect(result.text).toBe('hello');
      expect(result.steps).toBe(1);
    }),
  );

  it.effect(
    'uses the workflow idempotency key to return the recorded result',
    () =>
      Effect.gen(function* () {
        let calls = 0;
        const CountingModel = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([finish]),
            streamText: () => {
              calls += 1;
              return Stream.fromIterable<Response.StreamPartEncoded>([
                { type: 'text-start', id: 'answer' },
                { type: 'text-delta', id: 'answer', delta: `call:${calls}` },
                { type: 'text-end', id: 'answer' },
                finish,
              ]);
            },
          }),
        );
        const Live = binding.layer.pipe(
          Layer.provideMerge(WorkflowEngine.layerMemory),
          Layer.provide(CountingModel),
          Layer.provide(LogStoreMemory.layer),
        );
        const payload = {
          submissionId: 'same-submission',
          conversationId: 'same-conversation',
          input: 'once',
        };

        const results = yield* Effect.gen(function* () {
          const first = yield* binding.workflow.execute(payload);
          const second = yield* binding.workflow.execute(payload);
          return [first, second] as const;
        }).pipe(Effect.provide(Live));

        expect(results[0].text).toBe('call:1');
        expect(results[1].text).toBe('call:1');
        expect(calls).toBe(1);
      }),
  );

  it.effect('replays completed steps by stable name', () =>
    Effect.gen(function* () {
      let executions = 0;
      const once = AgentWorkflow.step({
        name: 'replay-once',
        key: (amount: number) => String(amount),
        success: Schema.Number,
        error: WorkflowFailure,
        execute: (amount: number) =>
          Effect.sync(() => {
            executions += 1;
            return amount * 2;
          }),
      });
      const StepWorkflow = Workflow.make('StepWorkflow', {
        payload: { id: Schema.String },
        idempotencyKey: ({ id }) => id,
        success: Schema.Tuple([Schema.Number, Schema.Number]),
        error: WorkflowFailure,
      });
      const StepLive = StepWorkflow.toLayer(() =>
        Effect.gen(function* () {
          const first = yield* once(21);
          const second = yield* once(21);
          return [first, second] as const;
        }),
      ).pipe(Layer.provideMerge(WorkflowEngine.layerMemory));

      const result = yield* StepWorkflow.execute({ id: 'step-execution' }).pipe(
        Effect.provide(StepLive),
      );

      expect(result).toEqual([42, 42]);
      expect(executions).toBe(1);
    }),
  );

  it.effect('isolates repeated step calls by their input-derived key', () =>
    Effect.gen(function* () {
      let executions = 0;
      const keyedStep = AgentWorkflow.step({
        name: 'keyed-step',
        key: ({ orderId }: { orderId: string }) => orderId,
        success: Schema.String,
        error: WorkflowFailure,
        execute: ({ orderId }) =>
          Effect.sync(() => {
            executions += 1;
            return `result:${orderId}`;
          }),
      });
      const KeyedWorkflow = Workflow.make('KeyedStepWorkflow', {
        payload: { id: Schema.String },
        idempotencyKey: ({ id }) => id,
        success: Schema.Array(Schema.String),
        error: WorkflowFailure,
      });
      const KeyedLive = KeyedWorkflow.toLayer(() =>
        Effect.all([
          keyedStep({ orderId: 'a/b' }),
          keyedStep({ orderId: 'b' }),
          keyedStep({ orderId: 'a/b' }),
        ]),
      ).pipe(Layer.provideMerge(WorkflowEngine.layerMemory));

      const result = yield* KeyedWorkflow.execute({
        id: 'keyed-execution',
      }).pipe(Effect.provide(KeyedLive));

      expect(result).toEqual(['result:a/b', 'result:b', 'result:a/b']);
      expect(executions).toBe(2);
    }),
  );

  it('rejects empty step keys before executing', () => {
    const invalid = AgentWorkflow.step({
      name: 'invalid-step',
      key: () => '',
      success: Schema.Void,
      error: WorkflowFailure,
      execute: () => Effect.void,
    });

    expect(() => invalid(undefined)).toThrow(
      'AgentWorkflow step "invalid-step" produced an empty key',
    );
  });

  it.effect(
    'derives stable external idempotency keys from workflow execution and step name',
    () =>
      Effect.gen(function* () {
        const KeyWorkflow = Workflow.make('KeyWorkflow', {
          payload: { id: Schema.String },
          idempotencyKey: ({ id }) => id,
          success: Schema.String,
          error: WorkflowFailure,
        });
        const keyed = AgentWorkflow.step({
          name: 'keyed-effect',
          key: () => 'payment',
          success: Schema.String,
          error: WorkflowFailure,
          execute: () => AgentWorkflow.idempotencyKey('payment'),
        });
        const KeyLive = KeyWorkflow.toLayer(() => keyed(undefined)).pipe(
          Layer.provideMerge(WorkflowEngine.layerMemory),
        );

        const [first, second] = yield* Effect.gen(function* () {
          return [
            yield* KeyWorkflow.execute({ id: 'same' }),
            yield* KeyWorkflow.execute({ id: 'same' }),
          ] as const;
        }).pipe(Effect.provide(KeyLive));

        expect(first).toBe(second);
        expect(first).toHaveLength(32);
      }),
  );
});
