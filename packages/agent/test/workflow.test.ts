import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import {
  Context,
  Effect,
  Exit,
  Fiber,
  Layer,
  Match,
  Option,
  Ref,
  Schema,
  SchemaIssue,
  SchemaTransformation,
  Stream,
} from 'effect';
import {
  type AiError,
  LanguageModel,
  type Prompt,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import {
  DurableDeferred,
  Workflow,
  WorkflowEngine,
} from 'effect/unstable/workflow';
import type { WorkflowInstance } from 'effect/unstable/workflow/WorkflowEngine';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import * as AgentLog from '../src/log.js';
import { AgentWorkflow } from '../src/workflow.js';

type LayerR<T> = T extends Layer.Layer<infer _A, infer _E, infer R> ? R : never;
type EffA<T> = T extends Effect.Effect<infer A, infer _E, infer _R> ? A : never;
type EffE<T> = T extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;
type EffR<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;
type Has<M, U> = [M] extends [U] ? 'yes' : 'no';
type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';
type Exact<A, B> = [A, B] extends [B, A] ? true : false;

class AgentDependency extends Context.Service<
  AgentDependency,
  Record<string, never>
>()('workflow-test/AgentDependency') {}

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

class WorkflowFailure extends Schema.TaggedError<WorkflowFailure>(
  'workflow-test/WorkflowFailure',
)('WorkflowFailure', { message: Schema.String }) {}

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

const ProofRequest = Schema.Struct({ requestId: Schema.String });
const ProofSuccess = Schema.Struct({ approved: Schema.Boolean });
const ProofFailure = Schema.Struct({ reason: Schema.String });
const proofWait = AgentWorkflow.wait({
  name: 'proof-wait',
  key: ({ requestId }) => requestId,
  request: ProofRequest,
  success: ProofSuccess,
  error: ProofFailure,
});
const proofPendingEffect = proofWait.awaitPending(
  Conversation.make(agent, 'proof-conversation'),
  'proof-request',
);
type ProofPending = EffA<typeof proofPendingEffect>;
type ProofComplete = ReturnType<ProofPending['complete']>;
type ProofFail = ReturnType<ProofPending['fail']>;

const _pendingRequestExact: Exact<
  ProofPending['request'],
  (typeof ProofRequest)['Type']
> = true;
const _pendingCompleteExact: Exact<
  Parameters<ProofPending['complete']>[0],
  (typeof ProofSuccess)['Type']
> = true;
const _pendingFailExact: Exact<
  Parameters<ProofPending['fail']>[0],
  (typeof ProofFailure)['Type']
> = true;
const _pendingNotAny: IsAny<ProofPending> = 'not-any';
const _awaitRequiresLog: Has<
  LogStore.Service,
  EffR<typeof proofPendingEffect>
> = 'yes';
const _awaitDoesNotRequireEngine: Has<
  WorkflowEngine.WorkflowEngine,
  EffR<typeof proofPendingEffect>
> = 'no';
const _awaitRequirementsExact: Exact<
  EffR<typeof proofPendingEffect>,
  LogStore.Service
> = true;
const _awaitErrorsExact: Exact<
  EffE<typeof proofPendingEffect>,
  LogStore.LogStoreError | Schema.SchemaError | AgentWorkflow.WaitStateError
> = true;
const _completeRequiresEngine: Has<
  WorkflowEngine.WorkflowEngine,
  EffR<ProofComplete>
> = 'yes';
const _failRequiresEngine: Has<
  WorkflowEngine.WorkflowEngine,
  EffR<ProofFail>
> = 'yes';
const _pendingRejectsWrongValues = (pending: ProofPending) => {
  // @ts-expect-error complete accepts only the success schema's decoded type
  void pending.complete({ approved: 'yes' });
  // @ts-expect-error fail accepts only the error schema's decoded type
  void pending.fail({ reason: 404 });
};

const _waitEncodingErrorsAreTyped: Has<
  AiError.AiError,
  EffE<ReturnType<typeof proofWait>>
> = 'yes';

const WorkflowRequest = AgentWorkflow.request({
  submissionId: Schema.String,
});

const MultiplayerInput = Schema.TaggedUnion({
  ParticipantMessage: {
    participantId: Schema.String,
    text: Schema.String,
  },
  ModeratorNotice: {
    moderatorId: Schema.String,
    text: Schema.String,
  },
});

const renderMultiplayerInput = Match.type<typeof MultiplayerInput.Type>().pipe(
  Match.tagsExhaustive({
    ParticipantMessage: ({ participantId, text }): Prompt.RawInput => [
      { role: 'user', content: `[${participantId}] ${text}` },
    ],
    ModeratorNotice: ({ moderatorId, text }): Prompt.RawInput => [
      { role: 'user', content: `[moderator:${moderatorId}] ${text}` },
    ],
  }),
);

const MultiplayerRequest = AgentWorkflow.request(
  { submissionId: Schema.String },
  MultiplayerInput,
);

const multiplayerBinding = AgentWorkflow.makeWithInput(agent, {
  tag: 'MultiplayerWorkflowTest',
  payload: MultiplayerRequest,
  idempotencyKey: ({ submissionId }) => submissionId,
  input: ({ input }) => renderMultiplayerInput(input),
  error: WorkflowFailure,
  mapError: (error) => new WorkflowFailure({ message: String(error) }),
});

const _typedInputRequiresProjection = () => {
  // @ts-expect-error non-string request input requires an explicit projection
  AgentWorkflow.makeWithInput(agent, {
    tag: 'MissingInputProjectionTest',
    payload: MultiplayerRequest,
    idempotencyKey: ({ submissionId }) => submissionId,
    error: WorkflowFailure,
    mapError: (error) => new WorkflowFailure({ message: String(error) }),
  });
};

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
  mapError: (error) => {
    const _exactError: Exact<
      typeof error,
      Conversation.Error<typeof agent>
    > = true;
    return new WorkflowFailure({ message: String(error) });
  },
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

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

const AppLive = binding.layer.pipe(
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provide(ModelLive),
  Layer.provide(testLogLayer),
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

  it.effect('keeps invalid identity input recoverable', () =>
    Effect.gen(function* () {
      const result = yield* binding
        .identify({
          submissionId: 'invalid-identity',
          conversationId: '',
          input: 'hello',
        })
        .pipe(Effect.result);

      expect(result._tag).toBe('Failure');
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

  it.effect('projects schema-typed application input into Effect Prompt', () =>
    Effect.gen(function* () {
      let observed: Prompt.Prompt | undefined;
      const ObservingModel = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([finish]),
          streamText: (options) => {
            observed = options.prompt;
            return Stream.fromIterable<Response.StreamPartEncoded>([
              { type: 'text-start', id: 'answer' },
              { type: 'text-delta', id: 'answer', delta: 'hello' },
              { type: 'text-end', id: 'answer' },
              finish,
            ]);
          },
        }),
      );
      const Live = multiplayerBinding.layer.pipe(
        Layer.provideMerge(WorkflowEngine.layerMemory),
        Layer.provide(ObservingModel),
        Layer.provide(testLogLayer),
      );

      const result = yield* multiplayerBinding.workflow
        .execute({
          submissionId: 'multiplayer-submission',
          conversationId: 'multiplayer-conversation',
          input: {
            _tag: 'ParticipantMessage',
            participantId: 'alice',
            text: 'hello from the room',
          },
        })
        .pipe(Effect.provide(Live));

      expect(result.text).toBe('hello');
      expect(JSON.stringify(observed)).toContain('[alice] hello from the room');
    }),
  );

  it.effect('maps storage failures through the declared workflow error', () =>
    Effect.gen(function* () {
      let mapped: unknown;
      const storageBinding = AgentWorkflow.make(agent, {
        tag: 'StorageFailureWorkflowTest',
        payload: WorkflowRequest,
        idempotencyKey: ({ submissionId }) => submissionId,
        error: WorkflowFailure,
        mapError: (error) => {
          mapped = error;
          return new WorkflowFailure({
            message:
              error instanceof LogStore.LogStoreError
                ? error.reason
                : String(error),
          });
        },
      });
      const FailingStore = Layer.effect(
        LogStore.Service,
        Effect.map(LogStore.Service, (store) =>
          LogStore.Service.of({
            ...store,
            meta: (path) =>
              Effect.fail(
                new LogStore.LogStoreError({
                  path,
                  operation: 'meta',
                  reason: 'storage',
                  detail: 'offline',
                }),
              ),
          }),
        ),
      ).pipe(Layer.provide(testLogLayer));
      const Live = storageBinding.layer.pipe(
        Layer.provideMerge(WorkflowEngine.layerMemory),
        Layer.provide(ModelLive),
        Layer.provide(FailingStore),
      );

      const error = yield* storageBinding.workflow
        .execute({
          submissionId: 'storage-submission',
          conversationId: 'storage-conversation',
          input: 'hello',
        })
        .pipe(Effect.provide(Live), Effect.flip);

      expect(error).toMatchObject({ message: 'storage' });
      expect(mapped).toBeInstanceOf(LogStore.LogStoreError);
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
                {
                  type: 'text-delta',
                  id: 'answer',
                  delta: `call:${String(calls)}`,
                },
                { type: 'text-end', id: 'answer' },
                finish,
              ]);
            },
          }),
        );
        const Live = binding.layer.pipe(
          Layer.provideMerge(WorkflowEngine.layerMemory),
          Layer.provide(CountingModel),
          Layer.provide(testLogLayer),
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
        success: Schema.Finite,
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
        success: Schema.Tuple([Schema.Finite, Schema.Finite]),
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

  it('rejects empty wait keys before creating a durable effect', () => {
    const invalid = AgentWorkflow.wait({
      name: 'invalid-wait',
      key: () => '',
      request: Schema.Void,
      success: Schema.Void,
      error: Schema.Never,
    });

    expect(() => invalid(undefined)).toThrow(
      'AgentWorkflow wait "invalid-wait" produced an empty key',
    );
  });

  it.effect(
    'keeps wait result encoding failures in the typed error channel',
    () =>
      Effect.gen(function* () {
        const EncodeFailure = Schema.String.pipe(
          Schema.decodeTo(
            Schema.Finite,
            SchemaTransformation.transformOrFail<number, string>({
              decode: (value) => Effect.succeed(Number(value)),
              encode: () =>
                Effect.fail(
                  new SchemaIssue.InvalidValue({ message: 'encode failed' }),
                ),
            }),
          ),
        );
        const approval = AgentWorkflow.wait({
          name: 'encoding-wait',
          key: () => 'request',
          request: Schema.Void,
          success: EncodeFailure,
          error: Schema.Never,
        });
        const token = new DurableDeferred.TokenParsed({
          workflowName: 'EncodingWorkflow',
          executionId: 'encoding-execution',
          deferredName: 'encoding-wait/request',
        }).asToken;

        const result = yield* Effect.exit(approval.complete(token, 1));
        if (Exit.isSuccess(result)) {
          return expect.unreachable('invalid result encoding must fail');
        }
        const error = Exit.findErrorOption(result);
        expect(Option.getOrThrow(error)).toBeInstanceOf(Schema.SchemaError);
      }).pipe(Effect.provide(WorkflowEngine.layerMemory)),
  );

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

  it.effect(
    'durably yields a tool request and resumes the same logical handler',
    () => {
      const review = AgentWorkflow.wait({
        name: 'review-order',
        key: ({ orderId }: { readonly orderId: string }) => orderId,
        request: Schema.Struct({
          orderId: Schema.String,
          prepared: Schema.String,
        }),
        success: Schema.Struct({
          approved: Schema.Boolean,
          actor: Schema.String,
        }),
        error: WorkflowFailure,
      });
      const unrelatedReview = AgentWorkflow.wait({
        name: 'review-unrelated',
        key: ({ orderId }: { readonly orderId: string }) => orderId,
        request: Schema.Struct({ orderId: Schema.String }),
        success: Schema.Struct({ approved: Schema.Boolean }),
        error: Schema.Never,
      });
      let preparations = 0;
      let handlerEntries = 0;
      const prepare = AgentWorkflow.step({
        name: 'prepare-order',
        key: (orderId: string) => orderId,
        success: Schema.String,
        error: WorkflowFailure,
        execute: (orderId: string) =>
          Effect.sync(() => {
            preparations += 1;
            return `prepared:${orderId}`;
          }),
      });
      const submit = AgentWorkflow.durable(
        Tool.make('submit_order', {
          description: 'submit an order after review',
          parameters: Schema.Struct({ orderId: Schema.String }),
          success: Schema.Struct({ status: Schema.String }),
          failure: WorkflowFailure,
          failureMode: 'return',
        }),
      );
      const waitingAgent = Agent.make({
        name: 'waiting-agent',
        revision: '1',
        instructions: 'Submit the order.',
        toolkit: Toolkit.make(submit),
      }).withHandlers({
        submit_order: ({ orderId }) =>
          Effect.gen(function* () {
            handlerEntries += 1;
            const prepared = yield* prepare(orderId);
            const decision = yield* review({ orderId, prepared });
            return {
              status: decision.approved
                ? `submitted-by:${decision.actor}`
                : 'denied',
            };
          }),
      });
      const WaitingRequest = AgentWorkflow.request({
        submissionId: Schema.String,
      });
      const waitingBinding = AgentWorkflow.make(waitingAgent, {
        tag: 'WaitingAgentWorkflow',
        payload: WaitingRequest,
        idempotencyKey: ({ submissionId }) => submissionId,
        error: WorkflowFailure,
        mapError: (error) => new WorkflowFailure({ message: String(error) }),
      });
      const WaitingModel = Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([finish]),
            streamText: () =>
              Stream.unwrap(
                Effect.map(
                  Ref.getAndUpdate(calls, (n) => n + 1),
                  (call) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      call === 0
                        ? [
                            {
                              type: 'tool-call',
                              id: 'submit-1',
                              name: 'submit_order',
                              params: { orderId: 'order-42' },
                            },
                            { ...finish, reason: 'tool-calls' },
                          ]
                        : [
                            { type: 'text-start', id: 'answer' },
                            {
                              type: 'text-delta',
                              id: 'answer',
                              delta: 'submitted',
                            },
                            { type: 'text-end', id: 'answer' },
                            finish,
                          ],
                    ),
                ),
              ),
          });
        }),
      );
      const WaitingLive = waitingBinding.layer.pipe(
        Layer.provideMerge(WorkflowEngine.layerMemory),
        Layer.provide(WaitingModel),
        Layer.provideMerge(testLogLayer),
      );
      const payload = {
        submissionId: 'submission-wait',
        conversationId: 'conversation-wait',
        input: 'submit order 42',
      };

      return Effect.gen(function* () {
        const sourceConversation = Conversation.make(
          waitingAgent,
          payload.conversationId,
        );
        // External consumers are allowed to install the keyed waiter before
        // the discarded workflow has created its conversation log.
        const awaitingSource = yield* review
          .awaitPending(sourceConversation, 'order-42')
          .pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        yield* waitingBinding.workflow.execute(payload, { discard: true });

        const requestEnvelope: Option.Option<Conversation.WaitEnvelope> =
          yield* Conversation.make(waitingAgent, payload.conversationId)
            .followWaits()
            .pipe(
              Stream.filter(({ record }) => record._tag === 'ToolSuspended'),
              Stream.runHead,
            );
        if (Option.isNone(requestEnvelope)) {
          return yield* Effect.die('missing ToolSuspended record');
        }
        expect(preparations).toBe(1);
        expect(handlerEntries).toBe(1);
        const request: Conversation.WaitRecord = requestEnvelope.value.record;
        expect(request).toMatchObject({
          _tag: 'ToolSuspended',
          name: 'submit_order',
          wait: 'review-order',
          request: {
            orderId: 'order-42',
            prepared: 'prepared:order-42',
          },
        });
        if (request._tag !== 'ToolSuspended') {
          return yield* Effect.die('missing ToolSuspended record');
        }

        const pendingSource = yield* Fiber.join(awaitingSource);
        expect(pendingSource).toMatchObject({
          conversationId: payload.conversationId,
          offset: requestEnvelope.value.offset,
          toolCallId: request.id,
          toolName: 'submit_order',
          key: 'order-42',
          token: request.token,
          request: {
            orderId: 'order-42',
            prepared: 'prepared:order-42',
          },
        });

        const wrongWait = yield* Effect.exit(
          unrelatedReview.complete(request.token, { approved: true }),
        );
        if (Exit.isFailure(wrongWait)) {
          const error = Exit.findErrorOption(wrongWait);
          expect(Option.isSome(error) ? error.value._tag : undefined).toBe(
            'WaitTokenError',
          );
        } else {
          expect.unreachable('a token must not complete a different wait');
        }

        const sourceIdentity = yield* waitingBinding.identify(payload);
        const forkPayload = {
          submissionId: 'submission-wait-fork',
          conversationId: 'conversation-wait-fork',
          input: 'submit order 42 in the fork',
        };
        const forkIdentity = yield* waitingBinding.forkFrom(
          sourceIdentity,
          requestEnvelope.value.offset,
          forkPayload,
          { discard: true },
        );
        expect(forkIdentity).toMatchObject({
          workflow: 'path',
          conversationId: forkPayload.conversationId,
        });
        const forkRequestEnvelope = yield* Conversation.make(
          waitingAgent,
          forkPayload.conversationId,
        )
          .followWaits()
          .pipe(
            Stream.filter(
              ({ record }) =>
                record._tag === 'ToolSuspended' &&
                record.token !== request.token,
            ),
            Stream.runHead,
          );
        if (Option.isNone(forkRequestEnvelope)) {
          return yield* Effect.die('missing restarted ToolSuspended record');
        }
        const forkRequest = forkRequestEnvelope.value.record;
        if (forkRequest._tag !== 'ToolSuspended') {
          return yield* Effect.die('missing restarted ToolSuspended record');
        }
        expect(forkRequest.token).not.toBe(request.token);
        expect(preparations).toBe(2);
        expect(handlerEntries).toBe(2);

        const pendingFork = yield* review.awaitPending(
          Conversation.make(waitingAgent, forkPayload.conversationId),
          'order-42',
        );
        expect(pendingFork).toMatchObject({
          conversationId: forkPayload.conversationId,
          key: 'order-42',
          token: forkRequest.token,
          request: {
            orderId: 'order-42',
            prepared: 'prepared:order-42',
          },
        });

        const sourceStillPending = yield* review.awaitPending(
          sourceConversation,
          'order-42',
        );
        expect(sourceStillPending.token).toBe(request.token);

        yield* pendingFork.fail(
          new WorkflowFailure({ message: 'review service unavailable' }),
        );
        const forkResult = yield* waitingBinding.forkFrom(
          sourceIdentity,
          requestEnvelope.value.offset,
          forkPayload,
        );
        expect(forkResult).toMatchObject({
          outcome: 'success',
          text: 'submitted',
        });
        const failedForkRecords = yield* Conversation.make(
          waitingAgent,
          forkPayload.conversationId,
        )
          .records()
          .pipe(Stream.runCollect);
        expect(
          [...failedForkRecords].some(
            ({ record }) =>
              record._tag === 'ToolWaitCompleted' &&
              record.token === forkRequest.token &&
              record.outcome === 'failure',
          ),
        ).toBe(true);
        expect(
          [...failedForkRecords].some(
            ({ record }) =>
              record._tag === 'ToolOutcome' && record.outcome === 'failure',
          ),
        ).toBe(true);
        expect(preparations).toBe(2);
        expect(handlerEntries).toBe(3);

        yield* Effect.all(
          [
            pendingSource.complete({
              approved: true,
              actor: 'alice',
            }),
            review.complete(request.token, {
              approved: true,
              actor: 'mallory',
            }),
          ],
          { concurrency: 'unbounded' },
        );
        yield* review.complete(request.token, {
          approved: true,
          actor: 'late',
        });
        const result = yield* waitingBinding.workflow.execute(payload);

        expect(result).toMatchObject({ outcome: 'success', text: 'submitted' });
        expect(preparations).toBe(2);
        expect(handlerEntries).toBe(4);

        const after = yield* Conversation.make(
          waitingAgent,
          payload.conversationId,
        )
          .records()
          .pipe(Stream.runCollect);
        expect(
          [...after].filter(({ record }) => record._tag === 'ToolSuspended'),
        ).toHaveLength(1);
        expect(
          [...after].filter(({ record }) => record._tag === 'ToolStarted'),
        ).toHaveLength(1);
        expect(
          [...after].filter(({ record }) => record._tag === 'ToolResumed'),
        ).toHaveLength(1);
        const completedWaits = [...after].filter(
          ({ record }) => record._tag === 'ToolWaitCompleted',
        );
        expect(completedWaits).toHaveLength(1);
        expect(completedWaits[0]?.record).toMatchObject({
          _tag: 'ToolWaitCompleted',
          wait: 'review-order',
          token: request.token,
          outcome: 'success',
        });
        const projected = yield* Conversation.make(
          waitingAgent,
          payload.conversationId,
        )
          .waits()
          .pipe(Stream.runCollect);
        expect([...projected].map(({ record }) => record._tag)).toEqual([
          'ToolSuspended',
          'ToolResumed',
          'ToolWaitCompleted',
        ]);
        expect(
          [...after].some(({ record }) => record._tag === 'ToolOutcome'),
        ).toBe(true);
        const sourceOutcome = [...after].find(
          ({ record }) => record._tag === 'ToolOutcome',
        )?.record;
        if (sourceOutcome?._tag !== 'ToolOutcome') {
          return yield* Effect.die('missing source ToolOutcome record');
        }
        expect(sourceOutcome.result).toMatchObject({
          status: expect.stringMatching(/^submitted-by:(alice|mallory)$/),
        });
      }).pipe(Effect.provide(WaitingLive), Effect.scoped);
    },
  );

  it.effect(
    'awaits independently keyed requests without returning a restarted token',
    () => {
      const approval = AgentWorkflow.wait({
        name: 'follow-approval',
        key: ({ requestId }: { readonly requestId: string }) => requestId,
        request: Schema.Struct({ requestId: Schema.String }),
        success: Schema.Boolean,
        error: Schema.Never,
      });
      const conversationId = LogVocabulary.ConversationId.make(
        'follow-pending-conversation',
      );
      const toolCallId = LogVocabulary.ToolCallId.make('approval-call');

      return Effect.gen(function* () {
        const session = yield* AgentLog.open(conversationId, {
          compatibility: {
            agent: agent.name,
            revision: agent.revision,
          },
        });
        yield* session.append([
          { _tag: 'ToolStarted', id: toolCallId, name: 'approve' },
          {
            _tag: 'ToolSuspended',
            id: toolCallId,
            name: 'approve',
            wait: 'follow-approval',
            token: 'prior-token',
            request: { requestId: 'prior' },
          },
        ]);

        const conversation = Conversation.make(agent, conversationId);
        const prior = yield* approval.awaitPending(conversation, 'prior');
        const awaitingFresh = yield* approval
          .awaitPending(conversation, 'fresh')
          .pipe(Effect.forkChild);
        yield* session.append([
          {
            _tag: 'ToolWaitRestarted',
            id: toolCallId,
            name: 'approve',
            wait: 'follow-approval',
            priorToken: 'prior-token',
          },
          {
            _tag: 'ToolSuspended',
            id: toolCallId,
            name: 'approve',
            wait: 'follow-approval',
            token: 'fresh-token',
            request: { requestId: 'fresh' },
          },
        ]);

        const fresh = yield* Fiber.join(awaitingFresh);
        expect(prior).toMatchObject({
          key: 'prior',
          token: 'prior-token',
          request: { requestId: 'prior' },
        });
        expect(fresh).toMatchObject({
          key: 'fresh',
          token: 'fresh-token',
          request: { requestId: 'fresh' },
        });

        yield* session.append([
          {
            _tag: 'ToolResumed',
            id: toolCallId,
            name: 'approve',
            token: 'fresh-token',
          },
          {
            _tag: 'ToolWaitCompleted',
            id: toolCallId,
            name: 'approve',
            wait: 'follow-approval',
            token: 'fresh-token',
            outcome: 'success',
            result: {},
          },
          {
            _tag: 'ToolOutcome',
            step: 1,
            id: toolCallId,
            name: 'approve',
            outcome: 'success',
            result: {},
          },
        ]);

        const awaitingRepeatedKey = yield* approval
          .awaitPending(conversation, 'fresh')
          .pipe(Effect.forkChild);
        const repeatedCallId = LogVocabulary.ToolCallId.make(
          'approval-call-repeated',
        );
        yield* session.append([
          { _tag: 'ToolStarted', id: repeatedCallId, name: 'approve' },
          {
            _tag: 'ToolSuspended',
            id: repeatedCallId,
            name: 'approve',
            wait: 'follow-approval',
            token: 'repeated-token',
            request: { requestId: 'fresh' },
          },
        ]);

        const repeated = yield* Fiber.join(awaitingRepeatedKey);
        expect(repeated).toMatchObject({
          key: 'fresh',
          token: 'repeated-token',
        });
      }).pipe(Effect.provide(testLogLayer), Effect.scoped);
    },
  );

  it.effect(
    'fails ambiguous keyed wait state instead of choosing a token',
    () =>
      Effect.gen(function* () {
        const conversationId = LogVocabulary.ConversationId.make(
          'ambiguous-pending-conversation',
        );
        const session = yield* AgentLog.open(conversationId, {
          compatibility: { agent: agent.name, revision: agent.revision },
        });
        const firstId = LogVocabulary.ToolCallId.make('ambiguous-first');
        const secondId = LogVocabulary.ToolCallId.make('ambiguous-second');
        yield* session.append([
          { _tag: 'ToolStarted', id: firstId, name: 'approve' },
          {
            _tag: 'ToolSuspended',
            id: firstId,
            name: 'approve',
            wait: 'proof-wait',
            token: 'ambiguous-token-a',
            request: { requestId: 'duplicate-key' },
          },
          { _tag: 'ToolStarted', id: secondId, name: 'approve' },
          {
            _tag: 'ToolSuspended',
            id: secondId,
            name: 'approve',
            wait: 'proof-wait',
            token: 'ambiguous-token-b',
            request: { requestId: 'duplicate-key' },
          },
        ]);

        const result = yield* proofWait
          .awaitPending(
            Conversation.make(agent, conversationId),
            'duplicate-key',
          )
          .pipe(Effect.exit);
        if (Exit.isSuccess(result)) {
          return expect.unreachable('ambiguous keys must not choose a token');
        }
        const error = Exit.findErrorOption(result);
        expect(Option.getOrThrow(error)).toMatchObject({
          _tag: 'WaitStateError',
          conversationId,
          wait: 'proof-wait',
          key: 'duplicate-key',
        });
      }).pipe(Effect.provide(testLogLayer), Effect.scoped),
  );

  it.effect(
    'keeps malformed projected requests in the schema error channel',
    () =>
      Effect.gen(function* () {
        const conversationId = LogVocabulary.ConversationId.make(
          'malformed-pending-conversation',
        );
        const toolCallId = LogVocabulary.ToolCallId.make('malformed-approval');
        const session = yield* AgentLog.open(conversationId, {
          compatibility: { agent: agent.name, revision: agent.revision },
        });
        yield* session.append([
          { _tag: 'ToolStarted', id: toolCallId, name: 'approve' },
          {
            _tag: 'ToolSuspended',
            id: toolCallId,
            name: 'approve',
            wait: 'proof-wait',
            token: 'malformed-token',
            request: { requestId: 42 },
          },
        ]);

        const result = yield* proofWait
          .awaitPending(Conversation.make(agent, conversationId), 'malformed')
          .pipe(Effect.exit);
        if (Exit.isSuccess(result)) {
          return expect.unreachable('malformed requests must fail decoding');
        }
        const error = Exit.findErrorOption(result);
        expect(Option.getOrThrow(error)).toBeInstanceOf(Schema.SchemaError);
      }).pipe(Effect.provide(testLogLayer), Effect.scoped),
  );
});
