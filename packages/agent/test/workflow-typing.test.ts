import type { LogStore } from '@sunfall/vesper-log/log-store';
import { Context, Effect, type Layer, Match, Schema } from 'effect';
import type { AiError, Prompt } from 'effect/ai';
import { Toolkit } from 'effect/ai';
import type { WorkflowEngine } from 'effect/workflow';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import { AgentWorkflow } from '../src/workflow.js';

// Compile-time contracts for AgentWorkflow; the scenarios are in workflow.test.ts.

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
  WorkflowEngine.WorkflowInstance | WorkflowEngine.WorkflowEngine,
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

const _multiplayerBinding = AgentWorkflow.makeWithInput(agent, {
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

const _binding = AgentWorkflow.make(agent, {
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

describe('AgentWorkflow typing', () => {
  it('is checked at compile time', () => {
    expect(true).toBe(true);
  });
});
