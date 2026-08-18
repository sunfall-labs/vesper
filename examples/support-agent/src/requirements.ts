import { Agent } from '@sunfall/vesper-agent/agent';
import type { Effect, Layer } from 'effect';
import type { LanguageModel, Tool } from 'effect/unstable/ai';
import type { WorkflowEngine } from 'effect/unstable/workflow';
import type { WorkflowInstance } from 'effect/unstable/workflow/WorkflowEngine';

import {
  OrderRepo,
  RefundAuthorization,
  SupportState,
  supportAgent,
  supportWorkflow,
} from './main.js';

type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';
type Has<Member, Union> = [Member] extends [Union] ? 'yes' : 'no';
type RunRequires =
  ReturnType<typeof supportAgent.run> extends Effect.Effect<
    infer _A,
    infer _E,
    infer R
  >
    ? R
    : never;
type WorkflowRequires =
  typeof supportWorkflow.layer extends Layer.Layer<infer _A, infer _E, infer R>
    ? R
    : never;

const _notAny: IsAny<RunRequires> = 'not-any';
const _repoRequired: Has<OrderRepo, RunRequires> = 'yes';
const _refundAuthorizationRequired: Has<RefundAuthorization, RunRequires> =
  'yes';
const _modelRequired: Has<LanguageModel.LanguageModel, RunRequires> = 'yes';
const _workflowEngineRequired: Has<WorkflowEngine.WorkflowEngine, RunRequires> =
  'yes';
const _workflowInstanceRequired: Has<WorkflowInstance, RunRequires> = 'yes';
const _bindingRequiresEngine: Has<
  WorkflowEngine.WorkflowEngine,
  WorkflowRequires
> = 'yes';
const _bindingDischargesInstance: Has<WorkflowInstance, WorkflowRequires> =
  'no';
const _stateHandleDischarged: Has<typeof SupportState, RunRequires> = 'no';
const _handlersDischarged: Has<
  Tool.HandlersFor<Agent.Tools<typeof supportAgent>>,
  RunRequires
> = 'no';
const _name: Agent.Name<typeof supportAgent> = 'support';

void [
  _notAny,
  _repoRequired,
  _refundAuthorizationRequired,
  _modelRequired,
  _workflowEngineRequired,
  _workflowInstanceRequired,
  _bindingRequiresEngine,
  _bindingDischargesInstance,
  _stateHandleDischarged,
  _handlersDischarged,
  _name,
];
