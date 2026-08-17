import { Agent } from '@sunfall/vesper-agent/agent';
import type { Effect } from 'effect';
import type { LanguageModel, Tool } from 'effect/unstable/ai';

import { OrderRepo, supportAgent } from './main.js';

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

const _notAny: IsAny<RunRequires> = 'not-any';
const _repoRequired: Has<OrderRepo, RunRequires> = 'yes';
const _modelRequired: Has<LanguageModel.LanguageModel, RunRequires> = 'yes';
const _handlersDischarged: Has<
  Tool.HandlersFor<Agent.Tools<typeof supportAgent>>,
  RunRequires
> = 'no';
const _name: Agent.Name<typeof supportAgent> = 'support';

void [_notAny, _repoRequired, _modelRequired, _handlersDischarged, _name];
