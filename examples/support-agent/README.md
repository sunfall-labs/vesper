# The support-agent example

This walkthrough is the complete, credential-free story behind
`nub run example:support-agent`. The example is compiled with
requirement-channel assertions and runs entirely against a scripted model,
in-memory application adapters, and the in-memory conversation log. The
definition below introduces the core composition; the source also exercises
State, handler-level authorization, and a durable human approval inside the
refund handler.

## Mocked world

```ts
import { ScriptedModel } from '@sunfall/vesper-agent/testing';

// Arrays of Effect `Response.StreamPartEncoded`, not Vesper response wrappers.
const fake = ScriptedModel.make(supportTurns);

const World = supportWorkflow.layer.pipe(
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provide(fake.layer),
  Layer.provide(OrderRepoTest),
  Layer.provide(RefundAuthorizationTest),
  Layer.provideMerge(LogLive),
);
```

The fake is the provider: it implements Effect's `LanguageModel` seam directly
and models no vendor. Its strict script found the extra turn caused by an
accepted steer while this example was built. Application behavior is the same
definition used with a real provider; only Layers change.

## Agent definition

This definition exercises tools, application requirements, subagents, skills,
run budgets, compaction, durable approval, and idempotent Workflow activities.

```ts
import { Agent } from '@sunfall/vesper-agent/agent';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { Skill } from '@sunfall/vesper-agent/skill';
import { Stop } from '@sunfall/vesper-agent/stop';
import { AgentWorkflow } from '@sunfall/vesper-agent/workflow';
import { Context, Effect, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

/** An ordinary application service. Nothing here is AI-specific. */
export class OrderRepo extends Context.Service<
  OrderRepo,
  {
    readonly status: (id: string) => Effect.Effect<string>;
    readonly refund: (
      id: string,
      idempotencyKey: string,
    ) => Effect.Effect<string>;
  }
>()('example/OrderRepo') {}

const lookupOrder = Tool.make('lookup_order', {
  description: 'Look up the fulfilment status of one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
  // Declaring the service is what puts `OrderRepo` into the agent's
  // requirement channel — the run will not compile without it.
  dependencies: [OrderRepo],
});

const issueRefund = Tool.make('issue_refund', {
  description: 'Refund one order after human approval.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({
    status: Schema.Literals(['refunded', 'declined']),
    detail: Schema.String,
    actor: Schema.String,
  }),
  dependencies: [OrderRepo],
});

const refundApproval = AgentWorkflow.wait({
  name: 'refund-approval',
  key: ({ orderId }) => orderId,
  request: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({
    decision: Schema.Literals(['approve', 'deny']),
    actor: Schema.String,
  }),
  error: Schema.Never,
});

const refundOrder = AgentWorkflow.step({
  name: 'refund-order',
  key: (orderId: string) => orderId,
  success: Schema.String,
  error: Schema.Never,
  execute: (orderId: string) =>
    Effect.gen(function* () {
      const orders = yield* OrderRepo;
      const key = yield* AgentWorkflow.idempotencyKey('refund-order');
      return yield* orders.refund(orderId, key);
    }),
});

const refundPolicy: Skill.Skill = {
  name: 'refund_policy',
  description: 'When a refund is allowed and how to issue one.',
  instructions: [
    'Refunds are allowed within 30 days of delivery.',
    'Damaged goods are refundable at any time.',
    'Always confirm the order status before promising a refund.',
  ].join('\n'),
};

/** A specialist the support agent can hand bounded work to. */
export const researcher = Agent.make({
  name: 'researcher',
  revision: '1',
  description: 'Digs through documentation to answer a specific question.',
  instructions: 'Answer the question directly. Cite nothing you did not read.',
  toolkit: Toolkit.make(),
});

export const supportAgent = Agent.make({
  name: 'support',
  revision: '1',
  instructions: [
    'You handle customer support for an online store.',
    'Check order status before making promises.',
    'Delegate open-ended research rather than guessing.',
  ].join('\n'),

  toolkit: Toolkit.make(lookupOrder, AgentWorkflow.durable(issueRefund)),

  // Compiled into the toolkit; the child's requirements ride along.
  subagents: [researcher],

  // The catalog goes in the system prompt, the bodies load on demand — so
  // the cacheable prefix stays byte-identical across turns.
  skills: [refundPolicy],

  // Let the model observe the approved or declined result before stopping.
  stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(12)),

  compaction: {
    reserveTokens: 8_000,
    keepRecentTokens: 4_000,
    instructions: 'Summarise the customer’s issue and what has been tried.',
  },

  // Hard and shared by this root run, the researcher, and all descendants.
  runPolicy: {
    maxTurns: 24,
    maxModelCalls: 32,
    maxDelegatedTasks: 8,
    maxInputTokens: 250_000,
    maxOutputTokens: 32_000,
    wallClockMillis: 120_000,
  },
}).withHandlers({
  lookup_order: ({ orderId }) =>
    Effect.gen(function* () {
      const orders = yield* OrderRepo;
      return { status: yield* orders.status(orderId) };
    }),

  issue_refund: ({ orderId }) =>
    Effect.gen(function* () {
      const approval = yield* refundApproval({ orderId });
      if (approval.decision === 'deny') {
        return {
          status: 'declined',
          detail: 'The supervisor declined the refund.',
          actor: approval.actor,
        } as const;
      }
      return {
        status: 'refunded',
        detail: yield* refundOrder(orderId),
        actor: approval.actor,
      } as const;
    }),
});

class SupportWorkflowFailure extends Schema.TaggedError<SupportWorkflowFailure>(
  'SupportWorkflowFailure',
)('SupportWorkflowFailure', { message: Schema.String }) {}

const SupportRequest = AgentWorkflow.request({ runId: Schema.String });
const supportWorkflow = AgentWorkflow.make(supportAgent, {
  tag: 'SupportStory',
  payload: SupportRequest,
  idempotencyKey: ({ runId }) => runId,
  error: SupportWorkflowFailure,
  mapError: (error) => new SupportWorkflowFailure({ message: String(error) }),
});
```

## Running it

```ts
Effect.gen(function* () {
  const request = {
    runId: 'case-1042/initial',
    conversationId: 'case-1042',
    input: 'Refund damaged order_1042 when allowed.',
  };
  yield* supportWorkflow.workflow.execute(request, { discard: true });

  const pending = yield* refundApproval.awaitPending(
    Conversation.make(supportAgent, request.conversationId),
    'order_1042',
  );
  yield* pending.complete({ decision: 'approve', actor: 'supervisor-7' });

  const result = yield* supportWorkflow.workflow.execute(request);
});
```

Run the complete credential-free story with:

```bash
nub run example:support-agent
```

It loads a skill, delegates policy research, invokes stateful tools whose typed
handlers enforce authorization, suspends on a typed approval, performs the
refund as an idempotent Workflow activity, accepts a steer, resumes, prints the full
durable trail, and evaluates the same researcher definition used as a
subagent. The mocked supervisor completes the approval automatically; use the
focused `approval-cli` example (`nub run example:approval-cli`) to choose
approve or deny interactively.

Workflow input can also be any application-owned Effect Schema rather than a
string. `AgentWorkflow.makeWithInput` requires an exhaustive projection into
Effect's `Prompt.RawInput`, so typed participant events remain durable without
introducing a second prompt codec. See
[Schema-typed workflow input](../../packages/agent/README.md#schema-typed-workflow-input)
for the multiplayer composition and serialization pattern.
