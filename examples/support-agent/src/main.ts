import { Agent } from '@sunfall/vesper-agent/agent';
import type { Skill } from '@sunfall/vesper-agent/skill';
import { AgentState } from '@sunfall/vesper-agent/state';
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

/** Application policy required by the typed refund handler. */
export class RefundAuthorization extends Context.Service<
  RefundAuthorization,
  { readonly allowed: Effect.Effect<boolean> }
>()('example/RefundAuthorization') {}

const SupportCase = Schema.Struct({
  orderId: Schema.NullOr(Schema.String),
  status: Schema.NullOr(Schema.String),
  refundIssued: Schema.Boolean,
});

/** Durable per-conversation state, supplied to handlers by the agent itself. */
export const SupportState = AgentState.make({
  id: 'support-case',
  version: '1',
  schema: SupportCase,
  initial: (): typeof SupportCase.Type => ({
    orderId: null,
    status: null,
    refundIssued: false,
  }),
});

const lookupOrder = Tool.make('lookup_order', {
  description: 'Look up the fulfilment status of one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
  failure: AgentState.Error,
  failureMode: 'return',
  dependencies: AgentState.dependencies(SupportState, OrderRepo),
});

const issueRefund = Tool.make('issue_refund', {
  description:
    'Refund one order. Irreversible; confirm the order and obtain human approval first.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({
    status: Schema.Literals(['refunded', 'declined']),
    detail: Schema.String,
    actor: Schema.String,
  }),
  failure: AgentState.Error,
  failureMode: 'return',
  dependencies: AgentState.dependencies(
    SupportState,
    OrderRepo,
    RefundAuthorization,
  ),
});

/** One independently keyed decision that may outlive the current process. */
export const refundApproval = AgentWorkflow.wait({
  name: 'refund-approval',
  key: (request: { readonly orderId: string }) => request.orderId,
  request: Schema.Struct({
    orderId: Schema.String,
    orderStatus: Schema.String,
    reason: Schema.String,
  }),
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
      const idempotencyKey =
        yield* AgentWorkflow.idempotencyKey('refund-order');
      return yield* orders.refund(orderId, idempotencyKey);
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

export const researcher = Agent.make({
  name: 'researcher',
  revision: '1',
  description: 'Digs through documentation to answer a specific question.',
  instructions: 'Answer the question directly. Cite nothing you did not read.',
  toolkit: Toolkit.make(),
});

/**
 * A worked composition whose handler dependencies remain visible to callers.
 * This lives in a compiled example rather than a published library package.
 */
export const supportAgent = Agent.make({
  name: 'support',
  revision: '1',
  instructions: [
    'You handle customer support for an online store.',
    'Check order status before making promises.',
    'Delegate open-ended research rather than guessing.',
  ].join('\n'),
  toolkit: Toolkit.make(lookupOrder, AgentWorkflow.durable(issueRefund)),
  subagents: [researcher],
  skills: [refundPolicy],
  state: SupportState,
  stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(12)),
  compaction: {
    reserveTokens: 8_000,
    keepRecentTokens: 4_000,
    instructions: "Summarise the customer's issue and what has been tried.",
  },
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
      const state = yield* SupportState;
      const status = yield* orders.status(orderId);
      yield* state.set({ orderId, status, refundIssued: false });
      return { status };
    }),
  issue_refund: ({ orderId }) =>
    Effect.gen(function* () {
      const authorization = yield* RefundAuthorization;
      if (!(yield* authorization.allowed)) {
        return {
          status: 'declined',
          detail: 'The refund is not allowed by policy.',
          actor: 'automated-policy',
        } as const;
      }

      const state = yield* SupportState;
      const current = yield* state.get;
      const approval = yield* refundApproval({
        orderId,
        orderStatus: current.status ?? 'unknown',
        reason: 'Refunds are irreversible and require supervisor approval.',
      });
      if (approval.decision === 'deny') {
        return {
          status: 'declined',
          detail: 'The supervisor declined the refund.',
          actor: approval.actor,
        } as const;
      }

      const confirmation = yield* refundOrder(orderId);
      yield* state.update((previous) => ({
        ...previous,
        orderId,
        refundIssued: true,
      }));
      return {
        status: 'refunded',
        detail: confirmation,
        actor: approval.actor,
      } as const;
    }),
});

class SupportWorkflowFailure extends Schema.TaggedError<SupportWorkflowFailure>(
  'example/SupportWorkflowFailure',
)('SupportWorkflowFailure', { message: Schema.String }) {}

const SupportRequest = AgentWorkflow.request({ runId: Schema.String });

/** Durable execution and wakeup for the recorded support agent. */
export const supportWorkflow = AgentWorkflow.make(supportAgent, {
  tag: 'SupportStory',
  payload: SupportRequest,
  idempotencyKey: ({ runId }) => runId,
  error: SupportWorkflowFailure,
  mapError: (error) => new SupportWorkflowFailure({ message: String(error) }),
});
