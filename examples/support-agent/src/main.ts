import { Agent } from '@sunfall/vesper-agent/agent';
import { Skill } from '@sunfall/vesper-agent/skill';
import { Stop } from '@sunfall/vesper-agent/stop';
import { Context, Effect, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

/** An ordinary application service. Nothing here is AI-specific. */
export class OrderRepo extends Context.Service<
  OrderRepo,
  {
    readonly status: (id: string) => Effect.Effect<string>;
    readonly refund: (id: string) => Effect.Effect<string>;
  }
>()('example/OrderRepo') {}

const lookupOrder = Tool.make('lookup_order', {
  description: 'Look up the fulfilment status of one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
  dependencies: [OrderRepo],
});

const issueRefund = Tool.make('issue_refund', {
  description: 'Refund one order. Irreversible; confirm the order first.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  dependencies: [OrderRepo],
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
  toolkit: Toolkit.make(lookupOrder, issueRefund),
  subagents: [researcher],
  skills: [refundPolicy],
  stopWhen: Stop.any(
    Stop.noToolCalls(),
    Stop.maxSteps(12),
    Stop.toolCalled('issue_refund'),
  ),
  compaction: {
    reserveTokens: 8_000,
    keepRecentTokens: 4_000,
    instructions: "Summarise the customer's issue and what has been tried.",
  },
}).withHandlers({
  lookup_order: ({ orderId }) =>
    Effect.gen(function* () {
      const orders = yield* OrderRepo;
      return { status: yield* orders.status(orderId) };
    }),
  issue_refund: ({ orderId }) =>
    Effect.gen(function* () {
      const orders = yield* OrderRepo;
      return { confirmation: yield* orders.refund(orderId) };
    }),
});
