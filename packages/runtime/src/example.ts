import { Agent } from '@sunfall/vesper-agent/agent';
import { Skill } from '@sunfall/vesper-agent/skill';
import { Stop } from '@sunfall/vesper-agent/stop';
import { Context, Effect, Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

// A worked example: a support agent that looks up orders, delegates research
// to a specialist, and loads refund policy on demand.
//
// This file is compiled and exercised by `example.test.ts`, so it cannot
// drift from the API it is documenting.

// ---------------------------------------------------------------- services

/** An ordinary application service. Nothing here is AI-specific. */
export class OrderRepo extends Context.Service<
  OrderRepo,
  {
    readonly status: (id: string) => Effect.Effect<string>;
    readonly refund: (id: string) => Effect.Effect<string>;
  }
>()('example/OrderRepo') {}

// ------------------------------------------------------------------- tools

const lookupOrder = Tool.make('lookup_order', {
  description: 'Look up the fulfilment status of one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
  // Declaring the service is what puts `OrderRepo` into the agent's
  // requirement channel — the run will not compile without it.
  dependencies: [OrderRepo],
});

/**
 * The terminal tool: once a refund is issued there is nothing left to decide,
 * so the loop stops. `Stop.toolCalled` below names this tool, and that name is
 * checked against the toolkit — a typo there would not compile.
 */
const issueRefund = Tool.make('issue_refund', {
  description: 'Refund one order. Irreversible; confirm the order first.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  dependencies: [OrderRepo],
});

const supportTools = Toolkit.make(lookupOrder, issueRefund);

// ------------------------------------------------------------------ skills

const refundPolicy: Skill.Skill = {
  name: 'refund_policy',
  description: 'When a refund is allowed and how to issue one.',
  instructions: [
    'Refunds are allowed within 30 days of delivery.',
    'Damaged goods are refundable at any time.',
    'Always confirm the order status before promising a refund.',
  ].join('\n'),
};

// --------------------------------------------------------------- subagent

/** A specialist the support agent can hand bounded work to. */
export const researcher = Agent.make({
  name: 'researcher',
  description: 'Digs through documentation to answer a specific question.',
  instructions: 'Answer the question directly. Cite nothing you did not read.',
  toolkit: Toolkit.make(),
});

// ------------------------------------------------------------------ agent

/**
 * The support agent, carrying its own handlers.
 *
 * Nothing has to be exported beside it and remembered at wiring time —
 * `OrderRepo` still surfaces in a caller's requirements, because a handler's
 * dependencies remain the application's to provide.
 */
export const supportAgent = Agent.make({
  name: 'support',
  instructions: [
    'You handle customer support for an online store.',
    'Check order status before making promises.',
    'Delegate open-ended research rather than guessing.',
  ].join('\n'),

  toolkit: supportTools,

  // Compiled into the toolkit; the child's requirements ride along.
  subagents: [researcher],

  // The catalog goes in the system prompt, the bodies load on demand — so
  // the cacheable prefix stays byte-identical across turns.
  skills: [refundPolicy],

  // Stop when the model stops calling tools, or at 12 steps, or as soon as
  // a refund has been issued.
  stopWhen: Stop.any(
    Stop.noToolCalls(),
    Stop.maxSteps(12),
    Stop.toolCalled('issue_refund'),
  ),

  // Compaction is on by default; this only tightens the headroom.
  compaction: {
    reserveTokens: 8_000,
    keepRecentTokens: 4_000,
    instructions: 'Summarise the customer’s issue and what has been tried.',
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

export * as Example from './example.js';
