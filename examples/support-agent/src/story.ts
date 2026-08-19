import * as NodeRuntime from '@effect/platform-node/NodeRuntime';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { AgentEval } from '@sunfall/vesper-agent/eval';
import { ScriptedModel } from '@sunfall/vesper-agent/testing';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { Console, Effect, Layer, Stream } from 'effect';
import type { Response } from 'effect/unstable/ai';
import { WorkflowEngine } from 'effect/unstable/workflow';

import {
  OrderRepo,
  RefundAuthorization,
  refundApproval,
  researcher,
  supportAgent,
  supportWorkflow,
} from './main.js';

const finish = (
  reason: Response.FinishPartEncoded['reason'] = 'stop',
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 8, uncached: 8, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const textTurn = (
  id: string,
  text: string,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id },
  { type: 'text-delta', id, delta: text },
  { type: 'text-end', id },
  finish(),
];

const toolTurn = (
  id: string,
  name: string,
  params: unknown,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'tool-call', id, name, params },
  finish('tool-calls'),
];

const supportTurns = (): ReadonlyArray<
  ReadonlyArray<Response.StreamPartEncoded>
> => [
  toolTurn('skill-1', 'load_skill', { name: 'refund_policy' }),
  toolTurn('research-1', 'task_researcher', {
    prompt: 'Does a damaged delivered item qualify for a refund?',
  }),
  textTurn(
    'research-answer',
    'Damaged goods remain refundable after delivery under the loaded policy.',
  ),
  toolTurn('lookup-1', 'lookup_order', { orderId: 'order_1042' }),
  toolTurn('refund-1', 'issue_refund', { orderId: 'order_1042' }),
  textTurn(
    'refund-answer',
    'The order qualified and a supervisor approved it. The refund is confirmed.',
  ),
  textTurn('follow-up', 'The customer confirmation has been noted.'),
  textTurn('closed', 'The case is closed and its durable history is intact.'),
];

const storyModel = ScriptedModel.make(supportTurns());
const evalModel = ScriptedModel.make([
  textTurn(
    'eval-answer',
    'Damaged goods remain refundable after delivery under the refund policy.',
  ),
]);

const OrderRepoTest = Layer.succeed(OrderRepo, {
  status: (id) => Effect.succeed(`${id}: delivered, damaged`),
  refund: (id, idempotencyKey) =>
    Effect.succeed(`refund:${id}:confirmed:${idempotencyKey}`),
});

const RefundAuthorizationTest = Layer.succeed(RefundAuthorization, {
  allowed: Effect.succeed(true),
});

const LogLive = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

const World = supportWorkflow.layer.pipe(
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provide(storyModel.layer),
  Layer.provide(OrderRepoTest),
  Layer.provide(RefundAuthorizationTest),
  Layer.provideMerge(LogLive),
);

const input = [
  'Customer customer@example.test received a damaged order_1042.',
  'Check whether it qualifies and refund it when allowed.',
].join(' ');

const program = Effect.gen(function* () {
  const conversationId = 'support-story-order-1042';
  const conversation = Conversation.make(supportAgent, conversationId);
  const firstRun = {
    runId: 'support-story-order-1042/initial',
    conversationId,
    input,
  };

  // Start without waiting for completion: the handler will durably suspend.
  yield* supportWorkflow.workflow.execute(firstRun, { discard: true });
  const pending = yield* refundApproval.awaitPending(
    conversation,
    'order_1042',
  );
  yield* Console.log(
    `Approval: ${pending.request.orderId} (${pending.request.orderStatus})`,
  );
  yield* pending.complete({ decision: 'approve', actor: 'mock-supervisor' });

  // This awaits the same idempotent execution after the external decision.
  const completed = yield* supportWorkflow.workflow.execute(firstRun);
  yield* Console.log(`Agent: ${completed.text}`);

  yield* conversation.send({
    kind: 'steer',
    text: 'The customer confirmed receipt of the refund.',
    source: 'support-console',
  });
  const resumed = yield* supportWorkflow.workflow.execute({
    runId: 'support-story-order-1042/follow-up',
    conversationId,
    input: 'Close the case.',
  });
  yield* Console.log(`Resumed: ${resumed.text}`);

  const records = yield* conversation.records().pipe(Stream.runCollect);
  yield* Console.log(
    `Durable records: ${Array.from(records)
      .map(({ record }) => record._tag)
      .join(' -> ')}`,
  );

  const capture = yield* AgentEval.run(
    researcher,
    'Does a damaged delivered item qualify for a refund?',
  ).pipe(Effect.provide(evalModel.layer));
  const report = yield* AgentEval.evaluate(capture, [
    AgentEval.check('answered from the policy', ({ result }) =>
      result.text.includes('refundable'),
    ),
  ]);
  yield* Console.log(`Eval: ${report.passed ? 'passed' : 'failed'}`);

  const requests = yield* storyModel.requests;
  yield* Console.log(`Fake provider calls: ${String(requests.length)}`);
}).pipe(Effect.provide(World), Effect.scoped);

NodeRuntime.runMain(program);
