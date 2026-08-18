import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { afterAll, beforeAll, describe, expect, it } from '@effect/vitest';
import { ClusterWorkflowEngine, SingleRunner } from 'effect/unstable/cluster';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';
import { Layer, ManagedRuntime, Option, Redacted, Schema } from 'effect';

import { VesperPgClient } from '@sunfall/vesper-log-pg/client';
import { LogStorePg } from '@sunfall/vesper-log-pg/layer';
import {
  createPostgresTestHarness,
  type PostgresTestHarness,
  type ProvisionedTestDatabase,
} from '../../log-pg/test/pg-test-harness.js';
import { Agent } from '../src/agent.js';
import { ScriptedModel } from '../src/testing.js';
import { AgentWorkflow } from '../src/workflow.js';

const describeIntegration =
  process.env['RUN_POSTGRES_INTEGRATION'] === '1' ? describe : describe.skip;

class WorkflowFailure extends Schema.TaggedError<WorkflowFailure>(
  'workflow-cluster-test/WorkflowFailure',
)('WorkflowFailure', { message: Schema.String }) {}

const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

const turn: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'persisted' },
  { type: 'text-end', id: 'answer' },
  finish,
];

const agent = Agent.make({
  name: 'workflow-cluster-test',
  revision: '1',
  instructions: 'Answer briefly.',
  toolkit: Toolkit.make(),
});

const Request = AgentWorkflow.request({
  submissionId: Schema.String,
});

const binding = AgentWorkflow.make(agent, {
  tag: 'WorkflowClusterProbe',
  payload: Request,
  idempotencyKey: ({ submissionId }) => submissionId,
  error: WorkflowFailure,
  mapError: (error) =>
    new WorkflowFailure({
      message: error instanceof Error ? error.message : String(error),
    }),
});

const makeRuntimeLayer = (
  connectionString: string,
  model: Layer.Layer<LanguageModel.LanguageModel>,
) => {
  const postgres = VesperPgClient.layer({
    url: Redacted.make(connectionString),
  });
  const infrastructure = Layer.mergeAll(
    postgres,
    NodeCrypto.layer,
    NodeServices.layer,
  );
  const runner = SingleRunner.layer({
    runnerStorage: 'sql',
    shardingConfig: {
      shardsPerGroup: 1,
      entityMessagePollInterval: 50,
      entityReplyPollInterval: 50,
      refreshAssignmentsInterval: 50,
    },
  }).pipe(Layer.provide(infrastructure));
  const engine = ClusterWorkflowEngine.layer.pipe(Layer.provide(runner));
  const store = LogStorePg.layer().pipe(Layer.provide(infrastructure));

  return binding.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(engine, store, model, NodeCrypto.layer)),
  );
};

/**
 * This is a restart/reopen probe for one process at a time. SingleRunner keeps
 * mailbox, reply, and runner state in Postgres, but its runner communication
 * and health services are no-op; this test is not distributed failover proof.
 */
describeIntegration('AgentWorkflow persistent cluster composition', () => {
  let harness: PostgresTestHarness;
  let database: ProvisionedTestDatabase;

  beforeAll(async () => {
    harness = await createPostgresTestHarness();
    database = await harness.provisionDatabase({
      namePrefix: 'workflow_cluster',
    });
  }, 180_000);

  afterAll(async () => {
    if (database) await database.cleanup();
    if (harness) await harness.stop();
  }, 120_000);

  it(
    'reopens a completed Vesper workflow from Postgres in a new runtime',
    { timeout: 180_000 },
    async () => {
      const payload = {
        conversationId: 'workflow-cluster-conversation',
        input: 'Give me the durable answer.',
        submissionId: 'submission-1',
      };
      const modelA = ScriptedModel.make([turn]);
      const runtimeA = ManagedRuntime.make(
        makeRuntimeLayer(database.connectionString, modelA.layer),
      );

      try {
        const resultA = await runtimeA.runPromise(
          binding.workflow.execute(payload),
        );
        expect(resultA).toMatchObject({
          outcome: 'success',
          text: 'persisted',
        });
        expect(await runtimeA.runPromise(modelA.requests)).toHaveLength(1);
      } finally {
        await runtimeA.dispose();
      }

      const modelB = ScriptedModel.make([]);
      const runtimeB = ManagedRuntime.make(
        makeRuntimeLayer(database.connectionString, modelB.layer),
      );

      try {
        const executionId = await runtimeB.runPromise(
          binding.workflow.executionId(payload),
        );
        const reopened = await runtimeB.runPromise(
          binding.workflow.poll(executionId),
        );
        expect(Option.isSome(reopened)).toBe(true);
        if (Option.isSome(reopened)) {
          expect(reopened.value).toMatchObject({
            _tag: 'Complete',
            exit: { _tag: 'Success' },
          });
        }

        const resultB = await runtimeB.runPromise(
          binding.workflow.execute(payload),
        );
        expect(resultB).toMatchObject({
          outcome: 'success',
          text: 'persisted',
        });
        expect(await runtimeB.runPromise(modelB.requests)).toHaveLength(0);
      } finally {
        await runtimeB.dispose();
      }
    },
  );
});
