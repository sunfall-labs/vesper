import { LogStore } from '@sunfall/vesper-log/log-store';
import {
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schema,
  SchemaIssue,
  SchemaTransformation,
  Stream,
} from 'effect';
import { type Prompt, Tool, Toolkit } from 'effect/ai';
import { DurableDeferred, Workflow, WorkflowEngine } from 'effect/workflow';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { AgentWorkflow } from '../src/workflow.js';
import {
  finish,
  says,
  scripted,
  testLogLayer,
  type Seen,
} from './delegation-fixtures.js';

// Workflow scenarios: an agent run as a durable Effect Workflow against the
// in-memory engine and log. Compile-time contracts live in workflow-typing.

class WorkflowFailure extends Schema.TaggedError<WorkflowFailure>(
  'workflow-test/WorkflowFailure',
)('WorkflowFailure', { message: Schema.String }) {}

const agent = Agent.make({
  name: 'workflow-test',
  revision: '1',
  instructions: 'Answer briefly.',
  toolkit: Toolkit.make(),
});

const mapError = (error: unknown) =>
  new WorkflowFailure({ message: String(error) });

const binding = AgentWorkflow.make(agent, {
  tag: 'WorkflowTest',
  payload: AgentWorkflow.request({ submissionId: Schema.String }),
  idempotencyKey: ({ submissionId }) => submissionId,
  error: WorkflowFailure,
  mapError,
});

const live = <A, E, R>(
  made: { readonly layer: Layer.Layer<A, E, R> },
  turns: Parameters<typeof scripted>[0],
  seen?: Seen,
  store: Layer.Layer<LogStore.Service> = testLogLayer,
) =>
  made.layer.pipe(
    Layer.provideMerge(WorkflowEngine.layerMemory),
    Layer.provide(scripted(turns, seen)),
    Layer.provide(store),
  );

describe('AgentWorkflow runs', () => {
  it.effect('derives identities, runs the agent once per idempotency key', () =>
    Effect.gen(function* () {
      const payload = {
        submissionId: 'same-submission',
        conversationId: 'same-conversation',
        input: 'once',
      };
      const ids = yield* binding.identify(payload);
      expect(ids).toMatchObject({
        executionId: yield* binding.workflow.executionId(payload),
        conversationId: 'same-conversation',
      });
      const invalid = yield* binding
        .identify({ ...payload, submissionId: 'bad', conversationId: '' })
        .pipe(Effect.result);
      expect(invalid._tag).toBe('Failure');

      const seen: Seen = { prompts: [], tools: [] };
      const [first, second] = yield* Effect.gen(function* () {
        return [
          yield* binding.workflow.execute(payload),
          yield* binding.workflow.execute(payload),
        ] as const;
      }).pipe(
        Effect.provide(live(binding, [says('call:1'), says('call:2')], seen)),
      );

      expect(first).toMatchObject({ outcome: 'success', text: 'call:1' });
      expect(second.text).toBe('call:1');
      expect(seen.prompts).toHaveLength(1);
    }),
  );

  it.effect('projects schema-typed application input into the prompt', () =>
    Effect.gen(function* () {
      const Input = Schema.TaggedUnion({
        ParticipantMessage: {
          participantId: Schema.String,
          text: Schema.String,
        },
      });
      const multiplayer = AgentWorkflow.makeWithInput(agent, {
        tag: 'MultiplayerWorkflowTest',
        payload: AgentWorkflow.request({ submissionId: Schema.String }, Input),
        idempotencyKey: ({ submissionId }) => submissionId,
        input: ({ input }): Prompt.RawInput => [
          { role: 'user', content: `[${input.participantId}] ${input.text}` },
        ],
        error: WorkflowFailure,
        mapError,
      });
      const seen: Seen = { prompts: [], tools: [] };

      const result = yield* multiplayer.workflow
        .execute({
          submissionId: 'multiplayer',
          conversationId: 'multiplayer-conversation',
          input: {
            _tag: 'ParticipantMessage',
            participantId: 'alice',
            text: 'hello from the room',
          },
        })
        .pipe(Effect.provide(live(multiplayer, [says('hello')], seen)));

      expect(result.text).toBe('hello');
      expect(JSON.stringify(seen.prompts[0])).toContain(
        '[alice] hello from the room',
      );
    }),
  );

  it.effect('maps storage failures through the declared workflow error', () =>
    Effect.gen(function* () {
      let mapped: unknown;
      const mapping = AgentWorkflow.make(agent, {
        tag: 'StorageFailureWorkflowTest',
        payload: AgentWorkflow.request({ submissionId: Schema.String }),
        idempotencyKey: ({ submissionId }) => submissionId,
        error: WorkflowFailure,
        mapError: (error) => {
          mapped = error;
          return new WorkflowFailure({
            message: Schema.is(LogStore.LogStoreError)(error)
              ? error.reason
              : String(error),
          });
        },
      });
      const offline = Layer.effect(
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

      const error = yield* mapping.workflow
        .execute({
          submissionId: 'storage',
          conversationId: 'storage-conversation',
          input: 'hello',
        })
        .pipe(
          Effect.provide(live(mapping, [says('hello')], undefined, offline)),
          Effect.flip,
        );

      expect(error).toMatchObject({ message: 'storage' });
      expect(mapped).toBeInstanceOf(LogStore.LogStoreError);
    }),
  );
});

describe('AgentWorkflow steps', () => {
  it.effect('replays by stable name and key, with stable external keys', () =>
    Effect.gen(function* () {
      const executions: string[] = [];
      const lookup = AgentWorkflow.step({
        name: 'lookup',
        key: ({ orderId }: { orderId: string }) => orderId,
        success: Schema.String,
        error: WorkflowFailure,
        execute: ({ orderId }) =>
          Effect.sync(() => {
            executions.push(orderId);
            return `result:${orderId}`;
          }),
      });
      const payment = AgentWorkflow.step({
        name: 'payment',
        key: () => 'payment',
        success: Schema.String,
        error: WorkflowFailure,
        execute: () => AgentWorkflow.idempotencyKey('payment'),
      });
      const Steps = Workflow.make('StepWorkflow', {
        payload: { id: Schema.String },
        idempotencyKey: ({ id }) => id,
        success: Schema.Struct({
          lookups: Schema.Array(Schema.String),
          payment: Schema.String,
        }),
        error: WorkflowFailure,
      });
      const StepsLive = Steps.toLayer(() =>
        Effect.gen(function* () {
          // The input-derived key separates "a/b" from "b" and replays repeats.
          const lookups = yield* Effect.all([
            lookup({ orderId: 'a/b' }),
            lookup({ orderId: 'b' }),
            lookup({ orderId: 'a/b' }),
          ]);
          return { lookups, payment: yield* payment(undefined) };
        }),
      ).pipe(Layer.provideMerge(WorkflowEngine.layerMemory));

      const [first, second] = yield* Effect.gen(function* () {
        return [
          yield* Steps.execute({ id: 'same' }),
          yield* Steps.execute({ id: 'same' }),
        ] as const;
      }).pipe(Effect.provide(StepsLive));

      expect(first.lookups).toEqual(['result:a/b', 'result:b', 'result:a/b']);
      expect(executions).toEqual(['a/b', 'b']);
      expect(second.payment).toBe(first.payment);
      expect(first.payment).toHaveLength(32);
    }),
  );

  it('rejects empty step and wait keys before doing anything', () => {
    const step = AgentWorkflow.step({
      name: 'invalid-step',
      key: () => '',
      success: Schema.Void,
      error: WorkflowFailure,
      execute: () => Effect.void,
    });
    const wait = AgentWorkflow.wait({
      name: 'invalid-wait',
      key: () => '',
      request: Schema.Void,
      success: Schema.Void,
      error: Schema.Never,
    });

    expect(() => step(undefined)).toThrow(
      'AgentWorkflow step "invalid-step" produced an empty key',
    );
    expect(() => wait(undefined)).toThrow(
      'AgentWorkflow wait "invalid-wait" produced an empty key',
    );
  });

  it.effect(
    'keeps wait result encoding failures in the typed error channel',
    () =>
      Effect.gen(function* () {
        const Unencodable = Schema.String.pipe(
          Schema.decodeTo(
            Schema.Finite,
            SchemaTransformation.transformEffect<number, string>({
              decode: (value) => Effect.succeed(Number(value)),
              encode: () =>
                Effect.fail(new SchemaIssue.InvalidValue({ message: 'bad' })),
            }),
          ),
        );
        const approval = AgentWorkflow.wait({
          name: 'encoding-wait',
          key: () => 'request',
          request: Schema.Void,
          success: Unencodable,
          error: Schema.Never,
        });
        const token = new DurableDeferred.TokenParsed({
          workflowName: 'EncodingWorkflow',
          executionId: 'encoding-execution',
          deferredName: 'encoding-wait/request',
        }).asToken;

        const result = yield* Effect.exit(approval.complete(token, 1));

        expect(Option.getOrThrow(Exit.findErrorOption(result))).toBeInstanceOf(
          Schema.SchemaError,
        );
      }).pipe(Effect.provide(WorkflowEngine.layerMemory)),
  );
});

const suspended = (conversation: Agent.Any, id: string, except?: string) =>
  Conversation.make(conversation, id)
    .followWaits()
    .pipe(
      Stream.filter(
        ({ record }) =>
          record._tag === 'ToolSuspended' && record.token !== except,
      ),
      Stream.runHead,
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.die('missing ToolSuspended record'),
          onSome: (envelope) =>
            envelope.record._tag === 'ToolSuspended'
              ? Effect.succeed({ envelope, record: envelope.record })
              : Effect.die('missing ToolSuspended record'),
        }),
      ),
    );

const submitTurn = (...orderIds: ReadonlyArray<string>) => [
  ...orderIds.map((orderId, index) => ({
    type: 'tool-call' as const,
    id: `submit-${String(index)}`,
    name: 'submit_order',
    params: { orderId },
  })),
  finish('tool-calls'),
];

describe('AgentWorkflow durable tool waits', () => {
  const review = AgentWorkflow.wait({
    name: 'review-order',
    key: ({ orderId }: { readonly orderId: string }) => orderId,
    request: Schema.Struct({ orderId: Schema.String, prepared: Schema.String }),
    success: Schema.Struct({ approved: Schema.Boolean, actor: Schema.String }),
    error: WorkflowFailure,
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

  const build = () => {
    const counts = { preparations: 0, handlerEntries: 0 };
    const prepare = AgentWorkflow.step({
      name: 'prepare-order',
      key: (orderId: string) => orderId,
      success: Schema.String,
      error: WorkflowFailure,
      execute: (orderId: string) =>
        Effect.sync(() => {
          counts.preparations += 1;
          return `prepared:${orderId}`;
        }),
    });
    const waiting = Agent.make({
      name: 'waiting-agent',
      revision: '1',
      instructions: 'Submit the order.',
      toolkit: Toolkit.make(submit),
      runPolicy: { maxToolConcurrency: 2 },
    }).withHandlers({
      submit_order: ({ orderId }) =>
        Effect.gen(function* () {
          counts.handlerEntries += 1;
          const prepared = yield* prepare(orderId);
          const decision = yield* review({ orderId, prepared });
          return {
            status: decision.approved
              ? `submitted-by:${decision.actor}`
              : 'denied',
          };
        }),
    });
    const waitingBinding = AgentWorkflow.make(waiting, {
      tag: 'WaitingAgentWorkflow',
      payload: AgentWorkflow.request({ submissionId: Schema.String }),
      idempotencyKey: ({ submissionId }) => submissionId,
      error: WorkflowFailure,
      mapError,
    });
    return { counts, waiting, waitingBinding };
  };

  it.effect('suspends, forks, restarts and resumes one logical handler', () => {
    const { counts, waiting, waitingBinding } = build();
    const other = AgentWorkflow.wait({
      name: 'review-unrelated',
      key: ({ orderId }: { readonly orderId: string }) => orderId,
      request: Schema.Struct({ orderId: Schema.String }),
      success: Schema.Struct({ approved: Schema.Boolean }),
      error: Schema.Never,
    });
    const mismatched = AgentWorkflow.wait({
      name: 'review-order',
      key: ({ orderId }: { readonly orderId: number }) => String(orderId),
      request: Schema.Struct({ orderId: Schema.Finite }),
      success: Schema.Boolean,
      error: Schema.Never,
    });
    const payload = {
      submissionId: 'submission-wait',
      conversationId: 'conversation-wait',
      input: 'submit order 42',
    };
    const Live = live(
      waitingBinding,
      [submitTurn('order-42'), says('submitted')],
      undefined,
    ).pipe(Layer.provideMerge(testLogLayer));
    const request = { orderId: 'order-42', prepared: 'prepared:order-42' };

    return Effect.gen(function* () {
      const source = Conversation.make(waiting, payload.conversationId);
      // Waiters may be installed before the discarded workflow has a log.
      const awaitingSource = yield* review
        .awaitPending(source, 'order-42')
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* waitingBinding.workflow.execute(payload, { discard: true });

      const first = yield* suspended(waiting, payload.conversationId);
      expect(counts).toEqual({ preparations: 1, handlerEntries: 1 });
      expect(first.record).toMatchObject({
        name: 'submit_order',
        wait: 'review-order',
        request,
      });
      const pendingSource = yield* Fiber.join(awaitingSource);
      expect(pendingSource).toMatchObject({
        conversationId: payload.conversationId,
        offset: first.envelope.offset,
        toolCallId: first.record.id,
        toolName: 'submit_order',
        key: 'order-42',
        token: first.record.token,
        request,
      });

      // A token only completes the wait that issued it, and a request that
      // does not match the wait's schema fails decoding.
      const wrong = yield* Effect.exit(
        other.complete(first.record.token, { approved: true }),
      );
      expect(
        Exit.isFailure(wrong) &&
          Option.getOrUndefined(Exit.findErrorOption(wrong))?._tag,
      ).toBe('WaitTokenError');
      const bad = yield* Effect.exit(
        mismatched.awaitPending(source, 'order-42'),
      );
      expect(
        Exit.isFailure(bad) && Option.getOrUndefined(Exit.findErrorOption(bad)),
      ).toBeInstanceOf(Schema.SchemaError);

      // Forking at the suspension restarts the wait with a fresh token while
      // the source keeps its own.
      const sourceIdentity = yield* waitingBinding.identify(payload);
      const forkPayload = {
        submissionId: 'submission-wait-fork',
        conversationId: 'conversation-wait-fork',
        input: 'submit order 42 in the fork',
      };
      const forkIdentity = yield* waitingBinding.forkFrom(
        sourceIdentity,
        first.envelope.offset,
        forkPayload,
        { discard: true },
      );
      expect(forkIdentity).toMatchObject({
        workflow: 'path',
        conversationId: forkPayload.conversationId,
      });
      const forked = yield* suspended(
        waiting,
        forkPayload.conversationId,
        first.record.token,
      );
      expect(counts).toEqual({ preparations: 2, handlerEntries: 2 });
      const pendingFork = yield* review.awaitPending(
        Conversation.make(waiting, forkPayload.conversationId),
        'order-42',
      );
      expect(pendingFork).toMatchObject({
        token: forked.record.token,
        request,
      });
      expect((yield* review.awaitPending(source, 'order-42')).token).toBe(
        first.record.token,
      );

      // A failed wait settles as a failed tool result and the run goes on.
      yield* pendingFork.fail(
        new WorkflowFailure({ message: 'review service unavailable' }),
      );
      const forkResult = yield* waitingBinding.forkFrom(
        sourceIdentity,
        first.envelope.offset,
        forkPayload,
      );
      expect(forkResult).toMatchObject({
        outcome: 'success',
        text: 'submitted',
      });
      const forkRecords = yield* Conversation.make(
        waiting,
        forkPayload.conversationId,
      )
        .records()
        .pipe(Stream.runCollect);
      expect(
        [...forkRecords].some(
          ({ record }) =>
            record._tag === 'ToolWaitCompleted' &&
            record.token === forked.record.token &&
            record.outcome === 'failure',
        ),
      ).toBe(true);
      expect(counts).toEqual({ preparations: 2, handlerEntries: 3 });

      // Concurrent and late completions resolve the wait exactly once.
      yield* Effect.all(
        [
          pendingSource.complete({ approved: true, actor: 'alice' }),
          review.complete(first.record.token, {
            approved: true,
            actor: 'mallory',
          }),
        ],
        { concurrency: 'unbounded' },
      );
      yield* review.complete(first.record.token, {
        approved: true,
        actor: 'late',
      });
      const result = yield* waitingBinding.workflow.execute(payload);

      expect(result).toMatchObject({ outcome: 'success', text: 'submitted' });
      expect(counts).toEqual({ preparations: 2, handlerEntries: 4 });
      const after = [...(yield* source.records().pipe(Stream.runCollect))].map(
        ({ record }) => record,
      );
      const count = (tag: string) =>
        after.filter((record) => record._tag === tag).length;
      expect([
        count('ToolSuspended'),
        count('ToolStarted'),
        count('ToolResumed'),
        count('ToolWaitCompleted'),
      ]).toEqual([1, 1, 1, 1]);
      expect(after.find((r) => r._tag === 'ToolWaitCompleted')).toMatchObject({
        wait: 'review-order',
        token: first.record.token,
        outcome: 'success',
      });
      expect(after.find((r) => r._tag === 'ToolOutcome')).toMatchObject({
        result: {
          status: expect.stringMatching(/^submitted-by:(alice|mallory)$/),
        },
      });
      const waits = yield* source.waits().pipe(Stream.runCollect);
      expect([...waits].map(({ record }) => record._tag)).toEqual([
        'ToolSuspended',
        'ToolResumed',
        'ToolWaitCompleted',
      ]);
    }).pipe(Effect.provide(Live), Effect.scoped);
  });

  it.effect(
    'awaits independently keyed requests from parallel tool calls',
    () => {
      const { waiting, waitingBinding } = build();
      return Effect.gen(function* () {
        const payload = {
          submissionId: 'parallel',
          conversationId: 'parallel-conversation',
          input: 'submit both',
        };
        yield* waitingBinding.workflow.execute(payload, { discard: true });
        const conversation = Conversation.make(waiting, payload.conversationId);
        const one = yield* review.awaitPending(conversation, 'o1');
        const two = yield* review.awaitPending(conversation, 'o2');

        expect(one.request.orderId).toBe('o1');
        expect(two.request.orderId).toBe('o2');
        expect(one.token).not.toBe(two.token);
      }).pipe(
        Effect.provide(
          live(waitingBinding, [submitTurn('o1', 'o2'), says('done')]).pipe(
            Layer.provideMerge(testLogLayer),
          ),
        ),
        Effect.scoped,
      );
    },
  );
});
