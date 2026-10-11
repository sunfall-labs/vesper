import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Layer, Schema, Stream } from 'effect';
import { AiError, type Response, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { CodeExecutor } from '../src/code-executor.js';
import { NodeCodeExecutor } from '../src/code-executor-node.js';
import { Conversation } from '../src/conversation.js';
import { DynamicToolkit } from '../src/dynamic-toolkit.js';
import { Interception } from '../src/interception.js';
import * as AgentLog from '../src/log.js';
import { ScriptedModel } from '../src/testing.js';
import { testBudget } from './budget-fixture.js';
import { digestOf } from './tool-fixtures.js';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

type Turn = ReadonlyArray<Response.StreamPartEncoded>;

const answeringTurn: Turn = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'done' },
  { type: 'text-end', id: 'answer' },
  finish(),
];

/** A model turn that calls `exec` (or any other tool) once. */
const callTurn = (
  name: string,
  params: Record<string, unknown>,
  id = 'exec-1',
): Turn => [{ type: 'tool-call', id, name, params }, finish('tool-calls')];
const execTurn = (source = 'compose', id = 'exec-1'): Turn =>
  callTurn('exec', { source }, id);

/** A scripted model and a scripted executor, ready to provide to a run. */
const rig = (
  events: ReadonlyArray<CodeExecutor.Event>,
  turns: ReadonlyArray<Turn> = [execTurn(), answeringTurn],
) => {
  const executor = CodeExecutor.fake(events);
  const model = ScriptedModel.make(turns);
  return {
    executor,
    model,
    layer: Layer.merge(model.layer, executor.layer),
  };
};

const toolCall = (
  id: string,
  name: string,
  input: CodeExecutor.JsonValue,
  key?: string,
): CodeExecutor.Event => ({
  _tag: 'ToolCall',
  id,
  name,
  input,
  ...(key === undefined ? {} : { key }),
});
const done: CodeExecutor.Event = { _tag: 'Completion', state: {} };

const logLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

const recordsOf = <E, R>(conversation: {
  readonly records: () => Stream.Stream<ConversationRecord.Envelope, E, R>;
}) =>
  conversation.records().pipe(
    Stream.runCollect,
    Effect.map((all) => Array.from(all).map(({ record }) => record)),
  );

const promptOf = (request: { readonly prompt: unknown } | undefined) =>
  JSON.stringify(request?.prompt);

const hiddenForTyping = Tool.make('hidden_for_typing', {
  parameters: Schema.Struct({}),
  success: Schema.String,
});
const codeModeForTyping = Agent.make({
  name: 'code-mode-typing',
  revision: '1',
  instructions: 'Use code mode.',
  toolkit: Toolkit.make(hiddenForTyping),
  codeMode: true,
});
const visibleExec: keyof Agent.Tools<typeof codeModeForTyping> = 'exec';
// @ts-expect-error hidden tools are broker capabilities, not model-visible tools
const hiddenIsNotVisible: keyof Agent.Tools<typeof codeModeForTyping> =
  'hidden_for_typing';
type Has<Needle, Haystack> = [Extract<Haystack, Needle>] extends [never]
  ? 'no'
  : 'yes';
const executorIsRequired: Has<
  CodeExecutor.Service,
  Agent.Requires<typeof codeModeForTyping>
> = 'yes';
void visibleExec;
void hiddenIsNotVisible;
void executorIsRequired;

const exceptedForTyping = Tool.make('excepted_for_typing', {
  parameters: Schema.Struct({}),
  success: Schema.String,
});
const exceptModeForTyping = Agent.make({
  name: 'code-mode-except-typing',
  revision: '1',
  instructions: 'Use code mode, except one tool.',
  toolkit: Toolkit.make(hiddenForTyping, exceptedForTyping),
  codeMode: { except: ['excepted_for_typing'] },
});
// Both halves of the split are visible in the model-facing tool record:
// `exec` for the brokered half, the excepted tool directly.
const exceptExecVisible: keyof Agent.Tools<typeof exceptModeForTyping> = 'exec';
const exceptToolVisible: keyof Agent.Tools<typeof exceptModeForTyping> =
  'excepted_for_typing';
// @ts-expect-error brokered tools stay hidden in except mode
const exceptHiddenStaysHidden: keyof Agent.Tools<typeof exceptModeForTyping> =
  'hidden_for_typing';
const exceptExecutorIsRequired: Has<
  CodeExecutor.Service,
  Agent.Requires<typeof exceptModeForTyping>
> = 'yes';
const exceptNameIsChecked = () =>
  Agent.make({
    name: 'code-mode-except-typo',
    revision: '1',
    instructions: 'Use code mode.',
    toolkit: Toolkit.make(hiddenForTyping),
    // @ts-expect-error an except name must be one of the toolkit's tools
    codeMode: { except: ['not_a_tool'] },
  });
void exceptExecVisible;
void exceptToolVisible;
void exceptHiddenStaysHidden;
void exceptExecutorIsRequired;
void exceptNameIsChecked;

const lookup = Tool.make('lookup', {
  description: 'Look up an order.',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});
const charge = Tool.make('charge', {
  description: 'Charge an order.',
  parameters: Schema.Struct({ amount: Schema.Finite }),
  success: Schema.Struct({ receipt: Schema.String }),
});

describe('code mode broker', () => {
  it.effect('advertises only exec and composes hidden typed tools', () =>
    Effect.gen(function* () {
      const summarize = Tool.make('summarize', {
        description: 'Summarize an order status.',
        parameters: Schema.Struct({ status: Schema.String }),
        success: Schema.String,
      });
      const agent = Agent.make({
        name: 'coder',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(lookup, summarize),
        codeMode: true,
      }).withHandlers({
        lookup: ({ id }) => Effect.succeed({ status: `open:${id}` }),
        summarize: ({ status }) => Effect.succeed(status.toUpperCase()),
      });
      const { executor, model, layer } = rig([
        toolCall('nested-1', 'lookup', { id: '42' }),
        toolCall('nested-2', 'summarize', { status: 'open:42' }),
        { _tag: 'Output', value: 'working' },
        { _tag: 'Completion', state: {}, result: { answer: 42 } },
      ]);

      const result = yield* agent.run('go').pipe(Effect.provide(layer));
      const requests = yield* model.requests;

      expect(result.text).toBe('done');
      expect(requests.map((request) => request.tools)).toEqual([
        ['exec'],
        ['exec'],
      ]);
      // The generated SDK the model writes against is in the exec description.
      const description = requests[0]?.toolDefinitions.find(
        ({ name }) => name === 'exec',
      )?.description;
      expect(description).toContain(
        'Execute erasable TypeScript that composes the hidden tools below.',
      );
      expect(description).toContain(
        'readonly lookup: (input: { readonly id: string }, options?: ToolCallOptions) => Promise<{ readonly status: string }>;',
      );
      expect(description).toContain(
        'readonly summarize: (input: { readonly status: string }, options?: ToolCallOptions) => Promise<string>;',
      );
      expect(description).toContain(
        'declare function text(value: string | JsonValue): void;',
      );
      expect(description).toContain(
        'declare class ToolCallError extends Error',
      );
      expect(description).not.toContain('Node');
      expect(
        (yield* executor.requests)[0]?.tools.map(({ name }) => name),
      ).toEqual(['lookup', 'summarize']);
      expect(yield* executor.responses).toEqual([
        { id: 'nested-1', outcome: 'success', value: { status: 'open:42' } },
        { id: 'nested-2', outcome: 'success', value: 'OPEN:42' },
      ]);
      // Text output and the structured result reach the model separately,
      // and the nested intermediate value does not.
      const prompt = promptOf(requests[1]);
      expect(prompt).toContain('"output":"working"');
      expect(prompt).toContain('"result":{"answer":42}');
      expect(prompt).not.toContain('open:42');
    }),
  );

  it.effect('returns a structured outer execution failure', () =>
    Effect.gen(function* () {
      const agent = Agent.make({
        name: 'structured-code-error',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
      });
      const { model, layer } = rig([
        { _tag: 'Failure', message: 'sandbox failed' },
      ]);

      yield* agent.run('go').pipe(Effect.provide(layer));
      const prompt = promptOf((yield* model.requests)[1]);

      expect(prompt).toContain('"isFailure":true');
      expect(prompt).toContain('"result":{"code":"execution_failed"');
      expect(prompt).toContain('sandbox failed');
    }),
  );

  it.effect(
    'validates nested calls and honors declared failures, policy and approval',
    () =>
      Effect.gen(function* () {
        let handled = 0;
        const reject = Tool.make('reject', {
          parameters: Schema.Struct({}),
          success: Schema.String,
          failure: Schema.Struct({ code: Schema.String }),
          failureMode: 'return',
        });
        const broken = Tool.make('broken', {
          parameters: Schema.Struct({}),
          success: Schema.String,
        });
        const policed = Tool.make('policed', {
          parameters: Schema.Struct({}),
          success: Schema.Struct({ status: Schema.String }),
        });
        const approve = Tool.make('approve', {
          parameters: Schema.Struct({}),
          success: Schema.String,
          needsApproval: true,
        });
        const approvalToolkit = Toolkit.make(approve);
        const dynamic = DynamicToolkit.make(
          approvalToolkit.pipe(
            Effect.provide(
              approvalToolkit.toLayer({
                approve: () =>
                  Effect.sync(() => {
                    handled += 1;
                    return 'approved';
                  }),
              }),
            ),
          ),
        );
        const agent = Agent.make({
          name: 'validated-code',
          revision: '1',
          instructions: 'Use code mode.',
          toolkit: Toolkit.make(lookup, reject, broken, policed),
          dynamicTools: [dynamic],
          codeMode: true,
        })
          .withHandlers({
            lookup: ({ id }) => Effect.succeed({ status: id }),
            reject: () => Effect.fail({ code: 'denied' }),
            broken: () => Effect.sync((): string => JSON.parse('42')),
            policed: () =>
              Effect.sync(() => {
                handled += 1;
                return { status: 'handler' };
              }),
          })
          .intercepting({
            beforeToolCall: ({ name }) =>
              Effect.succeed(
                name === 'policed'
                  ? Interception.answer({ status: 'policy' })
                  : Interception.dispatch,
              ),
          });
        const { executor, layer } = rig([
          toolCall('invalid', 'lookup', { id: 42 }),
          toolCall('declared', 'reject', {}),
          toolCall('result', 'broken', {}),
          toolCall('policy', 'policed', {}),
          toolCall('approval', 'approve', {}),
          done,
        ]);

        yield* agent.run('go').pipe(Effect.provide(layer));
        const responses = yield* executor.responses;
        const byId = (id: string) => responses.find((r) => r.id === id);

        const invalid = byId('invalid');
        if (invalid?.outcome !== 'failure') {
          throw new Error('expected a parameter validation failure');
        }
        expect(invalid.error.code).toBe('tool_failure');
        const parameterError = yield* Schema.decodeUnknownEffect(
          AiError.AiError,
        )(invalid.error.value);
        expect(parameterError.reason).toMatchObject({
          _tag: 'ToolParameterValidationError',
          toolName: 'lookup',
        });
        expect(byId('declared')).toEqual({
          id: 'declared',
          outcome: 'failure',
          error: {
            code: 'tool_failure',
            message: 'Tool "reject" failed',
            value: { code: 'denied' },
          },
        });
        expect(byId('result')).toEqual({
          id: 'result',
          outcome: 'failure',
          error: {
            code: 'dispatch_failed',
            message: expect.stringContaining(
              "Failed to encode result for tool 'broken'",
            ),
          },
        });
        expect(byId('policy')).toEqual({
          id: 'policy',
          outcome: 'success',
          value: { status: 'policy' },
        });
        expect(byId('approval')).toEqual({
          id: 'approval',
          outcome: 'failure',
          error: {
            code: 'approval_required',
            message:
              'Tool "approve" requires provider-mediated approval and cannot run inside code mode; move it to the agent toolkit and add it to codeMode.except',
          },
        });
        expect(handled).toBe(0);
      }).pipe(Effect.scoped),
  );

  it.live('runs concurrent nested calls under the run policy', () =>
    Effect.gen(function* () {
      const bothEntered = yield* Deferred.make<void>();
      let entered = 0;
      const work = Tool.make('work', {
        parameters: Schema.Struct({ id: Schema.String }),
        success: Schema.String,
      });
      const agent = Agent.make({
        name: 'concurrent-code',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(work),
        codeMode: true,
        runPolicy: { maxToolConcurrency: 2 },
      }).withHandlers({
        work: ({ id }) =>
          Effect.gen(function* () {
            entered += 1;
            if (entered === 2) {
              yield* Deferred.succeed(bothEntered, undefined);
            }
            // Each call only returns once the other has entered.
            yield* Deferred.await(bothEntered);
            return id;
          }),
      });
      const { layer } = rig([
        toolCall('nested-1', 'work', { id: '1' }),
        toolCall('nested-2', 'work', { id: '2' }),
        done,
      ]);

      yield* agent
        .run('go')
        .pipe(Effect.provide(layer), Effect.timeout('500 millis'));
      expect(entered).toBe(2);
    }),
  );

  it.live('serializes outer exec calls that share scratch state', () => {
    let active = 0;
    let maximum = 0;
    const executorLayer = Layer.succeed(CodeExecutor.Service, {
      start: () =>
        Effect.sync(() => {
          active += 1;
          maximum = Math.max(maximum, active);
          return {
            events: Stream.fromEffect(
              Effect.sleep('50 millis').pipe(
                Effect.as({ _tag: 'Completion' as const, state: {} }),
                Effect.ensuring(
                  Effect.sync(() => {
                    active -= 1;
                  }),
                ),
              ),
            ),
            respond: () => Effect.void,
            interrupt: Effect.void,
          };
        }),
    });
    const model = ScriptedModel.make([
      [
        {
          type: 'tool-call',
          id: 'exec-sequential-1',
          name: 'exec',
          params: { source: 'first' },
        },
        {
          type: 'tool-call',
          id: 'exec-sequential-2',
          name: 'exec',
          params: { source: 'second' },
        },
        finish('tool-calls'),
      ],
      answeringTurn,
    ]);

    return Effect.gen(function* () {
      const agent = Agent.make({
        name: 'sequential-code',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
        concurrency: 'unbounded',
      });
      yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executorLayer)));
      expect(maximum).toBe(1);
    });
  });

  it.effect(
    'keeps excepted tools beside exec and dispatches them directly',
    () =>
      Effect.gen(function* () {
        const release = Tool.make('release', {
          description: 'Release a build.',
          parameters: Schema.Struct({ id: Schema.String }),
          success: Schema.String,
        });
        const agent = Agent.make({
          name: 'except-coder',
          revision: '1',
          instructions: 'Use code mode, release directly.',
          toolkit: Toolkit.make(lookup, release),
          codeMode: { except: ['release'] },
        }).withHandlers({
          lookup: ({ id }) => Effect.succeed({ status: `open:${id}` }),
          release: ({ id }) => Effect.succeed(`released:${id}`),
        });
        const { executor, model, layer } = rig(
          [toolCall('nested-lookup', 'lookup', { id: '42' }), done],
          [
            callTurn('release', { id: 'r1' }, 'direct-release'),
            execTurn('nested lookup', 'exec-lookup'),
            answeringTurn,
          ],
        );

        const result = yield* agent.run('go').pipe(Effect.provide(layer));
        const requests = yield* model.requests;

        expect(result.text).toBe('done');
        expect(
          [...(requests[0]?.tools ?? [])].sort((a, b) => a.localeCompare(b)),
        ).toEqual(['exec', 'release']);
        expect(
          (yield* executor.requests)[0]?.tools.map(({ name }) => name),
        ).toEqual(['lookup']);
        expect(promptOf(requests[1])).toContain('released:r1');
      }),
  );

  it.effect('suspends an excepted approval tool durably and resumes it', () =>
    Effect.gen(function* () {
      let released = 0;
      const release = Tool.make('release', {
        parameters: Schema.Struct({ id: Schema.String }),
        success: Schema.String,
        needsApproval: true,
      });
      const agent = Agent.make({
        name: 'except-approval',
        revision: '1',
        instructions: 'Use code mode, release with approval.',
        toolkit: Toolkit.make(release),
        codeMode: { except: ['release'] },
      }).withHandlers({
        release: ({ id }) =>
          Effect.sync(() => {
            released += 1;
            return `released:${id}`;
          }),
      });
      const conversation = Conversation.make(
        agent,
        LogVocabulary.ConversationId.make('except-approval-conversation'),
      );
      const { layer } = rig(
        [],
        [callTurn('release', { id: 'r1' }, 'release-call'), answeringTurn],
      );

      const suspended = yield* conversation
        .run('release r1')
        .pipe(Effect.provide(layer));
      if (suspended.outcome !== 'suspended') {
        throw new Error('expected the approval-gated tool to suspend');
      }
      expect(suspended.pendingInteractions).toEqual([
        {
          toolCallId: 'release-call',
          toolName: 'release',
          kind: 'approval',
          request: { id: 'r1' },
        },
      ]);
      expect(released).toBe(0);

      yield* conversation.resolveApproval('release-call', 'approve');
      const resolved = yield* conversation.run().pipe(Effect.provide(layer));

      expect(resolved.outcome).toBe('success');
      expect(released).toBe(1);
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it('rejects invalid code mode definitions at construction', () => {
    const approve = Tool.make('approve', {
      parameters: Schema.Struct({}),
      success: Schema.String,
      needsApproval: true,
    });
    const exec = Tool.make('exec', {
      parameters: Schema.Struct({}),
      success: Schema.String,
    });
    const base = { revision: '1', instructions: 'Use code mode.' };

    expect(() =>
      Agent.make({
        ...base,
        name: 'approval-code',
        toolkit: Toolkit.make(approve),
        codeMode: true,
      }),
    ).toThrow(
      'Agent codeMode brokers approval-gated tool "approve"; add it to codeMode.except',
    );
    expect(() =>
      Agent.make({
        ...base,
        name: 'except-typo',
        toolkit: Toolkit.make(lookup),
        // The compile-time key check only covers literal arrays; a
        // dynamically built list degrades to this runtime rejection.
        codeMode: { except: ['nope' as 'lookup'] },
      }),
    ).toThrow('codeMode excepts tool "nope"');
    expect(() =>
      Agent.make({
        ...base,
        name: 'exec-collision',
        toolkit: Toolkit.make(exec),
        codeMode: true,
      }),
    ).toThrow('generated tool "exec"');
  });
});

describe('code mode scratch state', () => {
  /** A conversation whose turns each get a fresh scripted model and executor. */
  const scratch = (
    name: string,
    options?: Parameters<typeof Conversation.make>[2],
  ) => {
    const conversation = Conversation.make(
      Agent.make({
        name,
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
      }),
      `${name}-conversation`,
      options ?? {},
    );
    const turn = (events: ReadonlyArray<CodeExecutor.Event>, prompt = 'go') => {
      const turnRig = rig(events);
      return conversation
        .run(prompt)
        .pipe(Effect.provide(turnRig.layer), Effect.as(turnRig));
    };
    return { conversation, turn };
  };

  it.effect(
    'restores checkpoints without prompting them and drops failed writes',
    () =>
      Effect.gen(function* () {
        const { conversation, turn: runTurn } = scratch('stateful-code');

        const first = yield* runTurn(
          [
            { _tag: 'Output', value: 'first' },
            { _tag: 'Completion', state: { secret: 'remembered' } },
          ],
          'first',
        );
        const second = yield* runTurn(
          [{ _tag: 'Completion', state: { secret: 'remembered' } }],
          'second',
        );
        // A failed execution's earlier stores are not committed.
        yield* runTurn(
          [
            { _tag: 'Completion', state: { leaked: 'no' } },
            { _tag: 'Failure', message: 'execution failed' },
          ],
          'fail',
        );
        const after = yield* runTurn([done], 'after');

        expect((yield* first.executor.requests)[0]?.state).toEqual({});
        expect((yield* second.executor.requests)[0]?.state).toEqual({
          secret: 'remembered',
        });
        expect((yield* after.executor.requests)[0]?.state).toEqual({
          secret: 'remembered',
        });
        expect(
          (yield* recordsOf(conversation)).some(
            (record) => record._tag === 'CodeStateCheckpoint',
          ),
        ).toBe(true);
        for (const r of [first, second, after]) {
          for (const request of yield* r.model.requests) {
            expect(promptOf(request)).not.toContain('remembered');
          }
        }
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('rejects scratch state over the per-value and total limits', () =>
    Effect.gen(function* () {
      const cases = [
        ['per-value', { oversized: 'x'.repeat(64 * 1024 + 1) }, '65536'],
        [
          'total',
          Object.fromEntries(
            Array.from({ length: 5 }, (_, i) => [
              `v${String(i)}`,
              'x'.repeat(60 * 1024),
            ]),
          ),
          '262144',
        ],
      ] as const;
      for (const [name, state, limit] of cases) {
        const { conversation, turn } = scratch(`bounded-${name}`);
        const r = yield* turn([{ _tag: 'Completion', state }]);

        expect(promptOf((yield* r.model.requests)[1])).toContain(
          `exceeds ${limit} bytes`,
        );
        expect(
          (yield* recordsOf(conversation)).some(
            (record) => record._tag === 'CodeStateCheckpoint',
          ),
        ).toBe(false);
      }
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect(
    'applies the recording policy before persisting scratch state',
    () =>
      Effect.gen(function* () {
        const { conversation, turn: run } = scratch('redacted-code-state', {
          codeState: () => Effect.succeed({ token: '[redacted]' }),
        });

        yield* run([{ _tag: 'Completion', state: { token: 'raw-secret' } }]);
        const second = yield* run([done]);

        expect((yield* second.executor.requests)[0]?.state).toEqual({
          token: '[redacted]',
        });
        expect(JSON.stringify(yield* recordsOf(conversation))).not.toContain(
          'raw-secret',
        );
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );
});

// Nested calls made by a script are recorded durably, keyed by the parent
// `exec` call and a deterministic key, and served from the log when the same
// `exec` call is entered again after a crash.
interface Counts {
  lookup: number;
  charge: number;
  intercepted: number;
}
const newCounts = (): Counts => ({ lookup: 0, charge: 0, intercepted: 0 });

describe('code mode durable nested calls', () => {
  const agentWith = (counts: Counts) =>
    Agent.make({
      name: 'nested',
      revision: '1',
      instructions: 'Use code mode.',
      toolkit: Toolkit.make(lookup, charge),
      codeMode: true,
    }).withHandlers({
      lookup: ({ id }) =>
        Effect.sync(() => {
          counts.lookup += 1;
          return { status: `fresh:${id}` };
        }),
      charge: ({ amount }) =>
        Effect.sync(() => {
          counts.charge += 1;
          return { receipt: `fresh:${String(amount)}` };
        }),
    });
  const script = (...events: ReadonlyArray<CodeExecutor.Event>) =>
    [...events, { _tag: 'Output', value: 'ok' }, done] as const;
  const retry = {
    onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
  };

  const CONVERSATION = LogVocabulary.ConversationId.make('nested-conversation');
  const PATH = AgentLog.pathFor(CONVERSATION);
  const EXEC = LogVocabulary.ToolCallId.make('exec-1');

  /** Write an earlier, crashed run's records straight into the conversation. */
  const seed = Effect.fn('test.seed')(function* (
    records: ReadonlyArray<ConversationRecord.Record>,
  ) {
    const store = yield* LogStore.Service;
    yield* store.create(PATH, CONVERSATION).pipe(Effect.orDie);
    const claim = yield* store
      .acquire(PATH, LogVocabulary.ProducerId.make('previous-run'))
      .pipe(Effect.orDie);
    yield* store
      .append({
        path: PATH,
        producerId: claim.producerId,
        epoch: claim.epoch,
        sequence: claim.nextSequence,
        records: records.map((record) => ({
          conversationId: CONVERSATION,
          timestamp: 1_700_000_000_000,
          record,
        })),
      })
      .pipe(Effect.orDie);
  });

  const crashedExec: ReadonlyArray<ConversationRecord.Record> = [
    {
      _tag: 'RunStarted',
      budget: testBudget,
      agent: 'nested',
      formatVersion: 1,
      agentRevision: LogVocabulary.AgentRevision.make('1'),
      prompt: [],
    },
    {
      _tag: 'StepOpened',
      step: 1,
      id: EXEC,
      name: 'exec',
      inputDigest: digestOf('exec', { source: 'compose' }),
      params: { source: 'compose' },
    },
    { _tag: 'StepStarted', id: EXEC, name: 'exec', replay: 'unsafe' },
  ];

  /** The persisted records of one nested call, up to the given stage. */
  const nested = (
    key: string,
    name: string,
    input: CodeExecutor.JsonValue,
    stage: 'called' | 'started' | 'settled',
    result?: unknown,
    replay: 'safe' | 'unsafe' = 'unsafe',
  ): ReadonlyArray<ConversationRecord.Record> => {
    const id = LogVocabulary.ToolCallId.make(key);
    return [
      {
        _tag: 'StepOpened',
        id,
        parent: EXEC,
        name,
        inputDigest: digestOf(name, input),
        params: input,
      },
      ...(stage === 'called'
        ? []
        : [
            {
              _tag: 'StepStarted',
              id,
              parent: EXEC,
              name,
              replay,
            } as const,
          ]),
      ...(stage === 'settled'
        ? [
            {
              _tag: 'StepSettled',
              id,
              parent: EXEC,
              name,
              outcome: 'success',
              result,
            } as const,
          ]
        : []),
    ];
  };

  it.effect('records nested calls durably without touching the prompt', () =>
    Effect.gen(function* () {
      const events = script(
        toolCall('n1', 'lookup', { id: '42' }),
        toolCall('n2', 'charge', { amount: 5 }, 'charge-once'),
      );
      const counts = newCounts();
      const durable = rig(events);
      const conversation = Conversation.make(agentWith(counts), CONVERSATION);
      const plain = rig(events);

      yield* conversation.run('go').pipe(Effect.provide(durable.layer));
      yield* agentWith(newCounts()).run('go').pipe(Effect.provide(plain.layer));
      const written = (yield* recordsOf(conversation)).filter(
        (record) => 'parent' in record && record.parent !== undefined,
      );

      expect(
        ['StepOpened', 'StepStarted', 'StepSettled'].map(
          (tag) => written.filter((record) => record._tag === tag).length,
        ),
      ).toEqual([2, 2, 2]);
      expect(
        written.every((record) => 'parent' in record && record.parent === EXEC),
      ).toBe(true);
      expect(
        written.flatMap((record) =>
          record._tag === 'StepSettled'
            ? [[record.id, record.name, record.result]]
            : [],
        ),
      ).toEqual(
        expect.arrayContaining([
          ['#1', 'lookup', { status: 'fresh:42' }],
          ['k:charge-once', 'charge', { receipt: 'fresh:5' }],
        ]),
      );
      expect(counts).toEqual({ lookup: 1, charge: 1, intercepted: 0 });
      // Recording is invisible to the model.
      const prompts = (rigged: typeof durable) =>
        rigged.model.requests.pipe(
          Effect.map((requests) => requests.map(promptOf)),
        );
      expect(yield* prompts(durable)).toEqual(yield* prompts(plain));
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect(
    'serves settled calls from the log and fails an unsettled one closed on Retry',
    () =>
      Effect.gen(function* () {
        const counts = newCounts();
        yield* seed([
          ...crashedExec,
          ...nested('#1', 'lookup', { id: '42' }, 'settled', {
            status: 'from-log',
          }),
          ...nested('#2', 'charge', { amount: 5 }, 'started'),
          ...nested('#3', 'lookup', { id: '43' }, 'called'),
          ...nested('k:once', 'charge', { amount: 6 }, 'settled', {
            receipt: 'keyed-from-log',
          }),
        ]);
        const r = rig(
          script(
            toolCall('n1', 'lookup', { id: '42' }),
            toolCall('n2', 'charge', { amount: 5 }),
            toolCall('n3', 'lookup', { id: '43' }),
            toolCall('n4', 'lookup', { id: '44' }),
            toolCall('n5', 'charge', { amount: 6 }, 'once'),
          ),
          [answeringTurn],
        );
        const conversation = Conversation.make(
          agentWith(counts).intercepting({
            ...retry,
            beforeToolCall: () => {
              counts.intercepted += 1;
              return Effect.succeed(Interception.dispatch);
            },
          }),
          CONVERSATION,
        );

        const result = yield* conversation
          .run('go')
          .pipe(Effect.provide(r.layer));
        const responses = yield* r.executor.responses;
        const byId = (id: string) =>
          responses.find((response) => response.id === id);
        const written = yield* recordsOf(conversation);

        expect(result.outcome).toBe('success');
        // Only calls the earlier run never executed did real work (and were
        // intercepted); served and refused calls bypass both.
        expect(counts).toEqual({ lookup: 2, charge: 0, intercepted: 2 });
        expect(byId('n1')).toEqual({
          id: 'n1',
          outcome: 'success',
          value: { status: 'from-log' },
        });
        expect(byId('n2')).toMatchObject({
          outcome: 'failure',
          error: { code: 'indeterminate' },
        });
        expect(byId('n3')).toMatchObject({
          outcome: 'success',
          value: { status: 'fresh:43' },
        });
        expect(byId('n4')).toMatchObject({
          outcome: 'success',
          value: { status: 'fresh:44' },
        });
        expect(byId('n5')).toEqual({
          id: 'n5',
          outcome: 'success',
          value: { receipt: 'keyed-from-log' },
        });
        expect(
          written.some(
            (record) => record._tag === 'StepSettled' && record.id === EXEC,
          ),
        ).toBe(true);
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  // A script can finish and commit its scratch state, and the process die
  // before `exec` settles. The retried execution must start from the state
  // the crashed one started from, not from what it went on to commit.
  it.effect('retries an exec from the state before its unsettled commit', () =>
    Effect.gen(function* () {
      yield* seed([
        ...crashedExec,
        {
          _tag: 'CodeStateCheckpoint',
          producer: EXEC,
          state: { charged: true },
        },
      ]);
      const r = rig(script(), [answeringTurn]);
      const conversation = Conversation.make(
        agentWith(newCounts()).intercepting(retry),
        CONVERSATION,
      );

      yield* conversation.run('go').pipe(Effect.provide(r.layer));

      expect((yield* r.executor.requests)[0]?.state).toEqual({});
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  // Recovery's rule for a top-level call, at the sandbox boundary: a start
  // recorded replay-safe may run again, because running it twice was declared
  // to leave the world as running it once.
  it.effect('runs a replay-safe nested call that never settled again', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      yield* seed([
        ...crashedExec,
        ...nested('#1', 'lookup', { id: '42' }, 'started', undefined, 'safe'),
      ]);
      const r = rig(script(toolCall('n1', 'lookup', { id: '42' })), [
        answeringTurn,
      ]);
      const conversation = Conversation.make(
        agentWith(counts).intercepting(retry),
        CONVERSATION,
      );

      yield* conversation.run('go').pipe(Effect.provide(r.layer));
      const responses = yield* r.executor.responses;
      const written = yield* recordsOf(conversation);

      expect(counts.lookup).toBe(1);
      expect(responses.find((response) => response.id === 'n1')).toMatchObject({
        outcome: 'success',
        value: { status: 'fresh:42' },
      });
      // The same step, settled: not a second call under the same key.
      const steps = written.filter(
        (record) => 'parent' in record && record.parent === EXEC,
      );
      expect(steps.map((record) => record._tag)).toEqual([
        'StepOpened',
        'StepStarted',
        'StepStarted',
        'StepSettled',
      ]);
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect(
    'refuses to serve a recorded call that a drifted script remakes',
    () =>
      Effect.gen(function* () {
        const counts = newCounts();
        yield* seed([
          ...crashedExec,
          ...nested('#1', 'lookup', { id: '42' }, 'settled', {
            status: 'from-log',
          }),
        ]);
        const r = rig(script(toolCall('n1', 'lookup', { id: 'other' })), [
          answeringTurn,
        ]);

        yield* Conversation.make(
          agentWith(counts).intercepting(retry),
          CONVERSATION,
        )
          .run('go')
          .pipe(Effect.provide(r.layer));

        expect(counts.lookup).toBe(0);
        expect((yield* r.executor.responses)[0]).toMatchObject({
          outcome: 'failure',
          error: { code: 'dispatch_failed' },
        });
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('rejects a key reused within one execution', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      const r = rig(
        script(
          toolCall('n1', 'lookup', { id: '1' }, 'same'),
          toolCall('n2', 'lookup', { id: '2' }, 'same'),
        ),
      );

      yield* Conversation.make(agentWith(counts), CONVERSATION)
        .run('go')
        .pipe(Effect.provide(r.layer));

      expect(counts.lookup).toBe(1);
      expect(
        (yield* r.executor.responses).filter(
          ({ outcome }) => outcome === 'failure',
        ),
      ).toHaveLength(1);
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('runs nested calls as before when the run is not recorded', () =>
    Effect.gen(function* () {
      const counts = newCounts();
      const r = rig(
        script(
          toolCall('n1', 'lookup', { id: '42' }, 'explicit'),
          toolCall('n2', 'charge', { amount: 5 }),
        ),
      );

      const result = yield* agentWith(counts)
        .run('go')
        .pipe(Effect.provide(r.layer));

      expect(result.text).toBe('done');
      expect(counts).toEqual({ lookup: 1, charge: 1, intercepted: 0 });
      expect(yield* r.executor.responses).toHaveLength(2);
    }),
  );
});

describe('code mode with the real sandbox executor', () => {
  it.effect('runs a model-written script end to end across turns', () =>
    Effect.gen(function* () {
      const agent = Agent.make({
        name: 'real-sandbox',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(lookup),
        codeMode: true,
      }).withHandlers({
        lookup: ({ id }) => Effect.succeed({ status: `open:${id}` }),
      });
      const conversation = Conversation.make(
        agent,
        'real-sandbox-conversation',
      );
      const sources = [
        'const r = await tools.lookup({ id: "42" }); store("seen", r.status); return { status: r.status }',
        'text("restored:" + load("seen"))',
      ];
      const models = sources.map((source, index) =>
        ScriptedModel.make([
          execTurn(source, `exec-real-${String(index)}`),
          answeringTurn,
        ]),
      );

      for (const [index, model] of models.entries()) {
        yield* conversation
          .run(`turn ${String(index)}`)
          .pipe(
            Effect.provide(Layer.merge(model.layer, NodeCodeExecutor.layer())),
          );
      }
      const [first = [], second = []] = yield* Effect.all(
        models.map((m) => m.requests),
      );

      expect(
        first.at(0)?.toolDefinitions.find(({ name }) => name === 'exec')
          ?.description,
      ).toContain('readonly lookup: (input: { readonly id: string }');
      expect(promptOf(first[1])).toContain('"result":{"status":"open:42"}');
      expect(promptOf(second[1])).toContain('restored:open:42');
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );
});
