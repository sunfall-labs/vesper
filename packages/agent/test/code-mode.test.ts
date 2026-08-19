import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Layer, Schema, Stream } from 'effect';
import { type Response, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { CodeExecutor } from '../src/code-executor.js';
import { Conversation } from '../src/conversation.js';
import { DynamicToolkit } from '../src/dynamic-toolkit.js';
import { Interception } from '../src/interception.js';
import { ScriptedModel } from '../src/testing.js';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const answeringTurn: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'done' },
  { type: 'text-end', id: 'answer' },
  finish(),
];

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

const logLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

describe('code mode executor protocol', () => {
  it.effect(
    'provides a deterministic executor session for code-mode runs',
    () =>
      Effect.gen(function* () {
        const fake = CodeExecutor.fake([
          {
            _tag: 'ToolCall',
            id: 'nested-1',
            name: 'lookup',
            input: { id: '42' },
          },
          { _tag: 'Output', value: 'order:42' },
          { _tag: 'Completion', state: { remembered: '42' } },
        ]);
        const request: CodeExecutor.Request = {
          source: 'text(await tools.lookup({ id: "42" }))',
          tools: [
            {
              name: 'lookup',
              description: 'Look up an order.',
              parameters: { type: 'object' },
              result: { type: 'object' },
            },
          ],
          state: {},
          limits: CodeExecutor.defaultLimits,
        };

        const observed = yield* Effect.gen(function* () {
          const executor = yield* CodeExecutor.Service;
          const session = yield* executor.start(request);
          yield* session.respond({
            id: 'nested-1',
            outcome: 'success',
            value: { status: 'open' },
          });
          return yield* Stream.runCollect(session.events);
        }).pipe(Effect.provide(fake.layer));

        expect(Array.from(observed)).toEqual([
          {
            _tag: 'ToolCall',
            id: 'nested-1',
            name: 'lookup',
            input: { id: '42' },
          },
          { _tag: 'Output', value: 'order:42' },
          { _tag: 'Completion', state: { remembered: '42' } },
        ]);
        expect(yield* fake.requests).toEqual([request]);
        expect(yield* fake.responses).toEqual([
          {
            id: 'nested-1',
            outcome: 'success',
            value: { status: 'open' },
          },
        ]);
      }),
  );

  it('accepts code mode in an agent definition', () => {
    expect(() =>
      Agent.make({
        name: 'coder',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
      }),
    ).not.toThrow();
  });
});

describe('code mode tool broker', () => {
  it.effect('returns text output and a structured result separately', () =>
    Effect.gen(function* () {
      const agent = Agent.make({
        name: 'structured-code-result',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
      }).withHandlers({});
      const executor = CodeExecutor.fake([
        { _tag: 'Output', value: 'working' },
        { _tag: 'Completion', state: {}, result: { answer: 42 } },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-structured-result',
            name: 'exec',
            params: { source: 'return { answer: 42 }' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);

      yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const requests = yield* model.requests;
      const prompt = JSON.stringify(requests[1]?.prompt);

      expect(prompt).toContain('"output":"working"');
      expect(prompt).toContain('"result":{"answer":42}');
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
      const executor = CodeExecutor.fake([
        { _tag: 'Failure', message: 'sandbox failed' },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-structured-error',
            name: 'exec',
            params: { source: 'throw new Error("no")' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);

      yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const requests = yield* model.requests;
      const prompt = JSON.stringify(requests[1]?.prompt);

      expect(prompt).toContain('"isFailure":true');
      expect(prompt).toContain('"result":{"code":"execution_failed"');
      expect(prompt).toContain('sandbox failed');
    }),
  );

  it.effect('advertises only exec and composes hidden typed tools', () =>
    Effect.gen(function* () {
      const lookup = Tool.make('lookup', {
        description: 'Look up an order.',
        parameters: Schema.Struct({ id: Schema.String }),
        success: Schema.Struct({ status: Schema.String }),
      });
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
      const executor = CodeExecutor.fake([
        {
          _tag: 'ToolCall',
          id: 'nested-1',
          name: 'lookup',
          input: { id: '42' },
        },
        {
          _tag: 'ToolCall',
          id: 'nested-2',
          name: 'summarize',
          input: { status: 'open:42' },
        },
        { _tag: 'Output', value: 'OPEN:42' },
        { _tag: 'Completion', state: {} },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-1',
            name: 'exec',
            params: { source: 'compose hidden tools' },
          },
          finish('tool-calls'),
        ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
        [
          { type: 'text-start', id: 'answer' },
          { type: 'text-delta', id: 'answer', delta: 'done' },
          { type: 'text-end', id: 'answer' },
          finish(),
        ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
      ]);

      const result = yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const requests = yield* model.requests;
      const executions = yield* executor.requests;

      expect(result.text).toBe('done');
      expect(requests.map((request) => request.tools)).toEqual([
        ['exec'],
        ['exec'],
      ]);
      const execDescription = requests[0]?.toolDefinitions.find(
        ({ name }) => name === 'exec',
      )?.description;
      expect(execDescription).toContain(
        'Execute erasable TypeScript that composes the hidden tools below.',
      );
      expect(execDescription).toContain(
        'readonly lookup: (input: { readonly id: string }) => Promise<{ readonly status: string }>;',
      );
      expect(execDescription).toContain(
        'readonly summarize: (input: { readonly status: string }) => Promise<string>;',
      );
      expect(execDescription).toContain(
        'declare function text(value: string | JsonValue): void;',
      );
      expect(execDescription).toContain(
        'declare class ToolCallError extends Error',
      );
      expect(execDescription).not.toContain('Node');
      expect(executions[0]?.tools.map((tool) => tool.name)).toEqual([
        'lookup',
        'summarize',
      ]);
      expect(yield* executor.responses).toEqual([
        {
          id: 'nested-1',
          outcome: 'success',
          value: { status: 'open:42' },
        },
        { id: 'nested-2', outcome: 'success', value: 'OPEN:42' },
      ]);
      expect(JSON.stringify(requests[1]?.prompt)).toContain('OPEN:42');
      expect(JSON.stringify(requests[1]?.prompt)).not.toContain('open:42');
    }),
  );

  it.effect('preserves a hidden tool declared failure as structured data', () =>
    Effect.gen(function* () {
      const reject = Tool.make('reject', {
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.Struct({ code: Schema.String }),
        failureMode: 'return',
      });
      const agent = Agent.make({
        name: 'structured-code-failure',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(reject),
        codeMode: true,
      }).withHandlers({
        reject: () => Effect.fail({ code: 'denied' }),
      });
      const executor = CodeExecutor.fake([
        { _tag: 'ToolCall', id: 'nested-reject', name: 'reject', input: {} },
        { _tag: 'Completion', state: {} },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-reject',
            name: 'exec',
            params: { source: 'await tools.reject({})' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);

      yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

      expect(yield* executor.responses).toEqual([
        {
          id: 'nested-reject',
          outcome: 'failure',
          error: {
            code: 'tool_failure',
            message: 'Tool "reject" failed',
            value: { code: 'denied' },
          },
        },
      ]);
    }),
  );

  it.live('lets the run policy govern concurrent nested calls', () =>
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
            yield* Deferred.await(bothEntered);
            return id;
          }),
      });
      const executor = CodeExecutor.fake([
        { _tag: 'ToolCall', id: 'nested-1', name: 'work', input: { id: '1' } },
        { _tag: 'ToolCall', id: 'nested-2', name: 'work', input: { id: '2' } },
        { _tag: 'Completion', state: {} },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-concurrent',
            name: 'exec',
            params: { source: 'await Promise.all([call1, call2])' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);

      yield* agent
        .run('go')
        .pipe(
          Effect.provide(Layer.merge(model.layer, executor.layer)),
          Effect.timeout('500 millis'),
        );
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

    return Effect.gen(function* () {
      const agent = Agent.make({
        name: 'sequential-code',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
        concurrency: 'unbounded',
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

      yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executorLayer)));
      expect(maximum).toBe(1);
    });
  });

  it.effect(
    'rejects invalid nested tool parameters at the broker boundary',
    () =>
      Effect.gen(function* () {
        const lookup = Tool.make('lookup', {
          parameters: Schema.Struct({ id: Schema.String }),
          success: Schema.String,
        });
        const agent = Agent.make({
          name: 'validated-code',
          revision: '1',
          instructions: 'Use code mode.',
          toolkit: Toolkit.make(lookup),
          codeMode: true,
        }).withHandlers({ lookup: ({ id }) => Effect.succeed(id) });
        const executor = CodeExecutor.fake([
          {
            _tag: 'ToolCall',
            id: 'nested-invalid',
            name: 'lookup',
            input: { id: 42 },
          },
          { _tag: 'Completion', state: {} },
        ]);
        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call',
              id: 'exec-invalid',
              name: 'exec',
              params: { source: 'invalid call' },
            },
            finish('tool-calls'),
          ],
          answeringTurn,
        ]);

        yield* agent
          .run('go')
          .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

        expect(yield* executor.responses).toEqual([
          {
            id: 'nested-invalid',
            outcome: 'failure',
            error: {
              code: 'dispatch_failed',
              message: expect.stringContaining(
                "Invalid parameters for tool 'lookup'",
              ),
            },
          },
        ]);
      }),
  );

  it.effect('rejects invalid nested tool results at the broker boundary', () =>
    Effect.gen(function* () {
      const broken = Tool.make('broken', {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });
      const agent = Agent.make({
        name: 'result-validated-code',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(broken),
        codeMode: true,
      }).withHandlers({
        broken: () => Effect.sync((): string => JSON.parse('42')),
      });
      const executor = CodeExecutor.fake([
        {
          _tag: 'ToolCall',
          id: 'nested-invalid-result',
          name: 'broken',
          input: {},
        },
        { _tag: 'Completion', state: {} },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-invalid-result',
            name: 'exec',
            params: { source: 'invalid result' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);

      yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

      expect(yield* executor.responses).toEqual([
        {
          id: 'nested-invalid-result',
          outcome: 'failure',
          error: {
            code: 'dispatch_failed',
            message: expect.stringContaining(
              "Failed to encode result for tool 'broken'",
            ),
          },
        },
      ]);
    }),
  );

  it.effect('applies beforeToolCall policy to hidden nested calls', () =>
    Effect.gen(function* () {
      let handled = 0;
      const lookup = Tool.make('lookup', {
        parameters: Schema.Struct({ id: Schema.String }),
        success: Schema.Struct({ status: Schema.String }),
      });
      const agent = Agent.make({
        name: 'intercepted-code',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(lookup),
        codeMode: true,
      })
        .withHandlers({
          lookup: () =>
            Effect.sync(() => {
              handled += 1;
              return { status: 'handler' };
            }),
        })
        .intercepting({
          beforeToolCall: ({ name }) =>
            Effect.succeed(
              name === 'lookup'
                ? Interception.answer({ status: 'policy' })
                : Interception.dispatch,
            ),
        });
      const executor = CodeExecutor.fake([
        {
          _tag: 'ToolCall',
          id: 'nested-policy',
          name: 'lookup',
          input: { id: '42' },
        },
        { _tag: 'Output', value: 'policy' },
        { _tag: 'Completion', state: {} },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-policy',
            name: 'exec',
            params: { source: 'policy call' },
          },
          finish('tool-calls'),
        ],
        [
          { type: 'text-start', id: 'answer' },
          { type: 'text-delta', id: 'answer', delta: 'done' },
          { type: 'text-end', id: 'answer' },
          finish(),
        ],
      ]);

      yield* agent
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

      expect(handled).toBe(0);
      expect(yield* executor.responses).toEqual([
        {
          id: 'nested-policy',
          outcome: 'success',
          value: { status: 'policy' },
        },
      ]);
    }),
  );

  it('requires approval-gated tools to be excepted from code mode', () => {
    const approve = Tool.make('approve', {
      parameters: Schema.Struct({}),
      success: Schema.String,
      needsApproval: true,
    });

    expect(() =>
      Agent.make({
        name: 'approval-code',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(approve),
        codeMode: true,
      }),
    ).toThrow(
      'Agent codeMode brokers approval-gated tool "approve"; add it to codeMode.except',
    );
  });

  it.effect(
    'returns approval_required for a dynamically resolved approval tool',
    () =>
      Effect.gen(function* () {
        let handled = 0;
        const approve = Tool.make('approve', {
          parameters: Schema.Struct({}),
          success: Schema.String,
          needsApproval: true,
        });
        const dynamic = Toolkit.make(approve);
        const source = DynamicToolkit.make(
          dynamic.pipe(
            Effect.provide(
              dynamic.toLayer({
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
          name: 'dynamic-approval-code',
          revision: '1',
          instructions: 'Use code mode.',
          toolkit: Toolkit.make(),
          dynamicTools: [source],
          codeMode: true,
        });
        const executor = CodeExecutor.fake([
          {
            _tag: 'ToolCall',
            id: 'nested-approval',
            name: 'approve',
            input: {},
          },
          { _tag: 'Completion', state: {} },
        ]);
        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call',
              id: 'exec-approval',
              name: 'exec',
              params: { source: 'await tools.approve({})' },
            },
            finish('tool-calls'),
          ],
          answeringTurn,
        ]);

        yield* agent
          .run('go')
          .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

        expect(yield* executor.responses).toEqual([
          {
            id: 'nested-approval',
            outcome: 'failure',
            error: {
              code: 'approval_required',
              message:
                'Tool "approve" requires provider-mediated approval and cannot run inside code mode; move it to the agent toolkit and add it to codeMode.except',
            },
          },
        ]);
        expect(handled).toBe(0);
      }).pipe(Effect.scoped),
  );

  it.effect(
    'excepted tools stay advertised beside exec and dispatch directly',
    () =>
      Effect.gen(function* () {
        const lookup = Tool.make('lookup', {
          description: 'Look up an order.',
          parameters: Schema.Struct({ id: Schema.String }),
          success: Schema.Struct({ status: Schema.String }),
        });
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
        const executor = CodeExecutor.fake([
          {
            _tag: 'ToolCall',
            id: 'nested-lookup',
            name: 'lookup',
            input: { id: '42' },
          },
          { _tag: 'Output', value: 'open:42' },
          { _tag: 'Completion', state: {} },
        ]);
        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call',
              id: 'direct-release',
              name: 'release',
              params: { id: 'r1' },
            },
            finish('tool-calls'),
          ],
          [
            {
              type: 'tool-call',
              id: 'exec-lookup',
              name: 'exec',
              params: { source: 'nested lookup' },
            },
            finish('tool-calls'),
          ],
          answeringTurn,
        ]);

        const result = yield* agent
          .run('go')
          .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
        const requests = yield* model.requests;
        const executions = yield* executor.requests;

        expect(result.text).toBe('done');
        // Both halves advertised on every request, nothing else.
        expect(
          [...(requests[0]?.tools ?? [])].sort((a, b) => a.localeCompare(b)),
        ).toEqual(['exec', 'release']);
        // The broker catalogs only the brokered half.
        expect(executions[0]?.tools.map((tool) => tool.name)).toEqual([
          'lookup',
        ]);
        // The direct call's result reached the model without the executor.
        expect(JSON.stringify(requests[1]?.prompt)).toContain('released:r1');
      }),
  );

  it.effect(
    'an excepted approval tool suspends durably and resolves in code mode',
    () =>
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
        const executor = CodeExecutor.fake([]);
        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call',
              id: 'release-call',
              name: 'release',
              params: { id: 'r1' },
            },
            finish('tool-calls'),
          ],
          answeringTurn,
        ]);

        const suspended = yield* conversation
          .run('release r1')
          .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
        expect(suspended.outcome).toBe('suspended');
        expect(suspended.pendingApprovals).toEqual([
          {
            toolCallId: 'release-call',
            toolName: 'release',
            input: { id: 'r1' },
          },
        ]);
        expect(released).toBe(0);

        yield* conversation.resolveApproval('release-call', 'approve');
        const resolved = yield* conversation
          .run()
          .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));

        expect(resolved.outcome).toBe('success');
        expect(released).toBe(1);
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it('rejects an except name the toolkit does not define at construction', () => {
    const lookup = Tool.make('lookup', {
      parameters: Schema.Struct({}),
      success: Schema.String,
    });
    expect(() =>
      Agent.make({
        name: 'except-typo-runtime',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(lookup),
        // The compile-time key check only covers literal arrays; a
        // dynamically built list degrades to this runtime rejection. The
        // lying literal stands in for a value TypeScript cannot see through.
        codeMode: { except: ['nope' as 'lookup'] },
      }),
    ).toThrow('codeMode excepts tool "nope"');
  });

  it('reserves the exec name whenever code mode is enabled', () => {
    const exec = Tool.make('exec', {
      parameters: Schema.Struct({}),
      success: Schema.String,
    });
    expect(() =>
      Agent.make({
        name: 'exec-collision',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(exec),
        codeMode: true,
      }),
    ).toThrow('generated tool "exec"');
  });
});

describe('code mode scratch state', () => {
  it.effect(
    'restores successful checkpoints without adding them to prompts',
    () =>
      Effect.gen(function* () {
        const agent = Agent.make({
          name: 'stateful-code',
          revision: '1',
          instructions: 'Use code mode.',
          toolkit: Toolkit.make(),
          codeMode: true,
        });
        const conversation = Conversation.make(
          agent,
          LogVocabulary.ConversationId.make('code-state-conversation'),
        );
        const firstExecutor = CodeExecutor.fake([
          { _tag: 'Output', value: 'first' },
          { _tag: 'Completion', state: { secret: 'remembered' } },
        ]);
        const secondExecutor = CodeExecutor.fake([
          { _tag: 'Output', value: 'second' },
          { _tag: 'Completion', state: { secret: 'remembered' } },
        ]);
        const turn = (id: string) =>
          ScriptedModel.make([
            [
              {
                type: 'tool-call',
                id,
                name: 'exec',
                params: { source: 'use scratch state' },
              },
              finish('tool-calls'),
            ],
            [
              { type: 'text-start', id: `${id}-answer` },
              { type: 'text-delta', id: `${id}-answer`, delta: 'done' },
              { type: 'text-end', id: `${id}-answer` },
              finish(),
            ],
          ]);
        const firstModel = turn('exec-state-1');
        const secondModel = turn('exec-state-2');

        yield* conversation
          .run('first')
          .pipe(
            Effect.provide(Layer.merge(firstModel.layer, firstExecutor.layer)),
          );
        yield* conversation
          .run('second')
          .pipe(
            Effect.provide(
              Layer.merge(secondModel.layer, secondExecutor.layer),
            ),
          );
        const records = yield* conversation.records().pipe(Stream.runCollect);
        const prompts = [
          ...(yield* firstModel.requests),
          ...(yield* secondModel.requests),
        ].map((request) => JSON.stringify(request.prompt));

        expect((yield* firstExecutor.requests)[0]?.state).toEqual({});
        expect((yield* secondExecutor.requests)[0]?.state).toEqual({
          secret: 'remembered',
        });
        expect(Array.from(records).map(({ record }) => record._tag)).toContain(
          'CodeStateCheckpoint',
        );
        expect(prompts.every((prompt) => !prompt.includes('remembered'))).toBe(
          true,
        );
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('does not commit scratch writes from a failed execution', () =>
    Effect.gen(function* () {
      const agent = Agent.make({
        name: 'atomic-code-state',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
      });
      const conversation = Conversation.make(
        agent,
        'atomic-code-state-conversation',
      );
      const failedExecutor = CodeExecutor.fake([
        { _tag: 'Completion', state: { leaked: 'no' } },
        { _tag: 'Failure', message: 'execution failed' },
      ]);
      const failedModel = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-failed-state',
            name: 'exec',
            params: { source: 'fail after store' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);
      yield* conversation
        .run('fail')
        .pipe(
          Effect.provide(Layer.merge(failedModel.layer, failedExecutor.layer)),
        );

      const nextExecutor = CodeExecutor.fake([
        { _tag: 'Output', value: 'clean' },
        { _tag: 'Completion', state: {} },
      ]);
      const nextModel = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-after-failure',
            name: 'exec',
            params: { source: 'check state' },
          },
          finish('tool-calls'),
        ],
        [
          { type: 'text-start', id: 'answer' },
          { type: 'text-delta', id: 'answer', delta: 'done' },
          { type: 'text-end', id: 'answer' },
          finish(),
        ],
      ]);
      yield* conversation
        .run('continue')
        .pipe(Effect.provide(Layer.merge(nextModel.layer, nextExecutor.layer)));

      expect((yield* nextExecutor.requests)[0]?.state).toEqual({});
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('rejects scratch values over the per-value byte limit', () =>
    Effect.gen(function* () {
      const agent = Agent.make({
        name: 'bounded-code-state',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
      });
      const conversation = Conversation.make(
        agent,
        'bounded-code-state-conversation',
      );
      const executor = CodeExecutor.fake([
        {
          _tag: 'Completion',
          state: { oversized: 'x'.repeat(64 * 1024 + 1) },
        },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-oversized-state',
            name: 'exec',
            params: { source: 'store oversized value' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);

      yield* conversation
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const records = yield* conversation.records().pipe(Stream.runCollect);
      const requests = yield* model.requests;

      expect(JSON.stringify(requests[1]?.prompt)).toContain(
        'exceeds 65536 bytes',
      );
      expect(
        Array.from(records).some(
          ({ record }) => record._tag === 'CodeStateCheckpoint',
        ),
      ).toBe(false);
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect('rejects scratch state over the total byte limit', () =>
    Effect.gen(function* () {
      const agent = Agent.make({
        name: 'bounded-total-code-state',
        revision: '1',
        instructions: 'Use code mode.',
        toolkit: Toolkit.make(),
        codeMode: true,
      });
      const conversation = Conversation.make(
        agent,
        'bounded-total-code-state-conversation',
      );
      const executor = CodeExecutor.fake([
        {
          _tag: 'Completion',
          state: Object.fromEntries(
            Array.from({ length: 5 }, (_, index) => [
              `value-${String(index)}`,
              'x'.repeat(60 * 1024),
            ]),
          ),
        },
      ]);
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'exec-oversized-total-state',
            name: 'exec',
            params: { source: 'store too many values' },
          },
          finish('tool-calls'),
        ],
        answeringTurn,
      ]);

      yield* conversation
        .run('go')
        .pipe(Effect.provide(Layer.merge(model.layer, executor.layer)));
      const records = yield* conversation.records().pipe(Stream.runCollect);
      const requests = yield* model.requests;

      expect(JSON.stringify(requests[1]?.prompt)).toContain(
        'exceeds 262144 bytes',
      );
      expect(
        Array.from(records).some(
          ({ record }) => record._tag === 'CodeStateCheckpoint',
        ),
      ).toBe(false);
    }).pipe(Effect.provide(logLayer), Effect.scoped),
  );

  it.effect(
    'applies the recording policy before persisting scratch state',
    () =>
      Effect.gen(function* () {
        const agent = Agent.make({
          name: 'redacted-code-state',
          revision: '1',
          instructions: 'Use code mode.',
          toolkit: Toolkit.make(),
          codeMode: true,
        });
        const conversation = Conversation.make(
          agent,
          'redacted-code-state-conversation',
          {
            codeState: () => Effect.succeed({ token: '[redacted]' }),
          },
        );
        const firstExecutor = CodeExecutor.fake([
          { _tag: 'Completion', state: { token: 'raw-secret' } },
        ]);
        const secondExecutor = CodeExecutor.fake([
          { _tag: 'Output', value: 'ok' },
          { _tag: 'Completion', state: {} },
        ]);
        const model = (id: string) =>
          ScriptedModel.make([
            [
              {
                type: 'tool-call',
                id,
                name: 'exec',
                params: { source: 'use state' },
              },
              finish('tool-calls'),
            ],
            answeringTurn,
          ]);
        const firstModel = model('exec-redact-1');
        const secondModel = model('exec-redact-2');

        yield* conversation
          .run('first')
          .pipe(
            Effect.provide(Layer.merge(firstModel.layer, firstExecutor.layer)),
          );
        yield* conversation
          .run('second')
          .pipe(
            Effect.provide(
              Layer.merge(secondModel.layer, secondExecutor.layer),
            ),
          );

        expect((yield* secondExecutor.requests)[0]?.state).toEqual({
          token: '[redacted]',
        });
        const records = yield* conversation.records().pipe(Stream.runCollect);
        expect(JSON.stringify(Array.from(records))).not.toContain('raw-secret');
      }).pipe(Effect.provide(logLayer), Effect.scoped),
  );
});
