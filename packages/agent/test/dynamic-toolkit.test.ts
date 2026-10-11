import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Ref, Result, Schema } from 'effect';
import { AiError, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { DynamicToolkit } from '../src/dynamic-toolkit.js';
import { ScriptedModel } from '../src/testing.js';
import { callTurn, textTurn, toolResultsIn } from './tool-fixtures.js';

// Dynamic toolkits, observed through an agent run: what the model is offered,
// what runs, and what the model is shown when a source is down.

const dynamicTool = (name: string) =>
  Tool.dynamic(name, {
    description: 'An externally supplied tool.',
    parameters: {
      type: 'object',
      properties: { query: { type: 'string' } },
      required: ['query'],
    },
    success: Schema.String,
    failure: Schema.String,
    failureMode: 'return',
  });

const SEARCH = 'mcp__linear__search_issues';
const remote = dynamicTool(SEARCH);

/** A source offering `tool`, answered by `handler`. */
const sourceOf = (
  tool: typeof remote,
  handler: () => Effect.Effect<string, string>,
) => {
  const toolkit = Toolkit.make(tool);
  return DynamicToolkit.make(
    toolkit.pipe(Effect.provide(toolkit.toLayer({ [tool.name]: handler }))),
  );
};

const agentOf = (dynamicTools: ReadonlyArray<DynamicToolkit.Any>) =>
  Agent.make({
    name: 'dynamic',
    revision: '1',
    instructions: 'Use the available tools.',
    toolkit: Toolkit.make(),
    dynamicTools,
  });

const search = (id: string) =>
  callTurn({ id, name: SEARCH, params: { query: 'vesper' } });

const unavailable = (method: string) =>
  new AiError.AiError({
    module: 'DynamicTest',
    method,
    reason: new AiError.InvalidRequestError({ description: 'unavailable' }),
  });

describe('dynamic tools in an agent run', () => {
  it.effect(
    'opens once per run, keeps one snapshot across turns, dispatches, and closes',
    () => {
      const lifecycle = { opened: 0, called: 0, closed: 0 };
      const toolkit = Toolkit.make(remote);
      const source = DynamicToolkit.make(
        Effect.acquireRelease(
          Effect.sync(() => {
            lifecycle.opened += 1;
            return toolkit.pipe(
              Effect.provide(
                toolkit.toLayer({
                  [SEARCH]: () =>
                    Effect.sync(() => {
                      lifecycle.called += 1;
                      return 'VES-42';
                    }),
                }),
              ),
            );
          }).pipe(Effect.flatten),
          () => Effect.sync(() => void (lifecycle.closed += 1)),
        ),
      );
      const model = ScriptedModel.make([
        search('call-1'),
        textTurn('Found VES-42.'),
      ]);
      return Effect.gen(function* () {
        const result = yield* agentOf([source])
          .run('Find it.')
          .pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;

        expect(result.text).toBe('Found VES-42.');
        expect(lifecycle).toEqual({ opened: 1, called: 1, closed: 1 });
        expect(requests.map((request) => request.tools)).toEqual([
          [SEARCH],
          [SEARCH],
        ]);
      });
    },
  );

  it.effect(
    'keeps one advertised schema while execution availability changes between turns',
    () => {
      let calls = 0;
      let enabled = false;
      const source = sourceOf(remote, () =>
        Effect.suspend(() => {
          if (!enabled) {
            enabled = true;
            return Effect.fail('Linear is temporarily unavailable.');
          }
          calls += 1;
          return Effect.succeed('VES-42');
        }),
      );
      const model = ScriptedModel.make([
        search('call-disabled'),
        search('call-enabled'),
        textTurn('Found VES-42.'),
      ]);
      return Effect.gen(function* () {
        const result = yield* agentOf([source])
          .run('Find it.')
          .pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;
        const advertised = requests[0]?.toolDefinitions[0];

        expect(result.text).toBe('Found VES-42.');
        expect(calls).toBe(1);
        expect(advertised).toBeDefined();
        expect(
          requests.every(
            (request) => request.toolDefinitions[0] === advertised,
          ),
        ).toBe(true);
        expect(toolResultsIn(requests[1]?.prompt)[0]?.result).toBe(
          'Linear is temporarily unavailable.',
        );
      });
    },
  );

  it.effect(
    'answers a tool that was not advertised with ToolNotFound, and runs nothing',
    () => {
      let calls = 0;
      const source = sourceOf(remote, () =>
        Effect.sync(() => (calls += 1)).pipe(Effect.as('VES-42')),
      );
      const model = ScriptedModel.make([
        callTurn({
          id: 'call-unknown',
          name: 'mcp__linear__create_issue',
          params: { query: 'vesper' },
        }),
        textTurn('I used the advertised tool list.'),
      ]);
      return Effect.gen(function* () {
        yield* agentOf([source])
          .run('Create it.')
          .pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;
        const returned = toolResultsIn(requests[1]?.prompt).at(0);
        const error = yield* Schema.decodeUnknownEffect(AiError.AiError)(
          returned?.result,
        );

        expect(calls).toBe(0);
        expect(requests[0]?.tools).toEqual([SEARCH]);
        expect(returned?.name).toBe('mcp__linear__create_issue');
        expect(error.reason._tag).toBe('ToolNotFoundError');
      });
    },
  );

  it.effect('treats prototype-like tool names as ordinary, inert names', () => {
    let calls = 0;
    const source = sourceOf(dynamicTool('__proto__'), () =>
      Effect.sync(() => (calls += 1)).pipe(Effect.as('ok')),
    );
    const model = ScriptedModel.make([
      callTurn({ id: 'call-proto', name: '__proto__', params: { query: 'x' } }),
      textTurn('done'),
    ]);
    return Effect.gen(function* () {
      const result = yield* agentOf([source])
        .run('go')
        .pipe(Effect.provide(model.layer));
      const requests = yield* model.requests;

      expect(result.text).toBe('done');
      expect(calls).toBe(1);
      expect(requests[0]?.tools).toEqual(['__proto__']);
      expect(toolResultsIn(requests[1]?.prompt)[0]?.result).toBe('ok');
    });
  });

  it.effect('opens independent sources concurrently', () => {
    const bothStarted = Deferred.makeUnsafe<void>();
    const started = Ref.makeUnsafe(0);
    const source = () =>
      DynamicToolkit.make(
        Effect.gen(function* () {
          if ((yield* Ref.updateAndGet(started, (n) => n + 1)) === 2) {
            yield* Deferred.succeed(bothStarted, undefined);
          }
          // Each source waits for the other: opening them one at a time deadlocks.
          yield* Deferred.await(bothStarted);
          return DynamicToolkit.empty;
        }),
      );
    return agentOf([source(), source()])
      .run('hello')
      .pipe(
        Effect.timeout('1 second'),
        Effect.provide(ScriptedModel.make([textTurn('ok')]).layer),
        Effect.map((result) => {
          expect(result.text).toBe('ok');
        }),
      );
  });
});

describe('dynamic sources that are down or conflict', () => {
  it.effect(
    'fails before making a model request when a source cannot be resolved',
    () => {
      const model = ScriptedModel.make([]);
      return Effect.gen(function* () {
        const result = yield* agentOf([
          DynamicToolkit.make(Effect.fail(unavailable('open'))),
        ])
          .run('hello')
          .pipe(Effect.provide(model.layer), Effect.result);

        expect(Result.isFailure(result)).toBe(true);
        expect(yield* model.requests).toEqual([]);
      });
    },
  );

  const resource = { id: 'mcp:linear', description: 'MCP server "linear"' };
  const conflicts = [
    [
      'two sources offer the same tool name',
      () => sourceOf(remote, () => Effect.succeed('ok')),
    ],
    [
      'two sources claim the same resource even without tools',
      () =>
        DynamicToolkit.make(Effect.succeed(DynamicToolkit.empty), { resource }),
    ],
  ] as const;

  for (const [name, make] of conflicts) {
    it.effect(`fails the run, before any model request, when ${name}`, () => {
      const source = make();
      const model = ScriptedModel.make([]);
      return Effect.gen(function* () {
        const result = yield* agentOf([source, source])
          .run('hello')
          .pipe(Effect.provide(model.layer), Effect.result);

        expect(Result.isFailure(result)).toBe(true);
        expect(yield* model.requests).toEqual([]);
      });
    });
  }

  it.effect(
    'keeps an optional source’s failure model-visible without advertising tools',
    () => {
      const optional = DynamicToolkit.optional(
        DynamicToolkit.make<Record<never, never>>(
          Effect.fail(unavailable('open')),
        ),
        resource,
      );
      const model = ScriptedModel.make([textTurn('No tools.')]);
      return Effect.gen(function* () {
        yield* agentOf([optional])
          .run('Try Linear.')
          .pipe(Effect.provide(model.layer));
        const request = (yield* model.requests).at(0);
        const system = request?.prompt.content
          .filter((message) => message.role === 'system')
          .map((message) => message.content)
          .join('\n');

        expect(request?.tools).toEqual([]);
        expect(system).toContain('MCP server "linear": unavailable; no tools.');
        expect(system).toContain(
          'supersedes dynamic-resource state in earlier messages',
        );
      });
    },
  );
});
