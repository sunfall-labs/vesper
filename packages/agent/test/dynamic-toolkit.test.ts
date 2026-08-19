import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Ref, Schema, Stream } from 'effect';
import { AiError, type Response, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { DynamicToolkit } from '../src/dynamic-toolkit.js';
import { ScriptedModel } from '../src/testing.js';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const remote = Tool.dynamic('mcp__linear__search_issues', {
  description: 'Search Linear issues.',
  parameters: {
    type: 'object',
    properties: { query: { type: 'string' } },
    required: ['query'],
  },
  success: Schema.String,
});

const remoteToolkit = Toolkit.make(remote);

describe('Agent dynamic tools', () => {
  it.effect(
    'opens independent sources concurrently while preserving order',
    () =>
      Effect.gen(function* () {
        const started = yield* Ref.make(0);
        const bothStarted = yield* Deferred.make<void>();
        const source = () =>
          DynamicToolkit.make(
            Effect.gen(function* () {
              const count = yield* Ref.updateAndGet(
                started,
                (value) => value + 1,
              );
              if (count === 2) yield* Deferred.succeed(bothStarted, undefined);
              yield* Deferred.await(bothStarted);
              return DynamicToolkit.empty;
            }),
          );

        yield* Effect.raceFirst(
          DynamicToolkit.open([source(), source()]),
          Effect.sleep('1 second').pipe(
            Effect.andThen(
              Effect.fail(
                new AiError.AiError({
                  module: 'DynamicTest',
                  method: 'parallelOpen',
                  reason: new AiError.InvalidRequestError({
                    description: 'dynamic sources opened sequentially',
                  }),
                }),
              ),
            ),
          ),
        );

        expect(yield* Ref.get(started)).toBe(2);
      }).pipe(Effect.scoped),
  );

  it.effect(
    'opens once per run, keeps one snapshot across turns, dispatches, and closes',
    () =>
      Effect.gen(function* () {
        const lifecycle = { opened: 0, called: 0, closed: 0 };
        const source = DynamicToolkit.make(
          Effect.acquireRelease(
            Effect.sync(() => {
              lifecycle.opened += 1;
              return remoteToolkit.pipe(
                Effect.provide(
                  remoteToolkit.toLayer({
                    mcp__linear__search_issues: () =>
                      Effect.sync(() => {
                        lifecycle.called += 1;
                        return 'VES-42';
                      }),
                  }),
                ),
              );
            }).pipe(Effect.flatten),
            () =>
              Effect.sync(() => {
                lifecycle.closed += 1;
              }),
          ),
        );
        const agent = Agent.make({
          name: 'dynamic',
          revision: '1',
          instructions: 'Use the available tools.',
          toolkit: Toolkit.make(),
          dynamicTools: [source],
        });
        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call',
              id: 'call-1',
              name: 'mcp__linear__search_issues',
              params: { query: 'vesper' },
            },
            finish('tool-calls'),
          ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
          [
            { type: 'text-start', id: 'answer' },
            { type: 'text-delta', id: 'answer', delta: 'Found VES-42.' },
            { type: 'text-end', id: 'answer' },
            finish(),
          ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
        ]);

        const result = yield* agent
          .run('Find it.')
          .pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;

        expect(result.text).toBe('Found VES-42.');
        expect(lifecycle).toEqual({ opened: 1, called: 1, closed: 1 });
        expect(requests.map((request) => request.tools)).toEqual([
          ['mcp__linear__search_issues'],
          ['mcp__linear__search_issues'],
        ]);
      }),
  );

  it.effect('fails source resolution before making a model request', () =>
    Effect.gen(function* () {
      const source = DynamicToolkit.make(
        Effect.fail(
          new AiError.AiError({
            module: 'DynamicTest',
            method: 'open',
            reason: new AiError.InvalidRequestError({
              description: 'unavailable',
            }),
          }),
        ),
      );
      const agent = Agent.make({
        name: 'dynamic-failure',
        revision: '1',
        instructions: 'Use the available tools.',
        toolkit: Toolkit.make(),
        dynamicTools: [source],
      });
      const model = ScriptedModel.make([]);

      const result = yield* agent
        .stream('hello')
        .pipe(Stream.runDrain, Effect.provide(model.layer), Effect.result);

      expect(result._tag).toBe('Failure');
      expect(yield* model.requests).toEqual([]);
    }),
  );

  it('rejects duplicate names between resolved sources', () => {
    const handled = Effect.runSync(
      remoteToolkit.pipe(
        Effect.provide(
          remoteToolkit.toLayer({
            mcp__linear__search_issues: () => Effect.succeed('ok'),
          }),
        ),
      ),
    );

    expect(() => DynamicToolkit.merge(handled, handled)).toThrow(
      'Dynamic tool name collision',
    );
  });

  it.effect('reports source collisions in the typed error channel', () => {
    const handled = remoteToolkit.pipe(
      Effect.provide(
        remoteToolkit.toLayer({
          mcp__linear__search_issues: () => Effect.succeed('ok'),
        }),
      ),
    );
    const source = DynamicToolkit.make(handled);

    return Effect.gen(function* () {
      const result = yield* DynamicToolkit.open([source, source]).pipe(
        Effect.result,
      );

      expect(result._tag).toBe('Failure');
    }).pipe(Effect.scoped);
  });

  it.effect('rejects duplicate resource identities even without tools', () => {
    const source = () =>
      DynamicToolkit.make(Effect.succeed(DynamicToolkit.empty), {
        resource: {
          id: 'mcp:linear',
          description: 'MCP server "linear"',
        },
      });

    return Effect.gen(function* () {
      const result = yield* DynamicToolkit.open([source(), source()]).pipe(
        Effect.result,
      );

      expect(result._tag).toBe('Failure');
    }).pipe(Effect.scoped);
  });

  it.effect(
    'keeps optional failures model-visible without advertising tools',
    () =>
      Effect.gen(function* () {
        const unavailable = DynamicToolkit.optional(
          DynamicToolkit.make<{}>(
            Effect.fail(
              new AiError.AiError({
                module: 'DynamicTest',
                method: 'open',
                reason: new AiError.InvalidRequestError({
                  description: 'offline',
                }),
              }),
            ),
          ),
          {
            id: 'mcp:linear',
            description: 'MCP server "linear"',
          },
        );
        const agent = Agent.make({
          name: 'optional-dynamic',
          revision: '1',
          instructions: 'Use available tools.',
          toolkit: Toolkit.make(),
          dynamicTools: [unavailable],
        });
        const model = ScriptedModel.make([
          [
            { type: 'text-start', id: 'answer' },
            { type: 'text-delta', id: 'answer', delta: 'No tools.' },
            { type: 'text-end', id: 'answer' },
            finish(),
          ],
        ]);

        yield* agent.run('Try Linear.').pipe(Effect.provide(model.layer));
        const request = (yield* model.requests)[0];
        const system = request?.prompt.content
          .filter((message) => message.role === 'system')
          .map((message) => String(message.content))
          .join('\n');

        expect(request?.tools).toEqual([]);
        expect(system).toContain('MCP server "linear": unavailable; no tools.');
        expect(system).toContain('supersedes availability in earlier messages');
      }),
  );
});

type Assert<T extends true> = T;
type DynamicNameIsVisible = Assert<
  'mcp__linear__search_issues' extends keyof Agent.Tools<
    ReturnType<typeof typedAgent>
  >
    ? true
    : false
>;

const typedAgent = () =>
  Agent.make({
    name: 'typed-dynamic',
    revision: '1',
    instructions: 'Use tools.',
    toolkit: Toolkit.make(),
    dynamicTools: [
      DynamicToolkit.make(
        remoteToolkit.pipe(
          Effect.provide(
            remoteToolkit.toLayer({
              mcp__linear__search_issues: () => Effect.succeed('ok'),
            }),
          ),
        ),
      ),
    ],
  });

const dynamicNameIsVisible: DynamicNameIsVisible = true;
void dynamicNameIsVisible;
