import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Prompt,
  type Response,
  Tool,
  Toolkit,
} from 'effect/ai';

import { Agent } from '../src/agent.js';

const readFile = Tool.make('read_file', {
  description: 'read a file',
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
});

const toolkit = Toolkit.make(readFile);

const datedTool = Tool.make('dated_tool', {
  parameters: Schema.Struct({ at: Schema.DateFromString }),
  success: Schema.String,
});
const datedToolkit = Toolkit.make(datedTool);
type DatedTools = { readonly dated_tool: typeof datedTool };

// Vesper's loop asks the model with resolution disabled and deferred
// validation: tool-call parameters are the model-authored value, and a call
// may name any tool, including one the turn did not offer.
const loopBoundaryTypeAssertions = (
  part: Response.StreamPart<DatedTools, 'deferred'>,
): void => {
  if (part.type === 'tool-call') {
    const name: string = part.name;
    const params: unknown = part.params;
    void name;
    void params;
  }
};
void loopBoundaryTypeAssertions;

const decodedBoundaryTypeAssertions = (
  part: Response.StreamPart<DatedTools>,
): void => {
  if (part.type === 'tool-call') {
    const at: Date = part.params.at;
    void at;
  }
};
void decodedBoundaryTypeAssertions;

const encodedBoundaryTypeAssertions = (
  part: Response.StreamPart<DatedTools, 'encoded'>,
): void => {
  if (part.type === 'tool-call') {
    const at: string = part.params.at;
    void at;
  }
};
void encodedBoundaryTypeAssertions;

const handlers = (calls: Ref.Ref<number>) =>
  toolkit.toLayer({
    read_file: ({ path }) =>
      Ref.update(calls, (count) => count + 1).pipe(Effect.as(path)),
  });

const finish = (
  reason: 'stop' | 'tool-calls' = 'stop',
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const malformedModel = LanguageModel.make({
  generateText: () => Effect.succeed([]),
  streamText: () =>
    Stream.fromIterable<Response.StreamPartEncoded>([
      {
        type: 'tool-call',
        id: 'call-1',
        name: 'read_file',
        params: { path: { segments: ['src'] } },
      },
      finish('tool-calls'),
    ]),
});

describe('tool parameter recovery', () => {
  it.effect(
    'returns malformed and undeclared calls as plain calls under deferred validation',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const resolved = yield* toolkit.pipe(Effect.provide(handlers(calls)));
        const sent: ReadonlyArray<Response.ToolCallPartEncoded> = [
          {
            type: 'tool-call',
            id: 'call-malformed',
            name: 'read_file',
            params: { path: { segments: ['src'] } },
          },
          {
            type: 'tool-call',
            id: 'call-unknown',
            name: '__proto__',
            params: { path: 'README.md' },
          },
        ];
        const model = LanguageModel.make({
          generateText: () => Effect.succeed([...sent, finish('tool-calls')]),
          streamText: () =>
            Stream.fromIterable([...sent, finish('tool-calls')]),
        });

        const generated = yield* LanguageModel.generateText({
          prompt: 'read the folder',
          toolkit: resolved,
          disableToolCallResolution: true,
          toolCallValidation: 'deferred',
        }).pipe(
          Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
        );
        const streamed = yield* LanguageModel.streamText({
          prompt: 'read the folder',
          toolkit: resolved,
          disableToolCallResolution: true,
          toolCallValidation: 'deferred',
        }).pipe(
          Stream.runCollect,
          Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
        );

        for (const parts of [generated.content, Array.from(streamed)]) {
          expect(
            parts
              .filter((part) => part.type === 'tool-call')
              .map(({ id, name, params }) => ({ id, name, params })),
          ).toEqual(sent.map(({ id, name, params }) => ({ id, name, params })));
          expect(parts.some((part) => part.type === 'tool-result')).toBe(false);
        }
        expect(yield* Ref.get(calls)).toBe(0);
      }),
  );

  it.effect(
    'hands transformed parameters to the handler through the encoded boundary',
    () =>
      Effect.gen(function* () {
        const receivedDate = yield* Ref.make(false);
        const resolved = yield* datedToolkit.pipe(
          Effect.provide(
            datedToolkit.toLayer({
              dated_tool: ({ at }) =>
                Ref.set(receivedDate, at instanceof Date).pipe(
                  Effect.as(at.toISOString()),
                ),
            }),
          ),
        );
        const results = yield* resolved
          .handle('dated_tool', { at: '2026-08-26T00:00:00.000Z' }, 'dated')
          .pipe(Effect.flatMap(Stream.runCollect));
        expect(yield* Ref.get(receivedDate)).toBe(true);
        expect(Array.from(results).at(-1)).toMatchObject({
          isFailure: false,
          encodedResult: '2026-08-26T00:00:00.000Z',
        });
      }),
  );

  it.effect(
    'routes malformed model parameters through the tool failureMode',
    () =>
      Effect.gen(function* () {
        const returning = Tool.make('read_file', {
          description: 'read a file',
          parameters: Schema.Struct({ path: Schema.String }),
          success: Schema.String,
          failureMode: 'return',
        });
        const returningToolkit = Toolkit.make(returning);
        const calls = yield* Ref.make(0);
        const resolved = yield* returningToolkit.pipe(
          Effect.provide(
            returningToolkit.toLayer({
              read_file: ({ path }) =>
                Ref.update(calls, (count) => count + 1).pipe(Effect.as(path)),
            }),
          ),
        );
        const parts = yield* LanguageModel.streamText({
          prompt: 'read the folder',
          toolkit: resolved,
        }).pipe(
          Stream.runCollect,
          Effect.provideServiceEffect(
            LanguageModel.LanguageModel,
            malformedModel,
          ),
        );

        const result = Array.from(parts).find(
          (part) => part.type === 'tool-result',
        );
        expect(result).toMatchObject({
          type: 'tool-result',
          id: 'call-1',
          name: 'read_file',
          isFailure: true,
          encodedResult: {
            _tag: 'AiError',
            reason: {
              _tag: 'ToolParameterValidationError',
              toolName: 'read_file',
            },
          },
        });
        expect(yield* Ref.get(calls)).toBe(0);
      }),
  );

  it.effect('lets the Vesper loop continue after malformed parameters', () =>
    Effect.gen(function* () {
      const handlerCalls = yield* Ref.make(0);
      const modelCalls = yield* Ref.make(0);
      const agent = Agent.make({
        name: 'parameter-recovery',
        revision: '1',
        instructions: 'use the file tool',
        toolkit,
      }).withHandlers({
        read_file: ({ path }) =>
          Ref.update(handlerCalls, (count) => count + 1).pipe(Effect.as(path)),
      });
      const model = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                const turn = yield* Ref.getAndUpdate(
                  modelCalls,
                  (count) => count + 1,
                );
                return turn === 0
                  ? Stream.fromIterable<Response.StreamPartEncoded>([
                      {
                        type: 'tool-call',
                        id: 'call-1',
                        name: 'read_file',
                        params: { path: { segments: ['src'] } },
                      },
                      finish('tool-calls'),
                    ])
                  : Stream.fromIterable<Response.StreamPartEncoded>([
                      { type: 'text-start', id: 'answer' },
                      {
                        type: 'text-delta',
                        id: 'answer',
                        delta: 'Recovered.',
                      },
                      { type: 'text-end', id: 'answer' },
                      finish(),
                    ]);
              }),
            ),
        }),
      );

      const result = yield* agent
        .run('read the folder')
        .pipe(Effect.provide(model), Effect.orDie);

      expect(result.text).toBe('Recovered.');
      expect(yield* Ref.get(modelCalls)).toBe(2);
      expect(yield* Ref.get(handlerCalls)).toBe(0);
    }),
  );

  it.effect('returns an unknown tool name and continues the Vesper loop', () =>
    Effect.gen(function* () {
      const handlerCalls = yield* Ref.make(0);
      const modelCalls = yield* Ref.make(0);
      const agent = Agent.make({
        name: 'unknown-tool-recovery',
        revision: '1',
        instructions: 'use the file tool',
        toolkit,
      }).withHandlers({
        read_file: ({ path }) =>
          Ref.update(handlerCalls, (count) => count + 1).pipe(Effect.as(path)),
      });
      const model = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(modelCalls, (count) => count + 1).pipe(
                Effect.map((turn) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    turn === 0
                      ? [
                          {
                            type: 'tool-call',
                            id: 'call-unknown',
                            name: '__proto__',
                            params: { path: 'README.md' },
                          },
                          finish('tool-calls'),
                        ]
                      : [
                          { type: 'text-start', id: 'answer' },
                          {
                            type: 'text-delta',
                            id: 'answer',
                            delta: 'Recovered.',
                          },
                          { type: 'text-end', id: 'answer' },
                          finish(),
                        ],
                  ),
                ),
              ),
            ),
        }),
      );

      const result = yield* agent
        .run('read the file')
        .pipe(Effect.provide(model), Effect.orDie);

      expect(result.text).toBe('Recovered.');
      expect(yield* Ref.get(modelCalls)).toBe(2);
      expect(yield* Ref.get(handlerCalls)).toBe(0);
    }),
  );

  it.effect.each(['toString', '__proto__', 'constructor'])(
    'answers a call named %s once, as an undeclared tool',
    (name) =>
      Effect.gen(function* () {
        const handlerCalls = yield* Ref.make(0);
        const prompts: Array<Prompt.Prompt> = [];
        const turns = yield* Ref.make(0);
        const agent = Agent.make({
          name: 'prototype-names',
          revision: '1',
          instructions: 'use the file tool',
          toolkit,
        }).withHandlers({
          read_file: ({ path }) =>
            Ref.update(handlerCalls, (count) => count + 1).pipe(
              Effect.as(path),
            ),
        });
        const model = Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (options) =>
              Stream.unwrap(
                Ref.getAndUpdate(turns, (count) => count + 1).pipe(
                  Effect.map((turn) => {
                    prompts.push(options.prompt);
                    return Stream.fromIterable<Response.StreamPartEncoded>(
                      turn === 0
                        ? [
                            {
                              type: 'tool-call',
                              id: 'call-proto',
                              name,
                              params: { path: 'README.md' },
                            },
                            finish('tool-calls'),
                          ]
                        : [
                            { type: 'text-start', id: 'answer' },
                            { type: 'text-delta', id: 'answer', delta: 'ok' },
                            { type: 'text-end', id: 'answer' },
                            finish(),
                          ],
                    );
                  }),
                ),
              ),
          }),
        );

        const result = yield* agent
          .run('read the file')
          .pipe(Effect.provide(model), Effect.orDie);

        expect(result.text).toBe('ok');
        expect(yield* Ref.get(handlerCalls)).toBe(0);
        // A name that happens to be on Object.prototype is not a tool: one
        // failed result, never a second from a lookup that found the
        // prototype's property.
        const results = (prompts[1]?.content ?? []).flatMap((message) =>
          message.role === 'tool' ? message.content : [],
        );
        expect(results).toHaveLength(1);
        expect(results[0]).toMatchObject({
          type: 'tool-result',
          id: 'call-proto',
          isFailure: true,
          result: { _tag: 'AiError', reason: { _tag: 'ToolNotFoundError' } },
        });
      }),
  );
});
