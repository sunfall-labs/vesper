import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  type AiError,
  LanguageModel,
  Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

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
const widenedBoundaryCodec = (relaxParams: boolean) =>
  Response.StreamPart(datedToolkit, { relaxParams });
type WidenedBoundaryPart = ReturnType<typeof widenedBoundaryCodec>['Type'];

const frameworkFailureIsADeclaredResult = (
  error: AiError.AiError,
): Tool.Result<typeof readFile> => error;
void frameworkFailureIsADeclaredResult;

const modelBoundaryTypeAssertions = (
  part: Response.ModelStreamPart<DatedTools>,
): void => {
  if (part.type === 'tool-call') {
    // @ts-expect-error model-authored parameters remain unknown until Toolkit
    part.params.at;
  }
  if (part.type === 'tool-result') {
    const name: string = part.name;
    void name;
    // @ts-expect-error a framework result can name an unrecognized model tool
    const knownName: 'dated_tool' = part.name;
    void knownName;
  }
};
void modelBoundaryTypeAssertions;

const widenedBoundaryTypeAssertions = (part: WidenedBoundaryPart): void => {
  if (part.type === 'tool-call') {
    // @ts-expect-error a boolean option may be true, so params stay unknown
    part.params.at;
  }
};
void widenedBoundaryTypeAssertions;

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
    'keeps generate and stream model boundaries equally relaxed when resolution is disabled',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const resolved = yield* toolkit.pipe(Effect.provide(handlers(calls)));
        const rawCalls: ReadonlyArray<Response.ToolCallPartEncoded> = [
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
          generateText: () =>
            Effect.succeed([...rawCalls, finish('tool-calls')]),
          streamText: () =>
            Stream.fromIterable([...rawCalls, finish('tool-calls')]),
        });

        const generated = yield* LanguageModel.generateText({
          prompt: 'read the folder',
          toolkit: resolved,
          disableToolCallResolution: true,
        }).pipe(
          Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
        );
        const streamed = yield* LanguageModel.streamText({
          prompt: 'read the folder',
          toolkit: resolved,
          disableToolCallResolution: true,
        }).pipe(
          Stream.runCollect,
          Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
        );

        const generatedCalls = generated.content.filter(
          (part) => part.type === 'tool-call',
        );
        const streamedCalls = Array.from(streamed).filter(
          (part) => part.type === 'tool-call',
        );
        expect(
          generatedCalls.map(({ name, params }) => ({ name, params })),
        ).toEqual(rawCalls.map(({ name, params }) => ({ name, params })));
        expect(
          streamedCalls.map(({ name, params }) => ({ name, params })),
        ).toEqual(rawCalls.map(({ name, params }) => ({ name, params })));
        expect(
          generated.content.some((part) => part.type === 'tool-result'),
        ).toBe(false);
        expect(
          Array.from(streamed).some((part) => part.type === 'tool-result'),
        ).toBe(false);
        expect(yield* Ref.get(calls)).toBe(0);
      }),
  );

  it.effect(
    'keeps transformed parameters unknown until the typed handler boundary',
    () =>
      Effect.gen(function* () {
        const encodedCall: Response.ToolCallPartEncoded = {
          type: 'tool-call',
          id: 'dated-call',
          name: 'dated_tool',
          params: { at: '2026-08-26T00:00:00.000Z' },
        };
        const decoded = yield* Schema.decodeEffect(
          Response.StreamPart(datedToolkit, { relaxParams: true }),
        )(encodedCall);
        expect(decoded.type).toBe('tool-call');
        if (decoded.type !== 'tool-call') {
          throw new Error('expected a tool call');
        }
        expect(decoded.params).toEqual({
          at: '2026-08-26T00:00:00.000Z',
        });

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
        yield* resolved
          .handle('dated_tool', decoded.params, decoded.id)
          .pipe(Effect.flatMap(Stream.runDrain));
        expect(yield* Ref.get(receivedDate)).toBe(true);
      }),
  );

  it.effect(
    'returns malformed model parameters to the model as a typed tool failure',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const resolved = yield* toolkit.pipe(Effect.provide(handlers(calls)));
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

        const result = parts.find((part) => part.type === 'tool-result');
        expect(result).toMatchObject({
          type: 'tool-result',
          id: 'call-1',
          name: 'read_file',
          isFailure: true,
          result: {
            _tag: 'AiError',
            reason: {
              _tag: 'ToolParameterValidationError',
              toolName: 'read_file',
              toolParams: { path: { segments: ['src'] } },
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
});
