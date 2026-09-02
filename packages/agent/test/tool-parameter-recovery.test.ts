import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Prompt,
  Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';

// Recovery of an invalid model tool call is the operation's opt-in
// (`invalidToolCalls: 'return'` on `generateText` / `streamText`, which the
// Vesper loop always passes); the tools keep their own `failureMode`.
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

const modelBoundaryTypeAssertions = (
  part: Response.StreamPart<DatedTools, false, 'return'>,
): void => {
  if (part.type === 'tool-call') {
    const at: Date = part.params.at;
    void at;
  } else if (part.type === 'tool-call-error') {
    const name: string = part.name;
    const params: unknown = part.params;
    void name;
    void params;
  }
};
void modelBoundaryTypeAssertions;

const encodedBoundaryTypeAssertions = (
  part: Response.StreamPart<DatedTools, true>,
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
    'returns invalid generate and stream calls as model-visible errors when resolution is disabled',
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
          invalidToolCalls: 'return',
        }).pipe(
          Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
        );
        const streamed = yield* LanguageModel.streamText({
          prompt: 'read the folder',
          toolkit: resolved,
          disableToolCallResolution: true,
          invalidToolCalls: 'return',
        }).pipe(
          Stream.runCollect,
          Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
        );

        const generatedErrors = generated.content.filter(
          (part) => part.type === 'tool-call-error',
        );
        const streamedErrors = Array.from(streamed).filter(
          (part) => part.type === 'tool-call-error',
        );
        expect(
          generatedErrors.map(({ name, params }) => ({ name, params })),
        ).toEqual(rawCalls.map(({ name, params }) => ({ name, params })));
        expect(
          streamedErrors.map(({ name, params }) => ({ name, params })),
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
    'fails invalid and unknown calls unless the operation opts into returning them',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const resolved = yield* toolkit.pipe(Effect.provide(handlers(calls)));
        const cases: ReadonlyArray<
          readonly [Response.ToolCallPartEncoded, string]
        > = [
          [
            {
              type: 'tool-call',
              id: 'call-malformed',
              name: 'read_file',
              params: { path: { segments: ['src'] } },
            },
            'ToolParameterValidationError',
          ],
          [
            {
              type: 'tool-call',
              id: 'call-unknown',
              name: '__proto__',
              params: { path: 'README.md' },
            },
            'ToolNotFoundError',
          ],
        ];
        for (const [call, reason] of cases) {
          const model = LanguageModel.make({
            generateText: () => Effect.succeed([call, finish('tool-calls')]),
            streamText: () => Stream.fromIterable([call, finish('tool-calls')]),
          });
          const generated = yield* LanguageModel.generateText({
            prompt: 'read the folder',
            toolkit: resolved,
          }).pipe(
            Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
            Effect.flip,
          );
          const streamed = yield* LanguageModel.streamText({
            prompt: 'read the folder',
            toolkit: resolved,
          }).pipe(
            Stream.runDrain,
            Effect.provideServiceEffect(LanguageModel.LanguageModel, model),
            Effect.flip,
          );
          for (const error of [generated, streamed]) {
            expect(AiError.isAiError(error)).toBe(true);
            if (AiError.isAiError(error)) {
              expect(error.module).toBe('LanguageModel');
              expect(error.reason._tag).toBe(reason);
            }
          }
        }
        expect(yield* Ref.get(calls)).toBe(0);
      }),
  );

  it.effect('decodes transformed parameters once at the toolkit boundary', () =>
    Effect.gen(function* () {
      const encodedParams = { at: '2026-08-26T00:00:00.000Z' };
      const encodedCall: Response.ToolCallPartEncoded = {
        type: 'tool-call',
        id: 'dated-call',
        name: 'dated_tool',
        params: encodedParams,
      };
      const decoded = yield* Schema.decodeEffect(
        Response.StreamPart(datedToolkit),
      )(encodedCall);
      expect(decoded.type).toBe('tool-call');
      if (decoded.type !== 'tool-call') {
        throw new Error('expected a tool call');
      }
      const expectedDate = yield* Schema.decodeEffect(Schema.DateFromString)(
        '2026-08-26T00:00:00.000Z',
      );
      expect(decoded.params).toEqual({ at: expectedDate });

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
        .handle('dated_tool', encodedParams, decoded.id)
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
          invalidToolCalls: 'return',
        }).pipe(
          Stream.runCollect,
          Effect.provideServiceEffect(
            LanguageModel.LanguageModel,
            malformedModel,
          ),
        );

        const result = parts.find((part) => part.type === 'tool-call-error');
        expect(result).toMatchObject({
          type: 'tool-call-error',
          id: 'call-1',
          name: 'read_file',
          params: { path: { segments: ['src'] } },
          error: {
            module: 'LanguageModel',
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
      const secondPrompt = yield* Ref.make<Prompt.Prompt | undefined>(
        undefined,
      );
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
          streamText: (options) =>
            Stream.unwrap(
              Ref.getAndUpdate(modelCalls, (count) => count + 1).pipe(
                Effect.tap((turn) =>
                  turn === 1
                    ? Ref.set(secondPrompt, options.prompt)
                    : Effect.void,
                ),
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

      // The model's second turn sees its invented call answered by a failed
      // tool result naming that tool.
      const seen = yield* Ref.get(secondPrompt);
      const returned = seen?.content
        .flatMap((message) => (message.role === 'tool' ? message.content : []))
        .find((part) => part.type === 'tool-result');
      expect(returned?.name).toBe('__proto__');
      expect(returned?.isFailure).toBe(true);
      const error = yield* Schema.decodeUnknownEffect(AiError.AiError)(
        returned?.result,
      );
      expect(error.reason._tag).toBe('ToolNotFoundError');
    }),
  );
});
