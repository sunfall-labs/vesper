import {
  fauxAssistantMessage,
  fauxProvider,
  fauxText,
  fauxThinking,
  fauxToolCall,
  type FauxProviderHandle,
} from '@earendil-works/pi-ai/providers/faux';
import { Effect, Layer, Schema, Stream } from 'effect';
import { LanguageModel, Tool, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { CredentialStore } from '../src/credentials.js';
import { DEFAULT_STREAM_OPTIONS, PiModel } from '../src/model.js';
import { PiRegistry } from '../src/registry.js';

// End-to-end through the assembled `LanguageModel`: prompt in, Pi's event
// protocol out, Effect response parts back. The unit tests cover the pure
// translation functions in isolation; this covers the wiring between them,
// which is where an adapter usually breaks.
//
// Pi's own faux provider drives it, so there is no network and no fake of
// our own construction sitting between the test and the code under test.

const withFaux = <A>(
  responses: Parameters<FauxProviderHandle['setResponses']>[0],
  program: (
    handle: FauxProviderHandle,
  ) => Effect.Effect<A, unknown, LanguageModel.LanguageModel>,
): Promise<A> => {
  const handle = fauxProvider({
    provider: 'faux',
    models: [{ id: 'faux-1' }],
    // Emit the whole response in one chunk: chunk boundaries are provider
    // timing artifacts and asserting on them would test Pi, not this code.
    tokensPerSecond: 0,
  });
  handle.setResponses(responses);

  const registry = PiRegistry.layer({
    register: (models) =>
      Effect.sync(() => models.setProvider(handle.provider)),
  }).pipe(Layer.provide(CredentialStore.layerMemory));

  return Effect.runPromise(
    program(handle).pipe(
      Effect.provide(
        PiModel.model('faux', 'faux-1').pipe(Layer.provide(registry)),
      ),
    ) as Effect.Effect<A>,
  );
};

const lookupOrder = Tool.make('lookup_order', {
  description: 'Look up one order.',
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const lookupToolkit = Toolkit.make(lookupOrder);

const lookupHandlers = lookupToolkit.toLayer({
  lookup_order: () => Effect.succeed({ status: 'shipped' }),
});

describe('assembled LanguageModel', () => {
  it('returns model text as a response part', async () => {
    const parts = await withFaux(
      [fauxAssistantMessage('Order order_1042 shipped Tuesday.')],
      () =>
        Effect.gen(function* () {
          const response = yield* LanguageModel.generateText({
            prompt: 'where is order_1042?',
          });
          return response.content;
        }),
    );

    expect(parts).toContainEqual(
      expect.objectContaining({
        type: 'text',
        text: 'Order order_1042 shipped Tuesday.',
      }),
    );
  });

  it('carries the prompt through to the provider', async () => {
    const seen = await withFaux(
      [
        (context) =>
          fauxAssistantMessage(
            JSON.stringify({
              system: context.systemPrompt ?? null,
              messages: context.messages.length,
            }),
          ),
      ],
      () =>
        Effect.gen(function* () {
          const response = yield* LanguageModel.generateText({
            prompt: [
              { role: 'system', content: 'Be terse.' },
              { role: 'user', content: [{ type: 'text', text: 'hello' }] },
            ],
          });
          return response.text;
        }),
    );

    // The system message is lifted out of the message array into Pi's
    // dedicated `systemPrompt` field — the one structural difference
    // between the two conversation models.
    expect(JSON.parse(seen)).toEqual({ system: 'Be terse.', messages: 1 });
  });

  it('surfaces reasoning as a distinct part, not as text', async () => {
    const parts = await withFaux(
      [
        fauxAssistantMessage([
          fauxThinking('The order id looks like a shipped one.'),
          fauxText('It shipped.'),
        ]),
      ],
      () =>
        Effect.gen(function* () {
          const response = yield* LanguageModel.generateText({
            prompt: 'status?',
          });
          return response.content;
        }),
    );

    expect(parts.map((part) => part.type)).toEqual([
      'reasoning',
      'text',
      'finish',
    ]);
  });

  // Declaring a toolkit is not incidental to this test: with an empty
  // toolkit, `tool-call` is not a member of the response part union at all,
  // and emitting one fails schema validation. It also exercises the typebox
  // bridge — the tool's JSON Schema is derived from the same `Schema` that
  // types its parameters.
  it('exposes tool calls with parsed parameters', async () => {
    const parts = await withFaux(
      [
        fauxAssistantMessage(
          [fauxToolCall('lookup_order', { orderId: 'order_1042' })],
          { stopReason: 'toolUse' },
        ),
      ],
      () =>
        Effect.gen(function* () {
          const response = yield* LanguageModel.generateText({
            prompt: 'look it up',
            toolkit: yield* lookupToolkit,
          });
          return response.content;
        }).pipe(Effect.provide(lookupHandlers)),
    );

    expect(parts).toContainEqual(
      expect.objectContaining({
        type: 'tool-call',
        name: 'lookup_order',
        params: { orderId: 'order_1042' },
      }),
    );
  });

  it('resolves the tool call and feeds the result back as a part', async () => {
    const parts = await withFaux(
      [
        fauxAssistantMessage(
          [fauxToolCall('lookup_order', { orderId: 'order_1042' })],
          { stopReason: 'toolUse' },
        ),
      ],
      () =>
        Effect.gen(function* () {
          const response = yield* LanguageModel.generateText({
            prompt: 'look it up',
            toolkit: yield* lookupToolkit,
          });
          return response.content;
        }).pipe(Effect.provide(lookupHandlers)),
    );

    expect(parts).toContainEqual(
      expect.objectContaining({
        type: 'tool-result',
        name: 'lookup_order',
        result: { status: 'shipped' },
      }),
    );
  });

  it('reports usage on the finish part', async () => {
    const parts = await withFaux([fauxAssistantMessage('ok')], () =>
      Effect.gen(function* () {
        const response = yield* LanguageModel.generateText({ prompt: 'hi' });
        return response.content;
      }),
    );

    const finish = parts.find((part) => part.type === 'finish');
    expect(finish).toBeDefined();
    expect(finish).toMatchObject({
      usage: expect.objectContaining({ outputTokens: expect.anything() }),
    });
  });

  // The point of the whole package: a provider failure arrives in the error
  // channel as a typed, classified `AiError` instead of as an in-band event
  // carrying a string for every call site to re-parse.
  it('fails with a classified AiError instead of an in-band error part', async () => {
    const result = await withFaux(
      [
        fauxAssistantMessage('', {
          stopReason: 'error',
          errorMessage: 'status 429: rate limit exceeded, retry after 12',
        }),
      ],
      () => LanguageModel.generateText({ prompt: 'hi' }).pipe(Effect.result),
    );

    expect(result._tag).toBe('Failure');
    if (result._tag === 'Failure') {
      const error = result.failure as {
        reason: { _tag: string };
        isRetryable: boolean;
      };
      expect(error.reason._tag).toBe('RateLimitError');
      expect(error.isRetryable).toBe(true);
    }
  });

  it('streams the full lifecycle, not just a final value', async () => {
    const types = await withFaux([fauxAssistantMessage('streamed')], () =>
      LanguageModel.streamText({ prompt: 'hi' }).pipe(
        Stream.map((part) => part.type),
        Stream.runCollect,
      ),
    );

    expect(types).toContain('text-start');
    expect(types).toContain('text-delta');
    expect(types).toContain('text-end');
    expect(types[types.length - 1]).toBe('finish');
  });

  it('streams tool calls with the parameter grammar consumers key off', async () => {
    const types = await withFaux(
      [
        fauxAssistantMessage(
          [fauxToolCall('lookup_order', { orderId: 'order_1042' })],
          { stopReason: 'toolUse' },
        ),
      ],
      () =>
        Stream.unwrap(
          Effect.map(lookupToolkit, (toolkit) =>
            LanguageModel.streamText({ prompt: 'look it up', toolkit }),
          ),
        ).pipe(
          Stream.map((part) => part.type as string),
          Stream.runCollect,
          Effect.provide(lookupHandlers),
        ),
    );

    expect(types.indexOf('tool-params-start')).toBeGreaterThanOrEqual(0);
    expect(types.indexOf('tool-params-start')).toBeLessThan(
      types.indexOf('tool-call'),
    );
  });

  // Retrying is owned by one layer, and it is not this one. Pi's SDKs retry
  // internally by default, which would multiply against the policy above and
  // hide attempts from the layer that knows whether retrying is even safe.
  it("disables the provider SDK's own retries by default", async () => {
    const seen = await withFaux(
      [
        (_context, streamOptions) =>
          fauxAssistantMessage(
            JSON.stringify({ maxRetries: streamOptions?.maxRetries ?? null }),
          ),
      ],
      () =>
        Effect.gen(function* () {
          const response = yield* LanguageModel.generateText({ prompt: 'hi' });
          return response.text;
        }),
    );

    expect(JSON.parse(seen)).toEqual({ maxRetries: 0 });
    expect(DEFAULT_STREAM_OPTIONS.maxRetries).toBe(0);
  });

  it('lets a caller opt back into SDK retries', async () => {
    const handle = fauxProvider({
      provider: 'faux',
      models: [{ id: 'faux-1' }],
      tokensPerSecond: 0,
    });
    handle.setResponses([
      (_context, streamOptions) =>
        fauxAssistantMessage(String(streamOptions?.maxRetries ?? 'unset')),
    ]);

    const registry = PiRegistry.layer({
      register: (models) =>
        Effect.sync(() => models.setProvider(handle.provider)),
    }).pipe(Layer.provide(CredentialStore.layerMemory));

    const text = await Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* LanguageModel.generateText({ prompt: 'hi' });
        return response.text;
      }).pipe(
        Effect.provide(
          PiModel.model('faux', 'faux-1', {
            streamOptions: { maxRetries: 5 },
          }).pipe(Layer.provide(registry)),
        ),
      ) as Effect.Effect<string>,
    );

    expect(text).toBe('5');
  });
});
