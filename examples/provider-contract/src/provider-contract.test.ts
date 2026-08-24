import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic';
import { OpenAiClient, OpenAiLanguageModel } from '@effect/ai-openai';
import {
  OpenRouterClient,
  OpenRouterLanguageModel,
} from '@effect/ai-openrouter';
import { describe, expect, it } from '@effect/vitest';
import { Agent } from '@sunfall/vesper-agent/agent';
import { Compaction } from '@sunfall/vesper-agent/compaction';
import { ModelPlan } from '@sunfall/vesper-agent/model-plan';
import { Effect, ExecutionPlan, Layer, Schema, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  Prompt,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import {
  HttpClient,
  HttpClientError,
  HttpClientResponse,
  type HttpClientRequest,
} from 'effect/unstable/http';

type Reply =
  | {
      readonly status: number;
      readonly body: string;
      readonly headers?: Readonly<Record<string, string>>;
    }
  | { readonly transport: string };

const decoder = new TextDecoder();

const present = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('Expected provider-contract fixture value to be present');
  }
  return value;
};

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const requestJson = (
  request: HttpClientRequest.HttpClientRequest,
): Readonly<Record<string, unknown>> | undefined => {
  if (request.body._tag !== 'Uint8Array') {
    return undefined;
  }
  const value: unknown = JSON.parse(decoder.decode(request.body.body));
  return isRecord(value) ? value : undefined;
};

const fakeHttp = (replies: ReadonlyArray<Reply>) => {
  const requests: HttpClientRequest.HttpClientRequest[] = [];
  let index = 0;
  const client = HttpClient.make((request) =>
    Effect.suspend(() => {
      requests.push(request);
      const reply = present(replies[Math.min(index++, replies.length - 1)]);
      if ('transport' in reply) {
        return Effect.fail(
          new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({
              request,
              description: reply.transport,
            }),
          }),
        );
      }
      return Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new Response(reply.body, {
            status: reply.status,
            ...(reply.headers === undefined ? {} : { headers: reply.headers }),
          }),
        ),
      );
    }),
  );
  return { requests, layer: Layer.succeed(HttpClient.HttpClient, client) };
};

const sse = (...events: ReadonlyArray<unknown>): string =>
  events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join('');

const anthropicMessage = {
  id: 'msg_fixture',
  type: 'message',
  role: 'assistant',
  content: [],
  model: 'claude-fixture',
  stop_reason: null,
  stop_sequence: null,
  usage: {
    cache_creation: null,
    cache_creation_input_tokens: 3,
    cache_read_input_tokens: 5,
    inference_geo: null,
    input_tokens: 11,
    output_tokens: 0,
    service_tier: 'standard',
  },
} as const;

const anthropicSuccess = sse(
  { type: 'message_start', message: anthropicMessage },
  {
    type: 'content_block_start',
    index: 0,
    content_block: { type: 'text', text: '' },
  },
  {
    type: 'content_block_delta',
    index: 0,
    delta: { type: 'text_delta', text: 'hel' },
  },
  {
    type: 'content_block_delta',
    index: 0,
    delta: { type: 'text_delta', text: 'lo' },
  },
  { type: 'content_block_stop', index: 0 },
  {
    type: 'message_delta',
    delta: { stop_reason: 'end_turn', stop_sequence: null },
    usage: {
      cache_creation_input_tokens: 3,
      cache_read_input_tokens: 5,
      input_tokens: 11,
      output_tokens: 7,
    },
  },
  { type: 'message_stop' },
);

const openAiResponse = {
  id: 'resp_fixture',
  object: 'response',
  model: 'gpt-fixture',
  created_at: 1_700_000_000,
  output: [],
  usage: {
    input_tokens: 17,
    output_tokens: 9,
    total_tokens: 26,
    input_tokens_details: { cached_tokens: 4 },
    output_tokens_details: { reasoning_tokens: 2 },
  },
  error: null,
  incomplete_details: null,
} as const;

const openAiSuccess = sse(
  { type: 'response.created', response: openAiResponse, sequence_number: 0 },
  {
    type: 'response.output_item.added',
    output_index: 0,
    sequence_number: 1,
    item: {
      id: 'item_fixture',
      type: 'message',
      role: 'assistant',
      content: [],
      status: 'in_progress',
    },
  },
  {
    type: 'response.output_text.delta',
    item_id: 'item_fixture',
    output_index: 0,
    content_index: 0,
    delta: 'wor',
    sequence_number: 2,
  },
  {
    type: 'response.output_text.delta',
    item_id: 'item_fixture',
    output_index: 0,
    content_index: 0,
    delta: 'ld',
    sequence_number: 3,
  },
  {
    type: 'response.output_item.done',
    output_index: 0,
    sequence_number: 4,
    item: {
      id: 'item_fixture',
      type: 'message',
      role: 'assistant',
      content: [{ type: 'output_text', text: 'world', annotations: [] }],
      status: 'completed',
    },
  },
  { type: 'response.completed', response: openAiResponse, sequence_number: 5 },
);

const openRouterSuccess =
  sse(
    {
      id: 'gen_fixture',
      object: 'chat.completion.chunk',
      created: 1_700_000_000,
      model: 'openrouter-fixture',
      choices: [
        {
          index: 0,
          delta: { role: 'assistant', content: 'rout' },
          finish_reason: null,
        },
      ],
    },
    {
      id: 'gen_fixture',
      object: 'chat.completion.chunk',
      created: 1_700_000_000,
      model: 'openrouter-fixture',
      choices: [
        {
          index: 0,
          delta: { content: 'er' },
          finish_reason: 'stop',
        },
      ],
    },
  ) + 'data: [DONE]\n\n';

const openRouterNumericStringToolCall =
  sse(
    {
      id: 'gen_tool_fixture',
      object: 'chat.completion.chunk',
      created: 1_700_000_000,
      model: 'openrouter-fixture',
      choices: [
        {
          index: 0,
          delta: {
            role: 'assistant',
            tool_calls: [
              {
                index: 0,
                id: 'call_charge_fixture',
                type: 'function',
                function: {
                  name: 'charge_card',
                  arguments: '{"amountCents":"4999"}',
                },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    },
    {
      id: 'gen_tool_fixture',
      object: 'chat.completion.chunk',
      created: 1_700_000_000,
      model: 'openrouter-fixture',
      choices: [
        {
          index: 0,
          delta: {},
          finish_reason: 'tool_calls',
        },
      ],
    },
  ) + 'data: [DONE]\n\n';

const lookup = Tool.make('lookup', {
  description: 'look up a deterministic value',
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Struct({ value: Schema.String }),
});
const toolkit = Toolkit.make(lookup);
const handlers = toolkit.toLayer({
  lookup: ({ key }) => Effect.succeed({ value: key }),
});
const agent = Agent.make({
  name: 'provider-contract',
  revision: '1',
  instructions: 'Answer using the lookup tool when needed.',
  toolkit,
  compaction: false,
});

const chargeCard = Tool.make('charge_card', {
  description: "Charge the customer's card, in cents.",
  parameters: Schema.Struct({
    amountCents: Schema.Union([Schema.Finite, Schema.FiniteFromString]),
  }),
  success: Schema.Struct({ authorization: Schema.String }),
});
const chargeToolkit = Toolkit.make(chargeCard);
const chargeHandlers = chargeToolkit.toLayer({
  charge_card: () => Effect.succeed({ authorization: 'approved' }),
});

const anthropicLayer = (
  http: Layer.Layer<HttpClient.HttpClient>,
  transformClient?: (client: HttpClient.HttpClient) => HttpClient.HttpClient,
) =>
  AnthropicLanguageModel.model('claude-fixture', { max_tokens: 64 }).pipe(
    Layer.provide(
      AnthropicClient.layer({
        apiUrl: 'https://anthropic.invalid',
        transformClient,
      }),
    ),
    Layer.provide(http),
  );

const openAiLayer = (
  http: Layer.Layer<HttpClient.HttpClient>,
  transformClient?: (client: HttpClient.HttpClient) => HttpClient.HttpClient,
) =>
  OpenAiLanguageModel.model('gpt-fixture', { max_output_tokens: 64 }).pipe(
    Layer.provide(
      OpenAiClient.layer({
        apiUrl: 'https://openai.invalid/v1',
        transformClient,
      }),
    ),
    Layer.provide(http),
  );

const openRouterLayer = (http: Layer.Layer<HttpClient.HttpClient>) =>
  OpenRouterLanguageModel.model('openrouter-fixture', {
    max_tokens: 64,
  }).pipe(
    Layer.provide(
      OpenRouterClient.layer({ apiUrl: 'https://openrouter.invalid/v1' }),
    ),
    Layer.provide(http),
  );

const fallbackProviderLayer = (
  onFailure: (error: AiError.AiError) => void,
): Layer.Layer<
  LanguageModel.LanguageModel,
  never,
  AnthropicClient.AnthropicClient | OpenAiClient.OpenAiClient
> =>
  ModelPlan.layer(
    ExecutionPlan.make(
      {
        provide: AnthropicLanguageModel.model('claude-fixture', {
          max_tokens: 64,
        }),
        attempts: 2,
        while: ModelPlan.when((error) => {
          onFailure(error);
          return true;
        }),
      },
      {
        provide: OpenAiLanguageModel.model('gpt-fixture', {
          max_output_tokens: 64,
        }),
      },
    ),
  );

const runAgent = (
  model: Layer.Layer<LanguageModel.LanguageModel, never, never>,
) =>
  agent
    .stream('Find sun')
    .pipe(
      Stream.runCollect,
      Effect.provide(Layer.merge(model, handlers)),
      Effect.orDie,
    );

const failureOf = (
  model: Layer.Layer<LanguageModel.LanguageModel, never, never>,
): Effect.Effect<AiError.AiError> =>
  Effect.gen(function* () {
    const result = yield* agent
      .stream('overflow')
      .pipe(
        Stream.runDrain,
        Effect.provide(Layer.merge(model, handlers)),
        Effect.result,
      );
    if (result._tag === 'Success') {
      throw new Error('expected provider failure');
    }
    if (!AiError.isAiError(result.failure)) {
      throw new Error('expected an AiError provider failure');
    }
    return result.failure;
  });

type OverflowCase = readonly [
  name: string,
  makeLayer: typeof anthropicLayer,
  body: Record<string, unknown>,
];

type StructuredErrorCase = readonly [
  name: string,
  makeLayer: typeof anthropicLayer,
  body: string,
  code: string,
  codeField: string,
  requestId: string | undefined,
];

const textAndCompleted = (
  events: Iterable<{ readonly _tag: string; readonly [key: string]: unknown }>,
) => {
  let text = '';
  let completed: unknown;
  for (const event of events) {
    if (event._tag === 'Part') {
      const part = event['part'];
      if (
        isRecord(part) &&
        part['type'] === 'text-delta' &&
        typeof part['delta'] === 'string'
      ) {
        text += part['delta'];
      }
    }
    if (event._tag === 'Completed') {
      completed = event;
    }
  }
  return { text, completed };
};

describe('official Effect provider seam', () => {
  it.effect(
    'runs Anthropic request conversion and SSE parsing through one Vesper turn',
    () =>
      Effect.gen(function* () {
        const fake = fakeHttp([{ status: 200, body: anthropicSuccess }]);
        const events = yield* runAgent(anthropicLayer(fake.layer));
        const observed = textAndCompleted(events);
        const request = present(fake.requests[0]);
        const body = present(requestJson(request));

        expect(request.url).toBe('https://anthropic.invalid/v1/messages');
        expect(body).toMatchObject({
          model: 'claude-fixture',
          max_tokens: 64,
          stream: true,
          system: [
            { type: 'text', text: 'Answer using the lookup tool when needed.' },
          ],
          messages: [
            { role: 'user', content: [{ type: 'text', text: 'Find sun' }] },
          ],
          tools: [
            { name: 'lookup', description: 'look up a deterministic value' },
          ],
        });
        expect(observed.text).toBe('hello');
        expect(observed.completed).toMatchObject({
          usage: { input: 19, output: 7 },
        });
      }),
  );

  it.effect(
    'runs OpenAI request conversion and SSE parsing through one Vesper turn',
    () =>
      Effect.gen(function* () {
        const fake = fakeHttp([{ status: 200, body: openAiSuccess }]);
        const events = yield* runAgent(openAiLayer(fake.layer));
        const observed = textAndCompleted(events);
        const request = present(fake.requests[0]);
        const body = present(requestJson(request));

        expect(request.url).toBe('https://openai.invalid/v1/responses');
        expect(body).toMatchObject({
          model: 'gpt-fixture',
          max_output_tokens: 64,
          stream: true,
          input: [
            {
              role: 'system',
              content: [
                {
                  type: 'input_text',
                  text: 'Answer using the lookup tool when needed.',
                },
              ],
            },
            {
              role: 'user',
              content: [{ type: 'input_text', text: 'Find sun' }],
            },
          ],
          tools: [
            {
              type: 'function',
              name: 'lookup',
              description: 'look up a deterministic value',
            },
          ],
        });
        expect(observed.text).toBe('world');
        expect(observed.completed).toMatchObject({
          usage: { input: 17, output: 9 },
        });
      }),
  );

  it.effect(
    'replays assistant text through the native OpenRouter chat contract',
    () =>
      Effect.gen(function* () {
        const fake = fakeHttp([{ status: 200, body: openRouterSuccess }]);
        const prompt = Prompt.make([
          Prompt.makeMessage('system', {
            content: 'Remember the corrected shipment id.',
          }),
          Prompt.makeMessage('user', {
            content: [Prompt.makePart('text', { text: 'The id is ALPHA.' })],
          }),
          Prompt.makeMessage('assistant', {
            content: [Prompt.makePart('text', { text: 'The id is ALPHA.' })],
          }),
          Prompt.makeMessage('user', {
            content: [
              Prompt.makePart('text', {
                text: 'Correction: the id is BETA.',
              }),
            ],
          }),
        ]);

        const parts = yield* LanguageModel.streamText({ prompt }).pipe(
          Stream.runCollect,
          Effect.provide(openRouterLayer(fake.layer)),
        );
        const request = present(fake.requests[0]);
        const body = present(requestJson(request));

        expect(request.url).toBe(
          'https://openrouter.invalid/v1/chat/completions',
        );
        expect(body).toMatchObject({
          model: 'openrouter-fixture',
          stream: true,
          messages: [
            {
              role: 'system',
              content: [
                {
                  type: 'text',
                  text: 'Remember the corrected shipment id.',
                },
              ],
            },
            { role: 'user', content: 'The id is ALPHA.' },
            { role: 'assistant', content: 'The id is ALPHA.' },
            { role: 'user', content: 'Correction: the id is BETA.' },
          ],
        });
        expect(
          Array.from(parts)
            .filter((part) => part.type === 'text-delta')
            .map((part) => part.delta)
            .join(''),
        ).toBe('router');
      }),
  );

  it.effect(
    'decodes numeric-string OpenRouter tool arguments before handler dispatch',
    () =>
      Effect.gen(function* () {
        const fake = fakeHttp([
          { status: 200, body: openRouterNumericStringToolCall },
        ]);

        const parts = yield* LanguageModel.streamText({
          prompt: 'Charge 4999 cents.',
          toolkit: chargeToolkit,
        }).pipe(
          Stream.runCollect,
          Effect.provide(
            Layer.merge(openRouterLayer(fake.layer), chargeHandlers),
          ),
        );

        expect(
          Array.from(parts).find((part) => part.type === 'tool-call'),
        ).toMatchObject({
          name: 'charge_card',
          params: { amountCents: 4999 },
        });
        expect(
          Array.from(parts).find((part) => part.type === 'tool-result'),
        ).toMatchObject({
          name: 'charge_card',
          result: { authorization: 'approved' },
        });
        expect(
          yield* Schema.encodeEffect(chargeCard.parametersSchema)({
            amountCents: 4999,
          }),
        ).toEqual({ amountCents: 4999 });
      }),
  );

  it.effect(
    'retries then falls back between official providers without losing client requirements',
    () =>
      Effect.gen(function* () {
        const fake = fakeHttp([
          {
            status: 500,
            body: JSON.stringify({
              type: 'error',
              error: { type: 'api_error', message: 'primary unavailable' },
            }),
          },
          {
            status: 500,
            body: JSON.stringify({
              type: 'error',
              error: { type: 'api_error', message: 'still unavailable' },
            }),
          },
          { status: 200, body: openAiSuccess },
        ]);
        const failures: AiError.AiError[] = [];
        const clients = Layer.mergeAll(
          AnthropicClient.layer({ apiUrl: 'https://anthropic.invalid' }),
          OpenAiClient.layer({ apiUrl: 'https://openai.invalid/v1' }),
        ).pipe(Layer.provide(fake.layer));
        const model = fallbackProviderLayer((error) =>
          failures.push(error),
        ).pipe(Layer.provide(clients));

        const events = yield* runAgent(model);

        expect(textAndCompleted(events).text).toBe('world');
        expect(fake.requests.map((request) => request.url)).toEqual([
          'https://anthropic.invalid/v1/messages',
          'https://anthropic.invalid/v1/messages',
          'https://openai.invalid/v1/responses',
        ]);
        expect(failures).toHaveLength(2);
        expect(failures.map((error) => error.reason._tag)).toEqual([
          'InternalProviderError',
          'InternalProviderError',
        ]);
      }),
    30_000,
  );

  it.effect.each([
    [
      'Anthropic',
      anthropicLayer,
      {
        type: 'error',
        error: { type: 'invalid_request_error', message: 'prompt is too long' },
      },
    ],
    [
      'OpenAI',
      openAiLayer,
      {
        error: {
          message: 'maximum context length exceeded',
          type: 'invalid_request_error',
          code: 'context_length_exceeded',
        },
      },
    ],
  ] as const)(
    'recognizes a real %s client overflow error but not an unrelated 400',
    (row: OverflowCase, _context: unknown) =>
      Effect.gen(function* () {
        const [_name, makeLayer, overflowBody] = row;
        const overflow = fakeHttp([
          { status: 400, body: JSON.stringify(overflowBody) },
        ]);
        const overflowError = yield* failureOf(makeLayer(overflow.layer));
        expect(overflowError.reason._tag).toBe('InvalidRequestError');
        expect(Compaction.isContextOverflow(overflowError)).toBe(true);

        const other = fakeHttp([
          {
            status: 400,
            body: JSON.stringify(
              'type' in overflowBody
                ? {
                    type: 'error',
                    error: {
                      type: 'invalid_request_error',
                      message: 'tool schema is invalid',
                    },
                  }
                : {
                    error: {
                      message: 'tool schema is invalid',
                      type: 'invalid_request_error',
                      code: 'invalid_tool',
                    },
                  },
            ),
          },
        ]);
        expect(
          Compaction.isContextOverflow(
            yield* failureOf(makeLayer(other.layer)),
          ),
        ).toBe(false);
        expect(other.requests).toHaveLength(1);
        return Effect.void;
      }),
    30_000,
  );

  it.effect.each([
    [
      'Anthropic',
      anthropicLayer,
      sse({
        type: 'error',
        error: { type: 'overloaded_error', message: 'capacity exhausted' },
        request_id: 'req_anthropic',
      }),
      'overloaded_error',
      'type',
      'req_anthropic',
    ],
    [
      'OpenAI',
      openAiLayer,
      sse({
        type: 'error',
        code: 'server_error',
        message: 'capacity exhausted',
        param: null,
        sequence_number: 0,
        status: 500,
      }),
      'server_error',
      'code',
      undefined,
    ],
  ] as const)(
    'preserves %s in-band structured error details',
    (row: StructuredErrorCase, _context: unknown) =>
      Effect.gen(function* () {
        const [_name, makeLayer, body, code, codeField, requestId] = row;
        const fake = fakeHttp([{ status: 200, body }]);
        const error = yield* failureOf(makeLayer(fake.layer));
        expect(error.reason._tag).toBe('UnknownError');
        if (error.reason._tag !== 'UnknownError') {
          throw new Error('expected normalized provider error');
        }
        expect(error.reason.description).toContain(code);
        expect(error.reason.description).toContain('capacity exhausted');
        expect(error.reason.description).not.toContain('[object Object]');
        expect(error.reason.metadata).toMatchObject({ [codeField]: code });
        if (requestId !== undefined) {
          expect(error.reason.metadata).toMatchObject({
            anthropic: { requestId },
          });
        }
        return Effect.void;
      }),
    30_000,
  );

  it.effect(
    'retries one Anthropic 429 response below SSE parsing without duplicate output',
    () =>
      Effect.gen(function* () {
        const fake = fakeHttp([
          {
            status: 429,
            body: JSON.stringify({
              type: 'error',
              error: { type: 'rate_limit_error', message: 'slow down' },
            }),
          },
          { status: 200, body: anthropicSuccess },
        ]);
        const events = yield* runAgent(
          anthropicLayer(fake.layer, HttpClient.retryTransient({ times: 1 })),
        );
        expect(fake.requests).toHaveLength(2);
        expect(textAndCompleted(events).text).toBe('hello');
      }),
  );

  it.effect(
    'retries one OpenAI transport error below SSE parsing without duplicate output',
    () =>
      Effect.gen(function* () {
        const fake = fakeHttp([
          { transport: 'connection reset' },
          { status: 200, body: openAiSuccess },
        ]);
        const events = yield* runAgent(
          openAiLayer(fake.layer, HttpClient.retryTransient({ times: 1 })),
        );
        expect(fake.requests).toHaveLength(2);
        expect(textAndCompleted(events).text).toBe('world');
      }),
  );

  it.effect.each([
    [
      'Anthropic',
      anthropicLayer,
      {
        type: 'error',
        error: { type: 'invalid_request_error', message: 'bad request' },
      },
    ],
    [
      'OpenAI',
      openAiLayer,
      {
        error: {
          message: 'bad request',
          type: 'invalid_request_error',
          code: 'bad_request',
        },
      },
    ],
  ] as const)(
    'does not retry a %s 400',
    (row: OverflowCase, _context: unknown) =>
      Effect.gen(function* () {
        const [_name, makeLayer, body] = row;
        const fake = fakeHttp([
          { status: 400, body: JSON.stringify(body) },
          { status: 200, body: anthropicSuccess },
        ]);
        yield* failureOf(
          makeLayer(fake.layer, HttpClient.retryTransient({ times: 1 })),
        );
        expect(fake.requests).toHaveLength(1);
        return Effect.void;
      }),
    30_000,
  );
});
