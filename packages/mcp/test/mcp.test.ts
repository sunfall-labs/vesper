import {
  LATEST_PROTOCOL_VERSION,
  type CallToolResult,
  type FetchLike,
  type JSONRPCMessage,
  type ListToolsResult,
  type Tool as McpTool,
  type Transport as ProtocolTransport,
} from '@modelcontextprotocol/client';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Fiber, Redacted, Stream } from 'effect';
import { TestClock } from 'effect/testing';
import { type Response as AiResponse, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '@sunfall/vesper-agent/agent';
import { DynamicToolkit } from '@sunfall/vesper-agent/dynamic-toolkit';
import { ScriptedModel } from '@sunfall/vesper-agent/testing';
import { Mcp, type ClientLike } from '../src/mcp.js';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const search: McpTool = {
  name: 'search.issues',
  title: 'Search issues',
  description: 'Search issue titles and descriptions.',
  inputSchema: {
    type: 'object',
    properties: { query: { type: 'string' } },
    required: ['query'],
  },
};

const create: McpTool = {
  name: 'create_issue',
  description: 'Create an issue.',
  inputSchema: {
    type: 'object',
    properties: { title: { type: 'string' } },
    required: ['title'],
  },
  annotations: { destructiveHint: true },
};

const listing = (tools: McpTool[]): ListToolsResult => ({ tools });
const result = (text: string, isError = false): CallToolResult => ({
  content: [{ type: 'text', text }],
  isError,
});

const modelSystem = (request: {
  readonly prompt: {
    readonly content: ReadonlyArray<{
      readonly role: string;
      readonly content: unknown;
    }>;
  };
}): string =>
  request.prompt.content
    .filter((message) => message.role === 'system')
    .map((message) => String(message.content))
    .join('\n');

const protocolTransport = (
  lifecycle: {
    opened: number;
    closed: number;
  },
  options?: {
    readonly closeFailure?: Error | undefined;
  },
): ProtocolTransport => {
  const instance: ProtocolTransport = {
    start: async () => {
      lifecycle.opened += 1;
    },
    send: async (message) => {
      if (!('id' in message) || !('method' in message)) return;
      let response: JSONRPCMessage;
      if (message.method === 'initialize') {
        response = {
          jsonrpc: '2.0',
          id: message.id,
          result: {
            protocolVersion: LATEST_PROTOCOL_VERSION,
            capabilities: { tools: {} },
            serverInfo: { name: 'test', version: '1.0.0' },
          },
        };
      } else if (message.method === 'tools/list') {
        response = {
          jsonrpc: '2.0',
          id: message.id,
          result: listing([search]),
        };
      } else {
        response = {
          jsonrpc: '2.0',
          id: message.id,
          error: { code: -32601, message: 'unsupported test method' },
        };
      }
      instance.onmessage?.(response);
    },
    close: async () => {
      lifecycle.closed += 1;
      if (options?.closeFailure !== undefined) throw options.closeFailure;
      instance.onclose?.();
    },
  };
  return instance;
};

/** A minimal JSON-RPC transport whose `tools/list` response is parameterized. */
const transportFor = (tools: McpTool[]): ProtocolTransport => {
  const instance: ProtocolTransport = {
    start: async () => {},
    send: async (message) => {
      if (!('id' in message) || !('method' in message)) return;
      let response: JSONRPCMessage;
      if (message.method === 'initialize') {
        response = {
          jsonrpc: '2.0',
          id: message.id,
          result: {
            protocolVersion: LATEST_PROTOCOL_VERSION,
            capabilities: { tools: {} },
            serverInfo: { name: 'test', version: '1.0.0' },
          },
        };
      } else if (message.method === 'tools/list') {
        response = { jsonrpc: '2.0', id: message.id, result: listing(tools) };
      } else {
        response = {
          jsonrpc: '2.0',
          id: message.id,
          error: { code: -32601, message: 'unsupported test method' },
        };
      }
      instance.onmessage?.(response);
    },
    close: async () => {
      instance.onclose?.();
    },
  };
  return instance;
};

const requestId = (body: BodyInit | null | undefined): string | number => {
  if (typeof body !== 'string') throw new Error('Expected a JSON request.');
  const parsed: unknown = JSON.parse(body);
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    !('id' in parsed) ||
    (typeof parsed.id !== 'string' && typeof parsed.id !== 'number')
  ) {
    throw new Error('Expected a JSON-RPC request id.');
  }
  return parsed.id;
};

const jsonResponse = (value: JSONRPCMessage): Response =>
  new Response(JSON.stringify(value), {
    headers: { 'content-type': 'application/json' },
  });

describe('Mcp', () => {
  it.effect(
    'discovers namespaced tools and calls the original remote name',
    () =>
      Effect.gen(function* () {
        const calls: Array<{ name: string; args: unknown }> = [];
        const client = {
          listTools: async () => listing([search, create]),
          callTool: async ({ name, arguments: args }) => {
            calls.push({ name, args });
            return result('VES-42');
          },
        } satisfies ClientLike;
        const source = Mcp.fromClient({
          name: 'linear.prod',
          client: Effect.succeed(client),
          tools: ['search.issues'],
        });

        const ready = yield* source.open;
        const handled = yield* ready
          .handle('mcp__linear_prod__search_issues', { query: 'vesper' })
          .pipe(Stream.unwrap, Stream.runCollect);

        expect(Object.keys(ready.tools)).toEqual([
          'mcp__linear_prod__search_issues',
        ]);
        expect(
          ready.tools.mcp__linear_prod__search_issues?.description,
        ).toContain('MCP tool "search.issues" from "linear.prod"');
        expect(Array.from(handled).at(-1)).toMatchObject({
          result: 'VES-42',
          isFailure: false,
        });
        expect(calls).toEqual([
          { name: 'search.issues', args: { query: 'vesper' } },
        ]);
      }).pipe(Effect.scoped),
  );

  it.effect('canonicalizes unfiltered tools and schemas', () =>
    Effect.gen(function* () {
      const searchWith = (
        properties: McpTool['inputSchema']['properties'],
        required: string[],
      ): McpTool => ({
        ...search,
        inputSchema: { type: 'object', properties, required },
      });
      const firstSearch = searchWith(
        {
          query: { type: 'string', description: 'Search text' },
          project: {
            type: 'object',
            properties: {
              owner: { type: 'string' },
              id: { type: 'string' },
            },
            required: ['owner', 'id'],
          },
        },
        ['query', 'project'],
      );
      const secondSearch = searchWith(
        {
          project: {
            required: ['id', 'owner'],
            properties: {
              id: { type: 'string' },
              owner: { type: 'string' },
            },
            type: 'object',
          },
          query: { description: 'Search text', type: 'string' },
        },
        ['project', 'query'],
      );
      const source = (tools: McpTool[]) =>
        Mcp.fromClient({
          name: 'linear',
          client: Effect.succeed({
            listTools: async () => listing(tools),
            callTool: async () => result('unused'),
          } satisfies ClientLike),
        });

      const [first, second] = yield* Effect.all(
        [
          source([firstSearch, create]).open,
          source([create, secondSearch]).open,
        ],
        { concurrency: 'unbounded' },
      );
      const definitions = (toolkit: Toolkit.WithHandler<Mcp.Tools>) =>
        Object.values(toolkit.tools).map((tool) => ({
          name: tool.name,
          schema: Tool.getJsonSchema(tool),
        }));

      expect(Object.keys(first.tools)).toEqual([
        'mcp__linear__create_issue',
        'mcp__linear__search_issues',
      ]);
      expect(Object.keys(second.tools)).toEqual(Object.keys(first.tools));
      expect(JSON.stringify(definitions(second))).toBe(
        JSON.stringify(definitions(first)),
      );
      expect(DynamicToolkit.resourceContext(first)).toContain(
        'mcp__linear__create_issue, mcp__linear__search_issues',
      );
    }).pipe(Effect.scoped),
  );

  it.effect(
    'orders unfiltered tools by sanitized names but preserves allowlists',
    () =>
      Effect.gen(function* () {
        const punctuationTools: McpTool[] = [
          { ...search, name: '!zeta' },
          { ...create, name: 'alpha' },
        ];
        const source = (allowlist?: string[]) =>
          Mcp.fromClient({
            name: 'linear',
            client: Effect.succeed({
              listTools: async () => listing(punctuationTools),
              callTool: async () => result('unused'),
            } satisfies ClientLike),
            ...(allowlist === undefined ? {} : { tools: allowlist }),
          });

        const [unfiltered, allowlisted] = yield* Effect.all([
          source().open,
          source(['!zeta', 'alpha']).open,
        ]);

        expect(Object.keys(unfiltered.tools)).toEqual([
          'mcp__linear__alpha',
          'mcp__linear__zeta',
        ]);
        expect(Object.keys(allowlisted.tools)).toEqual([
          'mcp__linear__zeta',
          'mcp__linear__alpha',
        ]);
      }).pipe(Effect.scoped),
  );

  it.effect('mounts into Agent as one stable snapshot for the run', () =>
    Effect.gen(function* () {
      const lifecycle = { opened: 0, closed: 0 };
      const client = {
        listTools: async () => listing([search]),
        callTool: async () => result('VES-42'),
      } satisfies ClientLike;
      const source = Mcp.fromClient({
        name: 'linear',
        client: Effect.acquireRelease(
          Effect.sync(() => {
            lifecycle.opened += 1;
            return client;
          }),
          () =>
            Effect.sync(() => {
              lifecycle.closed += 1;
            }),
        ),
      });
      const agent = Agent.make({
        name: 'mcp-consumer',
        revision: '1',
        instructions: 'Use Linear.',
        toolkit: Toolkit.make(),
        dynamicTools: [source],
      });
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call',
            id: 'mcp-call',
            name: 'mcp__linear__search_issues',
            params: { query: 'vesper' },
          },
          finish('tool-calls'),
        ] satisfies ReadonlyArray<AiResponse.StreamPartEncoded>,
        [
          { type: 'text-start', id: 'answer' },
          { type: 'text-delta', id: 'answer', delta: 'Found it.' },
          { type: 'text-end', id: 'answer' },
          finish(),
        ] satisfies ReadonlyArray<AiResponse.StreamPartEncoded>,
      ]);

      yield* agent.run('Find the issue.').pipe(Effect.provide(model.layer));
      const requests = yield* model.requests;

      expect(lifecycle).toEqual({ opened: 1, closed: 1 });
      expect(requests.map((request) => request.tools)).toEqual([
        ['mcp__linear__search_issues'],
        ['mcp__linear__search_issues'],
      ]);
    }),
  );

  it.effect('refreshes and announces the resource snapshot between runs', () =>
    Effect.gen(function* () {
      let discoveries = 0;
      const client = {
        listTools: async (_params, options) => {
          expect(options?.cacheMode).toBe('refresh');
          discoveries += 1;
          return listing(discoveries === 1 ? [search] : []);
        },
        callTool: async () => result('unused'),
      } satisfies ClientLike;
      const source = Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(client),
      });
      const agent = Agent.make({
        name: 'changing-mcp',
        revision: '1',
        instructions: 'Use current resources.',
        toolkit: Toolkit.make(),
        dynamicTools: [source],
      });
      const answer = [
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: 'Done.' },
        { type: 'text-end', id: 'answer' },
        finish(),
      ] satisfies ReadonlyArray<AiResponse.StreamPartEncoded>;
      const model = ScriptedModel.make([answer, answer]);

      yield* agent.run('First.').pipe(Effect.provide(model.layer));
      yield* agent.run('Second.').pipe(Effect.provide(model.layer));
      const requests = yield* model.requests;

      expect(requests.map((request) => request.tools)).toEqual([
        ['mcp__linear__search_issues'],
        [],
      ]);
      expect(modelSystem(requests[0]!)).toContain(
        'MCP server "linear": available; mcp__linear__search_issues.',
      );
      expect(modelSystem(requests[1]!)).toContain(
        'MCP server "linear": available; no tools.',
      );
    }).pipe(Effect.scoped),
  );

  it.effect('continues with a model-visible unavailable optional server', () =>
    Effect.gen(function* () {
      const source = Mcp.fromClient({
        name: 'linear',
        optional: true,
        client: Effect.succeed({
          listTools: async () => {
            throw new Error('offline');
          },
          callTool: async () => result('unused'),
        } satisfies ClientLike),
      });

      const ready = yield* source.open;

      expect(Object.keys(ready.tools)).toEqual([]);
      expect(DynamicToolkit.resourceContext(ready)).toContain(
        'MCP server "linear": unavailable; no tools.',
      );
    }).pipe(Effect.scoped),
  );

  it.effect('reuses cached clients until the Effect cache layer closes', () =>
    Effect.gen(function* () {
      const lifecycle = { opened: 0, closed: 0 };
      const definition = {
        name: 'linear',
        transport: Mcp.transport(() => protocolTransport(lifecycle)),
      } satisfies Mcp.Definition<'linear'>;
      const source = Mcp.cached(definition);
      const agent = Agent.make({
        name: 'cached-mcp',
        revision: '1',
        instructions: 'Use Linear.',
        toolkit: Toolkit.make(),
        dynamicTools: [source],
      });
      const answer = [
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: 'Done.' },
        { type: 'text-end', id: 'answer' },
        finish(),
      ] satisfies ReadonlyArray<AiResponse.StreamPartEncoded>;
      const model = ScriptedModel.make([answer, answer]);

      yield* Effect.gen(function* () {
        yield* agent.run('First.');
        yield* agent.run('Second.');
        expect(lifecycle).toEqual({ opened: 1, closed: 0 });
      }).pipe(
        Effect.provide(model.layer),
        Effect.provide(Mcp.layerConnectionCache({ idleTimeToLive: '1 hour' })),
      );

      expect(lifecycle).toEqual({ opened: 1, closed: 1 });
    }),
  );

  it.effect('logs and ignores a rejected client close', () =>
    Effect.gen(function* () {
      const lifecycle = { opened: 0, closed: 0 };
      const source = Mcp.make({
        name: 'linear',
        transport: Mcp.transport(() =>
          protocolTransport(lifecycle, {
            closeFailure: new Error('close failed'),
          }),
        ),
      });

      const exit = yield* source.open.pipe(Effect.scoped, Effect.exit);

      expect(exit._tag).toBe('Success');
      expect(lifecycle).toEqual({ opened: 1, closed: 1 });
    }),
  );

  it.effect('resolves bearer auth for every remote request', () =>
    Effect.gen(function* () {
      const authorization: Array<string | null> = [];
      const customHeaders: Array<string | null> = [];
      let token = 0;
      const fetch: FetchLike = async (_input, init) => {
        const headers = new Headers(init?.headers);
        authorization.push(headers.get('authorization'));
        customHeaders.push(headers.get('x-vesper-test'));
        const body = typeof init?.body === 'string' ? init.body : '';

        if (body.includes('"method":"initialize"')) {
          return jsonResponse({
            jsonrpc: '2.0',
            id: requestId(init?.body),
            result: {
              protocolVersion: LATEST_PROTOCOL_VERSION,
              capabilities: { tools: {} },
              serverInfo: { name: 'test', version: '1.0.0' },
            },
          });
        }
        if (body.includes('"method":"tools/list"')) {
          return jsonResponse({
            jsonrpc: '2.0',
            id: requestId(init?.body),
            result: listing([search]),
          });
        }
        return new Response(undefined, { status: 202 });
      };
      const source = Mcp.remote({
        name: 'linear',
        url: 'https://mcp.example.test',
        auth: () => Redacted.make(`token-${(token += 1)}`),
        headers: { 'x-vesper-test': 'present' },
        fetch,
      });

      const ready = yield* source.open;

      expect(Object.keys(ready.tools)).toEqual(['mcp__linear__search_issues']);
      expect(authorization.length).toBeGreaterThanOrEqual(3);
      expect(authorization).toEqual(
        authorization.map((_header, index) => `Bearer token-${index + 1}`),
      );
      expect(customHeaders).toEqual(authorization.map(() => 'present'));
    }).pipe(Effect.scoped),
  );

  it.effect('fails closed for unknown or repeated allowlist names', () =>
    Effect.gen(function* () {
      const client = {
        listTools: async () => listing([search]),
        callTool: async () => result('unused'),
      } satisfies ClientLike;
      const unknown = Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(client),
        tools: ['missing'],
      });
      const repeated = Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(client),
        tools: ['search.issues', 'search.issues'],
      });

      const unknownResult = yield* unknown.open.pipe(Effect.result);
      const repeatedResult = yield* repeated.open.pipe(Effect.result);

      expect(unknownResult._tag).toBe('Failure');
      expect(repeatedResult._tag).toBe('Failure');
    }).pipe(Effect.scoped),
  );

  it.effect('bounds untrusted discovery metadata before mounting tools', () =>
    Effect.gen(function* () {
      const tooMany = Mcp.fromClient({
        name: 'linear',
        limits: { maxTools: 1 },
        client: Effect.succeed({
          listTools: async () => listing([search, create]),
          callTool: async () => result('unused'),
        } satisfies ClientLike),
      });
      const malformedSchema = Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed({
          // A real remote response crosses a JSON boundary before the SDK's
          // static ClientLike contract can help us. Reparse here to exercise
          // the same hostile-wire case without a type assertion.
          listTools: async () =>
            listing([
              JSON.parse(
                JSON.stringify({
                  ...search,
                  inputSchema: { type: 'string' },
                }),
              ),
            ]),
          callTool: async () => result('unused'),
        } satisfies ClientLike),
      });

      const countResult = yield* tooMany.open.pipe(Effect.result);
      const schemaResult = yield* malformedSchema.open.pipe(Effect.result);

      expect(countResult._tag).toBe('Failure');
      expect(schemaResult._tag).toBe('Failure');
      if (countResult._tag === 'Failure') {
        expect(countResult.failure.message).toContain('LimitError');
      }
    }).pipe(Effect.scoped),
  );

  it('validates limits before evaluating a client connection', () => {
    let evaluated = false;
    expect(() =>
      Mcp.fromClient({
        name: 'linear',
        limits: { maxTools: 0 },
        client: Effect.sync(() => {
          evaluated = true;
          return {
            listTools: async () => listing([]),
            callTool: async () => result('unused'),
          } satisfies ClientLike;
        }),
      }),
    ).toThrow(/maxTools/);
    expect(evaluated).toBe(false);
  });

  it.effect(
    'returns typed limit failures for oversized arguments and results',
    () =>
      Effect.gen(function* () {
        const client = {
          listTools: async () => listing([search]),
          callTool: async () => result('x'.repeat(100)),
        } satisfies ClientLike;
        const ready = yield* Mcp.fromClient({
          name: 'linear',
          limits: { maxArgumentBytes: 32, maxResultBytes: 32 },
          client: Effect.succeed(client),
        }).open;

        const argumentEvents = yield* ready
          .handle('mcp__linear__search_issues', { query: 'x'.repeat(100) })
          .pipe(Stream.unwrap, Stream.runCollect);
        const resultEvents = yield* ready
          .handle('mcp__linear__search_issues', { query: 'ok' })
          .pipe(Stream.unwrap, Stream.runCollect);

        expect(Array.from(argumentEvents).at(-1)).toMatchObject({
          isFailure: true,
          result: {
            _tag: 'LimitError',
            resource: 'tool-arguments',
            limit: 32,
          },
        });
        expect(Array.from(resultEvents).at(-1)).toMatchObject({
          isFailure: true,
          result: {
            _tag: 'LimitError',
            resource: 'tool-result',
            limit: 32,
          },
        });
      }).pipe(Effect.scoped),
  );

  it.effect('passes configured timeouts and aborts slow MCP calls', () =>
    Effect.gen(function* () {
      let started = false;
      const ready = yield* Mcp.fromClient({
        name: 'linear',
        timeout: 1,
        client: Effect.succeed({
          listTools: async () => listing([search]),
          callTool: async (_params, options) => {
            started = true;
            expect(options?.timeout).toBe(1);
            return new Promise<CallToolResult>((_resolve, reject) => {
              options?.signal?.addEventListener(
                'abort',
                () => reject(new Error('aborted')),
                { once: true },
              );
            });
          },
        } satisfies ClientLike),
      }).open;

      const fiber = yield* ready
        .handle('mcp__linear__search_issues', { query: 'vesper' })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.forkChild);

      yield* Effect.repeat(Effect.yieldNow, { until: () => started });
      yield* TestClock.adjust('1 millis');
      const events = yield* Fiber.join(fiber);

      expect(started).toBe(true);
      expect(Array.from(events).at(-1)).toMatchObject({ isFailure: true });
    }).pipe(Effect.scoped),
  );

  it.effect('propagates interruption to the MCP request signal', () =>
    Effect.gen(function* () {
      let started = false;
      let aborted = false;
      const client = {
        listTools: async () => listing([search]),
        callTool: (_params, options) =>
          new Promise<CallToolResult>((_resolve, reject) => {
            started = true;
            if (options?.signal?.aborted === true) {
              aborted = true;
              reject(new Error('aborted'));
              return;
            }
            options?.signal?.addEventListener(
              'abort',
              () => {
                aborted = true;
                reject(new Error('aborted'));
              },
              { once: true },
            );
          }),
      } satisfies ClientLike;
      const source = Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(client),
      });
      const ready = yield* source.open;
      const fiber = yield* ready
        .handle('mcp__linear__search_issues', { query: 'vesper' })
        .pipe(Stream.unwrap, Stream.runDrain, Effect.forkChild);

      yield* Effect.repeat(Effect.yieldNow, {
        until: () => started,
      });
      yield* Fiber.interrupt(fiber);

      expect(aborted).toBe(true);
    }).pipe(Effect.scoped),
  );
});

describe('Mcp tool fingerprinting and drift detection', () => {
  const driftedDescription: McpTool = {
    ...search,
    description: 'Completely different behavior now.',
  };
  const driftedSchema: McpTool = {
    ...search,
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string' },
        limit: { type: 'number' },
      },
      required: ['query', 'limit'],
    },
  };
  const reorderedSearch: McpTool = {
    ...search,
    inputSchema: {
      required: ['query'],
      type: 'object',
      properties: { query: { type: 'string' } },
    },
  };

  const clientFor = (tool: McpTool) =>
    ({
      listTools: async () => listing([tool]),
      callTool: async () => result('unused'),
    }) satisfies ClientLike;

  const fingerprintsFor = (tool: McpTool) =>
    Mcp.fingerprints({
      name: 'linear',
      transport: Mcp.transport(() => transportFor([tool])),
    }).pipe(Effect.scoped);

  it.effect('is stable across schema key order and repeated discovery', () =>
    Effect.gen(function* () {
      const first = yield* fingerprintsFor(search);
      const reordered = yield* fingerprintsFor(reorderedSearch);
      const again = yield* fingerprintsFor(search);

      expect(Object.keys(first)).toEqual(['search.issues']);
      expect(first['search.issues']).toMatch(/^[0-9a-f]{64}$/);
      expect(reordered).toEqual(first);
      expect(again).toEqual(first);
    }),
  );

  it.effect('changes when a tool description changes', () =>
    Effect.gen(function* () {
      const original = yield* fingerprintsFor(search);
      const drifted = yield* fingerprintsFor(driftedDescription);

      expect(drifted['search.issues']).not.toBe(original['search.issues']);
    }),
  );

  it.effect('changes when a tool input schema changes', () =>
    Effect.gen(function* () {
      const original = yield* fingerprintsFor(search);
      const drifted = yield* fingerprintsFor(driftedSchema);

      expect(drifted['search.issues']).not.toBe(original['search.issues']);
    }),
  );

  it.effect(
    "'reject' (the default) excludes a tool whose description drifted",
    () =>
      Effect.gen(function* () {
        const pins = yield* fingerprintsFor(search);
        const ready = yield* Mcp.fromClient({
          name: 'linear',
          client: Effect.succeed(clientFor(driftedDescription)),
          toolDrift: { fingerprints: pins },
        }).open;

        expect(Object.keys(ready.tools)).toEqual([]);
      }).pipe(Effect.scoped),
  );

  it.effect("'reject' excludes a tool whose input schema drifted", () =>
    Effect.gen(function* () {
      const pins = yield* fingerprintsFor(search);
      const ready = yield* Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(clientFor(driftedSchema)),
        toolDrift: { fingerprints: pins },
      }).open;

      expect(Object.keys(ready.tools)).toEqual([]);
    }).pipe(Effect.scoped),
  );

  it.effect("'warn' keeps a drifted tool available", () =>
    Effect.gen(function* () {
      const pins = yield* fingerprintsFor(search);
      const ready = yield* Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(clientFor(driftedDescription)),
        toolDrift: { fingerprints: pins, onDrift: 'warn' },
      }).open;

      expect(Object.keys(ready.tools)).toEqual(['mcp__linear__search_issues']);
    }).pipe(Effect.scoped),
  );

  it.effect('a matching pin leaves the tool available', () =>
    Effect.gen(function* () {
      const pins = yield* fingerprintsFor(search);
      const ready = yield* Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(clientFor(search)),
        toolDrift: { fingerprints: pins },
      }).open;

      expect(Object.keys(ready.tools)).toEqual(['mcp__linear__search_issues']);
    }).pipe(Effect.scoped),
  );

  it.effect(
    'a tool absent from pinned fingerprints is trusted on first discovery',
    () =>
      Effect.gen(function* () {
        const ready = yield* Mcp.fromClient({
          name: 'linear',
          client: Effect.succeed(clientFor(driftedDescription)),
          toolDrift: { fingerprints: {} },
        }).open;

        expect(Object.keys(ready.tools)).toEqual([
          'mcp__linear__search_issues',
        ]);
      }).pipe(Effect.scoped),
  );

  it.effect(
    'omitting toolDrift leaves discovery unchanged for what would be a drifted tool',
    () =>
      Effect.gen(function* () {
        const ready = yield* Mcp.fromClient({
          name: 'linear',
          client: Effect.succeed(clientFor(driftedDescription)),
        }).open;

        expect(Object.keys(ready.tools)).toEqual([
          'mcp__linear__search_issues',
        ]);
      }).pipe(Effect.scoped),
  );
});
