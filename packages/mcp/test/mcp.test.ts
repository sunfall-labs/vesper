import {
  LATEST_PROTOCOL_VERSION,
  type CallToolResult,
  type FetchLike,
  type JSONRPCMessage,
  type Tool as McpTool,
  type Transport as ProtocolTransport,
} from '@modelcontextprotocol/client';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Fiber, Layer, Redacted, Schema, Stream } from 'effect';
import { TestClock } from 'effect/testing';
import { type Response as AiResponse } from 'effect/ai';
import { Toolkit } from 'effect/ai';

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

type Turn = ReadonlyArray<AiResponse.StreamPartEncoded>;

const answer: Turn = [
  { type: 'text-start', id: 'answer' },
  { type: 'text-delta', id: 'answer', delta: 'Done.' },
  { type: 'text-end', id: 'answer' },
  finish(),
];
const callTurn = (name: string, params: Record<string, unknown>): Turn => [
  { type: 'tool-call', id: `call-${name}`, name, params },
  finish('tool-calls'),
];

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

const text = (value: string, isError = false): CallToolResult => ({
  content: [{ type: 'text', text: value }],
  isError,
});

const systemPrompt = (request: {
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

/** An in-process MCP server speaking JSON-RPC over the SDK's transport seam. */
interface ServerOptions {
  readonly tools: () => ReadonlyArray<McpTool>;
  readonly call?: (name: string, args: unknown) => CallToolResult;
  readonly lifecycle?: { opened: number; closed: number };
  readonly closeFailure?: Error;
}
const server = (options: ServerOptions): ProtocolTransport => {
  const instance: ProtocolTransport = {
    start: async () => {
      if (options.lifecycle !== undefined) {
        options.lifecycle.opened += 1;
      }
    },
    send: async (message) => {
      if (!('id' in message) || !('method' in message)) {
        return;
      }
      const raw: Record<string, unknown> = message.params ?? {};
      const callName = typeof raw['name'] === 'string' ? raw['name'] : '';
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
          result: { tools: [...options.tools()] },
        };
      } else if (message.method === 'tools/call') {
        response = {
          jsonrpc: '2.0',
          id: message.id,
          result: (options.call ?? (() => text('ok')))(
            callName,
            raw['arguments'],
          ),
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
      if (options.lifecycle !== undefined) {
        options.lifecycle.closed += 1;
      }
      if (options.closeFailure !== undefined) {
        throw options.closeFailure;
      }
      instance.onclose?.();
    },
  };
  return instance;
};

const serving = (
  tools: ReadonlyArray<McpTool>,
  extra: Partial<ServerOptions> = {},
) => Mcp.transport(() => server({ tools: () => tools, ...extra }));

const agentFor = <R>(
  name: string,
  ...sources: Array<DynamicToolkit.ScopedSource<Mcp.Tools, R>>
) =>
  Agent.make({
    name,
    revision: '1',
    instructions: 'Use MCP.',
    toolkit: Toolkit.make(),
    dynamicTools: sources,
  });

const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.optionalKey(Schema.Unknown),
      method: Schema.optionalKey(Schema.String),
    }),
  ),
);

const names = (ready: { readonly tools: object }) => Object.keys(ready.tools);

describe('Mcp in an agent', () => {
  it.effect(
    'mounts a namespaced snapshot and calls the original remote name',
    () =>
      Effect.gen(function* () {
        const lifecycle = { opened: 0, closed: 0 };
        const calls: Array<{ name: string; args: unknown }> = [];
        const source = Mcp.make({
          name: 'linear.prod',
          tools: ['search.issues'],
          transport: serving([search, create], {
            lifecycle,
            call: (name, args) => {
              calls.push({ name, args });
              return text('VES-42');
            },
          }),
        });
        const model = ScriptedModel.make([
          callTurn('mcp__linear_prod__search_issues', { query: 'vesper' }),
          answer,
        ]);

        yield* agentFor('mcp-consumer', source)
          .run('Find the issue.')
          .pipe(Effect.provide(model.layer));
        const requests = yield* model.requests;

        expect(calls).toEqual([
          { name: 'search.issues', args: { query: 'vesper' } },
        ]);
        expect(lifecycle).toEqual({ opened: 1, closed: 1 });
        expect(requests.map((request) => request.tools)).toEqual([
          ['mcp__linear_prod__search_issues'],
          ['mcp__linear_prod__search_issues'],
        ]);
        expect(requests[0]?.toolDefinitions[0]?.description).toContain(
          'MCP tool "search.issues" from "linear.prod"',
        );
        expect(encode(requests[1]?.prompt)).toContain('VES-42');
      }),
  );

  it.effect('announces fresh snapshots and unavailable optional servers', () =>
    Effect.gen(function* () {
      let discovered: ReadonlyArray<McpTool> = [search];
      const offline = Mcp.fromClient({
        name: 'offline',
        optional: true,
        client: Effect.succeed({
          listTools: async () => {
            throw new Error('offline');
          },
          callTool: () => Promise.resolve(text('unused')),
        } satisfies ClientLike),
      });
      const agent = agentFor(
        'changing-mcp',
        Mcp.make({
          name: 'linear',
          transport: Mcp.transport(() => server({ tools: () => discovered })),
        }),
        offline,
      );
      const model = ScriptedModel.make([answer, answer]);

      yield* agent.run('First.').pipe(Effect.provide(model.layer));
      discovered = [];
      yield* agent.run('Second.').pipe(Effect.provide(model.layer));
      const requests = yield* model.requests;

      expect(requests.map((request) => request.tools)).toEqual([
        ['mcp__linear__search_issues'],
        [],
      ]);
      const first = systemPrompt(requests[0] ?? { prompt: { content: [] } });
      const second = systemPrompt(requests[1] ?? { prompt: { content: [] } });
      expect(first).toContain(
        'MCP server "linear": available; mcp__linear__search_issues.',
      );
      expect(second).toContain('MCP server "linear": available; no tools.');
      expect(second).toContain('MCP server "offline": unavailable; no tools.');
    }).pipe(Effect.scoped),
  );

  it.effect('reuses cached connections until the cache layer closes', () =>
    Effect.gen(function* () {
      const lifecycle = { opened: 0, closed: 0 };
      const agent = agentFor(
        'cached-mcp',
        Mcp.cached({
          name: 'linear',
          transport: serving([search], { lifecycle }),
        }),
      );
      const model = ScriptedModel.make([answer, answer]);

      yield* Effect.gen(function* () {
        yield* agent.run('First.');
        yield* agent.run('Second.');
        expect(lifecycle).toEqual({ opened: 1, closed: 0 });
      }).pipe(
        Effect.provide(
          Layer.merge(
            model.layer,
            Mcp.layerConnectionCache({ idleTimeToLive: '1 hour' }),
          ),
        ),
      );

      expect(lifecycle).toEqual({ opened: 1, closed: 1 });
    }),
  );

  it.effect('ignores a rejected close and returns typed limit failures', () =>
    Effect.gen(function* () {
      const lifecycle = { opened: 0, closed: 0 };
      const closing = yield* Mcp.make({
        name: 'linear',
        transport: serving([search], {
          lifecycle,
          closeFailure: new Error('close failed'),
        }),
      }).open.pipe(Effect.scoped, Effect.exit);
      expect(closing._tag).toBe('Success');
      expect(lifecycle).toEqual({ opened: 1, closed: 1 });

      const source = Mcp.make({
        name: 'linear',
        limits: { maxArgumentBytes: 32, maxResultBytes: 32 },
        transport: serving([search], { call: () => text('x'.repeat(100)) }),
      });
      const model = ScriptedModel.make([
        callTurn('mcp__linear__search_issues', { query: 'x'.repeat(100) }),
        callTurn('mcp__linear__search_issues', { query: 'ok' }),
        answer,
      ]);

      yield* agentFor('limited-mcp', source)
        .run('Search.')
        .pipe(Effect.provide(model.layer));
      const prompt = encode((yield* model.requests)[2]?.prompt);

      expect(prompt).toContain('"_tag":"LimitError"');
      expect(prompt).toContain('"resource":"tool-arguments"');
      expect(prompt).toContain('"resource":"tool-result"');
    }),
  );

  it.effect('resolves bearer auth for every remote request', () =>
    Effect.gen(function* () {
      const authorization: Array<string | null> = [];
      const customHeaders: Array<string | null> = [];
      let token = 0;
      const respond = (id: unknown, result: object): Response =>
        new Response(encode({ jsonrpc: '2.0', id, result }), {
          headers: { 'content-type': 'application/json' },
        });
      const fetch: FetchLike = async (_input, init) => {
        const headers = new Headers(init?.headers);
        authorization.push(headers.get('authorization'));
        customHeaders.push(headers.get('x-vesper-test'));
        const body =
          typeof init?.body === 'string' ? decodeRequest(init.body) : {};
        if (body.method === 'initialize') {
          return respond(body.id, {
            protocolVersion: LATEST_PROTOCOL_VERSION,
            capabilities: { tools: {} },
            serverInfo: { name: 'test', version: '1.0.0' },
          });
        }
        if (body.method === 'tools/list') {
          return respond(body.id, { tools: [search] });
        }
        return new Response(undefined, { status: 202 });
      };

      const ready = yield* Mcp.remote({
        name: 'linear',
        url: 'https://mcp.example.test',
        auth: () => Redacted.make(`token-${String((token += 1))}`),
        headers: { 'x-vesper-test': 'present' },
        fetch,
      }).open;

      expect(names(ready)).toEqual(['mcp__linear__search_issues']);
      expect(authorization.length).toBeGreaterThanOrEqual(3);
      expect(authorization).toEqual(
        authorization.map(
          (_header, index) => `Bearer token-${String(index + 1)}`,
        ),
      );
      expect(customHeaders).toEqual(authorization.map(() => 'present'));
    }).pipe(Effect.scoped),
  );
});

describe('Mcp discovery', () => {
  it.effect(
    'canonicalizes tool order and schemas, keeping allowlist order',
    () =>
      Effect.gen(function* () {
        const reorderedSearch: McpTool = {
          ...search,
          inputSchema: {
            required: ['query'],
            type: 'object',
            properties: { query: { type: 'string' } },
          },
        };
        const open = (tools: ReadonlyArray<McpTool>, allowlist?: string[]) =>
          Mcp.make({
            name: 'linear',
            transport: serving(tools),
            ...(allowlist === undefined ? {} : { tools: allowlist }),
          }).open;

        const first = yield* open([search, create]);
        const second = yield* open([create, reorderedSearch]);
        const punctuation = [
          { ...search, name: '!zeta' },
          { ...create, name: 'alpha' },
        ];
        const unfiltered = yield* open(punctuation);
        const allowlisted = yield* open(punctuation, ['!zeta', 'alpha']);

        expect(names(first)).toEqual([
          'mcp__linear__create_issue',
          'mcp__linear__search_issues',
        ]);
        expect(names(second)).toEqual(names(first));
        expect(encode(second.tools)).toBe(encode(first.tools));
        expect(DynamicToolkit.resourceContext(first)).toContain(
          'mcp__linear__create_issue, mcp__linear__search_issues',
        );
        expect(names(unfiltered)).toEqual([
          'mcp__linear__alpha',
          'mcp__linear__zeta',
        ]);
        expect(names(allowlisted)).toEqual([
          'mcp__linear__zeta',
          'mcp__linear__alpha',
        ]);
      }).pipe(Effect.scoped),
  );

  it.effect('fails closed on bad allowlists, limits and hostile metadata', () =>
    Effect.gen(function* () {
      const open = (
        tools: ReadonlyArray<McpTool>,
        selection: Partial<Mcp.Definition<'linear'>>,
      ) =>
        Mcp.make({
          name: 'linear',
          transport: serving(tools),
          ...selection,
        }).open.pipe(Effect.result);
      // The SDK type forbids this wire shape; a hostile server need not.
      const hostile: McpTool = {
        ...search,
        inputSchema: { ...search.inputSchema },
      };
      Reflect.set(hostile.inputSchema, 'type', 'string');

      expect((yield* open([search], { tools: ['missing'] }))._tag).toBe(
        'Failure',
      );
      expect(
        (yield* open([search], { tools: ['search.issues', 'search.issues'] }))
          ._tag,
      ).toBe('Failure');
      expect((yield* open([hostile], {}))._tag).toBe('Failure');
      const tooMany = yield* open([search, create], {
        limits: { maxTools: 1 },
      });
      if (tooMany._tag !== 'Failure') {
        throw new Error('expected a limit failure');
      }
      expect(tooMany.failure.message).toContain('LimitError');

      let connected = false;
      expect(() =>
        Mcp.fromClient({
          name: 'linear',
          limits: { maxTools: 0 },
          client: Effect.sync(() => {
            connected = true;
            return {
              listTools: () => Promise.resolve({ tools: [] }),
              callTool: () => Promise.resolve(text('unused')),
            } satisfies ClientLike;
          }),
        }),
      ).toThrow(/maxTools/);
      expect(connected).toBe(false);
    }).pipe(Effect.scoped),
  );
});

describe('Mcp request signals', () => {
  const slowClient = (
    onCall: (options: Parameters<ClientLike['callTool']>[1]) => void,
  ) =>
    ({
      listTools: () => Promise.resolve({ tools: [search] }),
      callTool: (_params, options) =>
        new Promise<CallToolResult>((_resolve, reject) => {
          onCall(options);
          options?.signal?.addEventListener(
            'abort',
            () => {
              reject(new Error('aborted'));
            },
            { once: true },
          );
        }),
    }) satisfies ClientLike;

  it.effect('passes configured timeouts and aborts slow calls', () =>
    Effect.gen(function* () {
      let timeout: number | undefined;
      const ready = yield* Mcp.fromClient({
        name: 'linear',
        timeout: 1,
        client: Effect.succeed(
          slowClient((options) => {
            timeout = options?.timeout;
          }),
        ),
      }).open;
      const fiber = yield* ready
        .handle('mcp__linear__search_issues', { query: 'vesper' })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.forkChild);

      yield* Effect.repeat(Effect.yieldNow, {
        until: () => timeout !== undefined,
      });
      yield* TestClock.adjust('1 millis');
      const events = yield* Fiber.join(fiber);

      expect(timeout).toBe(1);
      expect(Array.from(events).at(-1)).toMatchObject({ isFailure: true });
    }).pipe(Effect.scoped),
  );

  it.effect('propagates interruption to the request signal', () =>
    Effect.gen(function* () {
      let signal: AbortSignal | undefined;
      const ready = yield* Mcp.fromClient({
        name: 'linear',
        client: Effect.succeed(
          slowClient((options) => {
            signal = options?.signal;
          }),
        ),
      }).open;
      const fiber = yield* ready
        .handle('mcp__linear__search_issues', { query: 'vesper' })
        .pipe(Stream.unwrap, Stream.runDrain, Effect.forkChild);

      yield* Effect.repeat(Effect.yieldNow, {
        until: () => signal !== undefined,
      });
      yield* Fiber.interrupt(fiber);

      expect(signal?.aborted).toBe(true);
    }).pipe(Effect.scoped),
  );
});

describe('Mcp tool drift detection', () => {
  const driftedDescription: McpTool = {
    ...search,
    description: 'Completely different behavior now.',
  };
  const driftedSchema: McpTool = {
    ...search,
    inputSchema: {
      type: 'object',
      properties: { query: { type: 'string' }, limit: { type: 'number' } },
      required: ['query', 'limit'],
    },
  };
  const fingerprintsFor = (tool: McpTool) =>
    Mcp.fingerprints({ name: 'linear', transport: serving([tool]) }).pipe(
      Effect.scoped,
    );
  const openWith = (tool: McpTool, toolDrift?: Mcp.ToolDriftPolicy) =>
    Mcp.make({
      name: 'linear',
      transport: serving([tool]),
      ...(toolDrift === undefined ? {} : { toolDrift }),
    }).open.pipe(Effect.map(names));

  it.effect(
    'fingerprints are stable but change with description or schema',
    () =>
      Effect.gen(function* () {
        const original = yield* fingerprintsFor(search);
        const reordered = yield* fingerprintsFor({
          ...search,
          inputSchema: {
            required: ['query'],
            type: 'object',
            properties: { query: { type: 'string' } },
          },
        });

        expect(Object.keys(original)).toEqual(['search.issues']);
        expect(original['search.issues']).toMatch(/^[0-9a-f]{64}$/);
        expect(reordered).toEqual(original);
        expect(yield* fingerprintsFor(search)).toEqual(original);
        for (const drifted of [driftedDescription, driftedSchema]) {
          expect((yield* fingerprintsFor(drifted))['search.issues']).not.toBe(
            original['search.issues'],
          );
        }
      }),
  );

  it.effect('applies the drift policy to pinned fingerprints', () =>
    Effect.gen(function* () {
      const pins = yield* fingerprintsFor(search);
      const available = ['mcp__linear__search_issues'];

      // Reject is the default: either kind of drift excludes the tool.
      expect(
        yield* openWith(driftedDescription, { fingerprints: pins }),
      ).toEqual([]);
      expect(yield* openWith(driftedSchema, { fingerprints: pins })).toEqual(
        [],
      );
      expect(
        yield* openWith(driftedDescription, {
          fingerprints: pins,
          onDrift: 'warn',
        }),
      ).toEqual(available);
      expect(yield* openWith(search, { fingerprints: pins })).toEqual(
        available,
      );
      // Unpinned tools are trusted on first discovery; no policy changes nothing.
      expect(yield* openWith(driftedDescription, { fingerprints: {} })).toEqual(
        available,
      );
      expect(yield* openWith(driftedDescription)).toEqual(available);
    }).pipe(Effect.scoped),
  );
});
