import {
  Client,
  SSEClientTransport,
  StreamableHTTPClientTransport,
  type AuthProvider,
  type CacheableRequestOptions,
  type CallToolResult,
  type ClientOptions,
  type FetchLike,
  type JSONObject,
  type StreamableHTTPClientTransportOptions,
  type SSEClientTransportOptions,
  type Tool as McpTool,
  type Transport as McpTransport,
} from '@modelcontextprotocol/client';
import {
  StdioClientTransport,
  type StdioServerParameters,
} from '@modelcontextprotocol/client/stdio';
import {
  Context,
  Duration,
  Effect,
  Layer,
  RcMap,
  Redacted,
  Schema,
  Scope,
} from 'effect';
import { AiError, Tool, Toolkit } from 'effect/unstable/ai';

import { DynamicToolkit } from '@sunfall/vesper-agent/dynamic-toolkit';

/** The model-facing names contributed by an MCP server. */
export type Tools = Record<`mcp__${string}__${string}`, Tool.AnyDynamic>;

/** The small MCP client surface needed by the adapter. */
export type ClientLike = Pick<Client, 'listTools' | 'callTool'>;

export interface CallOptions {
  readonly timeout?: number;
  readonly resetTimeoutOnProgress?: boolean;
  readonly signal?: AbortSignal;
}

/** A fresh transport factory; one transport is acquired for each agent run. */
export interface Transport {
  readonly make: () => McpTransport;
}

/** Wrap an application-defined MCP transport factory. */
export const transport = (make: () => McpTransport): Transport => ({ make });

/** Modern remote MCP transport. */
export const streamableHttp = (
  url: URL | string,
  options?: StreamableHTTPClientTransportOptions,
): Transport =>
  transport(
    () =>
      new StreamableHTTPClientTransport(
        typeof url === 'string' ? new URL(url) : url,
        options,
      ),
  );

/** Legacy remote SSE transport. */
export const sse = (
  url: URL | string,
  options?: SSEClientTransportOptions,
): Transport =>
  transport(
    () =>
      new SSEClientTransport(
        typeof url === 'string' ? new URL(url) : url,
        options,
      ),
  );

/** Local stdio MCP transport. */
export const stdio = (options: StdioServerParameters): Transport =>
  transport(() => new StdioClientTransport(options));

export interface Selection {
  /** Server tool names to expose, in the order advertised to the model. */
  readonly tools?: ReadonlyArray<string> | undefined;
  /** Require Vesper's ordinary approval flow for selected remote tools. */
  readonly needsApproval?: boolean | ((tool: McpTool) => boolean) | undefined;
  readonly timeout?: number | undefined;
  readonly resetTimeoutOnProgress?: boolean | undefined;
}

export interface Definition<Name extends string> extends Selection {
  readonly name: Name;
  readonly transport: Transport;
  readonly clientOptions?: ClientOptions | undefined;
  /** Continue with no tools and tell the model when this server is unavailable. */
  readonly optional?: boolean | undefined;
}

export interface FromClient<Name extends string, Requires> extends Selection {
  readonly name: Name;
  /** A scoped, connected client. Its finalizer lives for the whole run. */
  readonly client: Effect.Effect<
    ClientLike,
    AiError.AiError,
    Requires | Scope.Scope
  >;
  readonly optional?: boolean | undefined;
}

/** A redacted bearer token or a resolver invoked before every HTTP request. */
export type Auth =
  | Redacted.Redacted<string>
  | (() => Redacted.Redacted<string> | Promise<Redacted.Redacted<string>>);

/** Convenient remote-server definition; advanced transports still use `make`. */
export interface RemoteDefinition<Name extends string> extends Selection {
  readonly name: Name;
  readonly url: URL | string;
  readonly transport?: 'streamable-http' | 'sse' | undefined;
  readonly auth?: Auth | undefined;
  readonly headers?: HeadersInit | undefined;
  readonly requestInit?: RequestInit | undefined;
  readonly fetch?: FetchLike | undefined;
  readonly clientOptions?: ClientOptions | undefined;
  readonly optional?: boolean | undefined;
}

export interface ConnectionCacheService {
  readonly resolve: (
    definition: Definition<string>,
  ) => Effect.Effect<ClientLike, AiError.AiError, Scope.Scope>;
}

/** Shared MCP connections supplied explicitly through an Effect Layer. */
export class ConnectionCache extends Context.Service<
  ConnectionCache,
  ConnectionCacheService
>()('@sunfall/vesper-mcp/ConnectionCache') {}

export interface ConnectionCacheOptions {
  /** How long an unused connection remains warm. Defaults to five minutes. */
  readonly idleTimeToLive?: Duration.Input | undefined;
}

/**
 * A reference-counted, scoped connection cache.
 *
 * Concurrent and sequential runs of the same cached source share its client;
 * the client closes after its idle TTL or when the layer scope closes.
 */
export const layerConnectionCache = (
  options?: ConnectionCacheOptions,
): Layer.Layer<ConnectionCache> =>
  Layer.effect(
    ConnectionCache,
    Effect.map(
      RcMap.make<Definition<string>, ClientLike, AiError.AiError, Scope.Scope>({
        lookup: connectedClient,
        idleTimeToLive: options?.idleTimeToLive ?? '5 minutes',
      }),
      (connections) => ({
        resolve: (definition) => RcMap.get(connections, definition),
      }),
    ),
  );

/**
 * Consume one MCP server as a scoped dynamic toolkit.
 *
 * Discovery happens once per agent run. The resulting names, schemas, and
 * handlers remain fixed across every turn in that run.
 */
export const make = <const Name extends string>(
  definition: Definition<Name>,
): DynamicToolkit.ScopedSource<Tools> => {
  const validated = validateDefinition(definition);
  return fromClientUnchecked<Name, never>({
    ...validated,
    client: connectedClient(validated),
  });
};

/** Remote HTTP/SSE shorthand with a per-request bearer-token resolver. */
export const remote = <const Name extends string>(
  definition: RemoteDefinition<Name>,
): DynamicToolkit.ScopedSource<Tools> => {
  const resolved = resolveRemoteDefinition(definition);
  return fromClientUnchecked<Name, never>({
    ...resolved,
    client: connectedClient(resolved),
  });
};

const resolveRemoteDefinition = <const Name extends string>(
  definition: RemoteDefinition<Name>,
): Definition<Name> => {
  validateRemoteDefinition(definition);
  const url =
    typeof definition.url === 'string'
      ? new URL(definition.url)
      : definition.url;
  const requestInit = mergeRequestInit(
    definition.requestInit,
    definition.headers,
  );
  const transportOptions = {
    ...(definition.auth === undefined
      ? {}
      : { authProvider: bearerAuth(definition.auth) }),
    ...(requestInit === undefined ? {} : { requestInit }),
    ...(definition.fetch === undefined ? {} : { fetch: definition.fetch }),
  };
  const selectedTransport =
    definition.transport === 'sse'
      ? sse(url, transportOptions)
      : streamableHttp(url, transportOptions);
  return { ...definition, transport: selectedTransport };
};

/**
 * Consume one MCP server through `layerConnectionCache`.
 *
 * Tool discovery still refreshes once per run; only the initialized transport
 * is reused. The layer remains an explicit requirement in the Effect channel.
 */
export const cached = <const Name extends string>(
  definition: Definition<Name> | RemoteDefinition<Name>,
): DynamicToolkit.ScopedSource<Tools, ConnectionCache> => {
  const resolved =
    'url' in definition
      ? resolveRemoteDefinition(definition)
      : validateDefinition(definition);
  return fromClientUnchecked<Name, ConnectionCache>({
    ...resolved,
    client: ConnectionCache.use((cache) => cache.resolve(resolved)),
  });
};

/** Adapt an application-owned, connected MCP client. */
export const fromClient = <const Name extends string, Requires = never>(
  definition: FromClient<Name, Requires>,
): DynamicToolkit.ScopedSource<Tools, Requires> =>
  fromClientUnchecked(validateDefinition(definition));

const fromClientUnchecked = <const Name extends string, Requires>(
  definition: FromClient<Name, Requires>,
): DynamicToolkit.ScopedSource<Tools, Requires> => {
  const open = Effect.flatMap(definition.client, (client) =>
    discover(client, definition),
  );
  const resource = {
    id: `mcp:${definition.name}`,
    description: `MCP server ${JSON.stringify(definition.name)}`,
  };
  return definition.optional === true
    ? DynamicToolkit.optional(DynamicToolkit.make(open), resource)
    : DynamicToolkit.make(open, { resource });
};

const connectedClient = Effect.fn('Mcp.connect')(function* <
  Name extends string,
>(
  definition: Definition<Name>,
): Effect.fn.Return<ClientLike, AiError.AiError, Scope.Scope> {
  yield* Effect.annotateCurrentSpan('vesper.mcp.server', definition.name);
  const client = yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new Client(
          { name: 'vesper', version: '0.1.0' },
          definition.clientOptions,
        ),
    ),
    (client) =>
      Effect.tryPromise(() => client.close()).pipe(
        Effect.ignore({
          log: 'Warn',
          message: 'Failed to close MCP client',
        }),
        Effect.annotateLogs({
          'vesper.component': 'mcp',
          'vesper.mcp.server': definition.name,
        }),
      ),
  );
  return yield* Effect.tryPromise({
    try: (signal) =>
      client
        .connect(definition.transport.make(), { signal })
        .then(() => client),
    catch: (error) => failure('connect', definition.name, error),
  });
});

const discover = Effect.fn('Mcp.discover')(function* <Name extends string>(
  client: ClientLike,
  definition: Pick<FromClient<Name, never>, keyof Selection | 'name'>,
): Effect.fn.Return<Toolkit.WithHandler<Tools>, AiError.AiError> {
  yield* Effect.annotateCurrentSpan('vesper.mcp.server', definition.name);
  const listed = yield* Effect.tryPromise({
    try: (signal) =>
      client.listTools(undefined, listOptions(signal, definition)),
    catch: (error) => failure('listTools', definition.name, error),
  });
  const selected = yield* Effect.try({
    try: () => select(definition.name, listed.tools, definition.tools),
    catch: (error) => failure('selectTools', definition.name, error),
  });

  return yield* adapt(client, definition, selected);
});

const adapt = <Name extends string>(
  client: ClientLike,
  definition: Pick<FromClient<Name, never>, keyof Selection | 'name'>,
  remoteTools: ReadonlyArray<McpTool>,
): Effect.Effect<Toolkit.WithHandler<Tools>, AiError.AiError> =>
  Effect.gen(function* () {
    const names = new Set<string>();
    const tools: Tool.AnyDynamic[] = [];
    const handlers: Record<
      string,
      (params: unknown) => Effect.Effect<string, string>
    > = {};

    for (const remote of remoteTools) {
      const name = toolName(definition.name, remote.name);
      if (names.has(name)) {
        return yield* Effect.fail(
          failure(
            'adaptTools',
            definition.name,
            new Error(`multiple tools normalize to ${JSON.stringify(name)}`),
          ),
        );
      }
      names.add(name);

      const needsApproval =
        typeof definition.needsApproval === 'function'
          ? definition.needsApproval(remote)
          : definition.needsApproval;
      const tool = Tool.dynamic(name, {
        description: description(definition.name, remote),
        parameters: normalizeInputSchema(remote.inputSchema),
        success: Schema.String,
        failure: Schema.String,
        failureMode: 'return',
        needsApproval,
      });
      tools.push(tool);
      handlers[name] = (params) => call(client, definition, remote, params);
    }

    const toolkit = Toolkit.make(...tools);
    const resolved = yield* toolkit.pipe(
      Effect.provide(toolkit.toLayer(handlers)),
    );
    return resolved;
  });

const call = Effect.fn('Mcp.call')(function* <Name extends string>(
  client: ClientLike,
  definition: Pick<FromClient<Name, never>, keyof Selection | 'name'>,
  tool: McpTool,
  params: unknown,
): Effect.fn.Return<string, string> {
  yield* Effect.annotateCurrentSpan({
    'vesper.mcp.server': definition.name,
    'vesper.mcp.tool': tool.name,
  });
  if (!isJsonObject(params)) {
    return yield* Effect.fail('MCP tool arguments must be a JSON object.');
  }
  const result = yield* Effect.tryPromise({
    try: (signal) =>
      client.callTool(
        { name: tool.name, arguments: params },
        { ...callOptions(signal, definition), toolDefinition: tool },
      ),
    catch: (error) => message(error),
  });
  const formatted = formatResult(result);
  return result.isError
    ? yield* Effect.fail(formatted || 'MCP tool reported an error.')
    : formatted;
});

const select = (
  server: string,
  discovered: ReadonlyArray<McpTool>,
  allowlist: ReadonlyArray<string> | undefined,
): ReadonlyArray<McpTool> => {
  const callable = discovered.filter(
    (tool) => tool.execution?.taskSupport !== 'required',
  );
  if (allowlist === undefined) return callable;

  const duplicates = allowlist.filter(
    (name, index) => allowlist.indexOf(name) !== index,
  );
  if (duplicates.length > 0) {
    throw new Error(`tools allowlist repeats ${formatNames(duplicates)}`);
  }
  const byName = new Map(discovered.map((tool) => [tool.name, tool]));
  const unknown = allowlist.filter((name) => !byName.has(name));
  if (unknown.length > 0) {
    throw new Error(
      `server ${JSON.stringify(server)} does not expose ${formatNames(unknown)}`,
    );
  }

  return allowlist.map((name) => {
    const tool = byName.get(name);
    if (tool === undefined) {
      throw new Error(
        `server ${JSON.stringify(server)} does not expose ${name}`,
      );
    }
    if (tool.execution?.taskSupport === 'required') {
      throw new Error(
        `tool ${JSON.stringify(name)} requires MCP task execution`,
      );
    }
    return tool;
  });
};

const toolName = (server: string, tool: string): string =>
  `mcp__${sanitize(server)}__${sanitize(tool)}`;

const sanitize = (value: string): string => {
  const sanitized = value
    .replace(/[^A-Za-z0-9_-]/g, '_')
    .replace(/^_+|_+$/g, '');
  return sanitized || 'unnamed';
};

const description = (server: string, tool: McpTool): string => {
  const parts: string[] = [];
  if (sanitize(server) !== server || sanitize(tool.name) !== tool.name) {
    parts.push(
      `MCP tool ${JSON.stringify(tool.name)} from ${JSON.stringify(server)}.`,
    );
  }
  const title = tool.title ?? tool.annotations?.title;
  if (title !== undefined && title !== tool.name)
    parts.push(`Title: ${title}.`);
  if (tool.description !== undefined) parts.push(tool.description);
  return parts.length === 0
    ? `MCP tool ${JSON.stringify(tool.name)} from ${JSON.stringify(server)}.`
    : parts.join(' ');
};

const normalizeInputSchema = (
  schema: McpTool['inputSchema'],
): McpTool['inputSchema'] => ({
  ...schema,
  type: schema.type ?? 'object',
  properties: schema.properties ?? {},
});

const formatResult = (result: CallToolResult): string => {
  const parts: string[] = [];
  if (result.structuredContent !== undefined) {
    parts.push(
      `Structured content:\n${JSON.stringify(result.structuredContent, undefined, 2)}`,
    );
  }
  for (const item of result.content ?? []) {
    if (item.type === 'text') {
      parts.push(item.text);
    } else if (item.type === 'image' || item.type === 'audio') {
      parts.push(
        `[${item.type === 'image' ? 'Image' : 'Audio'}: ${item.mimeType}, ${item.data.length} base64 chars]`,
      );
    } else if (item.type === 'resource') {
      parts.push(
        'text' in item.resource
          ? `[Resource: ${item.resource.uri}]\n${item.resource.text}`
          : `[Resource: ${item.resource.uri}, ${item.resource.blob.length} base64 chars]`,
      );
    } else if (item.type === 'resource_link') {
      const description =
        item.description === undefined ? '' : ` - ${item.description}`;
      parts.push(`[Resource link: ${item.name} (${item.uri})${description}]`);
    } else {
      parts.push(JSON.stringify(item));
    }
  }
  return parts.filter(Boolean).join('\n\n') || '(MCP tool returned no content)';
};

const isJson = Schema.is(Schema.Json);

const isJsonObject = (value: unknown): value is JSONObject =>
  typeof value === 'object' &&
  value !== null &&
  !Array.isArray(value) &&
  isJson(value);

const formatNames = (names: ReadonlyArray<string>): string =>
  [...new Set(names)].map((name) => JSON.stringify(name)).join(', ');

const bearerAuth = (auth: Auth): AuthProvider => {
  const resolve = typeof auth === 'function' ? auth : () => auth;
  return {
    token: async () => {
      const token = Redacted.value(await resolve());
      if (token.length === 0) {
        throw new Error('MCP bearer token must be non-empty.');
      }
      return token;
    },
    onUnauthorized: async () => {},
  };
};

const mergeRequestInit = (
  requestInit: RequestInit | undefined,
  headers: HeadersInit | undefined,
): RequestInit | undefined => {
  if (headers === undefined) return requestInit;
  const mergedHeaders = new Headers(requestInit?.headers);
  for (const [name, value] of new Headers(headers)) {
    mergedHeaders.set(name, value);
  }
  return { ...requestInit, headers: mergedHeaders };
};

const validateDefinition = <
  Definition extends Selection & {
    readonly name: string;
    readonly optional?: boolean | undefined;
  },
>(
  definition: Definition,
): Definition => {
  if (definition.name.trim() === '') {
    throw new Error('MCP server name must be non-empty.');
  }
  if (
    definition.timeout !== undefined &&
    (!Number.isFinite(definition.timeout) || definition.timeout <= 0)
  ) {
    throw new Error(
      `MCP server ${JSON.stringify(definition.name)} timeout must be a positive number.`,
    );
  }
  if (
    definition.tools !== undefined &&
    definition.tools.some((tool) => tool.length === 0)
  ) {
    throw new Error(
      `MCP server ${JSON.stringify(definition.name)} tools must be non-empty names.`,
    );
  }
  return definition;
};

const validateRemoteDefinition = (
  definition: RemoteDefinition<string>,
): void => {
  validateDefinition(definition);
  if (typeof definition.url === 'string' && !URL.canParse(definition.url)) {
    throw new Error(
      `MCP server ${JSON.stringify(definition.name)} requires a valid absolute URL.`,
    );
  }
};

const message = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const callOptions = (
  signal: AbortSignal,
  selection: Selection,
): CallOptions => ({
  signal,
  ...(selection.timeout === undefined ? {} : { timeout: selection.timeout }),
  ...(selection.resetTimeoutOnProgress === undefined
    ? {}
    : { resetTimeoutOnProgress: selection.resetTimeoutOnProgress }),
});

const listOptions = (
  signal: AbortSignal,
  selection: Selection,
): CacheableRequestOptions => ({
  ...callOptions(signal, selection),
  cacheMode: 'refresh',
});

const failure = (
  method: string,
  server: string,
  error: unknown,
): AiError.AiError =>
  new AiError.AiError({
    module: 'Mcp',
    method,
    reason: new AiError.UnknownError({
      description: `MCP server ${JSON.stringify(server)}: ${message(error)}`,
      metadata: { server },
    }),
  });

export * as Mcp from './mcp.js';
