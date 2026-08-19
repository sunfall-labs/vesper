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
  Cause,
  Context,
  Duration,
  Effect,
  Encoding,
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
  /**
   * Server tool names to expose, in the order advertised to the model.
   * Omit to expose the callable catalog in canonical name order.
   */
  readonly tools?: ReadonlyArray<string> | undefined;
  /** Discovery and call timeout in milliseconds. Defaults to 30 seconds. */
  readonly timeout?: number | undefined;
  readonly resetTimeoutOnProgress?: boolean | undefined;
  /** Hard bounds applied to untrusted discovery metadata and tool results. */
  readonly limits?: Limits | undefined;
  /**
   * Detect a remote tool's definition changing after it was pinned.
   *
   * A tool absent from `fingerprints` is trusted on first discovery, exactly
   * as it is today; only pinned tools are checked. Omitting `toolDrift`
   * entirely leaves discovery unchanged.
   */
  readonly toolDrift?: ToolDriftPolicy | undefined;
}

/**
 * Pinned tool fingerprints and what to do when a pin no longer matches.
 *
 * Keyed by the remote tool name the server advertises — the same identity
 * `tools` allowlists use — not the sanitized `mcp__<server>__<tool>` name
 * exposed to the model. Obtain values to pin with {@link fingerprints}.
 */
export interface ToolDriftPolicy {
  readonly fingerprints: Readonly<Record<string, string>>;
  /**
   * `'reject'` (the default) excludes the drifted tool from the toolkit and
   * logs a {@link ToolDriftError}. `'warn'` logs the same error but keeps
   * the tool available.
   */
  readonly onDrift?: 'reject' | 'warn' | undefined;
}

/** A pinned MCP tool fingerprint no longer matches what the server advertises. */
export class ToolDriftError extends Schema.TaggedError<ToolDriftError>(
  '@sunfall/vesper-mcp/ToolDriftError',
)('ToolDriftError', {
  server: Schema.String,
  tool: Schema.String,
  expected: Schema.String,
  actual: Schema.String,
}) {}

/** Bounds for one MCP source. Values are validated before a connection opens. */
export interface Limits {
  /** Maximum number of tools accepted from one discovery response. */
  readonly maxTools?: number | undefined;
  /** Maximum length of one remote tool name. */
  readonly maxToolNameLength?: number | undefined;
  /** Maximum UTF-8 bytes in a remote title or rendered description. */
  readonly maxDescriptionLength?: number | undefined;
  /** Maximum UTF-8 bytes in one canonicalized input schema. */
  readonly maxSchemaBytes?: number | undefined;
  /** Maximum UTF-8 bytes in encoded tool arguments. */
  readonly maxArgumentBytes?: number | undefined;
  /** Maximum UTF-8 bytes returned to the model by one tool call. */
  readonly maxResultBytes?: number | undefined;
}

/** A bounded resource supplied by an MCP server exceeded its configured limit. */
export class LimitError extends Schema.TaggedError<LimitError>(
  '@sunfall/vesper-mcp/LimitError',
)('LimitError', {
  resource: Schema.String,
  limit: Schema.Natural,
  actual: Schema.Natural,
}) {}

const ToolFailure = Schema.Union([Schema.String, LimitError]);

/** The defaults are deliberately conservative for model-facing data. */
export const defaultLimits = Object.freeze({
  maxTools: 128,
  maxToolNameLength: 256,
  maxDescriptionLength: 8_192,
  maxSchemaBytes: 64 * 1024,
  maxArgumentBytes: 64 * 1024,
  maxResultBytes: 1024 * 1024,
});

const maximumLimits = {
  maxTools: 1_024,
  maxToolNameLength: 1_024,
  maxDescriptionLength: 64 * 1024,
  maxSchemaBytes: 1024 * 1024,
  maxArgumentBytes: 1024 * 1024,
  maxResultBytes: 16 * 1024 * 1024,
};

interface NormalizedLimits {
  readonly maxTools: number;
  readonly maxToolNameLength: number;
  readonly maxDescriptionLength: number;
  readonly maxSchemaBytes: number;
  readonly maxArgumentBytes: number;
  readonly maxResultBytes: number;
}

const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_TIMEOUT_MS = 5 * 60_000;
const MAX_SERVER_NAME_LENGTH = 256;

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
  }).pipe(
    Effect.timeout(Duration.millis(timeoutFor(definition))),
    Effect.mapError((error) =>
      AiError.isAiError(error)
        ? error
        : failure('connect', definition.name, error),
    ),
  );
});

/** listTools, validate, and apply the allowlist — shared by discovery and {@link fingerprints}. */
const listAndSelect = Effect.fn('Mcp.listAndSelect')(function* <
  Name extends string,
>(
  client: ClientLike,
  definition: Pick<FromClient<Name, never>, keyof Selection | 'name'>,
): Effect.fn.Return<
  {
    readonly selected: ReadonlyArray<McpTool>;
    readonly limits: NormalizedLimits;
  },
  AiError.AiError
> {
  yield* Effect.annotateCurrentSpan('vesper.mcp.server', definition.name);
  const listed = yield* Effect.tryPromise({
    try: (signal) =>
      client.listTools(undefined, listOptions(signal, definition)),
    catch: (error) => failure('listTools', definition.name, error),
  }).pipe(
    Effect.timeout(Duration.millis(timeoutFor(definition))),
    Effect.mapError((error) =>
      AiError.isAiError(error)
        ? error
        : failure('listTools', definition.name, error),
    ),
  );
  const limits = normalizeLimits(definition.limits);
  if (!Array.isArray(listed.tools)) {
    return yield* Effect.fail(
      failure(
        'listTools',
        definition.name,
        new Error('MCP server returned a malformed tools list'),
      ),
    );
  }
  if (listed.tools.length > limits.maxTools) {
    return yield* Effect.fail(
      failure(
        'listTools',
        definition.name,
        new LimitError({
          resource: 'tool-count',
          limit: limits.maxTools,
          actual: listed.tools.length,
        }),
      ),
    );
  }
  for (const tool of listed.tools) {
    yield* Effect.try({
      try: () => validateRemoteTool(definition.name, tool, limits),
      catch: (error) => failure('validateTools', definition.name, error),
    });
  }
  const selected = yield* Effect.try({
    try: () => select(definition.name, listed.tools, definition.tools),
    catch: (error) => failure('selectTools', definition.name, error),
  });
  return { selected, limits };
});

const discover = Effect.fn('Mcp.discover')(function* <Name extends string>(
  client: ClientLike,
  definition: Pick<FromClient<Name, never>, keyof Selection | 'name'>,
): Effect.fn.Return<Toolkit.WithHandler<Tools>, AiError.AiError> {
  const { selected, limits } = yield* listAndSelect(client, definition);
  return yield* adapt(client, definition, selected, limits);
});

/**
 * Discover a server's current tool fingerprints without building a toolkit.
 *
 * Run this once, out of band — a setup script or an admin command — to
 * obtain the values to pin into `toolDrift.fingerprints`. Vesper does not
 * persist pins itself; storing and reloading them across runs is the
 * application's job (see the MCP docs for a storage example).
 */
export const fingerprints = <const Name extends string>(
  definition: Definition<Name> | RemoteDefinition<Name>,
): Effect.Effect<
  Readonly<Record<string, string>>,
  AiError.AiError,
  Scope.Scope
> =>
  Effect.gen(function* () {
    const resolved =
      'url' in definition
        ? resolveRemoteDefinition(definition)
        : validateDefinition(definition);
    const client = yield* connectedClient(resolved);
    const { selected, limits } = yield* listAndSelect(client, resolved);
    const entries: Array<readonly [string, string]> = [];
    for (const remote of selected) {
      const fingerprint = yield* toolFingerprint(resolved.name, remote, limits);
      entries.push([remote.name, fingerprint]);
    }
    return Object.fromEntries(entries);
  });

const adapt = <Name extends string>(
  client: ClientLike,
  definition: Pick<FromClient<Name, never>, keyof Selection | 'name'>,
  remoteTools: ReadonlyArray<McpTool>,
  limits: NormalizedLimits,
): Effect.Effect<Toolkit.WithHandler<Tools>, AiError.AiError> =>
  Effect.gen(function* () {
    const names = new Set<string>();
    const tools: Tool.AnyDynamic[] = [];
    const handlers: Record<
      string,
      (params: unknown) => Effect.Effect<string, string | LimitError>
    > = {};

    for (const remote of remoteTools) {
      validateRemoteTool(definition.name, remote, limits);
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

      const rendered = yield* Effect.try({
        try: () => ({
          description: description(definition.name, remote, limits),
          inputSchema: normalizeInputSchema(
            remote.inputSchema,
            limits,
            definition.name,
            remote.name,
          ),
        }),
        catch: (error) => failure('adaptTools', definition.name, error),
      });

      if (definition.toolDrift !== undefined) {
        const pinned = definition.toolDrift.fingerprints[remote.name];
        if (pinned !== undefined) {
          const current = yield* fingerprintOf({
            name: remote.name,
            description: rendered.description,
            inputSchema: rendered.inputSchema,
          });
          if (current !== pinned) {
            const onDrift = definition.toolDrift.onDrift ?? 'reject';
            yield* reportDrift(
              definition.name,
              remote.name,
              pinned,
              current,
              onDrift,
            );
            if (onDrift === 'reject') continue;
          }
        }
      }

      const tool = yield* Effect.try({
        try: () =>
          Tool.dynamic(name, {
            description: rendered.description,
            parameters: rendered.inputSchema,
            success: Schema.String,
            failure: ToolFailure,
            failureMode: 'return',
          }),
        catch: (error) => failure('adaptTools', definition.name, error),
      });
      tools.push(tool);
      handlers[name] = (params) =>
        call(client, definition, remote, params, limits);
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
  limits: NormalizedLimits,
): Effect.fn.Return<string, string | LimitError> {
  yield* Effect.annotateCurrentSpan({
    'vesper.mcp.server': definition.name,
    'vesper.mcp.tool': tool.name,
  });
  if (!isJsonObject(params)) {
    return yield* Effect.fail('MCP tool arguments must be a JSON object.');
  }
  const encodedParams = JSON.stringify(params);
  if (encodedParams === undefined) {
    return yield* Effect.fail('MCP tool arguments must be JSON-serializable.');
  }
  const argumentBytes = utf8Bytes(encodedParams);
  if (argumentBytes > limits.maxArgumentBytes) {
    return yield* Effect.fail(
      new LimitError({
        resource: 'tool-arguments',
        limit: limits.maxArgumentBytes,
        actual: argumentBytes,
      }),
    );
  }
  const result = yield* Effect.tryPromise({
    try: (signal) =>
      client.callTool(
        { name: tool.name, arguments: params },
        { ...callOptions(signal, definition), toolDefinition: tool },
      ),
    catch: (error) => message(error),
  }).pipe(
    Effect.timeout(Duration.millis(timeoutFor(definition))),
    Effect.mapError((error) =>
      Cause.isTimeoutError(error)
        ? `MCP tool timed out after ${timeoutFor(definition)}ms`
        : message(error),
    ),
  );
  const formatted = yield* formatResult(result, limits.maxResultBytes);
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
  if (allowlist === undefined) {
    return callable.sort((left, right) =>
      compareToolNames(server, left, right),
    );
  }

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

const validateRemoteTool = (
  server: string,
  tool: McpTool,
  limits: NormalizedLimits,
): void => {
  if (
    typeof tool !== 'object' ||
    tool === null ||
    typeof tool.name !== 'string' ||
    tool.name.length === 0 ||
    typeof tool.inputSchema !== 'object' ||
    tool.inputSchema === null ||
    Array.isArray(tool.inputSchema)
  ) {
    throw new Error(
      `MCP server ${JSON.stringify(server)} returned malformed tool metadata`,
    );
  }
  if (tool.name.length > limits.maxToolNameLength) {
    throw new LimitError({
      resource: 'tool-name',
      limit: limits.maxToolNameLength,
      actual: tool.name.length,
    });
  }
  if (
    (tool.description !== undefined && typeof tool.description !== 'string') ||
    (tool.title !== undefined && typeof tool.title !== 'string') ||
    (tool.annotations?.title !== undefined &&
      typeof tool.annotations.title !== 'string')
  ) {
    throw new Error(
      `MCP tool ${JSON.stringify(tool.name)} from ${JSON.stringify(server)} returned malformed text metadata`,
    );
  }
};

const compareToolNames = (
  server: string,
  left: McpTool,
  right: McpTool,
): number => {
  const leftName = toolName(server, left.name);
  const rightName = toolName(server, right.name);
  return leftName < rightName ? -1 : leftName > rightName ? 1 : 0;
};

const toolName = (server: string, tool: string): string =>
  `mcp__${sanitize(server)}__${sanitize(tool)}`;

const sanitize = (value: string): string => {
  const sanitized = value
    .replace(/[^A-Za-z0-9_-]/g, '_')
    .replace(/^_+|_+$/g, '');
  return sanitized || 'unnamed';
};

const description = (
  server: string,
  tool: McpTool,
  limits: NormalizedLimits,
): string => {
  const parts: string[] = [];
  if (sanitize(server) !== server || sanitize(tool.name) !== tool.name) {
    parts.push(
      `MCP tool ${JSON.stringify(tool.name)} from ${JSON.stringify(server)}.`,
    );
  }
  const title = tool.title ?? tool.annotations?.title;
  if (title !== undefined && title !== tool.name) {
    parts.push(`Title: ${metadataText(title, limits)}.`);
  }
  if (tool.description !== undefined) {
    parts.push(metadataText(tool.description, limits));
  }
  const rendered =
    parts.length === 0
      ? `MCP tool ${JSON.stringify(tool.name)} from ${JSON.stringify(server)}.`
      : parts.join(' ');
  const renderedBytes = utf8Bytes(rendered);
  if (renderedBytes > limits.maxDescriptionLength) {
    throw new LimitError({
      resource: 'tool-description',
      limit: limits.maxDescriptionLength,
      actual: renderedBytes,
    });
  }
  return rendered;
};

const normalizeInputSchema = (
  schema: McpTool['inputSchema'],
  limits: NormalizedLimits,
  server: string,
  tool: string,
): McpTool['inputSchema'] => {
  if (schema.type !== undefined && schema.type !== 'object') {
    throw new Error(
      `MCP tool ${JSON.stringify(tool)} from ${JSON.stringify(server)} must use an object input schema`,
    );
  }
  const canonical = canonicalJson({
    ...schema,
    type: 'object',
    properties: schema.properties ?? {},
  });
  const encoded = JSON.stringify(canonical);
  const schemaBytes = encoded === undefined ? 0 : utf8Bytes(encoded);
  if (schemaBytes > limits.maxSchemaBytes) {
    throw new LimitError({
      resource: 'tool-schema',
      limit: limits.maxSchemaBytes,
      actual: schemaBytes,
    });
  }
  return isJsonObject(canonical)
    ? {
        ...canonical,
        type: 'object',
        properties: isJsonObject(canonical.properties)
          ? canonical.properties
          : {},
      }
    : { type: 'object', properties: {} };
};

const canonicalJson = (
  value: unknown,
  key?: string,
): Schema.Json | undefined => {
  if (value === undefined) return undefined;
  if (Array.isArray(value)) {
    const items = value.flatMap((item) => {
      const canonical = canonicalJson(item);
      return canonical === undefined ? [] : [canonical];
    });
    return key === 'required' && items.every((item) => typeof item === 'string')
      ? items.sort()
      : items;
  }
  if (typeof value !== 'object' || value === null)
    return isJson(value) ? value : undefined;
  const entries = Object.entries(value)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .flatMap(([name, item]) => {
      const canonical = canonicalJson(item, name);
      if (canonical === undefined) return [];
      const entry: readonly [string, Schema.Json] = [name, canonical];
      return [entry];
    });
  return Object.fromEntries(entries);
};

/**
 * SHA-256 over the exact model-facing surface of one discovered tool: its
 * remote name, rendered description, and canonicalized input schema. Key
 * order never affects the result — the same canonicalization already used
 * to keep cached tool prompts byte-stable makes the fingerprint stable too.
 *
 * Deliberately not threaded through Effect's `Crypto` service the way
 * `@sunfall/vesper-log` and `@sunfall/vesper-attachments` hash: a tool
 * fingerprint is not a persisted content address anything durable depends
 * on, and requiring `Crypto.Crypto` here would force it onto every MCP
 * source's `Requires`, including the overwhelming majority that never
 * configure `toolDrift`. `crypto.subtle` is a standard platform primitive,
 * not a bundled or vendor-specific implementation.
 */
const fingerprintOf = (input: {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: unknown;
}): Effect.Effect<string> => {
  const canonical = canonicalJson({
    name: input.name,
    description: input.description,
    inputSchema: input.inputSchema,
  });
  // `input` is always a defined plain object of defined fields, so
  // `canonicalJson` always returns a defined value here.
  const material = JSON.stringify(canonical) ?? '{}';
  return Effect.promise(async () => {
    const digest = await globalThis.crypto.subtle.digest(
      'SHA-256',
      new TextEncoder().encode(material),
    );
    return Encoding.encodeHex(new Uint8Array(digest));
  });
};

/** The fingerprint of one remote tool as it will be adapted for the model. */
const toolFingerprint = (
  server: string,
  remote: McpTool,
  limits: NormalizedLimits,
): Effect.Effect<string, AiError.AiError> =>
  Effect.gen(function* () {
    const rendered = yield* Effect.try({
      try: () => ({
        description: description(server, remote, limits),
        inputSchema: normalizeInputSchema(
          remote.inputSchema,
          limits,
          server,
          remote.name,
        ),
      }),
      catch: (error) => failure('adaptTools', server, error),
    });
    return yield* fingerprintOf({ name: remote.name, ...rendered });
  });

/** Log a drifted tool's fingerprint mismatch; `'reject'` also excludes it. */
const reportDrift = (
  server: string,
  tool: string,
  expected: string,
  actual: string,
  onDrift: 'reject' | 'warn',
): Effect.Effect<void> =>
  Effect.logWarning(
    onDrift === 'reject'
      ? `MCP tool ${JSON.stringify(tool)} from ${JSON.stringify(server)} no longer matches its pinned fingerprint; excluding it from the toolkit.`
      : `MCP tool ${JSON.stringify(tool)} from ${JSON.stringify(server)} no longer matches its pinned fingerprint.`,
    new ToolDriftError({ server, tool, expected, actual }),
  ).pipe(
    Effect.annotateLogs({
      'vesper.component': 'mcp',
      'vesper.mcp.server': server,
      'vesper.mcp.tool': tool,
      'vesper.mcp.driftDecision': onDrift,
    }),
  );

const formatResult = (
  result: CallToolResult,
  maxResultBytes: number,
): Effect.Effect<string, LimitError> =>
  Effect.gen(function* () {
    const parts: string[] = [];
    let totalBytes = 0;
    const append = (part: string): Effect.Effect<void, LimitError> =>
      Effect.gen(function* () {
        if (part.length === 0) return;
        const separator = parts.length === 0 ? '' : '\n\n';
        const actual = totalBytes + utf8Bytes(separator) + utf8Bytes(part);
        if (actual > maxResultBytes) {
          return yield* Effect.fail(
            new LimitError({
              resource: 'tool-result',
              limit: maxResultBytes,
              actual,
            }),
          );
        }
        parts.push(part);
        totalBytes = actual;
      });

    if (result.structuredContent !== undefined) {
      yield* append(
        `Structured content:\n${JSON.stringify(result.structuredContent, undefined, 2)}`,
      );
    }
    for (const item of result.content ?? []) {
      if (item.type === 'text') {
        yield* append(item.text);
      } else if (item.type === 'image' || item.type === 'audio') {
        yield* append(
          `[${item.type === 'image' ? 'Image' : 'Audio'}: ${item.mimeType}, ${item.data.length} base64 chars]`,
        );
      } else if (item.type === 'resource') {
        yield* append(
          'text' in item.resource
            ? `[Resource: ${item.resource.uri}]\n${item.resource.text}`
            : `[Resource: ${item.resource.uri}, ${item.resource.blob.length} base64 chars]`,
        );
      } else if (item.type === 'resource_link') {
        const description =
          item.description === undefined ? '' : ` - ${item.description}`;
        yield* append(
          `[Resource link: ${item.name} (${item.uri})${description}]`,
        );
      } else {
        yield* append(JSON.stringify(item));
      }
    }
    return parts.join('\n\n') || '(MCP tool returned no content)';
  });

const isJson = Schema.is(Schema.Json);

const isJsonObject = (value: unknown): value is JSONObject =>
  typeof value === 'object' &&
  value !== null &&
  !Array.isArray(value) &&
  isJson(value);

const utf8Bytes = (value: string): number =>
  new TextEncoder().encode(value).byteLength;

const metadataText = (value: string, limits: NormalizedLimits): string => {
  const cleaned = Array.from(value, (character) => {
    const code = character.codePointAt(0);
    return code !== undefined &&
      (code <= 0x08 ||
        code === 0x0b ||
        code === 0x0c ||
        (code >= 0x0e && code <= 0x1f) ||
        code === 0x7f)
      ? '�'
      : character;
  }).join('');
  const cleanedBytes = utf8Bytes(cleaned);
  if (cleanedBytes > limits.maxDescriptionLength) {
    throw new LimitError({
      resource: 'tool-description',
      limit: limits.maxDescriptionLength,
      actual: cleanedBytes,
    });
  }
  return cleaned;
};

const normalizeLimits = (limits?: Limits): NormalizedLimits => {
  const result: NormalizedLimits = {
    maxTools: limits?.maxTools ?? defaultLimits.maxTools,
    maxToolNameLength:
      limits?.maxToolNameLength ?? defaultLimits.maxToolNameLength,
    maxDescriptionLength:
      limits?.maxDescriptionLength ?? defaultLimits.maxDescriptionLength,
    maxSchemaBytes: limits?.maxSchemaBytes ?? defaultLimits.maxSchemaBytes,
    maxArgumentBytes:
      limits?.maxArgumentBytes ?? defaultLimits.maxArgumentBytes,
    maxResultBytes: limits?.maxResultBytes ?? defaultLimits.maxResultBytes,
  };
  const entries: ReadonlyArray<readonly [keyof NormalizedLimits, number]> = [
    ['maxTools', result.maxTools],
    ['maxToolNameLength', result.maxToolNameLength],
    ['maxDescriptionLength', result.maxDescriptionLength],
    ['maxSchemaBytes', result.maxSchemaBytes],
    ['maxArgumentBytes', result.maxArgumentBytes],
    ['maxResultBytes', result.maxResultBytes],
  ];
  for (const [key, value] of entries) {
    const maximum = maximumLimits[key];
    if (!Number.isSafeInteger(value) || value <= 0 || value > maximum) {
      throw new RangeError(
        `MCP ${key} must be a positive integer no greater than ${maximum}`,
      );
    }
  }
  return result;
};

const timeoutFor = (selection: Selection): number => {
  // Public constructors call validateDefinition synchronously. Keep this
  // helper total for internally-created definitions and cached layers.
  return selection.timeout ?? DEFAULT_TIMEOUT_MS;
};

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
  if (definition.name.length > MAX_SERVER_NAME_LENGTH) {
    throw new RangeError(
      `MCP server name must be no longer than ${MAX_SERVER_NAME_LENGTH} characters.`,
    );
  }
  if (
    definition.timeout !== undefined &&
    (!Number.isFinite(definition.timeout) ||
      definition.timeout <= 0 ||
      definition.timeout > MAX_TIMEOUT_MS)
  ) {
    throw new Error(
      `MCP server ${JSON.stringify(definition.name)} timeout must be a positive number no greater than ${MAX_TIMEOUT_MS}ms.`,
    );
  }
  normalizeLimits(definition.limits);
  if (
    definition.tools !== undefined &&
    definition.tools.some((tool) => tool.length === 0)
  ) {
    throw new Error(
      `MCP server ${JSON.stringify(definition.name)} tools must be non-empty names.`,
    );
  }
  if (
    definition.toolDrift?.onDrift !== undefined &&
    definition.toolDrift.onDrift !== 'reject' &&
    definition.toolDrift.onDrift !== 'warn'
  ) {
    throw new Error(
      `MCP server ${JSON.stringify(definition.name)} toolDrift.onDrift must be "reject" or "warn".`,
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

const message = (error: unknown): string => {
  if (error instanceof LimitError) {
    return `${error._tag}: ${error.resource} exceeded ${error.limit} (actual ${error.actual})`;
  }
  if (
    error instanceof Error &&
    typeof error.message === 'string' &&
    error.message.length > 0
  ) {
    return error.message;
  }
  if (
    typeof error === 'object' &&
    error !== null &&
    '_tag' in error &&
    typeof error._tag === 'string'
  ) {
    return error._tag;
  }
  return String(error);
};

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
