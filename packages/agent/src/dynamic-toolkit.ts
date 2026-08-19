import { Effect, type Scope } from 'effect';
import { AiError, type Tool, type Toolkit } from 'effect/unstable/ai';

const ResourcesTypeId: unique symbol = Symbol.for(
  '@sunfall/vesper-agent/DynamicToolkit/Resources',
);

/** A model-visible resource snapshot resolved with its run-start tool catalog. */
export interface Resource {
  readonly id: string;
  readonly description: string;
  readonly status: 'available' | 'unavailable';
  readonly tools: ReadonlyArray<string>;
}

/** The stable identity and model-facing description of a dynamic capability. */
export interface ResourceDefinition {
  readonly id: string;
  readonly description: string;
}

/** Type-erased runtime toolkit source used only as a generic constraint. */
export interface Any {
  readonly open: Effect.Effect<unknown, AiError.AiError, unknown>;
}

/** A toolkit whose definitions and handlers are resolved when a run starts. */
export interface Source<
  ToolSet extends Record<string, Tool.Any>,
  out Requires = never,
> extends Any {
  readonly open: Effect.Effect<
    Toolkit.WithHandler<ToolSet>,
    AiError.AiError,
    Requires
  >;
}

/** A source whose acquisition is scoped but adds no public Scope requirement. */
export type ScopedSource<
  ToolSet extends Record<string, Tool.Any>,
  Requires = never,
> = Source<ToolSet, Requires | Scope.Scope>;

/** Create a scoped runtime toolkit source. */
export const make = <
  ToolSet extends Record<string, Tool.Any>,
  Requires = never,
>(
  open: Effect.Effect<Toolkit.WithHandler<ToolSet>, AiError.AiError, Requires>,
  options?: { readonly resource?: ResourceDefinition | undefined },
): Source<ToolSet, Requires> => {
  const resource = options?.resource;
  return {
    open:
      resource === undefined
        ? open
        : Effect.map(open, (toolkit) =>
            annotate(toolkit, [available(resource, toolkit)]),
          ),
  };
};

/**
 * Let a run continue without a failed dynamic capability.
 *
 * The failure remains observable in logs while the model receives an
 * authoritative unavailable resource snapshot and no tools from the source.
 */
export function optional<
  Tools extends Record<string, Tool.Any>,
  Requires = never,
>(
  source: Source<Tools, Requires>,
  resource: ResourceDefinition,
): Source<Tools, Requires>;
export function optional(source: Any, resource: ResourceDefinition): Any {
  return {
    open: Effect.flatMap(source.open, resolvedToolkit).pipe(
      Effect.map((toolkit) =>
        annotate(toolkit, [available(resource, toolkit)]),
      ),
      Effect.catch((error) =>
        Effect.logWarning(
          `Dynamic resource ${JSON.stringify(resource.id)} is unavailable`,
          error,
        ).pipe(
          Effect.annotateLogs({
            'vesper.component': 'dynamic-toolkit',
            'vesper.dynamic.resource': resource.id,
          }),
          Effect.as(
            annotate(empty, [
              {
                ...resource,
                status: 'unavailable',
                tools: [],
              },
            ]),
          ),
        ),
      ),
    ),
  };
}

/** The tools contributed by a tuple of sources. */
export type Tools<Sources extends ReadonlyArray<Any>> =
  Sources extends readonly []
    ? Record<never, never>
    : Toolkit.MergeRecords<SourceTools<Sources[number]>>;

type SourceTools<S> =
  S extends Source<infer SourceToolsRecord, infer _Requires>
    ? SourceToolsRecord
    : never;

/** Services required to open a tuple of sources. */
export type Services<Sources extends ReadonlyArray<Any>> =
  Sources extends readonly []
    ? never
    : Sources[number] extends Source<infer _Tools, infer Requires>
      ? Exclude<Requires, Scope.Scope>
      : never;

/** Resolve all sources once and combine their definitions and dispatch. */
export function open<const Sources extends ReadonlyArray<Any>>(
  sources: Sources,
): Effect.Effect<
  Toolkit.WithHandler<Tools<Sources>>,
  AiError.AiError,
  Services<Sources> | Scope.Scope
>;
export function open(
  sources: ReadonlyArray<Any>,
): Effect.Effect<
  Toolkit.WithHandler<Record<string, Tool.Any>>,
  AiError.AiError,
  unknown
> {
  return Effect.flatMap(
    Effect.forEach(
      sources,
      (source) => Effect.flatMap(source.open, resolvedToolkit),
      { concurrency: 'unbounded' },
    ),
    (resolved) =>
      Effect.try({
        try: () => mergeRuntime(...resolved),
        catch: mergeFailure,
      }),
  );
}

/** An already-resolved empty dynamic toolkit. */
export const empty: Toolkit.WithHandler<Record<never, never>> = {
  tools: {},
  handle: () =>
    Effect.fail(
      new AiError.AiError({
        module: 'DynamicToolkit',
        method: 'handle',
        reason: new AiError.ToolNotFoundError({
          toolName: '(empty)',
          availableTools: [],
        }),
      }),
    ),
};

/** Resource snapshots carried by a resolved dynamic toolkit. */
export const resources = (toolkit: unknown): ReadonlyArray<Resource> =>
  hasResources(toolkit) ? toolkit[ResourcesTypeId] : [];

/**
 * Render the run-start dynamic resource snapshot for the model.
 *
 * This belongs in system context rather than conversation history: an
 * unchanged snapshot remains cacheable, while a genuinely changed catalog
 * supersedes stale discovery state without accumulating messages. Live
 * operation-specific availability, authorization, and approval belong in
 * typed tool handlers instead. Toolkit-wide policy may use interception.
 */
export const resourceContext = (toolkit: unknown): string => {
  const current = resources(toolkit);
  if (current.length === 0) {
    return '';
  }
  const lines = current.map((resource) => {
    const tools =
      resource.tools.length === 0 ? 'no tools' : resource.tools.join(', ');
    return `- ${resource.description}: ${resource.status}; ${tools}.`;
  });
  return [
    '<dynamic_resources>',
    'Dynamic resources resolved for this run follow. This run-start snapshot supersedes dynamic-resource state in earlier messages.',
    ...lines,
    '</dynamic_resources>',
  ].join('\n');
};

/** Merge resolved toolkits while rejecting ambiguous dispatch names. */
export function merge<const Toolkits extends ReadonlyArray<unknown>>(
  ...toolkits: Toolkits & OnlyResolvedToolkits<Toolkits>
): Toolkit.WithHandler<MergedTools<Toolkits>>;
export function merge(
  ...toolkits: ReadonlyArray<unknown>
): Toolkit.WithHandler<Record<string, Tool.Any>> {
  const resolved = toolkits.map((toolkit) => {
    if (isResolvedToolkit(toolkit)) {
      return toolkit;
    }
    throw new Error('Expected a resolved dynamic toolkit.');
  });
  return mergeRuntime(...resolved);
}

const mergeRuntime = (
  ...toolkits: ReadonlyArray<Toolkit.WithHandler<Record<string, Tool.Any>>>
): Toolkit.WithHandler<Record<string, Tool.Any>> => {
  const tools: Record<string, Tool.Any> = {};
  const owners = new Map<
    string,
    Toolkit.WithHandler<Record<string, Tool.Any>>
  >();
  const mergedResources: Resource[] = [];
  const resourceIds = new Set<string>();

  for (const toolkit of toolkits) {
    for (const resource of resources(toolkit)) {
      if (resourceIds.has(resource.id)) {
        throw new Error(
          `Dynamic resource id collision: ${JSON.stringify(resource.id)}`,
        );
      }
      resourceIds.add(resource.id);
      mergedResources.push(resource);
    }
    for (const [name, tool] of Object.entries(toolkit.tools)) {
      if (owners.has(name)) {
        throw new Error(`Dynamic tool name collision: ${JSON.stringify(name)}`);
      }
      // Assignment treats `__proto__` specially on ordinary objects. Defining
      // an own data property keeps every valid tool name inert and enumerable.
      Object.defineProperty(tools, name, {
        configurable: true,
        enumerable: true,
        value: tool,
        writable: true,
      });
      owners.set(name, toolkit);
    }
  }

  return annotate(
    {
      tools,
      handle: (name, params, toolCallId) => {
        const owner = owners.get(name);
        return owner === undefined
          ? Effect.fail(
              new AiError.AiError({
                module: 'DynamicToolkit',
                method: 'handle',
                reason: new AiError.ToolNotFoundError({
                  toolName: name,
                  availableTools: Object.keys(tools),
                }),
              }),
            )
          : owner.handle(name, params, toolCallId);
      },
    },
    mergedResources,
  );
};

const available = <ToolSet extends Record<string, Tool.Any>>(
  resource: ResourceDefinition,
  toolkit: Toolkit.WithHandler<ToolSet>,
): Resource => ({
  ...resource,
  status: 'available',
  tools: Object.keys(toolkit.tools),
});

const annotate = <ToolSet extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<ToolSet>,
  snapshots: ReadonlyArray<Resource>,
): Toolkit.WithHandler<ToolSet> & {
  readonly [ResourcesTypeId]: ReadonlyArray<Resource>;
} => ({
  tools: toolkit.tools,
  handle: toolkit.handle,
  [ResourcesTypeId]: snapshots,
});

const hasResources = (
  value: unknown,
): value is { readonly [ResourcesTypeId]: ReadonlyArray<Resource> } =>
  typeof value === 'object' &&
  value !== null &&
  ResourcesTypeId in value &&
  Array.isArray(value[ResourcesTypeId]);

const mergeFailure = (error: unknown): AiError.AiError =>
  new AiError.AiError({
    module: 'DynamicToolkit',
    method: 'open',
    reason: new AiError.InvalidRequestError({
      description:
        error instanceof Error
          ? error.message
          : 'Dynamic toolkits could not be combined.',
    }),
  });

const resolvedToolkit = (
  value: unknown,
): Effect.Effect<
  Toolkit.WithHandler<Record<string, Tool.Any>>,
  AiError.AiError
> =>
  isResolvedToolkit(value)
    ? Effect.succeed(value)
    : Effect.fail(
        mergeFailure(
          new Error('A dynamic source returned an invalid toolkit.'),
        ),
      );

const isResolvedToolkit = (
  value: unknown,
): value is Toolkit.WithHandler<Record<string, Tool.Any>> =>
  typeof value === 'object' &&
  value !== null &&
  'tools' in value &&
  typeof value.tools === 'object' &&
  value.tools !== null &&
  'handle' in value &&
  typeof value.handle === 'function';

type MergedTools<Toolkits extends ReadonlyArray<unknown>> =
  Toolkit.SimplifyRecord<
    Toolkit.MergeRecords<
      Toolkits[number] extends Toolkit.WithHandler<infer ToolSet>
        ? ToolSet
        : never
    >
  >;

type OnlyResolvedToolkits<Toolkits extends ReadonlyArray<unknown>> = {
  readonly [Index in keyof Toolkits]: Toolkits[Index] extends Toolkit.WithHandler<
    infer _Tools
  >
    ? Toolkits[Index]
    : never;
};

export * as DynamicToolkit from './dynamic-toolkit.js';
