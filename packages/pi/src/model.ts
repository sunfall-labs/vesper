import type {
  AssistantMessageEvent,
  StreamOptions as PiStreamOptions,
} from '@earendil-works/pi-ai';
import { Effect, Layer, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  Model,
  type Response,
} from 'effect/unstable/ai';

import { PiErrors } from './errors.js';
import { PiPrompt } from './prompt.js';
import { PiRegistry } from './registry.js';
import { PiStream } from './stream.js';
import { PiTools } from './tools.js';

// The provider seam. `LanguageModel.make` takes exactly two hooks, so this
// file is the whole implementation: translate the request in, translate the
// event stream out, convert Pi's in-band terminal error into a typed failure.
//
// Retrying is deliberately absent from here and lives in `./retry.ts`, which
// wraps these same two hooks. Same package, separate file: whether a call is
// retried is a policy a caller chooses, while translating a request is not.

const MODULE = 'PiModel';

export interface ModelOptions {
  /**
   * Provider-specific stream options passed through to Pi. `signal` is
   * managed here and is ignored if supplied.
   *
   * `maxRetries` defaults to 0 — see {@link DEFAULT_STREAM_OPTIONS}.
   */
  readonly streamOptions?: Omit<PiStreamOptions, 'signal'>;
}

/**
 * Retrying is owned by exactly one layer, and it is not this one.
 *
 * Pi's provider SDKs retry internally (the OpenAI and Anthropic clients
 * default to 2), which would silently multiply against any retry policy
 * above: three attempts here over two there is six real requests per model
 * call, with a backoff shape nobody chose. Worse, an SDK-internal retry is
 * invisible to the layer that decides whether retrying is safe at all — the
 * one that knows whether output has already streamed to a consumer.
 *
 * So this adapter turns them off and reports failures upward, where
 * `./retry.ts` retries with a policy that can see the whole picture. Callers
 * who genuinely want SDK-level retries can pass their own `maxRetries`.
 */
export const DEFAULT_STREAM_OPTIONS = { maxRetries: 0 } as const;

/**
 * The two provider hooks, before any middleware.
 *
 * Exposed separately from {@link model} so middleware can wrap the hooks
 * rather than the assembled service. Wrapping the service would put tool
 * resolution inside the retry, which is the wrong granularity — a retry must
 * cover exactly one provider call and nothing the loop did in reaction to it.
 *
 * The type is structural and built only from `effect` types, so anything
 * producing or consuming hooks needs no dependency on Pi's own vocabulary.
 */
export interface ProviderHooks {
  readonly generateText: (
    options: LanguageModel.ProviderOptions,
  ) => Effect.Effect<Array<Response.PartEncoded>, AiError.AiError>;
  readonly streamText: (
    options: LanguageModel.ProviderOptions,
  ) => Stream.Stream<Response.StreamPartEncoded, AiError.AiError>;
}

export const hooks = <const Provider extends string, const Id extends string>(
  provider: Provider,
  modelId: Id,
  options: ModelOptions = {},
): Effect.Effect<ProviderHooks, never, PiRegistry.Service> =>
  Effect.gen(function* () {
    const registry = yield* PiRegistry.Service;
    // A model missing from the registry is a wiring mistake, not a
    // recoverable request failure: fail during layer construction rather
    // than on every request at runtime.
    const piModel = yield* registry
      .resolve(provider, modelId)
      .pipe(Effect.orDie);

    const streamText = (
      providerOptions: LanguageModel.ProviderOptions,
    ): Stream.Stream<Response.StreamPartEncoded, AiError.AiError> =>
      Stream.unwrap(
        Effect.gen(function* () {
          // Fiber interruption must reach the provider. Acquiring the
          // controller in a scope means an interrupt, a timeout, and a race
          // all abort the underlying HTTP call through one path.
          const controller = yield* Effect.acquireRelease(
            Effect.sync(() => new AbortController()),
            (aborter) => Effect.sync(() => aborter.abort()),
          );

          const context = PiPrompt.toPiContext(
            providerOptions.prompt,
            PiTools.toPiTools(providerOptions.tools),
          );

          const events = registry.models.stream(piModel, context, {
            ...DEFAULT_STREAM_OPTIONS,
            ...options.streamOptions,
            signal: controller.signal,
          });

          return Stream.fromAsyncIterable(
            events as AsyncIterable<AssistantMessageEvent>,
            (cause) =>
              new AiError.AiError({
                module: MODULE,
                method: 'streamText',
                reason: new AiError.UnknownError({
                  description: String(cause),
                }),
              }),
          ).pipe(
            // Pi terminates in-band with an `error` event. Converting it to a
            // stream failure here is the whole reason this package exists:
            // retryability becomes a type, not a regex at every call site.
            Stream.flatMap((event) =>
              PiStream.isTerminalError(event)
                ? Stream.fail(
                    PiErrors.fromPiError(event.error, {
                      module: MODULE,
                      method: 'streamText',
                    }),
                  )
                : Stream.fromIterable(PiStream.toStreamParts(event)),
            ),
          );
        }),
      );

    const attributes = { 'ai.provider': provider, 'ai.model': modelId };

    return {
      streamText: (providerOptions) =>
        streamText(providerOptions).pipe(
          Stream.withSpan('PiModel.streamText', { attributes }),
        ),
      generateText: (providerOptions) =>
        streamText(providerOptions).pipe(
          Stream.runFold((): Array<Response.PartEncoded> => [], collectPart),
          Effect.withSpan('PiModel.generateText', { attributes }),
        ),
    };
  });

/**
 * A `LanguageModel` backed by one Pi model, with no durability.
 *
 * Returned as a `Model.Model`, so it is usable directly as a `Layer` and
 * carries provider and model name into context for telemetry.
 */
export const model = <const Provider extends string, const Id extends string>(
  provider: Provider,
  modelId: Id,
  options: ModelOptions = {},
): Model.Model<Provider, LanguageModel.LanguageModel, PiRegistry.Service> =>
  Model.make(
    provider,
    modelId,
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.flatMap(hooks(provider, modelId, options), LanguageModel.make),
    ),
  );

/**
 * Fold streaming parts into the non-streaming shape.
 *
 * Deltas accumulate into their owning block rather than appending, so
 * `generateText` returns whole content parts. Lifecycle markers carry no
 * information once the block is complete and are dropped.
 */
const collectPart = (
  parts: Array<Response.PartEncoded>,
  part: Response.StreamPartEncoded,
): Array<Response.PartEncoded> => {
  switch (part.type) {
    case 'text-start':
      return [...parts, { type: 'text', text: '' }];
    case 'text-delta':
      return appendText(parts, 'text', part.delta);
    case 'reasoning-start':
      return [...parts, { type: 'reasoning', text: '' }];
    case 'reasoning-delta':
      return appendText(parts, 'reasoning', part.delta);
    case 'tool-call':
    case 'tool-result':
    case 'file':
    case 'finish':
      return [...parts, part];
    default:
      return parts;
  }
};

const appendText = (
  parts: Array<Response.PartEncoded>,
  type: 'text' | 'reasoning',
  delta: string,
): Array<Response.PartEncoded> => {
  const last = parts[parts.length - 1];
  if (last === undefined || last.type !== type) {
    return [...parts, { type, text: delta } as Response.PartEncoded];
  }
  const text = `${(last as { readonly text: string }).text}${delta}`;
  return [...parts.slice(0, -1), { ...last, text } as Response.PartEncoded];
};

export * as PiModel from './model.js';
