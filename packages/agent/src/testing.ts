import { Effect, Layer, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Prompt,
  type Response,
} from 'effect/unstable/ai';

/** One normalized request observed at Effect's provider seam. */
export interface Request {
  readonly operation: 'generateText' | 'streamText';
  readonly index: number;
  readonly prompt: Prompt.Prompt;
  readonly tools: ReadonlyArray<string>;
  readonly toolChoice: LanguageModel.ProviderOptions['toolChoice'];
}

export interface Options {
  /** Responses for non-streaming calls such as compaction summaries. */
  readonly generate?: ReadonlyArray<GenerateStep> | undefined;
  /** Repeat the final entry after a script is exhausted. Defaults to false. */
  readonly repeatLast?: boolean | undefined;
}

/** Exact encoded output from one fake streaming provider call, or its failure. */
export type StreamStep =
  | ReadonlyArray<Response.StreamPartEncoded>
  | AiError.AiError;

/** Exact encoded output from one fake non-streaming provider call, or its failure. */
export type GenerateStep =
  | ReadonlyArray<Response.PartEncoded>
  | AiError.AiError;

/** A scripted model plus an inspectable record of every provider request. */
export interface Handle {
  readonly layer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly requests: Effect.Effect<ReadonlyArray<Request>>;
  readonly remaining: Effect.Effect<{
    readonly generate: number;
    readonly stream: number;
  }>;
}

const unexpected = (
  operation: Request['operation'],
  index: number,
): AiError.AiError =>
  new AiError.AiError({
    module: 'ScriptedModel',
    method: operation,
    reason: new AiError.InvalidRequestError({
      description: `Unexpected ${operation} call at index ${index}; the script is exhausted`,
    }),
  });

const requestOf = (
  operation: Request['operation'],
  index: number,
  options: LanguageModel.ProviderOptions,
): Request => ({
  operation,
  index,
  prompt: options.prompt,
  tools: options.tools.map((tool) => tool.name),
  toolChoice: options.toolChoice,
});

const at = <A>(
  steps: ReadonlyArray<A>,
  index: number,
  repeatLast: boolean,
): A | undefined =>
  steps[index] ??
  (repeatLast && steps.length > 0 ? steps[steps.length - 1] : undefined);

/**
 * Build a deterministic implementation of Effect's `LanguageModel` seam.
 *
 * Stream and generate scripts have independent cursors because production
 * uses `streamText` for turns and `generateText` for compaction. Scripts fail
 * when exhausted unless `repeatLast` is explicitly enabled.
 */
export const make = (
  turns: ReadonlyArray<StreamStep>,
  options: Options = {},
): Handle => {
  const requests: Request[] = [];
  let generateIndex = 0;
  let streamIndex = 0;
  const generated = options.generate ?? [];
  const repeatLast = options.repeatLast ?? false;

  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: (providerOptions) =>
        Effect.suspend(() => {
          const index = generateIndex++;
          requests.push(requestOf('generateText', index, providerOptions));
          const step = at(generated, index, repeatLast);
          if (step === undefined) {
            return Effect.fail(unexpected('generateText', index));
          }
          return step instanceof AiError.AiError
            ? Effect.fail(step)
            : Effect.succeed(Array.from(step));
        }),
      streamText: (providerOptions) =>
        Stream.suspend(() => {
          const index = streamIndex++;
          requests.push(requestOf('streamText', index, providerOptions));
          const step = at(turns, index, repeatLast);
          if (step === undefined) {
            return Stream.fail(unexpected('streamText', index));
          }
          return step instanceof AiError.AiError
            ? Stream.fail(step)
            : Stream.fromIterable(step);
        }),
    }),
  );

  return {
    layer,
    requests: Effect.sync(() => Array.from(requests)),
    remaining: Effect.sync(() => ({
      generate: Math.max(0, generated.length - generateIndex),
      stream: Math.max(0, turns.length - streamIndex),
    })),
  };
};

export * as ScriptedModel from './testing.js';
