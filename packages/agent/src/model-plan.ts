import { Effect, ExecutionPlan, Layer, Schema, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  type Tool,
} from 'effect/unstable/ai';

/**
 * An Effect execution plan whose steps provide a language model.
 *
 * Additional accepted input accommodates native plans whose predicates accept
 * a safe supertype of AiError. It never widens a model operation's error type.
 */
export type Plan<
  Provides = LanguageModel.LanguageModel,
  AdditionalAcceptedInput = never,
  Requirements = never,
> = LanguageModel.LanguageModel extends Provides
  ? ExecutionPlan.ExecutionPlan<{
      provides: Provides;
      input: AiError.AiError | AdditionalAcceptedInput;
      error: never;
      requirements: Requirements;
    }>
  : never;

type AttemptResult<A, E> =
  | { readonly _tag: 'Value'; readonly value: A }
  | { readonly _tag: 'Bypass'; readonly error: E };

type GenerateTextWithoutToolkit = Omit<
  LanguageModel.GenerateTextOptions<{}>,
  'toolkit'
> & {
  readonly toolkit?: undefined;
};

type GenerateTextWithToolkit = LanguageModel.GenerateTextOptions<
  Record<string, Tool.Any>
> & {
  readonly toolkit: LanguageModel.ToolkitInput<Record<string, Tool.Any>>;
};

/** Contextually type a native ExecutionPlan `while` predicate as an AiError. */
export function when<R>(
  predicate: (error: AiError.AiError) => Effect.Effect<boolean, never, R>,
): (error: AiError.AiError) => Effect.Effect<boolean, never, R>;
export function when(
  predicate: (error: AiError.AiError) => boolean,
): (error: AiError.AiError) => boolean;
export function when<R>(
  predicate: (
    error: AiError.AiError,
  ) => boolean | Effect.Effect<boolean, never, R>,
): (error: AiError.AiError) => boolean | Effect.Effect<boolean, never, R> {
  return predicate;
}

/**
 * Turn an Effect execution plan into the ordinary LanguageModel layer Vesper
 * already consumes.
 *
 * Streaming fallback stops after the first emitted part. Once output is
 * visible, retrying another model would splice two responses together and
 * could repeat tool work.
 */
export const layer = <Provides, AdditionalAcceptedInput, PlanR>(
  plan: Plan<Provides, AdditionalAcceptedInput, PlanR>,
): Layer.Layer<LanguageModel.LanguageModel, never, PlanR> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const capturedPlan = yield* plan.captureRequirements;

      const execute = <A, E, R>(
        operation: Effect.Effect<A, E, R>,
        fallbackOnAiError: boolean,
      ) =>
        operation.pipe(
          Effect.map(
            (value): AttemptResult<A, E> => ({ _tag: 'Value', value }),
          ),
          Effect.catch((error) =>
            fallbackOnAiError && AiError.isAiError(error)
              ? Effect.fail(error)
              : Effect.succeed<AttemptResult<A, E>>({
                  _tag: 'Bypass',
                  error,
                }),
          ),
          Effect.withExecutionPlan(capturedPlan),
          Effect.flatMap((result) =>
            result._tag === 'Value'
              ? Effect.succeed(result.value)
              : Effect.fail(result.error),
          ),
        );
      const executeStream = <A, E, R>(operation: Stream.Stream<A, E, R>) =>
        operation.pipe(
          Stream.map(
            (value): AttemptResult<A, E> => ({ _tag: 'Value', value }),
          ),
          Stream.catch((error) =>
            AiError.isAiError(error)
              ? Stream.fail(error)
              : Stream.succeed<AttemptResult<A, E>>({
                  _tag: 'Bypass',
                  error,
                }),
          ),
          Stream.withExecutionPlan(capturedPlan, {
            preventFallbackOnPartialStream: true,
          }),
          Stream.mapEffect((result) =>
            result._tag === 'Value'
              ? Effect.succeed(result.value)
              : Effect.fail(result.error),
          ),
        );

      function generateText<
        Request extends LanguageModel.GenerateTextOptions<{}> & {
          readonly toolkit?: undefined;
        },
      >(
        request: Request,
      ): Effect.Effect<
        LanguageModel.GenerateTextResponse<{}>,
        LanguageModel.ExtractError<Request>,
        LanguageModel.ExtractServices<Request>
      >;
      function generateText<
        Tools extends Record<string, Tool.Any>,
        Request extends LanguageModel.GenerateTextOptions<Tools> & {
          readonly toolkit: LanguageModel.ToolkitInput<Tools>;
        },
      >(
        request: Request,
      ): Effect.Effect<
        LanguageModel.GenerateTextResponse<Tools>,
        LanguageModel.ExtractError<Request>,
        LanguageModel.ExtractServices<Request>
      >;
      function generateText(
        request: GenerateTextWithoutToolkit | GenerateTextWithToolkit,
      ) {
        return execute(
          Effect.flatMap(LanguageModel.LanguageModel, (model) =>
            request.toolkit === undefined
              ? model.generateText(request)
              : model.generateText(request),
          ),
          request.toolkit === undefined ||
            request.disableToolCallResolution === true,
        );
      }

      function generateObject<
        ObjectEncoded extends Record<string, unknown>,
        StructuredOutputSchema extends Schema.Encoder<ObjectEncoded, unknown>,
        Tools extends Record<string, Tool.Any>,
        Request extends LanguageModel.GenerateObjectOptions<
          Tools,
          StructuredOutputSchema
        >,
      >(
        request: Request,
      ): Effect.Effect<
        LanguageModel.GenerateObjectResponse<
          Tools,
          StructuredOutputSchema['Type']
        >,
        LanguageModel.ExtractError<Request>,
        | LanguageModel.ExtractServices<Request>
        | StructuredOutputSchema['DecodingServices']
      >;
      function generateObject(
        request: LanguageModel.GenerateObjectOptions<
          Record<string, Tool.Any>,
          Schema.Encoder<Record<string, unknown>, unknown>
        >,
      ) {
        return execute(
          Effect.flatMap(LanguageModel.LanguageModel, (model) =>
            model.generateObject(request),
          ),
          request.toolkit === undefined ||
            request.disableToolCallResolution === true,
        );
      }

      function streamText<
        Request extends LanguageModel.GenerateTextOptions<{}> & {
          readonly toolkit?: undefined;
        },
      >(
        request: Request,
      ): Stream.Stream<
        Response.StreamPart<{}>,
        LanguageModel.ExtractError<Request>,
        LanguageModel.ExtractServices<Request>
      >;
      function streamText<
        Tools extends Record<string, Tool.Any>,
        Request extends LanguageModel.GenerateTextOptions<Tools> & {
          readonly toolkit: LanguageModel.ToolkitInput<Tools>;
        },
      >(
        request: Request,
      ): Stream.Stream<
        Response.StreamPart<Tools>,
        LanguageModel.ExtractError<Request>,
        LanguageModel.ExtractServices<Request>
      >;
      function streamText(
        request: GenerateTextWithoutToolkit | GenerateTextWithToolkit,
      ) {
        return executeStream(
          Stream.unwrap(
            Effect.map(LanguageModel.LanguageModel, (model) =>
              request.toolkit === undefined
                ? model.streamText(request)
                : model.streamText(request),
            ),
          ),
        );
      }

      return LanguageModel.LanguageModel.of({
        generateText,
        generateObject,
        streamText,
      });
    }),
  );

export * as ModelPlan from './model-plan.js';
