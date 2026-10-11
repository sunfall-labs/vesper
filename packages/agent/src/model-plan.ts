import { Effect, Layer, Stream, type ExecutionPlan, type Schema } from 'effect';
import { AiError, LanguageModel, type Tool } from 'effect/ai';

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

type TextRequest =
  | (Omit<
      LanguageModel.GenerateTextOptions<Record<never, never>>,
      'toolkit'
    > & {
      readonly toolkit?: undefined;
    })
  | (LanguageModel.GenerateTextOptions<Record<string, Tool.Any>> & {
      readonly toolkit: LanguageModel.ToolkitInput<Record<string, Tool.Any>>;
    });

type ObjectRequest = LanguageModel.GenerateObjectOptions<
  Record<string, Tool.Any>,
  Schema.Encoder<Record<string, unknown>, unknown>
>;

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

      // Each operation forwards its request unchanged to whichever model the
      // plan selected, so its full overload set is the service's own. The
      // implementations below are asserted to the interface members rather
      // than restating those overloads, which track Effect's internal option
      // types and would drift on every release.
      //
      // The two branches of each ternary below read the same, and are not
      // redundant: each narrows `request` to one side of `TextRequest` so the
      // call resolves to one of `LanguageModel`'s overloads. A union request
      // matches none of them.
      const generateText = (request: TextRequest) =>
        request.toolkit === undefined
          ? execute(
              Effect.flatMap(LanguageModel.LanguageModel, (model) =>
                model.generateText(request),
              ),
              true,
            )
          : execute(
              Effect.flatMap(LanguageModel.LanguageModel, (model) =>
                model.generateText(request),
              ),
              request.disableToolCallResolution === true,
            );

      const generateObject = (request: ObjectRequest) =>
        execute(
          Effect.flatMap(LanguageModel.LanguageModel, (model) =>
            model.generateObject(request),
          ),
          request.toolkit === undefined ||
            request.disableToolCallResolution === true,
        );

      const streamText = (request: TextRequest) =>
        request.toolkit === undefined
          ? executeStream(
              Stream.unwrap(
                Effect.map(LanguageModel.LanguageModel, (model) =>
                  model.streamText(request),
                ),
              ),
            )
          : executeStream(
              Stream.unwrap(
                Effect.map(LanguageModel.LanguageModel, (model) =>
                  model.streamText(request),
                ),
              ),
            );

      return LanguageModel.LanguageModel.of({
        [LanguageModel.TypeId]: LanguageModel.TypeId,
        generateText:
          generateText as LanguageModel.LanguageModel['generateText'],
        generateObject:
          generateObject as LanguageModel.LanguageModel['generateObject'],
        streamText: streamText as LanguageModel.LanguageModel['streamText'],
      });
    }),
  );

export * as ModelPlan from './model-plan.js';
