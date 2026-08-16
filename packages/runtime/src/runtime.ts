import { ContextWindow } from '@sunfall/vesper-agent/context-window';
import { PiCompaction } from '@sunfall/vesper-pi/compaction';
import { PiModel } from '@sunfall/vesper-pi/model';
import { PiRegistry } from '@sunfall/vesper-pi/registry';
import { PiRetry } from '@sunfall/vesper-pi/retry';
import { Config, Effect, Layer } from 'effect';
import { LanguageModel, Model } from 'effect/unstable/ai';

// The composition layer, and the only module that knows the whole stack.
//
// Everything below it stays ignorant of everything beside it: `@sunfall/vesper-agent`
// targets the `LanguageModel` tag and has never heard of Pi, and `@sunfall/vesper-pi`
// produces provider hooks without knowing who will run a loop over them. That
// separation is what makes each swappable — and it is only affordable because
// exactly one place pays for assembling them.
//
// This is deliberately not a barrel: it re-exports nothing, it decides which
// defaults apply and in what order layers stack.
//
// It is also deliberately not a service with `run`/`stream` methods. A
// manager object would be the one un-Effect-like thing in the family —
// Effect's own AI modules expose free functions that read services from
// context — so the shape is a `Layer` providing `LanguageModel`, after which
// `agent.run(input)` is just a call.
//
// It used to assemble a third thing: `@sunfall/vesper-durable`, which checkpointed
// every provider call so a crashed run could replay them. The conversation log
// subsumed that — a resumed run rebuilds its prompt from records and continues
// from the next turn, paying the provider for neither the completed turns nor
// the tool calls they made. What survived is the retry, which is a different
// concern that happened to live in the same package, and it now lives beside
// the adapter that classifies the errors it acts on.

export interface Options {
  /** Pi provider id, e.g. `"anthropic"`. */
  readonly provider: string;
  /** Model id within that provider. */
  readonly model: string;
  readonly modelOptions?: PiModel.ModelOptions;
  /**
   * How transient provider failures are retried, or `false` for none.
   *
   * On by default. A 429 absorbed inside the model call costs a wait; the
   * same 429 surfacing to the loop costs a re-run of the turn and everything
   * it did.
   */
  readonly retry?: PiRetry.Options | false;
}

/**
 * A Pi-backed model that retries transient provider failures.
 *
 * Returns Effect's `Model` rather than a bare `Layer`. `Model` *extends*
 * `Layer<LanguageModel | ProviderName | ModelName, never, …>`, so it is a
 * drop-in wherever a layer was used, and it adds the two things a single
 * layer cannot express:
 *
 * - `ProviderName` and `ModelName` in context, so telemetry and prompts can
 *   read which model actually served a call.
 * - `captureRequirements`, which reads this model's dependencies out of the
 *   ambient context once and hands back a self-contained layer. That is what
 *   makes a *second* model usable inside a program that already has one — a
 *   cheap judge auditing a capable speaker, say. Providing a second
 *   `AiRuntime.model(…)` per call instead rebuilds it on every call and
 *   leaks `PiRegistry` into the caller's requirements.
 *
 * Built once and shared. There is no per-run state here to scope wrongly —
 * which used not to be true: checkpointing needed a run id, it could not be a
 * service because provider hooks fix their own requirement channel, and the
 * `Context.Reference` that carried it is the cautionary tale the rest of this
 * family's "passed, not looked up" rules were written from.
 */
/**
 * Pi's context-window heuristics, installed over `@sunfall/vesper-agent`'s fallback.
 *
 * This assignment is the whole enforcement mechanism for the seam. Neither
 * package imports the other — `@sunfall/vesper-agent` states the shape it wants and
 * `@sunfall/vesper-pi` produces a value of that shape — so this line is the only
 * place a compiler ever compares the two. If either side drifts, this stops
 * compiling, which is the same guarantee `protocol.test.ts` gives the
 * context-overflow marker.
 *
 * Worth having on its own, not only inside {@link model}: a caller assembling
 * a stack by hand, or one running the loop against a non-Pi `LanguageModel`,
 * still wants the estimator that reads provider usage.
 */
export const contextWindow: Layer.Layer<never> = Layer.succeed(
  ContextWindow.Service,
  PiCompaction.heuristics,
);

export const model = (
  options: Options,
): Model.Model<string, LanguageModel.LanguageModel, PiRegistry.Service> =>
  Model.make(
    options.provider,
    options.model,
    Layer.merge(
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const hooks = yield* PiModel.hooks(
            options.provider,
            options.model,
            options.modelOptions,
          );

          return yield* LanguageModel.make(
            options.retry === false
              ? hooks
              : PiRetry.wrap(hooks, options.retry),
          );
        }),
      ),
      // Merged in rather than left to the caller, because "wired the Pi model
      // and got the character-count estimate" is a silent half-configuration:
      // nothing fails, compaction just fires late or early and the reactive
      // path absorbs the difference at the cost of a wasted turn. A
      // `Context.Reference` cannot report that it was never overridden, so the
      // default has to be the right one.
      //
      // It contributes nothing to `Provides` — a `Reference`'s identifier is
      // `never` — so this changes no signature anywhere.
      contextWindow,
    ),
  );

/**
 * The same model, with its options read from configuration.
 *
 * The pair — concrete `model(options)` alongside `modelConfig(wrapped)` — is
 * the shape Effect libraries use: production reads the provider and env,
 * tests pass values directly without going through `Config` decoding at all.
 *
 * ```ts
 * AiRuntime.modelConfig({
 *   provider: Config.string('AI_PROVIDER'),
 *   model: Config.string('AI_MODEL'),
 * });
 * ```
 *
 * A `Layer` rather than a `Model`: the provider and model names are not known
 * until the config resolves, and `Model` carries them as type parameters.
 * Anything omitted keeps its `model` default, so a caller configures only
 * what actually varies between environments.
 */
export const modelConfig = (
  config: Config.Wrap<Options>,
): Layer.Layer<
  LanguageModel.LanguageModel | Model.ProviderName | Model.ModelName,
  Config.ConfigError,
  PiRegistry.Service
> => Layer.unwrap(Effect.map(Config.unwrap(config), model));

export * as AiRuntime from './runtime.js';
