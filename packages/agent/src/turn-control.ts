import { Effect, Option, Queue } from 'effect';
import { Prompt, type LanguageModel, type Tool } from 'effect/unstable/ai';

import type { Stop } from './stop.js';

export interface State<
  Tools extends Record<string, Tool.Any>,
> extends Stop.State<Tools> {
  /** Whether `stopWhen` would end the run if no continuation is returned. */
  readonly wouldStop: boolean;
}

/**
 * Input and model selection for one more turn.
 *
 * A returned model is Effect's `LanguageModel.Service`, not a Vesper model
 * registry entry. Provider-specific reasoning and request configuration stay
 * at the official provider seam and apply to this and later turns.
 */
export interface Continuation {
  readonly input: Prompt.RawInput;
  readonly model?: LanguageModel.Service | undefined;
}

/** An effectful policy evaluated on complete turn information. */
export type Policy<Tools extends Record<string, Tool.Any>, R = never> = (
  state: State<Tools>,
) => Effect.Effect<Option.Option<Continuation>, never, R>;

/** Keep the ordinary `stopWhen` decision. */
export const keep: Option.Option<Continuation> = Option.none();

/** Continue, optionally adding input or selecting the model for later turns. */
export const continueWith = (
  input: Prompt.RawInput,
  options?: { readonly model?: LanguageModel.Service | undefined },
): Option.Option<Continuation> =>
  Option.some({
    input,
    ...(options?.model === undefined ? {} : { model: options.model }),
  });

export type QueueMode = 'one-at-a-time' | 'all';

const drainAll = (
  queue: Queue.Dequeue<Prompt.RawInput>,
  inputs: ReadonlyArray<Prompt.RawInput>,
): Effect.Effect<ReadonlyArray<Prompt.RawInput>> =>
  Effect.flatMap(
    Queue.poll(queue),
    Option.match({
      onNone: () => Effect.succeed(inputs),
      onSome: (input) => drainAll(queue, [...inputs, input]),
    }),
  );

const drain = (
  queue: Queue.Dequeue<Prompt.RawInput>,
  mode: QueueMode,
): Effect.Effect<ReadonlyArray<Prompt.RawInput>> =>
  mode === 'all'
    ? drainAll(queue, [])
    : Effect.map(Queue.poll(queue), Option.toArray);

/**
 * Continue with queued follow-ups only when the run would otherwise stop.
 *
 * The queue belongs to the run's application scope, not the inert agent
 * definition, so Effect supplies ownership and backpressure without mutable
 * state hidden inside an agent object.
 */
export const followUps =
  <Tools extends Record<string, Tool.Any>>(
    queue: Queue.Dequeue<Prompt.RawInput>,
    options?: { readonly mode?: QueueMode | undefined },
  ): Policy<Tools> =>
  (state) =>
    !state.wouldStop
      ? Effect.succeed(keep)
      : Effect.map(drain(queue, options?.mode ?? 'one-at-a-time'), (inputs) =>
          inputs.length === 0
            ? keep
            : continueWith(
                inputs.reduce<Prompt.Prompt>(
                  (prompt, input) => Prompt.concat(prompt, Prompt.make(input)),
                  Prompt.empty,
                ),
              ),
        );

export * as TurnControl from './turn-control.js';
