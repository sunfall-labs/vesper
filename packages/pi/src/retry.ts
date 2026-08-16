import { type Cause, Duration, Effect, Queue, Schedule, Stream } from 'effect';
import { AiError, type Response } from 'effect/unstable/ai';

import type { ProviderHooks } from './model.js';

// Absorbing a provider blip, at the only granularity where it is free.
//
// This is what is left of `@sunfall/vesper-durable` after the conversation log
// subsumed checkpointing. It lives here, next to the adapter that classifies
// provider errors in the first place, because retrying is a provider concern
// and nothing else here has an opinion about `AiError.isRetryable`. Keeping it
// in a package of its own would have meant a package named for a mechanism it
// no longer contains.
//
// ## Why the retry belongs *inside* the model call
//
// A 429 retried here never reaches the agent loop, so no turn is re-run and no
// tool is re-executed over a blip the provider would have served on the next
// attempt. Retried one level up — at the turn, at the workflow — the same blip
// re-runs tool calls and re-reads history for nothing. That is the whole
// argument for this file existing rather than telling callers to
// `Effect.retry` the run.
//
// Only `isRetryable` errors qualify, which `./errors.ts` has already decided by
// reason tag: rate limits, provider 5xx, and transport faults yes; auth,
// content policy, context overflow, and aborts no. Context overflow in
// particular must not retry here — the identical prompt fails identically, and
// compaction one level up is the only thing that changes the outcome.
//
// ## Two streaming rules ride with it
//
//   1. Once a delta has reached the consumer, retrying would re-stream from
//      the start and show that output twice. `emittedLive` is the guard.
//   2. A stream that produced no content and never finished fails retryably
//      rather than settling as an empty success, which the agent loop would
//      otherwise read as a model that answered with nothing.
//
// Both come from DBOS's `@dbos-inc/vercel-ai` middleware, which is where this
// project's durability design started.

export interface Options {
  /**
   * Attempts per model call, including the first. Default 3; set to 1 to
   * disable retrying.
   */
  readonly maxAttempts?: number;

  /** Base delay for the exponential backoff between attempts. Default 500ms. */
  readonly baseDelay?: Duration.Duration;
}

/**
 * How many stream parts may sit between the provider and a slow consumer.
 *
 * Large enough that normal scheduling jitter never suspends the producer,
 * small enough that a stalled consumer cannot pin an entire response in
 * memory. `Queue.make()` defaults to infinite capacity, which means a browser
 * on a bad connection or a Slack bridge behind a rate limit buffers the whole
 * response while the provider races ahead; with a capacity and the default
 * `suspend` strategy the backpressure reaches the provider instead.
 */
const STREAM_BUFFER_PARTS = 64;

const retryPolicy = (options: Options) =>
  // Jitter matters more than usual here: a rate-limited fleet retrying in
  // lockstep re-creates the burst that caused the limit. The attempt ceiling
  // rides alongside as `times` rather than being composed in, so "3 attempts"
  // stays readable at the call site.
  Schedule.jittered(
    Schedule.exponential(options.baseDelay ?? Duration.millis(500), 2),
  ).pipe(
    // When a provider states how long to wait, guessing is strictly worse:
    // retrying early against a rate limit earns another rejection and, on
    // some providers, extends the window. `retryAfter` is added to the
    // computed backoff rather than replacing it, so the wait is never
    // shorter than what was asked for.
    Schedule.addDelay((metadata) =>
      Effect.succeed(retryAfterOf(metadata.input) ?? Duration.zero),
    ),
  );

/** The provider-stated delay, when the failure carries one. */
const retryAfterOf = (error: unknown): Duration.Duration | undefined =>
  error instanceof AiError.AiError ? error.retryAfter : undefined;

/**
 * Wrap provider hooks so transient failures are retried before anyone above
 * sees them.
 *
 * A plain function, not an `Effect`: there is nothing to acquire and no
 * service to read, which is the difference between this and the checkpointing
 * middleware it replaces.
 */
export const wrap = (
  hooks: ProviderHooks,
  options: Options = {},
): ProviderHooks => {
  const schedule = retryPolicy(options);
  const times = Math.max(0, (options.maxAttempts ?? 3) - 1);

  const generateText: ProviderHooks['generateText'] = (providerOptions) =>
    Effect.retry(hooks.generateText(providerOptions), {
      schedule,
      times,
      // Nothing has reached a consumer, so there is no `emittedLive`
      // counterpart here: a non-streaming call can always be retried.
      while: (error: AiError.AiError) => error.isRetryable,
    });

  const streamText: ProviderHooks['streamText'] = (providerOptions) =>
    Stream.unwrap(
      Effect.gen(function* () {
        // The queue is what lets the retry stay an `Effect.retry` over a
        // whole attempt while the consumer still sees deltas as they arrive.
        // A `Stream.retry` would re-run the stream on any failure, including
        // one that arrived after output had already shipped.
        const queue = yield* Queue.make<
          Response.StreamPartEncoded,
          AiError.AiError | Cause.Done
        >({ capacity: STREAM_BUFFER_PARTS });

        // Flipped the moment anything is handed to the consumer, which is the
        // point after which a retry would duplicate visible output.
        let emittedLive = false;
        const seen = { content: false, finish: false };

        const attempt = hooks.streamText(providerOptions).pipe(
          Stream.tap((part) =>
            Queue.offer(queue, part).pipe(
              Effect.asVoid,
              Effect.tap(() =>
                Effect.sync(() => {
                  emittedLive = true;
                  if (part.type === 'finish') seen.finish = true;
                  else if (isContent(part)) seen.content = true;
                }),
              ),
            ),
          ),
          Stream.runDrain,
          // Rule 2. `InternalProviderError` is retryable, which is the
          // behaviour wanted: a provider that returned nothing usually
          // returns something on the next attempt.
          Effect.flatMap(() =>
            seen.content || seen.finish
              ? Effect.void
              : Effect.fail(emptyStreamError),
          ),
        );

        // Forked into the stream's scope: the consumer pulls from the queue
        // while this drains the provider. Interrupting the consumer
        // interrupts this too, which is what carries cancellation through to
        // the provider.
        yield* Effect.retry(attempt, {
          schedule,
          times,
          while: (error: AiError.AiError) => error.isRetryable && !emittedLive,
        }).pipe(
          // The queue must always terminate, including on a defect —
          // otherwise the consumer hangs forever waiting for a part that will
          // never arrive.
          Effect.matchCauseEffect({
            onFailure: (cause) => Queue.failCause(queue, cause),
            onSuccess: () => Queue.end(queue),
          }),
          Effect.forkScoped,
        );

        return Stream.fromQueue(queue);
      }),
    );

  return { generateText, streamText };
};

/**
 * Whether a part carries something the model actually said.
 *
 * Lifecycle markers and parameter streaming do not count: a stream of
 * `text-start`/`text-end` with nothing between them produced no answer, and
 * `finish` is tracked separately because a finished-but-empty turn is a
 * legitimate result while an unfinished-and-empty one is a fault.
 */
const isContent = (part: Response.StreamPartEncoded): boolean => {
  switch (part.type) {
    case 'text-delta':
    case 'reasoning-delta':
      return part.delta.length > 0;
    case 'tool-call':
    case 'tool-result':
    case 'file':
      return true;
    default:
      return false;
  }
};

// Mirrors the AI SDK's NoOutputGeneratedError.
const emptyStreamError = new AiError.AiError({
  module: 'PiRetry',
  method: 'streamText',
  reason: new AiError.InternalProviderError({
    description: 'Model stream ended without a finish part or any output.',
  }),
});

export * as PiRetry from './retry.js';
