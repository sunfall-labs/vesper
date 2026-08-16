import { Cause, Duration, Effect, Exit, Ref, Stream } from 'effect';
import {
  AiError,
  type LanguageModel,
  Prompt,
  type Response,
} from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { PiErrors } from '../src/errors.js';
import type { ProviderHooks } from '../src/model.js';
import { PiRetry } from '../src/retry.js';

// Retry granularity. The question these answer is not "does it retry" but "at
// which boundary" — and the boundary is inside one model call.
//
// A transient 429 absorbed here never reaches the agent loop, so no turn is
// re-run and no tool is re-executed over a blip the provider would have served
// on the next attempt. Retried one level up, the same blip does both.
//
// These came from `@sunfall/vesper-durable/retry.test.ts` with the checkpointing
// cases removed — the log replaced that, and what is left is the part
// `@sunfall/vesper-durable` was actually still earning.

const options = {
  prompt: Prompt.make([
    { role: 'user', content: [{ type: 'text', text: 'hello' }] },
  ]),
  tools: [],
  toolChoice: 'auto',
  responseFormat: { type: 'text' },
  span: undefined,
  previousResponseId: undefined,
  incrementalPrompt: undefined,
} as unknown as LanguageModel.ProviderOptions;

const finish: Response.StreamPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

const rateLimited = new AiError.AiError({
  module: 'test',
  method: 'streamText',
  reason: new AiError.RateLimitError({}),
});

// A provider that states how long to wait. Deliberately small so the test
// measures the policy rather than the clock.
const RETRY_AFTER = Duration.millis(120);
const rateLimitedWithDelay = new AiError.AiError({
  module: 'test',
  method: 'streamText',
  reason: new AiError.RateLimitError({ retryAfter: RETRY_AFTER }),
});

const authFailed = new AiError.AiError({
  module: 'test',
  method: 'streamText',
  reason: new AiError.AuthenticationError({ kind: 'InvalidKey' }),
});

// The one non-retryable failure the agent loop *recovers* from, one level up,
// by compacting and re-issuing a different request. Retrying it here would
// burn the whole attempt budget and the provider's rate limit on a prompt that
// is refused identically every time, and then hand the loop an error that
// arrives too late to be worth compacting for. Built the way `./errors.ts`
// builds it, so the constant it matches on is the one under test.
const contextOverflow = new AiError.AiError({
  module: 'test',
  method: 'streamText',
  reason: new AiError.InvalidRequestError({
    constraint: PiErrors.CONTEXT_OVERFLOW,
    description: 'prompt is too long: 272000 tokens > 200000 maximum',
  }),
});

/** Fails the first `failures` attempts, then succeeds. */
const flaky = (
  failures: number,
  attempts: Ref.Ref<number>,
  error: AiError.AiError,
  parts: ReadonlyArray<Response.StreamPartEncoded> = [
    { type: 'text-start', id: 'a' },
    { type: 'text-delta', id: 'a', delta: 'ok' },
    { type: 'text-end', id: 'a' },
    finish,
  ],
): ProviderHooks => ({
  generateText: () =>
    Effect.gen(function* () {
      const attempt = yield* Ref.updateAndGet(attempts, (n) => n + 1);
      if (attempt <= failures) return yield* Effect.fail(error);
      return [{ type: 'text' as const, text: 'ok' }, finish];
    }),
  streamText: () =>
    Stream.unwrap(
      Effect.gen(function* () {
        const attempt = yield* Ref.updateAndGet(attempts, (n) => n + 1);
        return attempt <= failures
          ? Stream.fail(error)
          : Stream.fromIterable(parts);
      }),
    ),
});

// Zero base delay so the tests exercise the policy, not the clock.
const fast = { baseDelay: Duration.millis(0) };

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
  Effect.runPromise(Effect.scoped(Effect.orDie(effect)));

describe('retry granularity', () => {
  it('absorbs a transient failure inside the model call', async () => {
    const result = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(2, attempts, rateLimited), fast);

        const parts = yield* wrapped
          .streamText(options)
          .pipe(Stream.runCollect);
        return { parts, attempts: yield* Ref.get(attempts) };
      }),
    );

    // Three provider attempts, one clean stream out.
    expect(result.attempts).toBe(3);
    expect(result.parts.map((part) => part.type)).toContain('finish');
  });

  // The whole point of classifying errors by reason tag rather than sniffing
  // strings: no amount of retrying fixes a bad key.
  it('does not retry a failure the provider called permanent', async () => {
    const result = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(99, attempts, authFailed), fast);

        const outcome = yield* wrapped
          .streamText(options)
          .pipe(Stream.runCollect, Effect.result);

        return { outcome, attempts: yield* Ref.get(attempts) };
      }),
    );

    expect(result.outcome._tag).toBe('Failure');
    expect(result.attempts).toBe(1);
  });

  // The case the file's own header singles out, and the expensive one to get
  // wrong: an overflowing prompt is refused identically on every attempt, so a
  // retry here spends the attempt budget and the provider's rate limit to
  // arrive at the same rejection — and delays the compaction one level up that
  // is the only thing which changes the outcome.
  it('does not retry a prompt that no longer fits the context window', async () => {
    const result = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(
          flaky(99, attempts, contextOverflow),
          fast,
        );

        const outcome = yield* wrapped
          .streamText(options)
          .pipe(Stream.runCollect, Effect.result);

        return { outcome, attempts: yield* Ref.get(attempts) };
      }),
    );

    expect(result.attempts).toBe(1);
    expect(result.outcome._tag).toBe('Failure');
    if (result.outcome._tag === 'Failure') {
      // Still recognisable as an overflow when it reaches the agent loop —
      // the retry wrapper passes it through rather than re-wrapping it, which
      // is what `Compaction.isContextOverflow` matches on.
      expect(PiErrors.isContextOverflow(result.outcome.failure)).toBe(true);
    }
  });

  it('gives up after the configured attempt ceiling', async () => {
    const attempts = await run(
      Effect.gen(function* () {
        const counter = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(99, counter, rateLimited), {
          ...fast,
          maxAttempts: 2,
        });

        yield* wrapped
          .streamText(options)
          .pipe(Stream.runCollect, Effect.result);

        return yield* Ref.get(counter);
      }),
    );

    expect(attempts).toBe(2);
  });

  it('honours maxAttempts: 1 as "do not retry"', async () => {
    const attempts = await run(
      Effect.gen(function* () {
        const counter = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(99, counter, rateLimited), {
          ...fast,
          maxAttempts: 1,
        });

        yield* wrapped
          .streamText(options)
          .pipe(Stream.runCollect, Effect.result);

        return yield* Ref.get(counter);
      }),
    );

    expect(attempts).toBe(1);
  });

  // The hazard live streaming introduces: a retried attempt re-streams from
  // the start. If output already reached the consumer, retrying duplicates
  // it. This is the case that makes the guard load-bearing rather than
  // decorative.
  it('stops retrying once output has reached the consumer', async () => {
    const result = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);

        // Emits real content, then fails — a mid-stream provider fault.
        const hooks: ProviderHooks = {
          generateText: () => Effect.fail(rateLimited),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Ref.update(attempts, (n) => n + 1);
                return Stream.concat(
                  Stream.fromIterable<Response.StreamPartEncoded>([
                    { type: 'text-start', id: 'a' },
                    { type: 'text-delta', id: 'a', delta: 'partial' },
                  ]),
                  Stream.fail(rateLimited),
                );
              }),
            ),
        };

        const wrapped = PiRetry.wrap(hooks, fast);
        const seen: string[] = [];

        const outcome = yield* wrapped.streamText(options).pipe(
          Stream.runForEach((part) =>
            Effect.sync(() => {
              seen.push(part.type);
            }),
          ),
          Effect.result,
        );

        return { outcome, attempts: yield* Ref.get(attempts), seen };
      }),
    );

    expect(result.outcome._tag).toBe('Failure');
    // One attempt only: the guard fired because deltas had already shipped.
    expect(result.attempts).toBe(1);
    // And the consumer saw that content exactly once, not twice.
    expect(result.seen.filter((type) => type === 'text-delta')).toHaveLength(1);
  });

  it('retries a non-streaming call freely, since nothing was emitted', async () => {
    const attempts = await run(
      Effect.gen(function* () {
        const counter = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(2, counter, rateLimited), fast);

        yield* wrapped.generateText(options);
        return yield* Ref.get(counter);
      }),
    );

    expect(attempts).toBe(3);
  });

  // `streamText` has two reasons to stop retrying — the error is permanent, or
  // output already shipped — and `generateText` has only the first. Testing it
  // separately is what stops the second hook from being wired to a bare
  // `Effect.retry` with no `while` at all, which would look correct on the
  // streaming tests above and hammer a bad key on the non-streaming one.
  it('does not retry a non-streaming call the provider called permanent', async () => {
    const result = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(99, attempts, authFailed), fast);

        const outcome = yield* wrapped
          .generateText(options)
          .pipe(Effect.result);

        return { outcome, attempts: yield* Ref.get(attempts) };
      }),
    );

    expect(result.outcome._tag).toBe('Failure');
    expect(result.attempts).toBe(1);
  });

  it('does not retry a non-streaming context overflow either', async () => {
    const attempts = await run(
      Effect.gen(function* () {
        const counter = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(99, counter, contextOverflow), fast);

        yield* wrapped.generateText(options).pipe(Effect.result);
        return yield* Ref.get(counter);
      }),
    );

    expect(attempts).toBe(1);
  });

  // Guessing a backoff when the provider stated one is strictly worse:
  // retrying early earns another rejection and, on some providers, extends
  // the window.
  it('waits at least as long as the provider asked', async () => {
    const elapsed = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(
          flaky(1, attempts, rateLimitedWithDelay),
          fast,
        );

        const started = Date.now();
        yield* wrapped.streamText(options).pipe(Stream.runDrain);
        return Date.now() - started;
      }),
    );

    expect(elapsed).toBeGreaterThanOrEqual(Duration.toMillis(RETRY_AFTER));
  });

  it('does not stall when the provider states no delay', async () => {
    const elapsed = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(1, attempts, rateLimited), fast);

        const started = Date.now();
        yield* wrapped.streamText(options).pipe(Stream.runDrain);
        return Date.now() - started;
      }),
    );

    expect(elapsed).toBeLessThan(Duration.toMillis(RETRY_AFTER));
  });

  // Same rule on the other hook. A `retryAfter` honoured only on the streaming
  // path is the shape a rate limit turns into a hammering loop: the
  // non-streaming call is the one a summarizer and a compaction pass make, and
  // those run in bursts.
  it('waits at least as long as the provider asked on a non-streaming call', async () => {
    const elapsed = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(
          flaky(1, attempts, rateLimitedWithDelay),
          fast,
        );

        const started = Date.now();
        yield* wrapped.generateText(options);
        return Date.now() - started;
      }),
    );

    expect(elapsed).toBeGreaterThanOrEqual(Duration.toMillis(RETRY_AFTER));
  });
});

describe('streaming rules that ride with the retry', () => {
  it('emits every part exactly once, in order', async () => {
    const seen = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(flaky(0, attempts, rateLimited), fast);
        return yield* wrapped.streamText(options).pipe(
          Stream.map((part) => part.type),
          Stream.runCollect,
        );
      }),
    );

    expect(seen).toEqual(['text-start', 'text-delta', 'text-end', 'finish']);
  });

  // A stream with no content and no finish is a fault, not an answer. Settled
  // as a success it becomes a turn where the model said nothing, which the
  // agent loop reads as a completed run with empty text.
  it('fails retryably rather than settling an empty stream as success', async () => {
    const outcome = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(
          flaky(0, attempts, rateLimited, []),
          // One attempt, so the retry does not mask the failure being asserted.
          { ...fast, maxAttempts: 1 },
        );

        return yield* wrapped
          .streamText(options)
          .pipe(Stream.runCollect, Effect.result);
      }),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect((outcome.failure as AiError.AiError).isRetryable).toBe(true);
    }
  });

  // The empty stream is retryable, and the retry is what makes the rule worth
  // having: a provider that returned nothing usually returns something next.
  it('retries an empty stream and takes the attempt that produced output', async () => {
    const result = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);

        const hooks: ProviderHooks = {
          generateText: () => Effect.fail(rateLimited),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                const attempt = yield* Ref.updateAndGet(attempts, (n) => n + 1);
                return attempt === 1
                  ? Stream.empty
                  : Stream.fromIterable<Response.StreamPartEncoded>([
                      { type: 'text-start', id: 'a' },
                      { type: 'text-delta', id: 'a', delta: 'ok' },
                      { type: 'text-end', id: 'a' },
                      finish,
                    ]);
              }),
            ),
        };

        const parts = yield* PiRetry.wrap(hooks, fast)
          .streamText(options)
          .pipe(
            Stream.map((part) => part.type),
            Stream.runCollect,
          );

        return { parts, attempts: yield* Ref.get(attempts) };
      }),
    );

    expect(result.attempts).toBe(2);
    expect(result.parts).toEqual([
      'text-start',
      'text-delta',
      'text-end',
      'finish',
    ]);
  });

  // A turn that finished without saying anything is a legitimate result — a
  // model that chose to stop — and must not be turned into a failure.
  it('accepts a finished stream that produced no content', async () => {
    const parts = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(
          flaky(0, attempts, rateLimited, [finish]),
          fast,
        );
        return yield* wrapped.streamText(options).pipe(
          Stream.map((part) => part.type),
          Stream.runCollect,
        );
      }),
    );

    expect(parts).toEqual(['finish']);
  });

  // The queue is the only thing between the provider and the consumer, so a
  // path that leaves it un-terminated hangs the consumer forever rather than
  // failing it — a run that never returns, never settles, and never times out.
  // A defect is the path that gets there, because `Effect.retry`'s `while`
  // never sees one: it is not in the error channel, so nothing above the
  // `matchCauseEffect` would end the queue if that match were an
  // `Effect.catch`.
  //
  // The timeout is the assertion. Without it a regression here does not fail
  // this test, it wedges the suite.
  it('ends the stream on a defect instead of leaving the consumer waiting', async () => {
    const exit = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const hooks: ProviderHooks = {
            generateText: () => Effect.fail(rateLimited),
            streamText: () =>
              Stream.fromEffect(
                Effect.die(new Error('the provider client blew up')),
              ),
          };

          return yield* PiRetry.wrap(hooks, fast)
            .streamText(options)
            .pipe(
              Stream.runDrain,
              Effect.timeout(Duration.seconds(2)),
              Effect.exit,
            );
        }),
      ),
    );

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      // The provider's defect, not a `TimeoutException` — which is exactly
      // what a consumer left waiting on an un-terminated queue would produce.
      expect(Cause.pretty(exit.cause)).toContain('the provider client blew up');
    }
  });

  // `Queue.make()` defaults to unbounded, and the capacity here is a deliberate
  // 64: a browser on a bad connection or a Slack bridge behind a rate limit
  // must push back on the provider rather than buffer a whole response. What
  // must not come with that is a dropped or reordered part, which is what a
  // `dropping`/`sliding` strategy would silently do to a long answer.
  it('backpressures a slow consumer without losing or reordering parts', async () => {
    const COUNT = 300; // Comfortably past the buffer capacity.
    const produced: Response.StreamPartEncoded[] = [
      { type: 'text-start', id: 'a' },
      ...Array.from({ length: COUNT }, (_, index) => ({
        type: 'text-delta' as const,
        id: 'a',
        delta: String(index),
      })),
      { type: 'text-end', id: 'a' },
      finish,
    ];

    const seen = await run(
      Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const wrapped = PiRetry.wrap(
          flaky(0, attempts, rateLimited, produced),
          fast,
        );

        const deltas: string[] = [];
        yield* wrapped.streamText(options).pipe(
          Stream.runForEach((part) =>
            // A consumer that is always behind the producer.
            Effect.yieldNow.pipe(
              Effect.andThen(
                Effect.sync(() => {
                  if (part.type === 'text-delta') deltas.push(part.delta);
                }),
              ),
            ),
          ),
        );
        return deltas;
      }),
    );

    expect(seen).toHaveLength(COUNT);
    expect(seen).toEqual(Array.from({ length: COUNT }, (_, i) => String(i)));
  });
});
