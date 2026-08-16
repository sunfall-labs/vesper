import type { AssistantMessage } from '@earendil-works/pi-ai';
import { Duration } from 'effect';
import { describe, expect, it } from 'vitest';

import {
  CONTEXT_OVERFLOW,
  fromPiError,
  isContextOverflow,
} from '../src/errors.js';

const failure = (
  errorMessage: string,
  stopReason: AssistantMessage['stopReason'] = 'error',
): AssistantMessage => ({
  role: 'assistant',
  content: [],
  api: 'anthropic-messages',
  provider: 'anthropic',
  model: 'claude-sonnet-4-6',
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
  stopReason,
  errorMessage,
  timestamp: 0,
});

const classify = (
  message: string,
  stopReason?: AssistantMessage['stopReason'],
) =>
  fromPiError(failure(message, stopReason), {
    module: 'PiModel',
    method: 'streamText',
  });

describe('fromPiError', () => {
  it('classifies rate limits as retryable and recovers retry-after', () => {
    const error = classify('status 429: rate limit exceeded, retry after 30');

    expect(error.reason._tag).toBe('RateLimitError');
    expect(error.isRetryable).toBe(true);
    expect(error.retryAfter).toStrictEqual(Duration.seconds(30));
  });

  it('reads millisecond retry hints', () => {
    const error = classify('Rate limit reached. Please try again in 1500ms');

    expect(error.retryAfter).toStrictEqual(Duration.millis(1500));
  });

  // Billing exhaustion looks like a rate limit but no amount of waiting
  // fixes it, so it must not land in a retryable bucket.
  it('separates quota exhaustion from rate limiting', () => {
    const error = classify('You have insufficient credit for this request');

    expect(error.reason._tag).toBe('QuotaExhaustedError');
    expect(error.isRetryable).toBe(false);
  });

  it('narrows authentication failures to a kind', () => {
    expect(classify('status 401: invalid api key').reason).toMatchObject({
      _tag: 'AuthenticationError',
      kind: 'InvalidKey',
    });
    expect(classify('status 403: forbidden').reason).toMatchObject({
      kind: 'InsufficientPermissions',
    });
    expect(classify('API key expired').reason).toMatchObject({
      kind: 'ExpiredKey',
    });
  });

  it('marks context overflow so the loop can compact instead of retrying', () => {
    const error = classify('maximum context length is 200000 tokens');

    expect(isContextOverflow(error)).toBe(true);
    expect(error.isRetryable).toBe(false);
    expect(error.reason).toMatchObject({ constraint: CONTEXT_OVERFLOW });
  });

  it('treats 5xx and transport failures as retryable provider errors', () => {
    expect(classify('status 503: upstream unavailable').isRetryable).toBe(true);
    expect(classify('socket hang up').isRetryable).toBe(true);
    expect(classify('fetch failed: ECONNRESET').isRetryable).toBe(true);
  });

  // An abort is a deliberate cancellation. Retrying re-runs work that was
  // already called off.
  it('never marks an abort retryable', () => {
    const error = classify('', 'aborted');

    expect(error.reason).toMatchObject({ constraint: 'aborted' });
    expect(error.isRetryable).toBe(false);
  });

  it('defaults unrecognized failures to non-retryable rather than guessing', () => {
    const error = classify('something entirely novel went wrong');

    expect(error.reason._tag).toBe('UnknownError');
    expect(error.isRetryable).toBe(false);
  });

  it('does not read a model id as an HTTP status', () => {
    const error = classify(
      'model gpt-4o-500k is not available to this account',
    );

    expect(error.reason._tag).not.toBe('InternalProviderError');
  });

  it('records the call site so telemetry can attribute the failure', () => {
    const error = classify('boom');

    expect(error.module).toBe('PiModel');
    expect(error.method).toBe('streamText');
    expect(error.message).toContain('PiModel.streamText');
  });
});

// Real overflow phrasings, one per provider family, taken from the corpus Pi
// maintains in `utils/overflow`. Recorded here because the hand-written regex
// this replaced matched only about seven of them, and every miss was silent:
// no overflow constraint means the agent loop never compacts, so a long
// conversation simply starts failing and looks like a provider fault.
const OVERFLOW_MESSAGES = [
  // Anthropic
  'prompt is too long: 213462 tokens > 200000 maximum',
  // OpenAI-compatible
  "Input length (265330) exceeds model's maximum context length (262144).",
  'Your input exceeds the context window of this model',
  // Google Gemini
  'input token count exceeds the maximum',
  // xAI (Grok)
  "This model's maximum prompt length is 131072 but the request contains 537812 tokens",
  // Mistral
  'Prompt contains 300000 tokens, too large for model with 131072 maximum context length',
  // OpenRouter / Poolside
  'Input length 265330 exceeds the maximum allowed input length of 262144 tokens.',
  // llama.cpp
  'exceeds the available context size',
  // Kimi For Coding
  'Your request exceeded model token limit: 131072 (requested: 200000)',
];

describe('context overflow', () => {
  it.each(OVERFLOW_MESSAGES)('classifies %s', (message) => {
    const error = classify(message);

    expect(isContextOverflow(error)).toBe(true);
    expect(error.reason._tag).toBe('InvalidRequestError');
  });

  // The screen Pi applies before its overflow patterns, and which the regex
  // did not: a throttling message must stay retryable rather than sending the
  // loop off to compact a prompt that was never too long.
  it('does not mistake throttling for overflow', () => {
    const error = classify('status 429: rate limit exceeded, retry after 30');

    expect(isContextOverflow(error)).toBe(false);
    expect(error.isRetryable).toBe(true);
  });

  // Not covered, and worth stating plainly: some providers accept an
  // oversized request and return success (z.ai) or a truncated `length` stop
  // (Xiaomi MiMo). Pi detects those from `usage.input` against the context
  // window, but they never reach `fromPiError` because they are not errors.
  // Catching them means checking a successful turn in the agent loop.
  it.skip('detects silent overflow on a successful turn', () => {});
});
