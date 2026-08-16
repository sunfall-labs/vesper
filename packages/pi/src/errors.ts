import {
  isContextOverflow as isPiContextOverflow,
  type AssistantMessage,
} from '@earendil-works/pi-ai';
import { Duration } from 'effect';
import { AiError } from 'effect/unstable/ai';

// Pi reports failure in-band: a stream terminates with an `error` event
// carrying an `AssistantMessage` whose `stopReason` is "error" or "aborted"
// and whose `errorMessage` is an unstructured provider string. Every caller
// that wants to know "should I retry this?" therefore ends up sniffing that
// string.
//
// This module is the single place that sniffing happens. Downstream code
// matches on `AiError` reason tags and reads `isRetryable` / `retryAfter`,
// which `effect/unstable/ai` already defines per reason.
//
// Classification is deliberately conservative: an unrecognized failure maps
// to `UnknownError` (not retryable) rather than being guessed into a
// retryable bucket, because a wrong retry on a non-idempotent turn is worse
// than a missed one.

export interface ClassifyOptions {
  /** Module name recorded on the `AiError`, e.g. `"PiModel"`. */
  readonly module: string;
  /** Method name recorded on the `AiError`, e.g. `"streamText"`. */
  readonly method: string;
}

/**
 * Convert Pi's terminal error message into a typed `AiError`.
 *
 * `stopReason: "aborted"` is mapped to `InvalidRequestError` rather than a
 * retryable reason — an abort is a deliberate cancellation, and retrying it
 * just re-runs work someone already cancelled. Durable-execution middleware
 * for other AI SDKs arrives at the same rule for the same reason.
 */
export const fromPiError = (
  message: AssistantMessage,
  options: ClassifyOptions,
): AiError.AiError =>
  new AiError.AiError({
    module: options.module,
    method: options.method,
    reason: reasonFor(message),
  });

const reasonFor = (message: AssistantMessage): AiError.AiErrorReason => {
  const text = message.errorMessage ?? '';

  if (message.stopReason === 'aborted') {
    return new AiError.InvalidRequestError({
      constraint: 'aborted',
      description: text === '' ? 'Request was aborted' : text,
    });
  }

  const status = httpStatusOf(text);

  if (status === 429 || /rate.?limit|too many requests/i.test(text)) {
    const retryAfter = retryAfterOf(text);
    return new AiError.RateLimitError(
      retryAfter === undefined ? {} : { retryAfter },
    );
  }

  if (/quota|insufficient.?(funds|credit|balance)|billing/i.test(text)) {
    return new AiError.QuotaExhaustedError({});
  }

  // Any mention of a key or credential is treated as auth. Quota is checked
  // first, so "insufficient credit" does not get pulled in here.
  if (
    status === 401 ||
    status === 403 ||
    /unauthorized|forbidden|authentication|api.?key|credential|token expired/i.test(
      text,
    )
  ) {
    return new AiError.AuthenticationError({ kind: authKindOf(text, status) });
  }

  if (/content.?(policy|filter)|safety|blocked by/i.test(text)) {
    return new AiError.ContentPolicyError({
      description: text === '' ? 'Content policy violation' : text,
    });
  }

  // Context-window overflow is a request-shape problem, not a transient one:
  // retrying the identical prompt fails identically. Callers recover by
  // compacting and re-issuing, which is a different request. `constraint` is
  // the field the agent loop matches on to decide to compact.
  //
  // Delegated to Pi rather than pattern-matched here. This was a regex —
  // `/context.?(length|window)|too many tokens|maximum context/i` — written
  // without a real provider to check it against, and it recognised roughly
  // seven of the sixteen phrasings Pi already handles. The nine it missed
  // (xAI's "maximum prompt length is X but request contains Y", Anthropic's
  // "prompt is too long: X tokens > Y maximum", Gemini's "input token count
  // exceeds the maximum", and others) would each have failed silently: no
  // overflow constraint, so the agent loop never compacts, so a long
  // conversation just starts dying and looks like a provider fault.
  //
  // Pi's detector also screens rate-limit phrasings out first, which the
  // regex did not, and is maintained against providers we do not test.
  if (isPiContextOverflow(message)) {
    return new AiError.InvalidRequestError({
      constraint: CONTEXT_OVERFLOW,
      description: text,
    });
  }

  if (status !== undefined && status >= 500) {
    return new AiError.InternalProviderError({
      description: text === '' ? `Provider returned ${status}` : text,
    });
  }

  // Transport failures map to InternalProviderError, not NetworkError:
  // NetworkError requires a full `request` record (method, url, params) and
  // Pi has already flattened the provider error to a string by the time we
  // see it. Fabricating a request to satisfy the schema would put invented
  // data in telemetry. InternalProviderError carries the same retryability
  // (`isRetryable === true`), which is what callers actually branch on.
  if (
    /ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|network|fetch failed/i.test(
      text,
    )
  ) {
    return new AiError.InternalProviderError({ description: text });
  }

  if (status !== undefined && status >= 400) {
    return new AiError.InvalidRequestError({ description: text });
  }

  return new AiError.UnknownError({
    description: text === '' ? 'Provider call failed' : text,
  });
};

/**
 * `InvalidRequestError.constraint` value marking a context-window overflow.
 * The agent loop matches on this to decide whether compacting and retrying
 * is worth attempting, rather than re-sniffing the provider string.
 */
export const CONTEXT_OVERFLOW = 'context-window';

/** True when this error means the prompt no longer fits the context window. */
export const isContextOverflow = (error: AiError.AiError): boolean =>
  error.reason._tag === 'InvalidRequestError' &&
  error.reason.constraint === CONTEXT_OVERFLOW;

const authKindOf = (
  text: string,
  status: number | undefined,
):
  | 'InvalidKey'
  | 'ExpiredKey'
  | 'MissingKey'
  | 'InsufficientPermissions'
  | 'Unknown' => {
  if (/expired/i.test(text)) return 'ExpiredKey';
  if (/missing|no api.?key|not configured/i.test(text)) return 'MissingKey';
  if (status === 403 || /forbidden|permission|scope/i.test(text)) {
    return 'InsufficientPermissions';
  }
  if (/invalid.?api.?key|invalid.?token|unauthorized/i.test(text)) {
    return 'InvalidKey';
  }
  return 'Unknown';
};

// Pi flattens provider errors to strings, so the status code — when there is
// one — has to be recovered from the text. Match a standalone 3-digit code
// so a token count or model id like "gpt-4o-500k" is not read as a status.
const httpStatusOf = (text: string): number | undefined => {
  const match = /\b(?:status|code|HTTP)\D{0,3}(\d{3})\b/i.exec(text);
  if (match?.[1] === undefined) return undefined;
  const status = Number.parseInt(match[1], 10);
  return status >= 100 && status < 600 ? status : undefined;
};

const retryAfterOf = (text: string): Duration.Duration | undefined => {
  const seconds = /retry.?after\D{0,3}(\d+(?:\.\d+)?)/i.exec(text);
  if (seconds?.[1] !== undefined) {
    return Duration.seconds(Number.parseFloat(seconds[1]));
  }
  const millis = /try again in (\d+(?:\.\d+)?)ms/i.exec(text);
  if (millis?.[1] !== undefined) {
    return Duration.millis(Number.parseFloat(millis[1]));
  }
  return undefined;
};

export * as PiErrors from './errors.js';
