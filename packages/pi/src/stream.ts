import type {
  AssistantMessageEvent,
  Usage as PiUsage,
} from '@earendil-works/pi-ai';
import type { Response } from 'effect/unstable/ai';

// Pi's event protocol and `Response.StreamPart` describe the same thing with
// different names. This module is that rename, and nothing else — no I/O, no
// Effect, no provider knowledge. Keeping it pure is what makes the mapping
// testable against recorded Pi transcripts.
//
// Two events have no direct counterpart and are handled deliberately:
//
//   `start`  Pi announces the assistant message before any content. There is
//            no "response started" part; the metadata it carries is emitted
//            as `response-metadata` instead.
//
//   `error`  Pi terminates in-band with an error event. That is NOT mapped to
//            an `error` part — it becomes a stream failure, so callers get a
//            typed `AiError` in the error channel. See `errors.ts`.
//
// Pi emits a single `toolcall_end` carrying the complete call. Effect's
// protocol separates parameter streaming (`tool-params-*`) from the resolved
// call (`tool-call`), so one Pi event fans out to two parts. Consumers that
// key tool lifecycle off `tool-params-start` would otherwise never fire.

/**
 * Translate one Pi event into zero or more encoded stream parts.
 *
 * Returns an empty array for events that carry no downstream meaning, and
 * for `error` — which the caller must convert into a stream failure rather
 * than a part.
 */
export const toStreamParts = (
  event: AssistantMessageEvent,
): ReadonlyArray<Response.StreamPartEncoded> => {
  switch (event.type) {
    case 'start': {
      const responseId = event.partial.responseId;
      return responseId === undefined
        ? []
        : [{ type: 'response-metadata', id: responseId }];
    }

    case 'text_start':
      return [{ type: 'text-start', id: contentId(event.contentIndex) }];
    case 'text_delta':
      return [
        {
          type: 'text-delta',
          id: contentId(event.contentIndex),
          delta: event.delta,
        },
      ];
    case 'text_end':
      return [{ type: 'text-end', id: contentId(event.contentIndex) }];

    case 'thinking_start':
      return [{ type: 'reasoning-start', id: contentId(event.contentIndex) }];
    case 'thinking_delta':
      return [
        {
          type: 'reasoning-delta',
          id: contentId(event.contentIndex),
          delta: event.delta,
        },
      ];
    case 'thinking_end':
      return [{ type: 'reasoning-end', id: contentId(event.contentIndex) }];

    // Pi streams raw argument JSON before it knows the tool name; the name
    // only lands with `toolcall_end`. `tool-params-start` requires a name, so
    // the start/delta pair is withheld and replayed from `toolcall_end` below.
    case 'toolcall_start':
    case 'toolcall_delta':
      return [];

    case 'toolcall_end': {
      const call = event.toolCall;
      return [
        { type: 'tool-params-start', id: call.id, name: call.name },
        {
          type: 'tool-params-delta',
          id: call.id,
          delta: JSON.stringify(call.arguments),
        },
        { type: 'tool-params-end', id: call.id },
        {
          type: 'tool-call',
          id: call.id,
          name: call.name,
          params: call.arguments,
        },
      ];
    }

    case 'done':
      return [
        {
          type: 'finish',
          reason: finishReasonOf(event.reason),
          usage: toUsage(event.message.usage),
        },
      ];

    // Terminal failure. The caller fails the stream with `fromPiError`.
    case 'error':
      return [];
  }
};

/** True for the Pi event that must terminate the stream with a failure. */
export const isTerminalError = (
  event: AssistantMessageEvent,
): event is Extract<AssistantMessageEvent, { type: 'error' }> =>
  event.type === 'error';

/**
 * Pi identifies content blocks positionally; Effect identifies them by
 * string id. The index is stable within one assistant message, which is the
 * only scope in which these ids must be unique.
 */
const contentId = (contentIndex: number): string => `pi-${contentIndex}`;

const finishReasonOf = (
  reason: Extract<AssistantMessageEvent, { type: 'done' }>['reason'],
): Response.FinishPartEncoded['reason'] => {
  switch (reason) {
    case 'stop':
      return 'stop';
    case 'length':
      return 'length';
    case 'toolUse':
      return 'tool-calls';
  }
};

/**
 * Pi reports `input` as the **uncached** prompt tokens, with cache reads and
 * cache writes counted separately — the same split every provider SDK it wraps
 * uses, and the same one Pi's own `totalTokens` reconstructs as
 * `input + output + cacheRead + cacheWrite`. Effect's `Response.Usage` asks for
 * the other decomposition: `total` is "the total number of input tokens used"
 * and `uncached` is "the number of non-cached input tokens", so the whole
 * prompt is `total` and Pi's `input` is `uncached`.
 *
 * This used to read `input` as the total and derive `uncached` by subtracting
 * `cacheRead`, which is wrong in a way nothing local could catch. Pi's
 * Anthropic adapter puts a `cache_control` breakpoint on the system prompt, the
 * tool list, and the last user message of **every** request, so any prompt over
 * Anthropic's cache minimum is cached whether or not the caller asked — and a
 * cached prompt reports `input_tokens: 3` beside `cache_creation_input_tokens:
 * 6711`. A live run of `scripts/live-smoke.ts --phase usage` reported a whole
 * two-call conversation as having spent six input tokens.
 *
 * Everything downstream reads this figure: the loop's running `Stop.Usage`, the
 * `TurnFinished`/`Completed`/`RunSettled` records, `AgentHistory.usageFrom`,
 * and — the one that silently changes behaviour rather than a number —
 * `ContextWindow.usageFromTurn`, which anchors the compaction estimate on it.
 * Anchored on the uncached remainder, a long cached conversation looks like a
 * few tokens of context and the proactive trigger never fires.
 *
 * Cost is dropped: `Response.Usage` is a token record, and pricing belongs to
 * the caller's accounting, not to the transport.
 */
export const toUsage = (
  usage: PiUsage,
): Response.FinishPartEncoded['usage'] => ({
  inputTokens: {
    total: usage.input + usage.cacheRead + usage.cacheWrite,
    uncached: usage.input,
    cacheRead: usage.cacheRead,
    cacheWrite: usage.cacheWrite,
  },
  outputTokens: {
    total: usage.output,
  },
});

export * as PiStream from './stream.js';
