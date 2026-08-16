import type {
  Context as PiContext,
  Message as PiMessage,
} from '@earendil-works/pi-ai';
import { Encoding, Option } from 'effect';
import type { Prompt } from 'effect/unstable/ai';

// `Prompt.Prompt` and Pi's `Context` model the same conversation with
// different message algebras. The differences that matter:
//
//   * Effect carries the system prompt as a `system` message inside the
//     content array; Pi carries it as a separate `systemPrompt` field. A
//     prompt with several system messages therefore has to be joined.
//   * Effect gives tool results their own `tool` role; Pi has a
//     `toolResult` message per call id.
//   * Effect assistant messages may carry `ToolResultPart` inline (provider
//     -executed tools); Pi keeps results in separate messages.
//   * Effect's `FilePart` covers images, documents, and anything else with a
//     media type, and its data may be base64, raw bytes, or a URL. Pi's
//     content algebra has exactly one binary carrier, `ImageContent`, and it
//     takes bare base64. See the note on attachments below.
//
// Timestamps are required by Pi's message types but never read on the
// request path — Pi only sends role and content upstream. They are set to 0
// rather than `Date.now()` so the same prompt always converts to the same
// value, which is what makes a replayed turn byte-identical to the live one.
const NO_TIMESTAMP = 0;

export const toPiContext = (
  prompt: Prompt.Prompt,
  tools: PiContext['tools'],
): PiContext => {
  const systemParts: string[] = [];
  const messages: PiMessage[] = [];

  for (const message of prompt.content) {
    switch (message.role) {
      case 'system':
        systemParts.push(message.content);
        break;

      case 'user':
        messages.push({
          role: 'user',
          content: message.content.flatMap(toUserContent),
          timestamp: NO_TIMESTAMP,
        });
        break;

      case 'assistant':
        messages.push(...toAssistantMessages(message));
        break;

      case 'tool':
        messages.push(...toToolResultMessages(message));
        break;
    }
  }

  const systemPrompt = systemParts.join('\n\n');
  return {
    ...(systemPrompt === '' ? {} : { systemPrompt }),
    messages,
    ...(tools === undefined || tools.length === 0 ? {} : { tools }),
  };
};

type PiUserContent = Extract<PiMessage, { role: 'user' }>['content'];
type PiUserPart = Exclude<PiUserContent, string>[number];

// ## Attachments
//
// Pi 0.80.2's content union is `TextContent | ThinkingContent | ImageContent |
// ToolCall`. There is no document, file, or PDF member anywhere in it, and no
// provider adapter in the package mentions `application/pdf`. **Document
// attachments cannot be sent through Pi at all**, whatever the underlying
// model supports — Anthropic's `document` block and OpenAI's `input_file` have
// no representation to travel in. That is a gap in Pi, not a gap here, and
// nothing this module does can close it. Any other consumer of Pi has the
// same hole for the same reason.
//
// So an unsendable attachment becomes a one-line marker instead of vanishing.
// Silently dropping it is the worse failure: the user writes "summarise the
// attached report", the model receives a request with no report and no
// indication one was meant to be there, and answers from imagination. The
// marker converts a confident hallucination into "I was not given the
// document", which is both true and actionable. It is deliberately not an
// attempt to represent the content — no byte stringification, no invented
// extraction. A caller that needs document input extracts text upstream and
// sends it as a text part, which is the only thing that actually reaches the
// model.
//
// Images do go through, and this handles all three shapes `FilePart.data`
// admits. Pi wants **bare** base64: its Anthropic adapter drops the string
// straight into `source.data`, and its OpenAI adapter builds
// `data:${mimeType};base64,${data}` around it. Passing a data URL through
// unchanged — which Effect's own `FilePart` docs use as the example value —
// therefore produces a doubled prefix and a request the provider rejects.

const toUserContent = (part: Prompt.UserMessagePart): PiUserPart[] => {
  if (part.type === 'text') {
    return [{ type: 'text', text: part.text }];
  }
  return [toFileContent(part)];
};

const toFileContent = (part: Prompt.FilePart): PiUserPart => {
  const data = toInlineBase64(part.data);
  if (Option.isNone(data)) {
    // A URL is the common case here. Resolving it would mean a network fetch
    // inside a pure, synchronous conversion that the durability layer replays,
    // so the adapter refuses rather than acquiring an invisible I/O dependency
    // and a non-deterministic prompt.
    return omitted(part, 'its data is a URL or is not inline base64');
  }
  if (!part.mediaType.startsWith('image/')) {
    return omitted(part, 'the provider protocol carries text and images only');
  }
  return { type: 'image', data: data.value, mimeType: part.mediaType };
};

const BASE64_DATA_URL = /^data:[^,]*;base64,([\s\S]*)$/;

/**
 * Reduce `FilePart.data` to the bare base64 Pi expects.
 *
 * `None` for a `URL`, and for a data URL that is not base64-encoded — passing
 * either along would hand the provider a string that is not the bytes.
 */
const toInlineBase64 = (
  data: string | Uint8Array | URL,
): Option.Option<string> => {
  if (typeof data === 'string') {
    if (!data.startsWith('data:')) return Option.some(data);
    const match = BASE64_DATA_URL.exec(data);
    return match === null ? Option.none() : Option.some(match[1]!);
  }
  if (data instanceof Uint8Array)
    return Option.some(Encoding.encodeBase64(data));
  return Option.none();
};

/**
 * The marker left in place of an attachment that cannot be sent.
 *
 * Deterministic in every input, so a replayed turn converts to the same string
 * as the live one and the checkpoint hash still matches.
 */
const omitted = (part: Prompt.FilePart, reason: string): PiTextPart => ({
  type: 'text',
  text: `[attachment omitted: ${part.fileName ?? 'unnamed'} (${part.mediaType}) — ${reason}]`,
});

type PiTextPart = Extract<PiUserPart, { type: 'text' }>;

const toAssistantMessages = (message: Prompt.AssistantMessage): PiMessage[] => {
  const content: Extract<PiMessage, { role: 'assistant' }>['content'] = [];
  const trailing: PiMessage[] = [];

  for (const part of message.content) {
    switch (part.type) {
      case 'text':
        content.push({ type: 'text', text: part.text });
        break;
      case 'reasoning':
        content.push({ type: 'thinking', thinking: part.text });
        break;
      case 'tool-call':
        content.push({
          type: 'toolCall',
          id: part.id,
          name: part.name,
          arguments: asArguments(part.params),
        });
        break;
      // Provider-executed tools put the result inline on the assistant
      // message. Pi has no such shape, so it becomes a following
      // toolResult message and conversation order is preserved.
      case 'tool-result':
        trailing.push(
          toolResultMessage(part.id, part.name, part.result, part.isFailure),
        );
        break;
      // Pi's assistant content is text, thinking, and tool calls — an
      // assistant-authored image has nowhere to go even though the same image
      // would be sendable on a user message. Marked rather than dropped, for
      // the reason above.
      case 'file':
        content.push(
          omitted(part, 'the provider protocol carries no assistant files'),
        );
        break;
      default:
        break;
    }
  }

  return [
    {
      role: 'assistant',
      content,
      api: 'openai-completions',
      provider: 'unknown',
      model: 'unknown',
      usage: EMPTY_USAGE,
      stopReason: 'stop',
      timestamp: NO_TIMESTAMP,
    },
    ...trailing,
  ];
};

const toToolResultMessages = (message: Prompt.ToolMessage): PiMessage[] =>
  message.content.flatMap((part) =>
    part.type === 'tool-result'
      ? [toolResultMessage(part.id, part.name, part.result, part.isFailure)]
      : [],
  );

/**
 * One tool result, with its failure flag carried across.
 *
 * `isError` used to be hardcoded `false`. Both sides of that seam have the
 * field — Effect's `ToolResultPart.isFailure` and Pi's `toolResult.isError`,
 * which its Anthropic adapter writes straight into `tool_result.is_error` — so
 * the flag was being dropped rather than being unrepresentable. What the model
 * saw was a failed tool reported as a successful one whose output happened to
 * be a description of a failure, on every turn after the one that failed and on
 * every resumed conversation containing a failure.
 *
 * It is invisible against a faux provider, which never reads the flag, and
 * mostly survivable against a real one, which usually infers the failure from
 * the payload text. "Mostly" is the problem.
 */
const toolResultMessage = (
  toolCallId: string,
  toolName: string,
  result: unknown,
  isFailure: boolean,
): PiMessage => ({
  role: 'toolResult',
  toolCallId,
  toolName,
  content: [{ type: 'text', text: stringify(result) }],
  isError: isFailure,
  timestamp: NO_TIMESTAMP,
});

// Pi types tool arguments as a record. A model can emit a non-object for a
// malformed call; wrapping preserves it for the provider to reject rather
// than throwing inside the adapter.
const asArguments = (params: unknown): Record<string, unknown> =>
  typeof params === 'object' && params !== null && !Array.isArray(params)
    ? (params as Record<string, unknown>)
    : { value: params };

const stringify = (value: unknown): string =>
  typeof value === 'string' ? value : (JSON.stringify(value) ?? 'null');

// Replayed assistant messages are never re-billed, and Pi ignores usage on
// input. A zeroed record keeps conversion total and deterministic.
const EMPTY_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
} as const;

export * as PiPrompt from './prompt.js';
