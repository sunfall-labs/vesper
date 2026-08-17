import { Encoding, Result } from 'effect';

const TAG = '@sunfall/vesper-agent/PromptFileData';

interface Envelope {
  readonly _tag: typeof TAG;
  readonly version: 1;
  readonly encoding: 'base64' | 'url';
  readonly value: string;
}

/** Make non-JSON file data durable without changing Effect's live prompt. */
export const encode = (prompt: unknown): unknown =>
  mapFileData(prompt, (data) => {
    if (data instanceof Uint8Array) {
      return {
        _tag: TAG,
        version: 1,
        encoding: 'base64',
        value: Encoding.encodeBase64(data),
      } satisfies Envelope;
    }
    if (data instanceof URL) {
      return {
        _tag: TAG,
        version: 1,
        encoding: 'url',
        value: data.href,
      } satisfies Envelope;
    }
    return data;
  });

/** Restore file data from Vesper's transport envelope, accepting legacy prompts. */
export const decode = (prompt: unknown): unknown =>
  mapFileData(prompt, (data) => {
    if (!isEnvelopeCandidate(data)) return data;
    if (
      data._tag !== TAG ||
      data.version !== 1 ||
      (data.encoding !== 'base64' && data.encoding !== 'url') ||
      typeof data.value !== 'string'
    ) {
      throw new Error('Malformed Vesper prompt file-data envelope');
    }
    if (data.encoding === 'url') {
      try {
        return new URL(data.value);
      } catch {
        throw new Error('Malformed URL in Vesper prompt file-data envelope');
      }
    }
    return Result.getOrThrowWith(
      Encoding.decodeBase64(data.value),
      () => new Error('Malformed base64 in Vesper prompt file-data envelope'),
    );
  });

const mapFileData = (
  prompt: unknown,
  transform: (data: unknown) => unknown,
): unknown => {
  if (!Array.isArray(prompt)) return prompt;
  let changed = false;
  const messages = prompt.map((message) => {
    if (!isObject(message) || !Array.isArray(message.content)) return message;
    let contentChanged = false;
    const content = message.content.map((part) => {
      if (!isObject(part) || part.type !== 'file') return part;
      const data = transform(part.data);
      if (data === part.data) return part;
      contentChanged = true;
      return { ...part, data };
    });
    if (!contentChanged) return message;
    changed = true;
    return { ...message, content };
  });
  return changed ? messages : prompt;
};

const isObject = (value: unknown): value is Record<PropertyKey, unknown> =>
  typeof value === 'object' && value !== null;

const isEnvelopeCandidate = (
  value: unknown,
): value is Record<PropertyKey, unknown> =>
  isObject(value) && '_tag' in value && value._tag === TAG;

export * as PromptTransport from './prompt-transport.js';
