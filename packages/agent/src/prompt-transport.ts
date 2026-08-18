import { Encoding, Effect, Result, Schema } from 'effect';
import { Prompt } from 'effect/unstable/ai';

const TAG = '@sunfall/vesper-agent/PromptFileData';

interface Envelope {
  readonly _tag: typeof TAG;
  readonly version: 1;
  readonly encoding: 'base64' | 'url';
  readonly value: string;
}

/** A persisted prompt could not be safely handed to the model. */
export class DecodeError extends Schema.TaggedError<DecodeError>(
  '@sunfall/vesper-agent/PromptDecodeError',
)('PromptDecodeError', {
  message: Schema.String,
}) {}

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

/**
 * Decode and validate a prompt read from durable history.
 *
 * `Prompt.make` intentionally has a synchronous convenience API and throws a
 * schema parse error for malformed message arrays. History is read inside an
 * Effect, so this boundary turns both transport failures and message-shape
 * failures into a typed error before the prompt reaches the model.
 */
export const decodeMessages = (
  prompt: unknown,
): Effect.Effect<ReadonlyArray<Prompt.Message>, DecodeError> =>
  Effect.try({
    try: () => decode(prompt),
    catch: (cause) => new DecodeError({ message: messageOf(cause) }),
  }).pipe(
    Effect.flatMap((decoded) =>
      Schema.decodeUnknownEffect(Schema.Array(Prompt.Message))(decoded).pipe(
        Effect.mapError(
          (cause) =>
            new DecodeError({
              message: `Malformed persisted prompt messages: ${messageOf(cause)}`,
            }),
        ),
      ),
    ),
  );

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

const messageOf = (cause: unknown): string =>
  cause instanceof Error ? cause.message : String(cause);

export * as PromptTransport from './prompt-transport.js';
