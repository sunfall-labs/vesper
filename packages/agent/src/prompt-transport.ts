import { AttachmentRef } from '@sunfall/vesper-attachments/ref';
import { AttachmentStore } from '@sunfall/vesper-attachments/attachment-store';
import { Encoding, Effect, Result, Schema } from 'effect';
import { Prompt } from 'effect/unstable/ai';

const TAG = '@sunfall/vesper-agent/PromptFileData';
const ATTACHMENT_TAG = '@sunfall/vesper-agent/PromptAttachment';

interface Envelope {
  readonly _tag: typeof TAG;
  readonly version: 1;
  readonly encoding: 'base64' | 'url';
  readonly value: string;
}

interface AttachmentEnvelope {
  readonly _tag: typeof ATTACHMENT_TAG;
  readonly version: 1;
  readonly ref: AttachmentRef.Ref;
}

interface AttachmentEnvelopeCandidate {
  readonly _tag: typeof ATTACHMENT_TAG;
  readonly version: number;
  readonly ref: unknown;
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
    if (!isEnvelopeCandidate(data)) {
      return data;
    }
    if (
      data['_tag'] !== TAG ||
      data['version'] !== 1 ||
      (data['encoding'] !== 'base64' && data['encoding'] !== 'url') ||
      typeof data['value'] !== 'string'
    ) {
      throw new Error('Malformed Vesper prompt file-data envelope');
    }
    if (data['encoding'] === 'url') {
      try {
        return new URL(data['value']);
      } catch {
        throw new Error('Malformed URL in Vesper prompt file-data envelope');
      }
    }
    return Result.getOrThrowWith(
      Encoding.decodeBase64(data['value']),
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

/**
 * Persist file bytes in an explicit {@link AttachmentStore} while retaining
 * URLs and ordinary provider data inline. This is deliberately separate from
 * {@link encode}: callers that do not provide an attachment store keep the
 * existing inline transport and acquire no new requirement.
 */
export const encodeWithAttachments = (
  prompt: unknown,
): Effect.Effect<
  unknown,
  AttachmentStore.AttachmentStoreError,
  AttachmentStore.Service
> =>
  mapFileDataEffect(prompt, (data, part) => {
    if (!(data instanceof Uint8Array)) {
      return Effect.succeed(data);
    }
    const mediaType =
      typeof part['mediaType'] === 'string'
        ? part['mediaType']
        : 'application/octet-stream';
    return Effect.gen(function* () {
      const store = yield* AttachmentStore.Service;
      const ref = yield* store.put(data, { mediaType });
      return {
        _tag: ATTACHMENT_TAG,
        version: 1,
        ref,
      } satisfies AttachmentEnvelope;
    });
  });

/** Restore attachment references before a resumed prompt is rebuilt. */
export const decodeWithAttachments = (
  prompt: unknown,
): Effect.Effect<
  unknown,
  DecodeError | AttachmentStore.GetError,
  AttachmentStore.Service
> =>
  mapFileDataEffect(prompt, (data) => {
    if (!isAttachmentEnvelope(data)) {
      return Effect.try({
        try: () => decode(data),
        catch: (cause) => new DecodeError({ message: messageOf(cause) }),
      });
    }
    if (data.version !== 1) {
      return Effect.fail(
        new DecodeError({ message: 'Unsupported Vesper attachment envelope' }),
      );
    }
    return Schema.decodeUnknownEffect(AttachmentRef.Ref)(data.ref).pipe(
      Effect.mapError(
        (cause) =>
          new DecodeError({
            message: `Malformed Vesper attachment reference: ${messageOf(cause)}`,
          }),
      ),
      Effect.flatMap((ref) =>
        Effect.gen(function* () {
          const store = yield* AttachmentStore.Service;
          return yield* store.get(ref);
        }),
      ),
    );
  });

/** Decode a persisted prompt while resolving every content-addressed file. */
export const decodeMessagesWithAttachments = (
  prompt: unknown,
): Effect.Effect<
  ReadonlyArray<Prompt.Message>,
  DecodeError | AttachmentStore.GetError,
  AttachmentStore.Service
> =>
  decodeWithAttachments(prompt).pipe(
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
  if (!isUnknownArray(prompt)) {
    return prompt;
  }
  const messages = prompt.map((message) => {
    if (!isObject(message) || !isUnknownArray(message['content'])) {
      return message;
    }
    const originalContent = message['content'];
    const content = message['content'].map((part) => {
      if (!isObject(part) || part['type'] !== 'file') {
        return part;
      }
      const data = transform(part['data']);
      if (data === part['data']) {
        return part;
      }
      return { ...part, data };
    });
    if (content.every((part, index) => part === originalContent[index])) {
      return message;
    }
    return { ...message, content };
  });
  return messages.some((message, index) => message !== prompt[index])
    ? messages
    : prompt;
};

const mapFileDataEffect = <E, R>(
  prompt: unknown,
  transform: (
    data: unknown,
    part: Record<PropertyKey, unknown>,
  ) => Effect.Effect<unknown, E, R>,
): Effect.Effect<unknown, E, R> =>
  !isUnknownArray(prompt)
    ? Effect.succeed(prompt)
    : Effect.map(
        Effect.forEach(prompt, (message) => {
          if (!isObject(message) || !isUnknownArray(message['content'])) {
            return Effect.succeed(message);
          }
          const originalContent = message['content'];
          return Effect.map(
            Effect.forEach(originalContent, (part) => {
              if (!isObject(part) || part['type'] !== 'file') {
                return Effect.succeed(part);
              }
              return Effect.map(transform(part['data'], part), (data) =>
                data === part['data'] ? part : { ...part, data },
              );
            }),
            (content) =>
              content.every((part, index) => part === originalContent[index])
                ? message
                : { ...message, content },
          );
        }),
        (messages) => messages,
      );

const isObject = (value: unknown): value is Record<PropertyKey, unknown> =>
  typeof value === 'object' && value !== null;

const isUnknownArray = (value: unknown): value is ReadonlyArray<unknown> =>
  Array.isArray(value);

const isEnvelopeCandidate = (
  value: unknown,
): value is Record<PropertyKey, unknown> =>
  isObject(value) && '_tag' in value && value['_tag'] === TAG;

const isAttachmentEnvelope = (
  value: unknown,
): value is AttachmentEnvelopeCandidate =>
  isObject(value) && '_tag' in value && value['_tag'] === ATTACHMENT_TAG;

const messageOf = (cause: unknown): string =>
  cause instanceof Error ? cause.message : String(cause);

export * as PromptTransport from './prompt-transport.js';
