import { AttachmentStoreMemory } from '@sunfall/vesper-attachments/layer-memory';
import { describe, expect, it } from '@effect/vitest';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer } from 'effect';
import { Prompt } from 'effect/ai';

import { Conversation } from '../src/conversation.js';
import {
  plainAgent,
  present,
  provider,
  readRecords,
  run,
  says,
  seed,
  started,
  testLogLayer,
} from './durability-fixtures.js';

// Prompt transport: file data in a prompt survives the log. The model that
// sees the conversation on the next run must get back exactly what the caller
// first sent, whether the bytes were inlined or externalized.

const filePrompt = (data: string | Uint8Array | URL): Prompt.RawInput => [
  {
    role: 'user',
    content: [
      Prompt.makePart('file', {
        mediaType: 'application/octet-stream',
        fileName: 'input.bin',
        data,
      }),
    ],
  },
];

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null;

/** The data of the first file part in a prompt, however it is nested. */
const fileData = (prompt: unknown): unknown => {
  const messages: ReadonlyArray<unknown> = Array.isArray(prompt) ? prompt : [];
  for (const message of messages) {
    const parts: ReadonlyArray<unknown> =
      isRecord(message) && Array.isArray(message['content'])
        ? message['content']
        : [];
    for (const part of parts) {
      if (isRecord(part) && part['data'] !== undefined) {
        return part['data'];
      }
    }
  }
  return undefined;
};

const persistedPrompt = (
  records: Effect.Success<ReturnType<typeof readRecords>>,
) => {
  const first = present(records[0]).record;
  if (first._tag !== 'RunStarted') {
    throw new Error('missing RunStarted');
  }
  return first.prompt;
};

describe('prompt transport', () => {
  it.effect.each([
    [
      'bytes',
      new Uint8Array([0, 1, 255]),
      { encoding: 'base64', value: 'AAH/' },
    ],
    [
      'a URL',
      new URL('https://example.com/files/input.bin?version=1'),
      {
        encoding: 'url',
        value: 'https://example.com/files/input.bin?version=1',
      },
    ],
  ] as const)(
    'persists %s as a Vesper envelope and rebuilds it',
    ([, data, envelope]) =>
      Effect.gen(function* () {
        const models = provider([says('ok')]);
        const written = yield* run(
          Effect.gen(function* () {
            const conversation = Conversation.make(plainAgent, 'transport');
            yield* conversation.run(filePrompt(data));
            yield* conversation.run('and then?');
            return yield* readRecords('transport');
          }),
          models.layer,
        );

        expect(fileData(persistedPrompt(written))).toMatchObject({
          _tag: '@sunfall/vesper-agent/PromptFileData',
          version: 1,
          ...envelope,
        });
        expect(fileData(present(models.asked[1]).content)).toEqual(data);
      }),
  );

  it.effect('leaves ordinary string file data unchanged', () =>
    Effect.gen(function* () {
      const data = 'data:application/octet-stream;base64,AAH/';
      const models = provider([says('ok')]);
      const written = yield* run(
        Effect.gen(function* () {
          const conversation = Conversation.make(plainAgent, 'transport');
          yield* conversation.run(filePrompt(data));
          yield* conversation.run('and then?');
          return yield* readRecords('transport');
        }),
        models.layer,
      );

      expect(fileData(persistedPrompt(written))).toBe(data);
      expect(fileData(present(models.asked[1]).content)).toBe(data);
    }),
  );

  it.effect(
    'externalizes bytes and hydrates them on resume and in a fork',
    () =>
      Effect.gen(function* () {
        const bytes = new Uint8Array([0, 1, 255]);
        const models = provider([says('ok')]);
        const written = yield* run(
          Effect.gen(function* () {
            const conversation = Conversation.make(
              plainAgent,
              'attachment-source',
            );
            yield* conversation.run(filePrompt(bytes));
            const first = present((yield* readRecords('attachment-source'))[0]);
            yield* conversation.run('resumed');
            yield* conversation.forkFrom(
              first.offset,
              'attachment-fork',
              'forked',
            );
            return yield* readRecords('attachment-source');
          }).pipe(
            Effect.provide(
              AttachmentStoreMemory.layer.pipe(
                Layer.provide(NodeServices.layer),
              ),
            ),
          ),
          models.layer,
        );

        // The log holds a reference, not the bytes.
        expect(fileData(persistedPrompt(written))).toMatchObject({
          _tag: '@sunfall/vesper-agent/PromptAttachment',
          version: 1,
          ref: { mediaType: 'application/octet-stream', byteLength: 3 },
        });
        expect(fileData(present(models.asked[1]).content)).toEqual(bytes);
        expect(fileData(present(models.asked[2]).content)).toEqual(bytes);
      }),
  );

  it.effect(
    'shows a recording policy the live bytes before transport encoding',
    () =>
      Effect.gen(function* () {
        const bytes = new Uint8Array([1, 2, 3]);
        let seen: unknown;
        const written = yield* run(
          Effect.gen(function* () {
            yield* Conversation.make(plainAgent, 'transport', {
              prompt: (prompt) => {
                seen = fileData(prompt);
                return Effect.succeed(prompt);
              },
            }).run(filePrompt(bytes));
            return yield* readRecords('transport');
          }),
          provider([says('ok')]).layer,
        );

        expect(seen).toBe(bytes);
        expect(fileData(persistedPrompt(written))).toMatchObject({
          encoding: 'base64',
        });
      }),
  );

  it.effect.each([
    [
      'transport envelope',
      [
        {
          role: 'user',
          content: [
            {
              type: 'file',
              mediaType: 'application/octet-stream',
              data: {
                _tag: '@sunfall/vesper-agent/PromptFileData',
                version: 2,
                encoding: 'base64',
                value: 'AQID',
              },
            },
          ],
        },
      ],
    ],
    [
      'message shape',
      [{ role: 'user', content: [{ type: 'not-a-prompt-part' }] }],
    ],
  ] as const)(
    'refuses to resume a malformed %s with a typed failure',
    ([, prompt]) =>
      Effect.gen(function* () {
        const failure = yield* Effect.gen(function* () {
          yield* seed('malformed', [{ ...started(), prompt }]);
          return yield* Conversation.make(plainAgent, 'malformed')
            .run('continue')
            .pipe(Effect.flip);
        }).pipe(
          Effect.provide(
            Layer.merge(provider([says('ok')]).layer, testLogLayer),
          ),
        );

        expect(failure).toMatchObject({ _tag: 'CompatibilityError' });
      }),
  );
});
