import { AttachmentStoreMemory } from '@sunfall/vesper-attachments/layer-memory';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Cause, Context, Effect, Exit, Layer, Option } from 'effect';
import { Prompt } from 'effect/unstable/ai';

import { AgentHistory } from '../src/history.js';
import * as AgentLog from '../src/log.js';
import { PromptTransport } from '../src/prompt-transport.js';
import { RecordingPolicyRuntime } from '../src/recording-policy-runtime.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

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

const runStarted = (
  prompt: unknown,
): ReadonlyArray<ConversationRecord.Envelope> => [
  {
    offset: LogOffset.fromSeq(1n),
    conversationId: LogVocabulary.ConversationId.make('transport'),
    timestamp: 0,
    record: { _tag: 'RunStarted', agent: 'test', prompt },
  },
];

const fileData = (prompt: unknown): unknown =>
  (
    prompt as ReadonlyArray<{
      readonly content: ReadonlyArray<{ readonly data?: unknown }>;
    }>
  )[0]?.content[0]?.data;

const persist = (input: Prompt.RawInput) =>
  Effect.gen(function* () {
    const session = yield* AgentLog.open(
      LogVocabulary.ConversationId.make('transport'),
      {
        compatibility: {
          agent: 'test',
          revision: LogVocabulary.AgentRevision.make('1'),
        },
      },
    );
    yield* AgentLog.start(session, {
      agent: 'test',
      revision: LogVocabulary.AgentRevision.make('1'),
      input,
    });
    const first = (yield* session.recorded)[0];
    if (first === undefined) {
      throw new Error('missing RunStarted record');
    }
    const started = first.record;
    if (started._tag !== 'RunStarted') {
      throw new Error('missing RunStarted');
    }
    return started.prompt;
  }).pipe(Effect.provide(testLogLayer));

describe('prompt transport', () => {
  it.effect('persists and rebuilds Uint8Array file data', () =>
    Effect.gen(function* () {
      const persisted = yield* persist(filePrompt(new Uint8Array([0, 1, 255])));

      expect(fileData(persisted)).toMatchObject({
        _tag: '@sunfall/vesper-agent/PromptFileData',
        version: 1,
        encoding: 'base64',
        value: 'AAH/',
      });
      expect(
        fileData(AgentHistory.messagesFrom(runStarted(persisted)).content),
      ).toEqual(new Uint8Array([0, 1, 255]));
    }),
  );

  it.effect('externalizes bytes and hydrates them on resume when enabled', () =>
    Effect.gen(function* () {
      const bytes = new Uint8Array([0, 1, 255]);
      const source = yield* AgentLog.open(
        LogVocabulary.ConversationId.make('attachment-source'),
        {
          compatibility: {
            agent: 'test',
            revision: LogVocabulary.AgentRevision.make('1'),
          },
        },
      );
      yield* AgentLog.start(source, {
        agent: 'test',
        revision: LogVocabulary.AgentRevision.make('1'),
        input: filePrompt(bytes),
      });
      const sourceRecords = yield* source.recorded;
      const sourceRecord = sourceRecords[0];
      if (sourceRecord === undefined) {
        throw new Error('missing source record');
      }
      const persisted = sourceRecord.record;
      if (persisted._tag !== 'RunStarted') {
        throw new Error('missing RunStarted');
      }

      expect(fileData(persisted.prompt)).toMatchObject({
        _tag: '@sunfall/vesper-agent/PromptAttachment',
        version: 1,
        ref: { mediaType: 'application/octet-stream', byteLength: 3 },
      });
      const decoded = yield* PromptTransport.decodeMessagesWithAttachments(
        persisted.prompt,
      );
      expect(fileData(decoded)).toEqual(bytes);

      const fork = yield* AgentLog.fork(
        LogVocabulary.ConversationId.make('attachment-source'),
        sourceRecord.offset,
        LogVocabulary.ConversationId.make('attachment-fork'),
        {
          agent: 'test',
          revision: LogVocabulary.AgentRevision.make('1'),
        },
      );
      const prompt = AgentHistory.messagesFrom(fork.history).content;
      expect(fileData(prompt)).toEqual(bytes);
    }).pipe(
      Effect.provide(
        Layer.merge(
          testLogLayer,
          AttachmentStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
        ),
      ),
    ),
  );

  it.effect('persists and rebuilds URL file data', () =>
    Effect.gen(function* () {
      const persisted = yield* persist(
        filePrompt(new URL('https://example.com/files/input.bin?version=1')),
      );

      expect(fileData(persisted)).toMatchObject({
        _tag: '@sunfall/vesper-agent/PromptFileData',
        version: 1,
        encoding: 'url',
        value: 'https://example.com/files/input.bin?version=1',
      });
      expect(
        fileData(AgentHistory.messagesFrom(runStarted(persisted)).content),
      ).toEqual(new URL('https://example.com/files/input.bin?version=1'));
    }),
  );

  it.effect('leaves ordinary string file data unchanged', () =>
    Effect.gen(function* () {
      const data = 'data:application/octet-stream;base64,AAH/';
      const persisted = yield* persist(filePrompt(data));

      expect(fileData(persisted)).toBe(data);
      expect(
        fileData(AgentHistory.messagesFrom(runStarted(persisted)).content),
      ).toBe(data);
    }),
  );

  it.effect(
    'shows recording policy the live file data before transport encoding',
    () =>
      Effect.gen(function* () {
        const bytes = new Uint8Array([1, 2, 3]);
        let seen: unknown;
        const runtime = RecordingPolicyRuntime.compile(
          {
            prompt: (prompt) => {
              seen = fileData(prompt);
              return Effect.succeed(prompt);
            },
          },
          Context.empty(),
        );

        const persisted = yield* Effect.gen(function* () {
          const session = AgentLog.withRecordingPolicy(
            yield* AgentLog.open(
              LogVocabulary.ConversationId.make('policy-transport'),
              {
                compatibility: {
                  agent: 'test',
                  revision: LogVocabulary.AgentRevision.make('1'),
                },
              },
            ),
            runtime,
          );
          yield* AgentLog.start(session, {
            agent: 'test',
            revision: LogVocabulary.AgentRevision.make('1'),
            input: filePrompt(bytes),
          });
          const first = (yield* session.recorded)[0];
          if (first === undefined) {
            throw new Error('missing RunStarted record');
          }
          const started = first.record;
          if (started._tag !== 'RunStarted') {
            throw new Error('missing RunStarted');
          }
          return started.prompt;
        }).pipe(Effect.provide(testLogLayer));

        expect(seen).toBe(bytes);
        expect(fileData(persisted)).toMatchObject({ encoding: 'base64' });
      }),
  );

  it('rejects a malformed Vesper file-data envelope', () => {
    const prompt = [
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
    ];

    expect(() => AgentHistory.messagesFrom(runStarted(prompt))).toThrow(
      'Malformed Vesper prompt file-data envelope',
    );
  });

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
    'reports a malformed %s as a typed open failure',
    ([kind, prompt]) =>
      Effect.gen(function* () {
        const conversationId = LogVocabulary.ConversationId.make(
          `typed-${kind.replaceAll(' ', '-')}-error`,
        );
        const compatibility = {
          agent: 'test',
          revision: LogVocabulary.AgentRevision.make('1'),
        };
        const session = yield* AgentLog.open(conversationId, { compatibility });
        yield* session.append([
          {
            _tag: 'RunStarted',
            agent: 'test',
            formatVersion: 1,
            agentRevision: LogVocabulary.AgentRevision.make('1'),
            prompt,
          },
        ]);

        const result = yield* Effect.exit(
          AgentLog.open(conversationId, { compatibility }),
        );
        expect(Exit.isFailure(result)).toBe(true);
        if (Exit.isFailure(result)) {
          const error = Cause.findErrorOption(result.cause);
          expect(Option.isSome(error)).toBe(true);
          if (Option.isSome(error)) {
            expect(error.value._tag).toBe('CompatibilityError');
          }
        }
      }).pipe(Effect.provide(testLogLayer)),
  );

  it('continues to rebuild legacy unwrapped prompts', () => {
    const legacy = Prompt.make('legacy prompt').content;

    expect(AgentHistory.messagesFrom(runStarted(legacy)).content).toEqual(
      legacy,
    );
  });
});
