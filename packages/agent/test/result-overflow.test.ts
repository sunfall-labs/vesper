import { AttachmentStore } from '@sunfall/vesper-attachments/attachment-store';
import { AttachmentStoreMemory } from '@sunfall/vesper-attachments/layer-memory';
import { AttachmentRef } from '@sunfall/vesper-attachments/ref';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Schema } from 'effect';
import { type Prompt, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { ResultOverflow } from '../src/result-overflow.js';
import { ScriptedModel } from '../src/testing.js';
import {
  callTurn,
  modelBy,
  outcomesOf,
  provide,
  readRecords,
  textTurn,
  toolResultsIn,
} from './tool-fixtures.js';

// Result overflow: an oversized tool result is replaced, for the model and in
// the durable record, by a small pointer; read_attachment pages the rest back.

const attachments = AttachmentStoreMemory.layer.pipe(
  Layer.provide(NodeCrypto.layer),
);

const THRESHOLD = 40;
const PREVIEW = 10;
const BIG = 5_000;

const echo = Tool.make('echo', {
  description: 'return a string of the requested size',
  parameters: Schema.Struct({ size: Schema.Natural }),
  success: Schema.String,
});
const failsBig = Tool.make('fails_big', {
  description: 'always fails with a long message',
  parameters: Schema.Struct({}),
  success: Schema.String,
  failure: Schema.String,
  failureMode: 'return',
});

const agentWith = (resultOverflow: ResultOverflow.Policy | undefined) =>
  Agent.make({
    name: 'overflow-test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(echo, failsBig),
    ...(resultOverflow === undefined ? {} : { resultOverflow }),
  }).withHandlers({
    // Distinguishable characters, so a page read back can be located in the whole.
    echo: ({ size }) =>
      Effect.succeed('0123456789'.repeat(Math.ceil(size / 10)).slice(0, size)),
    fails_big: () => Effect.fail('e'.repeat(THRESHOLD + 60)),
  });

const readPolicy = { threshold: 300, preview: PREVIEW };
const policy = { threshold: THRESHOLD, preview: PREVIEW };
const results = (model: ScriptedModel.Handle, request: number) =>
  Effect.map(model.requests, (requests) =>
    toolResultsIn(requests[request]?.prompt),
  );

describe('an agent without resultOverflow', () => {
  it.effect(
    'offers no read_attachment and shows every result unchanged',
    () => {
      const model = ScriptedModel.make([
        callTurn({ id: 'big', name: 'echo', params: { size: BIG } }),
        textTurn('done'),
      ]);
      return Effect.gen(function* () {
        yield* agentWith(undefined).run('go').pipe(Effect.provide(model.layer));
        const shown = (yield* results(model, 1)).at(0);

        expect(new Set((yield* model.requests)[0]?.tools)).toEqual(
          new Set(['echo', 'fails_big']),
        );
        expect(shown?.result).toHaveLength(BIG);
      }).pipe(Effect.provide(attachments));
    },
  );
});

describe('an agent with resultOverflow', () => {
  it.effect(
    'passes small results through, spills big successes and failures, and records only pointers',
    () => {
      const model = ScriptedModel.make([
        callTurn(
          { id: 'small', name: 'echo', params: { size: THRESHOLD } },
          { id: 'big', name: 'echo', params: { size: BIG } },
          { id: 'fail', name: 'fails_big', params: {} },
        ),
        textTurn('done'),
      ]);
      return provide(
        Effect.gen(function* () {
          const result = yield* Conversation.make(
            agentWith(policy),
            'spill',
          ).run('go');
          const requests = yield* model.requests;
          const shown = Object.fromEntries(
            (yield* results(model, 1)).map((part) => [part.id, part.result]),
          );
          const pointer = shown['big'];
          const stored = outcomesOf(yield* readRecords('spill'));

          expect(result.text).toBe('done');
          expect(new Set(requests[0]?.tools)).toEqual(
            new Set(['echo', 'fails_big', 'read_attachment']),
          );
          // At the threshold: untouched.
          expect(shown['small']).toBe('0123456789'.repeat(4));
          if (!ResultOverflow.isPointer(pointer)) {
            throw new Error('expected an overflow pointer');
          }
          expect(pointer).toMatchObject({
            _tag: 'ToolResultOverflow',
            byteLength: BIG,
            mediaType: 'text/plain; charset=utf-8',
            preview: '0123456789',
          });
          expect(ResultOverflow.isPointer(shown['fail'])).toBe(true);

          // The bytes the pointer names are exactly what overflowed.
          const bytes = yield* (yield* AttachmentStore.Service).get({
            digest: pointer.attachmentId,
            mediaType: pointer.mediaType,
            byteLength: pointer.byteLength,
          });
          expect(new TextDecoder().decode(bytes)).toBe(
            '0123456789'.repeat(BIG / 10),
          );
          // The durable record holds the pointer, never the payload.
          expect(JSON.stringify(stored)).not.toContain('0123456789'.repeat(10));
          expect(
            stored.find((outcome) => outcome.id === 'big')?.result,
          ).toMatchObject({
            _tag: 'ToolResultOverflow',
            byteLength: BIG,
          });
        }).pipe(Effect.provide(attachments)),
        model,
      );
    },
  );

  it.effect(
    'pages an overflowed result back through read_attachment; an over-cap read is capped and itself spilled',
    () => {
      let lastPrompt: Prompt.Prompt | undefined;
      const Page = Schema.Struct({
        content: Schema.String,
        offset: Schema.Finite,
        length: Schema.Finite,
        hasMore: Schema.Boolean,
      });
      // The model copies the pointer it was shown. It first asks for far more
      // than the cap, then reads 100 bytes at a time until hasMore is false.
      const model = modelBy((options, index) => {
        lastPrompt = options.prompt;
        if (index === 0) {
          return callTurn({ id: 'big', name: 'echo', params: { size: 1_000 } });
        }
        const shown = toolResultsIn(options.prompt);
        const pointer = shown.find((part) => part.name === 'echo')?.result;
        const reads = shown.filter((part) => part.name === 'read_attachment');
        const page =
          reads.length > 1
            ? Schema.decodeUnknownSync(Page)(reads.at(-1)?.result)
            : undefined;
        if (!ResultOverflow.isPointer(pointer) || page?.hasMore === false) {
          return textTurn('done');
        }
        return callTurn({
          id: `read-${String(index)}`,
          name: 'read_attachment',
          params: {
            attachmentId: pointer.attachmentId,
            mediaType: pointer.mediaType,
            byteLength: pointer.byteLength,
            ...(reads.length === 0
              ? { length: 5_000 }
              : {
                  offset: page === undefined ? 0 : page.offset + page.length,
                  length: 100,
                }),
          },
        });
      });
      return provide(
        Effect.gen(function* () {
          const result = yield* agentWith(readPolicy).run('go');
          const store = yield* AttachmentStore.Service;
          const text = (
            digest: string,
            mediaType: string,
            byteLength: number,
          ) =>
            store
              .get({
                digest: AttachmentRef.Digest.make(digest),
                mediaType,
                byteLength,
              })
              .pipe(Effect.map((bytes) => new TextDecoder().decode(bytes)));
          const shown = toolResultsIn(lastPrompt);
          const reads = shown.filter((part) => part.name === 'read_attachment');
          const pointer = shown.find((part) => part.name === 'echo')?.result;
          const overCap = reads[0]?.result;
          if (
            !ResultOverflow.isPointer(pointer) ||
            !ResultOverflow.isPointer(overCap)
          ) {
            throw new Error('expected overflow pointers');
          }
          const whole = yield* text(
            pointer.attachmentId,
            pointer.mediaType,
            pointer.byteLength,
          );
          const capped = yield* Schema.decodeEffect(
            Schema.fromJsonString(Schema.Struct({ length: Schema.Finite })),
          )(
            yield* text(
              overCap.attachmentId,
              overCap.mediaType,
              overCap.byteLength,
            ),
          );
          const pages = reads
            .slice(1)
            .map((part) => Schema.decodeUnknownSync(Page)(part.result));

          expect(result.text).toBe('done');
          expect(capped.length).toBe(readPolicy.threshold);
          expect(pages.length).toBeGreaterThan(2);
          expect(pages.map((page) => page.hasMore)).toEqual(
            pages.map((_, i) => i < pages.length - 1),
          );
          expect(pages.map((page) => page.content).join('')).toBe(whole);
        }).pipe(Effect.provide(attachments)),
        model,
      );
    },
  );

  it.effect(
    'answers a read of an unknown attachment with AttachmentNotFound',
    () => {
      const model = modelBy((options, index) =>
        index === 0
          ? callTurn({
              id: 'read-missing',
              name: 'read_attachment',
              params: {
                attachmentId: `sha256:${'0'.repeat(64)}`,
                mediaType: 'text/plain',
                byteLength: 3,
              },
            })
          : textTurn(JSON.stringify(toolResultsIn(options.prompt))),
      );
      return provide(
        Effect.gen(function* () {
          const result = yield* agentWith(readPolicy).run('go');
          expect(result.text).toContain('AttachmentNotFound');
          expect(result.text).toContain('"isFailure":true');
        }).pipe(Effect.provide(attachments)),
        model,
      );
    },
  );
});
