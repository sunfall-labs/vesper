import { AttachmentStore } from '@sunfall/vesper-attachments/attachment-store';
import { AttachmentStoreMemory } from '@sunfall/vesper-attachments/layer-memory';
import { AttachmentRef } from '@sunfall/vesper-attachments/ref';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Schema, Stream } from 'effect';
import { type Prompt, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { ResultOverflow } from '../src/result-overflow.js';
import { ScriptedModel } from '../src/testing.js';

// The overflow policy.
//
// What these have to prove:
//
//   - unset, an agent compiles and dispatches exactly as before: no extra
//     tool, no extra requirement, every result reaches the model unchanged;
//   - a result at or under the threshold is untouched;
//   - a result over it is replaced everywhere that matters — what the model
//     is shown and what a durable conversation records — by a small pointer,
//     and the bytes it points at are exactly what overflowed;
//   - `read_attachment` reads the stored bytes back, in ranges;
//   - the two ways an agent runs (plain and recorded) both work.

const attachments = AttachmentStoreMemory.layer.pipe(
  Layer.provide(NodeCrypto.layer),
);

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeCrypto.layer)),
  NodeServices.layer,
);

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const answerTurn = [
  { type: 'text-start' as const, id: 'a' },
  { type: 'text-delta' as const, id: 'a', delta: 'done' },
  { type: 'text-end' as const, id: 'a' },
  finish(),
];

const echo = Tool.make('echo', {
  description: 'return a string of the requested size',
  parameters: Schema.Struct({ size: Schema.Natural }),
  success: Schema.String,
});

/** One tool-call turn asking `echo` for `size` bytes of `'x'`. */
const callTurn = (size: number, id = 'call-1') => [
  { type: 'tool-call' as const, id, name: 'echo', params: { size } },
  finish('tool-calls'),
];

/** Every `tool-result` part in a prompt, across every message. */
interface ToolResultPart {
  readonly type: 'tool-result';
  readonly id: string;
  readonly result: unknown;
}
const toolResultsOf = (prompt: Prompt.Prompt): ReadonlyArray<ToolResultPart> =>
  prompt.content.flatMap((message) =>
    message.role === 'tool'
      ? (message.content.filter(
          (part) => part.type === 'tool-result',
        ) as ReadonlyArray<ToolResultPart>)
      : [],
  );

const THRESHOLD = 40;
const PREVIEW = 10;

const agentWith = (
  calls: { count: number },
  resultOverflow: ResultOverflow.Policy | undefined,
) =>
  Agent.make({
    name: 'overflow-test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(echo),
    ...(resultOverflow === undefined ? {} : { resultOverflow }),
  }).withHandlers({
    echo: ({ size }) =>
      Effect.sync(() => {
        calls.count += 1;
        return 'x'.repeat(size);
      }),
  });

describe('toolkit composition', () => {
  it('omits read_attachment when resultOverflow is unset', () => {
    const agent = agentWith({ count: 0 }, undefined);

    expect(Object.keys(agent.toolkit.tools)).toEqual(['echo']);
  });

  it('adds read_attachment only when resultOverflow is set', () => {
    const agent = agentWith({ count: 0 }, { threshold: THRESHOLD });

    expect(Object.keys(agent.toolkit.tools).sort()).toEqual([
      'echo',
      'read_attachment',
    ]);
  });
});

describe('spilling', () => {
  it.effect('passes a result at or under the threshold through unchanged', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls, {
        threshold: THRESHOLD,
        preview: PREVIEW,
      });
      const small = 'x'.repeat(THRESHOLD);
      const model = ScriptedModel.make([callTurn(THRESHOLD), answerTurn]);

      const result = yield* agent
        .run('go')
        .pipe(Effect.provide(model.layer), Effect.provide(attachments));
      const requests = yield* model.requests;
      const results = toolResultsOf(requests[1]!.prompt);

      expect(result.text).toBe('done');
      expect(calls.count).toBe(1);
      expect(results).toHaveLength(1);
      expect(results[0]!.result).toBe(small);
    }),
  );

  it.effect(
    'spills a result over the threshold into a pointer with matching stored bytes',
    () =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const agent = agentWith(calls, {
          threshold: THRESHOLD,
          preview: PREVIEW,
        });
        const size = 5_000;
        const big = 'x'.repeat(size);
        const model = ScriptedModel.make([callTurn(size), answerTurn]);

        const result = yield* agent
          .run('go')
          .pipe(Effect.provide(model.layer), Effect.provide(attachments));
        const requests = yield* model.requests;
        const results = toolResultsOf(requests[1]!.prompt);

        expect(result.text).toBe('done');
        expect(calls.count).toBe(1);
        expect(results).toHaveLength(1);
        const pointer = results[0]!.result as ResultOverflow.Pointer;

        expect(pointer._tag).toBe('ToolResultOverflow');
        expect(pointer.byteLength).toBe(size);
        expect(pointer.mediaType).toBe('text/plain; charset=utf-8');
        expect(pointer.preview).toBe(big.slice(0, PREVIEW));
        // The pointer is small; the payload it replaced is not.
        expect(JSON.stringify(pointer).length).toBeLessThan(size);

        const store = yield* AttachmentStore.Service;
        const stored = yield* store.get({
          digest: pointer.attachmentId,
          mediaType: pointer.mediaType,
          byteLength: pointer.byteLength,
        });
        expect(new TextDecoder().decode(stored)).toBe(big);
      }).pipe(Effect.provide(attachments)),
  );

  it.effect('spills a failure result the same way as a success', () =>
    Effect.gen(function* () {
      const failing = Tool.make('fails_big', {
        description: 'always fails with a long message',
        parameters: Schema.Struct({}),
        success: Schema.String,
        failure: Schema.String,
        failureMode: 'return',
      });
      const agent = Agent.make({
        name: 'overflow-failure-test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(failing),
        resultOverflow: { threshold: THRESHOLD, preview: PREVIEW },
      }).withHandlers({
        fails_big: () => Effect.fail('e'.repeat(THRESHOLD + 60)),
      });
      const model = ScriptedModel.make([
        [
          {
            type: 'tool-call' as const,
            id: 'call-1',
            name: 'fails_big',
            params: {},
          },
          finish('tool-calls'),
        ],
        answerTurn,
      ]);

      yield* agent
        .run('go')
        .pipe(Effect.provide(model.layer), Effect.provide(attachments));
      const requests = yield* model.requests;
      const results = toolResultsOf(requests[1]!.prompt);

      expect((results[0]!.result as ResultOverflow.Pointer)._tag).toBe(
        'ToolResultOverflow',
      );
    }).pipe(Effect.provide(attachments)),
  );
});

describe('read_attachment', () => {
  it.effect('reads the full stored content when no range is given', () =>
    Effect.gen(function* () {
      const store = yield* AttachmentStore.Service;
      const content = 'hello world, this is attachment content';
      const ref = yield* store.put(new TextEncoder().encode(content), {
        mediaType: 'text/plain',
      });
      const { handler } = ResultOverflow.reader({ threshold: 1_000 });

      const result = yield* handler({
        attachmentId: ref.digest,
        mediaType: ref.mediaType,
        byteLength: ref.byteLength,
      });

      expect(result).toEqual({
        content,
        offset: 0,
        length: ref.byteLength,
        totalBytes: ref.byteLength,
        hasMore: false,
      });
    }).pipe(Effect.provide(attachments)),
  );

  it.effect('pages through content by offset and length', () =>
    Effect.gen(function* () {
      const store = yield* AttachmentStore.Service;
      const content = '0123456789abcdefghij';
      const ref = yield* store.put(new TextEncoder().encode(content), {
        mediaType: 'text/plain',
      });
      const { handler } = ResultOverflow.reader({ threshold: 8 });
      const base = {
        attachmentId: ref.digest,
        mediaType: ref.mediaType,
        byteLength: ref.byteLength,
      };

      const first = yield* handler(base);
      expect(first.content).toBe(content.slice(0, 8));
      expect(first.hasMore).toBe(true);

      const second = yield* handler({
        ...base,
        offset: first.offset + first.length,
      });
      expect(second.content).toBe(content.slice(8, 16));
      expect(second.hasMore).toBe(true);

      const third = yield* handler({
        ...base,
        offset: second.offset + second.length,
      });
      expect(third.content).toBe(content.slice(16));
      expect(third.hasMore).toBe(false);
    }).pipe(Effect.provide(attachments)),
  );

  it.effect('caps a requested length at the policy threshold', () =>
    Effect.gen(function* () {
      const store = yield* AttachmentStore.Service;
      const ref = yield* store.put(new TextEncoder().encode('0'.repeat(50)), {
        mediaType: 'text/plain',
      });
      const { handler } = ResultOverflow.reader({ threshold: 5 });

      const result = yield* handler({
        attachmentId: ref.digest,
        mediaType: ref.mediaType,
        byteLength: ref.byteLength,
        length: 1_000,
      });

      expect(result.length).toBe(5);
    }).pipe(Effect.provide(attachments)),
  );

  it.effect('fails with AttachmentNotFound for an unknown id', () =>
    Effect.gen(function* () {
      const { handler } = ResultOverflow.reader({ threshold: 100 });

      const outcome = yield* handler({
        attachmentId: AttachmentRef.Digest.make(`sha256:${'0'.repeat(64)}`),
        mediaType: 'text/plain',
        byteLength: 3,
      }).pipe(Effect.result);

      expect(outcome._tag).toBe('Failure');
      if (outcome._tag === 'Failure') {
        expect(outcome.failure).toMatchObject({
          _tag: 'AttachmentNotFound',
        });
      }
    }).pipe(Effect.provide(attachments)),
  );
});

describe('durable conversations', () => {
  it.effect('records the pointer, not the spilled payload', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls, {
        threshold: THRESHOLD,
        preview: PREVIEW,
      });
      const size = THRESHOLD + 200;
      const model = ScriptedModel.make([callTurn(size), answerTurn]);
      const conversation = Conversation.make(agent, 'overflow-conversation');

      yield* conversation.run('go').pipe(Effect.provide(model.layer));
      const records = yield* conversation.records().pipe(Stream.runCollect);
      const outcome = Array.from(records)
        .map((envelope) => envelope.record)
        .find((record) => record._tag === 'ToolOutcome');

      expect(outcome).toBeDefined();
      const stored = (outcome as { readonly result: unknown }).result;
      expect(JSON.stringify(stored)).not.toContain('x'.repeat(size));
      expect(stored).toMatchObject({
        _tag: 'ToolResultOverflow',
        byteLength: size,
      });
    }).pipe(
      Effect.provide(attachments),
      Effect.provide(testLogLayer),
      Effect.scoped,
    ),
  );
});
