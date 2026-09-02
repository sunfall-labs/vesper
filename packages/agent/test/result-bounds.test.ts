import { AttachmentStoreMemory } from '@sunfall/vesper-attachments/layer-memory';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Schema, Stream } from 'effect';
import { type Prompt, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { ResultBounds } from '../src/result-bounds.js';
import { ResultOverflow } from '../src/result-overflow.js';
import { ScriptedModel } from '../src/testing.js';

// The default per-result byte bound.
//
// What these have to prove:
//
//   - unset, the 64 KiB default applies: a result nowhere near it is
//     untouched, one over it is replaced by a small truncation envelope;
//   - an explicit `maxBytes` is honoured instead of the default;
//   - `resultOverflow`, when configured, always gets the first chance to
//     spill — bounds only ever truncates a result overflow declined to
//     spill, even when overflow's own pointer would itself exceed a very
//     small `maxBytes`;
//   - the preview carried on a truncation envelope is a fixed-length head
//     of the original content;
//   - a truncated result recorded to a durable conversation is exactly the
//     small envelope, not the oversized payload.

const attachments = AttachmentStoreMemory.layer.pipe(
  Layer.provide(NodeCrypto.layer),
);

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeCrypto.layer)),
  NodeServices.layer,
);

const required = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('expected a value');
  }
  return value;
};

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

const agentWith = (
  calls: { count: number },
  options: {
    readonly resultBounds?: ResultBounds.Policy | false;
    readonly resultOverflow?: ResultOverflow.Policy;
  } = {},
) =>
  Agent.make({
    name: 'bounds-test',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(echo),
    ...(options.resultBounds === undefined
      ? {}
      : { resultBounds: options.resultBounds }),
    ...(options.resultOverflow === undefined
      ? {}
      : { resultOverflow: options.resultOverflow }),
  }).withHandlers({
    echo: ({ size }) =>
      Effect.sync(() => {
        calls.count += 1;
        return 'x'.repeat(size);
      }),
  });

// `agentWith` is not generic per call site, so its return type always
// carries `AttachmentStore.Service` in `Requires` — the same shape
// `resultOverflow` gives every agent `result-overflow.test.ts` builds,
// whether or not a given call actually configures it. Always providing
// `attachments` here, regardless of whether this run's policy spills
// anything, is what that file's own tests do too.
const runResults = (agent: ReturnType<typeof agentWith>, size: number) =>
  Effect.gen(function* () {
    const model = ScriptedModel.make([callTurn(size), answerTurn]);
    const result = yield* agent
      .run('go')
      .pipe(Effect.provide(Layer.merge(model.layer, attachments)));
    const requests = yield* model.requests;
    const results = toolResultsOf(required(requests[1]).prompt);
    return { result, results };
  });

describe('the default bound', () => {
  it.effect('passes a result well under 64 KiB through unchanged', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls);
      const small = 'x'.repeat(1_000);

      const { result, results } = yield* runResults(agent, 1_000);

      expect(result.text).toBe('done');
      expect(calls.count).toBe(1);
      expect(results).toHaveLength(1);
      expect(required(results[0]).result).toBe(small);
    }),
  );

  it.effect(
    'truncates a result over 64 KiB when resultBounds is left unset',
    () =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const agent = agentWith(calls);
        const size = ResultBounds.DEFAULT_MAX_BYTES + 1_000;

        const { results } = yield* runResults(agent, size);

        expect(calls.count).toBe(1);
        const stored = required(results[0]).result;
        if (!ResultBounds.isTruncation(stored)) {
          throw new Error('expected a truncation envelope');
        }
        expect(stored.truncated).toBe(true);
        expect(stored.bytes).toBe(size);
        expect(stored.maxBytes).toBe(ResultBounds.DEFAULT_MAX_BYTES);
      }),
  );

  it.effect('disables bounding when resultBounds is false', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls, { resultBounds: false });
      const size = ResultBounds.DEFAULT_MAX_BYTES + 1_000;
      const big = 'x'.repeat(size);

      const { results } = yield* runResults(agent, size);

      expect(calls.count).toBe(1);
      expect(required(results[0]).result).toBe(big);
    }),
  );
});

describe('an explicit maxBytes', () => {
  const MAX_BYTES = 40;

  it.effect('passes a result at or under maxBytes through unchanged', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls, { resultBounds: { maxBytes: MAX_BYTES } });
      const small = 'x'.repeat(MAX_BYTES);

      const { results } = yield* runResults(agent, MAX_BYTES);

      expect(calls.count).toBe(1);
      expect(required(results[0]).result).toBe(small);
    }),
  );

  it.effect('truncates a result over maxBytes into an envelope', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls, { resultBounds: { maxBytes: MAX_BYTES } });
      const size = 5_000;

      const { results } = yield* runResults(agent, size);

      expect(calls.count).toBe(1);
      const stored = required(results[0]).result;
      if (!ResultBounds.isTruncation(stored)) {
        throw new Error('expected a truncation envelope');
      }
      expect(stored.bytes).toBe(size);
      expect(stored.maxBytes).toBe(MAX_BYTES);
      // The envelope is small; the payload it replaced is not.
      expect(JSON.stringify(stored).length).toBeLessThan(size);
    }),
  );
});

describe('preview', () => {
  it.effect('carries a fixed-length head of the original content', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls, { resultBounds: { maxBytes: 40 } });
      const size = 5_000;
      const big = 'x'.repeat(size);

      const { results } = yield* runResults(agent, size);

      const stored = required(results[0]).result;
      if (!ResultBounds.isTruncation(stored)) {
        throw new Error('expected a truncation envelope');
      }
      expect(stored.preview).toBe(big.slice(0, ResultBounds.PREVIEW_CHARS));
      expect(stored.preview.length).toBe(ResultBounds.PREVIEW_CHARS);
    }),
  );
});

describe('precedence with resultOverflow', () => {
  it.effect(
    'lets a spilled pointer through even when it would itself exceed a tiny maxBytes',
    () =>
      Effect.gen(function* () {
        const calls = { count: 0 };
        const agent = agentWith(calls, {
          resultOverflow: { threshold: 40 },
          // Small enough that even the pointer overflow produces would fail
          // this bound, if bounds did not explicitly defer to overflow.
          resultBounds: { maxBytes: 1 },
        });
        const size = 5_000;

        const { results } = yield* runResults(agent, size);

        expect(calls.count).toBe(1);
        const stored = required(results[0]).result;
        expect(ResultOverflow.isPointer(stored)).toBe(true);
        expect(ResultBounds.isTruncation(stored)).toBe(false);
      }),
  );

  it.effect('still bounds a result resultOverflow declined to spill', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const size = 5_000;
      const agent = agentWith(calls, {
        // Large enough that overflow never spills this result.
        resultOverflow: { threshold: size + 1 },
        resultBounds: { maxBytes: 40 },
      });

      const { results } = yield* runResults(agent, size);

      expect(calls.count).toBe(1);
      const stored = required(results[0]).result;
      expect(ResultOverflow.isPointer(stored)).toBe(false);
      if (!ResultBounds.isTruncation(stored)) {
        throw new Error('expected a truncation envelope');
      }
      expect(stored.maxBytes).toBe(40);
    }),
  );
});

describe('durable conversations', () => {
  it.effect('records the truncation envelope, not the oversized payload', () =>
    Effect.gen(function* () {
      const calls = { count: 0 };
      const agent = agentWith(calls, { resultBounds: { maxBytes: 40 } });
      const size = 5_000;
      const model = ScriptedModel.make([callTurn(size), answerTurn]);
      const conversation = Conversation.make(agent, 'bounds-conversation');

      yield* conversation.run('go').pipe(Effect.provide(model.layer));
      const records = yield* conversation.records().pipe(Stream.runCollect);
      const outcome = Array.from(records)
        .map((envelope) => envelope.record)
        .find((record) => record._tag === 'ToolOutcome');

      if (outcome?._tag !== 'ToolOutcome') {
        throw new Error('missing tool outcome');
      }
      const stored = outcome.result;
      expect(JSON.stringify(stored)).not.toContain('x'.repeat(size));
      expect(stored).toMatchObject({
        truncated: true,
        bytes: size,
        maxBytes: 40,
      });
    }).pipe(
      Effect.provide(Layer.merge(testLogLayer, attachments)),
      Effect.scoped,
    ),
  );
});
