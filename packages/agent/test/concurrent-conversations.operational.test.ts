import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';

const finish: Response.FinishPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

const model = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([finish]),
    streamText: () =>
      Stream.fromIterable<Response.StreamPartEncoded>([
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: 'parallel answer' },
        { type: 'text-end', id: 'answer' },
        finish,
      ]),
  }),
);

const agent = Agent.make({
  name: 'concurrent-conversation-probe',
  revision: '1',
  instructions: 'Answer briefly.',
  toolkit: Toolkit.make(),
});

const live = Layer.mergeAll(
  model,
  LogStoreMemory.layer.pipe(Layer.provide(NodeCrypto.layer)),
  NodeServices.layer,
);

describe('concurrent recorded conversations', () => {
  it.effect('keeps independent histories isolated under concurrent runs', () =>
    Effect.gen(function* () {
      const left = Conversation.make(agent, 'parallel-left');
      const right = Conversation.make(agent, 'parallel-right');

      const results = yield* Effect.all(
        [left.run('left input'), right.run('right input')],
        { concurrency: 'unbounded' },
      );
      const leftRecords = yield* left.records().pipe(Stream.runCollect);
      const rightRecords = yield* right.records().pipe(Stream.runCollect);

      expect(results.map(({ text }) => text)).toEqual([
        'parallel answer',
        'parallel answer',
      ]);
      expect(JSON.stringify(Array.from(leftRecords))).toContain('left input');
      expect(JSON.stringify(Array.from(leftRecords))).not.toContain(
        'right input',
      );
      expect(JSON.stringify(Array.from(rightRecords))).toContain('right input');
      expect(JSON.stringify(Array.from(rightRecords))).not.toContain(
        'left input',
      );
      expect(
        Array.from(leftRecords).filter(
          ({ record }) => record._tag === 'Completed',
        ),
      ).toHaveLength(1);
      expect(
        Array.from(rightRecords).filter(
          ({ record }) => record._tag === 'Completed',
        ),
      ).toHaveLength(1);
    }).pipe(Effect.provide(live), Effect.scoped),
  );
});
