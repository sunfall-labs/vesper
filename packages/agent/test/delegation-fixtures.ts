import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Prompt,
  type Response,
  type Tool,
} from 'effect/ai';

import { Conversation } from '../src/conversation.js';
import type { Agent } from '../src/agent.js';

export const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeCrypto.layer)),
  NodeServices.layer,
);

export const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 5, uncached: 5, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

export const says = (text: string): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id: text },
  { type: 'text-delta' as const, id: text, delta: text },
  { type: 'text-end' as const, id: text },
  finish(),
];

export const calls = (
  id: string,
  name: string,
  params: Record<string, unknown>,
): Response.StreamPartEncoded[] => [
  { type: 'tool-call' as const, id, name, params },
  finish('tool-calls'),
];

export const delegates = (id: string, child: string) =>
  calls(id, `task_${child}`, { prompt: `do the ${child} part` });

export interface Seen {
  readonly prompts: Prompt.Prompt[];
  readonly tools: ReadonlyArray<Tool.Any>[];
}

/**
 * One model shared by every agent in the tree, replying in script order, so
 * parent and children interleave exactly as in production.
 */
export const scripted = (
  turns: ReadonlyArray<Response.StreamPartEncoded[]>,
  seen: Seen = { prompts: [], tools: [] },
) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const counter = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              seen.prompts.push(options.prompt);
              seen.tools.push(options.tools);
              const index = yield* Ref.getAndUpdate(counter, (n) => n + 1);
              return Stream.fromIterable(
                turns[Math.min(index, turns.length - 1)] ?? [],
              );
            }),
          ),
      });
    }),
  );

export const recordsOf = (agent: Agent.Any, id: string) =>
  Conversation.make(agent, id).records().pipe(Stream.runCollect);

export const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map((envelope) => envelope.record._tag);

export const childSessions = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'ChildSession' ? [envelope.record] : [],
  );

/** Stable text for asserting on prompts and tool definitions. */
export const show = (value: unknown): string =>
  Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(value);

/** The first element, failing the test loudly when there is none. */
export const first = <A>(items: ReadonlyArray<A>): A => {
  const item = items.at(0);
  if (item === undefined) {
    throw new Error('expected at least one element');
  }
  return item;
};
