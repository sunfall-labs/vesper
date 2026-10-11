import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import { LanguageModel, type Response } from 'effect/ai';

/** Scripted model plus the real in-memory log, as one layer. */
export const scriptedLayer = (scripted: Model) =>
  Layer.merge(scripted.layer, testLogLayer);

/** Real in-memory log plus the platform services signals need. */
export const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

export const required = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('expected a value');
  }
  return value;
};

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

export const callTool = (
  id: string,
  name: string,
  params: unknown = {},
): Response.StreamPartEncoded => ({ type: 'tool-call', id, name, params });

const encodePrompt = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export interface Model {
  readonly layer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly prompts: string[];
}

/**
 * A scripted model that remembers each serialized prompt it was asked.
 * `turn(index, prompt)` picks the stream for the index-th call; the default
 * always answers `turn N`, so every turn wants to stop.
 */
export const model = (
  turn: (index: number, prompt: string) => Response.StreamPartEncoded[] = (
    index,
  ) => says(`turn ${String(index + 1)}`),
): Model => {
  const prompts: string[] = [];
  return {
    prompts,
    layer: Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () =>
            Effect.succeed<Response.PartEncoded[]>([finish()]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
                const prompt = encodePrompt(options.prompt);
                prompts.push(prompt);
                return Stream.fromIterable(turn(index, prompt));
              }),
            ),
        });
      }),
    ),
  };
};

export const tagsOf = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): string[] => records.map(({ record }) => record._tag);

export const received = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap(({ record }) =>
    record._tag === 'SignalReceived' ? [record] : [],
  );

export const settlement = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
) =>
  records.flatMap(({ record }) =>
    record._tag === 'RunSettled' ? [record] : [],
  );
