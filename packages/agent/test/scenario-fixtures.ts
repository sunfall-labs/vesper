import * as NodeServices from '@effect/platform-node/NodeServices';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { Layer } from 'effect';
import type { Response } from 'effect/ai';

/** A finish part with fixed small usage (3 in, 2 out unless overridden). */
export const finish = (
  reason: 'stop' | 'tool-calls' = 'stop',
  usage: { readonly input?: number; readonly output?: number } = {},
): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: {
      total: usage.input ?? 3,
      uncached: usage.input ?? 3,
      cacheRead: 0,
      cacheWrite: 0,
    },
    outputTokens: { total: usage.output ?? 2 },
  },
});

/** One scripted streaming turn that answers with text deltas. */
export const textTurn = (
  text: string | ReadonlyArray<string>,
  id = 'text',
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'text-start', id },
  ...(typeof text === 'string' ? [text] : text).map(
    (delta) => ({ type: 'text-delta', id, delta }) as const,
  ),
  { type: 'text-end', id },
  finish(),
];

/** One scripted streaming turn that calls a single tool. */
export const toolTurn = (
  name: string,
  params: Record<string, unknown>,
  id = `${name}-call`,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: 'tool-call', id, name, params },
  finish('tool-calls'),
];

/** Real in-memory log store plus the platform services agents need. */
export const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);
