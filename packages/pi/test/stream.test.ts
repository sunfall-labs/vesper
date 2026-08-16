import type {
  AssistantMessage,
  AssistantMessageEvent,
} from '@earendil-works/pi-ai';
import { describe, expect, it } from 'vitest';

import { isTerminalError, toStreamParts, toUsage } from '../src/stream.js';

const emptyUsage: AssistantMessage['usage'] = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

const partial = (
  overrides: Partial<AssistantMessage> = {},
): AssistantMessage => ({
  role: 'assistant',
  content: [],
  api: 'anthropic-messages',
  provider: 'anthropic',
  model: 'claude-sonnet-4-6',
  usage: emptyUsage,
  stopReason: 'stop',
  timestamp: 0,
  ...overrides,
});

describe('toStreamParts', () => {
  it('maps text lifecycle to text parts sharing one id', () => {
    const events: AssistantMessageEvent[] = [
      { type: 'text_start', contentIndex: 0, partial: partial() },
      { type: 'text_delta', contentIndex: 0, delta: 'he', partial: partial() },
      { type: 'text_delta', contentIndex: 0, delta: 'llo', partial: partial() },
      {
        type: 'text_end',
        contentIndex: 0,
        content: 'hello',
        partial: partial(),
      },
    ];

    const parts = events.flatMap((event) => [...toStreamParts(event)]);

    expect(parts).toEqual([
      { type: 'text-start', id: 'pi-0' },
      { type: 'text-delta', id: 'pi-0', delta: 'he' },
      { type: 'text-delta', id: 'pi-0', delta: 'llo' },
      { type: 'text-end', id: 'pi-0' },
    ]);
  });

  it('keeps concurrent content blocks on distinct ids', () => {
    const first = toStreamParts({
      type: 'text_delta',
      contentIndex: 0,
      delta: 'a',
      partial: partial(),
    });
    const second = toStreamParts({
      type: 'text_delta',
      contentIndex: 1,
      delta: 'b',
      partial: partial(),
    });

    expect(first[0]).toMatchObject({ id: 'pi-0' });
    expect(second[0]).toMatchObject({ id: 'pi-1' });
  });

  it('maps thinking events onto reasoning parts', () => {
    const parts = toStreamParts({
      type: 'thinking_delta',
      contentIndex: 2,
      delta: 'hmm',
      partial: partial(),
    });

    expect(parts).toEqual([
      { type: 'reasoning-delta', id: 'pi-2', delta: 'hmm' },
    ]);
  });

  // Pi emits one complete tool call; consumers keying off `tool-params-start`
  // would never fire if that were not re-synthesized here.
  it('fans one toolcall_end into the full params + call grammar', () => {
    const parts = toStreamParts({
      type: 'toolcall_end',
      contentIndex: 0,
      toolCall: {
        type: 'toolCall',
        id: 'call_1',
        name: 'lookup_order',
        arguments: { orderId: 'order_1042' },
      },
      partial: partial(),
    });

    expect(parts.map((part) => part.type)).toEqual([
      'tool-params-start',
      'tool-params-delta',
      'tool-params-end',
      'tool-call',
    ]);
    expect(parts[3]).toEqual({
      type: 'tool-call',
      id: 'call_1',
      name: 'lookup_order',
      params: { orderId: 'order_1042' },
    });
  });

  it('withholds partial tool params, which carry no tool name yet', () => {
    expect(
      toStreamParts({
        type: 'toolcall_start',
        contentIndex: 0,
        partial: partial(),
      }),
    ).toEqual([]);
    expect(
      toStreamParts({
        type: 'toolcall_delta',
        contentIndex: 0,
        delta: '{"or',
        partial: partial(),
      }),
    ).toEqual([]);
  });

  it('translates toolUse into the tool-calls finish reason', () => {
    const parts = toStreamParts({
      type: 'done',
      reason: 'toolUse',
      message: partial({ stopReason: 'toolUse' }),
    });

    expect(parts[0]).toMatchObject({ type: 'finish', reason: 'tool-calls' });
  });

  // The error event must become a stream failure, never an in-band part —
  // that is the whole point of the port.
  it('emits no part for a terminal error', () => {
    const event: AssistantMessageEvent = {
      type: 'error',
      reason: 'error',
      error: partial({ stopReason: 'error', errorMessage: 'boom' }),
    };

    expect(toStreamParts(event)).toEqual([]);
    expect(isTerminalError(event)).toBe(true);
  });

  it('emits response metadata only when Pi supplies a response id', () => {
    expect(toStreamParts({ type: 'start', partial: partial() })).toEqual([]);
    expect(
      toStreamParts({
        type: 'start',
        partial: partial({ responseId: 'resp_9' }),
      }),
    ).toEqual([{ type: 'response-metadata', id: 'resp_9' }]);
  });
});

describe('toUsage', () => {
  it('totals the whole prompt, and reports Pi’s input as the uncached part', () => {
    const usage = toUsage({
      ...emptyUsage,
      input: 250,
      output: 200,
      cacheRead: 750,
      cacheWrite: 50,
    });

    expect(usage).toEqual({
      inputTokens: {
        total: 1050,
        uncached: 250,
        cacheRead: 750,
        cacheWrite: 50,
      },
      outputTokens: { total: 200 },
    });
  });

  // The shape a real Anthropic call reports on a cached conversation, and the
  // one the previous mapping collapsed to three tokens. Pi's adapter caches
  // every request, so this is the ordinary case rather than an edge one.
  it('counts a cache write as prompt tokens rather than as nothing', () => {
    const usage = toUsage({ ...emptyUsage, input: 3, cacheWrite: 6711 });

    expect(usage.inputTokens.total).toBe(6714);
    expect(usage.inputTokens.uncached).toBe(3);
  });

  it('counts a cache read as prompt tokens', () => {
    const usage = toUsage({ ...emptyUsage, input: 10, cacheRead: 40 });

    expect(usage.inputTokens.total).toBe(50);
    expect(usage.inputTokens.uncached).toBe(10);
  });
});
