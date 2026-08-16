import { Compaction } from '@sunfall/vesper-agent/compaction';
import { ContextWindow } from '@sunfall/vesper-agent/context-window';
import { PiCompaction } from '@sunfall/vesper-pi/compaction';
import { PiErrors } from '@sunfall/vesper-pi/errors';
import type { AssistantMessage } from '@earendil-works/pi-ai';
import { Effect, type Layer } from 'effect';
import { Prompt } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { AiRuntime } from '../src/runtime.js';

// `@sunfall/vesper-pi` classifies a provider failure as context overflow; the agent
// loop reads that classification to decide whether to compact and retry. They
// agree on one string, and they cannot share its definition: `agent` must not
// import `pi`, which is the rule that keeps the loop provider-agnostic.
//
// `runtime` depends on both, so this is the one place the agreement can be
// checked. Without it, renaming the marker on either side would silently
// disable compaction — a long conversation would simply start failing, which
// is exactly how the last overflow bug presented.

const overflow = (): AssistantMessage => ({
  role: 'assistant',
  content: [],
  api: 'anthropic-messages',
  provider: 'anthropic',
  model: 'claude-sonnet-4-6',
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
  stopReason: 'error',
  errorMessage: 'prompt is too long: 213462 tokens > 200000 maximum',
  timestamp: 0,
});

describe('context-overflow protocol', () => {
  it('lets the agent loop recognise what the Pi adapter classified', () => {
    const classified = PiErrors.fromPiError(overflow(), {
      module: 'PiModel',
      method: 'streamText',
    });

    // The producer says overflow...
    expect(PiErrors.isContextOverflow(classified)).toBe(true);
    // ...and the consumer, which cannot import the producer, agrees.
    expect(Compaction.isContextOverflow(classified)).toBe(true);
  });

  it('pins the marker string both sides hardcode', () => {
    expect(Compaction.CONTEXT_OVERFLOW).toBe(PiErrors.CONTEXT_OVERFLOW);
  });
});

// The context-window seam has the same shape and the same failure mode.
// `@sunfall/vesper-agent` states what it wants of an estimator, `@sunfall/vesper-pi`
// produces a value of that shape without importing the statement, and this
// package is the only one that sees both. The assignment in `runtime.ts` is
// what type-checks the agreement; these check that it is actually installed —
// a `Context.Reference` cannot report that it was never overridden, so an
// unwired seam is a run that quietly keeps using the character count.

describe('context-window seam', () => {
  const conversation = Prompt.make([
    { role: 'system' as const, content: 's'.repeat(200) },
    { role: 'user' as const, content: [{ type: 'text' as const, text: 'u' }] },
    {
      role: 'assistant' as const,
      content: [{ type: 'text' as const, text: 'a' }],
    },
  ]);

  const installed = (layer: Layer.Layer<never>) =>
    Effect.runSync(Effect.provide(ContextWindow.Service, layer));

  it('installs Pi heuristics over the pure fallback', () => {
    expect(installed(AiRuntime.contextWindow)).toBe(PiCompaction.heuristics);
    // And without the layer it is the fallback, which is what makes
    // `@sunfall/vesper-agent` runnable on its own.
    expect(Effect.runSync(ContextWindow.Service)).toBe(ContextWindow.pure);
  });

  // The behavioural difference, not just the identity. The fallback ignores
  // reported usage entirely; Pi's takes it as exact.
  it('is the difference between reading provider usage and guessing', () => {
    const usage = { inputTokens: 90_000, outputTokens: 500 };

    expect(ContextWindow.pure.estimate(conversation, usage).tokens).toBe(51);
    expect(
      installed(AiRuntime.contextWindow).estimate(conversation, usage).tokens,
    ).toBe(90_500);
  });
});
