import { ContextWindow } from '@sunfall/vesper-agent/context-window';
import { Prompt } from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

const conversation = Prompt.make([
  { role: 'system', content: 's'.repeat(200) },
  { role: 'user', content: [{ type: 'text', text: 'u' }] },
  { role: 'assistant', content: [{ type: 'text', text: 'a' }] },
  { role: 'user', content: [{ type: 'text', text: 'tail' }] },
]);

const _usageAnchoredType: ContextWindow.Heuristics =
  ContextWindow.usageAnchored;

describe('ContextWindow.usageAnchored', () => {
  it('uses reported usage as exact and estimates only the trailing messages', () => {
    expect(
      ContextWindow.usageAnchored.estimate(conversation, {
        inputTokens: 90_000,
        outputTokens: 500,
      }),
    ).toEqual({
      tokens: 90_501,
      usageTokens: 90_500,
      trailingTokens: 1,
    });
  });

  it('degrades to the pure estimate without usage', () => {
    expect(ContextWindow.usageAnchored.estimate(conversation)).toEqual(
      ContextWindow.pure.estimate(conversation),
    );
  });

  it('degrades to the pure estimate when there is no assistant anchor', () => {
    const prompt = Prompt.make([{ role: 'user', content: 'unanswered' }]);

    expect(
      ContextWindow.usageAnchored.estimate(prompt, {
        inputTokens: 100,
        outputTokens: 10,
      }),
    ).toEqual(ContextWindow.pure.estimate(prompt));
  });

  it('uses the same reserve threshold as the default', () => {
    expect(
      ContextWindow.usageAnchored.shouldCompact(91, 100, {
        reserveTokens: 10,
      }),
    ).toBe(true);
    expect(
      ContextWindow.usageAnchored.shouldCompact(90, 100, {
        reserveTokens: 10,
      }),
    ).toBe(false);
  });
});
