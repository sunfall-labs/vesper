import {
  estimateContextTokens,
  estimateTokens as estimatePiMessageTokens,
} from '@earendil-works/pi-agent-core';
import type { Message as PiMessage } from '@earendil-works/pi-ai';
import { Prompt } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { PiCompaction } from '../src/compaction.js';
import { PiPrompt } from '../src/prompt.js';

// What Pi's estimator is actually worth, in numbers.
//
// The claim under test is narrow and easy to overstate. Pi's per-message
// `estimateTokens` is the *same* four-characters-per-token heuristic
// `@sunfall/vesper-agent` already had — running it instead would change nothing. The
// improvement is `estimateContextTokens`: it takes the most recent
// provider-reported usage as exact and estimates only the messages that came
// after it. Everything below is written to make that distinction visible
// rather than to make Pi look good.

const message = (role: 'user' | 'assistant', text: string) => ({
  role,
  content: [{ type: 'text' as const, text }],
});

/**
 * The pure heuristic, reproduced here rather than imported.
 *
 * `@sunfall/vesper-pi` must not depend on `@sunfall/vesper-agent`, so the baseline this file
 * compares against cannot be `ContextWindow.pure`. Four characters per token
 * over every text part is the whole algorithm, and
 * `agent/src/context-window.test.ts` pins that the real one still is that.
 */
const charactersOver4 = (prompt: Prompt.Prompt): number => {
  let characters = 0;
  for (const entry of prompt.content) {
    if (typeof entry.content === 'string') {
      characters += entry.content.length;
      continue;
    }
    for (const part of entry.content) {
      if ('text' in part && typeof part.text === 'string') {
        characters += part.text.length;
      }
    }
  }
  return Math.ceil(characters / 4);
};

// ## The fixture
//
// A conversation whose visible text is short and whose real token cost is not
// — which is the ordinary case, not a contrived one. A long system prompt with
// a skill catalog, tool schemas the model is shown on every request, images,
// and provider framing all cost tokens that no character count of the message
// text can see. Here the provider has told us it billed 50,000 input tokens
// and produced 500 output tokens for the last turn, while the message text
// adds up to 2,200 characters.
const conversation = Prompt.make([
  { role: 'system' as const, content: 's'.repeat(200) },
  message('user', 'u'.repeat(800)),
  message('assistant', 'a'.repeat(800)),
  // Arrived after the turn the usage describes, so it is the only part any
  // estimator has to guess about.
  message('user', 'q'.repeat(400)),
]);

const REPORTED = { inputTokens: 50_000, outputTokens: 500 };

/**
 * What the provider would report for the next request.
 *
 * 50,500 for everything up to and including the assistant message it already
 * counted, plus the trailing user message. That message is 400 characters of a
 * repeated single character, which a real tokenizer prices at rather more than
 * the 100 tokens four-characters-per-token predicts; 120 is the figure used
 * here. The point of choosing a number the heuristic does *not* produce is
 * that Pi is then not being compared against its own output.
 */
const TRUTH = 50_500 + 120;

describe('estimate', () => {
  it('anchors on reported usage and lands near the truth', () => {
    const anchored = PiCompaction.heuristics.estimate(conversation, REPORTED);

    // 50,500 known, plus 400 characters of trailing text at four per token.
    expect(anchored).toEqual({
      tokens: 50_600,
      usageTokens: 50_500,
      trailingTokens: 100,
    });

    const guess = charactersOver4(conversation);
    expect(guess).toBe(550);

    // The comparison this whole change exists for.
    expect(Math.abs(anchored.tokens - TRUTH)).toBe(20);
    expect(Math.abs(guess - TRUTH)).toBe(50_070);
    expect(Math.abs(anchored.tokens - TRUTH)).toBeLessThan(
      Math.abs(guess - TRUTH),
    );
  });

  // The number is only interesting because a decision hangs off it. With a
  // 64,000-token window and 16,000 reserved, the truth is over budget — and
  // the character count says there is 47,000 tokens of room, so the run
  // proceeds and the provider rejects it.
  it('changes the compaction decision, not just the number', () => {
    const window = 64_000;
    const settings = { reserveTokens: 16_000 };

    expect(
      PiCompaction.heuristics.shouldCompact(
        PiCompaction.heuristics.estimate(conversation, REPORTED).tokens,
        window,
        settings,
      ),
    ).toBe(true);

    expect(
      PiCompaction.heuristics.shouldCompact(
        charactersOver4(conversation),
        window,
        settings,
      ),
    ).toBe(false);

    // And the truth agrees with the anchored answer, not the guess.
    expect(TRUTH > window - settings.reserveTokens).toBe(true);
  });

  // With nothing reported there is nothing to anchor to, and Pi's estimator is
  // the same heuristic under a different name. Stated explicitly because the
  // opposite is the easy thing to assume from "we adopted Pi's estimator".
  it('degrades to the same character count when no usage is available', () => {
    const unanchored = PiCompaction.heuristics.estimate(conversation);

    expect(unanchored).toEqual({
      tokens: 550,
      usageTokens: 0,
      trailingTokens: 550,
    });
    expect(unanchored.tokens).toBe(charactersOver4(conversation));
  });

  // Pi models a conversation as `{ systemPrompt, messages }` and its estimator
  // takes only the messages, so a naive adaptation silently drops the system
  // prompt — which for an agent is instructions plus a whole skill catalog.
  it('counts the system prompt that Pi own estimator never sees', () => {
    const context = PiPrompt.toPiContext(conversation, undefined);
    const piAlone = estimateContextTokens(context.messages).tokens;

    // 200 characters of system prompt, and Pi's own figure is short by
    // exactly that.
    expect(piAlone).toBe(500);
    expect(PiCompaction.heuristics.estimate(conversation).tokens).toBe(550);
  });

  // The other half of the same seam: once a provider has reported usage the
  // system prompt is already inside that figure, and adding it again inflates
  // every estimate on every long conversation — compacting early and paying
  // for summaries nobody needed.
  it('does not add the system prompt again once anchored', () => {
    const anchored = PiCompaction.heuristics.estimate(conversation, REPORTED);

    expect(anchored.trailingTokens).toBe(100);
    expect(anchored.tokens).toBe(
      anchored.usageTokens + anchored.trailingTokens,
    );
  });

  // A first turn: no assistant message exists to carry an anchor, so a
  // reported usage cannot be attached and the estimate is a character count.
  it('stays unanchored when there is no assistant message to anchor on', () => {
    const opening = Prompt.make([message('user', 'u'.repeat(400))]);

    expect(PiCompaction.heuristics.estimate(opening, REPORTED)).toEqual({
      tokens: 100,
      usageTokens: 0,
      trailingTokens: 100,
    });
  });
});

describe('shouldCompact', () => {
  const settings = { reserveTokens: 20 };

  it('fires strictly above the window minus the reserve', () => {
    expect(PiCompaction.heuristics.shouldCompact(80, 100, settings)).toBe(
      false,
    );
    expect(PiCompaction.heuristics.shouldCompact(81, 100, settings)).toBe(true);
  });
});

describe('the assignability this adapter rests on', () => {
  // `estimateContextTokens` takes `AgentMessage[]`, which is
  // `Message | CustomAgentMessages[keyof CustomAgentMessages]` — a union that
  // is `Message` alone until somebody augments the interface. If a dependency
  // ever augments it, or the two packages resolve different copies of
  // `@earendil-works/pi-ai`, this stops compiling. That is the whole reason
  // this adapter needs no cast anywhere.
  it('accepts pi-ai messages where pi-agent-core expects its own', () => {
    const messages: PiMessage[] = PiPrompt.toPiContext(
      conversation,
      undefined,
    ).messages;

    // No cast, no `satisfies` escape: if `Message` were not assignable to
    // `AgentMessage` this line would not typecheck.
    expect(estimateContextTokens(messages).tokens).toBe(500);
    expect(estimatePiMessageTokens(messages[0]!)).toBe(200);
  });
});
