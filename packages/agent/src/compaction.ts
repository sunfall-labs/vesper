import type { AiError } from 'effect/unstable/ai';

/** Policy for proactive and reactive conversation compaction. */
export interface Policy {
  /** Headroom left below the provider context window. */
  readonly reserveTokens: number;
  /** Recent tokens preserved without summarization. */
  readonly keepRecentTokens: number;
  /** Instruction given to the summarization model. */
  readonly instructions: string;
  /** Optional summarizer system prompt. */
  readonly system?: string | undefined;
  /**
   * The provider's context-window size, used only by proactive compaction.
   *
   * Proactive compaction is inactive without it: the loop targets the
   * `LanguageModel` tag, and that tag carries no window, so there is nothing
   * to compare a token estimate against. Leaving this unset does not disable
   * compaction — the reactive trigger still fires when a provider rejects an
   * oversized prompt — but it does mean every compaction costs a rejected
   * request and a re-run of the turn instead of the estimate catching it
   * first. The agent logs a one-time warning per run when a policy is
   * configured without this field.
   */
  readonly contextWindow?: number | undefined;
}

/** Default system prompt for the summarization call. */
export const defaultSystem =
  'You are a context summarization assistant. Your task is to read a ' +
  'conversation between a user and an AI assistant, then produce a ' +
  'structured summary following the exact format specified.\n\n' +
  'Do NOT continue the conversation. Do NOT respond to any questions in the ' +
  'conversation. ONLY output the structured summary.';

export const defaultPolicy: Policy = {
  reserveTokens: 20_000,
  keepRecentTokens: 8_000,
  system: defaultSystem,
  instructions:
    'Create a structured context checkpoint for another model to continue ' +
    'the work. Use this exact format:\n\n' +
    '## Goal\n[What the user is trying to accomplish.]\n\n' +
    '## Constraints & Preferences\n- [Requirements and preferences, or "(none)".]\n\n' +
    '## Progress\n### Done\n- [x] [Completed work.]\n\n' +
    '### In Progress\n- [ ] [Current work.]\n\n' +
    '### Blocked\n- [Anything preventing progress, or "(none)".]\n\n' +
    '## Key Decisions\n- **[Decision]**: [Brief rationale.]\n\n' +
    '## Next Steps\n1. [Ordered continuation steps.]\n\n' +
    '## Critical Context\n- [Facts and references needed to continue, or "(none)".]\n\n' +
    'Keep every section concise. Preserve exact file paths, function names, ' +
    'identifiers, and error messages. Omit pleasantries and superseded reasoning.',
};

/** Provider constraint used for a prompt that no longer fits. */
export const CONTEXT_OVERFLOW = 'context-window';

/** True when an Effect AI failure reports a context-window overflow. */
export const isContextOverflow = (error: AiError.AiError): boolean =>
  error.reason._tag === 'InvalidRequestError' &&
  (error.reason.constraint === CONTEXT_OVERFLOW ||
    /(?:context(?: length| window)?(?: is)?(?: exceeded| too long)|maximum context length|model_context_window_exceeded|prompt is too long|too many tokens|input is too long)/i.test(
      error.reason.description ?? '',
    ));

export * as Compaction from './compaction.js';
