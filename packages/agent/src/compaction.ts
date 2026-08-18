import { AiError } from 'effect/unstable/ai';

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
  /** Optional provider context-window size for proactive compaction. */
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
    'Summarize the conversation so far. Preserve decisions made, facts ' +
    'established, file paths, identifiers, and any task still outstanding. ' +
    'Omit pleasantries and superseded reasoning. Write it for a reader who ' +
    'must continue the work with no other context.',
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
