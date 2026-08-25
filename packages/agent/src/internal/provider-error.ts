import { Predicate } from 'effect';
import { AiError, type Response } from 'effect/unstable/ai';

import { Compaction } from '../compaction.js';
import type { AgentEvents } from '../event.js';

// Failure normalization at the provider seam. Everything a raw provider error
// or explicitly incomplete finish can be is folded into `AiError` here,
// including the context-overflow classification reactive compaction keys on.

type OutputMethod = 'streamText' | 'compact';

const incompleteReasons: Readonly<
  Partial<Record<Response.FinishReason, string>>
> = {
  length: 'generation reached its output token limit',
  'content-filter': 'generation was stopped by the provider content filter',
  error: 'the provider reported a generation error',
};

/** Convert an explicitly incomplete finish into the loop's typed failure. */
export const incompleteOutputError = (
  method: OutputMethod,
  reason: Response.FinishReason,
): AiError.AiError | undefined => {
  const description = incompleteReasons[reason];
  if (description === undefined) {
    return undefined;
  }
  return new AiError.AiError({
    module: method === 'compact' ? 'Compaction' : 'Agent',
    method,
    reason: new AiError.InvalidOutputError({
      description: `${method === 'compact' ? 'Compaction' : 'Model'} output was incomplete because ${description}`,
      metadata: { finishReason: reason },
    }),
  });
};

export const approvalRequiresConversationError = (
  pendingApprovals: ReadonlyArray<AgentEvents.PendingApproval>,
): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'run',
    reason: new AiError.InvalidRequestError({
      description:
        `Tool call${pendingApprovals.length === 1 ? '' : 's'} ` +
        `${pendingApprovals.map((approval) => `"${approval.toolName}" (${approval.toolCallId})`).join(', ')} ` +
        'require approval, which can only be resolved durably. Bind this ' +
        'agent to a Conversation and call Conversation.resolveApproval ' +
        'instead of running it directly.',
      metadata: {
        pendingApprovals: pendingApprovals.map((approval) => ({
          toolCallId: approval.toolCallId,
          toolName: approval.toolName,
        })),
      },
    }),
  });

export const normalizeProviderError = (
  error: unknown,
  partMetadata?: Record<string, unknown> | undefined,
): AiError.AiError => {
  if (AiError.isAiError(error)) {
    return error;
  }

  const structured = describeProviderError(error, partMetadata);
  const common = {
    description: structured.description,
    metadata: structured.metadata,
  };

  return new AiError.AiError({
    module: 'Agent',
    method: 'streamText',
    reason:
      /(?:context(?: length| window)?(?: is)?(?: exceeded| too long)|maximum context length|model_context_window_exceeded|prompt is too long|too many tokens|input is too long)/i.test(
        structured.description,
      )
        ? new AiError.InvalidRequestError({
            ...common,
            constraint: Compaction.CONTEXT_OVERFLOW,
          })
        : new AiError.UnknownError(common),
  });
};

const describeProviderError = (
  error: unknown,
  partMetadata?: Record<string, unknown> | undefined,
): {
  readonly description: string;
  readonly metadata: Record<string, unknown>;
} => {
  if (!Predicate.isObject(error)) {
    return { description: String(error), metadata: partMetadata ?? {} };
  }

  const value = error;
  const metadata = Predicate.isObject(value['metadata'])
    ? value['metadata']
    : {};
  const code =
    value['code'] ??
    (typeof value['type'] === 'string' && value['type'] !== 'error'
      ? value['type']
      : undefined);
  const details = [code, value['message'], value['error']]
    .map((part) =>
      typeof part === 'string'
        ? part
        : part === undefined
          ? ''
          : stringifyErrorPart(part),
    )
    .filter((part) => part !== '')
    .join(': ');

  return {
    description: details === '' ? stringifyErrorPart(value) : details,
    metadata: {
      ...partMetadata,
      ...metadata,
      ...(value['code'] === undefined ? {} : { code: value['code'] }),
      ...(typeof value['type'] !== 'string' || value['type'] === 'error'
        ? {}
        : { type: value['type'] }),
    },
  };
};

const stringifyErrorPart = (value: unknown): string => {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
};
