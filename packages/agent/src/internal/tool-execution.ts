import { Context } from 'effect';

import type * as AgentLog from '../log.js';

/** The recorded tool invocation currently evaluating its handler. */
export interface Execution {
  readonly session: AgentLog.Session;
  readonly name: string;
  readonly toolCallId: import('@sunfall/vesper-log/vocabulary').LogVocabulary.ToolCallId;
}

/**
 * Internal fiber-local bridge from dispatch to workflow-aware handlers.
 *
 * A required Tag makes using a workflow wait outside dispatch visible in the
 * Effect requirement channel. `AgentWorkflow.durable` declares it and
 * dispatch supplies it for the exact handler invocation.
 */
export class Current extends Context.Service<Current, Execution>()(
  '@sunfall/vesper-agent/internal/CurrentToolExecution',
) {}
