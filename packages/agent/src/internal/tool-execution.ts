import { Context, type Effect } from 'effect';

import type { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import type * as AgentLog from '../log.js';

/** The recorded tool invocation currently evaluating its handler. */
export interface Execution {
  readonly session: AgentLog.Session;
  readonly name: string;
  readonly toolCallId: LogVocabulary.ToolCallId;
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

/**
 * The durable start of one code-mode nested call, supplied around the hidden
 * gate's `handle` by the `exec` handler that owns the call.
 *
 * The gate invokes it at the same point it would append `ToolStarted` — after
 * interception and arbitration, immediately before the handler — so a nested
 * call is "started" under exactly the rule a top-level call is. The hidden
 * gate has no session of its own; this is how it learns that its caller is
 * recording.
 */
export class NestedStart extends Context.Service<
  NestedStart,
  { readonly started: Effect.Effect<void, AgentLog.DurabilityError> }
>()('@sunfall/vesper-agent/internal/NestedToolStart') {}
