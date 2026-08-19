import { Effect, Stream } from 'effect';
import type { Tool } from 'effect/unstable/ai';

import type { Agent } from '../agent.js';
import type { AgentEvents } from '../event.js';

/** Collapse an observed run stream into its terminal result. */
export const foldToResult = <
  Tools extends Record<string, Tool.Any>,
  Error,
  Requires,
>(
  events: Stream.Stream<AgentEvents.ObservedEvent<Tools>, Error, Requires>,
): Effect.Effect<Agent.Result, Error, Requires> =>
  Effect.gen(function* () {
    const terminal = yield* events.pipe(
      Stream.runFold(
        (): AgentEvents.Lifecycle | undefined => undefined,
        (last, event) =>
          event._tag === 'Completed' || event._tag === 'Suspended'
            ? event
            : last,
      ),
    );

    // Every run terminates with `Completed` or `Suspended`. Ending without
    // either means the loop and this fold have drifted, which is a defect
    // rather than a recoverable caller failure.
    if (terminal?._tag === 'Completed') {
      return {
        outcome: terminal.outcome,
        text: terminal.text,
        steps: terminal.steps,
        usage: terminal.usage,
      } satisfies Agent.Result;
    }
    if (terminal?._tag === 'Suspended') {
      return {
        outcome: 'suspended',
        text: terminal.text,
        steps: terminal.step,
        usage: terminal.usage,
        pendingApprovals: terminal.pendingApprovals,
      } satisfies Agent.Result;
    }

    return yield* Effect.die(
      new Error('Agent stream ended without completing'),
    );
  });
