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
    const completed = yield* events.pipe(
      Stream.runFold(
        (): AgentEvents.Lifecycle | undefined => undefined,
        (last, event) => (event._tag === 'Completed' ? event : last),
      ),
    );

    // Every run terminates with `Completed`. Ending without it means the loop
    // and this fold have drifted, which is a defect rather than a recoverable
    // caller failure.
    if (completed?._tag !== 'Completed') {
      return yield* Effect.die(
        new Error('Agent stream ended without completing'),
      );
    }

    return {
      outcome: completed.outcome,
      text: completed.text,
      steps: completed.steps,
      usage: completed.usage,
    } satisfies Agent.Result;
  });
