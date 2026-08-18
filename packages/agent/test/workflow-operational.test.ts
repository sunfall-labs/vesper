import { describe, expect, it } from '@effect/vitest';
import { Deferred, Effect, Exit, Layer, Option, Ref, Schema } from 'effect';
import {
  DurableDeferred,
  Workflow,
  WorkflowEngine,
} from 'effect/unstable/workflow';

/**
 * Operational boundary for this suite:
 *
 * - WorkflowEngine.layerMemory proves deterministic suspension, wake-up, and
 *   persisted-result re-read within one runtime. Its wake-up resumes the
 *   suspended handler fiber in place, so this suite does not claim a second
 *   handler entry as process replay evidence.
 * - It does not prove process crash recovery. The memory engine has no
 *   persistence boundary, so this suite deliberately does not pretend that
 *   memory state survives a runtime replacement. The opt-in
 *   workflow-cluster.integration.test.ts covers completed-result reopen with
 *   ClusterWorkflowEngine + SingleRunner + Postgres; it does not claim
 *   distributed runner failover.
 */

const WaitWorkflow = Workflow.make('OperationalWaitProbe', {
  payload: { id: Schema.String },
  idempotencyKey: ({ id }) => id,
  success: Schema.String,
  error: Schema.String,
});

const deferred = DurableDeferred.make('approval', {
  success: Schema.String,
  error: Schema.String,
});

describe('workflow operational evidence', () => {
  it.effect(
    'suspends, wakes, and re-reads a durable wait under the memory engine',
    () =>
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        const issued = yield* Deferred.make<DurableDeferred.Token>();
        const live = WaitWorkflow.toLayer(() =>
          Effect.gen(function* () {
            yield* Ref.update(calls, (count) => count + 1);
            const token = yield* DurableDeferred.token(deferred);
            yield* Deferred.succeed(issued, token);
            return yield* DurableDeferred.await(deferred);
          }),
        ).pipe(Layer.provideMerge(WorkflowEngine.layerMemory));
        const payload = { id: 'approval-1' };

        return yield* Effect.gen(function* () {
          const executionId = yield* WaitWorkflow.execute(payload, {
            discard: true,
          });
          const token = yield* Deferred.await(issued);
          const pending = yield* WaitWorkflow.poll(executionId);
          expect(Option.isNone(pending)).toBe(true);

          yield* DurableDeferred.succeed(deferred, {
            token,
            value: 'approved',
          });

          const result = yield* WaitWorkflow.execute(payload);
          expect(result).toBe('approved');
          // The memory engine resumes the suspended fiber in place. A second
          // handler entry is a crash/reopen property and is intentionally not
          // inferred from this in-process engine.
          expect(yield* Ref.get(calls)).toBe(1);

          const completed = yield* WaitWorkflow.poll(executionId);
          expect(Option.getOrThrow(completed)).toMatchObject({
            _tag: 'Complete',
            exit: Exit.succeed('approved'),
          });
        }).pipe(Effect.provide(live));
      }).pipe(Effect.scoped),
  );
});
