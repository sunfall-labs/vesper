import { performance } from 'node:perf_hooks';

import { Deferred, Effect, Layer, Ref, Schema } from 'effect';
import {
  DurableDeferred,
  Workflow,
  WorkflowEngine,
} from 'effect/unstable/workflow';

/**
 * A small deterministic operational probe, intentionally separate from the
 * latency workloads. It measures the accepted -> suspended -> completed ->
 * persisted-result re-read path using Effect's in-memory engine. It is not a
 * crash benchmark: WorkflowEngine.layerMemory has no persistence boundary to
 * reopen, and its wake-up resumes the handler fiber in place.
 */
const ProbeWorkflow = Workflow.make('OperationalBenchmarkWait', {
  payload: { id: Schema.String },
  idempotencyKey: ({ id }) => id,
  success: Schema.String,
  error: Schema.String,
});

const probeDeferred = DurableDeferred.make('approval', {
  success: Schema.String,
  error: Schema.String,
});

const probe = Effect.gen(function* () {
  const calls = yield* Ref.make(0);
  const issued = yield* Deferred.make<DurableDeferred.Token>();
  const live = ProbeWorkflow.toLayer(() =>
    Effect.gen(function* () {
      yield* Ref.update(calls, (count) => count + 1);
      const token = yield* DurableDeferred.token(probeDeferred);
      yield* Deferred.succeed(issued, token);
      return yield* DurableDeferred.await(probeDeferred);
    }),
  ).pipe(Layer.provideMerge(WorkflowEngine.layerMemory));

  return yield* Effect.gen(function* () {
    const started = performance.now();
    const executionId = yield* ProbeWorkflow.execute(
      { id: 'benchmark' },
      { discard: true },
    );
    const token = yield* Deferred.await(issued);
    yield* DurableDeferred.succeed(probeDeferred, {
      token,
      value: 'approved',
    });
    const result = yield* ProbeWorkflow.execute({ id: 'benchmark' });
    return {
      executionId,
      result,
      handlerEntries: yield* Ref.get(calls),
      elapsedMs: performance.now() - started,
      engine: 'WorkflowEngine.layerMemory',
      processCrashRecovery: 'not-proven',
    };
  }).pipe(Effect.provide(live));
}).pipe(Effect.scoped);

const result = await Effect.runPromise(probe);
console.log(JSON.stringify(result, null, 2));
