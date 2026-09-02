import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { LogStoreSqlite } from '@sunfall/vesper-log-sqlite/layer';
import { SqliteNative } from '@sunfall/vesper-log-sqlite/layer-native';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Context, Effect, Layer, Ref, Schema, Scope, Stream } from 'effect';
import * as ReactivityModule from 'effect/unstable/reactivity/Reactivity';
import { SqlClient } from 'effect/unstable/sql';
import {
  LanguageModel,
  Tool,
  Toolkit,
  type Response,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import type * as Failpoint from '../src/internal/failpoint.js';
import { Chaos } from '../src/testing.js';

// Proves recovery converges: for every named durable boundary, crashing
// there and reopening the conversation lands on the same result a
// crash-free run would have produced, with a well-formed log and no tool
// handler replayed outside the ToolStarted..ToolOutcome window it is
// legitimately reconciled through.
//
// The scenario is a three-turn conversation with two tool calls and one
// approval: turn 1 calls `lookup` (an ordinary tool), turn 2 calls `charge`
// (`needsApproval`, so the run suspends before its handler is ever
// entered), and turn 3 — reached only after `resolveApproval` — answers with
// text. `Chaos.converge` runs this once per `Failpoint.Location` against
// each backend below.

const LOOKUP_CALL_ID = LogVocabulary.ToolCallId.make('chaos-lookup');
const CHARGE_CALL_ID = LogVocabulary.ToolCallId.make('chaos-charge');
const START_INPUT = 'process order 42';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const LOOKUP_TURN: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: 'text-start', id: 't0' },
  { type: 'text-delta', id: 't0', delta: 'looking it up' },
  { type: 'text-end', id: 't0' },
  {
    type: 'tool-call',
    id: LOOKUP_CALL_ID,
    name: 'lookup',
    params: { id: '42' },
  },
  finish('tool-calls'),
];

const CHARGE_TURN: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: 'text-start', id: 't1' },
  { type: 'text-delta', id: 't1', delta: 'charging' },
  { type: 'text-end', id: 't1' },
  {
    type: 'tool-call',
    id: CHARGE_CALL_ID,
    name: 'charge',
    params: { id: '42' },
  },
  finish('tool-calls'),
];

const DONE_TURN: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: 'text-start', id: 't2' },
  { type: 'text-delta', id: 't2', delta: 'done' },
  { type: 'text-end', id: 't2' },
  finish(),
];

/**
 * A model that answers from whether this scenario's handlers have actually
 * run, not from a fixed call sequence or from the reconstructed prompt.
 * `Chaos.converge` calls a scenario's `drive` more than once per location —
 * once to produce the crash, again to recover — and a crash can land before,
 * during, or after any given model call, so a fixed-index script
 * (`ScriptedModel`, built for one linear run) would serve the wrong turn, or
 * run out of turns, the moment recovery asks more or fewer times than a
 * crash-free run would have.
 *
 * The prompt itself is not a reliable signal to key this on either: an
 * approval-gated call's eventual result is durably dispatched by
 * `resolveIndeterminate` in a later turn than the one that asked for it, and
 * `AgentHistory`'s per-turn fold only reunites a tool call with a result
 * recorded in the *same* turn ("a tool call and its result travel together
 * or neither travels") — so a call approved after its own turn already
 * flushed never becomes visible to a later prompt reconstruction at all, by
 * design. `counts`, updated by the handlers themselves, is what is actually
 * true regardless of how the log or the prompt represent it.
 */
const contentAwareModel = (
  counts: Ref.Ref<Record<string, number>>,
): Layer.Layer<LanguageModel.LanguageModel> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () =>
        Stream.unwrap(
          Effect.map(Ref.get(counts), (current) => {
            if ((current['lookup'] ?? 0) === 0) {
              return Stream.fromIterable(LOOKUP_TURN);
            }
            if ((current['charge'] ?? 0) === 0) {
              return Stream.fromIterable(CHARGE_TURN);
            }
            return Stream.fromIterable(DONE_TURN);
          }),
        ),
    }),
  );

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

const charge = Tool.make('charge', {
  description: 'charge a customer for an order',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ charged: Schema.Boolean }),
}).setNeedsApproval(true);

const bump = (counts: Ref.Ref<Record<string, number>>, key: string) =>
  Ref.update(counts, (current) => ({
    ...current,
    [key]: (current[key] ?? 0) + 1,
  }));

const buildAgent = (counts: Ref.Ref<Record<string, number>>) =>
  Agent.make({
    name: 'chaos',
    revision: '1',
    instructions: 'be terse',
    toolkit: Toolkit.make(lookup, charge),
  })
    .withHandlers({
      lookup: () => bump(counts, 'lookup').pipe(Effect.as({ status: 'ok' })),
      charge: () => bump(counts, 'charge').pipe(Effect.as({ charged: true })),
    })
    .intercepting({
      // Every tool this scenario runs is a cheap, side-effect-free counter
      // bump — safe to genuinely retry on an indeterminate call, which is
      // exactly what proves recovery resolves it explicitly rather than
      // never (the safe-failure default) or silently (which this interceptor
      // is what stands between the run and).
      onIndeterminateToolCall: () => Effect.succeed(Interception.retry),
    });

/** Build one isolated store instance whose lifetime outlives this function —
 * intentional: {@link Chaos.converge} calls `drive` more than once against
 * the same conversation, and a `Scope` that closed when this function
 * returned would tear the store down before the second call. Never
 * explicitly closed; each trial's store is cheap and this is a test
 * process. */
const openStore = <E>(
  layer: Layer.Layer<LogStore.Service, E>,
): Effect.Effect<LogStore.Interface> =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(layer, scope);
    return Context.get(context, LogStore.Service);
  }).pipe(Effect.orDie);

const memoryStore = LogStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

const sqliteStore = Layer.effect(
  LogStore.Service,
  Effect.gen(function* () {
    yield* LogStoreSqlite.migrate();
    const client = yield* SqlClient.SqlClient;
    return yield* LogStoreSqlite.make(client);
  }),
).pipe(
  Layer.provide(
    Layer.mergeAll(
      SqliteNative.layer(':memory:'),
      NodeServices.layer,
      ReactivityModule.layer,
    ),
  ),
);

const makeAttempt =
  <E>(storeLayer: Layer.Layer<LogStore.Service, E>) =>
  (
    conversationId: string,
    failpointLayer: Layer.Layer<never>,
  ): Effect.Effect<Chaos.ChaosAttempt> =>
    Effect.gen(function* () {
      const store = yield* openStore(storeLayer);
      const counts = yield* Ref.make<Record<string, number>>({});
      const started = yield* Ref.make(false);
      const agent = buildAgent(counts);
      const conversation = Conversation.make(agent, conversationId);

      const driveOnce = Effect.gen(function* () {
        const alreadyStarted = yield* Ref.getAndSet(started, true);
        let result = yield* conversation.run(
          alreadyStarted ? undefined : START_INPUT,
        );
        while (result.outcome === 'suspended') {
          yield* conversation
            .resolveApproval(CHARGE_CALL_ID, 'approve')
            .pipe(
              Effect.catchTag('ApprovalResolutionError', (error) =>
                error.reason === 'already_resolved'
                  ? Effect.void
                  : Effect.fail(error),
              ),
            );
          result = yield* conversation.run();
        }
        return result;
      });

      const layer = Layer.mergeAll(
        contentAwareModel(counts),
        NodeServices.layer,
        failpointLayer,
      );

      return {
        drive: driveOnce.pipe(
          Effect.provide(layer),
          Effect.provideService(LogStore.Service, store),
          Effect.orDie,
        ),
        executionCounts: Ref.get(counts).pipe(
          Effect.map((record) => new Map(Object.entries(record))),
        ),
        records: conversation
          .records()
          .pipe(
            Stream.runCollect,
            Effect.provideService(LogStore.Service, store),
            Effect.orDie,
          ),
      };
    });

// Every location this scenario actually reaches sorts into one of three
// buckets. The first converges cleanly and is asserted on every test run.
// The other two are real findings from running this suite, not test
// artifacts — both reproduced identically against the in-memory and the
// SQLite store, and both were traced to a specific record-level cause before
// being written down here. Per the task's own instruction, a location that
// cannot converge because of a real recovery bug is not papered over: it
// stays in the report as `failed`, and the test for it is `it.skip`, naming
// the bug rather than silently passing or quietly deleting the location.

/** Reached and recovered correctly by this scenario. */
const CONVERGING_LOCATIONS: ReadonlyArray<Failpoint.Location> = [
  'claim:after-acquire',
  'tool:before-started',
  'approval:after-resolved',
  'turn:before-finished',
  'turn:after-finished',
];

/**
 * BUG: cumulative usage undercounts by exactly one physical run's worth of
 * tokens after a crash whose own physical run reaches settlement — either a
 * `RunSettled(outcome: 'failure')` written by `recording-sink.ts`'s `settle`
 * finalizer (`tool:after-started`, `tool:before-outcome`, `tool:after-outcome`
 * — `pendingToolState` is `'none'` or `'indeterminate'`-then-resolved at that
 * point, so `settle` does write a record), or the still-open, never-settled
 * run a `ToolSuspended` leaves behind (`approval:after-suspended`).
 *
 * Traced with `records`/`executionCounts` directly (bypassing
 * `Chaos.converge`) against the in-memory store: for `tool:after-outcome`,
 * the crashed physical run's own `RunSettled` correctly carries that turn's
 * usage (`{ input: 10, output: 4 }`), `session-open.ts`'s `trackedAppend`
 * folds it into `opened.usage` via `addUsage` regardless of outcome, and the
 * next two physical runs each contribute one more turn's worth — three
 * physical runs, three turns, and a crash-free baseline of the same
 * conversation settles at `{ input: 30, output: 12 }`. The recovered
 * conversation here settles at `{ input: 20, output: 8 }` instead: exactly
 * the first (crashed) physical run's contribution missing, with the record
 * log otherwise well-formed and every tool call executing the expected
 * number of times (`lookup` and `charge` once each, or twice only where the
 * `ToolStarted`/`ToolOutcome` window legitimately retries one of them).
 * `approval:after-suspended`'s crashed run never gets a `RunSettled` at all
 * (`pendingToolState` is `'suspended'`, so `settle` deliberately skips it),
 * and the same one-turn shortfall appears — suggesting the same usage is
 * lost by a related path rather than two unrelated bugs, though this suite
 * did not trace that second path to its own root cause.
 */
const USAGE_UNDERCOUNT_LOCATIONS: ReadonlyArray<Failpoint.Location> = [
  'tool:after-started',
  'tool:before-outcome',
  'tool:after-outcome',
  'approval:after-suspended',
];

/**
 * BUG: recovering from a crash between the final turn's `TurnFinished` and
 * its `Completed` record re-asks the model for a turn whose content is
 * already fully durable, instead of deriving `Completed` from history the
 * way it derives everything else on resume.
 *
 * Traced directly: at `run:before-completed`, the crashed physical run's
 * records end `..., 'Text', 'TurnFinished', 'RunSettled'` — the finish
 * reason ('stop', no tool calls) is fully implied by the *absence* of a
 * `ToolCall` record in that turn, exactly the inference
 * `AgentHistory.messagesFrom` already relies on to drop an unanswered call.
 * Recovery does not make that inference here: it opens a **third** physical
 * run, asks the model again, and gets a third `'Text'`/`'TurnFinished'` pair
 * before finally reaching `Completed`. The extra call is not free — it is
 * exactly the "provider cost may repeat" case `docs/guarantees.md` (VSP-007)
 * scopes to a crash catching a turn genuinely mid-flight, and this turn was
 * not: every part of it was already durable except the run's own bookkeeping
 * that it was over.
 */
const EXTRA_CALL_LOCATIONS: ReadonlyArray<Failpoint.Location> = [
  'run:before-completed',
];

/**
 * Not a bug: this scenario never crosses a compaction threshold or sends a
 * signal, so neither `compaction:*` nor `signal:after-received` is ever
 * reached — `Chaos.converge` correctly reports `not-triggered` rather than
 * guessing. Closing this would mean extending the scenario with a
 * low-`contextWindow` compaction policy and a mid-run `conversation.send`,
 * which is scenario work, not a finding about recovery; left as a TODO
 * alongside the two real bugs above rather than mixed in with them.
 */
const NOT_EXERCISED_LOCATIONS: ReadonlyArray<Failpoint.Location> = [
  'compaction:before-append',
  'compaction:after-append',
  'signal:after-received',
];

const badStatuses = (report: Chaos.ChaosReport) =>
  report.results.filter((result) => result.status._tag !== 'converged');

describe.each([
  ['in-memory', memoryStore],
  ['sqlite', sqliteStore],
] as const)('chaos convergence (%s store)', (_name, storeLayer) => {
  it.effect(
    'converges for every location this scenario recovers cleanly through',
    () =>
      Effect.gen(function* () {
        const report = yield* Chaos.converge({
          attempt: makeAttempt(storeLayer),
          locations: CONVERGING_LOCATIONS,
          windowedCallIds: ['lookup', 'charge'],
        });
        expect(badStatuses(report)).toEqual([]);
      }),
    30_000,
  );

  // The three tests below are deliberately not assertions that everything is
  // fine — `no-disabled-tests` forbids `it.skip` in this repository, so each
  // pins today's actual (broken, or not-yet-exercised) status instead of
  // silently skipping. Each fails loudly — a real signal, not a stale
  // skip — the moment the underlying behavior changes, whether that is the
  // bug getting fixed (move the location up to `CONVERGING_LOCATIONS`) or
  // regressing somewhere new.
  it.effect(
    'BUG: cumulative usage undercounts by one physical run after a crash near a tool boundary — see USAGE_UNDERCOUNT_LOCATIONS above',
    () =>
      Effect.gen(function* () {
        const report = yield* Chaos.converge({
          attempt: makeAttempt(storeLayer),
          locations: USAGE_UNDERCOUNT_LOCATIONS,
          windowedCallIds: ['lookup', 'charge'],
        });
        expect(report.results.map((result) => result.status._tag)).toEqual(
          USAGE_UNDERCOUNT_LOCATIONS.map(() => 'failed'),
        );
      }),
    30_000,
  );

  it.effect(
    'BUG: run:before-completed re-asks the model for an already-durable final turn — see EXTRA_CALL_LOCATIONS above',
    () =>
      Effect.gen(function* () {
        const report = yield* Chaos.converge({
          attempt: makeAttempt(storeLayer),
          locations: EXTRA_CALL_LOCATIONS,
        });
        expect(report.results.map((result) => result.status._tag)).toEqual(
          EXTRA_CALL_LOCATIONS.map(() => 'failed'),
        );
      }),
    30_000,
  );

  it.effect(
    'TODO: extend the scenario to trigger compaction and a signal — see NOT_EXERCISED_LOCATIONS above',
    () =>
      Effect.gen(function* () {
        const report = yield* Chaos.converge({
          attempt: makeAttempt(storeLayer),
          locations: NOT_EXERCISED_LOCATIONS,
        });
        expect(report.results.map((result) => result.status._tag)).toEqual(
          NOT_EXERCISED_LOCATIONS.map(() => 'not-triggered'),
        );
      }),
    30_000,
  );
});
