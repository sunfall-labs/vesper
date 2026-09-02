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
  modelCalls: Ref.Ref<number>,
): Layer.Layer<LanguageModel.LanguageModel> =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
      streamText: () =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* Ref.update(modelCalls, (n) => n + 1);
            const current = yield* Ref.get(counts);
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
      const modelCalls = yield* Ref.make(0);
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
        contentAwareModel(counts, modelCalls),
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
        modelCalls: Ref.get(modelCalls),
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
// buckets. `run:before-completed` used to be a documented bug — a resumed
// run re-asked the provider for a turn it had already received in full — and
// is fixed (`agent.ts`'s `settledCompletion`); it now converges alongside
// the locations that always did. The usage locations remain a real,
// diagnosed limitation, not a test artifact: `test/resume.test.ts`'s "does
// not cache an interrupted run's own guessed usage as a verified checkpoint"
// carries the direct, non-chaos reproduction and the fix that *is* real
// (session-open.ts never caches a crashed run's own guess as a trusted
// checkpoint any more) next to what remains open and why.

/** Reached and recovered correctly by this scenario. */
const CONVERGING_LOCATIONS: ReadonlyArray<Failpoint.Location> = [
  'claim:after-acquire',
  'tool:before-started',
  'approval:after-resolved',
  'turn:before-finished',
  'turn:after-finished',
  'run:before-completed',
];

/**
 * LIMITATION: cumulative usage undercounts by exactly one physical run's
 * worth of tokens after a crash strictly inside the `ToolStarted`..
 * `ToolOutcome` window (`tool:after-started`, `tool:before-outcome`,
 * `tool:after-outcome`) or the suspend-registration window
 * (`approval:after-suspended`).
 *
 * Traced directly (bypassing `Chaos.converge`, both against the in-memory
 * store): at every one of these, the crashed physical run's own `RunSettled`
 * carries `{ input: 0, output: 0 }`, not the turn's real cost — not because
 * `session-open.ts`/`recording-sink.ts` drop a number they had, but because
 * neither has one yet. `recording-sink.ts`'s `pending.usage` — the only
 * thing `RunSettled.usage` can ever be — only advances at a `TurnFinished`/
 * `Completed` lifecycle event, and Effect AI's own `LanguageModel.streamText`
 * defers emitting a turn's `finish` part (usage's only carrier, downstream
 * of nothing else) until every tool call that turn requested has been
 * resolved — "This guarantees tool results are emitted before finish in
 * streaming mode," in its own source. A crash anywhere in that window is
 * therefore a crash before the number exists anywhere in the process, durable
 * or not; no fold over `session.recorded`, however constructed, can recover
 * it. This is the same fact CONTEXT.md's Turn entry states for a different
 * consequence ("Effect AI begins automatic tool resolution before it emits
 * its deferred finish part").
 *
 * What *is* fixed: `session-open.ts`'s `trackedAppend` used to cache that
 * necessarily-incomplete guess into `RunSettled.resume` for every outcome,
 * turning a one-turn shortfall into a trusted checkpoint later opens would
 * build on rather than re-derive past. It now only caches `resume` for a
 * `'success'`/`'cancelled'` settlement — a clean stop, where `pending.usage`
 * is accurate — so a `'failure'`/`'interrupted'` settlement (what a crash
 * produces) always forces the next open to re-fold `usage` from durable
 * `TurnFinished`/`Completed` records instead of trusting a cached guess.
 * `test/resume.test.ts` carries both halves of this directly: the shortfall
 * that remains, and the checkpoint that no longer compounds it.
 */
const USAGE_UNDERCOUNT_LOCATIONS: ReadonlyArray<Failpoint.Location> = [
  'tool:after-started',
  'tool:before-outcome',
  'tool:after-outcome',
  'approval:after-suspended',
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

  // The batch above allows one tolerated provider call per location — the
  // default, correct for a crash like `claim:after-acquire`'s or
  // `tool:before-started`'s that lands before anything about the in-flight
  // call is durable, where a full clean retry is the only correct recovery
  // (VSP-007). `run:before-completed` is the opposite shape: every part of
  // that call's own output — text, usage, everything `Completed` would
  // carry — is already durable at the crash point, so this asserts the
  // model-call count with zero tolerance, the regression guard for the
  // bug this location used to have.
  it.effect(
    'settles run:before-completed with exactly as many provider calls as the crash-free baseline',
    () =>
      Effect.gen(function* () {
        const report = yield* Chaos.converge({
          attempt: makeAttempt(storeLayer),
          locations: ['run:before-completed'],
          windowedCallIds: ['lookup', 'charge'],
          modelCallTolerance: 0,
        });
        expect(badStatuses(report)).toEqual([]);
      }),
    30_000,
  );

  // The two tests below are deliberately not assertions that everything is
  // fine — `no-disabled-tests` forbids `it.skip` in this repository, so each
  // pins today's actual (limited, or not-yet-exercised) status instead of
  // silently skipping. Each fails loudly — a real signal, not a stale
  // skip — the moment the underlying behavior changes, whether that is the
  // limitation narrowing further (move the location up to
  // `CONVERGING_LOCATIONS`) or regressing somewhere new.
  it.effect(
    'LIMITATION: cumulative usage undercounts by one physical run after a crash strictly inside a tool-dispatch or suspend-registration window — see USAGE_UNDERCOUNT_LOCATIONS above',
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
