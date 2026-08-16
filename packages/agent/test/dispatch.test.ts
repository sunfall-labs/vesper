import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { Effect, Layer, Ref, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { Agent } from '../src/agent.js';
import { AgentLog } from '../src/log.js';

// The tool-dispatch seam.
//
// What these have to prove, and each is mutation-checked:
//
//   - a tool whose outcome a *crashed* run already recorded is not run again,
//     and the model is shown the recorded answer;
//   - a tool whose outcome a *settled* run recorded IS run again, because a
//     conversation that finished has nothing to recover;
//   - a fresh conversation dispatches normally, so the seam costs nothing on
//     the ordinary path;
//   - what is stored and served is the toolkit's *encoding* of the result,
//     which is the form the provider is shown.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const CALL_ID = 'call-1';

const callingTurn: Response.StreamPartEncoded[] = [
  {
    type: 'tool-call' as const,
    id: CALL_ID,
    name: 'lookup',
    params: { id: '42' },
  },
  finish('tool-calls'),
];

const answeringTurn: Response.StreamPartEncoded[] = [
  { type: 'text-start' as const, id: 'b' },
  { type: 'text-delta' as const, id: 'b', delta: 'done' },
  { type: 'text-end' as const, id: 'b' },
  finish(),
];

/** A model that also records the prompt it was handed, per turn. */
const scripted = (prompts: string[]) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const turns = [callingTurn, answeringTurn];
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              prompts.push(JSON.stringify(options.prompt));
              return Stream.fromIterable(
                turns[Math.min(index, turns.length - 1)]!,
              );
            }),
          ),
      });
    }),
  );

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String, at: Schema.DateFromString }),
});

const CONVERSATION = 'dispatch-conversation';
const PATH = AgentLog.pathFor(CONVERSATION);
const WHEN = new Date('2026-01-02T03:04:05.000Z');
/** Deliberately not `WHEN`: a served result must be distinguishable. */
const RECORDED = new Date('2020-05-06T07:08:09.000Z');

const agentWith = (ran: { count: number }) =>
  Agent.make({
    name: 'test',
    instructions: 'be terse',
    toolkit: Toolkit.make(lookup),
  }).withHandlers({
    lookup: ({ id }) =>
      Effect.sync(() => {
        ran.count += 1;
        return { status: `fresh:${id}`, at: WHEN };
      }),
  });

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  prompts: string[] = [],
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.orDie,
      Effect.provide(scripted(prompts)),
      Effect.provide(LogStoreMemory.layer),
      Effect.scoped,
    ),
  );

/**
 * Write a previous run's records straight into the conversation.
 *
 * Deliberately not "run the agent and kill it": a crash has to leave the log
 * in an exact state for this to be testing the gate rather than testing
 * whatever a killed fiber happens to flush.
 */
const seed = Effect.fn('test.seed')(function* (
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  yield* store.create(PATH, CONVERSATION).pipe(Effect.orDie);
  const claim = yield* store.acquire(PATH, 'previous-run').pipe(Effect.orDie);
  yield* store
    .append({
      path: PATH,
      producerId: claim.producerId,
      epoch: claim.epoch,
      sequence: claim.nextSequence,
      records: records.map((record) => ({
        conversationId: CONVERSATION,
        timestamp: 1_700_000_000_000,
        record,
      })),
    })
    .pipe(Effect.orDie);
});

const started: ConversationRecord.Record = {
  _tag: 'RunStarted',
  agent: 'test',
  prompt: [],
};

const called: ConversationRecord.Record = {
  _tag: 'ToolCall',
  step: 1,
  id: CALL_ID,
  name: 'lookup',
  params: { id: '42' },
};

const outcome: ConversationRecord.Record = {
  _tag: 'ToolOutcome',
  step: 1,
  id: CALL_ID,
  name: 'lookup',
  outcome: 'success',
  result: { status: 'from-log', at: RECORDED.toISOString() },
};

const settledRun: ConversationRecord.Record = {
  _tag: 'RunSettled',
  outcome: 'success',
  detail: '',
  steps: 2,
  usage: { input: 0, output: 0 },
};

const outcomesOf = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'ToolOutcome' ? [envelope.record] : [],
  );

const readAll = Effect.fn('test.readAll')(function* () {
  const store = yield* LogStore.Service;
  const page = yield* store.read(PATH, { limit: 1000 }).pipe(Effect.orDie);
  return page.records;
});

describe('recovering a tool call from the log', () => {
  it('does not re-run a tool an unsettled run already settled', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    expect(ran.count).toBe(0);
    // The new run recorded the outcome it was served, not one it produced.
    expect(outcomesOf(written).at(-1)).toMatchObject({
      id: CALL_ID,
      result: { status: 'from-log' },
    });
  });

  it('shows the model the recovered result, not a fresh one', async () => {
    const ran = { count: 0 };
    const prompts: string[] = [];

    await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
      }),
      prompts,
    );

    // The second turn's prompt carries the tool result message. It is the
    // encoded result that lands there, which is why the log stores that form.
    expect(prompts[1]).toContain('from-log');
    expect(prompts[1]).not.toContain('fresh:');
  });

  it('re-runs the tool once the earlier run has settled', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome, settledRun]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    // A conversation whose last run finished has nothing to recover. Serving
    // its outcomes to a later run would answer a *new* question with an old
    // answer, which is worse than running the tool twice.
    expect(ran.count).toBe(1);
    expect(outcomesOf(written).at(-1)).toMatchObject({
      result: { status: 'fresh:42' },
    });
  });

  it('dispatches normally on a conversation with no history', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    expect(ran.count).toBe(1);
    expect(outcomesOf(written)).toHaveLength(1);
  });

  it('will not serve an outcome recorded under a different tool name', async () => {
    const ran = { count: 0 };

    await run(
      Effect.gen(function* () {
        yield* seed([
          started,
          called,
          { ...outcome, _tag: 'ToolOutcome', name: 'something_else' },
        ]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
      }),
    );

    expect(ran.count).toBe(1);
  });
});

describe('what a tool outcome stores', () => {
  it('stores the toolkit’s encoding, not the handler’s value', async () => {
    const ran = { count: 0 };

    const written = await run(
      Effect.gen(function* () {
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
    );

    // `at` is a `Date` in the handler and an ISO string in the log, because
    // the encoded form is the one the provider is shown and the only one a
    // resuming dispatch can serve back unchanged.
    expect(outcomesOf(written)[0]?.result).toEqual({
      status: 'fresh:42',
      at: WHEN.toISOString(),
    });
  });

  it('decodes a served result back through the tool’s schema', async () => {
    const ran = { count: 0 };
    const seen: unknown[] = [];

    await run(
      Effect.gen(function* () {
        yield* seed([started, called, outcome]);
        yield* agentWith(ran)
          .recordingTo(CONVERSATION)
          .stream('hi')
          .pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                if (
                  event._tag === 'Part' &&
                  (event.part as Response.StreamPartEncoded).type ===
                    'tool-result'
                ) {
                  seen.push(
                    (event.part as unknown as { result: { at: unknown } })
                      .result.at,
                  );
                }
              }),
            ),
            Effect.orDie,
          );
      }),
    );

    // A consumer of the live stream reads the decoded half of a tool result.
    // Serving the stored JSON there would hand it a string where its type
    // says `Date` — so the stored value goes back through the tool's own
    // codec first.
    expect(seen).toEqual([RECORDED]);
  });
});
