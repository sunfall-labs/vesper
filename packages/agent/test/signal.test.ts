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
import { AgentSignals } from '../src/signal.js';
import { Stop } from '../src/stop.js';

// Signals: out-of-band input to a running conversation.
//
// What these have to prove:
//
//   - a steer reaches the model as input on the next turn, and is recorded as
//     delivered;
//   - a steer outranks the stop condition, because a run that consumed an
//     instruction and stopped anyway has silently ignored it;
//   - a cancel ends the run at the turn boundary and settles it as cancelled,
//     without another model call;
//   - a signal queued before the run began is still delivered;
//   - a signal already delivered is not delivered again to the next run,
//     which is the property the `SignalReceived` cursor exists for;
//   - an agent that is not recording is unaffected.

const finish = () => ({
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 5, uncached: 5, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 2 },
  },
});

const says = (text: string): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id: text },
  { type: 'text-delta' as const, id: text, delta: text },
  { type: 'text-end' as const, id: text },
  finish(),
];

interface Model {
  readonly layer: Layer.Layer<LanguageModel.LanguageModel>;
  readonly prompts: string[];
}

/**
 * A model that always wants to stop, and remembers what it was asked.
 *
 * "Always wants to stop" is the point: every turn ends with no tool calls, so
 * the default stop condition fires at every boundary and a second turn can
 * only happen because a steer overrode it.
 */
const model = (): Model => {
  const prompts: string[] = [];
  return {
    prompts,
    layer: Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () =>
            Effect.succeed<Response.PartEncoded[]>([finish()]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
                prompts.push(JSON.stringify(options.prompt));
                return Stream.fromIterable(says(`turn ${index + 1}`));
              }),
            ),
        });
      }),
    ),
  };
};

const CONVERSATION = 'signalled-conversation';

const agent = Agent.make({
  name: 'test',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
});

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  scripted: Model,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.orDie,
      Effect.provide(scripted.layer),
      Effect.provide(LogStoreMemory.layer),
      Effect.scoped,
    ),
  );

const readAll = Effect.fn('test.readAll')(function* () {
  const store = yield* LogStore.Service;
  const page = yield* store
    .read(AgentLog.pathFor(CONVERSATION), { limit: 1000 })
    .pipe(Effect.orDie);
  return page.records;
});

const received = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'SignalReceived' ? [envelope.record] : [],
  );

const settlement = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.flatMap((envelope) =>
    envelope.record._tag === 'RunSettled' ? [envelope.record] : [],
  );

describe('steering', () => {
  it('reaches the model as input on the next turn', async () => {
    const scripted = model();

    await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'steer',
          text: 'also check the invoice',
          source: 'operator',
        }).pipe(Effect.orDie);

        yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
      }),
      scripted,
    );

    expect(scripted.prompts).toHaveLength(2);
    expect(scripted.prompts[1]).toContain('also check the invoice');
  });

  it('overrides a stop condition that would have ended the run', async () => {
    const scripted = model();

    const result = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'steer',
          text: 'keep going',
          source: 'operator',
        }).pipe(Effect.orDie);

        return yield* agent
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
      }),
      scripted,
    );

    // Every turn this model produces satisfies the default stop condition.
    // Two steps happened, so the steer is what carried it past the first.
    expect(result.steps).toBe(2);
    expect(result.text).toBe('turn 2');
  });

  it('records the delivery, with the offset it consumed', async () => {
    const scripted = model();

    const written = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'steer',
          text: 'keep going',
          source: 'operator',
        }).pipe(Effect.orDie);

        yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
        return yield* readAll();
      }),
      scripted,
    );

    expect(received(written)).toEqual([
      {
        _tag: 'SignalReceived',
        kind: 'steer',
        text: 'keep going',
        source: 'operator',
        step: 1,
        at: expect.any(String),
      },
    ]);

    // And in the right place: after the text the model had already produced,
    // before the turn boundary. A rebuilt prompt then reads as the model's
    // own words followed by the instruction that redirected it, which is the
    // order they happened in.
    expect(written.map((envelope) => envelope.record._tag)).toEqual([
      'RunStarted',
      'Text',
      'SignalReceived',
      'TurnFinished',
      'Text',
      'TurnFinished',
      'Completed',
      'RunSettled',
    ]);
  });

  it('is delivered once, not to every later run', async () => {
    const scripted = model();

    const written = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'steer',
          text: 'keep going',
          source: 'operator',
        }).pipe(Effect.orDie);

        yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
        yield* agent.recordingTo(CONVERSATION).run('again').pipe(Effect.orDie);
        return yield* readAll();
      }),
      scripted,
    );

    // Two turns in the first run, one in the second: the second run resumed
    // draining past what the first recorded taking, rather than re-reading
    // the signal stream from the beginning.
    expect(scripted.prompts).toHaveLength(3);
    expect(received(written)).toHaveLength(1);
  });

  it('is visible to a consumer of the event stream', async () => {
    const scripted = model();

    const observed = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'steer',
          text: 'keep going',
          source: 'operator',
        }).pipe(Effect.orDie);

        return yield* agent
          .recordingTo(CONVERSATION)
          .stream('hi')
          .pipe(
            Stream.filter((event) => event._tag === 'Signalled'),
            Stream.runCollect,
            Effect.orDie,
          );
      }),
      scripted,
    );

    expect(observed).toMatchObject([
      { _tag: 'Signalled', step: 1, kind: 'steer', text: 'keep going' },
    ]);
  });
});

describe('cancelling', () => {
  // The agent that shows cancel doing something the stop condition would not:
  // this one is told to keep going for five steps, so anything that ends it
  // sooner ended it because it was told to.
  const persistent = Agent.make({
    name: 'persistent',
    instructions: 'keep going',
    toolkit: Toolkit.make(),
    stopWhen: Stop.maxSteps(5),
  });

  it('ends the run at the turn boundary, without another model call', async () => {
    const scripted = model();

    const result = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'cancel',
          text: 'user closed the tab',
          source: 'ui',
        }).pipe(Effect.orDie);

        return yield* persistent
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
      }),
      scripted,
    );

    expect(scripted.prompts).toHaveLength(1);
    // Cancellation ends a run; it does not fail one. The work already done
    // comes back rather than being thrown away with the fiber.
    expect(result.text).toBe('turn 1');
    expect(result.steps).toBe(1);
  });

  it('leaves the run alone when nobody cancelled it', async () => {
    const scripted = model();

    const result = await run(
      persistent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie),
      scripted,
    );

    // The control for the case above: five steps, because the stop condition
    // is what ends this agent and it was not asked to stop early.
    expect(result.steps).toBe(5);
  });

  it('settles the run as cancelled', async () => {
    const scripted = model();

    const written = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'cancel',
          text: 'user closed the tab',
          source: 'ui',
        }).pipe(Effect.orDie);

        yield* agent.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
        return yield* readAll();
      }),
      scripted,
    );

    expect(settlement(written)).toMatchObject([{ outcome: 'cancelled' }]);
  });

  it('outranks a steer delivered in the same batch', async () => {
    const scripted = model();

    const result = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'steer',
          text: 'keep going',
          source: 'operator',
        }).pipe(Effect.orDie);
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'cancel',
          text: 'never mind',
          source: 'ui',
        }).pipe(Effect.orDie);

        return yield* agent
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);
      }),
      scripted,
    );

    expect(result.steps).toBe(1);
  });
});

// Everything above sends its signal before the run exists, which is the easy
// half: the signal stream is already there when the session opens, and the
// very first drain finds it. The production case is the other one — an
// operator types into a thread while the agent is mid-tool-call — and it
// exercises paths the pre-seeded tests cannot reach:
//
//   - the first drain runs against a signal stream that **does not exist yet**
//     and must come back empty rather than dying, and the cursor it leaves
//     behind must still be usable by the drain that follows;
//   - the drain happens at *every* turn boundary, not once at the start;
//   - the cursor advances *within* one run, so a steer taken at one boundary
//     is not taken again at the next.
//
// The signal is sent from a tool handler, because that is where a run
// genuinely is when somebody steers it: inside a turn, with the model waiting.

const CALL = (id: string, name: string): Response.StreamPartEncoded => ({
  type: 'tool-call',
  id,
  name,
  params: {},
});

/**
 * How many times an instruction appears in a rebuilt prompt.
 *
 * Not `toContain`: a prompt is the whole conversation so far, so a steer
 * delivered at turn 2 is legitimately still in the prompt at turn 4. The
 * failure worth catching is a *second copy* of it, which is what a cursor that
 * did not advance produces — and which `toContain` cannot see.
 */
const occurrences = (haystack: string, needle: string): number =>
  haystack.split(needle).length - 1;

const work = Tool.make('work', {
  description: 'do some work',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ ok: Schema.Boolean }),
  // The handler sends a signal, which needs the store. Declared rather than
  // captured, so the requirement is visible on the tool the way any other
  // service-using tool declares it.
  dependencies: [LogStore.Service],
});

/**
 * A model that calls `work` for the first `toolTurns` turns, then talks.
 *
 * The tool calls are what keep the loop going without a steer: the default
 * stop condition is "no tool calls", so every turn that asks for one continues
 * on its own merits and nothing here is measuring the steer's stop-override.
 * What is being measured is the *content* of the next prompt, and when the run
 * ends.
 */
const workThenTalk = (toolTurns: number): Model => {
  const prompts: string[] = [];
  return {
    prompts,
    layer: Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const calls = yield* Ref.make(0);
        return yield* LanguageModel.make({
          generateText: () =>
            Effect.succeed<Response.PartEncoded[]>([finish()]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
                prompts.push(JSON.stringify(options.prompt));
                return Stream.fromIterable(
                  index < toolTurns
                    ? [
                        CALL(`call-${index + 1}`, 'work'),
                        { ...finish(), reason: 'tool-calls' as const },
                      ]
                    : says(`turn ${index + 1}`),
                );
              }),
            ),
        });
      }),
    ),
  };
};

/**
 * An agent whose tool sends signals to its own conversation while it runs.
 *
 * `sendOn` is keyed by turn so a test can put a signal at a chosen boundary
 * without the model script and the signal script having to agree twice.
 */
const signallingAgent = (sendOn: ReadonlyMap<number, AgentSignals.Signal>) => {
  const turns = { count: 0 };
  return Agent.make({
    name: 'test',
    instructions: 'be terse',
    toolkit: Toolkit.make(work),
    // The default policy with a lower ceiling: a turn that asks for a tool
    // continues, a turn that does not stops, and nothing runs away.
    stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(8)),
  }).withHandlers({
    work: () =>
      Effect.gen(function* () {
        turns.count += 1;
        const signal = sendOn.get(turns.count);
        if (signal !== undefined) {
          yield* AgentSignals.send(CONVERSATION, signal).pipe(Effect.orDie);
        }
        return { ok: true };
      }),
  });
};

describe('a signal that arrives while the run is in flight', () => {
  it('is delivered at the next turn boundary, not only at the first', async () => {
    const scripted = workThenTalk(2);

    const written = await run(
      Effect.gen(function* () {
        yield* signallingAgent(
          new Map([
            [
              2,
              {
                kind: 'steer',
                text: 'also check the invoice',
                source: 'operator',
              } satisfies AgentSignals.Signal,
            ],
          ]),
        )
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);

        return yield* readAll();
      }),
      scripted,
    );

    // Turn 1's boundary drained a signal stream that did not exist yet and
    // survived it; turn 2's tool sent one; turn 3 is where it lands.
    expect(scripted.prompts).toHaveLength(3);
    expect(occurrences(scripted.prompts[0]!, 'also check the invoice')).toBe(0);
    expect(occurrences(scripted.prompts[1]!, 'also check the invoice')).toBe(0);
    expect(occurrences(scripted.prompts[2]!, 'also check the invoice')).toBe(1);

    expect(received(written)).toMatchObject([
      { kind: 'steer', text: 'also check the invoice', step: 2 },
    ]);
  });

  it('ends the run at the boundary when it is a cancel', async () => {
    const scripted = workThenTalk(4);

    const result = await run(
      signallingAgent(
        new Map([
          [
            1,
            {
              kind: 'cancel',
              text: 'user closed the tab',
              source: 'ui',
            } satisfies AgentSignals.Signal,
          ],
        ]),
      )
        .recordingTo(CONVERSATION)
        .run('hi')
        .pipe(Effect.orDie),
      scripted,
    );

    // The control is in the turn script: this model asks for a tool on the
    // first four turns, so the stop condition cannot fire at step 1 and the
    // step ceiling is 6. One step means the cancel ended it.
    expect(result.steps).toBe(1);
    expect(scripted.prompts).toHaveLength(1);
  });

  it('leaves the run alone when nothing is sent mid-flight', async () => {
    const scripted = workThenTalk(4);

    const result = await run(
      signallingAgent(new Map())
        .recordingTo(CONVERSATION)
        .run('hi')
        .pipe(Effect.orDie),
      scripted,
    );

    // The other half of the control above: four tool turns then an answer, so
    // an unsignalled run of this agent takes five steps.
    expect(result.steps).toBe(5);
  });

  // The cursor is per-run state advanced by each drain. Not advancing it — or
  // advancing it from the wrong page — re-delivers a steer the agent has
  // already acted on, at every remaining boundary of the same run.
  it('advances its cursor within one run, so no steer is taken twice', async () => {
    const scripted = workThenTalk(3);

    const written = await run(
      Effect.gen(function* () {
        yield* signallingAgent(
          new Map([
            [
              1,
              {
                kind: 'steer',
                text: 'first instruction',
                source: 'operator',
              } satisfies AgentSignals.Signal,
            ],
            [
              3,
              {
                kind: 'steer',
                text: 'second instruction',
                source: 'operator',
              } satisfies AgentSignals.Signal,
            ],
          ]),
        )
          .recordingTo(CONVERSATION)
          .run('hi')
          .pipe(Effect.orDie);

        return yield* readAll();
      }),
      scripted,
    );

    expect(received(written).map((record) => record.text)).toEqual([
      'first instruction',
      'second instruction',
    ]);

    // Four turns: tool, tool, tool, answer. Each instruction reaches the turn
    // after it was sent and appears exactly once thereafter — a second copy is
    // what a cursor stuck at its starting offset would produce.
    expect(scripted.prompts).toHaveLength(4);
    expect(occurrences(scripted.prompts[0]!, 'first instruction')).toBe(0);
    expect(occurrences(scripted.prompts[1]!, 'first instruction')).toBe(1);
    expect(occurrences(scripted.prompts[2]!, 'first instruction')).toBe(1);
    expect(occurrences(scripted.prompts[3]!, 'first instruction')).toBe(1);
    expect(occurrences(scripted.prompts[3]!, 'second instruction')).toBe(1);
  });

  // A run that took a steer records how far it drained. The next run has to
  // start from there and no earlier, or the instruction is injected a second
  // time into a conversation that already followed it.
  it('does not re-deliver a mid-run steer to the run after it', async () => {
    const scripted = workThenTalk(1);

    const written = await run(
      Effect.gen(function* () {
        const sending = signallingAgent(
          new Map([
            [
              1,
              {
                kind: 'steer',
                text: 'mid-run instruction',
                source: 'operator',
              } satisfies AgentSignals.Signal,
            ],
          ]),
        );

        yield* sending.recordingTo(CONVERSATION).run('hi').pipe(Effect.orDie);
        yield* sending
          .recordingTo(CONVERSATION)
          .run('again')
          .pipe(Effect.orDie);
        return yield* readAll();
      }),
      scripted,
    );

    expect(received(written)).toHaveLength(1);
  });
});

describe('an agent that is not recording', () => {
  it('has no conversation to signal, and is unaffected by one', async () => {
    const scripted = model();

    const result = await run(
      Effect.gen(function* () {
        yield* AgentSignals.send(CONVERSATION, {
          kind: 'cancel',
          text: 'never mind',
          source: 'ui',
        }).pipe(Effect.orDie);

        return yield* agent.run('hi').pipe(Effect.orDie);
      }),
      scripted,
    );

    // Signals are addressed to a conversation, and a run only has one when it
    // is recording. This is a real limit, not an oversight: without an
    // identity there is nothing for a sender to address.
    expect(result.text).toBe('turn 1');
    expect(scripted.prompts).toHaveLength(1);
  });
});
