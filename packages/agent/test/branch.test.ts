import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { beforeEach, describe, expect, it } from '@effect/vitest';
import { Effect, Layer, Ref, Schema, Stream, type Crypto } from 'effect';
import {
  LanguageModel,
  Prompt,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { AgentBranch } from '../src/branch.js';
import { Conversation } from '../src/conversation.js';
import { AgentHistory } from '../src/history.js';
import { AgentHistory as AgentHistoryRuntime } from '../src/internal/history.js';
import * as AgentLog from '../src/log.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

// Branching: the conversation as a tree, in a log that is a line.
//
// The capability is "edit an earlier message and re-run". What has to be true
// for it to be worth having, and what each block below is here to prove:
//
//   - the rebuilt prompt follows the new path and the abandoned one is gone
//     from it — otherwise the edit did nothing;
//   - a compaction on the abandoned branch does not replace the active path's
//     history — a summary of a conversation that no longer happened is worse
//     than no summary at all;
//   - **a steer delivered before the branch is not delivered again after it**
//     — the failure this design is most likely to be implemented into, because
//     the tidy refactor (filter to the path once, at the source) causes it
//     silently, and the symptom is an agent being told twice to do something
//     it already did;
//   - tool outcomes a crashed run recorded on the abandoned branch are not
//     served back to the branched run, which never made those calls;
//   - usage still counts what the abandoned branch cost, because it was spent.
//
// The last three are the rows of `branch.ts`'s table that are not obvious, and
// each of them fails if the corresponding fold is scoped the other way. See
// each test for the mutation that breaks it.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const present = <A>(value: A | undefined): A => {
  if (value === undefined) {
    throw new Error('Expected test fixture value to be present');
  }
  return value;
};

const says = (body: string): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id: body },
  { type: 'text-delta' as const, id: body, delta: body },
  { type: 'text-end' as const, id: body },
  finish(),
];

/** A turn that calls the tool and then waits for its result. */
const callingTurn: Response.StreamPartEncoded[] = [
  { type: 'text-start' as const, id: 'a' },
  { type: 'text-delta' as const, id: 'a', delta: 'looking' },
  { type: 'text-end' as const, id: 'a' },
  {
    type: 'tool-call' as const,
    id: 'call-1',
    name: 'lookup',
    params: { id: '42' },
  },
  finish('tool-calls'),
];

/**
 * A provider that keeps every prompt it was handed and replies from a script.
 *
 * The prompts are the evidence throughout this file: "the abandoned branch is
 * not in the prompt" is a string that is absent from a value the test can
 * read, not an assertion about the implementation.
 */
const provider = (
  turns: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
) => {
  const asked: Array<Prompt.Prompt> = [];

  const layer = Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              asked.push(options.prompt);
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              return Stream.fromIterable(
                present(turns[Math.min(index, turns.length - 1)]),
              );
            }),
          ),
      });
    }),
  );

  return { asked, layer };
};

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});

/** Counts handler invocations, so "the tool ran again" is a number. */
const dispatched = { count: 0 };

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(lookup),
}).withHandlers({
  lookup: ({ id }) =>
    Effect.sync(() => {
      dispatched.count += 1;
      return { status: `shipped:${id}` };
    }),
});

const CONVERSATION = LogVocabulary.ConversationId.make('branched-conversation');
const conversation = Conversation.make(agent, CONVERSATION);
const PATH = AgentLog.pathFor(CONVERSATION);

beforeEach(() => {
  dispatched.count = 0;
});

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  models: Layer.Layer<LanguageModel.LanguageModel>,
) =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(Layer.merge(models, testLogLayer)),
    Effect.scoped,
  );

const readAll = Effect.fn('test.readAll')(function* () {
  return Array.from(
    yield* conversation.records().pipe(Stream.runCollect, Effect.orDie),
  );
});

const tags = (records: ReadonlyArray<ConversationRecord.Envelope>) =>
  records.map((envelope) => envelope.record._tag);

const textIn = (prompt: Prompt.Prompt): string =>
  JSON.stringify(prompt.content);

const rolesOf = (prompt: Prompt.Prompt) =>
  prompt.content.map((message) => message.role);

/**
 * Envelopes at real, orderable offsets.
 *
 * `LogOffset.fromSeq` rather than the index as a string: the walk compares
 * offsets as bytes, and unpadded decimals stop ordering correctly at ten
 * records — which is exactly the size at which a branching test stops being
 * trivial.
 */
const envelopes = (
  records: ReadonlyArray<ConversationRecord.Record>,
): ReadonlyArray<ConversationRecord.Envelope> =>
  records.map((record, index) => ({
    offset: LogOffset.fromSeq(BigInt(index)),
    conversationId: CONVERSATION,
    timestamp: 0,
    record,
  }));

const at = (index: number) => LogOffset.fromSeq(BigInt(index));

const started = (text: string): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  agent: 'test',
  formatVersion: 1,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: Prompt.make(text).content,
});

const said = (text: string): ConversationRecord.Record => ({
  _tag: 'Text',
  step: 1,
  text,
});

const turn: ConversationRecord.Record = {
  _tag: 'TurnFinished',
  step: 1,
  usage: { input: 1, output: 1 },
};

describe('the active path', () => {
  it('is every record, when nothing branched', () => {
    const records = envelopes([started('hi'), said('there'), turn]);
    expect(AgentBranch.activePath(records)).toEqual(records);
  });

  it('drops the marker and everything it branched away from', () => {
    const path = AgentBranch.activePath(
      envelopes([
        started('hi'), // 0
        said('kept'), // 1
        said('abandoned'), // 2
        { _tag: 'BranchedFrom', at: at(1) }, // 3
        said('new'), // 4
      ]),
    );

    expect(tags(path)).toEqual(['RunStarted', 'Text', 'Text']);
    expect(
      path.flatMap((envelope) =>
        envelope.record._tag === 'Text' ? [envelope.record.text] : [],
      ),
    ).toEqual(['kept', 'new']);
  });

  // A branch off a branch. The second marker jumps over the first one's
  // records *and* the first marker, and the walk keeps going backwards from
  // wherever it lands rather than restarting.
  it('follows a branch taken from inside an abandoned range', () => {
    const path = AgentBranch.activePath(
      envelopes([
        started('hi'), // 0
        said('one'), // 1
        said('two'), // 2
        { _tag: 'BranchedFrom', at: at(1) }, // 3 — abandons 'two'
        said('three'), // 4
        { _tag: 'BranchedFrom', at: at(2) }, // 5 — returns to 'two'
        said('four'), // 6
      ]),
    );

    expect(
      path.flatMap((envelope) =>
        envelope.record._tag === 'Text' ? [envelope.record.text] : [],
      ),
    ).toEqual(['one', 'two', 'four']);
  });

  // The pointer may name a record that is not here — a trimmed log, or a
  // hand-written marker. Resuming at the last record before it is the same
  // tolerance `AgentHistory` shows a `firstKept` it cannot find.
  it('resumes at the last record before a pointer that names none', () => {
    const path = AgentBranch.activePath([
      ...envelopes([started('hi'), said('one')]),
      {
        offset: LogOffset.fromSeq(50n),
        conversationId: CONVERSATION,
        timestamp: 0,
        record: { _tag: 'BranchedFrom', at: LogOffset.fromSeq(20n) },
      },
    ]);

    expect(tags(path)).toEqual(['RunStarted', 'Text']);
  });

  it('is empty when the branch point is before every record', () => {
    const path = AgentBranch.activePath(
      envelopes([
        started('hi'),
        said('one'),
        { _tag: 'BranchedFrom', at: LogOffset.START },
      ]),
    );

    expect(path).toEqual([]);
  });

  // These records come out of a database. A marker naming its own offset, or a
  // later one, must degenerate into an ordinary step backwards — a walk that
  // could revisit a record could hang a tail, which is worse than a wrong
  // answer because nothing reports it.
  it('terminates on a marker that points at or after itself', () => {
    expect(
      tags(
        AgentBranch.activePath(
          envelopes([
            started('hi'),
            said('one'),
            { _tag: 'BranchedFrom', at: at(2) },
            said('two'),
            { _tag: 'BranchedFrom', at: at(99) },
          ]),
        ),
      ),
    ).toEqual(['RunStarted', 'Text', 'Text']);
  });
});

describe('rebuilding a branched conversation', () => {
  // The point of the whole exercise: what the model is shown follows the new
  // path. Mutation-checked — making `messagesFrom` skip `activePath` puts
  // 'abandoned' back into the prompt and fails this.
  it('excludes the abandoned branch from the prompt', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('what is the status?'), // 0
        said('abandoned answer'), // 1
        turn, // 2
        { _tag: 'BranchedFrom', at: at(0) }, // 3
        started('what is the status, precisely?'), // 4
        said('new answer'), // 5
        turn, // 6
      ]),
    );

    expect(textIn(rebuilt)).toContain('what is the status?');
    expect(textIn(rebuilt)).toContain('new answer');
    expect(textIn(rebuilt)).not.toContain('abandoned answer');
    expect(rolesOf(rebuilt)).toEqual(['user', 'user', 'assistant']);
  });

  // "The latest compaction wins" has to mean the latest one *on this path*. A
  // compaction the conversation branched away from summarized a history that,
  // from the active path's point of view, never happened — replaying its
  // summary would state a conversation nobody had.
  //
  // Mutation-checked: reverting `rebuild` to fold the whole log puts
  // 'summary of the abandoned turn' into the prompt and drops the verbatim
  // records before it.
  it('ignores a compaction that happened on the abandoned branch', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('first question'), // 0
        said('first answer'), // 1
        turn, // 2
        {
          _tag: 'Compacted',
          step: 1,
          summary: 'summary of the abandoned turn',
          firstKept: LogOffset.START,
          summarizedMessages: 2,
          keptMessages: 0,
        }, // 3
        said('abandoned'), // 4
        { _tag: 'BranchedFrom', at: at(2) }, // 5
        said('new answer'), // 6
        turn, // 7
      ]),
    );

    expect(textIn(rebuilt)).not.toContain('summary of the abandoned turn');
    expect(textIn(rebuilt)).toContain('first question');
    expect(textIn(rebuilt)).toContain('first answer');
    expect(textIn(rebuilt)).toContain('new answer');
  });

  // The control for the case above: a compaction that *is* on the path still
  // replaces the history before it. Without this, "ignore the abandoned
  // compaction" could be implemented as "ignore every compaction" and the
  // suite would not notice.
  it('still honours a compaction on the active path', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('first question'), // 0
        said('abandoned'), // 1
        { _tag: 'BranchedFrom', at: at(0) }, // 2
        said('first answer'), // 3
        turn, // 4
        {
          _tag: 'Compacted',
          step: 1,
          summary: 'summary of everything so far',
          firstKept: LogOffset.START,
          summarizedMessages: 2,
          keptMessages: 0,
        }, // 5
        said('after the summary'), // 6
        turn, // 7
      ]),
    );

    expect(textIn(rebuilt)).toContain('summary of everything so far');
    expect(textIn(rebuilt)).toContain('after the summary');
    expect(textIn(rebuilt)).not.toContain('first question');
    expect(textIn(rebuilt)).not.toContain('abandoned');
  });

  // A compaction boundary has to name a record the reader can still find.
  // `compactionBoundary` counts back from the end of the rebuilt history, so a count
  // resolved against the whole log lands a `firstKept` on the abandoned
  // branch — an offset `messagesFrom` will never return, which it then falls
  // back from by keeping nothing.
  //
  // Mutation-checked: dropping `activePath` from `compactionBoundary` returns the
  // abandoned `Text`'s offset and fails both assertions.
  it.effect('resolves a compaction boundary onto the active path', () =>
    Effect.gen(function* () {
      const records = envelopes([
        started('one'), // 0
        said('a'), // 1
        turn, // 2
        started('two'), // 3
        said('abandoned'), // 4
        turn, // 5
        { _tag: 'BranchedFrom', at: at(3) }, // 6
        said('redone'), // 7
        turn, // 8
      ]);

      // The path rebuilds as [user 'one', assistant 'a', user 'two', assistant
      // 'redone']; the last two of those start at the second `RunStarted`.
      const boundary = yield* AgentHistoryRuntime.compactionBoundary(records, {
        summarizedMessages: 2,
        keptMessages: 2,
      });
      expect(boundary).toBe(at(3));
      expect(
        AgentBranch.activePath(records).map((envelope) => envelope.offset),
      ).toContain(boundary);
    }),
  );

  // Money spent on a branch that was abandoned was still spent. Filtering here
  // would make a conversation's reported cost fall when its user changed their
  // mind. Mutation-checked: scoping `usageFrom` to the active path drops the
  // abandoned run's tokens and fails this.
  it('still counts what the abandoned branch cost', () => {
    const usage = AgentHistoryRuntime.usageFrom(
      envelopes([
        started('one'),
        { _tag: 'TurnFinished', step: 1, usage: { input: 7, output: 3 } },
        {
          _tag: 'RunSettled',
          outcome: 'success',
          detail: '',
          steps: 1,
          usage: { input: 7, output: 3 },
        },
        { _tag: 'BranchedFrom', at: at(0) },
        started('two'),
        { _tag: 'TurnFinished', step: 1, usage: { input: 5, output: 2 } },
        {
          _tag: 'RunSettled',
          outcome: 'success',
          detail: '',
          steps: 1,
          usage: { input: 5, output: 2 },
        },
      ]),
    );

    expect(usage).toEqual({ input: 12, output: 5 });
  });
});

describe('branching a live conversation', () => {
  it.effect(
    'writes one marker and keeps the abandoned records in the log',
    () => {
      const models = provider([says('turn 1'), says('turn 2')]);

      return run(
        Effect.gen(function* () {
          yield* conversation.run('hi').pipe(Effect.orDie);
          const first = yield* readAll();

          yield* conversation
            .branchFrom(present(first[0]).offset, 'actually, hello')
            .pipe(Effect.orDie);

          return yield* readAll();
        }),
        models.layer,
      ).pipe(
        Effect.tap((written) =>
          Effect.sync(() => {
            // Nothing was rewritten or removed — the first run's records are all still
            // there, with the marker and the new run appended after them.
            expect(tags(written)).toEqual([
              'RunStarted',
              'Text',
              'TurnFinished',
              'Completed',
              'RunSettled',
              'BranchedFrom',
              'RunStarted',
              'Text',
              'TurnFinished',
              'Completed',
              'RunSettled',
            ]);
          }),
        ),
      );
    },
  );

  it.effect(
    'runs the branched turn against the earlier point, not the end',
    () => {
      const models = provider([says('turn 1'), says('turn 2')]);

      return run(
        Effect.gen(function* () {
          yield* conversation.run('what is the status?').pipe(Effect.orDie);
          const first = yield* readAll();

          yield* conversation
            .branchFrom(
              present(first[0]).offset,
              'what is the status, precisely?',
            )
            .pipe(Effect.orDie);

          return models.asked;
        }),
        models.layer,
      ).pipe(
        Effect.tap((asked) =>
          Effect.sync(() => {
            const branched = present(asked[1]);
            expect(rolesOf(branched)).toEqual(['system', 'user', 'user']);
            expect(textIn(branched)).toContain('what is the status?');
            expect(textIn(branched)).toContain('precisely');
            // The answer the first run gave is on the abandoned branch.
            expect(textIn(branched)).not.toContain('turn 1');
          }),
        ),
      );
    },
  );

  it.effect('carries on from the branch on the next resume', () => {
    const models = provider([says('turn 1'), says('turn 2'), says('turn 3')]);

    return run(
      Effect.gen(function* () {
        yield* conversation.run('original').pipe(Effect.orDie);
        const first = yield* readAll();

        yield* conversation
          .branchFrom(present(first[0]).offset, 'edited')
          .pipe(Effect.orDie);
        yield* conversation.run('and then?').pipe(Effect.orDie);

        return models.asked;
      }),
      models.layer,
    ).pipe(
      Effect.tap((asked) =>
        Effect.sync(() => {
          // The branch is where the conversation continues from now: an ordinary
          // `resume` afterwards sees the edited turn and not the original one.
          const resumed = present(asked[2]);
          expect(textIn(resumed)).toContain('edited');
          expect(textIn(resumed)).toContain('turn 2');
          expect(textIn(resumed)).not.toContain('turn 1');
        }),
      ),
    );
  });

  // Cost is a fact about what was billed, not about what the conversation now
  // says. Mutation-checked end to end: scoping `usageFrom` to the active path
  // makes the branched run report only its own tokens.
  it.effect('reports usage across the branch it abandoned', () => {
    const models = provider([says('turn 1'), says('turn 2')]);

    return run(
      Effect.gen(function* () {
        const first = yield* conversation.run('original').pipe(Effect.orDie);
        const records = yield* readAll();

        const branched = yield* conversation
          .branchFrom(present(records[0]).offset, 'edited')
          .pipe(Effect.orDie);

        return { first: first.usage, branched: branched.usage };
      }),
      models.layer,
    ).pipe(
      Effect.tap((totals) =>
        Effect.sync(() => {
          expect(totals.branched.output).toBe(totals.first.output * 2);
          expect(totals.branched.input).toBe(totals.first.input * 2);
        }),
      ),
    );
  });
});

describe('a steer delivered before the branch', () => {
  // The one that matters most, and the one the obvious refactor breaks.
  //
  // A `SignalReceived` says an agent was handed an instruction and acted on
  // it. Branching away from the turn that took it does not un-hand it. If the
  // delivery cursor were scoped to the active path, this branch would rewind
  // it past the record, re-drain the same signal from the signal stream, and
  // steer the agent a second time with something it already did.
  //
  // Mutation-checked: changing `deliveredThrough(history)` in `log.ts` to
  // `deliveredThrough(AgentBranch.activePath(history))` fails both assertions
  // below — a second `SignalReceived` appears and the branched run takes an
  // extra turn to consume it.
  it.effect('is not delivered again on the new path', () => {
    const models = provider([says('turn 1'), says('turn 2'), says('turn 3')]);

    return run(
      Effect.gen(function* () {
        yield* conversation
          .send({
            kind: 'steer',
            text: 'also check the invoice',
            source: 'operator',
          })
          .pipe(Effect.orDie);

        yield* conversation.run('hi').pipe(Effect.orDie);
        const first = yield* readAll();
        const askedBeforeBranch = models.asked.length;

        yield* conversation
          .branchFrom(present(first[0]).offset, 'try again')
          .pipe(Effect.orDie);

        return {
          askedBeforeBranch,
          asked: models.asked,
          written: yield* readAll(),
        };
      }),
      models.layer,
    ).pipe(
      Effect.tap((observed) =>
        Effect.sync(() => {
          // The steer carried the first run past a stop condition every turn
          // satisfies: two prompts, one delivery.
          expect(observed.askedBeforeBranch).toBe(2);

          // One further prompt for the branched turn, and no second delivery. Three
          // prompts and two deliveries is the re-steer this test exists to catch.
          expect(observed.asked).toHaveLength(3);
          expect(
            observed.written.filter(
              (envelope) => envelope.record._tag === 'SignalReceived',
            ),
          ).toHaveLength(1);
        }),
      ),
    );
  });

  // The control: a steer that arrives *after* the branch is delivered
  // normally. Without this, "never re-deliver" could be implemented as "never
  // deliver to a branched run".
  it.effect('does not stop a later steer from being delivered', () => {
    const models = provider([says('turn 1'), says('turn 2'), says('turn 3')]);

    return run(
      Effect.gen(function* () {
        yield* conversation
          .send({
            kind: 'steer',
            text: 'first steer',
            source: 'operator',
          })
          .pipe(Effect.orDie);
        yield* conversation.run('hi').pipe(Effect.orDie);

        const first = yield* readAll();
        yield* conversation
          .send({
            kind: 'steer',
            text: 'second steer',
            source: 'operator',
          })
          .pipe(Effect.orDie);

        yield* conversation
          .branchFrom(present(first[0]).offset, 'try again')
          .pipe(Effect.orDie);

        return yield* readAll();
      }),
      models.layer,
    ).pipe(
      Effect.tap((written) =>
        Effect.sync(() => {
          expect(
            written.flatMap((envelope) =>
              envelope.record._tag === 'SignalReceived'
                ? [envelope.record.text]
                : [],
            ),
          ).toEqual(['first steer', 'second steer']);
        }),
      ),
    );
  });
});

/**
 * Write a previous run's records straight into the conversation.
 *
 * The same choice `dispatch.test.ts` makes, for the same reason: a crash has
 * to leave the log in an exact state — a `RunStarted` with no `RunSettled` —
 * for these to be testing the recovery gate rather than whatever a killed
 * fiber happens to flush. Running an agent and abandoning its stream does
 * settle the run, so it cannot produce this shape at all.
 */
const seed = Effect.fn('test.seed')(function* (
  records: ReadonlyArray<ConversationRecord.Record>,
) {
  const store = yield* LogStore.Service;
  yield* store.create(PATH, CONVERSATION).pipe(Effect.orDie);
  const claim = yield* store
    .acquire(PATH, LogVocabulary.ProducerId.make('previous-run'))
    .pipe(Effect.orDie);
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

/** A run that started, called a tool, recorded its result, and never settled. */
const crashed: ReadonlyArray<ConversationRecord.Record> = [
  started('where is order 42?'),
  said('looking'),
  {
    _tag: 'ToolCall',
    step: 1,
    id: LogVocabulary.ToolCallId.make('call-1'),
    name: 'lookup',
    params: { id: '42' },
  },
  {
    _tag: 'ToolStarted',
    id: LogVocabulary.ToolCallId.make('call-1'),
    name: 'lookup',
  },
  {
    _tag: 'ToolOutcome',
    step: 1,
    id: LogVocabulary.ToolCallId.make('call-1'),
    name: 'lookup',
    outcome: 'success',
    result: { status: 'from-log' },
  },
];

describe('a crashed run on the abandoned branch', () => {
  // Crash recovery serves a dead run's tool outcomes back to its successor so
  // the tool is not run twice. A branched run is not that successor: its
  // prompt contains none of those calls, so an outcome served into it answers
  // a question nobody asked — and the answer is stale by exactly the edit the
  // user made.
  //
  // Mutation-checked: dropping `AgentBranch.activePath` from the
  // `unsettledTools` call in `log.ts` leaves the handler un-run and puts
  // 'from-log' back into the branched run's prompt.
  it.effect('does not serve its tool outcomes to the branched run', () => {
    const models = provider([callingTurn, says('done')]);

    return run(
      Effect.gen(function* () {
        yield* seed(crashed);
        const before = yield* readAll();

        yield* conversation
          .branchFrom(present(before[0]).offset, 'where is order 43?')
          .pipe(Effect.orDie);

        return { dispatchedTotal: dispatched.count, asked: models.asked };
      }),
      models.layer,
    ).pipe(
      Effect.tap((observed) =>
        Effect.sync(() => {
          // The tool ran, rather than being answered from a branch that is no
          // longer part of the conversation.
          expect(observed.dispatchedTotal).toBe(1);
          expect(textIn(present(observed.asked[1]))).toContain('shipped:42');
          expect(textIn(present(observed.asked[1]))).not.toContain('from-log');
        }),
      ),
    );
  });

  // The control, and the reason the case above is about branching rather than
  // about recovery having stopped working: an ordinary `resume` of the very
  // same seeded crash still serves the recorded outcome and does not re-run
  // the tool.
  it.effect('still serves them to an ordinary resume', () => {
    const models = provider([callingTurn, says('done')]);

    return run(
      Effect.gen(function* () {
        yield* seed(crashed);
        yield* conversation.run('and then?').pipe(Effect.orDie);
        return { dispatchedTotal: dispatched.count, asked: models.asked };
      }),
      models.layer,
    ).pipe(
      Effect.tap((observed) =>
        Effect.sync(() => {
          expect(observed.dispatchedTotal).toBe(0);
          expect(textIn(present(observed.asked[1]))).toContain('from-log');
        }),
      ),
    );
  });
});
