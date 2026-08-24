import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Effect, Layer, Stream, type Crypto } from 'effect';
import { Prompt, Toolkit, type LanguageModel } from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { fakeProvider, turnOf } from './compaction-fixtures.js';
import { AgentHistory } from '../src/history.js';
import { AgentHistory as AgentHistoryRuntime } from '../src/internal/history.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

// Compaction across a resumption — the gap the `Compacted` record used to be.
//
// The defect this pins: a run compacts, the log records that it happened and
// nothing about what it produced, and the next run rebuilds from the full
// record set. The resumed conversation therefore comes back *longer* than the
// run it resumes, overflows on its first turn, and pays for a second summary
// of a history the log already had a summary for.
//
// Nothing here asserts that a conversation is "shorter" or "compacted". The
// provider rejects any prompt over `LIMIT` tokens, exactly as a real one does,
// so whether reconstruction honoured the compaction is observable as a number —
// the count of summarization calls — and as content: the text the summary
// replaced is either in the last prompt or it is not.
//
// This is the reconstruction half of compaction. When one compaction fires and
// what it does to a `Chat` in memory is `compaction.test.ts`; the split is
// along the dependency surface, because everything below needs a log store,
// records and offsets and nothing above needs any of them.
//
// Mutation-checked: making `AgentHistory.messagesFrom` ignore the `Compacted`
// record — `Prompt.fromMessages(fold(records)…)` instead of `rebuild` — fails
// seven of the ten cases here. The three that survive are `compactionBoundary`'s,
// which is the writing half and does not read the record back. The end-to-end
// three fail exactly as the defect described: two `Compacted` records instead
// of one, two summarization calls instead of one, and a final prompt that has
// lost `question two` because the third run compacted it away again.

/** Above this, the fake provider refuses the request as a real one would. */
const LIMIT = 500;

/** Long enough that a prompt still carrying it cannot fit under `LIMIT`. */
const BULK = 'L'.repeat(4000);

const CONVERSATION = LogVocabulary.ConversationId.make('compacted-1');

const POLICY = {
  reserveTokens: 0,
  keepRecentTokens: 10,
  instructions: 'sum',
};

const agent = Agent.make({
  name: 'test',
  revision: '1',
  instructions: 'be terse',
  toolkit: Toolkit.make(),
  compaction: POLICY,
});
const conversation = Conversation.make(agent, CONVERSATION, POLICY);

const run = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  models: Layer.Layer<LanguageModel.LanguageModel>,
): Effect.Effect<A> =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(Layer.merge(models, testLogLayer)),
    Effect.scoped,
  );

const readAll = Effect.fn('test.readAll')(function* () {
  return Array.from(yield* conversation.records().pipe(Stream.runCollect));
});

const textIn = (prompt: Prompt.Prompt): string =>
  JSON.stringify(prompt.content);

const compacted = (
  summary: string,
  firstKept: LogOffset.Offset,
): ConversationRecord.Record => ({
  _tag: 'Compacted',
  step: 1,
  summary,
  firstKept,
  summarizedMessages: 2,
  keptMessages: 1,
});

const started = (prompt: string): ConversationRecord.Record => ({
  _tag: 'RunStarted',
  agent: 'test',
  formatVersion: 1,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: Prompt.make(prompt).content,
});

const said = (step: number, text: string): ConversationRecord.Record => ({
  _tag: 'Text',
  step,
  text,
});

const finished = (step: number): ConversationRecord.Record => ({
  _tag: 'TurnFinished',
  step,
  usage: { input: 1, output: 1 },
});

const rolesOf = (prompt: Prompt.Prompt) =>
  prompt.content.map((message) => message.role);

/**
 * Three runs against one conversation.
 *
 * Run one fills the window. Run two overflows on its first turn, compacts, and
 * finishes. Run three is the one under test: it resumes a conversation that has
 * already been compacted, and what it is handed is the whole question.
 */
const threeRuns = (models: ReturnType<typeof fakeProvider>) =>
  Effect.gen(function* () {
    yield* conversation.run('question one');
    yield* conversation.run('question two');
    const summariesAfterCompacting = models.summaries.length;
    yield* conversation.run('question three');

    return {
      records: yield* readAll(),
      summariesAfterCompacting,
      summariesTotal: models.summaries.length,
      asked: models.asked,
    };
  });

const scripted = () =>
  fakeProvider({
    limit: LIMIT,
    turns: [
      turnOf('a', BULK),
      turnOf('b', 'second answer'),
      turnOf('c', 'third answer'),
    ],
  });

describe('a conversation that compacted, resumed', () => {
  it.effect('aligns proactive compaction with the current resumed input', () =>
    Effect.gen(function* () {
      const proactivePolicy = {
        ...POLICY,
        contextWindow: 100,
        keepRecentTokens: 0,
      };
      const proactiveAgent = Agent.make({
        name: 'proactive',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: proactivePolicy,
      });
      const proactive = Conversation.make(
        proactiveAgent,
        LogVocabulary.ConversationId.make('compacted-proactive'),
        proactivePolicy,
      );
      const models = fakeProvider({
        turns: [turnOf('first', BULK), turnOf('second', 'second answer')],
      });

      const observed = yield* run(
        Effect.gen(function* () {
          yield* proactive.run('question one');
          const second = yield* proactive
            .run('question two')
            .pipe(Effect.result);
          const summariesAfterSecond = models.summaries.length;
          const third = yield* proactive
            .run('question three')
            .pipe(Effect.result);
          const records = Array.from(
            yield* proactive.records().pipe(Stream.runCollect),
          );
          return { second, third, summariesAfterSecond, records };
        }),
        models.layer,
      );

      expect(observed.second).toMatchObject({
        _tag: 'Success',
        success: { text: 'second answer' },
      });
      expect(observed.third).toMatchObject({ _tag: 'Success' });
      expect(observed.summariesAfterSecond).toBe(1);
      expect(models.summaries).toHaveLength(1);
      const lastAsked = models.asked.at(-1);
      if (lastAsked === undefined) {
        throw new Error('missing resumed provider prompt');
      }
      expect(textIn(lastAsked)).toContain('question two');
      expect(textIn(AgentHistory.messagesFrom(observed.records))).toContain(
        'question three',
      );
    }),
  );

  it.effect('compacts again when the summary still exceeds the threshold', () =>
    Effect.gen(function* () {
      const policy = {
        ...POLICY,
        contextWindow: 100,
        keepRecentTokens: 0,
      };
      const verbose = Agent.make({
        name: 'verbose-summary',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
        compaction: policy,
      });
      const verboseConversation = Conversation.make(
        verbose,
        LogVocabulary.ConversationId.make('compacted-verbose-summary'),
        policy,
      );
      const models = fakeProvider({
        summaryText: 'S'.repeat(800),
        turns: [turnOf('first', BULK), turnOf('second', 'answer')],
      });

      yield* run(
        Effect.gen(function* () {
          yield* verboseConversation.run('question one');
          yield* verboseConversation.run('question two');
          yield* verboseConversation.run('question three');
        }),
        models.layer,
      );

      expect(models.summaries).toHaveLength(2);
    }),
  );

  it.effect(
    'records what the summary said and where the kept history starts',
    () =>
      Effect.gen(function* () {
        const models = scripted();
        const observed = yield* run(threeRuns(models), models.layer);

        const compactions = observed.records.filter(
          (
            envelope,
          ): envelope is ConversationRecord.Envelope & {
            readonly record: ConversationRecord.RecordOf<'Compacted'>;
          } => envelope.record._tag === 'Compacted',
        );
        expect(compactions).toHaveLength(1);

        const firstCompaction = compactions.at(0);
        if (firstCompaction === undefined) {
          throw new Error('missing compaction record');
        }
        const record = firstCompaction.record;
        expect(record.summary).toBe('SUMMARY');
        expect(record.summarizedMessages).toBe(2);
        expect(record.keptMessages).toBe(1);

        // The pointer names the record that opened the kept tail — the second
        // run's `RunStarted`, which is where `question two` entered the
        // conversation. Not a count, and not the compaction record itself.
        const runStarts = observed.records.filter(
          (envelope) => envelope.record._tag === 'RunStarted',
        );
        const secondRun = runStarts.at(1);
        if (secondRun === undefined) {
          throw new Error('missing second run');
        }
        expect(record.firstKept).toBe(secondRun.offset);
        expect(record.firstKept).not.toBe(LogOffset.START);

        const completions = observed.records.filter(
          (envelope) => envelope.record._tag === 'Completed',
        );
        expect(completions[1]?.record).toMatchObject({
          usage: { input: 20, output: 8 },
        });
      }),
  );

  it.effect(
    'does not hand the resumed run the history compaction replaced',
    () =>
      Effect.gen(function* () {
        const models = scripted();
        const observed = yield* run(threeRuns(models), models.layer);

        const lastAsked = observed.asked.at(-1);
        if (lastAsked === undefined) {
          throw new Error('missing final prompt');
        }
        const last = textIn(lastAsked);

        // The summary is there, in the words the compacted `Chat` used.
        expect(last).toContain('SUMMARY');
        expect(last).toContain('Summary of earlier conversation');
        // The kept tail is there verbatim, and so is everything after it.
        expect(last).toContain('question two');
        expect(last).toContain('second answer');
        expect(last).toContain('question three');
        // And the replaced history is not.
        expect(last).not.toContain('question one');
        expect(last).not.toContain(BULK);
      }),
  );

  it.effect('compacts once for the conversation, not once per resumption', () =>
    Effect.gen(function* () {
      const models = scripted();
      const observed = yield* run(threeRuns(models), models.layer);

      expect(observed.summariesAfterCompacting).toBe(1);
      expect(observed.summariesTotal).toBe(1);

      // And the third run's first prompt fit, so it was never retried: one
      // stream call for run one, two for run two (the overflow and the retry),
      // one for run three.
      expect(observed.asked).toHaveLength(4);
    }),
  );
});

describe('rebuilding a compacted conversation', () => {
  const envelopes = (
    records: ReadonlyArray<ConversationRecord.Record>,
  ): ReadonlyArray<ConversationRecord.Envelope> =>
    records.map((record, index) => ({
      offset: LogOffset.fromSeq(BigInt(index)),
      conversationId: CONVERSATION,
      timestamp: 0,
      record,
    }));

  it('replaces everything before the pointer with the summary', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('old'),
        said(1, 'older answer'),
        finished(1),
        started('kept'),
        compacted('WHAT HAPPENED', LogOffset.fromSeq(3n)),
        said(1, 'after'),
        finished(1),
      ]),
    );

    expect(rolesOf(rebuilt)).toEqual(['user', 'user', 'assistant']);
    const body = JSON.stringify(rebuilt.content);
    expect(body).toContain('WHAT HAPPENED');
    expect(body).toContain('kept');
    expect(body).toContain('after');
    expect(body).not.toContain('older answer');
    expect(body).not.toContain('"old"');
  });

  // A long conversation compacts repeatedly, and each summary already
  // subsumes the one before it — the earlier summary was an ordinary user
  // message in the history the later one summarized.
  it('reads the latest compaction and not the ones it superseded', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('old'),
        compacted('FIRST SUMMARY', LogOffset.START),
        said(1, 'middle'),
        finished(1),
        compacted('SECOND SUMMARY', LogOffset.fromSeq(2n)),
        said(2, 'latest'),
        finished(2),
      ]),
    );

    const body = JSON.stringify(rebuilt.content);
    expect(body).toContain('SECOND SUMMARY');
    expect(body).not.toContain('FIRST SUMMARY');
    expect(body).toContain('middle');
    expect(body).toContain('latest');
  });

  // `START` is the exclusive-lower-bound sentinel, so no record is ever
  // written at it — which makes it unambiguous as "nothing before this
  // survived".
  it('keeps only the summary when nothing before it survived', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('old'),
        said(1, 'older answer'),
        finished(1),
        compacted('ALL OF IT', LogOffset.START),
      ]),
    );

    expect(rolesOf(rebuilt)).toEqual(['user']);
    expect(JSON.stringify(rebuilt.content)).toContain('ALL OF IT');
  });

  // A trimmed log, or a pointer written by a version that numbered records
  // differently. Keeping too little is recoverable — the summary describes it
  // — and keeping too much is the bug.
  it('falls back to keeping nothing when the pointer names no record here', () => {
    const rebuilt = AgentHistory.messagesFrom(
      envelopes([
        started('old'),
        said(1, 'older answer'),
        finished(1),
        compacted('ALL OF IT', LogOffset.fromSeq(900n)),
        said(2, 'after'),
        finished(2),
      ]),
    );

    const body = JSON.stringify(rebuilt.content);
    expect(rolesOf(rebuilt)).toEqual(['user', 'assistant']);
    expect(body).toContain('ALL OF IT');
    expect(body).toContain('after');
    expect(body).not.toContain('older answer');
  });

  describe('compactionBoundary', () => {
    const records = envelopes([
      started('one'),
      said(1, 'answer'),
      finished(1),
      started('two'),
    ]);

    // Three messages: user(one), assistant(answer), user(two).
    it.effect('points at the record that opened the last kept message', () =>
      Effect.gen(function* () {
        expect(
          yield* AgentHistoryRuntime.compactionBoundary(records, {
            summarizedMessages: 2,
            keptMessages: 1,
          }),
        ).toBe(LogOffset.fromSeq(3n));
        expect(
          yield* AgentHistoryRuntime.compactionBoundary(records, {
            summarizedMessages: 1,
            keptMessages: 2,
          }),
        ).toBe(LogOffset.fromSeq(1n));
        expect(
          yield* AgentHistoryRuntime.compactionBoundary(records, {
            summarizedMessages: 0,
            keptMessages: 3,
          }),
        ).toBe(LogOffset.fromSeq(0n));
      }),
    );

    it.effect('is START when nothing was kept', () =>
      Effect.gen(function* () {
        expect(
          yield* AgentHistoryRuntime.compactionBoundary(records, {
            summarizedMessages: 3,
            keptMessages: 0,
          }),
        ).toBe(LogOffset.START);
        expect(
          yield* AgentHistoryRuntime.compactionBoundary([], {
            summarizedMessages: 0,
            keptMessages: 0,
          }),
        ).toBe(LogOffset.START);
      }),
    );

    it.effect(
      'fails instead of clamping when live and durable history drift',
      () =>
        Effect.gen(function* () {
          const outcome = yield* AgentHistoryRuntime.compactionBoundary(
            records,
            {
              summarizedMessages: 3,
              keptMessages: 1,
            },
          ).pipe(Effect.result);

          expect(outcome).toMatchObject({
            _tag: 'Failure',
            failure: {
              _tag: 'CompactionAlignmentError',
              expectedMessages: 4,
              recordedMessages: 3,
            },
          });
        }),
    );
  });
});
