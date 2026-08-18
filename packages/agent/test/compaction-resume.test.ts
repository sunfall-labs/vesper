import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, type Layer, Stream } from 'effect';
import { LanguageModel, Prompt, Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { fakeProvider, turnOf } from './compaction-fixtures.js';
import { AgentHistory } from '../src/history.js';

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
// seven of the ten cases here. The three that survive are `boundaryFor`'s,
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
const conversation = Conversation.recording(agent, CONVERSATION, POLICY);

const run = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
  models: Layer.Layer<LanguageModel.LanguageModel>,
): Effect.Effect<A> =>
  effect.pipe(
    Effect.orDie,
    Effect.provide(models),
    Effect.provide(LogStoreMemory.layer),
    Effect.scoped,
  );

const readAll = Effect.fn('test.readAll')(function* () {
  return Array.from(yield* conversation.records().pipe(Stream.runCollect));
});

const textIn = (prompt: Prompt.Prompt): string =>
  JSON.stringify(prompt.content);

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
    yield* conversation.resume('question two');
    const summariesAfterCompacting = models.summaries.length;
    yield* conversation.resume('question three');

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
  it.effect(
    'records what the summary said and where the kept history starts',
    () =>
      Effect.gen(function* () {
        const models = scripted();
        const observed = yield* run(threeRuns(models), models.layer);

        const compactions = observed.records.filter(
          (envelope) => envelope.record._tag === 'Compacted',
        );
        expect(compactions).toHaveLength(1);

        const record = compactions[0]!
          .record as ConversationRecord.RecordOf<'Compacted'>;
        expect(record.summary).toBe('SUMMARY');
        expect(record.summarizedMessages).toBe(2);
        expect(record.keptMessages).toBe(1);

        // The pointer names the record that opened the kept tail — the second
        // run's `RunStarted`, which is where `question two` entered the
        // conversation. Not a count, and not the compaction record itself.
        const runStarts = observed.records.filter(
          (envelope) => envelope.record._tag === 'RunStarted',
        );
        expect(record.firstKept).toBe(runStarts[1]!.offset);
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

        const last = textIn(observed.asked[observed.asked.length - 1]!);

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

  describe('boundaryFor', () => {
    const records = envelopes([
      started('one'),
      said(1, 'answer'),
      finished(1),
      started('two'),
    ]);

    // Three messages: user(one), assistant(answer), user(two).
    it('points at the record that opened the last kept message', () => {
      expect(AgentHistory.boundaryFor(records, 1)).toBe(LogOffset.fromSeq(3n));
      expect(AgentHistory.boundaryFor(records, 2)).toBe(LogOffset.fromSeq(1n));
      expect(AgentHistory.boundaryFor(records, 3)).toBe(LogOffset.fromSeq(0n));
    });

    it('clamps a tail longer than the conversation', () => {
      expect(AgentHistory.boundaryFor(records, 99)).toBe(LogOffset.fromSeq(0n));
    });

    it('is START when nothing was kept', () => {
      expect(AgentHistory.boundaryFor(records, 0)).toBe(LogOffset.START);
      expect(AgentHistory.boundaryFor([], 4)).toBe(LogOffset.START);
    });
  });
});
