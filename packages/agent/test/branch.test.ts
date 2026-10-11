import { LogOffset } from '@sunfall/vesper-log/offset';
import { describe, expect, it } from '@effect/vitest';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect } from 'effect';
import { Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { fakeProvider } from './compaction-fixtures.js';
import {
  callsLookup,
  lookupAgent,
  lookupCall,
  lookupOutcome,
  lookupStarted,
  plainAgent,
  present,
  provider,
  readRecords,
  rolesOf,
  run,
  said,
  says,
  seed,
  settled,
  started,
  tags,
  textIn,
  turn,
} from './durability-fixtures.js';

// Branching: the conversation as a tree, in a log that is a line.
//
// The capability is "edit an earlier message and re-run". What has to be true
// for it to be worth having:
//
//   - the prompt follows the new path and the abandoned one is gone from it;
//   - a compaction on the abandoned branch does not replace the active path's
//     history (a summary of a conversation that no longer happened is worse
//     than no summary), while one on the active path still does;
//   - a steer delivered before the branch is not delivered again after it;
//   - tool outcomes a crashed run recorded on the abandoned branch are not
//     served to the branched run, which never made those calls;
//   - usage still counts what the abandoned branch cost, because it was spent.

const ID = 'branched-conversation';

describe('branching a live conversation', () => {
  it.effect(
    're-roots at an earlier record and carries on from the branch',
    () =>
      Effect.gen(function* () {
        const conversation = Conversation.make(plainAgent, ID);
        const models = provider([
          says('turn 1'),
          says('turn 2'),
          says('turn 3'),
        ]);

        const observed = yield* run(
          Effect.gen(function* () {
            const first = yield* conversation.run('what is the status?');
            const opening = present((yield* readRecords(ID))[0]).offset;
            const branched = yield* conversation.branchFrom(
              opening,
              'what is the status, precisely?',
            );
            yield* conversation.run('and then?');
            return { first, branched, written: yield* readRecords(ID) };
          }),
          models.layer,
        );

        // One marker; nothing was rewritten or removed.
        expect(tags(observed.written).slice(0, 11)).toEqual([
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

        // The branched turn is asked against the earlier point, not the end.
        const branchedPrompt = present(models.asked[1]);
        expect(rolesOf(branchedPrompt)).toEqual(['system', 'user', 'user']);
        expect(textIn(branchedPrompt)).toContain('what is the status?');
        expect(textIn(branchedPrompt)).toContain('precisely');
        expect(textIn(branchedPrompt)).not.toContain('turn 1');

        // An ordinary resume afterwards sees the edited turn, not the original.
        const resumed = textIn(present(models.asked[2]));
        expect(resumed).toContain('precisely');
        expect(resumed).toContain('turn 2');
        expect(resumed).not.toContain('turn 1');

        // Cost is a fact about what was billed, not about what the conversation
        // now says.
        expect(observed.branched.usage.output).toBe(
          observed.first.usage.output * 2,
        );
        expect(observed.branched.usage.input).toBe(
          observed.first.usage.input * 2,
        );
      }),
  );

  // A branch off a branch. The second marker jumps over the first one's
  // records and the first marker, and the walk keeps going backwards from
  // wherever it lands rather than restarting.
  it.effect('follows a branch taken from inside an abandoned range', () =>
    Effect.gen(function* () {
      const conversation = Conversation.make(plainAgent, ID);
      const models = provider([says('a1'), says('a2'), says('a3'), says('a4')]);

      yield* run(
        Effect.gen(function* () {
          yield* conversation.run('q1');
          const afterFirst = present((yield* readRecords(ID)).at(-1)).offset;
          yield* conversation.run('q2');
          const inQ2 = present((yield* readRecords(ID)).at(-3)).offset;
          yield* conversation.branchFrom(afterFirst, 'q2b');
          yield* conversation.branchFrom(inQ2, 'q2c');
        }),
        models.layer,
      );

      const last = textIn(present(models.asked[3]));
      for (const kept of ['q1', 'a1', 'q2', 'a2', 'q2c']) {
        expect(last).toContain(kept);
      }
      expect(last).not.toContain('q2b');
      expect(last).not.toContain('a3');
    }),
  );

  // Records come out of a database. A marker naming a record that is not
  // there must degrade into an ordinary step backwards: a walk that could
  // revisit a record would hang a tail, which is worse than a wrong answer
  // because nothing reports it.
  it.effect('tolerates a branch marker that names no record', () =>
    Effect.gen(function* () {
      const models = provider([says('fine')]);
      const result = yield* run(
        Effect.gen(function* () {
          yield* seed(ID, [
            started('hi'),
            said('one'),
            turn(),
            settled(),
            { _tag: 'BranchedFrom', at: LogOffset.fromSeq(99n) },
          ]);
          return yield* Conversation.make(plainAgent, ID).run('next');
        }),
        models.layer,
      );

      expect(result.text).toBe('fine');
      expect(textIn(present(models.asked[0]))).toContain('one');
    }),
  );

  // A branch from `START` abandons every record before it, and keeps them
  // abandoned: the run after the branched one must not see them either.
  it.effect('keeps a conversation started over from START started over', () =>
    Effect.gen(function* () {
      const conversation = Conversation.make(plainAgent, ID);
      const models = provider([
        says('abandoned-answer'),
        says('fresh-answer'),
        says('next-answer'),
      ]);
      yield* run(
        Effect.gen(function* () {
          yield* conversation.run('abandoned-question');
          yield* conversation.branchFrom(LogOffset.START, 'fresh-question');
          yield* conversation.run('next-question');
        }),
        models.layer,
      );

      for (const turnAsked of [models.asked[1], models.asked[2]]) {
        const asked = textIn(present(turnAsked));
        expect(asked).not.toContain('abandoned-question');
        expect(asked).not.toContain('abandoned-answer');
      }
      expect(textIn(present(models.asked[2]))).toContain('fresh-answer');
    }),
  );
});

const compactedRecord = (summary: string, firstKept: LogOffset.Offset) =>
  ({
    _tag: 'Compacted',
    formatVersion: 1,
    agent: 'test',
    agentRevision: LogVocabulary.AgentRevision.make('1'),
    step: 1,
    summary,
    firstKept,
    summarizedMessages: 2,
    keptMessages: 0,
  }) as const;

const at = (index: number) => LogOffset.fromSeq(BigInt(index));

// What the model is shown after a branch is rebuilt from the recorded history
// alone. These histories are written by hand so the abandoned range sits
// exactly where each case needs it.
describe('rebuilding a branched conversation', () => {
  const promptAfter = (records: Parameters<typeof seed>[1]) => {
    const models = provider([says('ok')]);
    return run(
      Effect.gen(function* () {
        yield* seed(ID, records);
        yield* Conversation.make(plainAgent, ID).run('next');
      }),
      models.layer,
    ).pipe(Effect.map(() => textIn(present(models.asked[0]))));
  };

  it.effect(
    'ignores a compaction from the abandoned branch and honours one on the active path',
    () =>
      Effect.gen(function* () {
        // "The latest compaction wins" has to mean the latest one on this path.
        const ignored = yield* promptAfter([
          started('first question'), // 0
          said('first answer'), // 1
          turn(), // 2
          compactedRecord('summary of the abandoned turn', LogOffset.START), // 3
          said('abandoned'), // 4
          { _tag: 'BranchedFrom', at: at(2) }, // 5
          said('new answer'), // 6
          turn(), // 7
          settled(), // 8
        ]);
        expect(ignored).not.toContain('summary of the abandoned turn');
        for (const kept of ['first question', 'first answer', 'new answer']) {
          expect(ignored).toContain(kept);
        }

        // The control: a compaction that is on the path still replaces the
        // history before it, so "ignore the abandoned one" cannot be satisfied
        // by "ignore every compaction".
        const honoured = yield* promptAfter([
          started('first question'), // 0
          said('abandoned'), // 1
          { _tag: 'BranchedFrom', at: at(0) }, // 2
          said('first answer'), // 3
          turn(), // 4
          compactedRecord('summary of everything so far', LogOffset.START), // 5
          said('after the summary'), // 6
          turn(), // 7
          settled(), // 8
        ]);
        expect(honoured).toContain('summary of everything so far');
        expect(honoured).toContain('after the summary');
        expect(honoured).not.toContain('first question');
        expect(honoured).not.toContain('abandoned');
      }),
  );

  // A compaction boundary has to name a record the reader can still find. If
  // the boundary were counted against the whole log it would land on the
  // abandoned branch, an offset the rebuild never returns, and the compacted
  // conversation would silently forget its kept tail.
  it.effect(
    'compacts a branched conversation onto records of the active path',
    () =>
      Effect.gen(function* () {
        const bulk = 'L'.repeat(4000);
        const compacting = Agent.make({
          name: 'test',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
          compaction: {
            reserveTokens: 0,
            keepRecentTokens: 10,
            instructions: 'sum',
          },
        });
        const models = fakeProvider({ limit: 500, summaryText: 'THE SUMMARY' });
        const observed = yield* run(
          Effect.gen(function* () {
            yield* seed(ID, [
              started('one'), // 0
              said(`a ${bulk}`), // 1
              turn(), // 2
              started('two'), // 3
              said('abandoned'), // 4
              turn(), // 5
              { _tag: 'BranchedFrom', at: at(3) }, // 6
              said('redone'), // 7
              turn(), // 8
              settled(), // 9
            ]);
            const conversation = Conversation.make(compacting, ID);
            yield* conversation.run('three');
            const afterCompaction = yield* readRecords(ID);
            yield* conversation.run('four');
            return afterCompaction;
          }),
          models.layer,
        );

        const abandoned = observed.slice(4, 6).map(({ offset }) => offset);
        const compaction = present(
          observed.find(({ record }) => record._tag === 'Compacted'),
        ).record;
        if (compaction._tag !== 'Compacted') {
          throw new Error('expected a compaction');
        }
        expect(abandoned).not.toContain(compaction.firstKept);
        expect(models.summaries).toHaveLength(1);
        const final = textIn(present(models.asked.at(-1)));
        expect(final).toContain('THE SUMMARY');
        expect(final).toContain('four');
        expect(final).not.toContain('abandoned');
      }),
  );
});

describe('a steer delivered before the branch', () => {
  // The one the obvious refactor breaks. A `SignalReceived` says an agent was
  // handed an instruction and acted on it; branching away from the turn that
  // took it does not un-hand it. If the delivery cursor were scoped to the
  // active path, the branch would rewind it past the record, re-drain the same
  // signal, and steer the agent a second time with something it already did.
  // The control: a steer sent after the branch is still delivered normally.
  it.effect('is not delivered again, and does not stop a later steer', () =>
    Effect.gen(function* () {
      const conversation = Conversation.make(plainAgent, ID);
      const models = provider([says('turn 1'), says('turn 2'), says('turn 3')]);

      const observed = yield* run(
        Effect.gen(function* () {
          yield* conversation.send({
            kind: 'steer',
            text: 'first steer',
            source: 'operator',
          });
          yield* conversation.run('hi');
          const first = yield* readRecords(ID);
          const askedBeforeBranch = models.asked.length;

          yield* conversation.branchFrom(present(first[0]).offset, 'try again');
          const askedAfterBranch = models.asked.length;
          const delivered = (yield* readRecords(ID)).flatMap(({ record }) =>
            record._tag === 'SignalReceived' ? [record.text] : [],
          );

          yield* conversation.send({
            kind: 'steer',
            text: 'second steer',
            source: 'operator',
          });
          yield* conversation.branchFrom(present(first[0]).offset, 'again');
          const deliveredLater = (yield* readRecords(ID)).flatMap(
            ({ record }) =>
              record._tag === 'SignalReceived' ? [record.text] : [],
          );
          return {
            askedBeforeBranch,
            askedAfterBranch,
            delivered,
            deliveredLater,
          };
        }),
        models.layer,
      );

      // The steer carried the first run past a stop condition every turn
      // satisfies: two prompts, one delivery. The branched turn adds exactly
      // one prompt; three prompts and two deliveries is the re-steer.
      expect(observed.askedBeforeBranch).toBe(2);
      expect(observed.askedAfterBranch).toBe(3);
      expect(observed.delivered).toEqual(['first steer']);
      expect(observed.deliveredLater).toEqual(['first steer', 'second steer']);
    }),
  );
});

describe('a crashed run on the abandoned branch', () => {
  // A run that started, called a tool, recorded its result, and never settled.
  // Crash recovery serves a dead run's tool outcomes back to its successor so
  // the tool is not run twice. A branched run is not that successor: its prompt
  // contains none of those calls, so an outcome served into it answers a
  // question nobody asked, and the answer is stale by exactly the edit made.
  const crashed = [
    started('where is order 42?'),
    said('looking'),
    lookupCall(),
    lookupStarted(),
    lookupOutcome(),
  ];

  it.effect('does not serve its tool outcomes to the branched run', () =>
    Effect.gen(function* () {
      const { agent, ran } = lookupAgent();
      const models = provider([callsLookup(), says('done')]);
      yield* run(
        Effect.gen(function* () {
          yield* seed(ID, crashed);
          const opening = present((yield* readRecords(ID))[0]).offset;
          yield* Conversation.make(agent, ID).branchFrom(
            opening,
            'where is order 43?',
          );
        }),
        models.layer,
      );

      // The tool ran, rather than being answered from a dead branch.
      expect(ran.count).toBe(1);
      expect(textIn(present(models.asked[1]))).toContain('shipped:42');
      expect(textIn(present(models.asked[1]))).not.toContain('from-log');
    }),
  );

  // The control, and the reason the case above is about branching rather than
  // about recovery having stopped working.
  it.effect('still serves them to an ordinary resume', () =>
    Effect.gen(function* () {
      const { agent, ran } = lookupAgent();
      const models = provider([callsLookup(), says('done')]);
      yield* run(
        Effect.gen(function* () {
          yield* seed(ID, crashed);
          yield* Conversation.make(agent, ID).run('and then?');
        }),
        models.layer,
      );

      expect(ran.count).toBe(0);
      expect(textIn(present(models.asked[1]))).toContain('from-log');
    }),
  );
});
