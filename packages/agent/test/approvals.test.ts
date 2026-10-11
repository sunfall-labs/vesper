import { describe, expect, it } from '@effect/vitest';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Deferred, Effect, Exit, Fiber, Schema, Stream } from 'effect';
import { AiError, Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interaction } from '../src/interaction.js';
import { Replay } from '../src/replay.js';
import { ScriptedModel } from '../src/testing.js';
import {
  callTurn,
  outcomesOf,
  provide,
  readRecords,
  runStarted,
  seed,
  tagsOf,
  textTurn,
  toolCalled,
  unreachableModel,
} from './tool-fixtures.js';

// Durable approval and external interaction: a call the policy or the tool
// itself says must wait for a person is recorded as a suspension, survives a
// crash, and is settled by Conversation.resolveApproval / resolveInteraction.

const CALL_ID = LogVocabulary.ToolCallId.make('release-call');

const release = Tool.make('release', {
  description: 'release a build to an environment',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ released: Schema.Boolean }),
}).setNeedsApproval(true);

const releaseOptional = Tool.make('release_optional', {
  description: 'release a build to an optional environment',
  parameters: Schema.Struct({
    id: Schema.String,
    environment: Schema.optionalKey(Schema.String),
  }),
  success: Schema.Struct({ released: Schema.Boolean }),
}).setNeedsApproval(true);

const releaseAgent = (ran: { count: number }) =>
  Agent.make({
    name: 'approval-test',
    revision: '1',
    instructions: 'release only through the tool',
    toolkit: Toolkit.make(release),
  }).withHandlers({
    release: () =>
      Effect.sync(() => {
        ran.count += 1;
        return { released: true };
      }),
  });

/** The model states its intent, asks for the gated tool, then reacts to the resolution. */
const releasing = (name = 'release', params: unknown = { id: 'r1' }) =>
  ScriptedModel.make([
    [
      { type: 'text-start', id: 'a' },
      { type: 'text-delta', id: 'a', delta: 'Releasing r1.' },
      { type: 'text-end', id: 'a' },
      ...callTurn({ id: CALL_ID, name, params }),
    ],
    textTurn('done'),
  ]);

const recordsOf = <E, R>(conversation: {
  readonly records: () => Stream.Stream<ConversationRecord.Envelope, E, R>;
}) =>
  conversation.records().pipe(
    Stream.runCollect,
    Effect.map((records) => Array.from(records, ({ record }) => record)),
  );

describe('which calls are asked about', () => {
  const malformed = [
    ['a wrong type', releaseOptional, 'release_optional', { id: 42 }],
    [
      'a null for an optional key',
      releaseOptional,
      'release_optional',
      { id: 'r1', environment: null },
    ],
  ] as const;

  for (const [label, tool, name, params] of malformed) {
    it.effect(
      `answers malformed parameters (${label}) without asking for approval`,
      () => {
        const ran = { count: 0 };
        const agent = Agent.make({
          name: 'approval-parameter-test',
          revision: '1',
          instructions: 'release only through the tool',
          toolkit: Toolkit.make(tool),
        }).withHandlers({
          release_optional: () =>
            Effect.sync(() => void (ran.count += 1)).pipe(
              Effect.as({ released: true }),
            ),
        });
        return provide(
          Effect.gen(function* () {
            const conversation = Conversation.make(
              agent,
              `malformed-${label.replaceAll(' ', '-')}`,
            );
            const result = yield* conversation.run('release r1');
            const records = yield* recordsOf(conversation);
            const outcome = outcomesOf(records).find(
              (record) => record.name === name,
            );

            expect(result).toMatchObject({ outcome: 'success', text: 'done' });
            expect(ran.count).toBe(0);
            expect(tagsOf(records)).not.toContain('StepSuspended');
            expect(tagsOf(records)).not.toContain('StepStarted');
            expect(outcome?.outcome).toBe('failure');
            expect(
              (yield* Schema.decodeUnknownEffect(AiError.AiError)(
                outcome?.result,
              )).reason,
            ).toMatchObject({
              _tag: 'ToolParameterValidationError',
              toolName: name,
            });
          }),
          releasing(name, params),
        );
      },
    );
  }

  const failing = [
    ['fails', () => Effect.die(new Error('policy service unavailable'))],
    [
      'throws',
      () => {
        throw new Error('bad predicate');
      },
    ],
  ] as const;

  for (const [label, needsApproval] of failing) {
    it.effect(
      `fails closed when the approval predicate ${label}: approval is requested, the handler does not run`,
      () => {
        const ran = { count: 0 };
        const guarded = Tool.make('guarded', {
          parameters: Schema.Struct({ id: Schema.String }),
          success: Schema.String,
          failureMode: 'return',
        }).setNeedsApproval(needsApproval);
        const agent = Agent.make({
          name: 'predicate-test',
          revision: '1',
          instructions: 'x',
          toolkit: Toolkit.make(guarded),
        }).withHandlers({
          guarded: ({ id }) =>
            Effect.sync(() => void (ran.count += 1)).pipe(Effect.as(id)),
        });
        return provide(
          Effect.gen(function* () {
            const result = yield* Conversation.make(
              agent,
              `predicate-${label}`,
            ).run('go');
            expect(result.outcome).toBe('suspended');
            expect(
              result.outcome === 'suspended' && result.pendingInteractions,
            ).toMatchObject([{ toolName: 'guarded', kind: 'approval' }]);
            expect(ran.count).toBe(0);
          }),
          releasing('guarded', { id: 'r1' }),
        );
      },
    );
  }
});

describe('durable tool approval', () => {
  it.effect('suspends the run and surfaces the pending approval', () => {
    const ran = { count: 0 };
    return provide(
      Effect.gen(function* () {
        const conversation = Conversation.make(releaseAgent(ran), 'suspend');
        const result = yield* conversation.run('release r1');

        expect(result).toMatchObject({
          outcome: 'suspended',
          // The intent stated before the gate is kept: it is often exactly
          // what an approver should be shown.
          text: 'Releasing r1.',
          pendingInteractions: [
            {
              toolCallId: CALL_ID,
              toolName: 'release',
              kind: 'approval',
              request: { id: 'r1' },
            },
          ],
        });
        expect(ran.count).toBe(0);
        const tags = tagsOf(yield* recordsOf(conversation));
        expect(tags).toContain('StepSuspended');
        expect(tags).not.toContain('StepStarted');
        expect(tags).not.toContain('StepSettled');
      }),
      releasing(),
    );
  });

  it.effect('approve dispatches the handler for the first time', () => {
    const ran = { count: 0 };
    return provide(
      Effect.gen(function* () {
        const conversation = Conversation.make(releaseAgent(ran), 'approve');
        yield* conversation.run('release r1');
        expect(ran.count).toBe(0);

        yield* conversation.resolveApproval(CALL_ID, 'approve');
        // No new input: the decision is what there is to act on, so the
        // continuation runs from durable state alone.
        const events = Array.from(
          yield* conversation.stream().pipe(Stream.runCollect),
        );

        expect(ran.count).toBe(1);
        expect(
          events.find((event) => event._tag === 'Completed'),
        ).toMatchObject({ outcome: 'success' });
        expect(
          events.find(
            (event) =>
              event._tag === 'Part' && event.part.type === 'tool-result',
          ),
        ).toMatchObject({
          step: 1,
          part: {
            id: CALL_ID,
            name: 'release',
            result: { released: true },
            isFailure: false,
            resultSource: 'recovered',
          },
        });
        expect(outcomesOf(yield* recordsOf(conversation))).toEqual([
          {
            _tag: 'StepSettled',
            step: 1,
            id: CALL_ID,
            name: 'release',
            outcome: 'success',
            result: { released: true },
          },
        ]);
      }),
      releasing(),
    );
  });

  it.effect('deny settles a refusal-style result without dispatching', () => {
    const ran = { count: 0 };
    return provide(
      Effect.gen(function* () {
        const conversation = Conversation.make(releaseAgent(ran), 'deny');
        yield* conversation.run('release r1');
        yield* conversation.resolveApproval(CALL_ID, 'deny', 'not this week');
        const events = Array.from(
          yield* conversation.stream('release r1').pipe(Stream.runCollect),
        );
        const records = yield* recordsOf(conversation);
        const refusal = outcomesOf(records).at(0);
        const others = outcomesOf(records).slice(1);

        expect(ran.count).toBe(0);
        expect(
          events.find((event) => event._tag === 'Completed'),
        ).toMatchObject({ outcome: 'success' });
        expect(
          events.find(
            (event) =>
              event._tag === 'Part' && event.part.type === 'tool-result',
          ),
        ).toMatchObject({
          part: { id: CALL_ID, isFailure: true, resultSource: 'recovered' },
        });
        expect(tagsOf(records)).not.toContain('StepStarted');
        expect(others).toEqual([]);
        expect(refusal).toMatchObject({ id: CALL_ID, outcome: 'failure' });
        // Effect's own in-band denial shape, not one only this feature knows.
        const decoded = yield* Schema.decodeUnknownEffect(
          Tool.ExecutionFailure,
        )(refusal?.result);
        expect(decoded.type).toBe('execution-denied');
        expect(decoded.reason).toContain('not this week');
      }),
      releasing(),
    );
  });

  it.effect(
    'a crash leaves the approval pending without asking the model again',
    () => {
      const agent = releaseAgent({ count: 0 });
      return provide(
        Effect.gen(function* () {
          yield* Conversation.make(agent, 'crash').run('release r1');
          const result = yield* Conversation.make(agent, 'crash')
            .run('anything')
            .pipe(Effect.provide(unreachableModel));

          expect(result).toMatchObject({
            outcome: 'suspended',
            pendingInteractions: [
              { toolCallId: CALL_ID, toolName: 'release', kind: 'approval' },
            ],
          });
        }),
        releasing(),
      );
    },
  );

  it.effect(
    'settles malformed parameters from an already-approved durable call',
    () => {
      const ran = { count: 0 };
      const token = 'approval-1';
      const request = { id: 'r1', environment: null };
      const agent = Agent.make({
        name: 'approval-test',
        revision: '1',
        instructions: 'release only through the tool',
        toolkit: Toolkit.make(releaseOptional),
      }).withHandlers({
        release_optional: () =>
          Effect.sync(() => void (ran.count += 1)).pipe(
            Effect.as({ released: true }),
          ),
      });
      const model = ScriptedModel.make([textTurn('Recovered.')]);
      return provide(
        Effect.gen(function* () {
          yield* seed('approved-malformed', [
            runStarted('approval-test'),
            toolCalled('release_optional', request, CALL_ID),
            {
              _tag: 'StepSuspended',
              id: CALL_ID,
              name: 'release_optional',
              wait: '@sunfall/vesper-agent/interaction',
              token,
              request,
            },
            {
              _tag: 'StepResolved',
              id: CALL_ID,
              name: 'release_optional',
              wait: '@sunfall/vesper-agent/interaction',
              token,
              outcome: 'success',
              result: null,
            },
          ]);
          const result = yield* Conversation.make(
            agent,
            'approved-malformed',
          ).run();
          const records = yield* readRecords('approved-malformed');
          const tags = tagsOf(records);

          expect(result).toMatchObject({
            outcome: 'success',
            text: 'Recovered.',
          });
          expect(ran.count).toBe(0);
          expect(outcomesOf(records).at(-1)?.outcome).toBe('failure');
          expect(tags).not.toContain('StepStarted');
          expect(tags.indexOf('StepSettled')).toBeGreaterThan(
            tags.indexOf('StepResolved'),
          );
        }),
        model,
      );
    },
  );

  // An approved handler is a started call like any other. If the process dies
  // while it runs, nothing knows whether its effect happened, so resuming must
  // not run it again on its own: only `onIndeterminateToolCall` may decide.
  it.effect(
    'does not run an approved handler again after a crash without consent',
    () => {
      const ran = { count: 0 };
      return provide(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const hanging = Agent.make({
            name: 'approval-test',
            revision: '1',
            instructions: 'release only through the tool',
            toolkit: Toolkit.make(release),
          }).withHandlers({
            release: () =>
              Effect.sync(() => {
                ran.count += 1;
              }).pipe(
                Effect.andThen(Deferred.succeed(entered, undefined)),
                Effect.andThen(Effect.never),
              ),
          });
          const conversation = Conversation.make(hanging, 'approved-crash');
          yield* conversation.run('release r1');
          yield* conversation.resolveApproval(CALL_ID, 'approve');

          // The approved handler starts, and the process dies under it.
          const running = yield* Effect.forkChild(conversation.run());
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(running);
          const afterCrash = tagsOf(yield* recordsOf(conversation));
          expect(afterCrash).toContain('StepStarted');
          expect(afterCrash).not.toContain('StepSettled');

          const refused = yield* Effect.exit(conversation.run());
          expect(Exit.isFailure(refused)).toBe(true);
          expect(ran.count).toBe(1);

          const answered = yield* Conversation.make(
            hanging.intercepting(Replay.policy),
            'approved-crash',
          ).run();
          expect(answered.outcome).toBe('success');
          expect(ran.count).toBe(1);
          expect(outcomesOf(yield* recordsOf(conversation))).toMatchObject([
            {
              id: CALL_ID,
              outcome: 'failure',
              result: { type: 'execution-interrupted' },
            },
          ]);
        }),
        releasing(),
      );
    },
  );

  it.effect(
    'a second resolution, even after the run settled, is a typed conflict',
    () => {
      const ran = { count: 0 };
      return provide(
        Effect.gen(function* () {
          const conversation = Conversation.make(releaseAgent(ran), 'twice');
          yield* conversation.run('release r1');
          yield* conversation.resolveApproval(CALL_ID, 'approve');

          const during = yield* Effect.flip(
            conversation.resolveApproval(CALL_ID, 'deny'),
          );
          // Dispatches the approved call and settles the run; the suspension
          // is no longer in the recovery snapshot but must still read as
          // resolved, not `not_found`.
          const settled = yield* conversation.run();
          const after = yield* Effect.flip(
            conversation.resolveApproval(CALL_ID, 'deny', 'too late'),
          );

          expect(settled.outcome).toBe('success');
          expect(ran.count).toBe(1);
          for (const error of [during, after]) {
            expect(error).toMatchObject({
              _tag: 'ApprovalResolutionError',
              reason: 'already_resolved',
            });
          }
          expect(
            tagsOf(yield* recordsOf(conversation)).filter(
              (tag) => tag === 'StepResolved',
            ),
          ).toHaveLength(1);
        }),
        releasing(),
      );
    },
  );

  it.effect(
    'an unrecorded run fails outright instead of hanging on an approval',
    () =>
      provide(
        Effect.gen(function* () {
          const outcome = yield* Effect.exit(
            releaseAgent({ count: 0 }).run('release r1'),
          );
          expect(Exit.isFailure(outcome)).toBe(true);
          expect(String(outcome)).toContain('Conversation');
        }),
        releasing(),
      ),
  );
});

const asking = () =>
  ScriptedModel.make([
    callTurn({
      id: 'question-call',
      name: 'question',
      params: { question: 'Which color?', options: ['blue', 'green'] },
    }),
    textTurn('Blue it is.'),
  ]);

describe('external interactions', () => {
  const Question = Interaction.answer(
    Tool.make('question', {
      description: 'ask the user for information',
      parameters: Schema.Struct({
        question: Schema.NonEmptyString,
        options: Schema.NullOr(Schema.Array(Schema.NonEmptyString)),
      }),
      success: Schema.Struct({ answer: Schema.NonEmptyString }),
      failureMode: 'return',
    }),
  );
  const asker = Agent.make({
    name: 'interaction-test',
    revision: '1',
    instructions: 'ask through the question tool',
    toolkit: Toolkit.make(Question),
  }).withHandlers({ question: () => Interaction.unreachable });
  it.effect('durably turns a typed answer into the tool result', () =>
    provide(
      Effect.gen(function* () {
        const conversation = Conversation.make(asker, 'question');
        const suspended = yield* conversation.run('ask me');
        expect(suspended).toMatchObject({
          outcome: 'suspended',
          pendingInteractions: [
            {
              toolCallId: 'question-call',
              toolName: 'question',
              kind: 'question',
              request: { question: 'Which color?', options: ['blue', 'green'] },
            },
          ],
        });

        yield* conversation.resolveInteraction(Question, 'question-call', {
          answer: 'blue',
        });
        const completed = yield* conversation.run();
        const records = yield* recordsOf(conversation);

        expect(completed).toMatchObject({
          outcome: 'success',
          text: 'Blue it is.',
        });
        expect(
          records.find((record) => record._tag === 'StepSuspended'),
        ).toMatchObject({ interaction: { name: 'question', mode: 'answer' } });
        expect(outcomesOf(records)).toMatchObject([
          {
            id: 'question-call',
            outcome: 'success',
            result: { answer: 'blue' },
          },
        ]);
      }),
      asking(),
    ),
  );

  it.effect('validates the answer before recording it', () =>
    provide(
      Effect.gen(function* () {
        const conversation = Conversation.make(asker, 'invalid-answer');
        yield* conversation.run('ask me');
        const exit = yield* Effect.exit(
          conversation.resolveInteraction(Question, 'question-call', {
            answer: '',
          }),
        );
        expect(exit._tag).toBe('Failure');
        expect(tagsOf(yield* recordsOf(conversation))).not.toContain(
          'StepResolved',
        );
      }),
      asking(),
    ),
  );
});
