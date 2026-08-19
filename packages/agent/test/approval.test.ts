import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Crypto, Effect, Exit, Layer, Ref, Schema, Stream } from 'effect';
import {
  IdGenerator,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { ToolDispatch } from '../src/dispatch.js';
import * as AgentLog from '../src/log.js';

// Durable tool approval, without Effect WorkflowEngine.
//
// `effect/unstable/ai`'s `LanguageModel` already suspends a call marked
// `Tool.setNeedsApproval` before its handler is ever entered — its own
// dispatch wrapper checks `needsApproval` and emits a `tool-approval-request`
// part instead of dispatching. What this suite proves is Vesper's own half:
// that suspension is recorded durably by reusing
// `ToolSuspended`/`ToolResumed`/`ToolWaitCompleted` (the same record family
// `AgentWorkflow.wait` uses), that a crash leaves an orphan a later run
// recovers from without ever re-asking the model, and that
// `Conversation.resolveApproval` is the only new surface: approved
// dispatches the handler for the first time, denied settles a
// refusal-style result without ever entering it.

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 4 },
  },
});

const CALL_ID = LogVocabulary.ToolCallId.make('release-call');
const APPROVAL_ID = 'approval-1';

const release = Tool.make('release', {
  description: 'release a build to an environment',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ released: Schema.Boolean }),
}).setNeedsApproval(true);

const releaseAgentWith = (ran: { count: number }) =>
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

/**
 * First call asks for the tool and gets stopped on its own approval gate;
 * every later call reacts to whatever the resolved history now says, the
 * same as any ordinary second turn.
 */
const approvalScripted = () =>
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed<Response.PartEncoded[]>([finish()]),
        streamText: () =>
          Stream.unwrap(
            Effect.gen(function* () {
              const index = yield* Ref.getAndUpdate(calls, (n) => n + 1);
              if (index === 0) {
                // `release` is marked `needsApproval`, so
                // `LanguageModel.make`'s own dispatch wrapper is what turns
                // this raw tool call into a `tool-approval-request` part —
                // this script supplies only what a real provider would.
                return Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: 'tool-call',
                    id: CALL_ID,
                    name: 'release',
                    params: { id: 'r1' },
                  },
                  finish('tool-calls'),
                ]);
              }
              return Stream.fromIterable<Response.StreamPartEncoded>([
                { type: 'text-start', id: 'b' },
                { type: 'text-delta', id: 'b', delta: 'done' },
                { type: 'text-end', id: 'b' },
                finish(),
              ]);
            }),
          ),
      });
    }),
  );

/** A model that fails a test the moment it is asked anything. */
const unreachableModel = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.die('the model must not be called'),
    streamText: () => Stream.die('the model must not be called'),
  }),
);

/** One in-memory log, model, and id generator shared across a whole test. */
const provide = <A, E>(
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
  model: Layer.Layer<LanguageModel.LanguageModel> = approvalScripted(),
): Effect.Effect<A, E> =>
  effect.pipe(
    Effect.provide(model),
    Effect.provide(
      Layer.mergeAll(
        LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
        NodeServices.layer,
        Layer.succeed(IdGenerator.IdGenerator, IdGenerator.defaultIdGenerator),
      ),
    ),
    Effect.scoped,
  ) as Effect.Effect<A, E>;

const CONVERSATION = LogVocabulary.ConversationId.make('approval-conversation');
const PATH = AgentLog.pathFor(CONVERSATION);

/**
 * Write a previous run's records straight into the conversation, as if a
 * process suspended on the approval and then crashed before anything else
 * happened.
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

const started: ConversationRecord.Record = {
  _tag: 'RunStarted',
  agent: 'approval-test',
  formatVersion: 1,
  agentRevision: LogVocabulary.AgentRevision.make('1'),
  prompt: [],
};

const called: ConversationRecord.Record = {
  _tag: 'ToolCall',
  step: 1,
  id: CALL_ID,
  name: 'release',
  params: { id: 'r1' },
};

const suspended: ConversationRecord.Record = {
  _tag: 'ToolSuspended',
  id: CALL_ID,
  name: 'release',
  wait: ToolDispatch.APPROVAL_WAIT,
  token: APPROVAL_ID,
  request: { id: 'r1' },
};

describe('durable tool approval', () => {
  it.effect('suspends the run and surfaces the pending approval', () =>
    provide(
      Effect.gen(function* () {
        const ran = { count: 0 };
        const conversation = Conversation.make(
          releaseAgentWith(ran),
          'suspend-and-surface',
        );

        const result = yield* conversation.run('release r1');

        expect(result.outcome).toBe('suspended');
        expect(result.pendingApprovals).toEqual([
          { toolCallId: CALL_ID, toolName: 'release', input: { id: 'r1' } },
        ]);
        expect(ran.count).toBe(0);

        const records = yield* conversation.records().pipe(Stream.runCollect);
        const tags = Array.from(records).map(
          (envelope) => envelope.record._tag,
        );
        expect(tags).toContain('ToolSuspended');
        expect(tags).not.toContain('ToolStarted');
        expect(tags).not.toContain('ToolOutcome');
      }),
    ),
  );

  it.effect('approve dispatches the handler for the first time', () =>
    provide(
      Effect.gen(function* () {
        const ran = { count: 0 };
        const agent = releaseAgentWith(ran);
        const conversation = Conversation.make(agent, 'approve-executes');

        yield* conversation.run('release r1');
        expect(ran.count).toBe(0);

        yield* conversation.resolveApproval(CALL_ID, 'approve');
        const result = yield* conversation.run('release r1');

        expect(ran.count).toBe(1);
        expect(result.outcome).toBe('success');

        const records = yield* conversation.records().pipe(Stream.runCollect);
        const outcomes = Array.from(records).flatMap((envelope) =>
          envelope.record._tag === 'ToolOutcome' ? [envelope.record] : [],
        );
        expect(outcomes).toEqual([
          {
            _tag: 'ToolOutcome',
            step: 1,
            id: CALL_ID,
            name: 'release',
            outcome: 'success',
            result: { released: true },
          },
        ]);
      }),
    ),
  );

  it.effect('deny settles a refusal-style result without dispatching', () =>
    provide(
      Effect.gen(function* () {
        const ran = { count: 0 };
        const agent = releaseAgentWith(ran);
        const conversation = Conversation.make(agent, 'deny-refuses');

        yield* conversation.run('release r1');
        yield* conversation.resolveApproval(CALL_ID, 'deny', 'not this week');
        const result = yield* conversation.run('release r1');

        expect(ran.count).toBe(0);
        expect(result.outcome).toBe('success');

        const records = yield* conversation.records().pipe(Stream.runCollect);
        const tags = Array.from(records).map(
          (envelope) => envelope.record._tag,
        );
        expect(tags).not.toContain('ToolStarted');

        const outcomes = Array.from(records).flatMap((envelope) =>
          envelope.record._tag === 'ToolOutcome' ? [envelope.record] : [],
        );
        expect(outcomes).toEqual([
          {
            _tag: 'ToolOutcome',
            step: 1,
            id: CALL_ID,
            name: 'release',
            outcome: 'failure',
            result: { type: 'approval-denied', reason: 'not this week' },
          },
        ]);
      }),
    ),
  );

  it.effect(
    'resumes a pending approval after a crash without asking the model again',
    () =>
      provide(
        Effect.gen(function* () {
          yield* seed([started, called, suspended]);

          const conversation = Conversation.make(
            releaseAgentWith({ count: 0 }),
            CONVERSATION,
          );
          const result = yield* conversation.run('anything');

          expect(result.outcome).toBe('suspended');
          expect(result.pendingApprovals).toEqual([
            { toolCallId: CALL_ID, toolName: 'release', input: { id: 'r1' } },
          ]);
        }),
        unreachableModel,
      ),
  );

  it.effect('a second resolution for the same call is a typed conflict', () =>
    provide(
      Effect.gen(function* () {
        const agent = releaseAgentWith({ count: 0 });
        const conversation = Conversation.make(agent, 'double-resolve');

        yield* conversation.run('release r1');
        yield* conversation.resolveApproval(CALL_ID, 'approve');

        const second = yield* Effect.flip(
          conversation.resolveApproval(CALL_ID, 'deny'),
        );

        expect(second._tag).toBe('ApprovalResolutionError');
        if (second._tag === 'ApprovalResolutionError') {
          expect(second.reason).toBe('already_resolved');
        }
      }),
    ),
  );

  it.effect(
    'an unrecorded run fails outright instead of hanging on an approval',
    () =>
      provide(
        Effect.gen(function* () {
          const agent = releaseAgentWith({ count: 0 });
          const outcome = yield* Effect.exit(agent.run('release r1'));

          expect(Exit.isFailure(outcome)).toBe(true);
          expect(String(outcome)).toContain('Conversation');
        }),
      ),
  );
});
