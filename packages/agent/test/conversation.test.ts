import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { describe, expect, it } from '@effect/vitest';
import { Context, Effect, Layer, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import type { AgentLog } from '../src/log.js';
import { RecordingPolicy } from '../src/recording-policy.js';

const finish: Response.FinishPartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
};

const model = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([finish]),
    streamText: () =>
      Stream.make(
        { type: 'text-start', id: 'a' },
        { type: 'text-delta', id: 'a', delta: 'answer' },
        { type: 'text-end', id: 'a' },
        finish,
      ),
  }),
);

const agent = Agent.make({
  name: 'conversation-test',
  revision: '1',
  instructions: 'answer',
  toolkit: Toolkit.make(),
});

const provide = <A, E>(
  effect: Effect.Effect<A, E, LogStore.Service | LanguageModel.LanguageModel>,
) =>
  effect.pipe(
    Effect.provide(model),
    Effect.provide(LogStoreMemory.layer),
    Effect.scoped,
  );

type EffR<T> = T extends Effect.Effect<unknown, unknown, infer R> ? R : never;
type EffE<T> = T extends Effect.Effect<unknown, infer E, unknown> ? E : never;
type Exact<A, B> = [A, B] extends [B, A] ? true : false;

class Redactor extends Context.Service<
  Redactor,
  { readonly replacement: string }
>()('conversation-test/Redactor') {}

const policy = {
  prompt: () =>
    Effect.map(Redactor, ({ replacement }) => [
      { role: 'user' as const, content: replacement },
    ]),
} satisfies RecordingPolicy.Policy<Redactor>;

const plain = Conversation.make(agent, 'typed');
const filtered = Conversation.recording(agent, 'typed-filtered', policy);
const _plainRequirements: Exact<
  EffR<ReturnType<typeof plain.run>>,
  LanguageModel.LanguageModel | LogStore.Service
> = true;
const _plainError: Exact<
  EffE<ReturnType<typeof plain.run>>,
  AiError.AiError | AgentLog.CompatibilityError
> = true;
const _policyRequirements: Exact<
  EffR<ReturnType<typeof filtered.resume>>,
  LanguageModel.LanguageModel | LogStore.Service | Redactor
> = true;

describe('Conversation', () => {
  it.effect(
    'binds identity and exposes events, records, and signals separately',
    () =>
      provide(
        Effect.gen(function* () {
          const conversation = Conversation.make(agent, 'bound');
          expect(conversation.id).toBe('bound');
          yield* conversation.send({
            kind: 'steer',
            text: 'also this',
            source: 'test',
          });
          yield* conversation.stream('hello').pipe(Stream.runDrain);
          const records = yield* conversation.records().pipe(Stream.runCollect);
          expect(
            Array.from(records).some(
              ({ record }) => record._tag === 'SignalReceived',
            ),
          ).toBe(true);
        }),
      ),
  );

  it.effect('keeps source identity for branch and fork operations', () =>
    provide(
      Effect.gen(function* () {
        const source = Conversation.make(agent, 'source');
        yield* source.run('first');
        const records = yield* source.records().pipe(Stream.runCollect);
        const at = Array.from(records)[0]!.offset;
        yield* source.branchFrom(at, 'branched');
        yield* source.forkFrom(at, 'fork-target', 'forked');
        const fork = Conversation.make(agent, 'fork-target');
        const forkRecords = yield* fork
          .records()
          .pipe(Stream.take(6), Stream.runCollect);
        expect(Array.from(forkRecords)[0]!.conversationId).toBe('fork-target');
        expect(source.id).toBe('source');
      }),
    ),
  );

  it.effect(
    'applies one recording policy to run, resume, branch, and fork',
    () =>
      provide(
        Effect.gen(function* () {
          const conversation = Conversation.recording(
            agent,
            'policy-source',
            policy,
          );
          yield* conversation.run('run secret');
          let records = yield* conversation
            .records()
            .pipe(Stream.take(5), Stream.runCollect);
          const at = Array.from(records)[0]!.offset;
          yield* conversation.resume('resume secret');
          yield* conversation.branchFrom(at, 'branch secret');
          yield* conversation.forkFrom(at, 'policy-fork', 'fork secret');
          records = yield* conversation
            .records()
            .pipe(Stream.take(11), Stream.runCollect);
          const sourceBody = JSON.stringify(Array.from(records));
          const forkRecords = yield* Conversation.make(agent, 'policy-fork')
            .records()
            .pipe(Stream.take(6), Stream.runCollect);
          const body = sourceBody + JSON.stringify(Array.from(forkRecords));
          expect(body).toContain('[redacted]');
          expect(body).not.toContain('run secret');
          expect(body).not.toContain('resume secret');
          expect(body).not.toContain('branch secret');
          expect(body).not.toContain('fork secret');
        }).pipe(Effect.provideService(Redactor, { replacement: '[redacted]' })),
      ),
  );

  it('pins type assertions', () => {
    expect([_plainRequirements, _plainError, _policyRequirements]).toEqual([
      true,
      true,
      true,
    ]);
  });
});
