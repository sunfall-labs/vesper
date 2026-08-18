import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Context, Crypto, Effect, Layer, Stream } from 'effect';
import { LanguageModel, type Response, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { RecordingPolicy } from '../src/recording-policy.js';

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeCrypto.layer)),
  NodeServices.layer,
);

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
  effect: Effect.Effect<
    A,
    E,
    LogStore.Service | LanguageModel.LanguageModel | Crypto.Crypto
  >,
) =>
  effect.pipe(
    Effect.provide(model),
    Effect.provide(testLogLayer),
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
const filtered = Conversation.withRecordingPolicy(
  agent,
  'typed-filtered',
  policy,
);
const storageFailure = (
  operation: LogStore.LogStoreError['operation'],
  path: string,
) =>
  new LogStore.LogStoreError({
    path,
    operation,
    reason: 'storage',
    detail: 'offline',
  });

const failingStore = (
  override: (store: LogStore.Interface) => Partial<LogStore.Interface>,
) =>
  Layer.effect(
    LogStore.Service,
    Effect.map(LogStore.Service, (store) =>
      LogStore.Service.of({ ...store, ...override(store) }),
    ),
  ).pipe(Layer.provide(testLogLayer));

const _plainRequirements: Exact<
  EffR<ReturnType<typeof plain.run>>,
  LanguageModel.LanguageModel | LogStore.Service | Crypto.Crypto
> = true;
const _plainError: Exact<
  EffE<ReturnType<typeof plain.run>>,
  Conversation.Error<typeof agent>
> = true;
const _policyRequirements: Exact<
  EffR<ReturnType<typeof filtered.run>>,
  LanguageModel.LanguageModel | LogStore.Service | Redactor | Crypto.Crypto
> = true;

describe('Conversation', () => {
  it.effect('keeps storage failures typed while opening a run', () =>
    Effect.gen(function* () {
      const failing = failingStore(() => ({
        meta: (path) => Effect.fail(storageFailure('meta', path)),
      }));
      const error = yield* Conversation.make(agent, 'storage-failure')
        .run('hello')
        .pipe(
          Effect.provide(model),
          Effect.provide(failing),
          Effect.provide(NodeCrypto.layer),
          Effect.flip,
        );
      expect(error).toMatchObject({ reason: 'storage' });
    }),
  );

  it.effect('keeps continuation storage failures typed', () =>
    Effect.gen(function* () {
      const failingOpen = failingStore(() => ({
        meta: (path) => Effect.fail(storageFailure('meta', path)),
      }));
      const failingFork = failingStore(() => ({
        read: (path) => Effect.fail(storageFailure('read', path)),
      }));
      const conversation = Conversation.make(agent, 'continue-failure');
      const cases = [
        { operation: conversation.run('resume'), store: failingOpen },
        {
          operation: conversation.branchFrom(LogOffset.START, 'branch'),
          store: failingOpen,
        },
        {
          operation: conversation.forkFrom(
            LogOffset.START,
            'fork-failure',
            'fork',
          ),
          store: failingFork,
        },
      ];

      for (const { operation, store } of cases) {
        const error = yield* operation.pipe(
          Effect.provide(model),
          Effect.provide(store),
          Effect.provide(NodeCrypto.layer),
          Effect.flip,
        );
        expect(error).toMatchObject({ reason: 'storage' });
      }
    }),
  );

  it.effect('keeps snapshot and follow read failures typed', () =>
    Effect.gen(function* () {
      const failing = failingStore(() => ({
        read: (path) => Effect.fail(storageFailure('read', path)),
      }));
      const conversation = Conversation.make(agent, 'read-failure');
      const snapshotError = yield* conversation
        .records()
        .pipe(Stream.runDrain, Effect.provide(failing), Effect.flip);
      const followError = yield* conversation
        .follow()
        .pipe(Stream.runDrain, Effect.provide(failing), Effect.flip);
      expect(snapshotError).toMatchObject({ reason: 'storage' });
      expect(followError).toMatchObject({ reason: 'storage' });
    }),
  );

  it.effect('keeps signal write failures typed', () =>
    Effect.gen(function* () {
      const failing = failingStore(() => ({
        create: (path) => Effect.fail(storageFailure('create', path)),
      }));
      const error = yield* Conversation.make(agent, 'signal-failure')
        .send({ kind: 'cancel', text: 'stop', source: 'test' })
        .pipe(
          Effect.provide(failing),
          Effect.provide(NodeCrypto.layer),
          Effect.flip,
        );
      expect(error).toMatchObject({ reason: 'storage' });
    }),
  );

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
          const conversation = Conversation.withRecordingPolicy(
            agent,
            'policy-source',
            policy,
          );
          yield* conversation.run('run secret');
          let records = yield* conversation
            .records()
            .pipe(Stream.take(5), Stream.runCollect);
          const at = Array.from(records)[0]!.offset;
          yield* conversation.run('resume secret');
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
