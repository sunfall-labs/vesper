import type { LogStore } from '@sunfall/vesper-log/log-store';
import { describe, expect, it } from '@effect/vitest';
import {
  Context,
  Crypto,
  Effect,
  Schema,
  SchemaTransformation,
  type Stream,
} from 'effect';
import { LanguageModel, Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { Interception } from '../src/interception.js';
import type * as AgentLog from '../src/log.js';
import { RecordingPolicy } from '../src/recording-policy.js';
import { RunPolicy } from '../src/run-policy.js';
import { Stop } from '../src/stop.js';
import { AgentState } from '../src/state.js';
import { Subagent } from '../src/subagent.js';

// The eight narrowing assertions.
//
// `make` and `withHandlers` each hand back four entry points — `stream`,
// `run`, `streamIn`, `runIn` — and each is a hand-written `as`. TypeScript
// cannot verify them: `Layer.mergeAll` will not show inference that its
// output channel is discharged, so `Effect.provide` reports `any` and the
// assertion is what makes the result precise.
//
// That means an assertion that is too NARROW is invisible. It drops a service
// from the requirement channel, the call site compiles, and the run dies the
// first time it needs what was dropped. Two separate instances of exactly that
// shipped here: `make` discarded its subagents' services, and `withHandlers`
// discarded them again by naming its result instead of subtracting from it.
//
// So every one of the eight is pinned below, and each has been mutation-tested
// against the source rather than against itself.

class Notebook extends Context.Service<Notebook, { readonly n: string }>()(
  'assertions-test/Notebook',
) {}
class Db extends Context.Service<Db, { readonly q: string }>()(
  'assertions-test/Db',
) {}
class StateCodecService extends Context.Service<
  StateCodecService,
  { readonly offset: number }
>()('assertions-test/StateCodecService') {}

const write = Tool.make('write', {
  description: 'child tool',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.Struct({ ok: Schema.Boolean }),
  dependencies: [Notebook],
});

const child = Agent.make({
  name: 'child',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(write),
}).withHandlers({ write: () => Effect.succeed({ ok: true }) });

const own = Tool.make('own', {
  description: 'parent tool',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ ok: Schema.Boolean }),
  dependencies: [Db],
});

// The fixture that separates the two concerns: a tool of its own needing one
// service, and a subagent needing a different one. A single-service fixture
// cannot tell "propagated correctly" from "happened to be the same type".
const parent = Agent.make({
  name: 'parent',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(own),
  subagents: [child],
});

const handled = parent.withHandlers({
  own: () => Effect.succeed({ ok: true }),
});

type EffR<T> = T extends Effect.Effect<infer _A, infer _E, infer R> ? R : never;
type EffE<T> = T extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;
type StrR<T> = T extends Stream.Stream<infer _A, infer _E, infer R> ? R : never;
type StrA<T> = T extends Stream.Stream<infer A, infer _E, infer _R> ? A : never;
type Has<M, U> = [M] extends [U] ? 'yes' : 'no';
type IsAny<T> = 0 extends 1 & T ? 'ANY' : 'not-any';
type IsNever<T> = [T] extends [never] ? true : false;
type IsUnknown<T> =
  IsAny<T> extends 'ANY'
    ? false
    : unknown extends T
      ? [keyof T] extends [never]
        ? true
        : false
      : false;
type Exact<A, B> =
  IsAny<A> extends 'ANY'
    ? false
    : IsAny<B> extends 'ANY'
      ? false
      : IsNever<A> extends true
        ? IsNever<B>
        : IsNever<B> extends true
          ? false
          : IsUnknown<A> extends true
            ? IsUnknown<B>
            : IsUnknown<B> extends true
              ? false
              : [A, B] extends [B, A]
                ? true
                : false;

type Handlers = Tool.HandlersFor<Agent.OwnTools<typeof parent>>;

// Guard first: `[M] extends [any]` is true for every M, so every membership
// assertion below would pass vacuously against an `any` channel.
const _m1: IsAny<EffR<ReturnType<typeof parent.run>>> = 'not-any';
const _m2: IsAny<StrR<ReturnType<typeof parent.stream>>> = 'not-any';
const _h1: IsAny<EffR<ReturnType<typeof handled.run>>> = 'not-any';
const _h2: IsAny<StrR<ReturnType<typeof handled.stream>>> = 'not-any';

// --- make's four: own service, subagent's service, and the handler term ----

const _makeRun: Has<
  Db | Notebook | Handlers,
  EffR<ReturnType<typeof parent.run>>
> = 'yes';
const _makeRunIn: Has<
  Db | Notebook | Handlers,
  EffR<ReturnType<typeof parent.runIn>>
> = 'yes';
const _makeStream: Has<
  Db | Notebook | Handlers,
  StrR<ReturnType<typeof parent.stream>>
> = 'yes';
const _makeStreamIn: Has<
  Db | Notebook | Handlers,
  StrR<ReturnType<typeof parent.streamIn>>
> = 'yes';

// --- withHandlers' four: same services survive, handler term is gone -------
//
// Both halves matter. Only the first, and attaching handlers would be a no-op;
// only the second, and it would silently swallow real requirements — which is
// the bug this file was written for.

const _handledRun: Has<
  Db | Notebook,
  EffR<ReturnType<typeof handled.run>>
> = 'yes';
const _handledRunIn: Has<
  Db | Notebook,
  EffR<ReturnType<typeof handled.runIn>>
> = 'yes';
const _handledStream: Has<
  Db | Notebook,
  StrR<ReturnType<typeof handled.stream>>
> = 'yes';
const _handledStreamIn: Has<
  Db | Notebook,
  StrR<ReturnType<typeof handled.streamIn>>
> = 'yes';

const _dischargedRun: Has<
  Handlers,
  EffR<ReturnType<typeof handled.run>>
> = 'no';
const _dischargedRunIn: Has<
  Handlers,
  EffR<ReturnType<typeof handled.runIn>>
> = 'no';
const _dischargedStream: Has<
  Handlers,
  StrR<ReturnType<typeof handled.stream>>
> = 'no';
const _dischargedStreamIn: Has<
  Handlers,
  StrR<ReturnType<typeof handled.streamIn>>
> = 'no';

// The model is still required throughout — proof the channel is a real union
// and not something that collapsed to a single member.
const _model: Has<
  LanguageModel.LanguageModel,
  EffR<ReturnType<typeof handled.run>>
> = 'yes';

type ParentBase =
  | Crypto.Crypto
  | LanguageModel.LanguageModel
  | Handlers
  | Db
  | Notebook;
type HandledBase = Crypto.Crypto | LanguageModel.LanguageModel | Db | Notebook;

const _exactPlain: Exact<Agent.Requires<typeof parent>, ParentBase> = true;
const _exactHandled: Exact<Agent.Requires<typeof handled>, HandledBase> = true;

const State = AgentState.make({
  id: 'assertion-state',
  version: '1',
  schema: Schema.Struct({ count: Schema.Number }),
  initial: () => ({ count: 0 }),
});
const TransformedState = AgentState.make({
  id: 'assertion-transformed-state',
  version: '1',
  schema: Schema.String.pipe(
    Schema.decodeTo(
      Schema.Number,
      SchemaTransformation.transformOrFail<
        number,
        string,
        StateCodecService,
        StateCodecService
      >({
        decode: (value) =>
          Effect.map(StateCodecService, ({ offset }) => Number(value) + offset),
        encode: (value) =>
          Effect.map(StateCodecService, ({ offset }) => String(value - offset)),
      }),
    ),
  ),
  initial: () => 0,
});
const _directTransformedCodecRequired: Has<
  StateCodecService,
  AgentState.Services<typeof TransformedState>
> = 'yes';
const stateTool = Tool.make('stateful', {
  description: 'state-aware handler',
  parameters: Schema.Struct({}),
  success: Schema.Struct({ count: Schema.Number }),
  failure: AgentState.Error,
  dependencies: AgentState.dependencies(State, Db),
});
const stateful = Agent.make({
  name: 'stateful',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(stateTool),
  state: State,
}).withHandlers({
  stateful: () =>
    Effect.gen(function* () {
      yield* Db;
      const state = yield* State;
      return yield* state.update(({ count }) => ({ count: count + 1 }));
    }),
});
const _stateRequired: Has<typeof State, Agent.Requires<typeof stateful>> = 'no';
const transformedStateful = Agent.make({
  name: 'transformed-stateful',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(
    Tool.make('transformed', {
      description: 'transformed state handler',
      parameters: Schema.Struct({}),
      success: Schema.Number,
      failure: AgentState.Error,
      dependencies: AgentState.dependencies(
        TransformedState,
        StateCodecService,
      ),
    }),
  ),
  state: TransformedState,
}).withHandlers({
  transformed: () =>
    Effect.gen(function* () {
      const state = yield* TransformedState;
      return yield* state.update((value) => value + 1);
    }),
});
const _transformedCodecRequired: Has<
  StateCodecService,
  Agent.Requires<typeof transformedStateful>
> = 'yes';
const _otherHandlerServiceKept: Has<
  Db,
  Agent.Requires<typeof stateful>
> = 'yes';
const durable = Conversation.make(handled, 'durable');
const _exactRecording: Exact<
  EffR<ReturnType<typeof durable.run>>,
  HandledBase | LogStore.Service | Crypto.Crypto
> = true;
const _exactPlainError: Exact<
  EffE<ReturnType<typeof handled.run>>,
  Agent.RunFailure
> = true;
const _exactRecordingError: Exact<
  EffE<ReturnType<typeof durable.run>>,
  Conversation.Error<typeof handled>
> = true;
// @ts-expect-error Durable continuation is the meaning of run, not a second method.
durable.resume;
const _exactBranch: Exact<
  EffR<ReturnType<typeof durable.branchFrom>>,
  HandledBase | LogStore.Service | Crypto.Crypto
> = true;
const _exactBranchError: Exact<
  EffE<ReturnType<typeof durable.branchFrom>>,
  Conversation.Error<typeof handled>
> = true;
const _exactFork: Exact<
  EffR<ReturnType<typeof durable.forkFrom>>,
  HandledBase | LogStore.Service | Crypto.Crypto
> = true;
const _exactForkError: Exact<
  EffE<ReturnType<typeof durable.forkFrom>>,
  Conversation.Error<typeof handled>
> = true;
// @ts-expect-error Session/runtime invocation is not public Agent API.
handled.runInSession;
// @ts-expect-error Durable protocol invocation is internal to Conversation.
handled.streamConversation;
// @ts-expect-error Durable protocol invocation is internal to delegation.
handled.runWithSession;
// @ts-expect-error Child is the only public subagent view.
type _NoLegacyNamed = Agent.Named;
const _noPublicDelegationHandler: 'handler' extends keyof typeof Subagent
  ? false
  : true = true;
const _noPublicDelegationCompiler: 'delegateTo' extends keyof typeof Subagent
  ? false
  : true = true;
const _noLegacyRecording: 'recordingTo' extends keyof typeof handled
  ? false
  : true = true;
const _noLegacyResume: 'resume' extends keyof typeof handled ? false : true =
  true;
const _noLegacyBranch: 'branchFrom' extends keyof typeof handled
  ? false
  : true = true;
const _noLegacyFork: 'forkFrom' extends keyof typeof handled ? false : true =
  true;
const _noLegacyTail: 'streamFrom' extends keyof typeof Agent ? false : true =
  true;

// @ts-expect-error Session values are created by AgentLog, not structurally.
const _fabricatedSession: AgentLog.Session = { conversationId: 'forged' };

class FirstPolicy extends Context.Service<
  FirstPolicy,
  { readonly first: true }
>()('assertions-test/FirstPolicy') {}
class SecondPolicy extends Context.Service<
  SecondPolicy,
  { readonly second: true }
>()('assertions-test/SecondPolicy') {}

const recordingPolicy = {
  prompt: (prompt: unknown) => Effect.as(FirstPolicy, prompt),
} satisfies RecordingPolicy.Policy<FirstPolicy>;
const filteredRecording = Conversation.make(
  handled,
  'filtered',
  recordingPolicy,
);
const _noAmbiguousRecordingConstructor: 'recording' extends keyof typeof Conversation
  ? false
  : true = true;
const _noSecondRecordingConstructor: 'withRecordingPolicy' extends keyof typeof Conversation
  ? false
  : true = true;
const _exactFilteredRecording: Exact<
  EffR<ReturnType<typeof filteredRecording.run>>,
  HandledBase | LogStore.Service | Crypto.Crypto | FirstPolicy
> = true;

const _noRecordingRuntime: 'Runtime' extends keyof typeof RecordingPolicy
  ? false
  : true = true;
const _noRecordingCompile: 'compile' extends keyof typeof RecordingPolicy
  ? false
  : true = true;
const _noRunRuntime: 'Runtime' extends keyof typeof RunPolicy ? false : true =
  true;
const _noRunCreate: 'create' extends keyof typeof RunPolicy ? false : true =
  true;
const _noLowercaseStateError: 'error' extends keyof typeof AgentState
  ? false
  : true = true;
const firstInterceptor = {
  beforeTurn: () =>
    Effect.gen(function* () {
      yield* FirstPolicy;
      return Interception.proceed;
    }),
};
const secondInterceptor = {
  beforeTurn: () =>
    Effect.gen(function* () {
      yield* SecondPolicy;
      return Interception.proceed;
    }),
};
const first = handled.intercepting(firstInterceptor);
const second = first.intercepting(secondInterceptor);

type ReplacementRequires = Agent.Requires<typeof second>;
const _serviceExtracted: Has<
  SecondPolicy,
  Interception.Services<typeof secondInterceptor>
> = 'yes';
const _replacementDropped: Has<FirstPolicy, ReplacementRequires> = 'no';
const _replacementAdded: Has<SecondPolicy, ReplacementRequires> = 'yes';

const _exactReplacement: Exact<
  Agent.Requires<typeof second>,
  HandledBase | SecondPolicy
> = true;

// `Interception.compose` unions the two operands' `R` rather than replacing
// one with the other — the opposite of what a second `intercepting` call
// does above — so both `FirstPolicy` and `SecondPolicy` surface on the
// composed agent's requirement channel.
const composed = handled.intercepting(
  Interception.compose(firstInterceptor, secondInterceptor),
);
const _composedUnionsRequires: Exact<
  Agent.Requires<typeof composed>,
  HandledBase | FirstPolicy | SecondPolicy
> = true;

// And `compose`'s result is passed to a single `intercepting` call, so it is
// still subject to replace-don't-stack: attaching a third interceptor drops
// both composed services, the same as replacing any other interceptor.
const replacedComposed = composed.intercepting({
  beforeTurn: () => Effect.succeed(Interception.proceed),
});
const _composedReplaced: Exact<
  Agent.Requires<typeof replacedComposed>,
  HandledBase
> = true;

class StopService extends Context.Service<
  StopService,
  { readonly stop: true }
>()('assertions-test/StopService') {}
class OtherStopService extends Context.Service<
  OtherStopService,
  { readonly otherStop: true }
>()('assertions-test/OtherStopService') {}

const serviceStop: Stop.StopCondition<{}, StopService> = () =>
  Effect.as(StopService, false);
const otherStop: Stop.StopCondition<{}, OtherStopService> = () =>
  Effect.as(OtherStopService, false);
const stopped = Agent.make({
  name: 'stopped',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(),
  stopWhen: Stop.any(serviceStop, Stop.all(otherStop)),
});

const _exactStop: Exact<
  Agent.Requires<typeof stopped>,
  LanguageModel.LanguageModel | StopService | OtherStopService
> = true;

const compiled = Agent.make({
  name: 'compiled',
  revision: '1',
  instructions: 'x',
  toolkit: Toolkit.make(own),
  subagents: [child],
  skills: [{ name: 'guide', description: 'guide', instructions: 'guide' }],
});
const _exactOwnKeys: Exact<keyof Agent.OwnTools<typeof compiled>, 'own'> = true;
const _exactCompiledKeys: Exact<
  keyof Agent.Tools<typeof compiled>,
  'own' | 'task_child' | 'load_skill'
> = true;
type CompiledEvent = StrA<ReturnType<typeof compiled.stream>>;
type CompiledPart = Extract<CompiledEvent, { readonly _tag: 'Part' }>['part'];
type Substituted = Extract<
  CompiledPart,
  { readonly resultSource: 'substituted' }
>;
const _exactSubstitutedResult: Exact<Substituted['result'], unknown> = true;

// Meta-guards ensure `Exact` cannot pass vacuously on poisoned channels.
const _rejectAny: Exact<any, HandledBase> = false;
const _rejectNever: Exact<never, HandledBase> = false;
const _rejectUnknown: Exact<unknown, HandledBase> = false;

describe('the narrowing assertions', () => {
  it('keeps every entry point honest about what it still needs', () => {
    expect(_makeRun).toBe('yes');
    expect(_handledRun).toBe('yes');
    expect(_dischargedRun).toBe('no');
    expect(Object.keys(parent.toolkit.tools).sort()).toEqual([
      'own',
      'task_child',
    ]);
    expect([
      _exactPlain,
      _exactHandled,
      _exactRecording,
      _exactPlainError,
      _exactRecordingError,
      _exactFilteredRecording,
      _exactBranch,
      _exactFork,
      _exactReplacement,
      _exactStop,
      _exactOwnKeys,
      _exactCompiledKeys,
      _exactSubstitutedResult,
      _rejectAny,
      _rejectNever,
      _rejectUnknown,
    ]).toBeDefined();
  });
});
