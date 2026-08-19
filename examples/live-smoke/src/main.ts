import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic';
import { OpenAiClient, OpenAiLanguageModel } from '@effect/ai-openai';
// Subpath imports, not the barrel: the package root re-exports `NodeRedis`,
// which imports `ioredis` at module load and is not installed here.
import * as NodeRuntime from '@effect/platform-node/NodeRuntime';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Agent } from '@sunfall/vesper-agent/agent';
import {
  Conversation,
  DurabilityError,
} from '@sunfall/vesper-agent/conversation';
import { ContextWindow } from '@sunfall/vesper-agent/context-window';
import { AgentEvents } from '@sunfall/vesper-agent/event';
import { ModelPlan } from '@sunfall/vesper-agent/model-plan';
import { Skill } from '@sunfall/vesper-agent/skill';
import { Stop } from '@sunfall/vesper-agent/stop';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogStorePg } from '@sunfall/vesper-log-pg/layer';
import { LogStore } from '@sunfall/vesper-log/log-store';
import { LogOffset } from '@sunfall/vesper-log/offset';
import { VesperPgClient } from '@sunfall/vesper-log-pg/client';
import type { ConversationRecord } from '@sunfall/vesper-log/record';
import {
  Config,
  Console,
  Context,
  Crypto,
  Effect,
  ExecutionPlan,
  Layer,
  Redacted,
  Schema,
  Stream,
} from 'effect';
import {
  AiError,
  LanguageModel,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { Command, Flag } from 'effect/unstable/cli';

// The workspace toolkit is an application dependency, not something the agent
// package composes automatically.
import { WorkspaceLocal } from '@sunfall/vesper-workspace/layer-local';
import { WorkspaceTools } from '@sunfall/vesper-workspace/tools';

// This drives a real Effect AI model — Anthropic or OpenAI, chosen with
// `--provider` — through the parts of the loop scripted models have never seen:
// a toolkit whose handlers
// do work and one of whose tools fails, delegation to a child agent that needs
// a service of its own, skills loaded on demand, the conversation log written
// and read back, branching, forking two conversations that run at once, the
// workspace toolkit against a real directory, what a provider reports once the
// prompt is cached, and both compaction triggers.
//
// It is a smoke test, not an eval: short prompts, a low output cap, and the
// fewest turns that prove the point. The one genuinely expensive phase —
// `compaction-reactive`, which has to overflow a real 200k window — is opt-in
// and excluded from `--phase all`.
//
// It costs real money and needs a real API key:
//
//   ANTHROPIC_API_KEY=... nub run example:live-smoke --phase all
//   OPENROUTER_API_KEY=... nub run example:live-smoke --provider openrouter --phase log
//   ANTHROPIC_API_KEY=... OPENAI_API_KEY=... nub run example:live-smoke --fallback-provider openai --phase log
//   ANTHROPIC_API_KEY=... nub run example:live-smoke --phase compaction-reactive
//
/** Output cap on every call. Plumbing is what is under test, not prose. */
const MAX_OUTPUT_TOKENS = 300;

const PROVIDERS = ['anthropic', 'openai', 'openrouter'] as const;
type Provider = (typeof PROVIDERS)[number];
const FALLBACK_PROVIDERS: readonly ['none', ...Provider[]] = [
  'none',
  ...PROVIDERS,
];

const DEFAULT_PROVIDER: Provider = 'anthropic';
const DEFAULT_MODELS = {
  anthropic: 'claude-sonnet-4-6',
  openai: 'gpt-5.6-luna',
  openrouter: 'openrouter/free',
} satisfies Record<Provider, string>;

const modelFor = (provider: Provider, model: string) =>
  Effect.gen(function* () {
    const apiKey = yield* Config.redacted(
      provider === 'anthropic'
        ? 'ANTHROPIC_API_KEY'
        : provider === 'openrouter'
          ? 'OPENROUTER_API_KEY'
          : 'OPENAI_API_KEY',
    );
    return provider === 'anthropic'
      ? AnthropicLanguageModel.model(model, {
          max_tokens: MAX_OUTPUT_TOKENS,
        }).pipe(Layer.provide(AnthropicClient.layer({ apiKey })))
      : OpenAiLanguageModel.model(model, {
          max_output_tokens: MAX_OUTPUT_TOKENS,
        }).pipe(
          Layer.provide(
            OpenAiClient.layer({
              apiKey,
              ...(provider === 'openrouter'
                ? { apiUrl: 'https://openrouter.ai/api/v1' }
                : {}),
            }),
          ),
        );
  });

// ---------------------------------------------------------------- reporting

const dim = (text: string): string => `\x1b[2m${text}\x1b[0m`;
const green = (text: string): string => `\x1b[32m${text}\x1b[0m`;
const red = (text: string): string => `\x1b[31m${text}\x1b[0m`;
const bold = (text: string): string => `\x1b[1m${text}\x1b[0m`;

const heading = (title: string): Effect.Effect<void> =>
  Console.log(
    `\n${bold(`── ${title} ${'─'.repeat(Math.max(0, 60 - title.length))}`)}`,
  );

/**
 * A claim the run either supports or does not.
 *
 * Printed rather than thrown, so one failed expectation does not hide the
 * phases after it — the point of the exercise is to collect differences, not
 * to stop at the first.
 */
const checks: Array<{ readonly ok: boolean; readonly claim: string }> = [];

const memoryLogLayer = LogStoreMemory.layer.pipe(
  Layer.provide(NodeServices.layer),
);

const check = (ok: boolean, claim: string): Effect.Effect<void> => {
  checks.push({ ok, claim });
  return Console.log(`  ${ok ? green('PASS') : red('FAIL')}  ${claim}`);
};

// ------------------------------------------------------------------- spend

/**
 * What this run cost, in tokens, as the library itself reported them.
 *
 * Two sources, because the two entry-point shapes report differently. A
 * streamed run's turns arrive as `finish` parts and are added as they go. A
 * `conversation.run` returns only a `Result`, whose `usage` is cumulative
 * **for the whole conversation** rather than for that run — so it is added as
 * a delta against the last figure seen for that conversation id. That stops
 * five resumptions of one conversation from counting the first turn five times.
 *
 * It undercounts one thing on purpose rather than by accident: a summarization
 * call goes through `LanguageModel.generateText`, which neither shape observes.
 * Compaction phases therefore report less than they spent.
 */
const spend = { input: 0, output: 0 };
const cumulative = new Map<string, { input: number; output: number }>();

const spent = (usage: { input: number; output: number }): void => {
  spend.input += usage.input;
  spend.output += usage.output;
};

const spentByConversation = (
  conversationId: string,
  total: { input: number; output: number },
): void => {
  const last = cumulative.get(conversationId) ?? { input: 0, output: 0 };
  spent({
    input: Math.max(0, total.input - last.input),
    output: Math.max(0, total.output - last.output),
  });
  cumulative.set(conversationId, total);
};

// ------------------------------------------------------------------- traces

interface ToolCallSeen {
  readonly id: string;
  readonly name: string;
  readonly params: unknown;
}

interface ToolResultSeen {
  readonly id: string;
  readonly name: string;
  readonly isFailure: boolean;
  readonly result: unknown;
}

interface Trace {
  text: string;
  turns: number;
  steps: number;
  partTypes: Array<string>;
  toolCalls: Array<ToolCallSeen>;
  toolResults: Array<ToolResultSeen>;
  finishReasons: Array<string>;
  usage: { input: number; output: number };
  usageReported: boolean;
  /** Every turn's raw provider usage, exactly as the adapter reported it. */
  turnUsage: Array<Response.FinishPartEncoded['usage']>;
  compactions: Array<{
    readonly step: number;
    readonly summarizedMessages: number;
    readonly keptMessages: number;
    readonly summary: string;
  }>;
}

const emptyTrace = (): Trace => ({
  text: '',
  turns: 0,
  steps: 0,
  partTypes: [],
  toolCalls: [],
  toolResults: [],
  finishReasons: [],
  usage: { input: 0, output: 0 },
  usageReported: false,
  turnUsage: [],
  compactions: [],
});

/**
 * Fold an agent's event stream into something assertable.
 *
 * Reads the provider-facing sibling of each decoded part. Keeping both forms
 * makes transformed tool schemas observable without unsafe casts and matches
 * what the log stores and a resuming run serves back.
 */
const absorb = <Tools extends Record<string, Tool.Any>>(
  trace: Trace,
  event: AgentEvents.Event<Tools>,
): void => {
  if (!AgentEvents.isPart(event)) {
    switch (event._tag) {
      case 'TurnStarted':
        trace.turns += 1;
        break;
      case 'Completed':
        trace.text = event.text;
        trace.steps = event.steps;
        trace.usage = event.usage;
        break;
      case 'Compacted':
        trace.compactions.push({
          step: event.step,
          summarizedMessages: event.summarizedMessages,
          keptMessages: event.keptMessages,
          summary: event.summary,
        });
        break;
      default:
        break;
    }
    return;
  }

  const part = event.encodedPart;
  if (!trace.partTypes.includes(part.type)) trace.partTypes.push(part.type);

  switch (part.type) {
    case 'tool-call':
      trace.toolCalls.push({
        id: part.id,
        name: part.name,
        params: part.params,
      });
      return;
    case 'tool-result':
      trace.toolResults.push({
        id: part.id,
        name: part.name,
        isFailure: part.isFailure === true,
        result: part.result,
      });
      return;
    case 'finish':
      trace.finishReasons.push(part.reason);
      trace.turnUsage.push(part.usage);
      spent({
        input: part.usage.inputTokens.total ?? 0,
        output: part.usage.outputTokens.total ?? 0,
      });
      if (
        part.usage.inputTokens.total !== undefined ||
        part.usage.outputTokens.total !== undefined
      ) {
        trace.usageReported = true;
      }
      return;
    default:
      return;
  }
};

const observe = <Tools extends Record<string, Tool.Any>, E, R>(
  events: Stream.Stream<AgentEvents.Event<Tools>, E, R>,
): Effect.Effect<Trace, E, R> =>
  Effect.gen(function* () {
    const trace = emptyTrace();
    yield* Stream.runForEach(events, (event) =>
      Effect.sync(() => {
        absorb(trace, event);
      }),
    );
    return trace;
  });

const report = (trace: Trace): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* Console.log(
      dim(
        `  parts: ${trace.partTypes.join(', ')}` +
          `\n  turns: ${trace.turns}  finish: ${trace.finishReasons.join(',')}` +
          `\n  usage: in=${trace.usage.input} out=${trace.usage.output}` +
          `\n  tools: ${trace.toolCalls.map((call) => call.name).join(', ') || '(none)'}`,
      ),
    );
    yield* Console.log(
      `  ${dim('answer:')} ${trace.text.replace(/\s+/g, ' ').slice(0, 400)}`,
    );
  });

// ------------------------------------------------------------------ the log

const readAll = <A extends Agent.Any>(
  agent: A,
  conversationId: string,
): Effect.Effect<
  ReadonlyArray<ConversationRecord.Envelope>,
  LogStore.LogStoreError,
  LogStore.Service
> =>
  Conversation.make(agent, conversationId)
    .records()
    .pipe(
      Stream.runCollect,
      Effect.map((records) => Array.from(records)),
    );

const tagsOf = (
  records: ReadonlyArray<ConversationRecord.Envelope>,
): Array<string> => records.map((envelope) => envelope.record._tag);

// -------------------------------------------------------- phase: real tools

class Catalogue extends Context.Service<
  Catalogue,
  { readonly units: (sku: string) => Effect.Effect<number> }
>()('smoke/Catalogue') {}

const catalogueLayer = Layer.succeed(Catalogue, {
  units: (sku: string) =>
    Effect.succeed(sku === 'RD-1000' ? 42 : sku === 'RD-2000' ? 0 : 7),
});

const checkStock = Tool.make('check_stock', {
  description: 'How many units of one SKU are in the warehouse.',
  parameters: Schema.Struct({ sku: Schema.String }),
  success: Schema.Struct({ sku: Schema.String, units: Schema.Number }),
  dependencies: [Catalogue],
});

const chargeCard = Tool.make('charge_card', {
  description: 'Charge the customer’s card, in cents.',
  parameters: Schema.Struct({ amountCents: Schema.Number }),
  success: Schema.Struct({ authorization: Schema.String }),
  // The failure path, which is the point of this tool: the processor always
  // declines, so a real model has to read a real failure out of a real tool
  // result and say what happened.
  failure: Schema.Struct({ declined: Schema.String, code: Schema.String }),
  failureMode: 'return',
});

const shopAgent = Agent.make({
  name: 'shop',
  revision: '1',
  instructions: [
    'You are a warehouse assistant with two tools.',
    'Use them; never guess a stock level or invent an authorization code.',
    'When a tool fails, say which tool failed and quote the code it returned.',
    'Answer in at most three sentences.',
  ].join('\n'),
  toolkit: Toolkit.make(checkStock, chargeCard),
  stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(6)),
}).withHandlers({
  check_stock: ({ sku }) =>
    Effect.gen(function* () {
      const catalogue = yield* Catalogue;
      return { sku, units: yield* catalogue.units(sku) };
    }),
  charge_card: () =>
    Effect.fail({
      declined: 'issuer declined the charge',
      code: 'DO_NOT_HONOR',
    }),
});

const toolsPhase = Effect.gen(function* () {
  yield* heading('tools — real handlers, and one that fails');

  const trace = yield* observe(
    shopAgent.stream(
      'Check stock for SKU RD-1000, then charge the card for 4999 cents, ' +
        'then tell me what happened.',
    ),
  );

  yield* report(trace);

  yield* check(
    trace.toolCalls.some((call) => call.name === 'check_stock'),
    'the model called check_stock',
  );
  yield* check(
    trace.toolCalls.some((call) => call.name === 'charge_card'),
    'the model called charge_card',
  );
  yield* check(
    trace.toolResults.some(
      (result) => result.name === 'charge_card' && result.isFailure,
    ),
    'the failing tool came back as a tool result with isFailure=true',
  );
  yield* check(
    /DO_NOT_HONOR/i.test(trace.text),
    'the model read the failure payload and quoted the decline code',
  );
  yield* check(
    trace.usageReported && trace.usage.input > 0 && trace.usage.output > 0,
    'the provider reported usage the loop could accumulate',
  );
  yield* check(
    trace.partTypes.includes('tool-params-start') &&
      trace.partTypes.includes('tool-params-delta'),
    'the official provider streamed Effect tool-parameter parts',
  );
}).pipe(Effect.provide(catalogueLayer));

// ---------------------------------------------------- phase: subagent depth

class Archive extends Context.Service<
  Archive,
  { readonly fact: (topic: string) => Effect.Effect<string> }
>()('smoke/Archive') {}

const archiveLayer = Layer.succeed(Archive, {
  fact: (topic: string) =>
    Effect.succeed(
      topic.toLowerCase().includes('kiln')
        ? 'The north kiln was commissioned in 1974 and fires at 1280 degrees.'
        : `No archive entry for ${topic}.`,
    ),
});

const lookupFact = Tool.make('lookup_fact', {
  description: 'Look one topic up in the company archive.',
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ entry: Schema.String }),
  // Declared here, and this is what has to survive delegation: the parent
  // below lists `archivist` as a subagent and never mentions `Archive`, so a
  // parent whose requirement channel dropped it would fail at the moment the
  // model first delegates rather than at the call site.
  dependencies: [Archive],
});

const archivist = Agent.make({
  name: 'archivist',
  revision: '1',
  description:
    'Answers one narrow question from the company archive. Give it a single ' +
    'topic and nothing else.',
  instructions:
    'Look the topic up with lookup_fact and report exactly what the archive ' +
    'says. Never answer from memory.',
  toolkit: Toolkit.make(lookupFact),
}).withHandlers({
  lookup_fact: ({ topic }) =>
    Effect.gen(function* () {
      const archive = yield* Archive;
      return { entry: yield* archive.fact(topic) };
    }),
});

const curator = Agent.make({
  name: 'curator',
  revision: '1',
  instructions: [
    'You write short museum labels.',
    'You have no archive access yourself — delegate every factual lookup to ' +
      'the archivist agent and use what it reports.',
    'Answer in one sentence.',
  ].join('\n'),
  toolkit: Toolkit.make(),
  subagents: [archivist],
});

const delegatePhase = Effect.gen(function* () {
  yield* heading('subagents — delegation, child sessions, service propagation');

  const conversationId = `smoke-delegate-${Date.now()}`;
  const trace = yield* observe(
    Conversation.make(curator, conversationId).stream(
      'When was the north kiln commissioned, and how hot does it fire?',
    ),
  );

  yield* report(trace);

  const parentRecords = yield* readAll(curator, conversationId).pipe(
    Effect.orDie,
  );
  const childReference = parentRecords.find(
    (envelope) => envelope.record._tag === 'ChildSession',
  );

  yield* check(
    trace.toolCalls.some((call) => call.name === 'task_archivist'),
    'the parent delegated through the task_archivist tool',
  );
  // Asserted on the delegation tool's *result*, not on the parent's prose.
  //
  // The property under test is that `Archive` reached the child's handler
  // across the delegation boundary, and the tool result is where that becomes
  // observable. Reading it out of the parent's final sentence instead makes the
  // check a question about how tersely a given model summarises: the same run
  // that passed on Haiku failed on `gpt-5.6-luna`, which answered correctly in
  // half the words and dropped one of the two numbers.
  const delegated = trace.toolResults.find(
    (result) => result.name === 'task_archivist',
  );
  const reported = JSON.stringify(delegated?.result ?? null);

  yield* check(
    /1974/.test(reported) && /1280/.test(reported),
    'the child’s service reached its handler and the fact came back to the parent',
  );
  yield* check(
    /kiln/i.test(trace.text) && /1974|1280/.test(trace.text),
    'the parent answered from what the child reported',
  );
  yield* check(
    childReference !== undefined,
    'the parent log recorded a ChildSession reference',
  );

  if (childReference?.record._tag === 'ChildSession') {
    const childRecords = yield* readAll(
      archivist,
      childReference.record.childConversationId,
    ).pipe(Effect.orDie);

    yield* Console.log(
      dim(
        `  child conversation ${childReference.record.childConversationId}` +
          `\n  child records: ${tagsOf(childRecords).join(', ')}`,
      ),
    );

    yield* check(
      childRecords.some(
        (envelope) =>
          envelope.record._tag === 'ToolCall' &&
          envelope.record.name === 'lookup_fact',
      ),
      'the child recorded its own tool call in its own conversation',
    );
    yield* check(
      childReference.record.depth === 1,
      'the child session recorded delegation depth 1',
    );
  }
}).pipe(Effect.provide(archiveLayer));

// ----------------------------------------------------------- phase: skills

const wirePolicy: Skill.Skill = {
  name: 'wire_transfer_policy',
  description: 'Limits and cut-off times for outbound wires.',
  instructions: [
    'Outbound wires settle same-day if submitted before 14:30 Mountain Time.',
    'The single-transaction ceiling is 47,500 dollars.',
    'A wire above the ceiling must be split, and each part needs its own ' +
      'dual approval from a second signer.',
  ].join('\n'),
};

const refundPolicy: Skill.Skill = {
  name: 'refund_policy',
  description: 'When a refund is allowed and who approves it.',
  instructions:
    'Refunds are allowed within 30 days of delivery, and above 500 dollars ' +
    'they need a manager’s approval.',
};

const treasurer = Agent.make({
  name: 'treasurer',
  revision: '1',
  instructions: [
    'You answer operations questions about payments.',
    'You do not know the policies by heart. Load the relevant skill before ' +
      'answering, and answer only from what it says.',
  ].join('\n'),
  toolkit: Toolkit.make(),
  skills: [wirePolicy, refundPolicy],
});

const skillsPhase = Effect.gen(function* () {
  yield* heading('skills — catalog in the prompt, body loaded on demand');

  yield* check(
    treasurer.instructions.includes('wire_transfer_policy') &&
      !treasurer.instructions.includes('47,500'),
    'the system prompt carries the catalog but not the bodies',
  );

  const trace = yield* observe(
    treasurer.stream('Can I wire 60,000 dollars in one go this afternoon?'),
  );

  yield* report(trace);

  yield* check(
    trace.toolCalls.some((call) => call.name === Skill.TOOL_NAME),
    'the model called load_skill',
  );
  yield* check(
    trace.toolCalls.some(
      (call) =>
        call.name === Skill.TOOL_NAME &&
        JSON.stringify(call.params).includes('wire_transfer_policy'),
    ),
    'it asked for the wire skill, from the literal union the tool advertises',
  );
  yield* check(
    /47,?500/.test(trace.text),
    'the answer quotes a limit that exists only in the loaded skill body',
  );
});

// --------------------------------- phase: run, continue, branch conversation

const notetaker = Agent.make({
  name: 'notetaker',
  revision: '1',
  instructions: [
    'You keep track of what the user tells you about one shipment.',
    'Answer in one short sentence, using only what you have been told in ' +
      'this conversation. If you were not told, say you were not told.',
  ].join('\n'),
  toolkit: Toolkit.make(),
});

/** A conversation with two runs, whose second run supersedes the first. */
const seedConversation = (conversationId: string) =>
  Effect.gen(function* () {
    const conversation = Conversation.make(notetaker, conversationId);
    const first = yield* conversation.run(
      'The shipment container id is CONTAINER-ALPHA. Acknowledge it in five words.',
    );
    yield* Console.log(
      dim(`  run 1: ${first.text.replace(/\s+/g, ' ').slice(0, 120)}`),
    );

    const records = yield* readAll(notetaker, conversationId).pipe(
      Effect.orDie,
    );
    const afterFirstRun = records[records.length - 1]!.offset;

    const second = yield* conversation.run(
      'Correction: the container id is CONTAINER-BETA. Acknowledge it in five words.',
    );
    yield* Console.log(
      dim(`  run 2: ${second.text.replace(/\s+/g, ' ').slice(0, 120)}`),
    );

    // `conversation.run` reports what *this run* spent, so these add
    // directly — and they establish the conversation's running total, which is
    // what the resumption below reports cumulatively.
    spent(first.usage);
    spent(second.usage);
    cumulative.set(conversationId, {
      input: first.usage.input + second.usage.input,
      output: first.usage.output + second.usage.output,
    });

    return { afterFirstRun, usage: second.usage };
  });

const logPhase = Effect.gen(function* () {
  yield* heading('the log — run, then continue from records alone');

  const conversationId = `smoke-log-${Date.now()}`;
  const seeded = yield* seedConversation(conversationId);

  const records = yield* readAll(notetaker, conversationId).pipe(Effect.orDie);
  yield* Console.log(dim(`  records: ${tagsOf(records).join(', ')}`));

  const snapshot = yield* Conversation.make(notetaker, conversationId)
    .records()
    .pipe(Stream.runCollect);
  yield* check(
    snapshot.length === records.length,
    'records() returned the current finite snapshot and completed',
  );

  const followed = yield* Conversation.make(notetaker, conversationId)
    .follow(records[records.length - 2]!.offset)
    .pipe(Stream.take(1), Stream.runCollect);
  yield* check(
    followed[0]?.offset === records[records.length - 1]?.offset,
    'follow() replayed records after its exclusive cursor',
  );

  // A different agent value, and therefore a `Chat` this process has never
  // held. Everything it knows has to come out of the store.
  const conversation = Conversation.make(notetaker, conversationId);
  const resumed = yield* conversation.run(
    'What is the container id? Answer with just the id.',
  );
  spentByConversation(conversationId, resumed.usage);

  yield* Console.log(`  ${dim('resumed answer:')} ${resumed.text.trim()}`);

  yield* check(
    /CONTAINER-BETA/i.test(resumed.text),
    'the resumed run rebuilt both earlier runs and used the correction',
  );
  yield* check(
    resumed.usage.input > seeded.usage.input,
    'resumed usage is cumulative across the conversation, not just this run',
  );
  yield* check(
    records.filter((envelope) => envelope.record._tag === 'RunSettled')
      .length === 2,
    'both seeded runs settled',
  );

  yield* heading('branching — the model sees the rewritten history');

  const branched = yield* conversation.branchFrom(
    seeded.afterFirstRun,
    'What is the container id? Answer with just the id.',
  );
  spentByConversation(conversationId, branched.usage);

  // A fork's reported usage counts the prefix it copied, which the ancestor was
  // already billed for and this tally has already counted. Seeding each fork
  // with the ancestor's running total is what keeps that from being paid for
  // twice here.
  const inherited = cumulative.get(conversationId) ?? { input: 0, output: 0 };

  yield* Console.log(`  ${dim('branched answer:')} ${branched.text.trim()}`);

  const afterBranch = yield* readAll(notetaker, conversationId).pipe(
    Effect.orDie,
  );

  yield* check(
    afterBranch.some((envelope) => envelope.record._tag === 'BranchedFrom'),
    'branching cost exactly one BranchedFrom marker in the same stream',
  );
  yield* check(
    /CONTAINER-ALPHA/i.test(branched.text) &&
      !/CONTAINER-BETA/i.test(branched.text),
    'the branch answered from the pre-correction history',
  );

  yield* heading('forking — two conversations from one prefix, at once');

  const left = `${conversationId}-fork-left`;
  const right = `${conversationId}-fork-right`;
  cumulative.set(left, inherited);
  cumulative.set(right, inherited);

  const [leftResult, rightResult] = yield* Effect.all(
    [
      conversation.forkFrom(
        seeded.afterFirstRun,
        left,
        'Repeat the container id, then say the word LEFT.',
      ),
      conversation.forkFrom(
        seeded.afterFirstRun,
        right,
        'Repeat the container id, then say the word RIGHT.',
      ),
    ],
    { concurrency: 2 },
  );
  spentByConversation(left, leftResult.usage);
  spentByConversation(right, rightResult.usage);

  yield* Console.log(
    `  ${dim('left:')} ${leftResult.text.trim().slice(0, 120)}` +
      `\n  ${dim('right:')} ${rightResult.text.trim().slice(0, 120)}`,
  );

  const leftRecords = yield* readAll(notetaker, left).pipe(Effect.orDie);
  const rightRecords = yield* readAll(notetaker, right).pipe(Effect.orDie);

  yield* check(
    /LEFT/.test(leftResult.text) && /RIGHT/.test(rightResult.text),
    'both forks ran to completion against the live provider concurrently',
  );
  yield* check(
    /CONTAINER-ALPHA/i.test(leftResult.text) &&
      /CONTAINER-ALPHA/i.test(rightResult.text),
    'both forks inherited the ancestor prefix rather than the later correction',
  );
  yield* check(
    leftRecords.some((envelope) => envelope.record._tag === 'RunStarted') &&
      rightRecords.some((envelope) => envelope.record._tag === 'RunStarted'),
    'each fork is its own stream with its own producer claim',
  );
  yield* check(
    !leftRecords.some((envelope) => envelope.record._tag === 'BranchedFrom'),
    'a fork copies the prefix without inheriting the ancestor’s branch marker',
  );

  const leftConversation = Conversation.make(notetaker, left);
  const leftContinued = yield* leftConversation.run(
    'Remember the word LEFTWARD. Reply with just that word.',
  );
  spentByConversation(left, leftContinued.usage);
  const rightContinued = yield* Conversation.make(notetaker, right).run(
    'What directional word did I ask the sibling conversation to remember? ' +
      'If you were not told one, say NOT TOLD.',
  );
  spentByConversation(right, rightContinued.usage);
  yield* check(
    /LEFTWARD/i.test(leftContinued.text) &&
      !/LEFTWARD/i.test(rightContinued.text),
    'forks continued independently after sharing the same prefix',
  );

  yield* heading('signals — queued steering and cancellation');

  const signalId = `${conversationId}-signals`;
  const signalled = Conversation.make(notetaker, signalId);
  yield* signalled.send({
    kind: 'steer',
    text: 'Include the exact token STEERED in your answer.',
    source: 'live-smoke',
  });
  const steered = yield* signalled.run(
    'The shipment is delayed. Acknowledge briefly.',
  );
  spent(steered.usage);
  cumulative.set(signalId, steered.usage);
  yield* check(
    /STEERED/.test(steered.text),
    'a steer queued before the run changed the next provider prompt',
  );

  yield* signalled.send({
    kind: 'cancel',
    text: 'operator cancelled the next turn',
    source: 'live-smoke',
  });
  const cancelled = yield* signalled.run('Answer with SHOULD_NOT_APPEAR.');
  spentByConversation(signalId, cancelled.usage);
  const signalRecords = yield* signalled.records().pipe(Stream.runCollect);
  yield* check(
    cancelled.outcome === 'cancelled' &&
      !/SHOULD_NOT_APPEAR/.test(cancelled.text),
    'a queued cancel stopped the next turn before model output',
  );
  yield* check(
    Array.from(signalRecords).filter(
      ({ record }) => record._tag === 'SignalReceived',
    ).length === 2,
    'both delivered signals were acknowledged in conversation records',
  );
});

// --------------------------------------- phase: PostgreSQL runtime replacement

const durabilityPhase = Effect.gen(function* () {
  yield* heading('durability — resume through a fresh Postgres pool');

  const databaseUrl = yield* Config.redacted('VESPER_DATABASE_URL');
  const conversationId = `smoke-durable-${Date.now()}`;
  const storeLayer = () =>
    LogStorePg.layer().pipe(
      Layer.provide(
        VesperPgClient.layer({
          url: Redacted.make(Redacted.value(databaseUrl)),
        }),
      ),
      Layer.provide(NodeCrypto.layer),
    );

  yield* Effect.gen(function* () {
    const conversation = Conversation.make(notetaker, conversationId);
    const result = yield* conversation.run(
      'The durable shipment code is DURABLE-ORCHID. Reply with just the code.',
    );
    spent(result.usage);
    cumulative.set(conversationId, result.usage);
    yield* conversation.send({
      kind: 'steer',
      text: 'Include the exact token AFTER-RESTART in the next answer.',
      source: 'live-smoke',
    });
  }).pipe(Effect.provide(storeLayer()), Effect.scoped);

  yield* Console.log(dim('  first Postgres pool closed; opening a fresh one'));

  const { recordsBefore, recordsAfter, resumed } = yield* Effect.gen(
    function* () {
      const conversation = Conversation.make(notetaker, conversationId);
      const recordsBefore = yield* conversation
        .records()
        .pipe(Stream.runCollect);
      const resumed = yield* conversation.run(
        'What is the durable shipment code? Answer briefly.',
      );
      spentByConversation(conversationId, resumed.usage);
      const recordsAfter = yield* conversation
        .records()
        .pipe(Stream.runCollect);
      return { recordsBefore, recordsAfter, resumed };
    },
  ).pipe(Effect.provide(storeLayer()), Effect.scoped);

  yield* Console.log(`  ${dim('resumed answer:')} ${resumed.text.trim()}`);
  yield* check(
    Array.from(recordsBefore).some(
      ({ record }) => record._tag === 'RunSettled',
    ),
    'a fresh Postgres pool read the run written by the disposed pool',
  );
  yield* check(
    /DURABLE-ORCHID/i.test(resumed.text) && /AFTER-RESTART/.test(resumed.text),
    'resume rebuilt history and delivered the signal persisted by the first pool',
  );
  yield* check(
    Array.from(recordsAfter).some(
      ({ record }) => record._tag === 'SignalReceived',
    ),
    'the resumed run durably acknowledged the queued signal',
  );
});

// ------------------------------------------------------- phase: workspace

const explorer = Agent.make({
  name: 'explorer',
  revision: '1',
  instructions: [
    'You inspect a small code workspace with the tools you have been given.',
    'Paths are relative to the workspace root.',
    'Do the work with tools; never guess a file’s contents.',
  ].join('\n'),
  toolkit: WorkspaceTools.toolkit,
  stopWhen: Stop.any(Stop.noToolCalls(), Stop.maxSteps(10)),
});

const workspacePhase = Effect.gen(function* () {
  yield* heading('workspace toolkit — a real directory, driven by the model');

  const root = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), 'ai-smoke-'))),
    (path) => Effect.promise(() => rm(path, { recursive: true, force: true })),
  );

  yield* Effect.promise(async () => {
    await writeFile(
      join(root, 'README.md'),
      '# Kiln service\n\nThe firing schedule lives in schedule.txt.\n',
    );
    await writeFile(
      join(root, 'schedule.txt'),
      // Trailing newline on purpose: `wc -l` counts newlines, and without one
      // the shell's answer and the file's line count disagree, which makes the
      // "the model reported what the command printed" check pass for the wrong
      // reason.
      `${['06:00 preheat', '09:00 soak', '14:30 cool', '18:00 unload'].join('\n')}\n`,
    );
    await writeFile(join(root, 'notes.md'), 'Nothing to see here.\n');
  });

  yield* Console.log(dim(`  workspace: ${root}`));

  const trace = yield* observe(
    explorer.stream(
      'List the files in the workspace, read schedule.txt, then run a shell ' +
        'command that counts the lines in schedule.txt. Report the file list, ' +
        'the cool-down time, and the line count the command printed.',
    ),
  ).pipe(Effect.provide(WorkspaceTools.rootLayer(root)));

  yield* report(trace);

  const called = (name: string): boolean =>
    trace.toolCalls.some((call) => call.name === name);

  yield* check(called('list_files'), 'the model called list_files');
  yield* check(called('read_file'), 'the model called read_file');
  yield* check(called('run_shell'), 'the model called run_shell');
  yield* check(
    /14:30/.test(trace.text),
    'the answer carries content that only came from reading the real file',
  );
  yield* check(
    /\b4\b/.test(trace.text),
    'the answer carries the line count the real shell command printed',
  );
  yield* check(
    trace.toolResults.every((result) => !result.isFailure),
    'no workspace tool failed — the JSON schemas the provider was shown ' +
      'were accepted, and every tool call decoded',
  );
}).pipe(
  Effect.scoped,
  Effect.provide(
    Layer.mergeAll(
      WorkspaceTools.layer,
      WorkspaceTools.shellEnabledCommandPolicyLayer,
      WorkspaceTools.defaultFilesystemPolicyLayer,
    ),
  ),
  // The local driver needs a process spawner, and it is provided here rather
  // than inherited from the program's `NodeServices` so this phase's
  // requirement channel is `LanguageModel` alone, like every other phase's.
  Effect.provide(WorkspaceLocal.layer.pipe(Layer.provide(NodeServices.layer))),
);

// ------------------------------------------------------------- long prompts

const FILLER_SENTENCES = [
  'The kiln operator recorded the flue temperature at the top of every hour.',
  'A stoneware glaze devitrifies if the cooling ramp passes too slowly.',
  'The 1974 commissioning report lists eleven separate thermocouple faults.',
  'Saggars are stacked with a finger of clearance to let the draught move.',
];

/** Roughly `approximateTokens` tokens of prose, at four characters per token. */
const filler = (approximateTokens: number): string => {
  const targetChars = approximateTokens * 4;
  const parts: Array<string> = [];
  let length = 0;
  let index = 0;
  while (length < targetChars) {
    const sentence = `${index}. ${FILLER_SENTENCES[index % FILLER_SENTENCES.length]!}`;
    parts.push(sentence);
    length += sentence.length + 1;
    index += 1;
  }
  return parts.join('\n');
};

// ---------------------------------------------------------- phase: usage

/**
 * What the selected official provider reports for a long, resumed prompt.
 */
const parrot = Agent.make({
  name: 'parrot',
  revision: '1',
  instructions:
    'You repeat one word back. Never say anything else, ever, under any ' +
    'circumstances.',
  toolkit: Toolkit.make(),
  compaction: false,
});

const usagePhase = Effect.gen(function* () {
  yield* heading('usage — what the provider reports for a long prompt');

  const bulk = filler(6_000);
  const conversationId = `smoke-usage-${Date.now()}`;

  const conversation = Conversation.make(parrot, conversationId);
  const first = yield* observe(
    conversation.stream(
      `Ignore this log; it is only here to make the prompt long.\n\n${bulk}\n\nSay ONE.`,
    ),
  );
  // Already counted through the stream's `finish` parts; recorded here so the
  // cumulative figure the resumption reports is charged as a delta.
  cumulative.set(conversationId, first.usage);

  // The second call rebuilds the first from records, preserving the long prefix.
  const second = yield* conversation.run('Say TWO.');
  spentByConversation(conversationId, second.usage);
  const secondRecords = yield* readAll(parrot, conversationId).pipe(
    Effect.orDie,
  );

  const raw = first.turnUsage[0];
  yield* Console.log(
    dim(
      `  approximate prompt size: ${Math.round(bulk.length / 4)} tokens` +
        `\n  raw provider usage, call 1: ${JSON.stringify(raw)}` +
        `\n  loop’s accumulated usage, call 1: ${JSON.stringify(first.usage)}` +
        `\n  conversation usage after call 2: ${JSON.stringify(second.usage)}`,
    ),
  );

  const input = raw?.inputTokens;
  const cached = (input?.cacheRead ?? 0) + (input?.cacheWrite ?? 0);

  yield* check(
    (input?.total ?? 0) >= cached,
    'inputTokens.total includes any cache usage the provider reports',
  );
  yield* check(
    first.usage.input > cached,
    'the loop’s accumulated input usage reflects the size of the real prompt',
  );
  yield* check(
    secondRecords.some((envelope) => envelope.record._tag === 'TurnFinished'),
    'the second call was recorded',
  );
});

// ----------------------------------------------------- phase: compaction

/**
 * The proactive trigger, made affordable by lying about the context window.
 *
 * A `contextWindow` of a few thousand tokens is the cheap route the brief
 * names: the estimate crosses `contextWindow - reserveTokens` after two or
 * three ordinary turns, so compaction fires from the estimator with no
 * oversized prompt anywhere.
 */
const rambler = Agent.make({
  name: 'rambler',
  revision: '1',
  instructions:
    'You are a patient tour guide. Answer each question in about eighty ' +
    'words, in prose.',
  toolkit: Toolkit.make(),
  compaction: {
    contextWindow: 900,
    reserveTokens: 300,
    keepRecentTokens: 200,
    instructions:
      'Summarize what has been discussed so far, preserving every fact the ' +
      'user stated about themselves, including their name.',
  },
});

const compactionProactivePhase = Effect.gen(function* () {
  yield* heading('compaction — proactive, from the estimator');

  const conversationId = `smoke-compact-${Date.now()}`;

  // repeated `conversation.run` calls, which continue durable history.
  //
  // A first `conversation.run` starts an empty conversation; every later call
  // rebuilds its active history from records before continuing, so this needs
  // no special case for the opening turn.
  const prompts = [
    'My name is Wren and I collect antique barometers. Tell me about the ' +
      'history of the aneroid barometer.',
    'Now tell me about the mercury barometer, at the same length.',
    'And the storm glass — same length again.',
    'And the hygrometer — same length again.',
    'What is my name, and what do I collect?',
  ];

  let last = '';
  const conversation = Conversation.make(rambler, conversationId);
  const compactionsAfter: Array<number> = [];
  for (const prompt of prompts) {
    const result = yield* conversation.run(prompt);
    spentByConversation(conversationId, result.usage);
    last = result.text;
    const soFar = yield* readAll(rambler, conversationId).pipe(Effect.orDie);
    const count = soFar.filter(
      (envelope) => envelope.record._tag === 'Compacted',
    ).length;
    compactionsAfter.push(count);
    yield* Console.log(
      dim(
        `  turn: cumulative usage in=${result.usage.input} out=${result.usage.output}` +
          `  compactions so far=${count}`,
      ),
    );
  }

  const records = yield* readAll(rambler, conversationId).pipe(Effect.orDie);
  const compactedRecords = records.filter(
    (envelope) => envelope.record._tag === 'Compacted',
  );

  yield* Console.log(
    `  ${dim('final answer:')} ${last.replace(/\s+/g, ' ').slice(0, 200)}`,
  );

  yield* check(
    compactedRecords.length > 0,
    'compaction fired proactively and was written to the log',
  );
  yield* check(
    /Wren/i.test(last) && /barometer/i.test(last),
    'the summary carried the facts forward across the rewrite',
  );

  const first = compactedRecords[0]?.record;
  if (first?._tag === 'Compacted') {
    yield* Console.log(
      dim(
        `  Compacted: summarized=${first.summarizedMessages} ` +
          `kept=${first.keptMessages} firstKept=${first.firstKept}` +
          `\n  summary: ${first.summary.replace(/\s+/g, ' ').slice(0, 200)}`,
      ),
    );
    yield* check(
      first.firstKept !== LogOffset.START,
      'the Compacted record resolved firstKept to a real record in the log',
    );
    yield* check(
      first.summarizedMessages > 0 && first.keptMessages > 0,
      'the rewrite replaced older messages and kept a verbatim tail',
    );
  }

  const resumed = yield* conversation.run(
    'One more time: what do I collect? Two words.',
  );
  spentByConversation(conversationId, resumed.usage);
  compactionsAfter.push(
    (yield* readAll(rambler, conversationId).pipe(Effect.orDie)).filter(
      (envelope) => envelope.record._tag === 'Compacted',
    ).length,
  );

  yield* Console.log(
    `  ${dim('resumed:')} ${resumed.text.trim().slice(0, 120)}`,
  );
  yield* check(
    /barometer/i.test(resumed.text),
    'a resumed compacted conversation still knows what the summary preserved',
  );

  // The property compaction-aware resumption buys: a turn that follows a
  // compaction rebuilds to something the summary already shrank, so it does not
  // compact again immediately.
  //
  // Stated over every turn including the closing resumption, rather than over
  // the prompt loop alone. Which turn compaction lands on is the model's to
  // decide — Haiku crossed the threshold on turn four and `gpt-5.6-luna`, being
  // terser, not until turn five — and a check that needs a *later* prompt in
  // the array is really a check on where the model happened to trip the
  // estimator. It will compact again eventually; this agent's window is 900
  // tokens. What must not happen is compacting on the very next turn.
  yield* check(
    compactionsAfter.some(
      (count, index) =>
        index > 0 && count > 0 && count === compactionsAfter[index - 1],
    ),
    'a turn after a compaction rebuilt without compacting again',
  );
});

/**
 * The reactive trigger, against a genuine 200k-token rejection.
 *
 * **This phase costs real money, which is why it is excluded from `--phase
 * all`.** Anthropic's window is fixed at 200k and there is no cheaper model
 * with a smaller one, so the only way to make a real provider say "this no
 * longer fits" is to send more than 200k tokens. The rejected request itself is
 * free — it fails validation before anything is billed — but the turn that
 * builds the history up to the edge, and the summarization call over it, are
 * not: roughly 105k input tokens each, about 25 cents at Haiku list price.
 *
 * Two halves of ~105k rather than the 120k this was first written with: the
 * only thing that matters is crossing 200k, and the margin is pure cost.
 *
 * What it found — the retry re-sending the very input that overflowed — is
 * pinned by three faux-provider cases in `@sunfall/vesper-agent`'s `compaction.test.ts`,
 * so the fix does not need this phase re-run to stay honest. Run it when
 * Effect AI's provider error mapping changes, which is the half only a real
 * provider can check.
 */
const archivistOfLogs = Agent.make({
  name: 'log-reader',
  revision: '1',
  instructions:
    'You read kiln logs. Answer in one short sentence and never repeat the ' +
    'log back.',
  toolkit: Toolkit.make(),
  // Default compaction policy: no `contextWindow`, so the proactive trigger is
  // off and only the reactive one can fire. That is the path that has never
  // seen a real provider rejection.
});

const compactionReactivePhase = Effect.gen(function* () {
  yield* heading('compaction — reactive, from a real 200k overflow');

  // Anthropic counted 136k tokens for `filler(120_000)`, so this rule of thumb
  // runs about 14% under. 92k of estimate is ~105k real, and two of them clear
  // the 200k window with enough margin to be reliable and not a token more.
  const first = filler(92_000);
  const second = filler(92_000);
  yield* Console.log(
    dim(
      `  turn 1 filler: ${first.length} chars; turn 2 filler: ${second.length} chars`,
    ),
  );

  const conversationId = `smoke-overflow-${Date.now()}`;

  // `conversation.run` for both turns, so the second turn's prompt is the
  // first turn rebuilt from records *plus* the new half — which puts it over the
  // window. An unrecorded Agent run would start from an empty `Chat` each time
  // and neither call would overflow.
  const conversation = Conversation.make(archivistOfLogs, conversationId);
  const one = yield* conversation.run(
    `Here is the first half of the log.\n\n${first}\n\nHow many entries did I just give you, roughly?`,
  );
  spentByConversation(conversationId, one.usage);
  yield* Console.log(
    dim(`  turn 1: in=${one.usage.input} out=${one.usage.output}`),
  );

  const two = yield* conversation
    .run(
      `Here is the second half.\n\n${second}\n\nSay the word OVERFLOWED and nothing else.`,
    )
    .pipe(
      Effect.map((result) => result.text),
      // Reported rather than fatal: a reactive path that still fails after
      // compacting is exactly the outcome worth seeing written down.
      Effect.catchCause((cause) =>
        Effect.as(
          Console.log(`  ${red('turn 2 failed')}\n${dim(String(cause))}`),
          '',
        ),
      ),
    );

  const records = yield* readAll(archivistOfLogs, conversationId).pipe(
    Effect.orDie,
  );
  const compacted = records.filter(
    (envelope) => envelope.record._tag === 'Compacted',
  );

  yield* Console.log(`  ${dim('turn 2 answer:')} ${two.trim().slice(0, 200)}`);
  yield* Console.log(dim(`  records: ${tagsOf(records).join(', ')}`));

  yield* check(
    compacted.length > 0,
    'the provider rejection was classified as a context overflow, and the ' +
      'reactive rewrite was written to the log before the retry',
  );
  yield* check(
    /OVERFLOWED/i.test(two),
    'the retried turn succeeded against the compacted history',
  );
});

// --------------------------------------------------------------- the wiring

const phases: Record<
  string,
  Effect.Effect<
    void,
    | Agent.RunFailure
    | Conversation.CompatibilityError
    | Conversation.SuspendedConversationError
    | DurabilityError
    | LogStore.LogStoreError,
    Crypto.Crypto | LanguageModel.LanguageModel | LogStore.Service
  >
> = {
  tools: toolsPhase,
  delegate: delegatePhase,
  skills: skillsPhase,
  log: logPhase,
  workspace: workspacePhase,
  usage: usagePhase,
  'compaction-proactive': compactionProactivePhase,
  'compaction-reactive': compactionReactivePhase,
};

const PHASES = [...Object.keys(phases), 'durability'] as const;

const phaseFor = (name: string) =>
  name === 'durability'
    ? durabilityPhase.pipe(Effect.provide(NodeServices.layer))
    : phases[name] === undefined
      ? undefined
      : phases[name]!.pipe(Effect.provide(memoryLogLayer)).pipe(
          Effect.provide(NodeServices.layer),
        );

/** Everything except the one that sends a quarter of a million tokens. */
const DEFAULT_PHASES = [
  'tools',
  'delegate',
  'skills',
  'log',
  'workspace',
  'usage',
  'compaction-proactive',
] as const;

const summarize = (
  inputUsdPerMtok: number,
  outputUsdPerMtok: number,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const failed = checks.filter((entry) => !entry.ok);
    yield* Console.log(
      `\n${bold('summary')}  ${checks.length - failed.length}/${checks.length} checks passed`,
    );
    for (const entry of failed) {
      yield* Console.log(`  ${red('FAIL')}  ${entry.claim}`);
    }

    const dollars =
      (spend.input / 1_000_000) * inputUsdPerMtok +
      (spend.output / 1_000_000) * outputUsdPerMtok;

    yield* Console.log(
      dim(
        `\nspend  input=${spend.input} output=${spend.output} tokens` +
          `  ≈ $${dollars.toFixed(4)} at $${inputUsdPerMtok}/$${outputUsdPerMtok} per Mtok` +
          `\n       (summarization calls go through generateText and are not counted)`,
      ),
    );
  });

const command = Command.make(
  'live-smoke',
  {
    phase: Flag.choice('phase', ['all', ...PHASES]).pipe(
      Flag.withDescription(
        'Run one phase, or "all" for everything except durability and ' +
          'compaction-reactive.',
      ),
      Flag.withDefault('all'),
    ),
    model: Flag.string('model').pipe(
      Flag.withDescription(
        'Model id within the provider; defaults per provider.',
      ),
      Flag.withDefault('default'),
    ),
    fallbackModel: Flag.string('fallback-model').pipe(
      Flag.withDescription(
        'Fallback model id; defaults within --fallback-provider.',
      ),
      Flag.withDefault('default'),
    ),
    fallbackProvider: Flag.choice('fallback-provider', FALLBACK_PROVIDERS).pipe(
      Flag.withDescription(
        'Optional provider used when the primary fails before output.',
      ),
      Flag.withDefault('none'),
    ),
    provider: Flag.choice('provider', PROVIDERS).pipe(
      Flag.withDescription('Effect AI provider.'),
      Flag.withDefault(DEFAULT_PROVIDER),
    ),
    inputUsd: Flag.string('input-usd').pipe(
      Flag.withDescription(
        'Input price per million tokens, for the cost line only.',
      ),
      Flag.withDefault('1'),
    ),
    outputUsd: Flag.string('output-usd').pipe(
      Flag.withDescription('Output price per million tokens.'),
      Flag.withDefault('5'),
    ),
  },
  ({
    fallbackModel,
    fallbackProvider,
    inputUsd,
    model,
    outputUsd,
    phase,
    provider,
  }) =>
    Effect.gen(function* () {
      const selected: ReadonlyArray<string> =
        phase === 'all' ? DEFAULT_PHASES : [phase];

      const chain = Effect.gen(function* () {
        for (const name of selected) {
          const effect = phaseFor(name);
          if (effect === undefined) {
            return yield* Effect.die(new Error(`Unknown phase: ${name}`));
          }
          yield* effect;
        }
      });

      const primary = yield* modelFor(
        provider,
        model === 'default' ? DEFAULT_MODELS[provider] : model,
      );
      const selectedModel =
        fallbackProvider === 'none'
          ? primary
          : ModelPlan.layer(
              ExecutionPlan.make(
                { provide: primary },
                {
                  provide: yield* modelFor(
                    fallbackProvider,
                    fallbackModel === 'default'
                      ? DEFAULT_MODELS[fallbackProvider]
                      : fallbackModel,
                  ),
                },
              ),
            );

      yield* chain.pipe(
        Effect.provide(selectedModel),
        Effect.provideService(
          ContextWindow.Service,
          ContextWindow.usageAnchored,
        ),
      );

      yield* summarize(Number(inputUsd), Number(outputUsd));
    }).pipe(
      Effect.tapError((error: unknown) =>
        Console.error(
          AiError.isAiError(error)
            ? `\n${red(error._tag)}  reason=${error.reason._tag}` +
                ` retryable=${String(error.isRetryable)}\n${error.message}\n`
            : `\n${red(String(error))}\n`,
        ),
      ),
    ),
).pipe(
  Command.withDescription(
    'Drive a real model through tools, delegation, skills, the conversation ' +
      'log, provider fallback, Postgres runtime replacement, branching, ' +
      'forking, the workspace ' +
      'toolkit, ' +
      'and both compaction ' +
      'triggers.',
  ),
);

command.pipe(
  Command.run({ version: '0.1.0' }),
  Effect.provide(
    Layer.mergeAll(NodeServices.layer, NodeHttpClient.layerUndici),
  ),
  NodeRuntime.runMain,
);
