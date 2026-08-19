import * as NodeRuntime from '@effect/platform-node/NodeRuntime';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { Agent } from '@sunfall/vesper-agent/agent';
import { Conversation } from '@sunfall/vesper-agent/conversation';
import { AgentWorkflow } from '@sunfall/vesper-agent/workflow';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { Console, Effect, Layer, Schema, Stream } from 'effect';
import {
  LanguageModel,
  type Prompt as AiPrompt,
  type Response,
  Tool,
  Toolkit,
} from 'effect/unstable/ai';
import { Command, Flag, Prompt } from 'effect/unstable/cli';
import { WorkflowEngine } from 'effect/unstable/workflow';

// This is deliberately a story, not a real deployment. The scripted model
// makes the example deterministic and keeps the approval API visible instead
// of burying it under provider configuration.
const story = {
  changeId: 'release-vesper-2026-08-18',
  release: 'vesper@0.1.0-alpha.1',
  environment: 'production',
  prompt: [
    'The release candidate passed every automated check.',
    'Deploy vesper@0.1.0-alpha.1 to production now.',
    'Production changes require a human decision before execution.',
  ].join(' '),
} as const;

type Decision = 'approve' | 'deny';
type DecisionMode = Decision | 'ask';

class StoryFailure extends Schema.TaggedError<StoryFailure>(
  'approval-cli/StoryFailure',
)('StoryFailure', { message: Schema.String }) {}

const approval = AgentWorkflow.wait({
  name: 'release-approval',
  key: (request: { readonly changeId: string }) => request.changeId,
  request: Schema.Struct({
    changeId: Schema.String,
    release: Schema.String,
    environment: Schema.String,
    reason: Schema.String,
  }),
  success: Schema.Struct({
    decision: Schema.Literals(['approve', 'deny']),
    actor: Schema.String,
  }),
  error: Schema.Never,
});

const release = AgentWorkflow.durable(
  Tool.make('release_to_environment', {
    description: 'Release one build after its required human approval.',
    parameters: Schema.Struct({
      changeId: Schema.String,
      release: Schema.String,
      environment: Schema.String,
    }),
    success: Schema.Struct({
      status: Schema.Literals(['released', 'declined']),
      release: Schema.String,
      environment: Schema.String,
      actor: Schema.String,
    }),
    failure: Schema.Never,
    failureMode: 'return',
  }),
);

const releaseAgent = Agent.make({
  name: 'release-agent',
  revision: '1',
  instructions:
    'Release only through the provided tool, then explain what happened.',
  toolkit: Toolkit.make(release),
}).withHandlers({
  release_to_environment: ({ changeId, environment, release: releaseName }) =>
    Effect.gen(function* () {
      const choice = yield* approval({
        changeId,
        release: releaseName,
        environment,
        reason:
          'All automated checks passed; production still requires a human.',
      });

      return {
        status: choice.decision === 'approve' ? 'released' : 'declined',
        release: releaseName,
        environment,
        actor: choice.actor,
      } as const;
    }),
});

const Request = AgentWorkflow.request({ changeId: Schema.String });
const binding = AgentWorkflow.make(releaseAgent, {
  tag: 'ApprovalCliStory',
  payload: Request,
  idempotencyKey: ({ changeId }) => changeId,
  error: StoryFailure,
  mapError: (error) => new StoryFailure({ message: String(error) }),
});

const finish = (reason: 'stop' | 'tool-calls'): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const releaseResult = (
  prompt: AiPrompt.Prompt,
):
  | { readonly status: 'released' | 'declined'; readonly actor: string }
  | undefined => {
  for (const message of prompt.content) {
    if (message.role !== 'tool') {
      continue;
    }
    for (const part of message.content) {
      if (
        part.type !== 'tool-result' ||
        part.name !== 'release_to_environment' ||
        typeof part.result !== 'object' ||
        part.result === null ||
        !('status' in part.result) ||
        !('actor' in part.result) ||
        (part.result.status !== 'released' &&
          part.result.status !== 'declined') ||
        typeof part.result.actor !== 'string'
      ) {
        continue;
      }
      return { status: part.result.status, actor: part.result.actor };
    }
  }
  return undefined;
};

// First turn: request the release tool. Second turn: react to the result that
// came back from the resumed handler. The second branch therefore proves the
// decision reached the tool and was observed by the agent, rather than merely
// changing a line printed by the CLI.
const StoryModel = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.succeed([finish('stop')]),
    streamText: (options) => {
      const result = releaseResult(options.prompt);
      if (result === undefined) {
        return Stream.fromIterable<Response.StreamPartEncoded>([
          {
            type: 'tool-call',
            id: 'release-call',
            name: 'release_to_environment',
            params: {
              changeId: story.changeId,
              release: story.release,
              environment: story.environment,
            },
          },
          finish('tool-calls'),
        ]);
      }

      const text =
        result.status === 'released'
          ? `Approved by ${result.actor}. ${story.release} is now live in production.`
          : `Understood. ${result.actor} declined the release, so production is unchanged.`;
      return Stream.fromIterable<Response.StreamPartEncoded>([
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: text },
        { type: 'text-end', id: 'answer' },
        finish('stop'),
      ]);
    },
  }),
);

const LogLive = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

const AppLive = binding.layer.pipe(
  Layer.provideMerge(WorkflowEngine.layerMemory),
  Layer.provide(StoryModel),
  Layer.provideMerge(LogLive),
);

const choose = (mode: DecisionMode) =>
  mode === 'ask'
    ? Prompt.run(
        Prompt.select({
          message: 'What should happen?',
          choices: [
            {
              title: 'Approve release',
              value: 'approve' as const,
              description: 'Resume the handler and simulate the deployment.',
            },
            {
              title: 'Deny release',
              value: 'deny' as const,
              description: 'Resume the handler without changing production.',
            },
          ],
        }),
      )
    : Effect.succeed(mode);

const runStory = (mode: DecisionMode) =>
  Effect.gen(function* () {
    const payload = {
      changeId: story.changeId,
      conversationId: `approval-cli-${story.changeId}`,
      input: story.prompt,
    };
    const conversation = Conversation.make(
      releaseAgent,
      payload.conversationId,
    );

    yield* Console.log(`\nInjected story\n  ${story.prompt}`);
    yield* binding.workflow.execute(payload, { discard: true });

    // This waits for one exact durable approval instance. There is no polling,
    // shared inbox, or race to correlate a response with the right tool call.
    const pending = yield* approval.awaitPending(conversation, story.changeId);
    yield* Console.log(
      [
        '\nApproval requested',
        `  key:         ${pending.key}`,
        `  release:     ${pending.request.release}`,
        `  environment: ${pending.request.environment}`,
        `  reason:      ${pending.request.reason}`,
      ].join('\n'),
    );

    const decision = yield* choose(mode);
    yield* pending.complete({ decision, actor: 'cli-user' });
    yield* Console.log(
      `\nDecision submitted\n  ${decision} by cli-user (durable workflow state)`,
    );

    const result = yield* binding.workflow.execute(payload);
    yield* Console.log(`\nAgent\n  ${result.text}`);

    const records = yield* conversation.records().pipe(Stream.runCollect);
    const conversationTrail = Array.from(records)
      .map(({ record }) => record._tag)
      .flatMap((tag): ReadonlyArray<string> => {
        switch (tag) {
          case 'ToolStarted':
            return ['handler entered'];
          case 'ToolSuspended':
            return ['approval requested'];
          case 'ToolResumed':
            return ['handler re-entered'];
          case 'ToolWaitCompleted':
            return ['decision consumed'];
          case 'ToolOutcome':
            return ['tool completed'];
          default:
            return [];
        }
      });
    yield* Console.log(
      `\nConversation trail\n  ${conversationTrail.join(' -> ')}\n`,
    );
  }).pipe(Effect.provide(AppLive), Effect.scoped);

const command = Command.make(
  'approval-story',
  {
    decision: Flag.choice('decision', ['ask', 'approve', 'deny']).pipe(
      Flag.withDescription(
        'Prompt interactively, or choose a branch non-interactively.',
      ),
      Flag.withDefault('ask'),
    ),
  },
  ({ decision }) => runStory(decision),
).pipe(
  Command.withDescription(
    'Run a scripted agent that yields a durable production approval and reacts to the decision.',
  ),
);

command.pipe(
  Command.run({ version: '0.1.0' }),
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
