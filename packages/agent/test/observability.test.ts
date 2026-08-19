import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Effect, Layer, Metric, Option, Schema, Stream, Tracer } from 'effect';
import { Tool, Toolkit, type Response } from 'effect/unstable/ai';
import { describe, expect, it } from '@effect/vitest';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import * as Observability from '../src/internal/observability.js';
import { ScriptedModel } from '../src/testing.js';

const finish = (reason: 'stop' | 'tool-calls' = 'stop') => ({
  type: 'finish' as const,
  reason,
  usage: {
    inputTokens: { total: 3, uncached: 3, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: 1 },
  },
});

const textTurn = (
  id: string,
  deltas: ReadonlyArray<string>,
): Response.StreamPartEncoded[] => [
  { type: 'text-start' as const, id },
  ...deltas.map((delta) => ({ type: 'text-delta' as const, id, delta })),
  { type: 'text-end' as const, id },
  finish(),
];

/**
 * A span-capturing test `Tracer`.
 *
 * `Effect.withSpan`/`Stream.withSpan` allocate a span through the tracer's
 * `span` hook, then set attributes on it via `span.attribute(...)` — both up
 * front from `options.attributes` and later from any `annotateCurrentSpan`
 * call against the same span. Recording the live `Tracer.NativeSpan` at
 * allocation time, rather than snapshotting attributes then, is what lets a
 * test read the *final* attribute set after the run completes.
 */
interface CapturedSpan {
  readonly name: string;
  readonly parentName: string | undefined;
  readonly span: Tracer.Span;
}

const capturingTracer = (): {
  readonly tracer: Tracer.Tracer;
  readonly spans: ReadonlyArray<CapturedSpan>;
} => {
  const spans: CapturedSpan[] = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      const parent = Option.getOrUndefined(options.parent);
      spans.push({
        name: options.name,
        parentName: parent?._tag === 'Span' ? parent.name : undefined,
        span,
      });
      return span;
    },
  });
  return { tracer, spans };
};

const spanNamed = (
  spans: ReadonlyArray<CapturedSpan>,
  name: string,
): CapturedSpan | undefined =>
  spans.find((candidate) => candidate.name === name);

const testLogLayer = Layer.mergeAll(
  LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
  NodeServices.layer,
);

describe('agent observability metrics', () => {
  it.effect('records provider cache usage through Agent.stream', () =>
    Effect.gen(function* () {
      const before = {
        calls: yield* Metric.value(Observability.modelCalls),
        input: yield* Metric.value(Observability.modelInputTokens),
        output: yield* Metric.value(Observability.modelOutputTokens),
        uncached: yield* Metric.value(Observability.modelUncachedInputTokens),
        cacheRead: yield* Metric.value(Observability.modelCacheReadTokens),
        cacheWrite: yield* Metric.value(Observability.modelCacheWriteTokens),
      };
      const model = ScriptedModel.make([
        [
          {
            type: 'finish',
            reason: 'stop',
            usage: {
              inputTokens: {
                total: 13,
                uncached: 4,
                cacheRead: 7,
                cacheWrite: 2,
              },
              outputTokens: { total: 5 },
            },
          },
        ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
      ]);
      const agent = Agent.make({
        name: 'observability-test',
        revision: '1',
        instructions: 'be terse',
        toolkit: Toolkit.make(),
      });

      yield* agent
        .stream('hi')
        .pipe(Stream.runDrain, Effect.provide(model.layer));

      expect(
        (yield* Metric.value(Observability.modelCalls)).count -
          before.calls.count,
      ).toBe(1);
      expect(
        (yield* Metric.value(Observability.modelInputTokens)).count -
          before.input.count,
      ).toBe(13);
      expect(
        (yield* Metric.value(Observability.modelOutputTokens)).count -
          before.output.count,
      ).toBe(5);
      expect(
        (yield* Metric.value(Observability.modelUncachedInputTokens)).count -
          before.uncached.count,
      ).toBe(4);
      expect(
        (yield* Metric.value(Observability.modelCacheReadTokens)).count -
          before.cacheRead.count,
      ).toBe(7);
      expect(
        (yield* Metric.value(Observability.modelCacheWriteTokens)).count -
          before.cacheWrite.count,
      ).toBe(2);
    }).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
  );
});

// Span names and attributes are exactly what silently drifts: nothing fails
// to compile when `'Agent.turn'` becomes `'Agent.Turn'`, or when an attribute
// key is renamed. These pin the source's actual span contract — not a
// redesigned one — using a tracer that records every span this package
// creates.
describe('agent observability spans', () => {
  const echo = Tool.make('echo', {
    description: 'echo the input',
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.Struct({ text: Schema.String }),
  });

  it.effect(
    'names Agent.run, Agent.turn, Agent.model, and Agent.tool with their documented attributes',
    () =>
      Effect.gen(function* () {
        const { tracer, spans } = capturingTracer();
        const agent = Agent.make({
          name: 'span-agent',
          revision: '3',
          instructions: 'be terse',
          toolkit: Toolkit.make(echo),
        }).withHandlers({ echo: ({ text }) => Effect.succeed({ text }) });

        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call' as const,
              id: 'echo-call',
              name: 'echo',
              params: { text: 'hi' },
            },
            finish('tool-calls'),
          ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
          textTurn('a', ['done']),
        ]);

        yield* agent
          .stream('hi')
          .pipe(
            Stream.runDrain,
            Effect.provide(model.layer),
            Effect.provideService(Tracer.Tracer, tracer),
          );

        const run = spanNamed(spans, 'Agent.run');
        const turns = spans.filter(
          (candidate) => candidate.name === 'Agent.turn',
        );
        const models = spans.filter(
          (candidate) => candidate.name === 'Agent.model',
        );
        const tool = spanNamed(spans, 'Agent.tool');

        expect(run?.span.attributes.get('vesper.agent.name')).toBe(
          'span-agent',
        );
        expect(run?.span.attributes.get('vesper.agent.revision')).toBe('3');
        expect(run?.span.attributes.get('vesper.run.recorded')).toBe(false);
        // A plain `agent.stream` call has no session, so the run is not
        // durable and carries no conversation id at all — not even `undefined`.
        expect(run?.span.attributes.has('vesper.conversation.id')).toBe(false);

        // One turn for the tool call, one more once the tool result comes back.
        expect(turns).toHaveLength(2);
        expect(turns[0]?.span.attributes.get('vesper.agent.step')).toBe(1);
        expect(turns[1]?.span.attributes.get('vesper.agent.step')).toBe(2);

        expect(models).toHaveLength(2);
        expect(models[0]?.span.attributes.get('vesper.agent.step')).toBe(1);
        expect(models[0]?.span.attributes.get('vesper.model.attempt')).toBe(
          'initial',
        );

        expect(tool?.span.attributes.get('vesper.tool.name')).toBe('echo');
        expect(tool?.span.attributes.get('vesper.tool.call_id')).toBe(
          'echo-call',
        );

        // Parent-child structure, cheap to check because the capturing tracer
        // already recorded each span's parent by name: run -> turn -> model.
        // The tool span nests one level deeper still, under Effect ai's own
        // `Chat.streamText` span — dispatch happens while that provider
        // stream is still being pulled, not after `Agent.model` closes.
        expect(turns[0]?.parentName).toBe('Agent.run');
        expect(models[0]?.parentName).toBe('Agent.turn');
        expect(tool?.parentName).toBe('Chat.streamText');
      }),
  );

  it.effect(
    'names Agent.delegate, pins delegation depth, and chains run -> turn -> tool -> delegate -> child run',
    () =>
      Effect.gen(function* () {
        const { tracer, spans } = capturingTracer();
        const child = Agent.make({
          name: 'child-span',
          revision: '2',
          instructions: 'help',
          toolkit: Toolkit.make(),
        });
        const parent = Agent.make({
          name: 'parent-span',
          revision: '1',
          instructions: 'delegate to child-span',
          toolkit: Toolkit.make(),
          subagents: [child],
        });

        const model = ScriptedModel.make([
          [
            {
              type: 'tool-call' as const,
              id: 'delegate-call',
              name: 'task_child-span',
              params: { prompt: 'go' },
            },
            finish('tool-calls'),
          ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
          textTurn('c', ['child done']),
          textTurn('p', ['done']),
        ]);

        yield* parent.stream('hi').pipe(
          Stream.runDrain,
          Effect.provide(model.layer),
          // Delegation carries the child-session Crypto requirement even
          // when nothing records, so the type-tests project sees it.
          Effect.provide(NodeCrypto.layer),
          Effect.provideService(Tracer.Tracer, tracer),
        );

        const delegate = spanNamed(spans, 'Agent.delegate');
        const runs = spans.filter(
          (candidate) => candidate.name === 'Agent.run',
        );
        const parentRun = runs.find(
          (candidate) =>
            candidate.span.attributes.get('vesper.agent.name') ===
            'parent-span',
        );
        const childRun = runs.find(
          (candidate) =>
            candidate.span.attributes.get('vesper.agent.name') === 'child-span',
        );
        const delegateTool = spans.find(
          (candidate) =>
            candidate.name === 'Agent.tool' &&
            candidate.span.attributes.get('vesper.tool.name') ===
              'task_child-span',
        );

        expect(delegate?.span.attributes.get('vesper.agent.child.name')).toBe(
          'child-span',
        );
        expect(
          delegate?.span.attributes.get('vesper.agent.child.revision'),
        ).toBe('2');
        // Set via `Effect.annotateCurrentSpan` on the same span, not through
        // `Effect.withSpan`'s own `attributes` option — this is the top of a
        // fresh delegation chain, so depth is 0.
        expect(
          delegate?.span.attributes.get('vesper.agent.delegation.depth'),
        ).toBe(0);
        // No parent session, so no conversation id lands on the delegate span.
        expect(delegate?.span.attributes.has('vesper.conversation.id')).toBe(
          false,
        );

        // The delegating tool call nests under the model's own
        // `Chat.streamText` span, like any tool call (see the sibling test).
        // `Agent.delegate` itself, though, does not nest under that
        // `Agent.tool` span: the dispatch runtime runs tool handlers on a
        // separately forked fiber, so the ambient span when the delegation
        // handler actually starts is the run span the fork happened under,
        // not the tool span logically wrapping it. The child's own run does
        // nest under `Agent.delegate`, which is the parent-child edge that
        // actually matters for reading a delegation trace.
        expect(parentRun?.parentName).toBeUndefined();
        expect(delegateTool?.parentName).toBe('Chat.streamText');
        expect(delegate?.parentName).toBe('Agent.run');
        expect(childRun?.parentName).toBe('Agent.delegate');
      }),
  );

  it.effect(
    'lands vesper.conversation.id on the run span for a recorded conversation',
    () =>
      Effect.gen(function* () {
        const { tracer, spans } = capturingTracer();
        const CONVERSATION = LogVocabulary.ConversationId.make(
          'observability-span-conversation',
        );
        const agent = Agent.make({
          name: 'recorded-span-agent',
          revision: '1',
          instructions: 'be terse',
          toolkit: Toolkit.make(),
        });
        const model = ScriptedModel.make([textTurn('a', ['done'])]);

        yield* Conversation.make(agent, CONVERSATION)
          .run('hi')
          .pipe(
            Effect.orDie,
            Effect.provide(model.layer),
            Effect.provide(testLogLayer),
            Effect.provide(NodeCrypto.layer),
            Effect.provideService(Tracer.Tracer, tracer),
            Effect.scoped,
          );

        const run = spanNamed(spans, 'Agent.run');

        expect(run?.span.attributes.get('vesper.conversation.id')).toBe(
          CONVERSATION,
        );
        expect(run?.span.attributes.get('vesper.run.recorded')).toBe(true);
      }),
  );
});
