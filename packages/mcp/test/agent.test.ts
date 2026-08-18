import type { LogStore } from '@sunfall/vesper-log/log-store';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import * as NodeCrypto from '@effect/platform-node/NodeCrypto';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import { Crypto, Effect, Layer, Stream } from 'effect';
import {
  AiError,
  LanguageModel,
  McpServer,
  type Response,
  Toolkit,
} from 'effect/unstable/ai';

import { Agent } from '@sunfall/vesper-agent/agent';
import { DurabilityError } from '@sunfall/vesper-agent/conversation';
import { RunPolicy } from '@sunfall/vesper-agent/run-policy';
import { ScriptedModel } from '@sunfall/vesper-agent/testing';
import { AgentMcp } from '../src/agent.js';

const agent = Agent.make({
  name: 'support',
  revision: '1',
  instructions: 'Help the customer.',
  toolkit: Toolkit.make(),
});

const adapter = AgentMcp.make(agent);

type LayerRequires<L> =
  L extends Layer.Layer<infer _A, infer _E, infer R> ? R : never;
type Requires = LayerRequires<typeof adapter.layer>;
type Assert<T extends true> = T;
type Exact<A, B> = [A, B] extends [B, A] ? true : false;
type ExpectedRequires =
  | Agent.Requires<typeof agent>
  | LogStore.Service
  | Crypto.Crypto
  | McpServer.McpServer;

const exactRequirements: Assert<Exact<Requires, ExpectedRequires>> = true;

const languageRequirement: Assert<
  LanguageModel.LanguageModel extends Requires ? true : false
> = true;
const logRequirement: Assert<LogStore.Service extends Requires ? true : false> =
  true;
const cryptoRequirement: Assert<Crypto.Crypto extends Requires ? true : false> =
  true;
const serverRequirement: Assert<
  McpServer.McpServer extends Requires ? true : false
> = true;
void [
  exactRequirements,
  languageRequirement,
  logRequirement,
  cryptoRequirement,
  serverRequirement,
];

describe('AgentMcp', () => {
  it('exposes one stable durable-conversation tool', () => {
    expect(Object.keys(adapter.toolkit.tools)).toEqual(['run_agent']);
    expect(adapter.toolkit.tools.run_agent.description).toContain(
      'durable conversation',
    );
  });

  it.effect(
    'runs the configured agent through its ordinary requirements',
    () => {
      const model = ScriptedModel.make([
        [
          { type: 'text-start', id: 'answer' },
          { type: 'text-delta', id: 'answer', delta: 'Resolved.' },
          { type: 'text-end', id: 'answer' },
          {
            type: 'finish',
            reason: 'stop',
            usage: {
              inputTokens: {
                total: 1,
                uncached: 1,
                cacheRead: 0,
                cacheWrite: 0,
              },
              outputTokens: { total: 1 },
            },
          },
        ] satisfies ReadonlyArray<Response.StreamPartEncoded>,
      ]);
      const world = Layer.mergeAll(
        model.layer,
        LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
        NodeServices.layer,
      );

      return Effect.gen(function* () {
        const ready = yield* adapter.toolkit;
        const results = yield* ready
          .handle('run_agent', {
            conversationId: 'mcp-support-test',
            input: 'Please help.',
          })
          .pipe(Stream.unwrap, Stream.runCollect);
        expect(Array.from(results).at(-1)?.result).toMatchObject({
          outcome: 'success',
          text: 'Resolved.',
        });
      }).pipe(
        Effect.provide(adapter.handlers),
        Effect.provide(world),
        Effect.scoped,
      );
    },
  );

  it.effect(
    'registers the tool with Effect MCP without adding a transport',
    () => {
      const model = ScriptedModel.make([]);
      const world = adapter.layer.pipe(
        Layer.provideMerge(McpServer.McpServer.layer),
        Layer.provideMerge(
          Layer.mergeAll(
            model.layer,
            NodeCrypto.layer,
            LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
            NodeServices.layer,
          ),
        ),
      );

      return Effect.gen(function* () {
        const server = yield* McpServer.McpServer;
        expect(server.tools.map(({ tool }) => tool.name)).toEqual([
          'run_agent',
        ]);
      }).pipe(Effect.provide(world), Effect.scoped);
    },
  );

  it.effect('turns declared agent failures into MCP-safe tool failures', () => {
    const model = ScriptedModel.make([
      new AiError.AiError({
        module: 'mcp-test',
        method: 'streamText',
        reason: new AiError.InvalidRequestError({
          description: 'the scripted provider failed',
        }),
      }),
    ]);
    const world = Layer.mergeAll(
      model.layer,
      LogStoreMemory.layer.pipe(Layer.provide(NodeServices.layer)),
      NodeServices.layer,
    );

    return Effect.gen(function* () {
      const ready = yield* adapter.toolkit;
      const result = yield* ready
        .handle('run_agent', {
          conversationId: 'mcp-failure-test',
          input: 'Please help.',
        })
        .pipe(Stream.unwrap, Stream.runCollect, Effect.result);
      expect(result._tag).toBe('Failure');
      if (result._tag === 'Failure') {
        expect(result.failure).toMatchObject({
          _tag: 'RunError',
          classification: 'provider',
          code: 'ai.InvalidRequestError',
          retryable: false,
          details: {
            module: 'mcp-test',
            method: 'streamText',
            reason: 'InvalidRequestError',
          },
        });
      }
    }).pipe(
      Effect.provide(adapter.handlers),
      Effect.provide(world),
      Effect.scoped,
    );
  });

  it('classifies framework failures without exposing arbitrary fields', () => {
    expect(
      AgentMcp.failure(
        new DurabilityError({
          source: 'log',
          operation: 'append',
          reason: 'storage',
          detail: 'database unavailable',
          cause: new Error('connection string must stay private'),
        }),
      ),
    ).toMatchObject({
      classification: 'durability',
      code: 'log.storage',
      retryable: true,
      details: {
        source: 'log',
        operation: 'append',
        reason: 'storage',
      },
    });
    expect(
      AgentMcp.failure(
        new RunPolicy.RunPolicyExhausted({
          limit: 'turns',
          used: 3,
          maximum: 3,
        }),
      ),
    ).toMatchObject({
      classification: 'run-policy',
      code: 'run-policy.turns',
      retryable: false,
    });

    const application = AgentMcp.failure({
      _tag: 'OrderDenied',
      message: 'The order was denied',
      internalToken: 'must-not-leak',
    });
    expect(application).toMatchObject({
      classification: 'application',
      code: 'OrderDenied',
      message: 'The order was denied',
      retryable: false,
      details: {},
    });
    expect(JSON.stringify(application)).not.toContain('must-not-leak');
  });
});
