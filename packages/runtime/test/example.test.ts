import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { CredentialStore } from '@sunfall/vesper-pi/credentials';
import { PiRegistry } from '@sunfall/vesper-pi/registry';
import {
  fauxAssistantMessage,
  fauxToolCall,
  fauxProvider,
} from '@earendil-works/pi-ai/providers/faux';
import { Effect, Layer, Stream } from 'effect';
import { describe, expect, it } from 'vitest';

import { OrderRepo, supportAgent } from '../src/example.js';
import { AiRuntime } from '../src/runtime.js';

// Runs the worked example end to end. Its job is to keep `example.ts`
// honest: if the API changes shape, the documentation stops compiling and
// this stops passing, rather than quietly becoming a lie.

const orders = Layer.succeed(OrderRepo, {
  status: (id: string) =>
    Effect.succeed(id === 'order_1042' ? 'shipped' : 'lost'),
  refund: (id: string) => Effect.succeed(`refunded ${id}`),
});

const live = (responses: ReadonlyArray<unknown>) => {
  const handle = fauxProvider({
    provider: 'faux',
    models: [{ id: 'faux-1' }],
    tokensPerSecond: 0,
  });
  handle.setResponses(responses as never);

  return Layer.mergeAll(
    AiRuntime.model({ provider: 'faux', model: 'faux-1' }).pipe(
      Layer.provide(
        PiRegistry.layer({
          register: (models) =>
            Effect.sync(() => models.setProvider(handle.provider)),
        }),
      ),
      Layer.provide(CredentialStore.layerMemory),
    ),
    LogStoreMemory.layer,
    orders,
  );
};

const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> =>
  Effect.runPromise(Effect.orDie(effect));

describe('worked example', () => {
  it('calls a tool, gets a real answer, and reports it', async () => {
    const result = await run(
      supportAgent
        .run('where is order_1042?')
        .pipe(
          Effect.provide(
            live([
              fauxAssistantMessage(
                [fauxToolCall('lookup_order', { orderId: 'order_1042' })],
                { stopReason: 'toolUse' },
              ),
              fauxAssistantMessage('Your order has shipped.'),
            ]),
          ),
        ),
    );

    // Two turns: the tool call, then the answer that used its result.
    expect(result.steps).toBe(2);
    expect(result.text).toBe('Your order has shipped.');
  });

  it('offers the subagent and the skill loader alongside its own tools', async () => {
    const names = Object.keys(supportAgent.toolkit.tools).sort();

    expect(names).toEqual([
      'issue_refund',
      'load_skill',
      'lookup_order',
      'task_researcher',
    ]);
  });

  // The terminal-tool pattern, which is what `Stop.toolCalled('issue_refund')`
  // buys. Without it the loop would run another turn to narrate the refund it
  // just issued — a whole extra model call for nothing.
  it('stops the moment the terminal tool is called', async () => {
    const result = await run(
      supportAgent.run('refund order_1042 please').pipe(
        Effect.provide(
          live([
            fauxAssistantMessage(
              [fauxToolCall('issue_refund', { orderId: 'order_1042' })],
              { stopReason: 'toolUse' },
            ),
            // Never reached: the stop condition fires on the call above.
            fauxAssistantMessage('...and here is a summary nobody asked for.'),
          ]),
        ),
      ),
    );

    expect(result.steps).toBe(1);
  });

  it('puts the skill catalog in the prompt but not the policy text', () => {
    expect(supportAgent.instructions).toContain('refund_policy');
    expect(supportAgent.instructions).toContain('When a refund is allowed');
    expect(supportAgent.instructions).not.toContain('within 30 days');
  });

  it('streams turn boundaries and content as it goes', async () => {
    const tags = await run(
      supportAgent.stream('hello?').pipe(
        Stream.map((event) => event._tag),
        Stream.runCollect,
        Effect.provide(live([fauxAssistantMessage('Hi — how can I help?')])),
      ),
    );

    expect(tags[0]).toBe('TurnStarted');
    expect(tags).toContain('Part');
    expect(tags[tags.length - 1]).toBe('Completed');
  });

  it('remembers the conversation across calls', async () => {
    const layers = live([
      fauxAssistantMessage('First reply.'),
      (context: { messages: ReadonlyArray<unknown> }) =>
        fauxAssistantMessage(`saw ${context.messages.length} messages`),
    ]);

    const result = await run(
      Effect.gen(function* () {
        yield* supportAgent.resume('cust-99', 'my order is late');
        return yield* supportAgent.resume('cust-99', 'any update?');
      }).pipe(Effect.provide(layers)),
    );

    // The second turn saw the first exchange, not just its own input.
    expect(result.text).toMatch(/saw [2-9] messages/);
  });
});
