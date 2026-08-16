import { Agent } from '@sunfall/vesper-agent/agent';
import { LogStoreMemory } from '@sunfall/vesper-log/layer-memory';
import { CredentialStore } from '@sunfall/vesper-pi/credentials';
import { PiRegistry } from '@sunfall/vesper-pi/registry';
import {
  fauxAssistantMessage,
  fauxProvider,
} from '@earendil-works/pi-ai/providers/faux';
import { Effect, Layer } from 'effect';
import { Toolkit } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { AiRuntime } from '../src/runtime.js';

// Resuming a conversation, through the real provider adapter.
//
// `@sunfall/vesper-agent` proves the reconstruction against a synthetic
// `LanguageModel`; this proves the rebuilt prompt survives the trip through
// `@sunfall/vesper-pi`'s prompt conversion and is accepted by something that behaves
// like a provider. Those are different failures — a history that is correct as
// `Prompt` and unrepresentable in the provider's own format would pass the
// first and fail here.
//
// It replaces `AiSession.resume`, which did the same job from a whole-history
// snapshot in `@sunfall/vesper-store`. The snapshot was a second source of truth for
// a conversation the log already held, so it went; the tests it justified are
// these, retargeted.

const agent = Agent.make({
  name: 'chat',
  instructions: 'BE TERSE',
  toolkit: Toolkit.make(),
});

// Each reply reports what the provider was actually given, so the assertions
// are about conversation state rather than model output.
const echoing = () => {
  const handle = fauxProvider({
    provider: 'faux',
    models: [{ id: 'faux-1' }],
    tokensPerSecond: 0,
  });
  handle.setResponses(
    Array.from(
      { length: 8 },
      () =>
        (context: {
          messages: ReadonlyArray<unknown>;
          systemPrompt?: string;
        }) =>
          fauxAssistantMessage(
            `msgs=${context.messages.length}|sys=${context.systemPrompt ?? ''}`,
          ),
    ) as never,
  );
  return handle;
};

const run = <A, E>(program: Effect.Effect<A, E, never>) =>
  Effect.runPromise(Effect.orDie(program));

const live = () => {
  const handle = echoing();
  return Layer.mergeAll(
    AiRuntime.model({ provider: 'faux', model: 'faux-1', retry: false }).pipe(
      Layer.provide(
        PiRegistry.layer({
          register: (models) =>
            Effect.sync(() => models.setProvider(handle.provider)),
        }),
      ),
      Layer.provide(CredentialStore.layerMemory),
    ),
    LogStoreMemory.layer,
  );
};

describe('resuming a conversation', () => {
  it("seeds a new conversation with the agent's instructions", async () => {
    const result = await run(
      agent
        .resume('conv-1', 'hello')
        .pipe(Effect.provide(live())) as Effect.Effect<Agent.Result>,
    );

    expect(result.text).toContain('sys=BE TERSE');
  });

  // The property that makes this a conversation rather than two unrelated
  // runs. Under the snapshot this came from `Chat.export`; it now comes from
  // records, which is the only change a caller can observe.
  it('carries history into the next turn', async () => {
    const result = await run(
      Effect.gen(function* () {
        yield* agent.resume('conv-1', 'first');
        return yield* agent.resume('conv-1', 'second');
      }).pipe(Effect.provide(live())) as Effect.Effect<Agent.Result>,
    );

    expect(result.text).toMatch(/msgs=[2-9]/);
  });

  it('keeps separate conversations separate', async () => {
    const result = await run(
      Effect.gen(function* () {
        yield* agent.resume('conv-1', 'first');
        return yield* agent.resume('conv-2', 'first');
      }).pipe(Effect.provide(live())) as Effect.Effect<Agent.Result>,
    );

    expect(result.text).toContain('msgs=1');
  });

  // A resumed conversation that reset its counter would under-report every
  // turn after the first, which is exactly the number anyone asking about
  // cost cares about. The snapshot carried this in a `usage` field; the log
  // derives it from what each run settled with.
  it('accumulates usage across the life of the conversation', async () => {
    const totals = await run(
      Effect.gen(function* () {
        const first = yield* agent.resume('conv-1', 'a');
        const second = yield* agent.resume('conv-1', 'b');
        return { first: first.usage, second: second.usage };
      }).pipe(Effect.provide(live())) as Effect.Effect<{
        first: { input: number; output: number };
        second: { input: number; output: number };
      }>,
    );

    expect(totals.first.output).toBeGreaterThan(0);
    expect(totals.second.output).toBeGreaterThan(totals.first.output);
  });
});
