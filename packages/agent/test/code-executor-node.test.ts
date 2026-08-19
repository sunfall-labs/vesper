import { describe, expect, it } from '@effect/vitest';
import { Effect, Stream } from 'effect';

import { CodeExecutor } from '../src/code-executor.js';
import { NodeCodeExecutor } from '../src/code-executor-node.js';

const request = (
  source: string,
  limits: CodeExecutor.Limits = CodeExecutor.defaultLimits,
): CodeExecutor.Request => ({
  source,
  tools: [
    {
      name: 'echo',
      description: 'Return the input.',
      parameters: { type: 'object' },
      result: { type: 'object' },
    },
  ],
  state: {},
  limits,
});

describe('node code executor', () => {
  it.effect('runs code in a standalone host and brokers nested tools', () =>
    Effect.gen(function* () {
      const executor = yield* CodeExecutor.Service;
      const execution = yield* executor.start(
        request(`
          const result = await tools.echo({ value: "hidden" })
          store("answer", result)
          text(result.value)
        `),
      );
      const events = yield* execution.events.pipe(
        Stream.tap((event) =>
          event._tag === 'ToolCall'
            ? execution.respond({
                id: event.id,
                outcome: 'success',
                value: event.input,
              })
            : Effect.void,
        ),
        Stream.runCollect,
      );

      expect(Array.from(events)).toEqual([
        {
          _tag: 'ToolCall',
          id: '1',
          name: 'echo',
          input: { value: 'hidden' },
        },
        { _tag: 'Output', value: 'hidden' },
        { _tag: 'Completion', state: { answer: { value: 'hidden' } } },
      ]);
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('accepts JSON values with repeated object references', () =>
    Effect.gen(function* () {
      const executor = yield* CodeExecutor.Service;
      const execution = yield* executor.start(
        request(`
          const shared = { value: 1 }
          store("pair", { left: shared, right: shared })
          text(load("pair"))
        `),
      );
      const events = yield* Stream.runCollect(execution.events);

      expect(Array.from(events)).toEqual([
        {
          _tag: 'Output',
          value: '{"left":{"value":1},"right":{"value":1}}',
        },
        {
          _tag: 'Completion',
          state: {
            pair: { left: { value: 1 }, right: { value: 1 } },
          },
        },
      ]);
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('does not expose ambient Node, network, or module authority', () =>
    Effect.gen(function* () {
      const executor = yield* CodeExecutor.Service;
      const execution = yield* executor.start(
        request(
          'text([typeof process, typeof require, typeof fetch].join(","))',
        ),
      );
      const events = yield* Stream.runCollect(execution.events);

      expect(Array.from(events)).toEqual([
        { _tag: 'Output', value: 'undefined,undefined,undefined' },
        { _tag: 'Completion', state: {} },
      ]);

      const escape = yield* executor.start(
        request(`
          let escaped = false
          for (const value of [globalThis, text, store, load, tools.echo, ALL_TOOLS[0]]) {
            try {
              value.constructor.constructor("return process")()
              escaped = true
            } catch {}
          }
          text(escaped)
        `),
      );
      const escapeEvents = yield* Stream.runCollect(escape.events);
      expect(Array.from(escapeEvents)[0]).toEqual({
        _tag: 'Output',
        value: 'false',
      });

      const importing = yield* executor.start(
        request('await import("node:fs")'),
      );
      const importEvents = yield* Stream.runCollect(importing.events);
      expect(Array.from(importEvents).at(-1)?._tag).toBe('Failure');
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('enforces source, output, and nested-call limits', () =>
    Effect.gen(function* () {
      const executor = yield* CodeExecutor.Service;
      const sourceError = yield* executor
        .start(
          request('text("too large")', {
            ...CodeExecutor.defaultLimits,
            maxSourceBytes: 4,
          }),
        )
        .pipe(Effect.flip);
      expect(sourceError.message).toContain('exceeds 4 bytes');

      const output = yield* executor.start(
        request('text("12345")', {
          ...CodeExecutor.defaultLimits,
          maxOutputBytes: 4,
        }),
      );
      expect(Array.from(yield* Stream.runCollect(output.events))).toEqual([
        { _tag: 'Failure', message: 'Code output exceeds 4 bytes' },
      ]);

      const nested = yield* executor.start(
        request(
          'await tools.echo({ value: 1 }); await tools.echo({ value: 2 })',
          { ...CodeExecutor.defaultLimits, maxNestedCalls: 1 },
        ),
      );
      const nestedEvents = yield* nested.events.pipe(
        Stream.tap((event) =>
          event._tag === 'ToolCall'
            ? nested.respond({
                id: event.id,
                outcome: 'success',
                value: event.input,
              })
            : Effect.void,
        ),
        Stream.runCollect,
      );
      expect(Array.from(nestedEvents).at(-1)).toEqual({
        _tag: 'Failure',
        message: 'Nested tool calls exceed 1',
      });
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect(
    'terminates the host on timeout and fails closed when unavailable',
    () =>
      Effect.gen(function* () {
        const executor = yield* CodeExecutor.Service;
        const timed = yield* executor.start(
          request('while (true) {}', {
            ...CodeExecutor.defaultLimits,
            wallClockMillis: 100,
          }),
        );
        const timedEvents = yield* Stream.runCollect(timed.events);
        expect(Array.from(timedEvents)).toHaveLength(1);
        expect(Array.from(timedEvents)[0]?._tag).toBe('Failure');

        const unavailable = yield* Effect.gen(function* () {
          const missingExecutor = yield* CodeExecutor.Service;
          const execution = yield* missingExecutor.start(request('text("no")'));
          return yield* Stream.runDrain(execution.events).pipe(Effect.flip);
        }).pipe(
          Effect.provide(
            NodeCodeExecutor.layer({
              hostUrl: new URL('../host/does-not-exist.mjs', import.meta.url),
            }),
          ),
        );
        expect(unavailable.reason).toBe('unavailable');
      }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('fails closed on malformed host protocol events', () =>
    Effect.gen(function* () {
      const executor = yield* CodeExecutor.Service;
      const execution = yield* executor.start(request('text("no")'));
      const error = yield* Stream.runDrain(execution.events).pipe(Effect.flip);

      expect(error.reason).toBe('protocol');
      expect(error.message).toContain('invalid event');
    }).pipe(
      Effect.provide(
        NodeCodeExecutor.layer({
          hostUrl: new URL(
            './fixtures/malformed-code-host.mjs',
            import.meta.url,
          ),
        }),
      ),
    ),
  );
});
