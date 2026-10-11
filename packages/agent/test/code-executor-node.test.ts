import { describe, expect, it } from '@effect/vitest';
import { Effect, Stream } from 'effect';

import { CodeExecutor } from '../src/code-executor.js';
import { NodeCodeExecutor } from '../src/code-executor-node.js';

// The real sandbox host process, driven through the public executor service.

const echo: CodeExecutor.Request['tools'] = [
  {
    name: 'echo',
    description: 'Return the input.',
    parameters: { type: 'object' },
    result: { type: 'object' },
  },
];

/** Runs `source` in the real host, answering nested calls via `answer`. */
const run = (
  source: string,
  options: {
    readonly limits?: Partial<CodeExecutor.Limits>;
    readonly answer?: (
      call: CodeExecutor.ToolCall,
    ) => CodeExecutor.ToolResponse;
  } = {},
) =>
  Effect.gen(function* () {
    const executor = yield* CodeExecutor.Service;
    const execution = yield* executor.start({
      source,
      tools: echo,
      state: {},
      limits: { ...CodeExecutor.defaultLimits, ...options.limits },
    });
    const answer = options.answer;
    const events = yield* execution.events.pipe(
      Stream.tap((event) =>
        event._tag === 'ToolCall' && answer !== undefined
          ? execution.respond(answer(event))
          : Effect.void,
      ),
      Stream.runCollect,
    );
    return Array.from(events);
  });

const echoBack = (call: CodeExecutor.ToolCall): CodeExecutor.ToolResponse => ({
  id: call.id,
  outcome: 'success',
  value: call.input,
});

describe('node code executor', () => {
  it.effect('runs erasable TypeScript and brokers nested calls and state', () =>
    Effect.gen(function* () {
      const events = yield* run(
        `
          type Echo = { readonly value: string }
          const result: Echo = await tools.echo({ value: "typed" } satisfies Echo)
          store("answer", result)
          const shared = { value: 1 }
          store("pair", { left: shared, right: shared })
          text(result.value)
          return { answer: 42, nested: [true, null] }
        `,
        { answer: echoBack },
      );

      expect(events).toEqual([
        { _tag: 'ToolCall', id: '1', name: 'echo', input: { value: 'typed' } },
        { _tag: 'Output', value: 'typed' },
        {
          _tag: 'Completion',
          state: {
            answer: { value: 'typed' },
            pair: { left: { value: 1 }, right: { value: 1 } },
          },
          result: { answer: 42, nested: [true, null] },
        },
      ]);
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('surfaces nested failures and call keys to the script', () =>
    Effect.gen(function* () {
      const failed = yield* run(
        `
          try {
            await tools.echo({ value: "fail" })
          } catch (error) {
            if (!(error instanceof ToolCallError)) throw error
            return { name: error.name, code: error.code, tool: error.tool,
              message: error.message, value: error.value }
          }
        `,
        {
          answer: (call) => ({
            id: call.id,
            outcome: 'failure',
            error: {
              code: 'tool_failure',
              message: 'Echo refused the value',
              value: { reason: 'refused' },
            },
          }),
        },
      );
      expect(failed.at(-1)).toEqual({
        _tag: 'Completion',
        state: {},
        result: {
          name: 'ToolCallError',
          code: 'tool_failure',
          tool: 'echo',
          message: 'Echo refused the value',
          value: { reason: 'refused' },
        },
      });

      const keyed = yield* run(
        `
          await tools.echo({ n: 1 }, { key: "first" })
          await tools.echo({ n: 2 })
          try { await tools.echo({ n: 3 }, { key: "" }) } catch (error) {
            text(String(error.message))
          }
        `,
        { answer: echoBack },
      );
      expect(
        keyed.flatMap((event) =>
          event._tag === 'ToolCall' ? [event.key] : [],
        ),
      ).toEqual(['first', undefined]);
      expect(keyed).toContainEqual({
        _tag: 'Output',
        value:
          'Tool call key must be a non-empty string of at most 256 characters',
      });
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('isolates the script from ambient authority', () =>
    Effect.gen(function* () {
      expect(
        yield* run(
          'text([typeof process, typeof require, typeof fetch].join(","))',
        ),
      ).toEqual([
        { _tag: 'Output', value: 'undefined,undefined,undefined' },
        { _tag: 'Completion', state: {} },
      ]);

      const escape = yield* run(`
        let escaped = false
        for (const value of [globalThis, text, store, load, tools.echo, ALL_TOOLS[0]]) {
          try {
            value.constructor.constructor("return process")()
            escaped = true
          } catch {}
        }
        text(escaped)
      `);
      expect(escape[0]).toEqual({ _tag: 'Output', value: 'false' });

      expect((yield* run('await import("node:fs")')).at(-1)?._tag).toBe(
        'Failure',
      );
      expect(
        yield* run('enum Answer { Yes = "yes" }; text(Answer.Yes)'),
      ).toEqual([
        {
          _tag: 'Failure',
          message: 'TypeScript source must use erasable syntax',
        },
      ]);
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('enforces source, output, and nested-call limits', () =>
    Effect.gen(function* () {
      const executor = yield* CodeExecutor.Service;
      const tooLarge = yield* executor
        .start({
          source: 'text("too large")',
          tools: echo,
          state: {},
          limits: { ...CodeExecutor.defaultLimits, maxSourceBytes: 4 },
        })
        .pipe(Effect.flip);
      expect(tooLarge.message).toContain('exceeds 4 bytes');

      const outputFailure = [
        { _tag: 'Failure', message: 'Code output exceeds 4 bytes' },
      ];
      const limits = { maxOutputBytes: 4 };
      expect(yield* run('text("12345")', { limits })).toEqual(outputFailure);
      expect(yield* run('return { value: "12345" }', { limits })).toEqual(
        outputFailure,
      );

      const nested = yield* run(
        'await tools.echo({ value: 1 }); await tools.echo({ value: 2 })',
        { limits: { maxNestedCalls: 1 }, answer: echoBack },
      );
      expect(nested.at(-1)).toEqual({
        _tag: 'Failure',
        message: 'Nested tool calls exceed 1',
      });
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );

  it.effect('terminates a runaway script and fails closed when unusable', () =>
    Effect.gen(function* () {
      const timed = yield* run('while (true) {}', {
        limits: { wallClockMillis: 100 },
      });
      expect(timed).toHaveLength(1);
      expect(timed[0]?._tag).toBe('Failure');

      const failure = (hostUrl: URL) =>
        Effect.gen(function* () {
          const executor = yield* CodeExecutor.Service;
          const execution = yield* executor.start({
            source: 'text("no")',
            tools: echo,
            state: {},
            limits: CodeExecutor.defaultLimits,
          });
          return yield* Stream.runDrain(execution.events).pipe(Effect.flip);
        }).pipe(
          Effect.provide(
            NodeCodeExecutor.layer({
              hostUrl,
            }),
          ),
        );
      expect(
        (yield* failure(new URL('../host/does-not-exist.mjs', import.meta.url)))
          .reason,
      ).toBe('unavailable');
      const malformed = yield* failure(
        new URL('./fixtures/malformed-code-host.mjs', import.meta.url),
      );
      expect(malformed.reason).toBe('protocol');
      expect(malformed.message).toContain('invalid event');
    }).pipe(Effect.provide(NodeCodeExecutor.layer())),
  );
});
