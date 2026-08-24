import assert from 'node:assert/strict';

import { Effect, Stream } from 'effect';

import { CodeExecutor } from '../packages/agent/dist/code-executor.js';
import { NodeCodeExecutor } from '../packages/agent/dist/code-executor-node.js';

const events = await Effect.gen(function* () {
  const executor = yield* CodeExecutor.Service;
  const execution = yield* executor.start({
    source: `
      type Answer = { readonly value: number }
      const answer: Answer = await tools.echo({ value: 42 } satisfies Answer)
      text("working")
      store("answer", answer.value)
      return answer
    `,
    tools: [
      {
        name: 'echo',
        description: 'Return the input.',
        parameters: { type: 'object' },
        result: { type: 'object' },
      },
    ],
    state: {},
    limits: CodeExecutor.defaultLimits,
  });
  return yield* execution.events.pipe(
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
}).pipe(
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- application entry point
  Effect.provide(NodeCodeExecutor.layer()),
  Effect.runPromise,
);

assert.deepEqual(Array.from(events), [
  {
    _tag: 'ToolCall',
    id: '1',
    name: 'echo',
    input: { value: 42 },
  },
  { _tag: 'Output', value: 'working' },
  { _tag: 'Completion', state: { answer: 42 }, result: { value: 42 } },
]);

const isolationEvents = await Effect.gen(function* () {
  const executor = yield* CodeExecutor.Service;
  const execution = yield* executor.start({
    source: `
      let generatedCode
      try {
        generatedCode = (() => {}).constructor("return 1")()
      } catch (error) {
        generatedCode = error.name
      }
      return {
        process: typeof process,
        bun: typeof Bun,
        fetch: typeof fetch,
        generatedCode,
      }
    `,
    tools: [],
    state: {},
    limits: CodeExecutor.defaultLimits,
  });
  return yield* Stream.runCollect(execution.events);
}).pipe(
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- application entry point
  Effect.provide(NodeCodeExecutor.layer()),
  Effect.runPromise,
);

assert.deepEqual(Array.from(isolationEvents), [
  {
    _tag: 'Completion',
    state: {},
    result: {
      process: 'undefined',
      bun: 'undefined',
      fetch: 'undefined',
      generatedCode: 'EvalError',
    },
  },
]);

const runtime =
  'deno' in process.versions
    ? `Deno ${String(process.versions.deno)}`
    : 'bun' in process.versions
      ? `Bun ${String(process.versions.bun)}`
      : `Node.js ${process.versions.node}`;

process.stdout.write(`code executor smoke passed on ${runtime}\n`);
