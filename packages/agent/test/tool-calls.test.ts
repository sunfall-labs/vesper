import { describe, expect, it } from '@effect/vitest';
import { Effect, Schema, Stream } from 'effect';
import { Tool, Toolkit } from 'effect/ai';

import { Agent } from '../src/agent.js';
import { Conversation } from '../src/conversation.js';
import { ScriptedModel } from '../src/testing.js';
import {
  callTurn,
  date,
  outcomesOf,
  promptText,
  provide,
  readRecords,
  tagsOf,
  textTurn,
  toolResultsIn,
} from './tool-fixtures.js';

// Tool-call scenarios: what the model asks for, what the handler receives,
// what is recorded and what the model is shown next.

const WHEN = date('2026-01-02T03:04:05.000Z');

const lookup = Tool.make('lookup', {
  description: 'look an order up',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ status: Schema.String, at: Schema.DateFromString }),
});

const dated = Tool.make('dated', {
  description: 'echo a date',
  parameters: Schema.Struct({ at: Schema.DateFromString }),
  success: Schema.String,
});

const readFileWith = (failureMode: 'error' | 'return') =>
  Tool.make('read_file', {
    description: 'read a file',
    parameters: Schema.Struct({ path: Schema.String }),
    success: Schema.String,
    failureMode,
  });

const agentOf = <const Tools extends Record<string, Tool.Any>>(
  name: string,
  toolkit: Toolkit.Toolkit<Tools>,
) =>
  Agent.make({
    name,
    revision: '1',
    instructions: 'use the tools',
    toolkit,
  });

describe('a tool call that runs', () => {
  it.effect(
    'is recorded ToolCall, ToolStarted, ToolOutcome in order, stored in the toolkit encoding',
    () => {
      const ran: string[] = [];
      const agent = agentOf('ordering', Toolkit.make(lookup)).withHandlers({
        lookup: ({ id }) =>
          Effect.sync(() => {
            ran.push(id);
            return { status: `fresh:${id}`, at: WHEN };
          }),
      });
      const model = ScriptedModel.make([
        callTurn({ id: 'call-1', name: 'lookup', params: { id: '42' } }),
        textTurn('done'),
      ]);
      return provide(
        Effect.gen(function* () {
          const conversation = Conversation.make(agent, 'ordering');
          const events = yield* conversation.stream('hi').pipe(
            Stream.runCollect,
            Effect.map((all) => Array.from(all)),
          );

          const records = yield* readRecords('ordering');
          const tags = tagsOf(records);
          expect(tags.filter((tag) => tag.startsWith('Tool'))).toEqual([
            'ToolCall',
            'ToolStarted',
            'ToolOutcome',
          ]);
          expect(tags[0]).toBe('RunStarted');
          expect(tags.at(-1)).toBe('RunSettled');
          // `at` is a Date in the handler and an ISO string in the log: the
          // encoded form is what the provider is shown and the only form a
          // resuming run can serve back unchanged.
          expect(outcomesOf(records)[0]?.result).toEqual({
            status: 'fresh:42',
            at: WHEN.toISOString(),
          });
          expect(ran).toEqual(['42']);
          expect(promptText(yield* model.requests, 1)).toContain(
            WHEN.toISOString(),
          );

          const result = events.find(
            (event) =>
              event._tag === 'Part' && event.part.type === 'tool-result',
          );
          expect(result).toMatchObject({
            part: { result: { status: 'fresh:42', at: WHEN } },
          });
        }),
        model,
      );
    },
  );

  it.effect(
    'hands the handler the decoded form of transformed parameters',
    () => {
      const seen: unknown[] = [];
      const agent = agentOf('transformed', Toolkit.make(dated)).withHandlers({
        dated: ({ at }) =>
          Effect.sync(() => {
            seen.push(at);
            return at.toISOString();
          }),
      });
      const model = ScriptedModel.make([
        callTurn({
          id: 'call-1',
          name: 'dated',
          params: { at: '2026-08-26T00:00:00.000Z' },
        }),
        textTurn('ok'),
      ]);
      return provide(
        Effect.gen(function* () {
          yield* agent.run('go');
          expect(seen).toEqual([date('2026-08-26T00:00:00.000Z')]);
          const shown = toolResultsIn((yield* model.requests)[1]?.prompt).at(0);
          expect(shown?.result).toBe('2026-08-26T00:00:00.000Z');
        }),
        model,
      );
    },
  );
});

describe('a tool call the model got wrong', () => {
  it.effect.each(['error', 'return'] as const)(
    'answers malformed parameters with a parameter failure and lets the run continue (failureMode %s)',
    (failureMode) => {
      const readFile = readFileWith(failureMode);
      const ran: string[] = [];
      const agent = agentOf('malformed', Toolkit.make(readFile)).withHandlers({
        read_file: ({ path }) =>
          Effect.sync(() => void ran.push(path)).pipe(Effect.as(path)),
      });
      const model = ScriptedModel.make([
        callTurn({
          id: 'call-1',
          name: 'read_file',
          params: { path: { segments: ['src'] } },
        }),
        textTurn('Recovered.'),
      ]);
      return provide(
        Effect.gen(function* () {
          const result = yield* agent.run('read the folder');
          const requests = yield* model.requests;
          const [shown, ...rest] = toolResultsIn(requests[1]?.prompt);

          expect(result.text).toBe('Recovered.');
          expect(requests).toHaveLength(2);
          expect(ran).toEqual([]);
          expect(rest).toEqual([]);
          expect(shown).toMatchObject({
            id: 'call-1',
            isFailure: true,
            result: {
              _tag: 'AiError',
              reason: {
                _tag: 'ToolParameterValidationError',
                toolName: 'read_file',
              },
            },
          });
        }),
        model,
      );
    },
  );

  it.effect.each(['toString', '__proto__', 'constructor', 'no_such_tool'])(
    'answers a call to undeclared tool %s with exactly one failed result',
    (name) => {
      const ran: string[] = [];
      const agent = agentOf(
        'undeclared',
        Toolkit.make(readFileWith('error')),
      ).withHandlers({
        read_file: ({ path }) =>
          Effect.sync(() => void ran.push(path)).pipe(Effect.as(path)),
      });
      const model = ScriptedModel.make([
        callTurn({ id: 'call-x', name, params: { path: 'README.md' } }),
        textTurn('ok'),
      ]);
      return provide(
        Effect.gen(function* () {
          const result = yield* agent.run('read the file');
          const results = toolResultsIn((yield* model.requests)[1]?.prompt);

          expect(result.text).toBe('ok');
          expect(ran).toEqual([]);
          // A name that merely exists on Object.prototype is not a tool: one
          // failed result, never a second from a lookup that found the
          // prototype's property.
          expect(results).toHaveLength(1);
          expect(results[0]).toMatchObject({
            id: 'call-x',
            isFailure: true,
            result: { _tag: 'AiError', reason: { _tag: 'ToolNotFoundError' } },
          });
        }),
        model,
      );
    },
  );
});
