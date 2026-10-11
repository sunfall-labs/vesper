import { describe, expect, it } from '@effect/vitest';
import { Effect, Ref, Schema, Stream } from 'effect';
import { Response, Tool, Toolkit } from 'effect/ai';

import * as ToolResolution from '../src/internal/tool-resolution.js';

const call = (name: string, params: unknown): ToolResolution.ModelCall =>
  Response.makePart('tool-call', {
    id: `call-${name}`,
    name,
    params,
    providerExecuted: false,
  });

const resolveOne = (
  needsApproval: Tool.NeedsApproval<
    Schema.Struct<{ id: typeof Schema.String }>
  >,
  params: unknown,
) =>
  Effect.gen(function* () {
    const ran = yield* Ref.make(0);
    const guarded = Tool.make('guarded', {
      parameters: Schema.Struct({ id: Schema.String }),
      success: Schema.String,
      failureMode: 'return',
    }).setNeedsApproval(needsApproval);
    const toolkit = Toolkit.make(guarded);
    const resolved = yield* toolkit.pipe(
      Effect.provide(
        toolkit.toLayer({
          guarded: ({ id }) =>
            Ref.update(ran, (n) => n + 1).pipe(Effect.as(id)),
        }),
      ),
    );
    const parts = yield* ToolResolution.resolve(
      resolved,
      [call('guarded', params)],
      { answered: new Set(), messages: [], concurrency: undefined },
    ).pipe(Stream.runCollect);
    return { parts: Array.from(parts), ran: yield* Ref.get(ran) };
  });

describe('ToolResolution approval', () => {
  it.effect('fails closed when the approval predicate fails', () =>
    Effect.gen(function* () {
      const { parts, ran } = yield* resolveOne(
        () => Effect.die(new Error('policy service unavailable')),
        { id: 'r1' },
      );

      expect(ran).toBe(0);
      expect(parts.map((part) => part.type)).toEqual(['tool-approval-request']);
    }),
  );

  it.effect('fails closed when the approval predicate throws', () =>
    Effect.gen(function* () {
      const { parts, ran } = yield* resolveOne(
        () => {
          throw new Error('bad predicate');
        },
        { id: 'r1' },
      );

      expect(ran).toBe(0);
      expect(parts.map((part) => part.type)).toEqual(['tool-approval-request']);
    }),
  );

  it.effect('asks for no approval when the parameters do not decode', () =>
    Effect.gen(function* () {
      const { parts, ran } = yield* resolveOne(true, { id: 42 });

      // The parameter failure answers the call before anything runs, so
      // nobody is asked to approve a call that cannot run.
      expect(ran).toBe(0);
      expect(parts).toMatchObject([{ type: 'tool-result', isFailure: true }]);
    }),
  );
});
