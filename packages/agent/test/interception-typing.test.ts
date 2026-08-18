import { describe, expect, it } from '@effect/vitest';
import { Effect, Schema } from 'effect';
import { Tool } from 'effect/unstable/ai';

import { Interception } from '../src/interception.js';

const approve = Tool.make('approve', {
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ accepted: Schema.Boolean }),
  failure: Schema.Struct({ reason: Schema.String }),
});

const compileTimeAssertions = () => {
  // @ts-expect-error success values are checked against the selected tool
  Interception.answerFor(approve, { accepted: 'yes' });
  // @ts-expect-error failures are checked against the selected tool
  Interception.refuseFor(approve, { reason: 403 });

  Interception.forTool(approve, ({ name, params }) => {
    const literalName: 'approve' = name;
    const id: string = params.id;
    void [literalName, id];
    // @ts-expect-error parameters are decoded to the selected tool's schema
    params.missing;
    return Effect.succeed(Interception.dispatch);
  });
};
void compileTimeAssertions;

describe('typed interception answers', () => {
  it.effect('encodes success and failure through the selected tool', () =>
    Effect.gen(function* () {
      expect(
        yield* Interception.answerFor(approve, { accepted: true }),
      ).toEqual({
        _tag: 'Answer',
        result: { accepted: true },
        isFailure: false,
      });
      expect(
        yield* Interception.refuseFor(approve, { reason: 'policy' }),
      ).toEqual({
        _tag: 'Answer',
        result: { reason: 'policy' },
        isFailure: true,
      });
    }),
  );

  it('rejects values outside the tool schema at compile time', () => {
    expect(true).toBe(true);
  });

  it.effect('decodes matching calls and ignores every other tool', () => {
    const seen: Array<string> = [];
    const interceptor = Interception.forTool(approve, ({ params }) => {
      seen.push(params.id);
      return Effect.succeed(
        Interception.refuse({ reason: 'approval-required' }),
      );
    });
    const call = (name: string, params: unknown) =>
      interceptor.beforeToolCall!({
        agent: 'support',
        conversationId: undefined,
        toolCallId: undefined,
        name,
        params,
      });

    return Effect.gen(function* () {
      expect(yield* call('other', { ignored: true })).toEqual(
        Interception.dispatch,
      );
      expect(yield* call('approve', { id: 'order-1' })).toEqual(
        Interception.refuse({ reason: 'approval-required' }),
      );
      expect(seen).toEqual(['order-1']);
    });
  });

  it.effect('reports invalid matching parameters as an AiError', () => {
    const interceptor = Interception.forTool(approve, () =>
      Effect.succeed(Interception.dispatch),
    );

    return Effect.gen(function* () {
      const result = yield* interceptor.beforeToolCall!({
        agent: 'support',
        conversationId: undefined,
        toolCallId: undefined,
        name: 'approve',
        params: { id: 42 },
      }).pipe(Effect.result);

      expect(result._tag).toBe('Failure');
      if (result._tag === 'Failure') {
        expect(result.failure).toMatchObject({
          _tag: 'AiError',
          module: 'Interception',
          method: 'decodeParameters',
        });
      }
    });
  });
});
