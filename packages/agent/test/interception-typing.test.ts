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
};
void compileTimeAssertions;

describe('typed cross-cutting interception answers', () => {
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
});
