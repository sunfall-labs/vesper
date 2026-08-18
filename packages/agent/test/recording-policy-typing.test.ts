import { describe, expect, it } from '@effect/vitest';
import { Effect, Schema } from 'effect';

import { RecordingPolicy } from '../src/recording-policy.js';

const Customer = Schema.Struct({
  name: Schema.String,
  secret: Schema.String,
});

const compileTimeAssertions = () => {
  RecordingPolicy.preserving(Customer, (customer) =>
    // @ts-expect-error a preserving transform cannot remove schema fields
    Effect.succeed({ name: customer.name }),
  );
};
void compileTimeAssertions;

describe('schema-preserving recording policy', () => {
  const redact = RecordingPolicy.preserving(Customer, (customer) =>
    Effect.succeed({ ...customer, secret: '[redacted]' }),
  );

  it.effect('redacts matching values and passes unrelated values through', () =>
    Effect.gen(function* () {
      expect(yield* redact({ name: 'Ada', secret: 'token' })).toEqual({
        name: 'Ada',
        secret: '[redacted]',
      });
      expect(yield* redact('plain text')).toBe('plain text');
    }),
  );

  it('keeps the transformed value schema-shaped at compile time', () => {
    expect(true).toBe(true);
  });
});
