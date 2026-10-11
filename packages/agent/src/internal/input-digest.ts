import { Array as Arr, Order } from 'effect';

/**
 * The text a step's `inputDigest` covers: its name and input as JSON, with
 * object keys sorted so two encodings of the same value agree.
 *
 * The digest is taken before any recording policy runs, so a redacted
 * `StepOpened.params` still identifies its call exactly.
 */
export const material = (name: string, input: unknown): string =>
  JSON.stringify([name, canonical(input)]);

const byKey = Order.mapInput(
  Order.String,
  ([key]: readonly [string, unknown]) => key,
);

const canonical = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(canonical);
  }
  if (value === null || typeof value !== 'object') {
    return value;
  }
  return Object.fromEntries(
    Arr.sort(Object.entries(value), byKey).map(([key, item]) => [
      key,
      canonical(item),
    ]),
  );
};
