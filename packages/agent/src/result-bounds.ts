import { Effect, Schema, Stream } from 'effect';
import type { Tool, Toolkit } from 'effect/unstable/ai';

import { ResultOverflow } from './result-overflow.js';

// A default per-result byte bound, so one oversized tool result cannot
// poison a conversation that never configured `resultOverflow`.
//
// `resultOverflow` is opt-in and spills into an `AttachmentStore` the
// application has to wire. Most agents never set it, and an unbounded
// result still reaches the model's context and the durable log exactly as
// the handler produced it — a single verbose tool response can then blow the
// context window or bloat every future turn's history. `resultBounds` is the
// unconditional backstop: unlike `resultOverflow` it needs no extra service,
// contributes no extra tool, and is on by default at 64 KiB. It has no
// recovery path — the excess is simply gone, replaced by a small preview —
// which is why `resultOverflow`, when configured, always gets the chance to
// spill first; see {@link wrap}.
//
// `Agent.Definition.resultBounds` is the only new surface. Passing `false`
// restores the pre-existing unbounded behaviour.

/** Configuration for one agent's default result-bounding behaviour. */
export interface Policy {
  /** Encoded results at or under this many UTF-8 bytes pass through unchanged. */
  readonly maxBytes: number;
}

/** The default bound: 64 KiB of encoded result. */
export const DEFAULT_MAX_BYTES: number = 64 * 1024;

/** The policy applied when `resultBounds` is left unset. */
export const defaultPolicy: Policy = { maxBytes: DEFAULT_MAX_BYTES };

/** Characters of head preview carried on a truncation envelope. Not configurable. */
export const PREVIEW_CHARS: number = 500;

const utf8 = new TextEncoder();

/**
 * What the model sees in place of a result that exceeded its bound.
 *
 * Deliberately the same small shape regardless of which tool produced it:
 * how big the real thing was, what the bound was, and enough of the start to
 * often make the truncation harmless. Unlike {@link ResultOverflow.Pointer}
 * there is nothing left to read back — the excess is not stored anywhere —
 * so this is a terminal notice, not a pointer.
 */
export const Truncation: Schema.Struct<{
  truncated: Schema.Literal<true>;
  bytes: typeof Schema.Natural;
  maxBytes: typeof Schema.Natural;
  preview: typeof Schema.String;
}> = Schema.Struct({
  truncated: Schema.Literal(true),
  bytes: Schema.Natural,
  maxBytes: Schema.Natural,
  preview: Schema.String,
});
export interface Truncation extends Schema.Struct.Type<
  typeof Truncation.fields
> {}

/** Whether a value is a truncation envelope, independent of any tool's schema. */
export const isTruncation: (value: unknown) => value is Truncation =
  Schema.is(Truncation);

/**
 * Bound every tool result to `policy.maxBytes` of encoded size, replacing an
 * excess with a small {@link Truncation} envelope.
 *
 * Wraps `handle` the same way {@link ResultOverflow.wrap} does, and is meant
 * to compose with it rather than replace it: pass the overflow-wrapped
 * toolkit in as `toolkit` so overflow's spill runs first. A spilled result is
 * already a small {@link ResultOverflow.Pointer} by the time this sees it, so
 * it passes the size check trivially — this only ever bounds a result
 * overflow did not spill, which is exactly right when overflow is configured
 * with a threshold larger than `maxBytes`, or not configured at all.
 *
 * `policy` is `Policy | false | undefined`, not `Policy | undefined`, because
 * this bound is on by default: unset applies {@link defaultPolicy}, and `false`
 * is the explicit opt-out back to unbounded results.
 *
 * A provider-executed tool call never reaches a toolkit's `handle` at all, so
 * it never reaches this wrapper either — nothing here has to special-case it.
 */
export function wrap<ToolSet extends Record<string, Tool.Any>, E, R>(
  policy: Policy | false | undefined,
  toolkit: Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R>,
): Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R> {
  if (policy === false) {
    return toolkit;
  }
  const maxBytes = Math.max(
    1,
    Math.trunc(policy === undefined ? DEFAULT_MAX_BYTES : policy.maxBytes),
  );

  return Effect.map(toolkit, (resolved) => {
    const handle: Toolkit.WithHandler<ToolSet>['handle'] = (
      name,
      params,
      toolCallId,
    ) =>
      resolved
        .handle(name, params, toolCallId)
        .pipe(
          Effect.map((stream) =>
            Stream.map(stream, (result) => bound(maxBytes, result)),
          ),
        );

    return { tools: resolved.tools, handle };
  });
}

const bound = <T extends Tool.Any>(
  maxBytes: number,
  result: Tool.HandlerResult<T>,
): Tool.HandlerResult<T> => {
  // Already spilled by `resultOverflow`, or already bounded by an earlier
  // wrap in the same pipeline: either way, nothing left here that needs
  // measuring again.
  if (
    ResultOverflow.isPointer(result.encodedResult) ||
    isTruncation(result.encodedResult)
  ) {
    return result;
  }

  const text = encodeAsText(result.encodedResult);
  // Same bound trick `result-overflow.ts` uses: one UTF-16 code unit never
  // encodes to more than 3 UTF-8 bytes, so this proves the common case — a
  // result nowhere near the bound — without a full encode of every result.
  if (text.length * 3 <= maxBytes) {
    return result;
  }
  const bytes = utf8.encode(text);
  if (bytes.byteLength <= maxBytes) {
    return result;
  }

  const envelope: Truncation = {
    truncated: true,
    bytes: bytes.byteLength,
    maxBytes,
    preview: sanitizePreview(text, PREVIEW_CHARS),
  };

  return {
    ...result,
    // Same escape hatch `result-overflow.ts`'s `spill` needs, for the same
    // reason: `Tool.Result<T>` is a conditional type resolved from `T`'s own
    // success/failure schema, and nothing here can prove a `Truncation`
    // envelope satisfies it for an arbitrary `T`. `dispatch.ts`'s `answered`
    // takes the identical cast when substituting a value nobody's handler
    // produced.
    result: envelope as Tool.Result<T>,
    encodedResult: envelope,
  };
};

const encodeAsText = (value: unknown): string => {
  if (typeof value === 'string') {
    return value;
  }
  const encoded = JSON.stringify(value);
  return encoded === undefined ? String(value) : encoded;
};

/** Truncate to a head preview and blank out control characters. */
const sanitizePreview = (text: string, maxChars: number): string => {
  const truncated = text.length > maxChars ? text.slice(0, maxChars) : text;
  return Array.from(truncated, (character) => {
    const code = character.codePointAt(0);
    return code !== undefined && code < 0x20 && code !== 0x0a && code !== 0x09
      ? ' '
      : character;
  }).join('');
};

export * as ResultBounds from './result-bounds.js';
