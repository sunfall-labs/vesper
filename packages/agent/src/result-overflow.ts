import { AttachmentRef } from '@sunfall/vesper-attachments/ref';
import { AttachmentStore } from '@sunfall/vesper-attachments/attachment-store';
import { Effect, Schema, Stream } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

// Oversized tool results, spilled instead of rejected.
//
// `packages/mcp` hard-rejects a result once it crosses `maxResultBytes`: the
// tool call fails and the model never sees the data at all. That is the
// wrong failure mode for a result that is merely *big* — a large file read,
// a verbose log dump — because the model usually only needs the first part
// of it, or a part it can ask for by offset. Claude's Agent SDK's answer is
// the shape reused here: store the full result and hand the model a small,
// well-defined pointer instead, plus a tool to read the stored bytes back in
// ranges.
//
// Off by default. `Agent.Definition.resultOverflow` is the only new surface;
// unset, `wrap` below returns its input toolkit unchanged and `Tools`
// resolves to `{}`, so an agent that never sets it compiles and dispatches
// exactly as it did before this module existed.
//
// This spills the value the *toolkit* returns — the encoded result a tool
// handler produced — not anything at the MCP transport. An MCP SDK call
// still buffers its whole response before this ever sees it; see the
// package README for that boundary.

/** Configuration for one agent's overflow behaviour. */
export interface Policy {
  /** Encoded results at or under this many UTF-8 bytes pass through unchanged. */
  readonly threshold: number;
  /** Characters of head preview carried on the pointer. Defaults to 500. */
  readonly preview?: number | undefined;
}

const DEFAULT_PREVIEW_CHARS = 500;

const utf8 = new TextEncoder();

export const TOOL_NAME = 'read_attachment';

/**
 * What the model sees in place of a result that overflowed.
 *
 * Deliberately small and self-describing: an id, how big the real thing is,
 * what it looks like, and enough of the start to often make a second call
 * unnecessary. `mediaType` and `byteLength` are carried again on `read_attachment`'s
 * own parameters — see the note on {@link reader} — so the pointer alone is
 * everything the model needs to retrieve the rest.
 */
// TaggedStruct currently emits the branded attachment id as explicit `any` in
// the published declaration. Keep the equivalent explicit struct until its
// declaration output preserves the Digest type.
export const Pointer: Schema.Struct<{
  _tag: Schema.Literal<'ToolResultOverflow'>;
  attachmentId: typeof AttachmentRef.Digest;
  byteLength: typeof Schema.Natural;
  mediaType: typeof Schema.String;
  preview: typeof Schema.String;
}> =
  // oxlint-disable-next-line effecttsgo/schema-struct-with-tag
  Schema.Struct({
    _tag: Schema.Literal('ToolResultOverflow'),
    attachmentId: AttachmentRef.Digest,
    byteLength: Schema.Natural,
    mediaType: Schema.String,
    preview: Schema.String,
  });
export interface Pointer extends Schema.Struct.Type<typeof Pointer.fields> {}

/** Whether a value is a spilled-result pointer, independent of any tool's schema. */
export const isPointer: (value: unknown) => value is Pointer =
  Schema.is(Pointer);

/** The tool record contributed when overflow is enabled. */
export type Tools<P extends Policy | undefined> = P extends Policy
  ? Record<typeof TOOL_NAME, ReturnType<typeof makeReadTool>>
  : Record<never, never>;

/** Services `read_attachment`'s handler needs from the run. */
export type Services<P extends Policy | undefined> = P extends Policy
  ? AttachmentStore.Service
  : never;

const ReadSuccess = Schema.Struct({
  content: Schema.String,
  offset: Schema.Natural,
  length: Schema.Natural,
  totalBytes: Schema.Natural,
  hasMore: Schema.Boolean,
});

const ReadFailure: Schema.Union<
  readonly [
    typeof AttachmentStore.AttachmentNotFound,
    typeof AttachmentStore.AttachmentIntegrityError,
    typeof AttachmentStore.AttachmentStoreError,
  ]
> = Schema.Union([
  AttachmentStore.AttachmentNotFound,
  AttachmentStore.AttachmentIntegrityError,
  AttachmentStore.AttachmentStoreError,
]);

const ReadParameters: Schema.Struct<{
  attachmentId: typeof AttachmentRef.Digest;
  mediaType: typeof Schema.String;
  byteLength: typeof Schema.Natural;
  offset: Schema.optionalKey<typeof Schema.Natural>;
  length: Schema.optionalKey<typeof Schema.Natural>;
}> = Schema.Struct({
  attachmentId: AttachmentRef.Digest,
  mediaType: Schema.String,
  byteLength: Schema.Natural,
  offset: Schema.optionalKey(Schema.Natural),
  length: Schema.optionalKey(Schema.Natural),
});

type ReadToolConfig = {
  readonly parameters: typeof ReadParameters;
  readonly success: typeof ReadSuccess;
  readonly failure: typeof ReadFailure;
  readonly failureMode: 'return';
};

const makeReadTool = (
  maxLength: number,
): Tool.Tool<typeof TOOL_NAME, ReadToolConfig, AttachmentStore.Service> =>
  Tool.make(TOOL_NAME, {
    description:
      'Read back a tool result that overflowed into storage. Copy ' +
      'attachmentId, mediaType, and byteLength from the pointer you received. ' +
      `offset and length are byte offsets into the original content; length ` +
      `defaults to ${String(maxLength)} and is capped at it, so a large attachment is ` +
      'read in pages by advancing offset until hasMore is false.',
    parameters: ReadParameters,
    success: ReadSuccess,
    failure: ReadFailure,
    failureMode: 'return',
    // The handler pulls `AttachmentStore.Service` from context; declaring it
    // here is what lets that be a genuine dependency rather than a captured
    // ambient. See docs/contributing.md, "Subagent services".
    dependencies: [AttachmentStore.Service],
  });

/**
 * The reading tool and its handler, generated for an agent whose
 * `resultOverflow` policy is set.
 *
 * Mirrors `skill.ts`'s `loader`: a self-contained tool the agent supplies its
 * own handler for, so an application never implements it. Unlike the skill
 * loader, the handler is not closure-complete — it needs `AttachmentStore.Service`
 * from context, which is why the tool declares it as a dependency and why
 * `Agent.make` folds {@link Services} into the compiled agent's requirements.
 */
export const reader = (policy: Policy) => {
  const maxLength = Math.max(1, Math.trunc(policy.threshold));
  const tool = makeReadTool(maxLength);

  const handler = (
    input: typeof ReadParameters.Type,
  ): Effect.Effect<
    typeof ReadSuccess.Type,
    typeof ReadFailure.Type,
    AttachmentStore.Service
  > =>
    Effect.gen(function* () {
      const store = yield* AttachmentStore.Service;
      const ref = AttachmentRef.Ref.make({
        digest: input.attachmentId,
        mediaType: input.mediaType,
        byteLength: input.byteLength,
      });
      const bytes = yield* store.get(ref);
      const offset = clamp(input.offset ?? 0, 0, bytes.byteLength);
      const requested = Math.min(input.length ?? maxLength, maxLength);
      const end = clamp(offset + requested, offset, bytes.byteLength);
      const slice = bytes.slice(offset, end);
      return {
        content: new TextDecoder().decode(slice),
        offset,
        length: slice.byteLength,
        totalBytes: bytes.byteLength,
        hasMore: end < bytes.byteLength,
      };
    });

  const kit = Toolkit.make(tool);
  return {
    tool,
    handler,
    toolkit: kit,
    layer: kit.toLayer({ [TOOL_NAME]: handler }),
  };
};

const clamp = (value: number, min: number, max: number): number =>
  Math.min(Math.max(value, min), max);

/**
 * Spill a tool's oversized results into `AttachmentStore` and hand the model
 * a {@link Pointer} instead.
 *
 * Wraps `handle` rather than any one tool's schema, so it applies uniformly
 * to every tool a toolkit resolves to — including `read_attachment` itself, a
 * deliberate choice: a caller who asks for more than the threshold in one
 * read gets the extra spilled again rather than a special case.
 *
 * A storage failure while spilling is left to die rather than added to the
 * tool's typed failure channel. `Toolkit.WithHandler['handle']`'s error type
 * is fixed by the resolved toolkit and cannot be widened per call; a backend
 * that cannot store is an infrastructure fault a tool caller has no
 * meaningful recovery for, the same judgment `AttachmentRef.digestOf` makes
 * about a broken platform digest.
 */
export function wrap<
  ToolSet extends Record<string, Tool.Any>,
  E,
  R,
  P extends Policy | undefined,
>(
  policy: P,
  toolkit: Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R>,
): Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R | Services<P>>;
export function wrap<ToolSet extends Record<string, Tool.Any>, E, R>(
  policy: Policy | undefined,
  toolkit: Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R>,
): Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R | AttachmentStore.Service>;
export function wrap<ToolSet extends Record<string, Tool.Any>, E, R>(
  policy: Policy | undefined,
  toolkit: Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R>,
): Effect.Effect<Toolkit.WithHandler<ToolSet>, E, R | AttachmentStore.Service> {
  if (policy === undefined) {
    return toolkit;
  }
  const threshold = policy.threshold;
  const previewChars = policy.preview ?? DEFAULT_PREVIEW_CHARS;

  return Effect.gen(function* () {
    const resolved = yield* toolkit;
    const store = yield* AttachmentStore.Service;

    const handle: Toolkit.WithHandler<ToolSet>['handle'] = (
      name,
      params,
      toolCallId,
    ) =>
      resolved
        .handle(name, params, toolCallId)
        .pipe(
          Effect.map((stream) =>
            Stream.mapEffect(stream, (result) =>
              spill(store, threshold, previewChars, result),
            ),
          ),
        );

    return { tools: resolved.tools, handle };
  });
}

const spill = <T extends Tool.Any>(
  store: AttachmentStore.Interface,
  threshold: number,
  previewChars: number,
  result: Tool.HandlerResult<T>,
): Effect.Effect<Tool.HandlerResult<T>> =>
  Effect.gen(function* () {
    if (isPointer(result.encodedResult)) {
      return result;
    }

    const text = encodeAsText(result.encodedResult);
    // One UTF-16 code unit never encodes to more than 3 UTF-8 bytes (a
    // surrogate pair is 2 units for 4 bytes), so this bound proves the
    // common case — a result nowhere near the threshold — without paying
    // for a full encode of every result that passes through.
    if (text.length * 3 <= threshold) {
      return result;
    }
    const bytes = utf8.encode(text);
    if (bytes.byteLength <= threshold) {
      return result;
    }

    const mediaType =
      typeof result.encodedResult === 'string'
        ? 'text/plain; charset=utf-8'
        : 'application/json';
    const ref = yield* store.put(bytes, { mediaType }).pipe(Effect.orDie);
    const pointer: Pointer = {
      _tag: 'ToolResultOverflow',
      attachmentId: ref.digest,
      byteLength: ref.byteLength,
      mediaType,
      preview: sanitizePreview(text, previewChars),
    };

    return {
      ...result,
      result: pointer as Tool.Result<T>,
      encodedResult: pointer,
    };
  });

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

export * as ResultOverflow from './result-overflow.js';
