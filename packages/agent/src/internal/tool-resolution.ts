import { Cause, Effect, Option, Predicate, Schema, Stream } from 'effect';
import {
  AiError,
  IdGenerator,
  type Prompt,
  Response,
  type Tool,
  type Toolkit,
} from 'effect/ai';
import * as ToolLookup from './tool-lookup.js';

// The agent runs its own tool calls.
//
// The model is asked with resolution disabled and deferred validation, so
// every call it makes comes back as a plain tool-call part: a call to a tool
// this turn did not offer, or with parameters that do not decode, is a call
// like any other rather than a reason to fail the turn. This module decides
// what each call gets, once, and every later consumer — execution, the event
// stream, the recording sink, the rebuilt prompt — reads that one decision.
//
// Calls run after the model's response has finished, never during it. A
// response the provider cut off (`length`, `content-filter`, `error`) is not
// resolved at all: none of its calls has started, so there is nothing to
// interrupt and nothing whose effects are unknown.

/** A tool-call part as the model wrote it. */
export type ModelCall = Response.ToolCallPart<string, unknown>;

/** The failed result for a call to a tool the turn did not offer. */
export type UndeclaredResult = Response.ToolResultPart<
  string,
  unknown,
  unknown
>;

/**
 * What resolution emits in place of a call: the tool's own result, the
 * failure for an undeclared tool, or an approval request.
 */
export type Resolution<
  Tools extends Record<string, Tool.Any> = Record<string, Tool.Any>,
> =
  | Response.ToolResultParts<Tools>
  | UndeclaredResult
  | Response.ToolApprovalRequestPart;

/**
 * The decision for one call, made once.
 *
 * `Object.hasOwn` and nothing else decides whether a name is a tool: a model
 * that writes `toString` or `__proto__` has named nothing in the toolkit,
 * whatever the prototype chain of the record holding it says.
 */
type Classified =
  | { readonly _tag: 'Unknown'; readonly call: ModelCall }
  | {
      readonly _tag: 'Approval';
      readonly call: ModelCall;
    }
  | { readonly _tag: 'Run'; readonly call: ModelCall };

const toolNotFound = (
  call: ModelCall,
  available: ReadonlyArray<string>,
): Effect.Effect<UndeclaredResult> =>
  Effect.map(
    Schema.encodeUnknownEffect(AiError.AiError)(
      new AiError.AiError({
        module: 'Agent',
        method: 'resolveToolCall',
        reason: new AiError.ToolNotFoundError({
          toolName: call.name,
          availableTools: [...available],
        }),
      }),
    ).pipe(Effect.orDie),
    (encoded) =>
      Response.toolResultPart({
        id: call.id,
        name: call.name,
        result: encoded,
        encodedResult: encoded,
        isFailure: true,
        providerExecuted: false,
        preliminary: false,
      }),
  );

/**
 * Whether a call to a known tool waits for approval.
 *
 * A static `needsApproval: true` and a predicate are both asked only about
 * parameters that decode: nobody should be asked to approve a call that cannot
 * run. A call whose parameters do not decode goes to `handle`, which answers
 * it with the parameter failure before anything runs.
 *
 * A predicate that fails or throws fails closed: the call waits for approval.
 * An approval gate that disappears whenever its policy check is unavailable
 * is not a gate.
 */
const needsApproval = (
  tool: Tool.Any,
  call: ModelCall,
  messages: ReadonlyArray<Prompt.Message>,
): Effect.Effect<boolean> => {
  const gate = tool.needsApproval;
  if (gate === undefined || gate === false) {
    return Effect.succeed(false);
  }
  // A tool's parameter schema carries its decoding services as `unknown`;
  // the same services `handle` decodes with are in context here, which is
  // what the assertion below states (`LanguageModel` makes the same one).
  return Schema.decodeUnknownEffect(tool.parametersSchema)(call.params).pipe(
    Effect.matchEffect({
      onFailure: () => Effect.succeed(false),
      onSuccess: (params): Effect.Effect<boolean> =>
        gate === true
          ? Effect.succeed(true)
          : Effect.suspend(() => {
              const answer = gate(params, { toolCallId: call.id, messages });
              return Effect.isEffect(answer) ? answer : Effect.succeed(answer);
            }).pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause)
                  : Effect.succeed(true),
              ),
            ),
    }),
  ) as Effect.Effect<boolean>;
};

/**
 * The id of an approval request. A recorded conversation supplies one drawn
 * from `Crypto`, because the id becomes its durable wait token; otherwise this
 * falls back to Effect's default, as `LanguageModel` itself does.
 */
const approvalId: Effect.Effect<string> = Effect.serviceOption(
  IdGenerator.IdGenerator,
).pipe(
  Effect.flatMap((ids) =>
    Option.getOrElse(ids, () => IdGenerator.defaultIdGenerator).generateId(),
  ),
);

const classify = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
  call: ModelCall,
  messages: ReadonlyArray<Prompt.Message>,
): Effect.Effect<Classified> => {
  const tool = ToolLookup.toolNamed(toolkit.tools, call.name);
  if (tool === undefined) {
    return Effect.succeed({ _tag: 'Unknown', call });
  }
  return Effect.map(
    needsApproval(tool, call, messages),
    (approval): Classified =>
      approval ? { _tag: 'Approval', call } : { _tag: 'Run', call },
  );
};

/**
 * A handler result as a tool-result part.
 *
 * Fields beyond `Tool.HandlerResult`'s own — the dispatch seam marks results it
 * answered in a tool's place with `resultSource` — are carried onto the part,
 * which is why this builds the part from the result rather than listing its
 * fields.
 */
const resultPart = <Tools extends Record<string, Tool.Any>>(
  call: ModelCall,
  result: Tool.HandlerResult<Tools[keyof Tools]>,
): Resolution<Tools> => {
  const resultSource =
    'resultSource' in result && Predicate.isString(result.resultSource)
      ? { resultSource: result.resultSource }
      : {};
  return {
    ...Response.makePart('tool-result', {
      id: call.id,
      name: call.name,
      result: result.result,
      encodedResult: result.encodedResult,
      isFailure: result.isFailure,
      providerExecuted: false,
      preliminary: result.preliminary,
    }),
    ...resultSource,
  };
};

/**
 * Resolve one turn's calls.
 *
 * `answered` holds the ids `LanguageModel` already answered in this response —
 * approvals granted on an earlier run, executed before the request — so they
 * are not run a second time.
 */
export const resolve = <Tools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<Tools>,
  calls: ReadonlyArray<ModelCall>,
  options: {
    readonly answered: ReadonlySet<string>;
    readonly messages: ReadonlyArray<Prompt.Message>;
    readonly concurrency: number | 'unbounded' | undefined;
  },
): Stream.Stream<
  Resolution<Tools>,
  AiError.AiError | Tool.HandlerError<Tools[keyof Tools]>,
  Tool.HandlerServices<Tools[keyof Tools]>
> => {
  const pending = calls.filter(
    (call) => !call.providerExecuted && !options.answered.has(call.id),
  );
  if (pending.length === 0) {
    return Stream.empty;
  }
  const available = Object.keys(toolkit.tools);
  return Stream.mergeAll(
    pending.map((call) =>
      Stream.unwrap(
        Effect.map(
          classify(toolkit, call, options.messages),
          (
            decided,
          ): Stream.Stream<
            Resolution<Tools>,
            AiError.AiError | Tool.HandlerError<Tools[keyof Tools]>,
            Tool.HandlerServices<Tools[keyof Tools]>
          > => {
            if (decided._tag === 'Unknown') {
              return Stream.fromEffect(toolNotFound(call, available));
            }
            if (decided._tag === 'Approval') {
              return Stream.fromEffect(
                approvalId.pipe(
                  Effect.map(
                    (id): Resolution<Tools> =>
                      Response.makePart('tool-approval-request', {
                        approvalId: id,
                        toolCallId: call.id,
                      }),
                  ),
                ),
              );
            }
            // The model's own value; `handle` decodes it against the tool's
            // parameter schema before anything runs.
            return Stream.unwrap(
              toolkit.handle(
                call.name,
                call.params as Tool.ParametersEncoded<Tools[keyof Tools]>,
                call.id,
              ),
            ).pipe(Stream.map((result) => resultPart<Tools>(call, result)));
          },
        ),
      ),
    ),
    { concurrency: options.concurrency ?? 'unbounded' },
  );
};

/** The model's own calls in a response, as written, for {@link resolve}. */
export const callsOf = (
  parts: ReadonlyArray<Response.AnyPart>,
): {
  readonly calls: ReadonlyArray<ModelCall>;
  readonly answered: ReadonlySet<string>;
} => {
  const calls: Array<ModelCall> = [];
  const answered = new Set<string>();
  for (const part of parts) {
    if (part.type === 'tool-call') {
      calls.push(part);
    } else if (part.type === 'tool-result') {
      answered.add(part.id);
    }
  }
  return { calls, answered };
};
