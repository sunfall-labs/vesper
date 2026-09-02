import { Effect, Schema } from 'effect';
import { AiError, Response, type Tool, type Toolkit } from 'effect/unstable/ai';

/**
 * A part of one model turn as the Vesper loop requests it: tool parameters
 * decoded, and a call that never reaches a handler returned to the model as a
 * `tool-call-error` part (`invalidToolCalls: 'return'`) rather than failing
 * the turn.
 */
export type ModelTurnPart<PartTools extends Record<string, Tool.Any>> =
  Response.StreamPart<PartTools, false, 'return'>;

const modelTurnPartSchema = <PartTools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<PartTools>,
) => Response.StreamPart(toolkit, { invalidToolCalls: 'return' });

type EncodableToolResult<PartTools extends Record<string, Tool.Any>> = Extract<
  ModelTurnPart<PartTools>,
  { readonly type: 'tool-result' }
>;
type EncodableFile<PartTools extends Record<string, Tool.Any>> = Extract<
  ModelTurnPart<PartTools>,
  { readonly type: 'file' }
>;
const StandardPart = Schema.Union([
  Response.TextStartPart,
  Response.TextDeltaPart,
  Response.TextEndPart,
  Response.ReasoningStartPart,
  Response.ReasoningDeltaPart,
  Response.ReasoningEndPart,
  Response.ToolParamsStartPart,
  Response.ToolParamsDeltaPart,
  Response.ToolParamsEndPart,
  Response.ToolApprovalRequestPart,
  Response.DocumentSourcePart,
  Response.UrlSourcePart,
  Response.ResponseMetadataPart,
  Response.FinishPart,
  Response.ErrorPart,
]);
type StandardPart = typeof StandardPart.Type;

const encodePartError = (error: Schema.SchemaError): AiError.AiError =>
  new AiError.AiError({
    module: 'Agent',
    method: 'encodePart',
    reason: AiError.InvalidOutputError.fromSchemaError(error),
  });

const encodeStandardPart = (
  part: StandardPart,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> =>
  Schema.encodeEffect(StandardPart)(part).pipe(
    Effect.mapError(encodePartError),
  );

const encodeToolCall = <PartTools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<PartTools>,
  part: Extract<ModelTurnPart<PartTools>, { readonly type: 'tool-call' }>,
): Effect.Effect<
  Response.StreamPartEncoded,
  AiError.AiError,
  Tool.ResultEncodingServices<PartTools[keyof PartTools]>
> =>
  Schema.encodeEffect(modelTurnPartSchema(toolkit))(part).pipe(
    Effect.mapError(encodePartError),
  );

const encodeToolCallError = <PartTools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<PartTools>,
  part: Extract<ModelTurnPart<PartTools>, { readonly type: 'tool-call-error' }>,
): Effect.Effect<
  Response.StreamPartEncoded,
  AiError.AiError,
  Tool.ResultEncodingServices<PartTools[keyof PartTools]>
> =>
  // The part is only admitted by a schema built for an
  // `invalidToolCalls: 'return'` operation, so it is encoded through the same
  // stream part schema the model turn decodes with.
  Schema.encodeEffect(modelTurnPartSchema(toolkit))(part).pipe(
    Effect.mapError(encodePartError),
  );

const encodeToolResult = <PartTools extends Record<string, Tool.Any>>(
  part: EncodableToolResult<PartTools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> =>
  // The decoded result can be a substituted value that deliberately does not
  // satisfy the tool schema. `encodedResult` is already the exact
  // provider-facing value in this case.
  Effect.succeed({
    type: 'tool-result',
    id: part.id,
    name: part.name,
    result: part.encodedResult,
    isFailure: part.isFailure,
    providerExecuted: part.providerExecuted,
    preliminary: part.preliminary,
  });

const encodeFile = <PartTools extends Record<string, Tool.Any>>(
  part: EncodableFile<PartTools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> =>
  Schema.encodeEffect(Response.FilePart)(part).pipe(
    Effect.mapError(encodePartError),
  );

const assertPartEncodingStrategy = (part: never): never => {
  throw new Error(`Unhandled response part encoding strategy: ${String(part)}`);
};

/** Preserve a model part in its provider-facing form for persistence. */
export const encodePart = <PartTools extends Record<string, Tool.Any>>(
  toolkit: Toolkit.WithHandler<PartTools>,
  part: ModelTurnPart<PartTools>,
): Effect.Effect<
  Response.StreamPartEncoded,
  AiError.AiError,
  Tool.ResultEncodingServices<PartTools[keyof PartTools]>
> => {
  switch (part.type) {
    case 'tool-call':
      return encodeToolCall(toolkit, part);
    case 'tool-call-error':
      return encodeToolCallError(toolkit, part);
    case 'tool-result':
      return encodeToolResult(part);
    case 'file':
      return encodeFile(part);
    case 'text-start':
    case 'text-delta':
    case 'text-end':
    case 'reasoning-start':
    case 'reasoning-delta':
    case 'reasoning-end':
    case 'tool-params-start':
    case 'tool-params-delta':
    case 'tool-params-end':
    case 'tool-approval-request':
    case 'source':
    case 'response-metadata':
    case 'finish':
    case 'error':
      return encodeStandardPart(part);
    default:
      return assertPartEncodingStrategy(part);
  }
};
