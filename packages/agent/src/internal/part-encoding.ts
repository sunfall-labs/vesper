import { Effect, Schema } from 'effect';
import { AiError, Response, type Tool } from 'effect/unstable/ai';

type EncodableToolResult<PartTools extends Record<string, Tool.Any>> = Extract<
  Response.ModelStreamPart<PartTools>,
  { readonly type: 'tool-result' }
>;
type EncodableFile<PartTools extends Record<string, Tool.Any>> = Extract<
  Response.StreamPart<PartTools>,
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

const encodeToolCall = (
  part: Response.ToolCallPart<string, unknown>,
): Effect.Effect<Response.StreamPartEncoded> =>
  Effect.succeed({
    type: 'tool-call',
    id: part.id,
    name: part.name,
    params: part.params,
    providerExecuted: part.providerExecuted,
  });

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
  part: Response.ModelStreamPart<PartTools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> => {
  switch (part.type) {
    case 'tool-call':
      return encodeToolCall(part);
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
