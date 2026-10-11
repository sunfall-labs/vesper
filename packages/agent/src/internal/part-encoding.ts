import { Effect, Schema } from 'effect';
import { AiError, Response, type Tool, Toolkit } from 'effect/ai';

import type * as ToolResolution from './tool-resolution.js';

/**
 * A part as the agent loop sees it: the model's response, read with deferred
 * validation, and the results and approval requests the agent resolves for it.
 */
type ModelPart<PartTools extends Record<string, Tool.Any>> =
  | Response.StreamPart<PartTools, 'deferred'>
  | ToolResolution.Resolution<PartTools>;
type EncodableToolCall<PartTools extends Record<string, Tool.Any>> = Extract<
  ModelPart<PartTools>,
  { readonly type: 'tool-call' }
>;
type EncodableToolResult<PartTools extends Record<string, Tool.Any>> = Extract<
  ModelPart<PartTools>,
  { readonly type: 'tool-result' }
>;
// Every part that carries no tool-specific schema.
const StandardPart = Response.StreamPart(Toolkit.empty);
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
  part: EncodableToolCall<PartTools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> =>
  // Parameters are opaque: the model-authored value, which is already the
  // provider-facing form. Re-encoding through the tool schema would run a
  // transformation such as DateFromString backwards over a string.
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

const assertPartEncodingStrategy = (part: never): never => {
  throw new Error(`Unhandled response part encoding strategy: ${String(part)}`);
};

/** Preserve a model part in its provider-facing form for persistence. */
export const encodePart = <PartTools extends Record<string, Tool.Any>>(
  part: ModelPart<PartTools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> => {
  switch (part.type) {
    case 'tool-call':
      return encodeToolCall(part);
    case 'tool-result':
      return encodeToolResult(part);
    case 'file':
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
