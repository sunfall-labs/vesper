import { Effect, Schema } from 'effect';
import { AiError, Response, type Tool, type Toolkit } from 'effect/unstable/ai';

// The loop's persistence boundary: every decoded model part is re-encoded to
// the provider representation before it reaches observers or the durable
// recording sink. Private to the loop — `agent.ts` re-imports only the
// `ParameterEncodingServices` requirement term its public types name.

export type ParameterEncodingServices<
  EncodingTools extends Record<string, Tool.Any>,
> = EncodingTools[keyof EncodingTools] extends infer Candidate
  ? Candidate extends Tool.Any
    ? Tool.ParametersSchema<Candidate>['EncodingServices']
    : never
  : never;

type EncodableToolCall<PartTools extends Record<string, Tool.Any>> = Extract<
  Response.StreamPart<PartTools>,
  { readonly type: 'tool-call' }
>;
type EncodableToolResult<PartTools extends Record<string, Tool.Any>> = Extract<
  Response.StreamPart<PartTools>,
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

const encodeToolCall = <PartTools extends Record<string, Tool.Any>>(
  part: EncodableToolCall<PartTools>,
  toolkit: Toolkit.WithHandler<PartTools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> => {
  const tool = Object.hasOwn(toolkit.tools, part.name)
    ? toolkit.tools[part.name]
    : undefined;
  if (tool === undefined) {
    return Effect.fail(
      new AiError.AiError({
        module: 'Agent',
        method: 'encodePart',
        reason: new AiError.InvalidOutputError({
          description: `Model emitted unknown tool ${part.name}`,
        }),
      }),
    );
  }
  // The toolkit lookup erases the name-to-schema relationship. The runtime
  // schema is still the one that produced `part.params`; this assertion only
  // restores its encoding-service requirement for the generic helper, just as
  // dispatch restores the handler relationship.
  const encoded = Schema.encodeEffect(tool.parametersSchema)(
    part.params,
  ) as Effect.Effect<
    unknown,
    Schema.SchemaError,
    ParameterEncodingServices<PartTools>
  >;
  return encoded.pipe(
    Effect.mapError(encodePartError),
    Effect.map((params) => ({
      type: 'tool-call',
      id: part.id,
      name: part.name,
      params,
      providerExecuted: part.providerExecuted,
    })),
  );
};

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

/** Encode a decoded model part before it reaches observers or persistence. */
export const encodePart = <PartTools extends Record<string, Tool.Any>>(
  part: Response.StreamPart<PartTools>,
  toolkit: Toolkit.WithHandler<PartTools>,
): Effect.Effect<Response.StreamPartEncoded, AiError.AiError> => {
  switch (part.type) {
    case 'tool-call':
      return encodeToolCall(part, toolkit);
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
