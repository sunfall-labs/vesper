import { Context, Effect, Option, Schema } from 'effect';
import type { Tool } from 'effect/unstable/ai';

/** Metadata persisted when a tool pauses for an external interaction. */
export const Metadata = Schema.Struct({
  name: Schema.NonEmptyString,
  mode: Schema.Literals(['dispatch', 'answer']),
});
export interface Metadata extends Schema.Struct.Type<typeof Metadata.fields> {}

/** Type-level marker for a tool whose result comes from an external answer. */
export const AnswerTypeId: unique symbol = Symbol.for(
  '@sunfall/vesper-agent/Interaction/Answer',
);
export interface Answer {
  readonly [AnswerTypeId]: typeof AnswerTypeId;
}
export type AnyAnswer = Tool.Any & Answer;
const answerMarker: Answer = { [AnswerTypeId]: AnswerTypeId };

/** Tool annotation understood by Vesper's durable dispatch seam. */
export class Annotation extends Context.Service<Annotation, Metadata>()(
  '@sunfall/vesper-agent/Interaction/Annotation',
) {}

export type ToolConfig = {
  readonly parameters: Schema.Constraint;
  readonly success: Schema.Constraint;
  readonly failure: Schema.Constraint;
  readonly failureMode: Tool.FailureMode;
};

const annotate = <Name extends string, C extends ToolConfig, R>(
  tool: Tool.Tool<Name, C, R>,
  metadata: Metadata,
): Tool.Tool<Name, C, R> => {
  if (metadata.name.length === 0) {
    throw new Error('Interaction name must not be empty');
  }
  return tool.setNeedsApproval(true).annotate(Annotation, metadata);
};

/**
 * Require authorization before dispatching a tool's ordinary handler.
 *
 * This is the explicit form of Effect AI's `needsApproval` flag. Tools using
 * that flag directly are the equivalent implicit interaction named
 * `approval`.
 */
export const approval = <Name extends string, C extends ToolConfig, R>(
  tool: Tool.Tool<Name, C, R>,
): Tool.Tool<Name, C, R> =>
  annotate(tool, { name: 'approval', mode: 'dispatch' });

/**
 * Make the externally supplied response the tool result.
 *
 * The real handler is never entered. Use {@link unreachable} as the Effect AI
 * handler; it only guards against a defect in the dispatch seam.
 */
export const answer = <Name extends string, C extends ToolConfig, R>(
  tool: Tool.Tool<Name, C, R>,
  options?: { readonly name?: string },
): Tool.Tool<Name, C, R> & Answer =>
  Object.assign(
    annotate(tool, { name: options?.name ?? tool.name, mode: 'answer' }),
    answerMarker,
  );

/** Read Vesper interaction metadata from an Effect AI tool. */
export const metadata = (tool: {
  readonly annotations: Context.Context<never>;
}): Option.Option<Metadata> => Context.getOption(tool.annotations, Annotation);

/** True when the tool is resolved by an external answer, not a handler. */
export const isAnswer = (tool: Tool.Any): boolean =>
  AnswerTypeId in tool &&
  Option.exists(metadata(tool), (value) => value.mode === 'answer');

/** Guard effect for an answer interaction; correct dispatch never runs it. */
export const unreachable: Effect.Effect<never> = Effect.die(
  new Error('Answer interaction reached its unreachable handler'),
);

export * as Interaction from './interaction.js';
