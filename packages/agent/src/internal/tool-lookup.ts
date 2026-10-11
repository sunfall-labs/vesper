import type { Tool } from 'effect/ai';

// The one rule for turning a model-written name into a tool.
//
// A toolkit's tools are a plain record, so a bracket lookup walks the
// prototype chain: a model that writes `toString`, `constructor`, or
// `__proto__` would get back a property nobody declared. Every place the
// agent resolves a name goes through here, so the rule cannot drift between
// them.

/** Whether `name` is one of the toolkit's own tools. */
export const isToolName = <Tools extends Record<string, Tool.Any>>(
  tools: Tools,
  name: string,
): name is Extract<keyof Tools, string> => Object.hasOwn(tools, name);

/** The tool `name` refers to, or `undefined` when it names none. */
export const toolNamed = <Tools extends Record<string, Tool.Any>>(
  tools: Tools,
  name: string,
): Tools[keyof Tools] | undefined =>
  isToolName(tools, name) ? tools[name] : undefined;
