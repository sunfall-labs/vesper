import type { Tool as PiTool } from '@earendil-works/pi-ai';
import { Tool } from 'effect/unstable/ai';

// Pi types `Tool.parameters` as a typebox `TSchema`; Effect derives JSON
// Schema from a `Schema.Constraint`. Neither library reads the other's brand
// — every Pi provider serializes `parameters` straight into the request body
// as JSON Schema — so the bridge is structural, not semantic.
//
// The cast below is the one place two schema libraries meet. It is
// deliberately isolated here rather than spread across call sites, and it is
// the only cast in this package.
//
// This also closes a failure mode worth naming, because it is easy to ship: a
// permissive placeholder schema on the advertised side means the model is told
// the tool takes no parameters, and nothing validates what comes back. Here the advertised schema and the parsed type
// derive from the same `Schema`, so they cannot drift.

export const toPiTool = (tool: Tool.Any): PiTool => ({
  name: tool.name,
  description: Tool.getDescription(tool) ?? '',
  parameters: Tool.getJsonSchema(tool) as PiTool['parameters'],
});

export const toPiTools = (
  tools: ReadonlyArray<Tool.Any>,
): PiTool[] | undefined =>
  tools.length === 0 ? undefined : tools.map(toPiTool);

export * as PiTools from './tools.js';
