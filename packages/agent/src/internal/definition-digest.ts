import { createHash } from 'node:crypto';
import { LogVocabulary } from '@sunfall/vesper-log/vocabulary';
import { Schema } from 'effect';
import { Tool } from 'effect/unstable/ai';

import type { CodeMode } from '../code-mode.js';

// The compile-time compatibility digest: a canonical SHA-256 over the parts
// of a compiled `Agent.Definition` that change what durable history *means*
// — tool wire shape, delegation, skills, and the two toggles
// (`codeMode`/`resultOverflow`) that change what gets *recorded*. `agent.ts`
// computes this once in `make` and carries it beside `revision`; comparing
// it on open is what catches a forgotten revision bump after one of these
// changed. See `docs/conversations.md`'s "Compatibility and revisions" and
// this package's README.
//
// Deliberately not routed through Effect's `Crypto` service the way
// `@sunfall/vesper-log` and `@sunfall/vesper-attachments` hash bytes: this is
// a pure function of a compiled definition, not a persisted content address,
// and `Agent.make` is a synchronous compile step with nowhere to run an
// Effect. `node:crypto`'s synchronous `createHash` is the platform primitive
// both Bun and Node provide for that. `@sunfall/vesper-mcp`'s tool
// fingerprint (`packages/mcp/src/mcp.ts`) reasons about the identical
// trade-off for `crypto.subtle` — not routed through `Crypto` either, for the
// same "not a persisted content address" reason — but `subtle.digest` has no
// synchronous form, so it cannot run inside `Agent.make` directly. The
// canonicalization approach below is lifted from that fingerprint rather
// than reimplemented from scratch; it is duplicated rather than imported
// because `@sunfall/vesper-mcp` depends on `@sunfall/vesper-agent`, not the
// reverse (`docs/contributing.md`'s layering rule).
//
// What is deliberately excluded, and why:
//   - `instructions`: an application may build it per run (see the agent
//     guide's compaction section), so it is not durable identity.
//   - Model choice and `runPolicy`: both are run-time/application wiring,
//     not durable definition shape — `docs/conversations.md` already
//     excludes them from `revision`'s contract, and the digest follows it.
//   - Tool descriptions: documentation shown to the model, not the wire
//     shape a resumed run must decode.
//   - `resultOverflow.preview`: changes what head preview the model sees,
//     not the pointer shape a resumed run decodes back — see the agent
//     guide's "Tool-result overflow" section.

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

/** One subagent's contribution to its parent's digest. */
export interface SubagentDigestInput {
  readonly name: string;
  readonly digest: LogVocabulary.AgentDefinitionDigest;
}

/** One skill's contribution to its agent's digest: the catalog name only. */
export interface SkillDigestInput {
  readonly name: string;
}

/** Everything `Agent.make` has compiled that durable compatibility depends on. */
export interface DefinitionDigestInput {
  readonly name: string;
  readonly tools: Readonly<Record<string, Tool.Any>>;
  readonly subagents: ReadonlyArray<SubagentDigestInput>;
  readonly skills: ReadonlyArray<SkillDigestInput>;
  readonly codeMode: CodeMode.Option<Record<string, Tool.Any>> | undefined;
  /** `resultOverflow.threshold`, or `undefined` when overflow is unset. */
  readonly resultOverflowThreshold: number | undefined;
}

const compareStrings = (left: string, right: string): number =>
  left < right ? -1 : left > right ? 1 : 0;

/**
 * A copy, sorted. Lifted from `@sunfall/vesper-mcp`'s tool fingerprint
 * `sorted` helper (`packages/mcp/src/mcp.ts`) rather than `Array#toSorted`,
 * which needs ES2023 and this workspace targets ES2022.
 */
const sorted = <T>(
  values: ReadonlyArray<T>,
  compare: (left: T, right: T) => number,
): T[] => {
  const result = [...values];
  result.sort(compare);
  return result;
};

const normalizeCodeMode = (
  codeMode: CodeMode.Option<Record<string, Tool.Any>> | undefined,
): unknown =>
  codeMode === undefined || codeMode === false
    ? false
    : codeMode === true
      ? true
      : { except: sorted(codeMode.except, compareStrings) };

const toolSurface = (name: string, tool: Tool.Any) => ({
  name,
  parameters: Tool.getJsonSchema(tool),
  success: Tool.getJsonSchemaFromSchema(tool.successSchema),
  failure: Tool.getJsonSchemaFromSchema(tool.failureSchema),
});

/** Compute the compatibility digest for one compiled agent definition. */
export const compute = (
  input: DefinitionDigestInput,
): LogVocabulary.AgentDefinitionDigest => {
  const structure = {
    name: input.name,
    tools: sorted(Object.entries(input.tools), ([left], [right]) =>
      compareStrings(left, right),
    ).map(([name, tool]) => toolSurface(name, tool)),
    subagents: sorted(input.subagents, (left, right) =>
      compareStrings(left.name, right.name),
    ).map((child) => ({ name: child.name, digest: child.digest })),
    skills: sorted(
      input.skills.map((skill) => skill.name),
      compareStrings,
    ),
    codeMode: normalizeCodeMode(input.codeMode),
    resultOverflowThreshold: input.resultOverflowThreshold ?? null,
  };
  const canonical = canonicalJson(structure) ?? {};
  const material = encodeJson(canonical);
  const hash = createHash('sha256').update(material, 'utf8').digest('hex');
  return LogVocabulary.AgentDefinitionDigest.make(hash);
};

/**
 * Recursively sort object keys and drop `undefined`, so the same definition
 * always canonicalizes to the same bytes regardless of construction order.
 * Lifted from `@sunfall/vesper-mcp`'s tool fingerprint canonicalization; see
 * the module doc comment above for why it is duplicated rather than shared.
 */
const canonicalJson = (value: unknown): Schema.Json | undefined => {
  if (value === undefined) {
    return undefined;
  }
  if (Array.isArray(value)) {
    return value.flatMap((item) => {
      const canonical = canonicalJson(item);
      return canonical === undefined ? [] : [canonical];
    });
  }
  if (typeof value !== 'object' || value === null) {
    return isJson(value) ? value : undefined;
  }
  const entries = sorted(Object.entries(value), ([left], [right]) =>
    compareStrings(left, right),
  ).flatMap(([name, item]): ReadonlyArray<readonly [string, Schema.Json]> => {
    const canonical = canonicalJson(item);
    return canonical === undefined ? [] : [[name, canonical]];
  });
  return Object.fromEntries(entries);
};

const isJson = Schema.is(Schema.Json);
