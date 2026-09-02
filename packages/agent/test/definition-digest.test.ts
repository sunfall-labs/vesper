import { describe, expect, it } from '@effect/vitest';
import { Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

import { Agent } from '../src/agent.js';

// The compile-time compatibility digest (`Agent.make(...).digest`), pure
// and synchronous, so these run without a provider or a log store. See
// `src/internal/definition-digest.ts` for what it covers and why.

const baseTool = () =>
  Tool.make('echo', {
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.Struct({ text: Schema.String }),
  });

const agentWithTool = (
  tool: Tool.Any,
  options?: { readonly instructions?: string; readonly revision?: string },
) =>
  Agent.make({
    name: 'digest-test',
    revision: options?.revision ?? '1',
    instructions: options?.instructions ?? 'be terse',
    toolkit: Toolkit.make(tool),
  });

describe('Agent.digest', () => {
  it('is stable across identical definitions', () => {
    const first = agentWithTool(baseTool());
    const second = agentWithTool(baseTool());

    expect(first.digest).toBe(second.digest);
  });

  it('is stable across independently constructed but identical tool schemas', () => {
    const rebuilt = Tool.make('echo', {
      parameters: Schema.Struct({ text: Schema.String }),
      success: Schema.Struct({ text: Schema.String }),
    });

    expect(agentWithTool(baseTool()).digest).toBe(
      agentWithTool(rebuilt).digest,
    );
  });

  it('changes when a tool parameter schema changes', () => {
    const withNumberField = Tool.make('echo', {
      parameters: Schema.Struct({ text: Schema.String, count: Schema.Finite }),
      success: Schema.Struct({ text: Schema.String }),
    });

    expect(agentWithTool(baseTool()).digest).not.toBe(
      agentWithTool(withNumberField).digest,
    );
  });

  it('changes when a tool failure schema changes', () => {
    const withFailure = Tool.make('echo', {
      parameters: Schema.Struct({ text: Schema.String }),
      success: Schema.Struct({ text: Schema.String }),
      failure: Schema.Struct({ reason: Schema.String }),
    });

    expect(agentWithTool(baseTool()).digest).not.toBe(
      agentWithTool(withFailure).digest,
    );
  });

  it('changes when a tool name changes', () => {
    const renamed = Tool.make('echo2', {
      parameters: Schema.Struct({ text: Schema.String }),
      success: Schema.Struct({ text: Schema.String }),
    });

    expect(agentWithTool(baseTool()).digest).not.toBe(
      agentWithTool(renamed).digest,
    );
  });

  it('does not change when instructions change', () => {
    const terse = agentWithTool(baseTool(), { instructions: 'be terse' });
    const verbose = agentWithTool(baseTool(), {
      instructions: 'be thorough and explain every step in detail',
    });

    expect(terse.digest).toBe(verbose.digest);
  });

  it('does not change when only revision changes', () => {
    const one = agentWithTool(baseTool(), { revision: '1' });
    const two = agentWithTool(baseTool(), { revision: '2' });

    expect(one.digest).toBe(two.digest);
  });

  it('changes when a subagent is added', () => {
    const child = Agent.make({
      name: 'child',
      revision: '1',
      instructions: 'help',
      toolkit: Toolkit.make(),
    });
    const withoutChild = agentWithTool(baseTool());
    const withChild = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(baseTool()),
      subagents: [child],
    });

    expect(withoutChild.digest).not.toBe(withChild.digest);
  });

  it("changes when a subagent's own digest changes, name unchanged", () => {
    const childV1 = Agent.make({
      name: 'child',
      revision: '1',
      instructions: 'help',
      toolkit: Toolkit.make(baseTool()),
    });
    const withNumberField = Tool.make('echo', {
      parameters: Schema.Struct({ text: Schema.String, count: Schema.Finite }),
      success: Schema.Struct({ text: Schema.String }),
    });
    const childV1Different = Agent.make({
      name: 'child',
      revision: '1',
      instructions: 'help',
      toolkit: Toolkit.make(withNumberField),
    });

    const parentWithV1 = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(),
      subagents: [childV1],
    });
    const parentWithV1Different = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(),
      subagents: [childV1Different],
    });

    expect(parentWithV1.digest).not.toBe(parentWithV1Different.digest);
  });

  it('changes when the skill catalog changes', () => {
    const withoutSkill = agentWithTool(baseTool());
    const withSkill = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(baseTool()),
      skills: [
        { name: 'formatting', description: 'format things', instructions: 'x' },
      ],
    });

    expect(withoutSkill.digest).not.toBe(withSkill.digest);
  });

  it('changes when resultOverflow.threshold changes', () => {
    const small = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(baseTool()),
      resultOverflow: { threshold: 100 },
    });
    const large = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(baseTool()),
      resultOverflow: { threshold: 10_000 },
    });

    expect(small.digest).not.toBe(large.digest);
  });

  it('does not change when only resultOverflow.preview changes', () => {
    const short = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(baseTool()),
      resultOverflow: { threshold: 100, preview: 10 },
    });
    const long = Agent.make({
      name: 'digest-test',
      revision: '1',
      instructions: 'be terse',
      toolkit: Toolkit.make(baseTool()),
      resultOverflow: { threshold: 100, preview: 400 },
    });

    expect(short.digest).toBe(long.digest);
  });
});
