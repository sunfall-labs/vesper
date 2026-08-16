import { Effect } from 'effect';
import { Tool } from 'effect/unstable/ai';
import { describe, expect, it } from 'vitest';

import { catalog, loader, TOOL_NAME, type Skill } from '../src/skill.js';

const skills: Skill[] = [
  {
    name: 'refunds',
    description: 'How to process a refund request.',
    instructions: 'STEP 1: verify the order. STEP 2: check the window.',
  },
  {
    name: 'escalation',
    description: 'When and how to escalate to a human.',
    instructions: 'Page the on-call owner in #support-escalations.',
  },
];

describe('catalog', () => {
  // The catalog goes in the cacheable prompt prefix; the instructions do not.
  // A model cannot ask for a skill it does not know exists, so names and
  // one-liners must be visible while the bodies stay out.
  it('lists names and descriptions but never instructions', () => {
    const text = catalog(skills);

    expect(text).toContain('refunds');
    expect(text).toContain('How to process a refund request.');
    expect(text).not.toContain('STEP 1');
  });

  it('is empty when there are no skills, so the prefix stays unchanged', () => {
    expect(catalog([])).toBe('');
  });
});

describe('loader', () => {
  it('advertises the load tool under a stable name', () => {
    const { tool } = loader(skills);

    expect(tool.name).toBe(TOOL_NAME);
  });

  // Constraining the parameter to the known set means a hallucinated skill
  // name fails validation and comes back to the model as a usable error,
  // rather than reaching the handler and returning nothing.
  it('constrains the skill name to the known set', () => {
    const { tool } = loader(skills);
    const schema = JSON.stringify(Tool.getJsonSchema(tool));

    expect(schema).toContain('refunds');
    expect(schema).toContain('escalation');
  });

  it('returns full instructions only when loaded', async () => {
    // The same function the toolkit layer wires, so this asserts on the
    // shipped behaviour rather than a copy of it.
    const { handler } = loader(skills);
    const handled = await Effect.runPromise(handler({ name: 'refunds' }));

    expect(handled).toEqual({
      instructions: 'STEP 1: verify the order. STEP 2: check the window.',
    });
  });

  it('fails with the unknown name rather than returning nothing', async () => {
    const { handler } = loader(skills);
    const outcome = await Effect.runPromise(
      handler({ name: 'nope' }).pipe(Effect.result),
    );

    expect(outcome._tag).toBe('Failure');
    if (outcome._tag === 'Failure') {
      expect(outcome.failure).toEqual({ unknownSkill: 'nope' });
    }
  });
});
