import { readFile } from 'node:fs/promises';

import { describe, expect, it } from '@effect/vitest';

const isRecord = (value: unknown): value is Readonly<Record<string, unknown>> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

describe('approval CLI wrapper', () => {
  it('does not nest a task runner around the interactive terminal', async () => {
    const parsed: unknown = JSON.parse(
      await readFile(new URL('../../../package.json', import.meta.url), 'utf8'),
    );
    const scripts =
      isRecord(parsed) && isRecord(parsed['scripts'])
        ? parsed['scripts']
        : undefined;
    const command =
      isRecord(scripts) && typeof scripts['example:approval-cli'] === 'string'
        ? scripts['example:approval-cli']
        : undefined;

    expect(command).toBe(
      'node --experimental-strip-types examples/approval-cli/src/main.ts',
    );
    expect(command).not.toContain('nub run');
  });
});
