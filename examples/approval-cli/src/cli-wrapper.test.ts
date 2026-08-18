import { readFile } from 'node:fs/promises';

import { describe, expect, it } from '@effect/vitest';

describe('approval CLI wrapper', () => {
  it('does not nest a task runner around the interactive terminal', async () => {
    const rootPackage = JSON.parse(
      await readFile(new URL('../../../package.json', import.meta.url), 'utf8'),
    ) as { readonly scripts?: Readonly<Record<string, string>> };
    const command = rootPackage.scripts?.['example:approval-cli'];

    expect(command).toBe(
      'node --experimental-strip-types examples/approval-cli/src/main.ts',
    );
    expect(command).not.toContain('nub run');
  });
});
