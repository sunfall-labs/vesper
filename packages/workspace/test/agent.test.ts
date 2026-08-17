import { describe, expect, it } from '@effect/vitest';
import { Schema } from 'effect';
import { Tool, Toolkit } from 'effect/unstable/ai';

import { WorkspaceAgent } from '../src/agent.js';

const custom = Tool.make('lookup_issue', {
  description: 'Look up one issue.',
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ title: Schema.String }),
});

describe('WorkspaceAgent', () => {
  it('provides the six standard workspace tools explicitly', () => {
    expect(Object.keys(WorkspaceAgent.standard.toolkit.tools).sort()).toEqual([
      'edit_file',
      'list_files',
      'read_file',
      'run_shell',
      'search_files',
      'write_file',
    ]);
  });

  it('adds standard tools without losing application tools', () => {
    const adapter = WorkspaceAgent.compose(Toolkit.make(custom));
    expect(Object.keys(adapter.toolkit.tools).sort()).toEqual([
      'edit_file',
      'list_files',
      'lookup_issue',
      'read_file',
      'run_shell',
      'search_files',
      'write_file',
    ]);
  });

  it('rejects collisions from a widened toolkit at runtime', () => {
    const widenedTools: ReadonlyArray<Tool.Any> = Object.values(
      WorkspaceAgent.standard.toolkit.tools,
    );
    const widened = Toolkit.make(...widenedTools);
    // @ts-expect-error erased toolkits cannot preserve exact merged tool names
    expect(() => WorkspaceAgent.compose(widened)).toThrow(
      'application toolkit already defines it',
    );
  });

  it('rejects literal standard-tool collisions at compile time', () => {
    const readFile = WorkspaceAgent.standard.toolkit.tools.read_file;
    expect(() => {
      // @ts-expect-error standard tool names are reserved by the composition
      WorkspaceAgent.compose(Toolkit.make(readFile));
    }).toThrow('application toolkit already defines it');
  });
});
