import { describe, expect, it } from 'vitest';

import { WorkspacePath } from '../src/path.js';

describe('normalize', () => {
  it('collapses the segments POSIX collapses', () => {
    expect(WorkspacePath.normalize('/a//b/./c')).toBe('/a/b/c');
    expect(WorkspacePath.normalize('/a/b/../c')).toBe('/a/c');
    expect(WorkspacePath.normalize('/a/b/')).toBe('/a/b');
    expect(WorkspacePath.normalize('/')).toBe('/');
    expect(WorkspacePath.normalize('')).toBe('.');
  });

  it('drops `..` at the top of an absolute path, as POSIX does', () => {
    expect(WorkspacePath.normalize('/../..')).toBe('/');
    expect(WorkspacePath.normalize('/../etc')).toBe('/etc');
  });

  it('keeps a leading `..` on a relative path, so it can still escape', () => {
    // If this collapsed silently, `resolve` would accept `../secrets`.
    expect(WorkspacePath.normalize('../secrets')).toBe('../secrets');
    expect(WorkspacePath.normalize('a/../../b')).toBe('../b');
  });
});

describe('contains', () => {
  it('accepts the root itself and anything under it', () => {
    expect(WorkspacePath.contains('/work', '/work')).toBe(true);
    expect(WorkspacePath.contains('/work', '/work/a/b')).toBe(true);
  });

  it('rejects a sibling whose name starts with the root', () => {
    expect(WorkspacePath.contains('/work', '/work-backup/a')).toBe(false);
    expect(WorkspacePath.contains('/work', '/works')).toBe(false);
  });

  it('handles `/` as a root without doubling the separator', () => {
    expect(WorkspacePath.contains('/', '/anything')).toBe(true);
  });
});

describe('resolve', () => {
  it('takes a relative path as relative to the root', () => {
    expect(WorkspacePath.resolve('/work', 'src/a.ts')).toEqual({
      ok: true,
      path: '/work/src/a.ts',
    });
    expect(WorkspacePath.resolve('/work', '.')).toEqual({
      ok: true,
      path: '/work',
    });
  });

  it('accepts an absolute path that is already inside', () => {
    expect(WorkspacePath.resolve('/work', '/work/src/a.ts')).toEqual({
      ok: true,
      path: '/work/src/a.ts',
    });
  });

  it('rejects a traversal out of the root', () => {
    expect(WorkspacePath.resolve('/work', '../secrets')).toEqual({
      ok: false,
      reason: 'escapes-root',
    });
    expect(WorkspacePath.resolve('/work', 'a/../../secrets')).toEqual({
      ok: false,
      reason: 'escapes-root',
    });
  });

  it('rejects an absolute path outside the root', () => {
    expect(WorkspacePath.resolve('/work', '/etc/passwd')).toEqual({
      ok: false,
      reason: 'escapes-root',
    });
  });

  it('rejects a sibling directory that shares the root prefix', () => {
    expect(WorkspacePath.resolve('/work', '/work-backup/a')).toEqual({
      ok: false,
      reason: 'escapes-root',
    });
  });

  it('rejects a NUL byte, which `node:fs` would throw on rather than classify', () => {
    expect(WorkspacePath.resolve('/work', 'a\0b')).toEqual({
      ok: false,
      reason: 'nul-byte',
    });
  });

  it('normalizes an unnormalized root before comparing', () => {
    expect(WorkspacePath.resolve('/work/', 'a')).toEqual({
      ok: true,
      path: '/work/a',
    });
  });
});

describe('relative', () => {
  it('reports a path back in the terms it was asked in', () => {
    expect(WorkspacePath.relative('/work', '/work/src/a.ts')).toBe('src/a.ts');
    expect(WorkspacePath.relative('/work', '/work')).toBe('.');
  });

  it('leaves a path that is not under the root alone', () => {
    expect(WorkspacePath.relative('/work', '/etc/passwd')).toBe('/etc/passwd');
  });
});
