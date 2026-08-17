import { describe, expect, it } from '@effect/vitest';

import { WorkspaceGlob } from '../src/glob.js';

describe('matches', () => {
  it('keeps `*` inside one segment', () => {
    expect(WorkspaceGlob.matches('*.ts', 'a.ts')).toBe(true);
    expect(WorkspaceGlob.matches('*.ts', 'src/a.ts')).toBe(false);
    expect(WorkspaceGlob.matches('src/*.ts', 'src/a.ts')).toBe(true);
    expect(WorkspaceGlob.matches('src/*.ts', 'src/deep/a.ts')).toBe(false);
  });

  it('lets `**/` span zero or more directories', () => {
    expect(WorkspaceGlob.matches('**/*.ts', 'a.ts')).toBe(true);
    expect(WorkspaceGlob.matches('**/*.ts', 'src/deep/a.ts')).toBe(true);
    expect(WorkspaceGlob.matches('src/**/*.ts', 'src/a.ts')).toBe(true);
    expect(WorkspaceGlob.matches('src/**/*.ts', 'src/deep/a.ts')).toBe(true);
    expect(WorkspaceGlob.matches('src/**/*.ts', 'other/a.ts')).toBe(false);
  });

  it('lets a trailing `**` cross separators', () => {
    expect(WorkspaceGlob.matches('src/**', 'src/a/b/c.ts')).toBe(true);
    expect(WorkspaceGlob.matches('src/**', 'other/a.ts')).toBe(false);
  });

  it('matches exactly one non-separator with `?`', () => {
    expect(WorkspaceGlob.matches('a?.ts', 'ab.ts')).toBe(true);
    expect(WorkspaceGlob.matches('a?.ts', 'abc.ts')).toBe(false);
    expect(WorkspaceGlob.matches('a?b', 'a/b')).toBe(false);
  });

  it('supports character classes, ranges, and negation', () => {
    expect(WorkspaceGlob.matches('a[bc].ts', 'ab.ts')).toBe(true);
    expect(WorkspaceGlob.matches('a[bc].ts', 'ad.ts')).toBe(false);
    expect(WorkspaceGlob.matches('a[a-z].ts', 'aq.ts')).toBe(true);
    expect(WorkspaceGlob.matches('a[!bc].ts', 'ad.ts')).toBe(true);
    expect(WorkspaceGlob.matches('a[!bc].ts', 'ab.ts')).toBe(false);
  });

  it('anchors both ends, so a prefix is not a match', () => {
    expect(WorkspaceGlob.matches('*.ts', 'a.tsx')).toBe(false);
    expect(WorkspaceGlob.matches('a.ts', 'xa.ts')).toBe(false);
  });

  it('treats regular-expression metacharacters as literal text', () => {
    expect(WorkspaceGlob.matches('a.ts', 'axts')).toBe(false);
    expect(WorkspaceGlob.matches('a+b.txt', 'a+b.txt')).toBe(true);
    expect(WorkspaceGlob.matches('(x).txt', '(x).txt')).toBe(true);
  });

  it('treats braces as literal, which is the documented gap', () => {
    expect(WorkspaceGlob.matches('a.{ts,tsx}', 'a.ts')).toBe(false);
    expect(WorkspaceGlob.matches('a.{ts,tsx}', 'a.{ts,tsx}')).toBe(true);
  });

  it('treats an unterminated `[` as a literal bracket rather than failing', () => {
    expect(() => WorkspaceGlob.compile('a[bc')).not.toThrow();
    expect(WorkspaceGlob.matches('a[bc', 'a[bc')).toBe(true);
  });

  it('rejects a malformed character range during compilation', () => {
    expect(() => WorkspaceGlob.compile('[z-a]')).toThrow(
      WorkspaceGlob.InvalidGlobPattern,
    );
  });
});
