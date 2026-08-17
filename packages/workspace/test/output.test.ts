import { describe, expect, it } from '@effect/vitest';

import { WorkspaceOutput } from '../src/output.js';

// Pure logic, exercised directly. Every case here is one a tool would
// otherwise get wrong quietly.

describe('utf8Size', () => {
  it('counts bytes, not UTF-16 code units', () => {
    expect(WorkspaceOutput.utf8Size('abc')).toBe(3);
    // 2 bytes, 3 bytes, 4 bytes (one astral character, two code units).
    expect(WorkspaceOutput.utf8Size('é')).toBe(2);
    expect(WorkspaceOutput.utf8Size('€')).toBe(3);
    expect(WorkspaceOutput.utf8Size('😀')).toBe(4);
    expect('😀'.length).toBe(2);
  });

  it('agrees with an encoder on mixed content', () => {
    const text = 'a é € 😀 \n tail';
    expect(WorkspaceOutput.utf8Size(text)).toBe(
      new TextEncoder().encode(text).byteLength,
    );
  });

  it('counts a lone surrogate as the replacement it would encode to', () => {
    expect(WorkspaceOutput.utf8Size('\ud800')).toBe(3);
  });
});

describe('head', () => {
  it('leaves content that fits untouched', () => {
    const result = WorkspaceOutput.head('one\ntwo\nthree');
    expect(result).toMatchObject({
      content: 'one\ntwo\nthree',
      truncated: false,
      truncatedBy: null,
      totalLines: 3,
      outputLines: 3,
      partialLine: false,
    });
  });

  it('cuts at the line budget and says so', () => {
    const result = WorkspaceOutput.head('a\nb\nc\nd', { maxLines: 2 });
    expect(result.content).toBe('a\nb');
    expect(result.truncated).toBe(true);
    expect(result.truncatedBy).toBe('lines');
    expect(result.totalLines).toBe(4);
    expect(result.outputLines).toBe(2);
  });

  it('cuts at the byte budget and says so', () => {
    const result = WorkspaceOutput.head('aaaa\nbbbb\ncccc', {
      maxLines: 100,
      maxBytes: 6,
    });
    expect(result.content).toBe('aaaa');
    expect(result.truncatedBy).toBe('bytes');
    expect(result.totalBytes).toBe(14);
  });

  it('returns nothing rather than a fragment when the first line is over budget', () => {
    const result = WorkspaceOutput.head('x'.repeat(100), { maxBytes: 10 });
    expect(result.content).toBe('');
    expect(result.outputLines).toBe(0);
    expect(result.truncatedBy).toBe('bytes');
    expect(result.partialLine).toBe(false);
  });

  it('measures the budget in bytes, not characters', () => {
    // Four 4-byte characters. A character-counting implementation would keep
    // the line; a byte-counting one cannot.
    const result = WorkspaceOutput.head('😀😀😀😀\nnext', { maxBytes: 10 });
    expect(result.content).toBe('');
    expect(result.truncatedBy).toBe('bytes');
  });
});

describe('tail', () => {
  it('keeps the end, which is where a failing build says why', () => {
    const result = WorkspaceOutput.tail('a\nb\nc\nd', { maxLines: 2 });
    expect(result.content).toBe('c\nd');
    expect(result.truncatedBy).toBe('lines');
    expect(result.outputLines).toBe(2);
  });

  it('keeps the tail of a single over-budget line rather than nothing', () => {
    const result = WorkspaceOutput.tail(`${'a'.repeat(50)}END`, {
      maxBytes: 10,
    });
    expect(result.content).toBe('aaaaaaaEND');
    expect(result.partialLine).toBe(true);
    expect(result.truncatedBy).toBe('bytes');
  });

  it('cuts a fragment on a code point, never inside one', () => {
    // Three 4-byte characters, a 9-byte budget: only two whole ones fit.
    const result = WorkspaceOutput.tail('😀😀😀', { maxBytes: 9 });
    expect(result.content).toBe('😀😀');
    expect(result.partialLine).toBe(true);
    expect(result.content).not.toContain('�');
  });

  it('leaves content that fits untouched', () => {
    const result = WorkspaceOutput.tail('one\ntwo');
    expect(result.truncated).toBe(false);
    expect(result.content).toBe('one\ntwo');
  });
});

describe('decodeText', () => {
  it('decodes real UTF-8', () => {
    const bytes = new TextEncoder().encode('héllo 😀');
    expect(WorkspaceOutput.decodeText(bytes)).toEqual({
      ok: true,
      text: 'héllo 😀',
    });
  });

  it('refuses content with a NUL byte', () => {
    expect(
      WorkspaceOutput.decodeText(Uint8Array.from([0x68, 0x00, 0x69])),
    ).toEqual({ ok: false, reason: 'nul-byte' });
  });

  it('refuses bytes that are not UTF-8 even without a NUL', () => {
    // Latin-1 "café" — 0xe9 is not a valid UTF-8 lead byte here, and a
    // lenient decode would hand back "caf�" as if it were content.
    expect(
      WorkspaceOutput.decodeText(
        Uint8Array.from([0x63, 0x61, 0x66, 0xe9, 0x0a]),
      ),
    ).toEqual({ ok: false, reason: 'invalid-utf8' });
  });

  it('only sniffs the leading window for NUL bytes', () => {
    const bytes = new Uint8Array(WorkspaceOutput.BINARY_SNIFF_BYTES + 10);
    bytes.fill(0x61);
    bytes[WorkspaceOutput.BINARY_SNIFF_BYTES + 5] = 0;
    const decoded = WorkspaceOutput.decodeText(bytes);
    // Documented blind spot: a NUL past the window reads as text.
    expect(decoded.ok).toBe(true);
  });

  it('accepts an empty file', () => {
    expect(WorkspaceOutput.decodeText(new Uint8Array(0))).toEqual({
      ok: true,
      text: '',
    });
  });
});
