// Making bytes fit a context window without lying about what was dropped.
//
// Two jobs, both of which a tool gets wrong quietly if nobody writes them
// down: deciding whether a file is text at all, and cutting text that is too
// large while reporting the cut. A tool that silently returns the first 50KB
// of a 5MB file is worse than one that refuses, because the model reads a
// prefix as the whole and reasons from it.
//
// Pure functions over strings and bytes. Nothing here touches a driver, a
// filesystem, or Effect, so every case below is exercised directly.
//
// Two independent limits,
// whichever is hit first wins, and complete lines wherever possible — but the
// code is ours. See `tools.ts` for why we did not import theirs.

/** Line budget applied when a caller does not pick one. */
export const DEFAULT_MAX_LINES = 2000;

/** Byte budget applied when a caller does not pick one. */
export const DEFAULT_MAX_BYTES = 50 * 1024;

/**
 * How much of a file is inspected when deciding whether it is text.
 *
 * A prefix rather than the whole file, because the decision is a heuristic
 * either way and scanning gigabytes to reach the same answer is waste. It is
 * the same rule `git` applies, and it has the same blind spot: a file that is
 * clean text for its first 8KB and binary afterwards reads as text.
 */
export const BINARY_SNIFF_BYTES = 8000;

export interface Limits {
  /** Maximum number of lines to keep. Defaults to {@link DEFAULT_MAX_LINES}. */
  readonly maxLines?: number | undefined;
  /** Maximum UTF-8 bytes to keep. Defaults to {@link DEFAULT_MAX_BYTES}. */
  readonly maxBytes?: number | undefined;
}

export interface Truncated {
  /** What survived the cut. */
  readonly content: string;
  readonly truncated: boolean;
  /** Which budget ran out first, or `null` when nothing was cut. */
  readonly truncatedBy: 'lines' | 'bytes' | null;
  readonly totalLines: number;
  readonly totalBytes: number;
  readonly outputLines: number;
  readonly outputBytes: number;
  /**
   * Whether the edge line of the output is a fragment rather than a whole
   * line. Only {@link tail} can produce one, and only when a single line is
   * itself larger than the byte budget.
   */
  readonly partialLine: boolean;
}

/**
 * The UTF-8 length of a string, without building the bytes.
 *
 * `TextEncoder` would allocate a copy of every file just to measure it, and
 * this runs once per line. Unpaired surrogates are counted as the three bytes
 * their replacement character costs, which is what an encoder would emit.
 */
export const utf8Size = (text: string): number => {
  let bytes = 0;
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    if (code <= 0x7f) {
      bytes += 1;
    } else if (code <= 0x7ff) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff && index + 1 < text.length) {
      const next = text.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }
  return bytes;
};

const untouched = (
  content: string,
  totalLines: number,
  totalBytes: number,
  maxLines: number,
  maxBytes: number,
): Truncated | undefined =>
  totalLines <= maxLines && totalBytes <= maxBytes
    ? {
        content,
        truncated: false,
        truncatedBy: null,
        totalLines,
        totalBytes,
        outputLines: totalLines,
        outputBytes: totalBytes,
        partialLine: false,
      }
    : undefined;

/**
 * Keep the beginning of the content.
 *
 * What a file read wants: a file is read top-down, and the first lines are the
 * ones that say what it is.
 *
 * Only whole lines come back. When even the first line is over budget the
 * content is empty and `truncatedBy` is `'bytes'` — an honest nothing, rather
 * than a fragment the model would read as a line.
 */
export const head = (content: string, limits?: Limits): Truncated => {
  const maxLines = limits?.maxLines ?? DEFAULT_MAX_LINES;
  const maxBytes = limits?.maxBytes ?? DEFAULT_MAX_BYTES;
  const totalBytes = utf8Size(content);
  const lines = content.split('\n');
  const totalLines = lines.length;

  const whole = untouched(content, totalLines, totalBytes, maxLines, maxBytes);
  if (whole !== undefined) {
    return whole;
  }

  const kept: Array<string> = [];
  let keptBytes = 0;
  let truncatedBy: 'lines' | 'bytes' = 'lines';

  for (let index = 0; index < lines.length && index < maxLines; index += 1) {
    // `lines[index]` is in range by the loop guard; the index signature is
    // what makes TypeScript hand back `string | undefined` here.
    const line = lines[index] ?? '';
    const cost = utf8Size(line) + (index > 0 ? 1 : 0);
    if (keptBytes + cost > maxBytes) {
      truncatedBy = 'bytes';
      break;
    }
    kept.push(line);
    keptBytes += cost;
  }

  const output = kept.join('\n');
  return {
    content: output,
    truncated: true,
    truncatedBy,
    totalLines,
    totalBytes,
    outputLines: kept.length,
    outputBytes: utf8Size(output),
    partialLine: false,
  };
};

/**
 * Cut a single line down to a byte budget, keeping its end.
 *
 * Cuts on code points, never inside one — slicing UTF-16 code units would
 * split a surrogate pair and produce a replacement character that was not in
 * the file.
 */
const lastBytesOf = (line: string, maxBytes: number): string => {
  const points = Array.from(line);
  let bytes = 0;
  let start = points.length;
  for (let index = points.length - 1; index >= 0; index -= 1) {
    const cost = utf8Size(points[index] ?? '');
    if (bytes + cost > maxBytes) {
      break;
    }
    bytes += cost;
    start = index;
  }
  return points.slice(start).join('');
};

/**
 * Keep the end of the content.
 *
 * What command output wants: a build prints thousands of lines of progress and
 * then the error, and the error is the only part worth spending context on.
 *
 * Unlike {@link head} this will return a fragment, but only in the one case
 * where the alternative is nothing at all — a single line longer than the byte
 * budget, which is what a minified file or a JSON dump on one line looks like.
 * `partialLine` says so.
 */
export const tail = (content: string, limits?: Limits): Truncated => {
  const maxLines = limits?.maxLines ?? DEFAULT_MAX_LINES;
  const maxBytes = limits?.maxBytes ?? DEFAULT_MAX_BYTES;
  const totalBytes = utf8Size(content);
  const lines = content.split('\n');
  const totalLines = lines.length;

  const whole = untouched(content, totalLines, totalBytes, maxLines, maxBytes);
  if (whole !== undefined) {
    return whole;
  }

  const kept: Array<string> = [];
  let keptBytes = 0;
  let truncatedBy: 'lines' | 'bytes' = 'lines';
  let partialLine = false;

  for (
    let index = lines.length - 1;
    index >= 0 && kept.length < maxLines;
    index -= 1
  ) {
    const line = lines[index] ?? '';
    const cost = utf8Size(line) + (kept.length > 0 ? 1 : 0);
    if (keptBytes + cost > maxBytes) {
      truncatedBy = 'bytes';
      if (kept.length === 0) {
        const fragment = lastBytesOf(line, maxBytes);
        kept.unshift(fragment);
        keptBytes = utf8Size(fragment);
        partialLine = true;
      }
      break;
    }
    kept.unshift(line);
    keptBytes += cost;
  }

  const output = kept.join('\n');
  return {
    content: output,
    truncated: true,
    truncatedBy,
    totalLines,
    totalBytes,
    outputLines: kept.length,
    outputBytes: utf8Size(output),
    partialLine,
  };
};

export type Decoded =
  | { readonly ok: true; readonly text: string }
  | { readonly ok: false; readonly reason: 'nul-byte' | 'invalid-utf8' };

/**
 * Bytes to text, or a reason why not.
 *
 * Two checks, because either alone lets a class of binary through as
 * plausible-looking text:
 *
 * - **A NUL byte** in the leading {@link BINARY_SNIFF_BYTES}. Executables,
 *   images, and archives have them; text does not.
 * - **Strict UTF-8 decoding.** A latin-1 or UTF-16 file may carry no NUL at
 *   all, and a lenient decode would hand back a string full of `U+FFFD` that
 *   the model has no way to recognise as damage. Failing is the honest
 *   answer, and it is why this is not just `Buffer.toString('utf8')`.
 */
export const decodeText = (bytes: Uint8Array): Decoded => {
  const limit = Math.min(bytes.length, BINARY_SNIFF_BYTES);
  for (let index = 0; index < limit; index += 1) {
    if (bytes[index] === 0) {
      return { ok: false, reason: 'nul-byte' };
    }
  }

  try {
    return {
      ok: true,
      text: new TextDecoder('utf-8', { fatal: true }).decode(bytes),
    };
  } catch {
    return { ok: false, reason: 'invalid-utf8' };
  }
};

export * as WorkspaceOutput from './output.js';
