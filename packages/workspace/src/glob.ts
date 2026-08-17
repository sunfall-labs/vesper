// Glob patterns, compiled to regular expressions.
//
// Written here rather than taken from a library because the alternative is a
// dependency in a package whose whole dependency list is `effect`, and because
// a glob that runs against a *driver* cannot be one that reads the filesystem
// itself — every published glob does. Matching is separated from walking for
// exactly that reason: this file decides whether a path matches, `tools.ts`
// decides which paths exist, and only the latter needs a workspace.
//
// The supported syntax is stated rather than implied, because a pattern that
// silently means something else is how a model concludes a file is missing:
//
// | Pattern  | Matches                                              |
// | -------- | ---------------------------------------------------- |
// | `*`      | any run of characters within one segment              |
// | `**`     | any run of characters, crossing `/`                   |
// | `**/`    | zero or more whole directory segments                 |
// | `?`      | exactly one character, not `/`                        |
// | `[abc]`  | one character from the set; `[a-z]` ranges, `[!…]` negates |
//
// Brace alternation (`{ts,tsx}`) is **not** supported and a `{` is a literal
// brace. Two patterns cost one extra call; a half-working `{}` costs a wrong
// answer nobody checks.

const REGEXP_METACHARACTERS = /[.*+?^${}()|[\]\\]/;
const MAX_PATTERN_LENGTH = 4096;

export class InvalidGlobPattern extends Error {
  readonly name = 'InvalidGlobPattern';

  constructor(
    readonly pattern: string,
    message: string,
  ) {
    super(message);
  }
}

const escapeLiteral = (character: string): string =>
  REGEXP_METACHARACTERS.test(character) ? `\\${character}` : character;

/**
 * Read a `[...]` character class, or report that there is no closing bracket.
 *
 * An unterminated `[` is treated as a literal by the caller rather than as an
 * error: `grep '[TODO'` is a plausible thing to type, and failing the whole
 * call over it helps nobody.
 */
const readClass = (
  pattern: string,
  start: number,
): { readonly source: string; readonly next: number } | undefined => {
  let index = start + 1;
  let negated = false;

  if (pattern[index] === '!' || pattern[index] === '^') {
    negated = true;
    index += 1;
  }
  // A `]` immediately after the opening bracket is a literal `]`, per POSIX.
  if (pattern[index] === ']') {
    index += 1;
  }

  while (index < pattern.length && pattern[index] !== ']') {
    index += 1;
  }
  if (index >= pattern.length) {
    return undefined;
  }

  const body = pattern.slice(start + 1, index).replace(/^[!^]/, '');
  const source = `[${negated ? '^' : ''}${body.replace(/\\/g, '\\\\')}]`;
  try {
    // Let the engine validate range ordering and other class grammar once,
    // during compilation rather than while walking paths.
    new RegExp(source);
  } catch (error) {
    throw new InvalidGlobPattern(
      pattern,
      error instanceof Error ? error.message : String(error),
    );
  }
  return {
    source,
    next: index + 1,
  };
};

/**
 * Compile a glob to an anchored regular expression.
 *
 * Anchored at both ends: an unanchored glob would make `*.ts` match
 * `notes.tsx`, and a model reading that list has no way to tell.
 */
export const compile = (pattern: string): RegExp => {
  if (pattern.length > MAX_PATTERN_LENGTH) {
    throw new InvalidGlobPattern(
      pattern,
      `glob exceeds ${String(MAX_PATTERN_LENGTH)} characters`,
    );
  }
  let source = '';
  let index = 0;

  while (index < pattern.length) {
    const character = pattern[index] ?? '';

    if (character === '*') {
      if (pattern[index + 1] === '*') {
        index += 2;
        if (pattern[index] === '/') {
          // `**/` spans whole directory segments, including none at all, so
          // `**/x.ts` finds a top-level `x.ts` as well as a nested one.
          index += 1;
          source += '(?:[^/]+/)*';
        } else {
          source += '.*';
        }
      } else {
        index += 1;
        source += '[^/]*';
      }
      continue;
    }

    if (character === '?') {
      index += 1;
      source += '[^/]';
      continue;
    }

    if (character === '[') {
      const parsed = readClass(pattern, index);
      if (parsed !== undefined) {
        source += parsed.source;
        index = parsed.next;
        continue;
      }
    }

    index += 1;
    source += escapeLiteral(character);
  }

  try {
    return new RegExp(`^${source}$`);
  } catch (error) {
    throw new InvalidGlobPattern(
      pattern,
      error instanceof Error ? error.message : String(error),
    );
  }
};

/** Whether one `/`-separated path matches one glob. */
export const matches = (pattern: string, path: string): boolean =>
  compile(pattern).test(path);

export * as WorkspaceGlob from './glob.js';
