// Turning a path the model wrote into a path inside the workspace root.
//
// **Lexical containment, not confinement.** This is the same distinction the
// driver makes in `driver.ts`, one layer up: resolving `../../etc/passwd` to
// something outside the root and refusing it stops a model that wandered, and
// it does not stop code that meant to escape. A symlink inside the root
// pointing anywhere is followed by the driver, and `exec` runs a command
// string nothing here inspects. If hostile code is in scope, the containment
// has to come from the driver's substrate.
//
// POSIX only, and deliberately not `node:path`. `node:path` on Windows treats
// `\` as a separator and `C:` as a root, so a driver talking to a Linux
// container would get its paths reinterpreted by whatever OS the *host*
// happens to run. The workspace contract already fixes `/`-separated paths;
// this implements exactly that and nothing platform-dependent.

export type Resolution =
  | { readonly ok: true; readonly path: string }
  | { readonly ok: false; readonly reason: 'escapes-root' | 'nul-byte' };

/**
 * Collapse `.`, `..`, empty, and repeated segments.
 *
 * `..` at the top of an absolute path is dropped rather than kept, matching
 * POSIX: `/..` is `/`. That is what makes the containment check below sound —
 * a path cannot normalize to something with an unresolved `..` still in it.
 */
export const normalize = (path: string): string => {
  const absolute = path.startsWith('/');
  const segments: Array<string> = [];

  for (const segment of path.split('/')) {
    if (segment === '' || segment === '.') {
      continue;
    }
    if (segment === '..') {
      const last = segments[segments.length - 1];
      if (last !== undefined && last !== '..') {
        segments.pop();
      } else if (!absolute) {
        segments.push('..');
      }
      continue;
    }
    segments.push(segment);
  }

  const joined = segments.join('/');
  if (absolute) {
    return `/${joined}`;
  }
  return joined === '' ? '.' : joined;
};

/**
 * Whether `path` is `root` or sits beneath it. Both must already be
 * normalized.
 *
 * The trailing separator is what keeps `/workspace-backup` from passing as a
 * child of `/workspace`, which a naive `startsWith` would allow.
 */
export const contains = (root: string, path: string): boolean =>
  path === root || path.startsWith(root === '/' ? '/' : `${root}/`);

/**
 * Resolve a model-supplied path against the workspace root.
 *
 * A relative path is taken as relative to the root, which is the reading a
 * model expects when it has been told what the root is. An absolute path is
 * accepted only if it is already inside.
 *
 * A NUL byte is rejected outright: `node:fs` throws on one rather than
 * returning an `errno`, so it would arrive as a defect instead of a typed
 * failure, and there is no legitimate path containing it.
 */
export const resolve = (root: string, path: string): Resolution => {
  if (root.includes('\0') || path.includes('\0')) {
    return { ok: false, reason: 'nul-byte' };
  }

  const normalizedRoot = normalize(root);
  const candidate = normalize(
    path.startsWith('/') ? path : `${normalizedRoot}/${path}`,
  );

  return contains(normalizedRoot, candidate)
    ? { ok: true, path: candidate }
    : { ok: false, reason: 'escapes-root' };
};

/**
 * The part of `path` below `root`, for reporting a result back to the model in
 * the same terms it asked in. Returns `.` for the root itself.
 */
export const relative = (root: string, path: string): string => {
  const normalizedRoot = normalize(root);
  if (path === normalizedRoot) {
    return '.';
  }
  const prefix = normalizedRoot === '/' ? '/' : `${normalizedRoot}/`;
  return path.startsWith(prefix) ? path.slice(prefix.length) : path;
};

export * as WorkspacePath from './path.js';
