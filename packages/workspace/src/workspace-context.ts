import { Context, Layer } from 'effect';

/**
 * The directory the toolkit treats as the workspace.
 *
 * A separate service rather than a constructor argument, for the same reason
 * the driver is: it belongs in the requirement channel. An agent whose tools
 * can reach the filesystem should not compile until someone has said *which*
 * filesystem and *which* directory.
 *
 * **Containment here is lexical and preflight.** Paths are resolved and
 * checked against this root before they reach the driver, which stops a model
 * that wandered — not code that meant to leave. The no-symlink policy probes
 * existing components immediately before the operation, but cannot close a
 * filesystem TOCTOU race with another process or with an enabled shell tool.
 * When the application explicitly permits symlinks, one inside the root is
 * followed wherever it points; `run_shell` executes a command string nothing
 * inspects. See the boundary note in `driver.ts`; this narrows what the tools
 * address, and the driver's substrate is still what confines.
 */
export class Root extends Context.Service<Root, { readonly path: string }>()(
  '@sunfall/vesper-workspace/WorkspaceRoot',
) {}

/** Select the workspace root used by every workspace tool. */
export const rootLayer = (path: string): Layer.Layer<Root> =>
  Layer.succeed(Root, { path });

/** Filesystem policy for model-addressed paths. */
export interface FilesystemPolicyConfig {
  /** Follow links only when the application explicitly opts in. */
  readonly allowSymlinks: boolean;
}

export class FilesystemPolicy extends Context.Service<
  FilesystemPolicy,
  FilesystemPolicyConfig
>()('@sunfall/vesper-workspace/FilesystemPolicy') {}

export const filesystemPolicyLayer = (
  config: FilesystemPolicyConfig,
): Layer.Layer<FilesystemPolicy> => Layer.succeed(FilesystemPolicy, config);

/** Safe default: a model cannot turn a workspace path into a link traversal. */
export const defaultFilesystemPolicyLayer: Layer.Layer<FilesystemPolicy> =
  filesystemPolicyLayer({ allowSymlinks: false });

/** Explicit opt-in for legacy host-local link-following behavior. */
export const unrestrictedFilesystemPolicyLayer: Layer.Layer<FilesystemPolicy> =
  filesystemPolicyLayer({ allowSymlinks: true });
