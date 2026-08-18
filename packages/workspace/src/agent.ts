import { Layer } from 'effect';
import { type Tool, Toolkit } from 'effect/unstable/ai';

import { WorkspaceTools } from './tools.js';

export type StandardTools = Toolkit.Tools<typeof WorkspaceTools.toolkit>;
export type StandardToolName = keyof StandardTools;
type ComposedTools<ApplicationTools extends Record<string, Tool.Any>> =
  Toolkit.MergedTools<
    [Toolkit.Toolkit<ApplicationTools>, typeof WorkspaceTools.toolkit]
  >;

type Collision<Tools extends Record<string, Tool.Any>> = Extract<
  keyof Tools,
  StandardToolName
>;

type CollisionFree<Tools extends Record<string, Tool.Any>> = [
  string extends keyof Tools ? 'erased' : Collision<Tools>,
] extends [never]
  ? unknown
  : {
      readonly __workspaceToolkitMustBePrecise__: string extends keyof Tools
        ? true
        : Collision<Tools>;
    };

/** Explicit workspace tools plus the layer that owns their standard handlers. */
export interface Composition<
  ApplicationTools extends Record<string, Tool.Any>,
> {
  readonly toolkit: Toolkit.Toolkit<ComposedTools<ApplicationTools>>;
  readonly layer: Layer.Layer<
    | Tool.HandlersFor<StandardTools>
    | WorkspaceTools.CommandPolicy
    | WorkspaceTools.FilesystemPolicy
  >;
}

const standardLayer = Layer.merge(
  Layer.merge(WorkspaceTools.layer, WorkspaceTools.defaultCommandPolicyLayer),
  WorkspaceTools.defaultFilesystemPolicyLayer,
);

/** The standard workspace toolkit without application-owned tools. */
export const standard: Composition<{}> = {
  toolkit: WorkspaceTools.toolkit,
  layer: standardLayer,
};

/** Add the standard workspace tools to an application toolkit. */
export const compose = <ApplicationTools extends Record<string, Tool.Any>>(
  application: Toolkit.Toolkit<ApplicationTools> &
    CollisionFree<ApplicationTools>,
): Composition<ApplicationTools> => {
  const collision = Object.keys(application.tools).find((name) =>
    Object.hasOwn(WorkspaceTools.toolkit.tools, name),
  );
  if (collision !== undefined) {
    throw new Error(
      `WorkspaceAgent cannot add standard tool "${collision}": the application toolkit already defines it`,
    );
  }

  return {
    toolkit: Toolkit.merge(application, WorkspaceTools.toolkit),
    layer: standardLayer,
  };
};

export * as WorkspaceAgent from './agent.js';
