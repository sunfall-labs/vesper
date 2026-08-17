import { Layer } from 'effect';
import { type Tool, Toolkit } from 'effect/unstable/ai';

import { WorkspaceTools } from './tools.js';

export type StandardTools = Toolkit.Tools<typeof WorkspaceTools.toolkit>;
export type StandardToolName = keyof StandardTools;

type Collision<Tools extends Record<string, Tool.Any>> = Extract<
  keyof Tools,
  StandardToolName
>;

type CollisionFree<Tools extends Record<string, Tool.Any>> = [
  string extends keyof Tools ? never : Collision<Tools>,
] extends [never]
  ? unknown
  : { readonly __workspaceToolNameCollision__: Collision<Tools> };

/** Explicit workspace tools plus the layer that owns their standard handlers. */
export interface Adapter<ApplicationTools extends Record<string, Tool.Any>> {
  readonly toolkit: Toolkit.Toolkit<ApplicationTools & StandardTools>;
  readonly layer: Layer.Layer<
    Tool.HandlersFor<StandardTools> | WorkspaceTools.CommandPolicy
  >;
}

const standardLayer = Layer.merge(
  WorkspaceTools.layer,
  WorkspaceTools.defaultCommandPolicyLayer,
);

/** The standard workspace toolkit without application-owned tools. */
export const standard: Adapter<{}> = {
  toolkit: WorkspaceTools.toolkit,
  layer: standardLayer,
};

/** Add the standard workspace tools to an application toolkit. */
export const addTo = <ApplicationTools extends Record<string, Tool.Any>>(
  application: Toolkit.Toolkit<ApplicationTools> &
    CollisionFree<ApplicationTools>,
): Adapter<ApplicationTools> => {
  const collision = Object.keys(application.tools).find((name) =>
    Object.hasOwn(WorkspaceTools.toolkit.tools, name),
  );
  if (collision !== undefined) {
    throw new Error(
      `WorkspaceAgent cannot add standard tool "${collision}": the application toolkit already defines it`,
    );
  }

  return {
    toolkit: Toolkit.merge(
      application,
      WorkspaceTools.toolkit,
    ) as Toolkit.Toolkit<ApplicationTools & StandardTools>,
    layer: standardLayer,
  };
};

export * as WorkspaceAgent from './agent.js';
