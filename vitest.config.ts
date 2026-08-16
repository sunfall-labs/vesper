import { defineConfig } from 'vitest/config';

// Tests run from the repository root against package **sources**, not against
// built output, so a failing test points at the file you would edit. The map
// below mirrors every subpath each package declares in its `exports`; the same
// list appears as `paths` in `tsconfig.base.json`, which is what typechecks
// them.
const alias = {
  '@sunfall/vesper-agent/agent': new URL(
    './packages/agent/src/agent.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/branch': new URL(
    './packages/agent/src/branch.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/context-window': new URL(
    './packages/agent/src/context-window.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/dispatch': new URL(
    './packages/agent/src/dispatch.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/event': new URL(
    './packages/agent/src/event.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/history': new URL(
    './packages/agent/src/history.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/interception': new URL(
    './packages/agent/src/interception.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/log': new URL(
    './packages/agent/src/log.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/signal': new URL(
    './packages/agent/src/signal.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/stop': new URL(
    './packages/agent/src/stop.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/subagent': new URL(
    './packages/agent/src/subagent.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/skill': new URL(
    './packages/agent/src/skill.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-agent/compaction': new URL(
    './packages/agent/src/compaction.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-log/offset': new URL(
    './packages/log/src/offset.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-log/record': new URL(
    './packages/log/src/record.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-log/log-store': new URL(
    './packages/log/src/log-store.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-log/tail': new URL(
    './packages/log/src/tail.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-log/layer-memory': new URL(
    './packages/log/src/layer-memory.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-log/log-store-contract': new URL(
    './packages/log/src/log-store-contract.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-log/layer-pg': new URL(
    './packages/log/src/layer-pg.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-pi/compaction': new URL(
    './packages/pi/src/compaction.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-pi/credentials': new URL(
    './packages/pi/src/credentials.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-pi/errors': new URL(
    './packages/pi/src/errors.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-pi/model': new URL(
    './packages/pi/src/model.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-pi/provider': new URL(
    './packages/pi/src/provider.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-pi/registry': new URL(
    './packages/pi/src/registry.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-pi/retry': new URL(
    './packages/pi/src/retry.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-runtime/runtime': new URL(
    './packages/runtime/src/runtime.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-workspace/driver': new URL(
    './packages/workspace/src/driver.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-workspace/glob': new URL(
    './packages/workspace/src/glob.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-workspace/layer-local': new URL(
    './packages/workspace/src/layer-local.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-workspace/output': new URL(
    './packages/workspace/src/output.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-workspace/path': new URL(
    './packages/workspace/src/path.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-workspace/tools': new URL(
    './packages/workspace/src/tools.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-workspace/workspace-contract': new URL(
    './packages/workspace/src/workspace-contract.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-attachments/ref': new URL(
    './packages/attachments/src/ref.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-attachments/attachment-store': new URL(
    './packages/attachments/src/attachment-store.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-attachments/layer-memory': new URL(
    './packages/attachments/src/layer-memory.ts',
    import.meta.url,
  ).pathname,
  '@sunfall/vesper-attachments/attachment-store-contract': new URL(
    './packages/attachments/src/attachment-store-contract.ts',
    import.meta.url,
  ).pathname,
};

export default defineConfig({
  resolve: { alias },
  test: {
    name: 'vesper',
    globals: true,
    environment: 'node',
    include: ['packages/*/test/**/*.test.ts'],
    // The Postgres suite provisions and drops a real database in
    // beforeAll/afterAll. Vitest's 10s hook default is not enough for that
    // under parallel load, and afterAll hooks rarely pass their own timeout.
    hookTimeout: 180_000,
  },
});
