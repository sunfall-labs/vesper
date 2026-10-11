# Releasing

Vesper is pre-1.0. The six publishable `@sunfall/vesper-*` packages are released
together at the same version so their generated types and sibling dependencies
stay compatible. The `effect` peer dependency is `^4.0.3`, and the
workspace catalog pins the whole Effect family to `4.0.3` for development;
update that family deliberately as one change. The version appears in the
install commands of the root and package READMEs; find every markdown
occurrence with `grep -rn '4\.0\.3' --include='*.md' .` when bumping it.

## Before a release

Use Node 24 for the release rehearsal (the publish workflow uses Node 24); the
published packages declare Node `>=22`, which CI verifies on both Node 22 and 24. On a fresh checkout, install with the pinned Bun first:

```bash
bun install --frozen-lockfile
```

1. Update every publishable package version and any exact Vesper sibling
   dependency ranges in `packages/*/package.json`.
2. Add release notes to [`CHANGELOG.md`](../CHANGELOG.md). Call out breaking
   API changes, persisted conversation changes, and migration steps.
3. Run the full gate and inspect the package contents:

   ```bash
   bun run verify
   bun run publish:npm:dry-run
   ```

4. Confirm the migration at `packages/log-pg/migrations/001-initial.sql` is
   included when the PostgreSQL package changes, and that the public export
   maps still point at built files.

Publishing is deliberately blocked while the root has `patchedDependencies`,
because dependency patches are not inherited by registry consumers. Every
patched behavior has to land upstream, the pinned Effect family has to move to a
release carrying it, the patch has to go, and the packed-consumer behavior check
has to pass against the released dependency before Vesper can publish.

The patches carry upstream [#9008][9008], which is merged but not yet in a
release. They come off at the first Effect release that includes it, which
also unblocks publishing.

### What the Effect patches carry

The pin is `effect@4.0.3`. `patches/effect@4.0.3.patch`,
`patches/@effect%2Fai-anthropic@4.0.3.patch`, and
`patches/@effect%2Fai-openai@4.0.3.patch` backport exactly [#9008][9008]:
`toolCallValidation: "deferred"`, which, with `disableToolCallResolution:
true`, returns every tool call the model makes as a plain call with the
model-authored parameters — including a call to a tool the request did not
offer and one whose parameters do not decode — instead of failing the
response. It adds the `deferred` parameters mode to `Response`, and makes the
Anthropic and OpenAI providers return calls to undeclared tools instead of
failing them with `ToolNotFoundError` before `LanguageModel` sees them.
OpenRouter needed no change.

Vesper runs its own tool calls on top of it (`internal/tool-resolution.ts`):
the loop asks the model with resolution disabled and deferred validation, and
once the response has finished classifies each call exactly once — undeclared
(`Object.hasOwn`, never a prototype lookup), awaiting approval, or run through
the dispatch gate — so recovery, execution, and history all read one decision.
Vesper needs this because `dynamic-toolkit` changes the toolkit between turns,
so a model calling a tool that has since been removed is ordinary rather than
exceptional.

[9008]: https://github.com/Effect-TS/effect/pull/9008

### On upgrading the Effect pin

Patch files are version-pinned by filename, so a new pin means regenerating
rather than editing. Cherry-pick #9008 onto the new `effect@<version>` tag in
an Effect checkout, build `effect`, `@effect/ai-anthropic`, and
`@effect/ai-openai` the way the release does (`set-strip-internal`,
`codemod`, then the build) so unchanged files are byte-identical to the
published tarballs, and diff the changed files against each extracted tarball
with paths relative to the package root. `git apply --check` against a fresh
copy of each tarball confirms a patch before `bun install` applies it.
`bun run check:packed-consumer` asserts the patched source and built output
both carry the change.

When a release ships #9008, delete the patches and the root
`patchedDependencies` entries; nothing else in Vesper changes.

The packed-consumer check installs every generated tarball into a clean
temporary project, imports every runtime export, reads exported assets, and
typechecks every public module from its published declarations. It is part of
`bun run verify`; `bun run audit:packed-consumer` adds a production dependency
vulnerability audit.

## Publishing

The release workflow verifies that a pushed `vX.Y.Z` tag matches every package
version, runs `bun run verify`, and publishes with provenance. Pre-release tags
map to npm dist-tags as follows: `-alpha` to `alpha`, `-beta` to `beta`, and
`-rc` to `next`. Stable tags publish to `latest`; use the workflow dispatch
input when an intentional override is needed. Use the workflow's dry-run mode
to inspect a release without uploading it.

After publishing, install the exact version in a clean Node.js 22+ project and
verify the documented subpath imports, type declarations, and (for PostgreSQL)
the migration artifact. Record any follow-up in the changelog rather than
silently changing a published version.

`scripts/publish-packages.mjs` skips versions already present on the registry. If a multi-package upload
is interrupted, rerun the same workflow/tag after fixing the cause; already
published versions are skipped and the remaining packages are retried. Do not
republish a changed tarball under an existing version.

## Required external setup

The repository cannot verify these registry and GitHub settings locally:

- The npm scope must permit public publishing for the release account.
- Trusted publishers can only be configured for packages that already exist
  on npm. Bootstrap each package's first public version with an authenticated
  release account and 2FA, without changing the rehearsed tarballs. Then
  configure OIDC before creating the next release tag.
- npm Trusted Publishing must be configured for the `sunfall-labs/vesper`
  repository and `.github/workflows/publish-npm.yml`, with the `npm-publish`
  GitHub environment selected if the npm configuration asks for one.
- GitHub Actions must continue allowing the workflow's `id-token: write`
  permission. The `npm-publish` environment is configured to require a review
  from the repository owner; preserve that gate.
- Release operators need permission to push `v*` tags. A pushed matching tag
  performs a real publish; use workflow dispatch with `dry-run: true` for the
  rehearsal.

Provenance is attached only for real uploads from the workflow's OIDC-capable
environment. The local `publish:npm` script is alpha-only and should not be
used as a substitute for the trusted-publisher workflow.

## Compatibility policy

Before 1.0, minor and patch releases may contain breaking changes, especially
in Effect release-candidate integrations or persisted schemas. Agent revisions
and conversation-format versions are compatibility gates, not automatic
migrations: bump the application-owned agent revision when durable history is
no longer safe, and provide an explicit migration for old histories.
