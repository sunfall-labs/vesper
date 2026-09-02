# Releasing

Vesper is pre-1.0. The six publishable `@sunfall/vesper-*` packages are released
together at the same version so their generated types and sibling dependencies
stay compatible. The `effect` peer dependency is currently pinned to
`4.0.0-rc.112`; update that family deliberately as one change. The pin appears
in the install commands of the root and package READMEs and in two prose
statements (the root README and this file); find every markdown occurrence
with `grep -rn '4.0.0-rc' --include='*.md' .` when bumping it.

## Before a release

Use Node 24 for the release rehearsal (the publish workflow uses Node 24); the
published packages declare Node `>=22`, which CI verifies on both Node 22 and 24. On a fresh checkout, bootstrap the pinned package manager first:

```bash
npx @nubjs/nub@0.7.5 install
```

1. Update every publishable package version and any exact Vesper sibling
   dependency ranges in `packages/*/package.json`.
2. Add release notes to [`CHANGELOG.md`](../CHANGELOG.md). Call out breaking
   API changes, persisted conversation changes, and migration steps.
3. Run the full gate and inspect the package contents:

   ```bash
   nub run verify
   nub run publish:npm:dry-run
   ```

4. Confirm the migration at `packages/log-pg/migrations/001-initial.sql` is
   included when the PostgreSQL package changes, and that the public export
   maps still point at built files.

Publishing is deliberately blocked while the root has `patchedDependencies`,
because dependency patches are not inherited by registry consumers. Every
patched behavior has to land upstream, the pinned Effect family has to move to a
release carrying it, the patch has to go, and the packed-consumer behavior check
has to pass against the released dependency before Vesper can publish.

Of the three pieces below, two clear themselves on the next Effect release. Only
recovering a call to a tool which is not in the toolkit needs something to land,
so #7574 is the one thing between here and an unpatched publish.

### What the Effect patches carry

The pin is `effect@4.0.0-rc.112`, published 2026-08-25. Three separate pieces of
behavior are patched in, at different stages of landing upstream.

**Tool call parameter validation through `failureMode`.** Landed upstream as
[#7588][7588] on 2026-09-02, after rc.112 was cut, so it is not in the pinned
release. It makes `Toolkit` the single authority for parameter validation and
routes a decode failure through the tool's own `failureMode`, and it reworks the
provider packages to pass raw parameters through. The first Effect release after
rc.112 makes this native: at that point these hunks are deleted rather than
rebased, and the three `@effect/ai-*` patches should go with them — all three
exist only to give providers the patch's `Response.toolCallPartFromJson`, which
that release replaces with passthrough.

**Recovering a call to a tool which is not in the toolkit.** Proposed upstream
as [#7574][7574], still open. Supplies `Response.ToolCallErrorPart`,
`Response.ToolCallError`, the `Prompt.fromResponseParts` case which turns one
into a failed tool result, and the `internal/http-details` split that lets
`Response` reference `AiError` without a cycle. Vesper needs this because
`dynamic-toolkit` changes the toolkit between turns, so a model calling a tool
that has since been removed is ordinary rather than exceptional. Consumed by
`internal/loop.ts`, `internal/part-encoding.ts`, and `recording-sink.ts`.

Note that the upstream proposal is opt-in per operation (`unknownToolCalls`)
while the patch recovers unconditionally, and that upstream leaves a call naming
a *known* tool to that tool's `failureMode`. Vesper already sets
`failureMode: 'return'` on effectively every tool, so the behavioral outcome
matches; the call sites in `internal/loop.ts` will need the option passed
explicitly when the patch is dropped.

**A decoded-parameter entry point on `Toolkit`.** `Toolkit.DecodedHandle`,
`Toolkit.Execute`, `Toolkit.makeWithHandler`, and the `Tool.Parameters` /
`Tool.ParametersEncoded` / `Tool.HandlerResult` utility types.

This is not proposed upstream, and should not be. It exists only to serve the
patch's own `LanguageModel`, which validates parameters once at the model
boundary and then hands decoded parameters to `Toolkit` to avoid decoding a
schema transformation twice — the design #7588 replaced by making `Toolkit` the
single validation authority and accepting that second decode. Proposing this
entry point would be proposing the optimization for a design upstream has just
removed.

It dissolves on upgrade instead. A release carrying #7588 hands
`Toolkit.handle` opaque parameters, so `dispatch.ts` falls to its existing
`parameters: 'encoded'` path — already exercised today — and decodes once itself
for interception, arbitration, and `ToolStarted`, exactly as it does now for
every call that arrives encoded. `code-mode.ts`, `result-overflow.ts`, and
`dynamic-toolkit.ts` then wrap one boundary rather than two, and `makeHandle`
loses its overload.

The cost is one extra parameter decode per tool call, which is already what the
encoded path pays.

It cannot be done before that upgrade. While the patched `LanguageModel` still
pre-decodes, the decoded boundary carries genuinely decoded values — a `Date`
for a `Schema.DateFromString` parameter, not the string it came from — and
routing those through the encoded path decodes them a second time, which fails.
Vesper answers that failure with a `ToolParameterValidationError` in the tool's
place rather than raising, so the whole suite stays green while every call to a
tool with a transforming parameter schema silently stops executing. The
`transformed_lookup` case in `packages/agent/test/log.test.ts` asserts the
`ToolOutcome` the handler produced, not just the `ToolCall` record written
before it runs, precisely so that this fails loudly when the time comes.

[7588]: https://github.com/Effect-TS/effect/pull/7588
[7574]: https://github.com/Effect-TS/effect/pull/7574

### On upgrading the Effect pin

Patch files are version-pinned by filename, so a new pin means regenerating
rather than editing: `pnpm patch effect@<version>`, reapply only the pieces
above that are still unlanded, `pnpm patch-commit`. Check `Response.Part` and
`Response.StreamPart` first — #7588 added a `ToolParametersMode` type parameter
in the slot the patch also threads through, so those hunks conflict structurally
rather than textually.

The packed-consumer check installs every generated tarball into a clean
temporary project, imports every runtime export, reads exported assets, and
typechecks every public module from its published declarations. It is part of
`nub run verify`; `nub run audit:packed-consumer` adds a production dependency
vulnerability audit.

## Publishing

The release workflow verifies that a pushed `vX.Y.Z` tag matches every package
version, runs `nub run verify`, and publishes with provenance. Pre-release tags
map to npm dist-tags as follows: `-alpha` to `alpha`, `-beta` to `beta`, and
`-rc` to `next`. Stable tags publish to `latest`; use the workflow dispatch
input when an intentional override is needed. Use the workflow's dry-run mode
to inspect a release without uploading it.

After publishing, install the exact version in a clean Node.js 22+ project and
verify the documented subpath imports, type declarations, and (for PostgreSQL)
the migration artifact. Record any follow-up in the changelog rather than
silently changing a published version.

Nub skips versions already present on the registry. If a multi-package upload
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
