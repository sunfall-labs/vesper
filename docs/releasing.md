# Releasing

Vesper is pre-1.0. The six publishable `@sunfall/vesper-*` packages are released
together at the same version so their generated types and sibling dependencies
stay compatible. The `effect` peer dependency is currently pinned to
`4.0.0-rc.109`; update that family deliberately as one change.

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
