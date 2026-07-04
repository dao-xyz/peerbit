# Releasing

This repository publishes its public `@peerbit/*` packages to npm using
[changesets](https://github.com/changesets/changesets). Versioning is authored
per pull request instead of inferred from commit messages, so majors, minors,
and mixed bumps are explicit and reviewable.

## Authoring a change

When a pull request changes the behavior of one or more publishable packages,
add a changeset describing the bump:

```sh
pnpm changeset
```

The prompt lets you pick the affected packages and the bump level
(`major` / `minor` / `patch`) and write a short summary. This writes a
`.changeset/*.md` file — commit it with your PR. A PR that only touches
tooling, tests, CI, or docs does not need a changeset.

Internal dependencies are cascaded automatically: when an internal dependency
is bumped, its dependents are bumped at least a `patch`
(`updateInternalDependencies: "patch"` in `.changeset/config.json`), mirroring
the old release-please node-workspace behavior. Packages listed under `ignore`
in the config (private, e2e, example, and app packages, plus `@peerbit/test-lib`)
are never versioned or published.

## The Version Packages PR

On every push to `master`, the `Release` workflow runs `changesets/action`,
which opens (or updates) a bot pull request titled **"chore: version
packages"**. That PR accumulates all pending changesets: it applies the version
bumps to each `package.json`, updates the `CHANGELOG.md` files, and deletes the
consumed `.changeset/*.md` files. As more changesets land, the PR keeps
updating itself.

Review that PR like any other. Nothing is published while it is open.

> [!NOTE]
> The Version Packages PR is opened with `GITHUB_TOKEN`, which does not
> trigger downstream workflows — so CI does not automatically run on it. If you
> want CI to run on the Version Packages PR, add a bot Personal Access Token
> secret (repo + workflow scopes) and pass it as `token:` to
> `changesets/action` in `.github/workflows/release.yml`.

## Publishing a stable release

Merge the **"chore: version packages"** PR into `master`. The `Release`
workflow then:

1. installs the wasm toolchain and dependencies,
2. runs `pnpm run build`,
3. runs `pnpm run release` (`scripts/publish-public-packages.mjs`), which
   topologically sorts the public packages and publishes every version that is
   not yet on npm (`from-package` semantics), and
4. creates the `<name>@<version>` git tags (e.g. `peerbit@5.2.21`,
   `@peerbit/crypto@3.2.0`) with `pnpm exec changeset tag` and pushes them. See
   "Git tags and the format change" below — this is a new tag namespace, not the
   old `<component>-v<version>` one.

The downstream `Post Release Automation` workflow then restores the
`workspace:*` protocol (a no-op with changesets, which preserves it) and, when
`@peerbit/server` changed, opens the bootstrap rollout PR.

## Git tags and the format change

changesets tags each released package as `<name>@<version>`, e.g.
`peerbit@5.2.21`, `@peerbit/crypto@3.2.0`. This is **not** the format
release-please used — it tagged `<component>-v<version>`, e.g. `peerbit-v5.2.20`,
`crypto-v3.1.1`. The two namespaces do not overlap, so:

- Legacy `<component>-v<version>` tags remain in history untouched; nothing
  rewrites or deletes them.
- The **first** release run after this migration sees every current package as
  untagged (no `<name>@<version>` tag exists yet) and creates a one-time
  `@`-format tag baseline at the current versions. It publishes nothing new to
  npm (every current version is already published — `from-package` skips them),
  so this baseline is metadata only.
- Every subsequent run tags only the versions that were actually bumped.

Any external tooling or consumer that resolved the old `peerbit-v5.2.21` tag
format must be updated to the `peerbit@5.2.21` format.

GitHub Releases are intentionally **not** created by the release workflow
(`createGithubReleases: false`). The existing `CHANGELOG.md` files are in
release-please's `### [x.y.z](compare-url) (date)` format, which changesets
cannot parse into per-version release notes, so enabling releases would emit one
GitHub Release per package with the entire changelog as its body. npm publishing
and git tagging are unaffected. Once the changelogs are migrated to the
changesets `## <version>` format, releases can be re-enabled.

## Manual publishing and release candidates

Both are driven from the **Actions** tab via **Run workflow** on the `Release`
workflow:

- **stable** — builds and runs the from-package publisher directly, as an
  escape hatch to publish any package whose `package.json` version is not yet
  on npm without going through the Version Packages PR.
- **rc** — builds and runs `pnpm run release:rc` (`aegir release-rc`) to
  publish prerelease versions.

## Caveat: "no changeset" does not mean "no publish"

The workflow has a publish script, so on any push to `master` with no pending
changesets, `changesets/action` runs the publish command directly instead of
opening a Version Packages PR. The from-package publisher
(`scripts/publish-public-packages.mjs`) is idempotent — it queries npm and skips
every version already published — so ordinary pushes do not double-publish. But
this means a `package.json` version bumped **by hand** and pushed to `master`
without a changeset **will** be published on the next release run, bypassing the
authored-bump model. Always bump versions through a changeset and the Version
Packages PR; never hand-edit a publishable `package.json` version on `master`.
