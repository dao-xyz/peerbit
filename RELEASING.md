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
4. creates the `pkg@version` git tags with `pnpm exec changeset tag` and pushes
   them.

The downstream `Post Release Automation` workflow then restores the
`workspace:*` protocol (a no-op with changesets, which preserves it) and, when
`@peerbit/server` changed, opens the bootstrap rollout PR.

## Manual publishing and release candidates

Both are driven from the **Actions** tab via **Run workflow** on the `Release`
workflow:

- **stable** — builds and runs the from-package publisher directly, as an
  escape hatch to publish any package whose `package.json` version is not yet
  on npm without going through the Version Packages PR.
- **rc** — builds and runs `pnpm run release:rc` (`aegir release-rc`) to
  publish prerelease versions.
