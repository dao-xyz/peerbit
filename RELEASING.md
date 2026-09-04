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
the old release-please node-workspace behavior. Every internal dependency,
optional dependency, peer dependency, and dev dependency must preserve one of
the supported `workspace:*`, `workspace:^`, or `workspace:~` shorthands.
Changesets config is pinned to the GitHub changelog adapter for
`dao-xyz/peerbit`, `commit: false`, public access, the current Changesets 3.1.4
schema, and the exact supported config-key set. Every `ignore` entry must be the
exact name of an existing authoritative workspace package. Ignored packages
must be private and are never versioned or published, with the sole deliberate
exception of the public, frozen `@peerbit/test-lib` package. A package is also
skipped when it is private or its version is absent, `null`, or empty, whether
or not it is in `ignore`. `private`, when present, must be a boolean; every
truthy version must be a canonical string `major.minor.patch`. A public,
versioned, non-ignored package may not have a runtime, optional, or peer
dependency on any skipped workspace package; dev dependencies are allowed for
test and tooling support. The private repository-root manifest is workspace
policy only and is never treated as a Changesets package.

Pull-request CI enforces this per package. It compares the PR head with its
validated Git merge base and requires every publishable package with a
release-relevant change to appear in a **new**, direct, non-dot-prefixed,
case-sensitive `.changeset/*.md` file (excluding `README.md`,
case-insensitively), matching `@changesets/read` discovery. Source, `src_js`,
Cargo, packaged assets, `.gitignore`/`.npmignore` packlist inputs, positive
`files` patterns, and runtime `package.json` semantics count. Repeated leading
`!` operators in `files` use pnpm-negation parity; unsupported extglob, group,
class, brace, or interior-bang patterns are treated conservatively as relevant
instead of suppressing coverage. Exact targets named by `main`, `module`,
`types`, `typings`, `browser`, `bin`, or recursively by `exports`, along with
their extension-resolved siblings (`target.*`) and descendants (`target/**`),
always count even under an otherwise exempt test/tool path. Targets selected
by `types` or `typings`, plus packable TypeScript-resolvable files selected by a
validated `typesVersions` target, also follow TypeScript's terminal `.js`,
`.jsx`, `.mjs`, and `.cjs` extension substitutions; runtime-only fields do not
inherit those substitutions. Malformed selector maps, non-array targets,
unsafe paths, and keys or targets with more than one `*` wildcard fail closed.
The private server frontend is a trusted artifact producer: every
`packages/clients/peerbit-server/frontend/src/**` and
`packages/clients/peerbit-server/frontend/public/**` input, plus its package and
build config, requires an `@peerbit/server` changeset. Documentation, tests, e2e
fixtures, benchmarks, unrelated tool configuration, private nested packages,
and packages already listed under Changesets `ignore` do not. Only regular Git
blobs with mode `100644` or `100755` are accepted anywhere in either tree, so
symlinks, submodules, and other special tree entries fail closed. An existing
changeset cannot be edited or reused as coverage. Every new direct changeset
summary must also be safe for the configured GitHub changelog adapter: raw
HTML, ATX H1/H2 headings, Setext H1/H2 headings, and authored backtick or tilde
fence delimiters are rejected before merge, including delimiters indented with
arbitrary horizontal whitespace. Line endings are normalized before these
structural checks so bare carriage returns cannot hide a heading. An active
package version must never be bumped by hand. Adding the first canonical
version to activate a previously skipped unversioned package is allowed only
with new changeset coverage. Every PR head must descend from the exact base SHA
in its event.

The primary `.github/workflows/changeset-guard.yml` check is a minimal,
base-owned `pull_request_target` workflow with only `contents: read`. It checks
out the exact event base, fetches `refs/pull/<number>/head` to a remote ref as
data, verifies the full event SHAs and repository identity, and executes only
the base guard and its tests. It never checks out or executes the PR head, uses
secrets, installs dependencies, caches artifacts, or runs a build. The workflow
itself must exactly match the guard's canonical commit-pinned contract.

Ordinary secret-free `pull_request` CI remains as defense in depth. It loads
the guard and mutation tests with `git show` from the trusted base SHA into a
private temporary directory, never executing the PR head's copy. The guard
evaluates ownership from the union of the base owner, the surviving base-policy
head owner, and the head-policy owner for both sides of every change. This
preserves ownership across edits, deletes, renames, nested-root transitions,
and workspace-policy changes while still requiring coverage for a newly
exposed public package. A removed root is not retained as a head owner unless
the root is still authoritative in the head. An arbitrary or same-PR nested
private `package.json` therefore cannot shadow an existing publishable owner.

There is one explicit bootstrap exception for the PR that first introduces the
three-file guard boundary. When the trusted base contains neither executable
guard file and no target workflow, ordinary CI runs the introducing PR's guard
and tests as a pair and validates the target workflow as data. Any partial base
boundary fails. As soon as all three files exist on `master`, CI always executes
the base copies and there is no head fallback. The two executable guard files
must also remain byte-identical to their base versions, while the target
workflow must remain byte-identical to its embedded canonical definition. The
guard rejects changing or removing any of the three files, so an ordinary PR
cannot mutate the root of trust or reset the repository into bootstrap mode.
Any future guard-boundary update requires an explicitly administered
root-of-trust transition outside the ordinary pull-request lane. Both guard
workflows rerun on `edited` as well as opened, reopened, and synchronized pull
request activity.

A publishable package cannot be removed, moved, renamed, or made private in the
same PR that attempts to version it: Changesets cannot consume an entry for a
package absent from the head release graph. Stage removal instead:

1. keep the package at the same public root and name and release its final
   deprecation changeset;
2. after that Version Packages PR is merged, use a policy-only PR to add the
   exact package name to `.changeset/config.json` `ignore` and set only
   `private: true`, preserving its root, name, version, source, and all other
   release policy; and
3. only after that retirement policy is present on the base branch, remove the
   private package and its exact ignore entry together in a later policy PR.

The only active-package version-bump exception is the generated **Version
Packages** PR. CI
recognizes it only when `peerbit-org` authored the same-repository
`changeset-release/master` branch, the event sender is also the exact
`peerbit-org` account (GitHub user ID `273107789`), the base is exactly the
current `master` SHA, and the diff contains generated artifacts only. It must
consume the exact pending base changeset set, apply each
direct semver bump exactly, reproduce the deterministic transitive runtime and
peer-dependency cascade, preserve old changelog history, and either leave the
lockfile unchanged or make only deterministic `package@old` to `package@new`
lockfile projections. The lockfile projection is checked even when
`pnpm-lock.yaml` is omitted from the reported diff. Existing changelog history
must remain the exact terminal tail beneath a generated prefix with exactly one
H1 title and exactly one ATX H2 for the expected version. Extra H1 titles,
Setext H1/H2 sections, copied, commented-out, duplicated, or unclosed-fenced
history are rejected, as is raw HTML in newly generated release content that
could wrap or hide the preserved structure. Headings inside a properly closed
CommonMark code fence are treated as code rather than changelog structure;
because backticks are forbidden in a backtick fence's info string, such a line
cannot hide a following heading. A lookalike PR, stale branch, arbitrary extra
bump, or one containing source code is rejected. `@peerbit/test-lib` remains
deliberately ignored/frozen under the current policy; this guard does not change
its publication status. The guard requires non-empty, generator-safe changeset
summaries and preserved changelog history, but it does not prove that generated
changelog prose is identical to the authored summary; that review still relies
on the exact bot identity and human inspection of the generated PR.

## The Version Packages PR

On every push to `master`, the `Release` workflow runs `changesets/action`,
which opens (or updates) a bot pull request titled **"chore: version
packages"**. That PR accumulates all pending changesets: it applies the version
bumps to each `package.json`, updates the `CHANGELOG.md` files, and deletes the
consumed `.changeset/*.md` files. As more changesets land, the PR keeps
updating itself.

Review that PR like any other. Nothing is published while it is open.

> [!NOTE]
> The Version Packages PR is opened with `RELEASE_PR_TOKEN`, authenticated as
> `peerbit-org`, so it triggers downstream CI. The release workflow verifies
> that bot identity before passing the step-scoped token to `changesets/action`;
> checkout itself retains only the read-only workflow credential.

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

After each successful publish command, the publisher verifies that the exact
package version is visible on npm. A missing version is retried with nearly
eight minutes of bounded scheduled backoff, excluding registry query time, to
accommodate npm processing and propagation. Authentication, network, and other
unexpected registry errors are not retried. If the version remains absent, the
release fails before publishing the next package.

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

The published-package security smoke retains a Node 18 compatibility check for
`@peerbit/crypto`, even though the repository toolchain itself requires Node
22 or newer. CI and release jobs provision Node 18.20.8 explicitly. To run
`pnpm run release:security-gate`, `pnpm run release`, or `pnpm run release:rc`
locally from a newer Node runtime, set `PEERBIT_NODE18_EXECUTABLE` to the
absolute path of a real Node 18 executable. The gate validates its major
version before doing any package work and fails closed when it is absent or
incorrect; it never downloads a runtime implicitly through npm.

CI runs the packed-consumer, dependency-contract, and Node 18 crypto proofs on
both supported Node majors. Stable and release-candidate publication run those
same compatibility proofs, but do not call a live vulnerability-advisory API.
An advisory service outage therefore cannot turn an otherwise verified publish
into a false security failure. The packed-consumer proof still performs a real
npm installation, so registry package reads remain part of its installability
check.

## Published dependency advisories

`.github/workflows/published-dependency-advisories.yml` performs the one live
advisory scan: a production-only `npm audit` over a consumer composed from every
publishable package as an exact local tarball. It runs when dependency surfaces
change, after matching pushes to `master`, once per day, and on manual dispatch.
It is deliberately separate from stable and release-candidate publication.

The consumer is installed with `--legacy-peer-deps`, matching the release smoke,
so npm does not auto-install react-native-webrtc's optional React Native peer
toolchain. The scan asserts that react-native, metro, metro-config,
metro-transform-worker, image-size, and every `@react-native/*` package are
absent. It has no advisory ignores and does not scan workspace-only development
tools that are never published to users.

A structurally valid audit report containing any production vulnerability fails
the workflow. A recognized timeout, network failure, rate limit, or server-side
audit endpoint failure produces a visible `scanner unavailable` warning without
blocking publication. Malformed reports, authentication/configuration failures,
missing tooling, and internally inconsistent results still fail closed. This
keeps a real finding actionable without making npm's advisory-service uptime a
release dependency.

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
