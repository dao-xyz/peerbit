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
   not yet on npm (`from-package` semantics) through npm trusted publishing
   (see "npm authentication" below), and
4. creates the `<name>@<version>` git tags (e.g. `peerbit@5.2.21`,
   `@peerbit/crypto@3.2.0`) with `pnpm exec changeset tag` and pushes them. See
   "Git tags and the format change" below — this is a new tag namespace, not the
   old `<component>-v<version>` one.

The publisher verifies that every exact package version it hands to npm becomes
visible on the registry. npm can take more than twenty minutes to process an
upload, so versions of packages that already exist on npm are all published
first and then verified together within one shared window: 60 minutes, polled
every 30 seconds, overridable with `PUBLISH_VERIFY_TIMEOUT_MS` and
`PUBLISH_VERIFY_POLL_MS`. A brand-new package is verified straight away, before
anything after it is published, so a package that npm silently refuses to create
never has dependents shipped against it. A publish error fails the release
immediately. The one exception is npm rejecting an upload of a version it is
still processing from an earlier run; that version is verified with the rest. A
version that never appears fails the release and names every missing version.
Because existing packages are verified in batch, a dependent can be published
before an updated dependency is visible; if that dependency never lands, the
failed release reports it and it needs a new version.

The downstream `Post Release Automation` workflow then restores the
`workspace:*` protocol (a no-op with changesets, which preserves it) and, when
`@peerbit/server` changed, opens the bootstrap rollout PR.

## npm authentication

Both publishing jobs in the `Release` workflow, `release` and `release-rc`,
publish with npm
[trusted publishing](https://docs.npmjs.com/trusted-publishers): npm exchanges
the job's GitHub OIDC token for a short-lived token for the package being
published, so a release needs no long-lived npm token. This requires:

- a trusted publisher on every package the job publishes (on npmjs.com, the
  package's **Settings** → **Trusted Publisher**): GitHub Actions, organization
  `dao-xyz`, repository `peerbit`, workflow filename `release.yml`, environment
  `npm-release`, and `npm publish` selected under **Allowed actions**. Trusted
  publishers created after 2026-09-03 allow only `npm stage publish` until
  `npm publish` is selected as well.
- `id-token: write` on both jobs. Both must keep running in the `npm-release`
  environment, which the trusted publisher names, on GitHub-hosted runners; npm
  does not support self-hosted runners.
- npm 11.5.1 or newer on Node 22.14.0 or newer. Both jobs install Node `24.x`,
  whose bundled npm qualifies from 24.5.0 on. `pnpm publish`, which
  `aegir release-rc` also uses, packs each package and uploads it with the npm
  next to the Node running pnpm.

Because the repository is public, npm also attaches a provenance statement to
every version published this way, and it rejects the upload unless the
package's `repository.url` names `https://github.com/dao-xyz/peerbit`. Every
public package must therefore keep its `repository` field.

A trusted publisher is added in a package's settings on npmjs.com, and a package
has settings only once it exists. Publish the first version of a new package
with a token or by hand, then add its trusted publisher.

A new trusted publisher expires unless a publish uses it within 2 days; an
expired one has to be deleted and created again. Before merging a Version
Packages PR, run `npm view <name> _npmUser.trustedPublisher.id` for every
package whose version it bumps. It prints `github` when the latest version was
published through trusted publishing, and nothing otherwise. For each package
that prints nothing, add its trusted publisher if it is missing, make sure
`NPM_TOKEN` is unset (see below), and merge the PR within 2 days so that the
release validates the trusted publisher.

After that release, `npm view <name>@<version> _npmUser.trustedPublisher.id`
should print `github` for each of those packages. If it prints nothing, the
trusted publisher did not match and `NPM_TOKEN` published the version instead:
recreate the trusted publisher, unset `NPM_TOKEN`, and release the package again
within 2 days.

If a publish fails with `ENEEDAUTH` (or, with a fallback token set, with the
token's rejection, such as `E404 Not Found - PUT`), the package has no matching
trusted publisher. Fix that as above, then run the `Release` workflow with
publish mode **stable** (see "Manual publishing and release candidates"). It
publishes only the versions that are not on npm yet, then creates the tags.

The `NPM_TOKEN` secret in the `npm-release` environment is an optional
fallback. When it is set, npm publishes with it only if the OIDC exchange fails,
and the from-package publisher's `npm view` checks send it too. Leave it unset
unless a release must publish the first version of a new package. A token that
can still publish hides a missing or mismatched trusted publisher: npm publishes
with the token instead, without provenance, and leaves a new trusted publisher
unvalidated until it expires. Tokens that bypass 2FA lose `npm publish` in a
change targeted for January 2027
([GitHub changelog](https://github.blog/changelog/2026-07-31-restricting-npm-bypass-2fa-granular-access-tokens/)).

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
  publish prerelease versions of every public package, so each one needs a
  trusted publisher (see "npm authentication"). That includes
  `@peerbit/test-lib`, which never appears in a Version Packages PR: if it has
  none yet, add it right before dispatching a release candidate, since a new
  trusted publisher expires unless a publish uses it within 2 days.

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
so npm does not auto-install react-native-webrtc's required transitive React
Native peer toolchain. This peer-pruned fixture does not prove the dependency
closure of a default npm consumer install. The unused Node-side subtree remains
tracked in [Peerbit #1320](https://github.com/dao-xyz/peerbit/issues/1320), blocked
by [libp2p #3609](https://github.com/libp2p/js-libp2p/issues/3609).
The scan asserts that react-native, metro, metro-config,
metro-transform-worker, image-size, and every `@react-native/*` package are
absent. It has no advisory ignores and does not scan workspace-only development
tools that are never published to users.

`@peerbit/vite` takes Vite as a peer dependency: the plugin only runs inside an
application's own Vite, so applications install Vite themselves. The scanned
consumer does the same. Its only `devDependencies` entry is `vite` with the
packed plugin's peer range, so npm installs the newest Vite the plugin admits,
and that Vite and the packages only it needs are outside the production audit.
The scan asserts this boundary rather than assuming it: no packed manifest
lists Vite in `dependencies` or `optionalDependencies`, the packed plugin keeps
its required peer, and the lock holds exactly one Vite, marked as a development
dependency and reached only from the consumer and the plugin's peer edge. As
with the React Native peer, this does not describe a default npm install: there
a production dependency's peer counts as production, so applications keep
`@peerbit/vite` with `vite` in `devDependencies`. Advisories in Vite and its own
toolchain therefore belong to the application's development audit, not to this
scan. Each peer floor is the first release of its major with the fixes for every
Vite advisory published when the range was set. When Vite publishes another
security fix, raise the floor in `packages/clients/vite/package.json` and in
`scripts/test-release-security-contracts.mjs`, with a `@peerbit/vite`
changeset. The workspace tests run on the oldest admitted Vite line, and the
full release smoke builds a project with the packed plugin on the newest.

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
