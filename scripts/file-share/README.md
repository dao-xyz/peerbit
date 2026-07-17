# File-share benchmark evidence

Run a single checkout with `pnpm bench:file-share:local` or compare pinned
Peerbit revisions with `pnpm bench:file-share:matrix`.

Result schema v3 defines `errorCount` as every uncaught browser `pageerror`,
every browser `console.error`, every console message at any level that contains
a declared Peerbit failure signature, and scenario-recorded operation failures.
Each result embeds the exact `errorCollectionDefinition` and signature list.
Playwright `requestfailed` events are retained separately under
`requestFailures`; they are diagnostics and are not automatically fatal because
peer-to-peer discovery can legitimately exercise failed network candidates.

The standalone runner continues counterbalanced repetitions after an individual
Playwright or result-validation failure when setup remains usable. Once
invocation execution begins, it writes a summary with planned, completed,
passed, and failed counts, then exits nonzero if any repetition failed. A
preflight, dependency-installation, or build failure can still stop before that
summary exists. If browser evidence is missing or malformed, the synthetic
failure uses `errorCount: null` and
`errorCollectionComplete: false` instead of claiming that zero errors occurred.

The matrix runner also reads nonzero sub-run summaries, keeps their failure
evidence in the matrix summary, continues later invocations, and exits nonzero
after writing the aggregate matrix summary when any invocation failed.
