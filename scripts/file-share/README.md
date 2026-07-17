# File-share benchmark evidence

Run a single checkout with `pnpm bench:file-share:local` or compare pinned
Peerbit revisions with `pnpm bench:file-share:matrix`.

Result schema v4 defines `errorCount` as every uncaught browser `pageerror`,
every browser `console.error`, every console message at any level that contains
a declared Peerbit failure signature, and scenario-recorded operation failures.
Each result embeds the exact `errorCollectionDefinition` and signature list.
Playwright `requestfailed` events are retained separately under
`requestFailures`; they are diagnostics and are not automatically fatal because
peer-to-peer discovery can legitimately exercise failed network candidates.

Upload benchmarks default to `--download-sink hash-only`, which verifies the
deterministic stream with library SHA-256 plus an independent browser CRC-32
while discarding each chunk. This keeps one loopback request and filesystem
write per 512 KiB chunk out of the standardized measurement cohort. Use
`--download-sink opfs` for browser-native persistence or
`--download-sink node-file` for the loopback Node-file diagnostic cohort.
`libraryStreamWallMs` is the authoritative primary download metric only within
one fixed sink cohort; `hash-only` is the standardized cohort used for revision
and replication-mode comparisons. Results also retain click-to-sink duration,
sink-write timing, and `sinkAwaitSubtractedDiagnosticMs`. The latter is only the
arithmetic wall time minus awaited writes: read-ahead can progress during those
waits, so it is overlap-sensitive and must not be interpreted as sink-free or
Peerbit-only time. The validator reconstructs every `readTransfer` field from
exact canonical per-chunk library series and binds those diagnostics to the
nonce-derived file name, manifest file ID, and click-to-sink time window.
Click-to-sink duration ends at the sink's recorded completion epoch, not when
Playwright later observes the resolved completion promise. OPFS and Node-file
cohorts require persisted size, SHA-256, and CRC-32 readback. Compare different
sinks only in separate benchmark sessions; aggregate comparison objects fail
closed on mixed sinks, and every passed result and aggregate labels
non-hash-only cohorts non-authoritative.

The standalone runner continues counterbalanced repetitions after an individual
Playwright or result-validation failure when setup remains usable. Once
invocation execution begins, it writes a summary with planned, completed,
passed, and failed counts, then exits nonzero if any repetition failed. A
mode comparison is emitted only when every planned repetition completed and
passed. A preflight, dependency-installation, or build failure can still stop
before that
summary exists. If browser evidence is missing or malformed, the synthetic
failure uses `errorCount: null` and
`errorCollectionComplete: false` instead of claiming that zero errors occurred.

The matrix runner also reads nonzero sub-run summaries, keeps their failure
evidence in the matrix summary, continues later invocations, and exits nonzero
after writing the aggregate matrix summary when any invocation failed. Its
per-variant and cross-variant comparisons use the same complete-plan gate.
