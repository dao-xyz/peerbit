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

For a controlled reader-locality download cohort, run an upload with
`--mode fixed1`, `--reader-local-chunk-target <N>`, and
`--reader-local-chunk-max-overshoot <M>`. The two locality options are a required
pair, `M` is capped at eight chunks, and this profile uses a one-replicator
readiness baseline. The writer is fixed at factor one and the reader is an
observer before upload starts; topology evidence must show exactly one
replicator, with the writer in and reader out of that set, both before upload and
again immediately before the timed read. Both peers must report the same exact
singleton replicator identity, and that identity must be the writer.

After the normal upload and post-upload monitor finish, the harness enables
persistent reader chunk reads and performs an untimed sequential prefix preload
that yields exactly `N` chunks. A nonzero preload has one aggregate download
deadline enforced through an abort signal, rather than resetting the deadline
for every chunk. It explicitly closes the iterator within the bounded cleanup
tolerance and requires no active transfer or queued download work afterward.
`N=0` is the cold observer-persistent cohort and opens no preload stream. The
harness then records three identical exact locality samples, spaced by
`min(--poll-ms, 100ms)`. Each sample includes both the manifest-entry blocks
available in the local block store (`K`) and the local Documents index rows
(`J`). Both sets must be contiguous prefixes, `N <= K <= N + M`, `J <= K`, and
`K` must remain smaller than the file's full chunk count. `K` can exceed `N`
because Peerbit may read ahead; a cached entry block also need not have a local
index row yet.

The measured cohort key is therefore
`observer-persistent-prefix-b<K>-i<J>`, not merely the requested target. Timed
read diagnostics must report those exact initial block and index-row counts.
Here, `observer` describes the upload and timed-read **start** topology, not the
entire persistent transfer. A persistent timed read is expected to promote the
reader into replication. After sink completion, integrity verification, and an
idle transfer scheduler, the harness collects three additional stable topology
samples outside the measured download duration. Both peers must then report the
same exact sorted `[writer, reader]` replicator set, both must report themselves
in that set, and the peer identities must match the pre-upload and pre-read
checkpoints. Non-convergence fails the run rather than creating or mixing an
implicit terminal-topology cohort.

Repeated standalone and matrix results are grouped by the exact key, so timings
from different read-ahead outcomes are never averaged together. Raw canonical
per-chunk timings remain in each result and can be rebucketed into 5% progress
windows without losing the underlying samples. Without the paired locality
options, roles, persistence policy, and seeder-drop gates are unchanged.

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
