# File-share benchmark evidence

Run a single checkout with `pnpm bench:file-share:local` or compare pinned
Peerbit revisions with `pnpm bench:file-share:matrix`.

Result schema v7 defines `errorCount` as every uncaught browser `pageerror`,
every browser `console.error`, every console message at any level that contains
a declared Peerbit failure signature, and scenario-recorded operation failures.
Each result embeds the exact `errorCollectionDefinition` and signature list.
Playwright `requestfailed` events are retained separately under
`requestFailures`; they are diagnostics and are not automatically fatal because
peer-to-peer discovery can legitimately exercise failed network candidates.

Passed schema v7 upload results require `writerDiagnostics.lastUploadDiagnostics`
to contain exactly 21 bounded progress milestones from 0% through 100% in 5%
increments. Each point records the aggregate bytes whose application-level
chunk puts completed, using the library upload clock and exact ceil-rounded byte
target. This is local chunk-commit throughput, not wire-byte progress, remote
acknowledgement throughput, or a contiguous source prefix. Concurrent puts may
finish out of index order, and one completion may cross multiple milestones.
The 0% point includes manifest/startup overhead; the 100% point precedes the
ready-manifest commit. The validator binds the series to the canonical file,
requires a clean completed upload lifecycle, and rejects missing, partial,
unbounded, stale, reordered-byte, or contradictory passed evidence. Failed
runs may retain a bounded partial milestone prefix for diagnosis but are never
accepted as performance evidence.

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

Schema v7 upload results also record bounded download-window memory telemetry. After any
controlled-locality stabilization, the harness arms serial samplers immediately
before the timed click and forces a final sample as soon as the selected sink
completion is observed, before integrity readback or terminal-topology checks.
Reader and writer renderer JavaScript heaps use CDP `JSHeapUsedSize` samples.
Host RSS combines all Chromium processes, grouped by Chromium process role,
with the Playwright worker Node process; for local runs that Node value also
includes the in-process bootstrap peer. Chromium RSS cannot be assigned
reliably to one page and RSS is not PSS or USS, so page-level comparisons should
use the renderer heap series. Samples run serially every five seconds, include
forced initial/final endpoints, reserve one endpoint slot, cap each series at
4,096 entries, and cap sampling errors. Passed evidence requires at least two
error-free samples in every series and coverage of the canonical library read
window. The validator recomputes all start, end, peak, process-role, and combined
RSS summaries and rejects reordered, oversized, partial, or contradictory
telemetry. Failed runs retain whatever bounded telemetry was collected, but are
never accepted as performance evidence.

Every CDP sample and setup operation has a four-second deadline; sampler
cleanup has a nine-second aggregate deadline, late-created CDP sessions are
detached best-effort, and a timed-out probe becomes terminal so another sample
cannot overlap it. Result validation binds every series to the actual click and
completion-observation timestamps: setup may begin at most 30 seconds before
the click, and the terminal sample plus cleanup must finish within 30 seconds
after completion is observed. Host samples are additionally capped at 256
Chromium processes and 32 process-role groups.

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

Schema v7 records the exact versioned `seederDropPolicy` and a final numeric
`terminal` seeder snapshot after download and integrity verification. Every
upload cohort also records top-level `integrityVerifiedAt`: it remains `null`
until the aggregate size, SHA-256, CRC-32, manifest, and persistence gates have
succeeded. The final snapshot must be at or after that timestamp and, for a
controlled-locality cohort, at or after terminal-topology completion. The
validator recomputes both drop flags from the ordered snapshots. A single
below-baseline sample followed by recovery remains visible through
`droppedSeeders: true` but does not invalidate an otherwise valid run. Two
consecutive below-baseline samples, or a below-baseline terminal sample, set
`unexpectedSeederDrop: true` and fail the run. Missing policy evidence,
contradictory flags, and missing or non-final terminal snapshots fail closed.

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
Upload failures likewise use a null/false/null integrity projection unless the
browser result contains internally consistent integrity evidence. A completed
browser integrity gate is projected with its original timestamp and is
revalidated; malformed fields cannot be promoted into canonical evidence.

The matrix runner also reads nonzero sub-run summaries, keeps their failure
evidence in the matrix summary, continues later invocations, and exits nonzero
after writing the aggregate matrix summary when any invocation failed. Its
per-variant and cross-variant comparisons use the same complete-plan gate.
