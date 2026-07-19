# File-share benchmark evidence

Run a single checkout with `pnpm bench:file-share:local` or compare pinned
Peerbit revisions with `pnpm bench:file-share:matrix`.

Result schema v11, summary and matrix-summary schema v7, and invocation schema
v6 bind each result to the exact effective benchmark inputs. Invocation v6
binds the reader chunk-persistence policy and the actual browser-storage cohort
in addition to the v5 `postTransferSoakMs` input. Uploads default to 60,000 ms
and preserve an explicit
`--post-transfer-soak-ms 0` for
fast smoke runs, while seeder probes resolve zero and reject a nonzero soak
because they have no transfer. The value is exported as
`PW_POST_TRANSFER_SOAK_MS`. Result schema v11 defines
`errorCount` as every uncaught browser `pageerror`,
every browser `console.error`, every console message at any level that contains
a declared Peerbit failure signature, and scenario-recorded operation failures.
Each result embeds the exact `errorCollectionDefinition` and signature list.
Playwright `requestfailed` events are retained separately under
`requestFailures`; they are diagnostics and are not automatically fatal because
peer-to-peer discovery can legitimately exercise failed network candidates.

Passed schema v11 upload results require `writerDiagnostics.lastUploadDiagnostics`
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

`--browser-storage memory|opfs` selects Peerbit's own writer and reader backing
stores and defaults to `memory`. This is independent of `--download-sink`: the
former controls where Peerbit stores blocks and indexes, while the latter
controls what consumes the completed downloaded byte stream. Every upload cell
uses two separate persistent Chromium processes and fresh profile directories,
including memory cells, so context topology is held constant. The page-init
hook configures `PeerProvider` before it starts. Four resource checkpoints must
then prove the requested mode, whether a Peerbit directory was configured, and
the measured `persisted()` state of Peerbit storage, block storage, and indexer.
An OPFS label is rejected unless all three Peerbit components report persistent;
`navigator.storage.persisted()` is recorded separately because eviction
protection can be false even when OPFS is available and in use.

`readTransfer.receiverProgress` keeps three receiver boundaries separate over
the exact 0%, 5%, …, 100% milestone set.
`available` means a contiguous file prefix was materialized and available to
the receiver library (`chunkMaterializeFinishedAt`). `peerbitDurable` means the
exact signed manifest-entry blocks for a contiguous prefix were confirmed in
the receiver's local Peerbit block store (`chunkPersistenceConfirmedAt`), with
the confirmation sources counted separately. Observer/non-persisting reads
therefore make no `peerbitDurable` claim. This is local Peerbit persistence
evidence, not remote replication, remote acknowledgement, filesystem `fsync`,
or stable-media proof.

`sinkAccepted` means a contiguous prefix was accepted by the configured
benchmark sink (`chunkWriteFinishedAt`) and always carries `durable: false`. For
the hash-only sink it means the bytes were accepted by the streaming
SHA-256/CRC-32 consumer; for OPFS or Node-file it means the configured sink write
resolved. The later persisted readback is a whole-file benchmark integrity gate,
but the `sinkAccepted` milestones themselves are neither Peerbit nor filesystem
durability evidence. None of these three series is a wire acknowledgement or
proof that another peer retained the bytes.

Schema v11 upload results record the `download-memory-v3` bounded telemetry
contract. After any controlled-locality prefix stabilization, the harness arms
serial samplers before the bounded pre-read transport-counter gate and timed
click. Sampling remains armed through sink completion, integrity and terminal
topology checks, the requested post-transfer soak, final diagnostics, and
explicit writer/reader Peerbit shutdown. Cleanup then forces the terminal sample
before detaching the CDP sessions. The default soak is 60 seconds, so retained
memory and storage that keep growing or fail to settle are visible separately
from transfer-time peaks.

Result v11 records bounded `shutdownOutcomes` for writer and reader separately;
a passed upload requires both shutdown hooks to prove that the program closed
and the Peerbit/libp2p peer stopped. Each shutdown outcome repeats the exact
program, peer, and per-page session identity captured by that role's four
resource snapshots, preventing a stale or replaced hook from satisfying the
gate. The memory terminal sample is captured only after both hooks finish,
allowing the live-peer after-soak checkpoint and the post-shutdown endpoint to
be distinguished.

Reader and writer renderers use CDP `Runtime.getHeapUsage`. Every sample records
V8 used and total bytes, embedder heap used bytes, and backing-storage bytes.
Host RSS combines the deduplicated processes from both Chromium instances,
grouped by Chromium process role,
with the Playwright worker Node process; for local runs that Node value also
includes the in-process bootstrap peer. Chromium RSS cannot be assigned
reliably to one page and RSS is not PSS or USS, so page-level comparisons should
use the renderer series. Node `external` and `arrayBuffers` are recorded as
overlapping allocation diagnostics; neither is added to Node or combined RSS.
Every accepted upload result must prove that exactly two unique browser CDP
sessions exposed two distinct Chromium root PIDs and that both root processes
were present in every host-RSS sample.
Samples run serially, scheduling the next periodic read five seconds after the
previous read completes, and reserve three endpoints: the initial sample, one
live manual checkpoint immediately after the exact requested soak timer and
before the `afterSoak` resource capture, and one terminal checkpoint after peer
shutdown. For both the transfer and soak phase, passed evidence must have a live
sample at or before the phase start and another at or after the phase finish,
with no adjacent live-sample gap above the exact five-second interval plus the
four-second sampling-operation deadline plus the recorded scheduling tolerance.
Consequently a short phase may validly have no periodic sample, while a long
phase cannot pass on endpoint samples alone. Capacity is derived from the full
Playwright lifecycle timeout, not just the download deadline, and is capped at
4,096 entries per series. Premature exhaustion is explicit and fatal. Passed
evidence requires all three ordered, error-free checkpoint kinds plus bounded
coverage of the canonical transfer, soak, and shutdown window. The validator
recomputes all start, end, peak,
process-role, and combined RSS summaries and rejects reordered, oversized,
partial, or contradictory telemetry. Failed runs retain whatever bounded
telemetry was collected, but are never accepted as performance evidence.

Every CDP sample and setup operation has a four-second deadline; sampler
cleanup has a nine-second aggregate deadline, late-created CDP sessions are
detached best-effort, and a timed-out probe becomes terminal so another sample
cannot overlap it. Cleanup timeouts are bounded warnings, while setup, sampling,
and non-timeout cleanup errors remain fatal. Host samples are additionally
capped at 256 Chromium processes and 32 process-role groups.

Four resource checkpoints—`beforeTimedRead`, `afterSink`, `beforeSoak`, and
`afterSoak`—also record writer and reader storage and effective runtime state.
Resource-evidence schema v2 derives four later-minus-earlier intervals:
`timedReadEnvelope` is `beforeTimedRead`→`afterSink`, `postTransferWork` is
`afterSink`→`beforeSoak`, `soak` is `beforeSoak`→`afterSoak`, and `total` is
`beforeTimedRead`→`afterSoak`. `timedReadEnvelope` deliberately includes the
bounded pre-read and post-read transport-counter gates around the primary timed
read, so it must not be mislabeled as the exact download window. The exact
memory phase remains `downloadStartedAt`→`downloadFinishedAt`. The resource
`soak` interval begins at the dedicated `beforeSoak` checkpoint after integrity
and terminal-topology work, while the memory soak phase uses the exact requested
soak timestamps. Because `afterSoak` is intentionally captured only after the
manual memory checkpoint completes, the resource `soak` interval envelopes the
requested timer plus that bounded checkpoint operation; it is not an exact
timer-duration metric.

Storage intervals report Peerbit logical-log usage and browser origin-wide
`navigator.storage` estimates; these are distinct scopes and should not be
added together. Runtime evidence preserves each page's effective
`nativeGraph.active`/`useHeads`, eager-cache enabled/availability state and
limits, and root/node pubsub upload limits. Writer and reader configurations are
kept separate and normalized as one ordered writer/reader pair. Each role must
retain one browser origin and one exact program/peer/session identity across all
four snapshots; the roles must share the program address while exposing distinct
peer IDs, peer hashes, and session IDs. Upload timing
rows are split by that pair in addition to reader locality. A runtime cohort key
is emitted only when every passed repetition has the same complete pair; mode
comparisons return no value when pairs differ or runtime-evidence availability
is mixed. If every result predates runtime evidence, the legacy grouping and
comparison shape is preserved. When eager telemetry is enabled, aggregate
summaries include after-soak current entries, bytes, pending entries, and
pending bytes; lifetime peaks; and `timedReadEnvelope`, `postTransferWork`,
`soak`, and `total` deltas for admitted blocks, hits, evictions, expirations, and
every rejection category.

Aggregate upload summaries expose this evidence under `runtimeEvidence`.
`memoryPhases` reports the exact transfer and soak peak and end-minus-start
distributions for renderer used/embedder/backing storage, combined host RSS, and
Node external/ArrayBuffer bytes. Because telemetry is sampled every five
seconds, each phase uses the last live sample at or before its start through the
first live sample at or after its end and records that boundary-overhang
definition. The same recorded maximum-gap contract bounds the boundary
overhang and every adjacent captured-sample gap. Terminal post-shutdown samples
are excluded from both transfer and soak summaries; the required live manual
checkpoint after soak supplies the soak endpoint without folding teardown into
the phase.
`resourceStorageDeltas`, `effectiveRuntimeConfiguration`, and `eagerCache`
retain the corresponding storage, effective-policy, and eager-cache summaries.
Older result objects that do not carry v11/v3 resource evidence retain the
pre-existing aggregate output shape without empty placeholder fields.

For a controlled reader-locality download cohort, run an upload with
`--mode fixed1`, `--reader-local-chunk-target <N>`, and
`--reader-local-chunk-max-overshoot <M>`, and
`--reader-terminal-topology <observer|replicator>`. Add
`--reader-persist-chunk-reads false` for the transient-read cohort; omitting it
preserves the historical persistent-read behavior. The three locality options
are required together, `M` is capped at eight chunks, and this profile uses a one-replicator
readiness baseline. The writer is fixed at factor one and the reader is an
observer before upload starts; topology evidence must show exactly one
replicator, with the writer in and reader out of that set, both before upload and
again immediately before the timed read. Both peers must report the same exact
singleton replicator identity, and that identity must be the writer.

After the normal upload and post-upload monitor finish, the harness applies the
requested reader chunk-persistence policy and imports exactly the first `N`
entry blocks
from the ready root manifest. This benchmark-only provisioning bypasses the
product stream read-ahead path, uses bounded batches of eight raw block
requests, retains every imported head under its exact `${file.id}:${index}`
document identity so adaptive pruning cannot classify a current manifest entry
as stale, and applies one aggregate download deadline. It asserts that the
only local manifest-entry heads afterward are indices `0..N-1` and that no
active transfer or queued download work remains. `N=0` with persistence enabled
is the cold observer-persistent cohort and imports no entry block. Transient
reads require `N=0` and terminal `observer`; they must retain zero manifest-entry
blocks and zero local chunk index rows after the timed read. The harness then records
three identical exact locality samples, spaced by
`min(--poll-ms, 100ms)`. Each sample includes both the manifest-entry blocks
available in the local block store (`K`) and the local Documents index rows
(`J`). Both sets must be contiguous prefixes, `N <= K <= N + M`, `J <= K`, and
`K` must remain smaller than the file's full chunk count. The direct provisioning
step itself requires `K=N` with zero speculative imports; `M` only bounds any
unexpected locality change before the timed read. A cached entry block need not
have a local index row yet.

The measured cohort key is therefore
`observer-<persistent|transient>-<memory|opfs>-prefix-b<K>-i<J>`, not merely the
requested target. Timed
read diagnostics must report those exact initial block and index-row counts for
persistent reads. The product intentionally skips those count queries for a
transient read, so its timed-read diagnostic fields remain null while the
independent pre-download and terminal observations must each prove exact zero
blocks and index rows.
After download-memory telemetry is armed, a bounded pre-read gate samples
writer and reader topology in parallel every 100 ms for at most five seconds.
It requires three consecutive samples in which the writer's outbound and the
reader's inbound TopicControlPlane pubsub stream key sets and per-key byte
counters are unchanged (with 1 ms of timestamp quantization tolerance). The
final accepted sample becomes
`writerTopologyBeforeTimedRead` and `readerTopologyBeforeTimedRead`; the timed
click must start within the recorded one-second handoff tolerance. This excludes
late preload control traffic from the measured counter delta.

Immediately after sink completion is observed, while download-memory sampling
remains armed, the same bounded gate captures post-read topology before integrity
readback, idle waiting, terminal-topology stabilization, or the transport's
ten-second inbound idle pruning. Its deadline is capped so an accepted gate
finishes no later than nine seconds after sink completion, leaving one second
of pruning margin even when memory-telemetry shutdown is delayed; the actual
completion-to-gate-finish delay is recorded and validated. The final accepted
sample becomes
`writerTopologyAfterTimedRead` and `readerTopologyAfterTimedRead`, so later
convergence cannot erase the transport diagnostics visible at the end of the
measured read. Both gates require exact counterpart peer hashes and peer IDs,
the TopicControlPlane service and negotiated protocols, raw-counter/stream
identity, and a unique live connection identity. They reject aborted outbound
streams, duplicate audit keys, decreasing counters for an unchanged local key
set, more than 1 MiB of aggregate writer/reader byte skew, malformed counters,
capture errors, and deadline overruns. Each local audit key includes service,
remote peer hash and ID, direction, connection and stream IDs, multiplexer, and
protocol. Across the timed read, each endpoint must preserve its own exact key
set and every post-read counter must be at least its pre-read value; only the
writer-outbound and reader-inbound total deltas are compared, with at most 1 MiB
of skew. Local stream, connection, and multiplexer identifiers are not compared
across endpoints because libp2p assigns them independently. Every completed
observation is retained in the result, including on a later failure. A valid
post-read checkpoint also preserves the original two peer identities, retains
the writer in the replication set, and contains either the writer
singleton or the exact writer+reader pair.

After integrity verification and an idle transfer scheduler, a persistent-read
cohort requires every manifest-entry block to be local; a transient-read cohort
requires none to be local. The explicit
terminal expectation keeps historical and fixed implementations in separate,
fail-closed cohorts: `observer` requires zero local chunk index rows and the
writer as the exact singleton replicator; `replicator` requires the complete
chunk index prefix and the exact writer+reader replication pair. The harness
collects three stable topology samples outside the measured download duration;
both peer identities must match the pre-upload and pre-read checkpoints. This
terminal endpoint evidence does not claim the reader held one role continuously
during the timed window. Final diagnostics must report the requested replicator
count and one locally indexed root on both peers (`replicationSetSize=1`).
Non-convergence fails the run rather than creating or mixing an implicit
terminal-topology cohort.

Repeated standalone and matrix results are grouped by the exact key, so timings
from different read-ahead outcomes are never averaged together. Raw canonical
per-chunk timings remain in each result and can be rebucketed into 5% progress
windows without losing the underlying samples. Without the paired locality
options, reader chunk persistence is not changed by this control and its
locality evidence fields remain null.

Schema v11 records the exact versioned `seederDropPolicy` and a final numeric
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
