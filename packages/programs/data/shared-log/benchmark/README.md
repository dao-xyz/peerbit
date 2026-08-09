# shared-log benchmarks

Integration benchmarks for the shared-log sync pipeline. They run real
networked sessions inside one process, so results measure the combined
sender+receiver pipeline and are more variable than pure algorithmic
benches — prefer comparing means over several runs.

The TypeScript `benchmark:*` harnesses in `package.json` run with
`node --loader ts-node/esm`; validation helpers use plain Node.js. Common
environment knobs are documented in each file's header. `BENCH_JSON=1`
switches every harness to machine-readable JSON on stdout.

## sync-peer-state.ts

Deterministic in-process microbenchmark for the state a per-peer state fold
would move. Every scenario runs at 1, 8, and 64 peers:

- `simple-dispatch-quota` captures and disposes the real Simple dispatch
  lifecycle while acquiring and settling one real coordinate-lookup quota
  permit per peer.
- `rateless-target-lifecycle` captures the real Rateless target lifecycle,
  finishes dispatch while process/response retention is live, then releases
  that retention and disposes the target sets.
- `disconnect-reconnect-retained-physical` leaves real Simple physical
  permits unsettled for the full timed sample while repeatedly creating and
  disconnecting successor logical rows. It retains one per peer through 32
  peers and saturates the real global cap of 32 for the 64-peer case. The
  permits are settled after timing so the benchmark process itself does not
  leak.

The workload is fixed and has no network, storage, timers, randomness, or
generated identities. That makes it the primary performance signal for a
state-fold change; the networked benchmarks remain useful corroboration, not
the sole gate.

Capture baseline and candidate results from their intended revisions on the
same otherwise-idle host with the exact same Node 22 runtime. The gate-eligible
configuration is exactly the default: peers `1,8,64`, three warmups, 15
measured samples, and 500 iterations. The exact confidence calculation caps
measured samples at 1,000:

```bash
# Run at the baseline revision.
BENCH_JSON=1 pnpm --silent --filter @peerbit/shared-log run benchmark:sync-peer-state > baseline.json
# Switch to the candidate revision (or copy a second worktree's result here).
BENCH_JSON=1 pnpm --silent --filter @peerbit/shared-log run benchmark:sync-peer-state > candidate.json
pnpm --filter @peerbit/shared-log run benchmark:sync-peer-state:compare "$PWD/baseline.json" "$PWD/candidate.json"
```

The comparison ratchet fails only when both conditions hold for a matching
scenario/peer count: candidate median round time is more than 5% slower and
the candidate's distribution-free 95% median confidence interval is wholly
more than 5% above the baseline interval. An above-threshold point estimate
without that evidence is inconclusive, as is any noncanonical smoke/custom
configuration; the CLI returns nonzero for both. Mismatched exact
Node/V8/CPU/platform/architecture, run configuration, task sets, sample counts,
or deterministic workload counters make the comparison invalid instead of
silently passing. This deliberately avoids treating a single noisy timing as
proof. Correctness/lifecycle invariants stay gated by the focused sync test
suites.

## network-preset-e2e.ts

End-to-end comparison of the native network preset (`peerbit/rust`: rust
core stream + wire-sync receive fusion + native shared-log defaults) against
the all-default TS client. Two real `Peerbit.create` nodes connected over
TCP on 127.0.0.1, mirroring `test/network-e2e-native.spec.ts`:

- `cold-sync` — N pre-written independent entries, time to full convergence
  on the joining peer (entries/s). Also reports the sender-side
  `simple.exchangeHeads` share of wall time: if the unfused outbound path
  dominated, receive-side wins would be hidden (it does not, at the default
  sizes — see the metric).
- `live-puts` — sustained awaited puts on peer1; peer2-visible throughput
  and per-put visibility latency p50/p95 (attributed by polling the
  receiver's log length, so it is an in-order approximation).
- `stash-pressure` — native leg only. A large-payload burst pushed through
  while the receiver's program dispatch is gated (held and replayed in
  arrival order), the stalled-consumer situation the wire-sync stash FIFO
  caps (512 messages / 64 MB) exist for. In a normal 2-node run the inbound
  wire stays in lockstep with the consumer, so the caps are unreachable
  without the gate. The run asserts convergence — evicted stash entries are
  recovered through the TS RPC decode fallback and the synchronizer's
  retries — and reports commit-phase throughput below and above the
  eviction boundary.

Defaults: 2 warmup + 5 measured runs per leg, strictly sequential. Do not
run anything else (builds, tests, other benchmarks) concurrently.

Indicative shape of the results (one dev machine, defaults; absolute
numbers vary): cold-sync well above 2x entries/s for the native preset
with the sender-side send loop at ~2-3% of wall on both legs. Sustained
singleton live-puts run several times faster on the native preset with the
fused send: awaited puts coalesce into multi-entry raw frames serialized
inside wasm, which amortizes the receiver's per-message fixed costs that
previously made this leg ~0.4-0.9x of the default (one instrumented run on
one dev machine: ~6x default throughput with ~7x lower p50 visibility
latency; before send fusion the same machine measured ~0.9x). Above the
stash cap the commit phase pays ~4-5x in MB/s versus just below it
(eviction fallback + the synchronizer's retry duplicates) while still
converging on every run.

```
cd packages/programs/data/shared-log
pnpm run benchmark:network-preset-e2e
NET_BENCH_SCENARIOS=cold-sync NET_BENCH_LEGS=native pnpm run benchmark:network-preset-e2e
```
