# Relay-node profiling — does a native rust relay scale better than the js relay?

**Why the relay workload is different from the receive path.** `PROFILING.md`
found the native transport gives little RECEIVE-side win because the shared-log
receive path is compute-bound on per-frame Ed25519 verify — the socket→wasm copy
it removes is < 1 % of per-message cost. A **circuit-relay-v2 relay node is a
different workload entirely.** A relay forwards opaque bytes between two peers
that cannot connect directly; it operates at the libp2p transport layer and
**never decodes or verifies the `/peerbit/*` payloads it forwards.** The Ed25519
cost that dominated the receive path is therefore ABSENT. Relaying is an
**I/O + concurrency** workload — pipe bytes socket→socket across many
simultaneous relayed connections — which is exactly where native tokio async is
expected to beat the JS event loop: lower per-connection overhead, no GC pauses
under load, and true multi-core concurrency instead of one event loop pinned to
one core.

This document tests that hypothesis with evidence, in two steps:

1. **The interop gate** (must pass before any A/B): can a native rust-libp2p
   relay actually *serve js-libp2p circuit-relay clients*? If not, a native
   relay for the js fleet is a non-starter and that is the verdict.
2. **The A/B scaling sweep** (only if the gate passes): the js
   `circuitRelayServer` (Path A) vs the native rust-libp2p relay (Path B),
   forwarding between the same js source/dest peer pairs under increasing
   concurrent-circuit load — the **scaling curve**, not a single number.

---

## STEP 1 — Interop gate: PASS

**Result: a native rust-libp2p 0.56 circuit-relay-v2 relay forwards real bytes
between two js-libp2p 3.3.4 circuit-relay clients, byte-exact, end to end.**

### What was proven

`src/bin/relay_node.rs` boots a native relay-server swarm
(TCP + WebSocket(+DNS) + Noise + Yamux + identify + **`libp2p::relay::Behaviour`
server**, `src/relay.rs`). `js/relay-gate.mjs` then, through **only** that relay:

1. boots two js-libp2p circuit-relay CLIENT peers (Peerbit node transport spec:
   `tcp() + circuitRelayTransport(client)`, noise, yamux, Ed25519, identify —
   `js/relay-common.mjs`), a DEST and a SOURCE;
2. DEST reserves on the native relay (`/p2p-circuit` in its listen set → it
   dials the relay and gets a HOP reservation);
3. SOURCE dials DEST **through** the native relay via
   `<relayAddr>/p2p-circuit/p2p/<destPeerId>`;
4. SOURCE streams a known 64 KiB payload over a `/relay-bench` stream on that
   relayed circuit; DEST reassembles it, **verifies it is byte-exact**, and
   echoes an ack;
5. the harness asserts both ends see a **limited/circuit connection**
   (`/p2p-circuit` in the remote address) and the payload arrived byte-exact.

### Evidence (one gate run)

js-side end-to-end proof (`js/relay-gate.mjs`):

```
RELAY_GATE=PASS relay=".../tcp/58678/p2p/12D3KooWGTSz…"
  src=12D3KooWDiqz… dst=12D3KooWHKXR…
  bytes_forwarded=65536 byte_exact=true relayed_circuit=true
```

native-relay-side transport-layer proof (`relay_node` stdout — the relay's own
`libp2p::relay::Event`s, independent of the js harness):

```
RESERVATION_ACCEPTED src=12D3KooWHKXR… renewed=false      # DEST reserved on the rust relay
RESERVATION_ACCEPTED src=12D3KooWDiqz… renewed=false      # SOURCE reserved on the rust relay
CIRCUIT_ACCEPTED     src=12D3KooWDiqz… dst=12D3KooWHKXR… live=1 total=1   # rust relay opened the js↔js circuit
CIRCUIT_CLOSED       src=12D3KooWDiqz… dst=12D3KooWHKXR… live=0 error=None # clean close after all bytes forwarded
```

`CIRCUIT_ACCEPTED` between two DISTINCT js peer ids, followed by a
`CIRCUIT_CLOSED … error=None` (clean — not `ConnectionAborted`), is the native
relay confirming at the transport layer that it forwarded a full js↔js circuit.

### Why interop works (protocol-level)

Both stacks speak the frozen circuit-relay-v2 wire: `libp2p-relay 0.21.1` mounts
`/libp2p/circuit/relay/0.2.0/hop` + `/stop`, and `@libp2p/circuit-relay-v2
4.2.7` (paired with libp2p 3.3.4) uses the byte-identical `RELAY_V2_HOP_CODEC` /
`RELAY_V2_STOP_CODEC` strings. This extends the Phase-1 spike (which proved
DIRECT-DIAL rust↔js) to the relay control plane: the js clients' HOP reservation
+ CONNECT and the relay's STOP dial all negotiate cross-implementation. A js
`circuitRelayServer` gate (`run-relay-gate.sh js`) also passes, as a harness
control.

**Gate verdict: PASS. The relay use case does NOT collapse on interop.** The A/B
below is therefore meaningful.

---

## STEP 2 — A/B scaling sweep

### Methodology

For each relay under test — **Path A: js `circuitRelayServer`** (Peerbit's exact
config, `circuitRelayServer({ reservations: { applyDefaultLimit: false,
maxReservations: 1000 } })`, `js/relay-js.mjs`) vs **Path B: the native rust
relay** (`relay_node`, limits raised + rate limiters cleared to match the js
`applyDefaultLimit:false` relay, `src/relay.rs::relay_config`) — the driver
(`js/relay-bench.mjs`):

- boots a pool of js circuit-relay client peers: for each concurrent circuit one
  DEST (reserves on the relay) and one SOURCE (dials the DEST through the relay);
- establishes the persistent relayed circuit source→dest **once** (the one-time
  TCP+noise+yamux+circuit-CONNECT handshake is paid in setup, outside every
  timed region), then in each measured iteration opens a fresh `/relay-bench`
  stream on that circuit, streams a **256 KiB** payload, half-closes, and awaits
  a 2-byte ack — the relay forwards every byte socket→socket without decoding;
- sweeps **concurrency ∈ {1, 10, 50, 100, 200}**, each level **2 warmup iters
  discarded + 6 measured iters**, and — because the same js source/dest pairs
  drive both relays — the ONLY variable between A and B is which process
  forwards the bytes.

Per level we record: aggregate forwarding throughput (MB/s), per-circuit
round-trip latency p50/p95/p99, the **relay process CPU-seconds** spent in that
level's measured window (`ps -o time` centisecond deltas on the relay's own pid,
bracketing exactly the measured runs), and for the js relay the **event-loop
delay** (`perf_hooks.monitorEventLoopDelay`, the known JS-under-load failure
mode). **2 full replications** of each sweep; strictly sequential (one relay at a
time, never concurrent with a build or with each other), quiet machine.

**Machine:** Apple Silicon, node v24.14.1, `cargo --release`. `ulimit -n` = 1 M
(file descriptors were never the ceiling). Concurrency ceiling reached: **200
concurrent relayed circuits** (= 400 js-libp2p client nodes in the driver
process); both relays sustained it.

### A crucial harness caveat (read before the numbers)

All 400 client peers run **in the driver's single node process**, doing the
noise encryption/decryption for every circuit. At 256 KiB payloads that
**client-side crypto is the throughput bottleneck**, not either relay: the
driver process runs at 75–130 % CPU while the native relay sits at < 50 % of one
core. **Throughput and latency are therefore CLIENT-bound and near-identical for
both relays — they do NOT show relay saturation.** The metric that *does*
isolate the relay runtime, and is independent of the shared client bottleneck,
is **relay CPU-seconds per GB forwarded** and the **relay's core utilization**.
That is the decision input; the throughput/latency columns are reported for
completeness and to show the client ceiling is shared (fair) between A and B.

### Scaling table (mean of 2 replications × 6 runs each)

Payload 256 KiB. `s/GB` = relay CPU-seconds per GB forwarded (lower is better).
`util` = relay CPU-seconds ÷ wall-seconds in the measured window (1.0 = one core
saturated; the js relay is single-event-loop so ~1.0 is its ceiling).

| concurrency | thr MB/s (nat) | thr MB/s (js) | p50 ms (nat) | p50 ms (js) | p99 ms (nat) | p99 ms (js) | **s/GB (nat)** | **s/GB (js)** | **util (nat)** | **util (js)** | js/nat CPU |
|------------:|---------------:|--------------:|-------------:|------------:|-------------:|------------:|---------------:|--------------:|---------------:|--------------:|-----------:|
| 1           | 58.0           | 74.1          | 4.7          | 3.6         | 4.7          | 3.6         | **6.36**       | **12.72**     | **0.23**       | **0.52**      | 2.00×      |
| 10          | 66.4           | 78.2          | 39.4         | 30.7        | 40.8         | 35.0        | **6.99**       | **14.31**     | **0.42**       | **0.99**      | 2.05×      |
| 50          | 69.1           | 77.3          | 183.6        | 139.8       | 189.7        | 170.9       | **7.12**       | **13.61**     | **0.48**       | **1.02**      | 1.91×      |
| 100         | 61.5           | 73.0          | 397.4        | 317.2       | 438.9        | 366.0       | **8.27**       | **13.19**     | **0.49**       | **0.94**      | 1.60×      |
| 200         | 66.0           | 75.7          | 772.0        | 614.1       | 798.3        | 692.4       | **7.15**       | **13.67**     | **0.47**       | **1.03**      | 1.91×      |

js relay **event-loop lag** over the two full sweeps: worst-window p99 = 13.9 ms
and 13.3 ms; worst-window max = **38.2 ms and 43.6 ms**. The native relay has no
event loop and no such stall metric.

Both replications agree closely; the one noisy cell is js `s/GB @ c=10`
(9.5–19.1 across replications) — that low-concurrency level forwards the fewest
bytes so its CPU delta is closest to the `ps` centisecond resolution floor. All
c ≥ 50 cells are stable to ±1 s/GB.

### What the curve says

- **Throughput / latency are client-bound and show NO native win.** In fact the
  js relay is *marginally faster* end-to-end (≈ 74–78 vs 58–69 MB/s; lower p50/
  p99) — because neither relay is the bottleneck, so this reflects the native
  relay's slightly heavier per-circuit bookkeeping and measurement noise, not a
  js advantage in forwarding capacity. **A native relay buys no throughput or
  latency at any concurrency measured here.**

- **CPU efficiency is where native wins, and it is ≈ 2×, flat across
  concurrency.** The native relay forwards a GB for **~7 CPU-seconds**; the js
  relay needs **~13.5** — a **~1.9× lower** CPU cost per unit of forwarding work,
  holding at every concurrency from 1 to 200.

- **The decisive fact is the utilization ceiling.** The js `circuitRelayServer`
  is a **single event loop → single core**: its utilization pins at **~1.0 from
  c = 10 onward** (0.99, 1.02, 0.94, 1.03) — it is *already CPU-saturated on its
  one core* across the whole sweep. The native relay sits at **~0.48** at the
  same load and is a multi-threaded tokio runtime with cores to spare. So at
  equal CPU budget the native relay has **~2× lower per-GB cost AND ≥ 2× the
  cores** to apply — roughly **4× the forwarding headroom** before it saturates.

- **Where the gap becomes material.** In this loopback harness the in-process
  clients saturate before either relay does, so we never drove throughput to the
  relay's own ceiling — but the utilization curve locates that ceiling: the js
  relay reaches its single-core limit at **c ≈ 10** (util already ~1.0), while
  the native relay at c = 200 is still at half of one core. A relay-heavy node
  whose forwarding load pushes the relay itself toward its core ceiling — i.e.
  once the relay, not the client, is the bottleneck — is exactly the regime where
  the js relay stalls (event-loop max spikes to 38–44 ms under this load,
  degrading every OTHER task on that event loop: the node's own pubsub, block
  serving, sync) and the native relay keeps scaling across cores.

---

## Verdict — decision-grade

**Does the native relay scale meaningfully better than the js relay? YES on CPU
headroom and multi-core scaling; NO on throughput or latency in any regime this
env could reach.** Specifically:

1. **No throughput/latency case.** At every concurrency from 1 to 200, the
   native relay is not faster (it is marginally slower) end-to-end. If the goal
   is lower per-circuit latency or higher aggregate MB/s at these loads, a native
   relay does not deliver it.

2. **A real, ~2× CPU-efficiency case that compounds with multi-core.** The
   native relay forwards each GB for ~half the CPU AND, being multi-threaded, is
   not capped at one core. The js `circuitRelayServer` is pinned at ~1.0 core
   utilization from c ≈ 10 upward — it is single-core-bound for the entire sweep.
   The native relay's advantage is therefore **capacity/headroom**, not speed:
   ~4× more forwarding it can absorb before saturating, and it does so without
   the event-loop stalls (worst-case 38–44 ms) that a saturated js relay imposes
   on **every other workload sharing that node's event loop**.

3. **At what concurrency does the gap become material?** Wherever the **relay
   itself** is the bottleneck rather than the peers it serves. The js relay hits
   its single-core ceiling at **c ≈ 10** in this harness; beyond that it can only
   scale by adding processes. So for a **relay-heavy node** — one dedicating
   significant CPU to forwarding many simultaneous circuits, where the relay
   competes with the node's own pubsub/block/sync work on the same event loop —
   a native relay is justified: it removes the single-core cap and halves the CPU
   the forwarding steals from the rest of the node. For a node that relays only
   a handful of circuits, the js relay is already below its ceiling and a native
   relay buys nothing worth the integration cost.

**Bottom line for the native-transport track.** Unlike the receive path (where
`PROFILING.md` found the perf case weak — the copy is immaterial and the codec
speedup is obtainable other ways), the **relay path has a genuine, structural
native advantage**: escaping the js single-event-loop core ceiling and halving
CPU-per-GB. It is a *scaling/headroom* win for relay-heavy nodes, not a latency
win, and it should be justified on that basis. Interop is proven, so the option
is real.

### Caveats / threats to validity

- The absolute throughput/latency numbers are **client-bound** by the in-process
  400-node driver, not relay-bound; do not read them as relay capacity. The
  client-independent CPU-per-GB and utilization metrics carry the verdict.
- `ps -o time` has centisecond resolution; the c=1/c=10 CPU deltas are small and
  noisier than c ≥ 50. The verdict rests on the stable c ≥ 50 cells and the
  consistent util ceiling, both reproduced across 2 replications.
- A true relay-saturation crossover (driving load until the *relay* is the
  bottleneck) would need the source/dest peers spread across **multiple
  processes/hosts** so the clients stop being the ceiling. That is the follow-up
  to convert "≈ 4× headroom, extrapolated from the util curve" into a measured
  saturation-concurrency for each relay.

---

## Reproducing

Never run concurrently with any build/test or with each other; sequential only.
Warmups are discarded; ≥ 6 measured runs per level; re-run for a 2nd replication.

```sh
# 0. Build the native relay bin (a BUILD — do it BEFORE any measurement) and
#    install the js harness deps (also a build step).
cargo build --release --bin relay_node          # in this crate
( cd js && npm install )                          # circuit-relay-v2 + client deps

# 1. INTEROP GATE (STEP 1) — prove the native relay serves js circuit-relay
#    clients (and, as a control, that the js relay does too):
bash scripts/run-relay-gate.sh native             # the gate that matters -> RELAY_GATE=PASS
bash scripts/run-relay-gate.sh js                  # harness control

# 2. A/B SCALING SWEEP (STEP 2) — one relay at a time, sequential:
PAYLOAD_BYTES=262144 CONCURRENCY=1,10,50,100,200 RUNS=6 WARMUP=2 \
  bash scripts/run-relay-sweep.sh native /tmp/relay-native.json
PAYLOAD_BYTES=262144 CONCURRENCY=1,10,50,100,200 RUNS=6 WARMUP=2 \
  bash scripts/run-relay-sweep.sh js     /tmp/relay-js.json
# (re-run each into -run2.json for a 2nd replication)
```

Knobs (env): `PAYLOAD_BYTES` (default 262144), `CONCURRENCY` (comma list),
`RUNS` (measured iters/level, default 6), `WARMUP` (discarded, default 2). The
sweep runner passes the relay's own pid to the bench so relay CPU-seconds are
attributed to each level's measured window; the js runner also merges the
relay's event-loop-lag summary into the result JSON.

## Files

- `src/relay.rs` — native circuit-relay-v2 **server** swarm + `relay_config`
  (mirrors the js `applyDefaultLimit:false, maxReservations:1000` relay:
  high-capacity, no byte cap, rate limiters cleared). Unit-tested.
- `src/bin/relay_node.rs` — native relay node bin; prints `RELAY_ADDR` /
  `RELAY_SELF_PID` and grep-able `RESERVATION_ACCEPTED` / `CIRCUIT_ACCEPTED` /
  `CIRCUIT_CLOSED` transport-layer evidence.
- `js/relay-common.mjs` — js circuit-relay CLIENT + js `circuitRelayServer`
  (Peerbit node transport spec) builders, shared by the gate and the bench.
- `js/relay-js.mjs` — Path A: the js relay node; emits `EVENT_LOOP_LAG` samples.
- `js/relay-gate.mjs` — STEP 1 interop gate (js↔js byte-exact through a relay).
- `js/relay-bench.mjs` — STEP 2 sweep driver (throughput / latency percentiles /
  relay CPU-per-GB per concurrency level).
- `scripts/run-relay-gate.sh` — boots a relay + runs the gate through it.
- `scripts/run-relay-sweep.sh` — boots a relay + runs the sweep, attributing
  relay CPU-time and (for js) event-loop lag.
- `benchmark/relay-results/{native,js}-run{1,2}.json` — the raw sweep results
  behind the table.
