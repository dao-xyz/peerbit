# Native-transport receive profiling — is the socket→wasm copy material?

**Question this answers.** Phase 2 of the native transport has to choose between
two shapes:

1. **napi (in-process native addon).** Runs the codec in the same process as the
   node, so the transport can hand `peerbit_wire` **socket slices in place** —
   the per-frame `array.to_vec()` js→wasm ingress copy (ARCHITECTURE.md
   §8 exception 2, `network-rust/src/lib.rs:79`) never happens. This breaks the
   repo's "the native swarm stays out-of-process / no zero-copy assumption" rule.
2. **sidecar (out-of-process).** Rule-compliant, but the codec output crosses a
   process boundary, so it **re-adds a copy** on that boundary (IPC/shared-mem
   marshalling) — it does *not* get the zero-copy win.

The only thing napi buys over a sidecar is eliminating that one copy. So the
decision reduces to a measurable fact: **how big is the socket→wasm copy as a
fraction of per-message receive cost?** If it is a large fraction, napi's
zero-copy is worth breaking the rule. If it is small, the sidecar loses almost
nothing and the rule-compliant path wins — the native track then rests on its
strategic goal (owning the connection layer), not on this micro-optimization.

This is exactly the gate ARCHITECTURE.md §10 names: native transports "only
become worthwhile once profiling shows the transport layer itself is material —
exception 2 in section 8 is the cost they would remove."

## What was measured

Two measurements, both over a fixed corpus of **N = 4000 signed `DataMessage`
frames** per payload size **{32 B, 1 KB, 16 KB, 64 KB}**, built with the real
`@peerbit/crypto` Ed25519 signer + the real `@peerbit/stream-interface`
`DataMessage` codec (identical construction to
`network-rust/benchmark/index.ts`). Each frame carries one sha256-prehashed
Ed25519 signature — the direct-stream inbound hot path. **8 measured runs, 3
warmups discarded, mean ± stdev**; every run was re-run a second time
(`run1`/`run2`) and the two agree within noise. Runs are strictly sequential on
a quiet machine, never concurrent with any build or with each other.

### Measurement 1 — isolate the copy (the napi-specific payoff), in wasm

Both timed on the same corpus in the JS/wasm runtime a node uses today:

- **`T_copy`** — a **bench-only wasm export `copy_batch_only`**
  (`network-rust/src/lib.rs`) that performs *only* the per-frame
  `array.to_vec()` ingress copy into wasm linear memory that
  `decode_and_verify_batch` does, folds a checksum over the copied bytes (so the
  copy cannot be elided and every byte is touched, as decode would), and returns
  — **no decode, no verify.**
- **`T_decode`** — the full `decode_and_verify_batch` (copy + borsh decode +
  Ed25519 batch verify) — the whole wasm-side receive cost.
- Report **`T_copy / T_decode`**: the copy's fraction of the wasm-side receive
  cost — i.e. exactly the slice a native transport removes.

### Measurement 2 — end-to-end A/B (total native benefit)

- **Path A (current)** — the full JS receive path: frames delivered to JS,
  batched, `decodeAndVerifyBatch` (JS pump + boundary marshalling + copy +
  decode + verify). `benchmark/wire-receive-profile.ts`.
- **Path B (native, zero-copy)** — a native rust binary
  (`benchmark/wire_receive_native.rs`, `cargo` bin `wire_receive_native`) that
  reads the **byte-identical corpus** the JS harness wrote and calls
  `FrameCodec::decode_and_verify(&[&[u8]], now)` — the Phase-1 crate's real
  decode path — on **borrowed socket slices: no copy, no JS, no wasm.** It
  asserts all 4000 frames verify, so the A/B compares identical, valid work.

The A−B delta is the *total* native benefit (copy elimination + JS-pump removal
+ native-vs-wasm codegen of the codec). Measurement 1 attributes how much of it
is the copy alone. We deliberately do **not** interpose real TCP/noise/yamux in
Path B — that would measure the network, not the receive-codec cost, and the
socket byte-work is identical for both transports; the "decode in place" slice
shape is the same one `FrameCodec` uses on a real socket buffer.

## Results

Batch = 64 frames/call. Mean of the two replications (`run1`, `run2`); per-run
stdev in the raw JSON. Machine: Apple Silicon, node v24, `cargo --release`.

### Measurement 1 — the copy vs the wasm receive cost

| payload | T_copy (ms/4000) | T_decode (ms/4000) | **copy / decode** | copy (µs/frame) | decode (µs/frame) |
|--------:|-----------------:|-------------------:|------------------:|----------------:|------------------:|
| 32 B    | 0.92 ± 0.2       | 379.5 ± 17         | **0.24 %**        | 0.231           | 94.9              |
| 1 KB    | 1.10 ± 0.3       | 393.2 ± 12         | **0.28 %**        | 0.276           | 98.3              |
| 16 KB   | 4.51 ± 0.5       | 610.9 ± 19         | **0.74 %**        | 1.127           | 152.7             |
| 64 KB   | 9.87 ± 0.7       | 1300.3 ± 26        | **0.76 %**        | 2.468           | 325.1             |

The copy is **≤ 0.8 % of the wasm-side receive cost at every payload size.**
Ed25519 verify + borsh decode dominates by 130×–420×. The copy scales with
payload bytes (memcpy) while decode is dominated by the fixed-cost signature
verify, so the fraction *grows* with payload — but even at 64 KB (4× the
16 KB direct-stream chunk ceiling) it is under 1 %.

### Measurement 2 — Path A (JS) vs Path B (native), and attribution

| payload | Path A µs/frame | Path B µs/frame | A frames/s | B frames/s | native speedup | copy µs/frame | **copy as % of (A−B) delta** |
|--------:|----------------:|----------------:|-----------:|-----------:|---------------:|--------------:|-----------------------------:|
| 32 B    | 94.4            | 42.0            | 10,601     | 23,838     | **2.25×**      | 0.231         | **0.4 %**                    |
| 1 KB    | 97.6            | 46.4            | 10,250     | 21,533     | **2.10×**      | 0.276         | **0.5 %**                    |
| 16 KB   | 152.7           | 91.7            | 6,548      | 10,908     | **1.67×**      | 1.127         | **1.8 %**                    |
| 64 KB   | 325.5           | 240.4           | 3,072      | 4,159      | **1.35×**      | 2.468         | **2.9 %**                    |

The native path is a real **1.35×–2.25×** faster end to end — but the copy is
only **0.4 %–2.9 % of that A−B delta.** The remaining ~97 %+ is native-vs-wasm
codegen of the Ed25519/SHA-256/borsh work (and a sliver of JS-pump removal, which
Measurement 1 shows is itself negligible: Path A per-frame is unchanged whether
the JS pump batches 1 or 64 frames per wasm call, see below).

### Batch-size sensitivity (does the copy ever start to dominate?)

Path A per-frame cost and the copy fraction, batch = 1 (per-frame wasm call, the
worst case for boundary crossings) vs batch = 64:

| payload | copy/decode @batch1 | copy/decode @batch64 | Path A µs/frame @batch1 | @batch64 |
|--------:|--------------------:|---------------------:|------------------------:|---------:|
| 32 B    | 0.32 %              | 0.24 %               | 93.5                    | 94.4     |
| 64 KB   | 0.78 %              | 0.76 %               | 326.3                   | 325.5    |

Batching barely moves anything: the boundary/pump overhead is already trivial
against the per-frame Ed25519 verify, so there is no batch size or message rate
at which the copy or the JS boundary becomes the bottleneck. **There is no
crossover in the realistic regime.** The copy would only approach parity with
decode at payloads on the order of **hundreds of KB to MB** *and* only if
signature verification were removed from the path (e.g. pre-verified bulk
transfer) — neither holds for the signed direct-stream frames this transport
carries. Frames are capped at `MAX_DATA_LENGTH_IN` = 15 MB, but a 15 MB frame is
one memcpy (~a few hundred µs) against a fixed ~40 µs verify, so even the
absolute worst legal frame keeps the copy well under the cost of the crypto — and
such frames are rare bulk blocks, not the hot path.

## Verdict — the copy is immaterial; napi is not justified; the perf case is weak

**The socket→wasm ingress copy is immaterial.** It is ≤ 0.8 % of the wasm
receive cost and ≤ 2.9 % of the total native-transport benefit, at every payload
size from 32 B to 64 KB, at every batch size, with no crossover anywhere in the
realistic regime. The receive hot path is **compute-bound on Ed25519 signature
verification**, not memory-bound on the copy.

Consequences for the Phase-2 decision:

- **napi's zero-copy is NOT worth breaking the no-napi rule.** The one thing napi
  uniquely removes is the socket→wasm ingress copy, and that copy is under 1 % of
  per-message cost at every payload and batch size measured. This conclusion is
  decision-grade and independently reproduced.
- **The sidecar's real cost is NOT measured here — do not over-read it.** Path B
  in this harness decodes in a single process with *zero* boundary; it is the
  **napi/in-process upper bound**, not a sidecar. A sidecar re-adds a *process
  boundary* whose cost — IPC of frame bytes one way plus the decoded records array
  back — this harness does not model. Equating that to the tiny M1 ingress copy
  only holds under shared memory, and even then the records-return trip is
  unaccounted. Before committing to a sidecar at high message rates, run a
  shared-memory round-trip micro-bench to close this gap.
- **The 1.35×–2.25× "native speedup" is a wasm→native codec-codegen gap, not a
  transport property.** ~97 % of the A−B delta is native code being faster than
  wasm at Ed25519/SHA-256/borsh — a lever available to napi, a sidecar, *or* the
  existing in-process native-backbone independent of who owns the transport.
- **So the native-transport track's *performance* case is weak.** The copy is
  immaterial and the codec speedup is obtainable other ways. If pursued, it should
  be justified on its **strategic goal** — owning the connection layer (native
  tcp/ws/noise/yamux/relay, removing js-libp2p from node) and the architectural
  purity of a JS-free node path — not on a measured perf win. Recommended
  mechanism if pursued: the rule-compliant sidecar (keeps the libp2p 0.56 tree out
  of the node process), pending the round-trip micro-bench above.

## Reproducing

Never run concurrently with any build/test or with each other; sequential only.

```sh
# 0. Build the wasm codec (adds the copy_batch_only bench export) and the
#    native Path B bin. (Both are builds — do them BEFORE any measurement.)
pnpm --filter @peerbit/network-rust run build          # or: wasm-pack build --target web ...
cargo build --release --bin wire_receive_native        # in this crate

# 1. Compile the JS harness (benchmark/ is not in the default tsc include):
tsc -p benchmark/tsconfig.json                          # emits dist/benchmark/

# 2. Measurement 1 + Path A, and WRITE the shared corpus:
node ./dist/benchmark/wire-receive-profile.js --corpus /tmp/corpus --out /tmp/js.json

# 3. Path B over the SAME corpus (separate, sequential invocation):
./target/release/wire_receive_native --corpus /tmp/corpus --out /tmp/native.json
```

Knobs (env): `PEERBIT_PROFILE_COUNT` (frames, default 4000),
`PEERBIT_PROFILE_BATCH` (default 64), `PEERBIT_PROFILE_WARMUP` (3),
`PEERBIT_PROFILE_RUNS` (8). The native bin takes `--batch` to match.

## Files

- `network-rust/src/lib.rs` — `copy_batch_only` bench-only wasm export (T_copy).
- `network-rust/src/index.ts` — `copyBatchOnly` on the `NativeWireModule` surface.
- `benchmark/wire-receive-profile.ts` — Measurement 1 + Path A; writes the corpus.
- `benchmark/wire_receive_native.rs` — Path B native bin (`wire_receive_native`).
- `benchmark/tsconfig.json` — standalone tsc project for the harness.
