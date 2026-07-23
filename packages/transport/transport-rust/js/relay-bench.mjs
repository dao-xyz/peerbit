// RELAY A/B scaling driver (RELAY-PROFILING.md STEP 2).
//
// Given a relay's dial-through multiaddr (RELAY_ADDR — a native rust relay OR a
// js circuitRelayServer, the ONLY variable between Path A and Path B), this
// driver:
//
//   1. boots a pool of js-libp2p circuit-relay CLIENT peers (Peerbit node
//      transport spec) — for each concurrent circuit, one DEST peer that
//      reserves on the relay and one SOURCE peer that dials the dest THROUGH
//      the relay (/p2p-circuit),
//   2. for each measured iteration, every source opens a /relay-bench stream
//      over its relayed circuit, streams PAYLOAD_BYTES to its dest, half-closes,
//      and waits for a 1-byte ack — the relay forwards every one of those bytes
//      socket→socket without ever decoding them,
//   3. measures per-circuit round-trip latency (p50/p95/p99) and aggregate
//      forwarding throughput (MB/s = concurrency*PAYLOAD_BYTES / window),
//   4. sweeps concurrency across CONCURRENCY_LEVELS, WARMUP warmup iters
//      discarded + RUNS measured iters per level, reports mean±stdev.
//
// The point of the sweep is the SCALING CURVE: where each relay saturates and
// how per-connection cost grows with concurrency. Native tokio async is
// expected to hold latency/throughput flatter than the js event loop at high
// concurrency.
//
// Usage:
//   RELAY_ADDR=<addr> node relay-bench.mjs [--label js|native]
// Env knobs:
//   PAYLOAD_BYTES (default 262144 = 256 KiB)
//   CONCURRENCY   (default "1,10,50,100")
//   RUNS          (default 6)   measured iterations per level
//   WARMUP        (default 2)   discarded iterations per level
// Emits a single RESULT_JSON=<json> line at the end (+ human-readable table).

import { execSync } from 'node:child_process'
import { multiaddr } from '@multiformats/multiaddr'
import { createClientNode, BENCH_PROTOCOL } from './relay-common.mjs'

const RELAY_ADDR = process.env.RELAY_ADDR
if (!RELAY_ADDR) {
  console.error('RELAY_ADDR env is required')
  process.exit(2)
}
const argLabel = (() => {
  const i = process.argv.indexOf('--label')
  return i >= 0 ? process.argv[i + 1] : (process.env.LABEL ?? 'relay')
})()

const PAYLOAD_BYTES = Number(process.env.PAYLOAD_BYTES ?? 262144)
const CONCURRENCY_LEVELS = (process.env.CONCURRENCY ?? '1,10,50,100')
  .split(',')
  .map((s) => Number(s.trim()))
  .filter((n) => n > 0)
const RUNS = Number(process.env.RUNS ?? 6)
const WARMUP = Number(process.env.WARMUP ?? 2)

// One reusable payload buffer (deterministic, non-zero so nothing is elided).
const PAYLOAD = new Uint8Array(PAYLOAD_BYTES)
for (let i = 0; i < PAYLOAD.length; i++) PAYLOAD[i] = (i * 31 + 7) & 0xff
const CHUNK = 65536 // 64 KiB write chunk

function mean (xs) {
  return xs.reduce((a, b) => a + b, 0) / xs.length
}
function stdev (xs) {
  if (xs.length < 2) return 0
  const m = mean(xs)
  return Math.sqrt(xs.reduce((a, b) => a + (b - m) ** 2, 0) / (xs.length - 1))
}
function percentile (sorted, p) {
  if (sorted.length === 0) return 0
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))
  return sorted[idx]
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// Optional relay PID: when set, the driver snapshots the relay's CUMULATIVE CPU
// time (via `ps -o time`, centisecond resolution on macOS) at the start and end
// of each level's MEASURED window. The delta is the relay CPU-seconds spent
// forwarding exactly this level's bytes — a client-independent metric that
// isolates the relay runtime even when the in-process clients are the
// throughput bottleneck.
const RELAY_PID = process.env.RELAY_PID ? Number(process.env.RELAY_PID) : null

/** Parse macOS `ps -o time` ("[H:]MM:SS.SS") into seconds. */
function parsePsTime (s) {
  const t = s.trim()
  if (!t) return null
  const parts = t.split(':').map((x) => parseFloat(x))
  let sec = 0
  for (const p of parts) sec = sec * 60 + p
  return sec
}

/** Cumulative CPU-seconds of the relay process, or null if unavailable. */
function relayCpuSeconds () {
  if (RELAY_PID == null) return null
  try {
    const out = execSync(`ps -o time= -p ${RELAY_PID}`, { encoding: 'utf8' })
    return parsePsTime(out)
  } catch {
    return null
  }
}

/**
 * Dial with bounded retries. SETUP-ONLY (untimed): establishing many relayed
 * circuits back-to-back can transiently ECONNRESET (a busy relay resetting a
 * mid-handshake connection); a couple of retries makes setup robust without
 * touching any measured region.
 */
async function dialWithRetry (node, addr, opts, tries = 5) {
  let lastErr
  for (let t = 0; t < tries; t++) {
    try {
      return await node.dial(addr, opts)
    } catch (err) {
      lastErr = err
      await sleep(100 * (t + 1))
    }
  }
  throw lastErr
}

/**
 * DEST handler: drain the incoming payload until the source half-closes, then
 * send a 1-byte ack and close. The relay forwards all of this without decoding.
 */
function installDestHandler (destNode) {
  return destNode.handle(
    BENCH_PROTOCOL,
    (stream) => {
      ;(async () => {
        try {
          let received = 0
          for await (const chunk of stream) {
            received += chunk.byteLength ?? chunk.length
          }
          // Ack the byte count implicitly with a single byte.
          stream.send(new Uint8Array([received & 0xff]))
          await stream.close()
        } catch {
          try { await stream.close() } catch {}
        }
      })()
    },
    // runOnLimitedConnection:true — a relayed circuit is a "limited" connection;
    // the handler MUST run over it (that is the workload being measured).
    { runOnLimitedConnection: true }
  )
}

/**
 * SOURCE side of one iteration over an ALREADY-ESTABLISHED relayed connection:
 * open a fresh /relay-bench stream on the persistent circuit, stream the
 * payload, half-close, await the ack. Returns the round-trip latency in ms.
 *
 * Reusing the persistent circuit is deliberate: we measure STEADY-STATE relay
 * byte-forwarding (open stream + pipe payload + ack) over an established
 * circuit, NOT the one-time TCP+noise+yamux+circuit-CONNECT handshake. That
 * handshake is paid once in setup (and again nowhere in the timed region), so
 * the sweep isolates the forwarding hot path — exactly the I/O+concurrency
 * workload the native-vs-js relay comparison is about.
 */
async function runOneCircuit (relayedConn) {
  const t0 = performance.now()
  const stream = await relayedConn.newStream(BENCH_PROTOCOL, {
    runOnLimitedConnection: true
  })
  // Write the payload in chunks with backpressure.
  let offset = 0
  while (offset < PAYLOAD.length) {
    const end = Math.min(offset + CHUNK, PAYLOAD.length)
    const ok = stream.send(PAYLOAD.subarray(offset, end))
    offset = end
    if (!ok) {
      await new Promise((resolve) => stream.addEventListener('drain', resolve, { once: true }))
    }
  }
  // Half-close our write side so the dest's `for await` ends.
  await stream.close()
  // Await the 1-byte ack from the dest (reading side).
  for await (const _chunk of stream) {
    // first byte received == ack; break.
    break
  }
  return performance.now() - t0
}

async function main () {
  const relayMa = multiaddr(RELAY_ADDR)

  const maxConc = Math.max(...CONCURRENCY_LEVELS)
  console.error(`[bench:${argLabel}] booting ${maxConc} dest + ${maxConc} source client peers...`)

  // Boot the client pools ONCE (reused across concurrency levels). Each dest
  // reserves on the relay and yields its relayed dial address; each source
  // connects to the relay too (so the /p2p-circuit dial can hop).
  const dests = []
  const sources = []
  for (let i = 0; i < maxConc; i++) {
    const dest = await createClientNode()
    await dest.start()
    installDestHandler(dest)
    const src = await createClientNode()
    await src.start()
    dests.push(dest)
    sources.push(src)
  }

  // Reserve every dest on the relay and establish the PERSISTENT relayed
  // connection source[i] → dest[i] ONCE. The one-time TCP+noise+yamux+circuit
  // CONNECT handshake is paid here, outside every timed region; the sweep then
  // reuses these established circuits so it measures forwarding, not setup.
  //
  // Reservations + circuit dials are done SEQUENTIALLY here to avoid a thundering
  // herd of simultaneous noise handshakes through the relay at setup (which can
  // trip the 5s upgrade timeout); this is setup, not a measured region.
  const relayedConns = new Array(maxConc)
  for (let i = 0; i < maxConc; i++) {
    const dest = dests[i]
    // Dial the relay to trigger a reservation (dest advertises /p2p-circuit).
    await dialWithRetry(dest, relayMa, {})
    const relayed = relayMa
      .encapsulate('/p2p-circuit')
      .encapsulate(`/p2p/${dest.peerId.toString()}`)
    // Establish the persistent relayed connection from source[i] to dest[i].
    relayedConns[i] = await dialWithRetry(sources[i], relayed, {
      runOnLimitedConnection: true
    })
    // Small settle between circuits so a busy relay is not hammered during
    // setup (untimed). Keeps the reservation store stable at high maxConc.
    await sleep(20)
  }
  console.error(
    `[bench:${argLabel}] ${maxConc} reservations + persistent relayed circuits established; starting sweep`
  )

  const results = []
  for (const concurrency of CONCURRENCY_LEVELS) {
    // One iteration = `concurrency` circuits opened in parallel; we time the
    // whole batch (throughput) and collect each circuit's latency.
    const runIteration = async () => {
      const latencies = new Array(concurrency)
      const t0 = performance.now()
      await Promise.all(
        Array.from({ length: concurrency }, (_, k) =>
          runOneCircuit(relayedConns[k]).then((ms) => {
            latencies[k] = ms
          })
        )
      )
      const wallMs = performance.now() - t0
      return { latencies, wallMs }
    }

    // Warmups (discarded).
    for (let w = 0; w < WARMUP; w++) await runIteration()

    // MEASURED-WINDOW markers: a co-process CPU sampler brackets relay %CPU to
    // exactly the measured runs of this level (excludes setup/warmup), so the
    // reported relay CPU is attributable to this concurrency's forwarding load.
    // A tiny settle + repeated iterations give the macOS decaying-average %cpu
    // time to converge on the true utilization for this level.
    console.error(`SWEEP_LEVEL_START c=${concurrency}`)
    const levelT0 = performance.now()
    const relayCpu0 = relayCpuSeconds()

    // Measured runs.
    const runThroughputMBs = []
    const runP50 = []
    const runP95 = []
    const runP99 = []
    const runMeanLat = []
    for (let r = 0; r < RUNS; r++) {
      const { latencies, wallMs } = await runIteration()
      const sorted = [...latencies].sort((a, b) => a - b)
      // Aggregate forwarding throughput: every circuit pushed PAYLOAD_BYTES
      // through the relay in this window.
      const totalBytes = concurrency * PAYLOAD_BYTES
      const mbs = totalBytes / 1e6 / (wallMs / 1000)
      runThroughputMBs.push(mbs)
      runP50.push(percentile(sorted, 50))
      runP95.push(percentile(sorted, 95))
      runP99.push(percentile(sorted, 99))
      runMeanLat.push(mean(latencies))
    }

    const relayCpu1 = relayCpuSeconds()
    const levelWallMs = performance.now() - levelT0
    console.error(`SWEEP_LEVEL_END c=${concurrency}`)

    const relayCpuSecondsDelta =
      relayCpu0 != null && relayCpu1 != null ? relayCpu1 - relayCpu0 : null
    // Relay CPU-seconds per GB forwarded (the client-independent discriminator).
    const gbForwarded = (concurrency * PAYLOAD_BYTES * RUNS) / 1e9
    const relayCpuSecPerGB =
      relayCpuSecondsDelta != null && gbForwarded > 0
        ? relayCpuSecondsDelta / gbForwarded
        : null
    // Relay CPU utilisation during the window (CPU-seconds / wall-seconds; a
    // value near 1.0 means one core fully saturated, >1 means multi-core).
    const relayCpuUtil =
      relayCpuSecondsDelta != null && levelWallMs > 0
        ? relayCpuSecondsDelta / (levelWallMs / 1000)
        : null

    const row = {
      concurrency,
      throughput_MBs_mean: mean(runThroughputMBs),
      throughput_MBs_stdev: stdev(runThroughputMBs),
      lat_mean_ms_mean: mean(runMeanLat),
      lat_p50_ms_mean: mean(runP50),
      lat_p95_ms_mean: mean(runP95),
      lat_p99_ms_mean: mean(runP99),
      lat_p99_ms_stdev: stdev(runP99),
      // Total bytes forwarded through the relay during this level's measured
      // window (one direction; the 2-byte acks are negligible). Divided by the
      // relay's CPU-seconds in the same window (captured by the runner) this
      // yields the client-independent CPU-per-GB discriminator.
      measured_bytes: concurrency * PAYLOAD_BYTES * RUNS,
      measured_wall_ms: levelWallMs,
      relay_cpu_seconds: relayCpuSecondsDelta,
      relay_cpu_sec_per_GB: relayCpuSecPerGB,
      relay_cpu_util: relayCpuUtil,
      runs: RUNS
    }
    results.push(row)
    console.error(
      `[bench:${argLabel}] c=${String(concurrency).padStart(4)} ` +
        `thr=${row.throughput_MBs_mean.toFixed(1)}±${row.throughput_MBs_stdev.toFixed(1)}MB/s ` +
        `p50=${row.lat_p50_ms_mean.toFixed(1)} p95=${row.lat_p95_ms_mean.toFixed(1)} ` +
        `p99=${row.lat_p99_ms_mean.toFixed(1)}±${row.lat_p99_ms_stdev.toFixed(1)}ms ` +
        (relayCpuSecPerGB != null
          ? `relayCPU=${relayCpuSecPerGB.toFixed(2)}s/GB util=${relayCpuUtil.toFixed(2)}`
          : 'relayCPU=n/a')
    )
  }

  const out = {
    label: argLabel,
    relay_addr: RELAY_ADDR,
    payload_bytes: PAYLOAD_BYTES,
    runs: RUNS,
    warmup: WARMUP,
    node: process.version,
    results
  }
  console.log(`RESULT_JSON=${JSON.stringify(out)}`)

  // Teardown.
  await Promise.all([
    ...sources.map((n) => n.stop().catch(() => {})),
    ...dests.map((n) => n.stop().catch(() => {}))
  ])
  process.exit(0)
}

main().catch((err) => {
  console.error('[bench] fatal:', err?.stack ?? err)
  process.exit(1)
})
