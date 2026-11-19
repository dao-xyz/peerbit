#!/usr/bin/env node
/**
 * rc-driver.mjs: two-peer reconnect-cost harness (JUDGED_PLAN.md section 5).
 *
 *   node rc-driver.mjs --root <tree> --variant <name> --n <entries> --min <floor> --out <dir>
 *     [--phases rest,writes,control] [--writes 60] [--partition-ms <ms>]
 *     [--commit <sha>] [--quiet-ms 5000] [--observe-min-ms 15000]
 *     [--observe-cap-ms 180000] [--converge-timeout-ms 1800000]
 *     [--wait-load <load1>] [--wait-load-timeout-ms 1800000] [--keep-peers]
 *     [--linked] [--profile-log <regex>] [--run-timeout-ms <ms>]
 *     [--cpu-mark-ms 12000]
 *
 * --run-timeout-ms bounds the whole run (default 0: unbounded). When it
 * expires the run is abandoned where it stands: the workers are stopped (and
 * killed if they do not exit), report.json is written with ok: false,
 * timedOut: true and the phases completed so far, and the exit code is 1.
 *
 * Forks peer A and peer B (rc-worker.mjs), seeds N entries on A, has B dial
 * and open the store, waits for convergence, then runs the phases. Every
 * phase follows a quiesce (both sides idle at once) and a counter reset.
 *
 * Both sides run SharedLog's production timings (distributionDebounceTime
 * 500 ms, timeUntilRoleMaturity 5000 ms): each worker drops the test
 * EventStore's fast test values before log.open and fails the run unless
 * SharedLog reports the defaults. B constructs the store with A's id, so it
 * can do that before the store opens, and checks that it has A's address
 * (the plan has B open the address).
 *
 * --cpu-mark-ms arms a CPU snapshot on both sides that long after the dial
 * (after the first write in control). The default, 12 s, falls between the
 * join-authoritative sweeps at 7 and 15 s after the join, and before the
 * 15 s observation floor. Each side's report carries it as cpuMark; cpuMs
 * stays the total at the end of the observation, whose length varies with
 * inventory work.
 *
 *   rest     partition (hang up while the gater denies redials), drain the
 *            dial queues, heal, B dials A; observe until inventory completed
 *            on both sides and at least 15 s since the dial (cap 180 s).
 *   writes   like rest, but right after the dial resolves A writes one entry
 *            per second (--writes, default 60); observe until all are on B,
 *            inventory completed and 15 s since the last write (cap 180 s
 *            after the last write).
 *   control  no partition; A writes as in writes; same observation.
 *
 * --partition-ms holds each partition that long (from the gate) before the
 * heal, and A writes one partition-era entry once the departure is handled.
 *
 * Every seeded or written entry is its own head (meta.next = []), as a
 * document put is, so the coordinate index holds one row per entry and the
 * rebalance scan and inventory walk cover N entries. --linked uses the plain
 * EventStore append instead (each entry links to the previous head, leaving a
 * single coordinate row).
 *
 * "Inventory completed" means the entry-inventory scheduler on that side has
 * no pass queued or running (a pass that ends "retry" is re-queued until it
 * completes or its peer session ends). Not every reconnect wakes a pass on
 * both sides; the report has the passes per side.
 *
 * Writes <out>/report.json, <out>/events.ndjson (driver), and
 * <out>/events-{A,B}.ndjson plus <out>/worker-{A,B}.log (workers). Refuses a
 * non-empty --out. Product code is never changed; see rc-worker.mjs.
 */
import { fork } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const WORKER = path.join(HERE, "rc-worker.mjs");

const parseArgs = (list) => {
	const out = {};
	for (let i = 0; i < list.length; i++) {
		const key = list[i];
		if (!key.startsWith("--")) continue;
		const next = list[i + 1];
		out[key.slice(2)] =
			next !== undefined && !next.startsWith("--") ? list[++i] : "true";
	}
	return out;
};

const usage = () => {
	console.error(
		"usage: rc-driver.mjs --root <tree> --variant <name> --n <entries> --min <floor> --out <dir> [--phases rest,writes,control] [--writes 60] [--partition-ms <ms>]",
	);
	process.exit(2);
};

const opts = parseArgs(process.argv.slice(2));
for (const key of ["root", "variant", "n", "min", "out"]) {
	if (!opts[key]) usage();
}
const ROOT = path.resolve(opts.root);
const OUT = path.resolve(opts.out);
const VARIANT = opts.variant;
const N = Number(opts.n);
const MIN = Number(opts.min);
const PHASES = (opts.phases ?? "rest,writes,control")
	.split(",")
	.map((phase) => phase.trim())
	.filter(Boolean);
const WRITES = Number(opts.writes ?? 60);
const PARTITION_MS = Number(opts["partition-ms"] ?? 0);
const QUIET_MS = Number(opts["quiet-ms"] ?? 5_000);
const OBSERVE_MIN_MS = Number(opts["observe-min-ms"] ?? 15_000);
const OBSERVE_CAP_MS = Number(opts["observe-cap-ms"] ?? 180_000);
const CONVERGE_TIMEOUT_MS = Number(opts["converge-timeout-ms"] ?? 1_800_000);
const WAIT_LOAD = opts["wait-load"] !== undefined ? Number(opts["wait-load"]) : null;
const WAIT_LOAD_TIMEOUT_MS = Number(opts["wait-load-timeout-ms"] ?? 1_800_000);
const KEEP_PEERS = opts["keep-peers"] === "true";
// Entries are independent heads (meta.next = []) unless --linked: linked
// appends leave one coordinate row however many entries exist.
const LINKED = opts.linked === "true";
// Diagnostics only: workers log every profile event whose name matches.
const PROFILE_LOG = opts["profile-log"] ?? null;
if (PROFILE_LOG !== null) new RegExp(PROFILE_LOG);
// Whole-run deadline (0 = none), so one stuck run cannot eat a CI job.
const RUN_TIMEOUT_MS = Number(opts["run-timeout-ms"] ?? 0);
// The CPU snapshot criterion 3 judges, this long after the dial.
const CPU_MARK_MS = Number(opts["cpu-mark-ms"] ?? 12_000);

if (
	!Number.isInteger(N) ||
	N < 1 ||
	!Number.isInteger(MIN) ||
	MIN < 1 ||
	!Number.isInteger(WRITES) ||
	WRITES < 1 ||
	!(PARTITION_MS >= 0) ||
	!Number.isInteger(RUN_TIMEOUT_MS) ||
	RUN_TIMEOUT_MS < 0 ||
	!Number.isInteger(CPU_MARK_MS) ||
	CPU_MARK_MS < 1
) {
	usage();
}
for (const phase of PHASES) {
	if (!["rest", "writes", "control"].includes(phase)) {
		console.error(`unknown phase ${phase}`);
		process.exit(2);
	}
}
const SHARED_LOG_INDEX = path.join(
	ROOT,
	"packages/programs/data/shared-log/dist/src/index.js",
);
for (const required of [
	SHARED_LOG_INDEX,
	path.join(ROOT, "packages/clients/peerbit/dist/src/index.js"),
	path.join(
		ROOT,
		"packages/programs/data/shared-log/dist/test/utils/stores/event-store.js",
	),
]) {
	if (!fs.existsSync(required)) {
		console.error(`missing build output ${required}`);
		process.exit(2);
	}
}
if (fs.existsSync(OUT) && fs.readdirSync(OUT).length > 0) {
	console.error(`refusing non-empty --out ${OUT}`);
	process.exit(2);
}
fs.mkdirSync(OUT, { recursive: true });

const wall = () => performance.timeOrigin + performance.now();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const round = (value, digits = 1) =>
	value === null || value === undefined || Number.isNaN(value)
		? null
		: Math.round(value * 10 ** digits) / 10 ** digits;

const eventsStream = fs.createWriteStream(path.join(OUT, "events.ndjson"));
const emit = (type, data = {}) => {
	eventsStream.write(`${JSON.stringify({ t: round(wall(), 3), type, ...data })}\n`);
};
const log = (message) => {
	const line = `[rc ${VARIANT} n=${N} min=${MIN}] ${message}`;
	console.log(line);
	emit("log", { message });
};

const driverErrors = { unhandled: 0, uncaught: 0, samples: [] };
process.on("unhandledRejection", (reason) => {
	driverErrors.unhandled += 1;
	driverErrors.samples.push(String(reason?.stack ?? reason));
	emit("driver:unhandledRejection", { reason: String(reason) });
});
process.on("uncaughtException", (error) => {
	driverErrors.uncaught += 1;
	driverErrors.samples.push(String(error?.stack ?? error));
	emit("driver:uncaughtException", { error: String(error) });
});

// ---------------------------------------------------------------------------
// Workers

class Worker {
	constructor(side) {
		this.side = side;
		this.pending = new Map();
		this.seq = 0;
		this.exited = null;
		this.logStream = fs.createWriteStream(path.join(OUT, `worker-${side}.log`));
		this.child = fork(
			WORKER,
			[
				"--root",
				ROOT,
				"--side",
				side,
				"--events",
				path.join(OUT, `events-${side}.ndjson`),
				...(PROFILE_LOG !== null ? ["--profile-log", PROFILE_LOG] : []),
			],
			{ stdio: ["ignore", "pipe", "pipe", "ipc"], serialization: "advanced" },
		);
		this.child.stdout.pipe(this.logStream, { end: false });
		this.child.stderr.pipe(this.logStream, { end: false });
		this.ready = new Promise((resolve, reject) => {
			this.readyResolve = resolve;
			this.readyReject = reject;
		});
		this.child.on("message", (message) => this.onMessage(message));
		this.exitPromise = new Promise((resolve) => {
			this.child.on("exit", (code, signal) => {
				this.exited = { code, signal, at: wall() };
				for (const pending of this.pending.values()) {
					clearTimeout(pending.timer);
					pending.reject(
						new Error(`worker ${side} exited (${code ?? signal}) during ${pending.cmd}`),
					);
				}
				this.pending.clear();
				this.readyReject(new Error(`worker ${side} exited before ready`));
				emit("worker:exit", { side, code, signal });
				resolve(this.exited);
			});
		});
	}

	onMessage(message) {
		if (message?.ready) {
			this.readyResolve(message);
			return;
		}
		if (message?.event) {
			emit("worker:event", message);
			return;
		}
		const pending = this.pending.get(message?.id);
		if (!pending) return;
		this.pending.delete(message.id);
		clearTimeout(pending.timer);
		if (message.ok) {
			pending.resolve(message.result);
		} else {
			const error = new Error(`${this.side}.${pending.cmd}: ${message.error}`);
			error.workerStack = message.stack;
			pending.reject(error);
		}
	}

	call(cmd, args = {}, timeoutMs = 600_000) {
		return new Promise((resolve, reject) => {
			if (this.exited) {
				reject(new Error(`worker ${this.side} already exited; cannot ${cmd}`));
				return;
			}
			const id = ++this.seq;
			const timer = setTimeout(() => {
				this.pending.delete(id);
				reject(new Error(`${this.side}.${cmd} timed out after ${timeoutMs} ms`));
			}, timeoutMs);
			this.pending.set(id, { resolve, reject, timer, cmd });
			this.child.send({ id, cmd, args });
		});
	}
}

const both = (cmd, args, timeoutMs) =>
	Promise.all([A.call(cmd, args, timeoutMs), B.call(cmd, args, timeoutMs)]);

// ---------------------------------------------------------------------------
// Measurements computed by the driver

const quantile = (sorted, q) => {
	if (sorted.length === 0) return null;
	const index = Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1);
	return sorted[Math.max(0, index)];
};
const stats = (values) => {
	const sorted = [...values].sort((a, b) => a - b);
	return {
		count: sorted.length,
		p50: round(quantile(sorted, 0.5), 2),
		p90: round(quantile(sorted, 0.9), 2),
		max: round(sorted.at(-1) ?? null, 2),
		mean: sorted.length
			? round(sorted.reduce((total, value) => total + value, 0) / sorted.length, 2)
			: null,
	};
};

const writeLatency = (records, receiverReport) => {
	if (!records || records.length === 0) return null;
	const latencies = [];
	let viaPoll = 0;
	let missing = 0;
	for (const record of records) {
		let arrivedAt = receiverReport.arrivals?.[record.hash];
		if (arrivedAt === undefined) {
			arrivedAt = receiverReport.polledArrivals?.[record.hash];
			if (arrivedAt !== undefined) viaPoll += 1;
		}
		if (arrivedAt === undefined) missing += 1;
		else latencies.push(arrivedAt - record.t0);
	}
	return {
		writes: records.length,
		arrived: latencies.length,
		missing,
		viaPoll,
		latencyMs: stats(latencies),
		appendMs: stats(records.map((record) => record.t1 - record.t0)),
		firstWriteAt: records[0].t0,
		lastWriteAt: records.at(-1).t1,
	};
};

// ---------------------------------------------------------------------------
// Phases

let A;
let B;
let addressA;

const quiesceBoth = async () => {
	const attempts = [];
	for (let attempt = 0; attempt < 4; attempt++) {
		const [qa, qb] = await both("quiesce", { quietMs: QUIET_MS }, 400_000);
		const [ia, ib] = await both("idle");
		attempts.push({ A: qa, B: qb, idleAfter: { A: ia.idle, B: ib.idle } });
		if (qa.ok && qb.ok && ia.idle && ib.idle) return { ok: true, attempts };
	}
	return { ok: false, attempts };
};

const inventoryDone = (status) =>
	(status.inventory.pending ?? 0) === 0 && (status.inventory.active ?? 0) === 0;

const observe = async ({ result, since, writesPromise, extraHashes }) => {
	const observation = {
		startedAt: wall(),
		since,
		endedBy: null,
		polls: 0,
		inventoryDoneAt: { A: null, B: null },
		allArrivedAt: null,
	};
	let writeRecords = null;
	let writesError = null;
	let writesDone = !writesPromise;
	writesPromise?.then(
		(records) => {
			writeRecords = records;
			writesDone = true;
		},
		(error) => {
			writesError = error;
			writesDone = true;
		},
	);
	const absoluteCapAt =
		wall() + (writesPromise ? WRITES * 1_000 * 3 : 0) + OBSERVE_CAP_MS + 60_000;
	let last;
	for (;;) {
		await sleep(500);
		observation.polls += 1;
		const hashes = [
			...(writeRecords ?? []).map((record) => record.hash),
			...extraHashes,
		];
		const [statusA, statusB] = await Promise.all([
			A.call("status", {}),
			B.call("status", { hashes }),
		]);
		last = { A: statusA, B: statusB };
		const now = wall();
		for (const [side, status] of [
			["A", statusA],
			["B", statusB],
		]) {
			if (inventoryDone(status)) {
				observation.inventoryDoneAt[side] ??= now;
			} else {
				observation.inventoryDoneAt[side] = null;
			}
		}
		const arrivedAll = writesDone && statusB.arrived === hashes.length;
		if (arrivedAll) observation.allArrivedAt ??= now;
		else observation.allArrivedAt = null;
		const anchor = writesPromise
			? writeRecords?.length
				? writeRecords.at(-1).t1
				: null
			: since;
		if (writesError) {
			observation.endedBy = "writes-error";
			observation.writesError = String(writesError?.message ?? writesError);
			break;
		}
		if (
			writesDone &&
			arrivedAll &&
			inventoryDone(statusA) &&
			inventoryDone(statusB) &&
			anchor !== null &&
			now - anchor >= OBSERVE_MIN_MS
		) {
			observation.endedBy = "complete";
			break;
		}
		if (writesDone && anchor !== null && now - anchor >= OBSERVE_CAP_MS) {
			observation.endedBy = "cap";
			break;
		}
		if (now > absoluteCapAt) {
			observation.endedBy = "absolute-cap";
			break;
		}
	}
	observation.endedAt = wall();
	observation.ms = round(observation.endedAt - observation.startedAt);
	observation.final = {
		A: { inventory: last.A.inventory, idle: last.A.idle },
		B: { inventory: last.B.inventory, idle: last.B.idle, arrived: last.B.arrived },
	};
	result.observe = observation;
	return writeRecords;
};

/** Both sides snapshot their CPU at anchor + CPU_MARK_MS (see the header). */
const armCpuMark = async (result, anchor, anchorAt) => {
	const at = anchorAt + CPU_MARK_MS;
	const [armA, armB] = await both("armCpuMark", { at, label: anchor });
	result.cpuMark = { anchor, anchorAt, at, ms: CPU_MARK_MS, armA, armB };
};

const reconnect = async (phase, result) => {
	const timeline = result.timeline;
	await both("gate");
	timeline.partitionStart = wall();
	const [partitionA, partitionB] = await both("partition", {}, 60_000);
	timeline.partitioned = wall();
	result.partition = { A: partitionA, B: partitionB };
	log(
		`${phase}: partitioned in ${round(timeline.partitioned - timeline.partitionStart)} ms; hold A=${JSON.stringify(partitionA.park)} B=${JSON.stringify(partitionB.park)}`,
	);
	let partitionEntry = null;
	if (PARTITION_MS > 0) {
		const [record] = await A.call("write", {
			count: 1,
			intervalMs: 0,
			label: "partition",
			linked: LINKED,
		});
		partitionEntry = { hash: record.hash, writtenAt: record.t0 };
		const remaining = timeline.partitionStart + PARTITION_MS - wall();
		log(`${phase}: holding the partition ${round(Math.max(0, remaining))} ms more`);
		if (remaining > 0) await sleep(remaining);
	}
	timeline.drainStart = wall();
	const [drainA, drainB] = await both("drainDials", {}, 60_000);
	const [healA, healB] = await both("heal", {}, 60_000);
	timeline.healed = wall();
	result.heal = { drainA, drainB, healA, healB };
	const dial = await B.call("dial", { address: addressA }, 120_000);
	timeline.dialStart = dial.startedAt;
	timeline.dialResolved = dial.resolvedAt;
	result.dial = dial;
	const writesPromise =
		phase === "writes"
			? A.call(
					"write",
					{ count: WRITES, intervalMs: 1_000, label: "writes", linked: LINKED },
					WRITES * 1_000 * 4 + 120_000,
				)
			: null;
	await armCpuMark(result, "dial", dial.startedAt);
	const [handshakeA, handshakeB] = await both("waitHandshake", {}, 120_000);
	timeline.handshakeA = handshakeA.at;
	timeline.handshakeB = handshakeB.at;
	log(
		`${phase}: dial ${round(dial.ms)} ms, handshake A +${round(handshakeA.at - dial.startedAt)} B +${round(handshakeB.at - dial.startedAt)} ms`,
	);
	const writeRecords = await observe({
		result,
		since: dial.startedAt,
		writesPromise,
		extraHashes: partitionEntry ? [partitionEntry.hash] : [],
	});
	result.writeRecords = writeRecords;
	result.partitionEntry = partitionEntry;
};

const control = async (result) => {
	result.timeline.writesStart = wall();
	const writesPromise = A.call(
		"write",
		{ count: WRITES, intervalMs: 1_000, label: "control", linked: LINKED },
		WRITES * 1_000 * 4 + 120_000,
	);
	await armCpuMark(result, "writes-start", result.timeline.writesStart);
	const writeRecords = await observe({
		result,
		since: result.timeline.writesStart,
		writesPromise,
		extraHashes: [],
	});
	result.writeRecords = writeRecords;
};

const runPhase = async (phase) => {
	const result = { phase, timeline: {}, load1: {} };
	log(`${phase}: quiesce`);
	result.quiesce = await quiesceBoth();
	if (!result.quiesce.ok) log(`${phase}: quiesce did not settle; continuing (flagged)`);
	await both("reset", { phase });
	result.startedAt = wall();
	result.load1.start = os.loadavg()[0];
	emit("phase:start", { phase, load1: result.load1.start });
	if (phase === "control") await control(result);
	else await reconnect(phase, result);
	result.load1.end = os.loadavg()[0];
	const [reportA, reportB] = await both("report");
	result.endedAt = wall();
	result.A = reportA;
	result.B = reportB;
	result.writeLatency = writeLatency(result.writeRecords, reportB);
	if (result.writeLatency && result.writeRecords?.length) {
		const first = result.writeRecords[0];
		const arrivedAt =
			reportB.arrivals?.[first.hash] ?? reportB.polledArrivals?.[first.hash] ?? null;
		const anchor = result.timeline.dialStart ?? result.timeline.writesStart;
		result.writeLatency.first = {
			issuedAfterAnchorMs: round(first.t0 - anchor),
			anchor: result.timeline.dialStart ? "dial" : "phase start",
			latencyMs: arrivedAt === null ? null : round(arrivedAt - first.t0),
		};
	}
	if (result.partitionEntry) {
		const arrivedAt =
			reportB.arrivals?.[result.partitionEntry.hash] ??
			reportB.polledArrivals?.[result.partitionEntry.hash] ??
			null;
		result.partitionEntry.arrivedAt = arrivedAt;
		result.partitionEntry.msFromDial =
			arrivedAt === null ? null : round(arrivedAt - result.timeline.dialStart);
		result.partitionEntry.msFromHeal =
			arrivedAt === null ? null : round(arrivedAt - result.timeline.healed);
	}
	const brief = (report) =>
		`refOther=${report.passes.refOther.calls}[${report.passes.refOther.shapes.join(",")}] pages1048=${report.coordinatePages?.pages1048} park=[${report.park.refOther.sequence.join(",")}] cpu=${report.cpuMark?.taken ? `${report.cpuMark.cpuMs}@mark/` : ""}${report.cpuMs}ms invHashes=${report.inventory.pages?.hashes}`;
	log(
		`${phase}: done (${result.observe.endedBy}); A ${brief(reportA)}; B ${brief(reportB)}; write p90=${result.writeLatency?.latencyMs.p90 ?? "-"} ms`,
	);
	emit("phase:end", { phase, endedBy: result.observe.endedBy });
	return result;
};

// ---------------------------------------------------------------------------
// Main

const fileSha256 = (file) =>
	createHash("sha256").update(fs.readFileSync(file)).digest("hex");

const report = {
	schema: "rc-report/1",
	variant: VARIANT,
	n: N,
	min: MIN,
	phases: PHASES,
	writes: WRITES,
	partitionMs: PARTITION_MS,
	long: PARTITION_MS > 0,
	entries: LINKED ? "linked" : "independent-heads",
	options: {
		quietMs: QUIET_MS,
		observeMinMs: OBSERVE_MIN_MS,
		observeCapMs: OBSERVE_CAP_MS,
		convergeTimeoutMs: CONVERGE_TIMEOUT_MS,
		profileLog: PROFILE_LOG,
		runTimeoutMs: RUN_TIMEOUT_MS,
		cpuMarkMs: CPU_MARK_MS,
		// SharedLog's, checked by each worker after open (setup.logTimings).
		logTimings: "production defaults",
	},
	// Where this harness departs from or adds to JUDGED_PLAN.md section 5.
	notes: [
		"SharedLog runs its production timings: each worker drops the test EventStore's distributionDebounceTime 50 and timeUntilRoleMaturity 3000 before log.open and checks the values after open (setup.logTimings).",
		"B constructs the store with A's id and checks the address, instead of opening A's address, so that its log.open is wrapped before it runs.",
		`Entries are ${LINKED ? "linked appends (one coordinate row)" : "independent heads (meta.next = []), one coordinate row each"}.`,
		`cpuMark is each side's CPU at ${CPU_MARK_MS} ms after the dial (after the first write in control); cpuMs is the total at the end of the observation.`,
	],
	root: ROOT,
	build: {
		commit: opts.commit ?? null,
		sharedLogIndexSha256: fileSha256(SHARED_LOG_INDEX),
		sharedLogHasRunPlacementPass: fs
			.readFileSync(SHARED_LOG_INDEX, "utf8")
			.includes("runPlacementPass"),
	},
	host: {
		platform: process.platform,
		arch: process.arch,
		node: process.version,
		cpus: os.cpus().length,
		cpuModel: os.cpus()[0]?.model ?? null,
		totalMemGB: round(os.totalmem() / 2 ** 30, 1),
	},
	startedAt: new Date().toISOString(),
	load1AtStart: os.loadavg()[0],
	setup: {},
	phaseResults: [],
	stop: {},
	errors: {},
	ok: false,
	timedOut: false,
	failures: [],
};

/** After the run deadline, main() must not start anything new. */
const assertBeforeDeadline = (what) => {
	if (report.timedOut) throw new Error(`run deadline passed before ${what}`);
};

const writeReport = () => {
	fs.writeFileSync(
		path.join(OUT, "report.json"),
		`${JSON.stringify(report, (_key, value) => (typeof value === "bigint" ? value.toString() : value), 2)}\n`,
	);
};

const stopWorker = async (worker) => {
	if (!worker) return null;
	if (worker.exited) return { alreadyExited: worker.exited };
	const sentAt = wall();
	let reply = null;
	let error = null;
	try {
		reply = await worker.call("stop", {}, 60_000);
	} catch (stopError) {
		error = String(stopError?.message ?? stopError);
	}
	// A cleared timer, not sleep(): a pending 30 s timer would keep the driver
	// alive that long after both workers have exited.
	let exitWaitTimer;
	const exited = await Promise.race([
		worker.exitPromise,
		new Promise((resolve) => {
			exitWaitTimer = setTimeout(() => resolve(null), 30_000);
		}),
	]);
	clearTimeout(exitWaitTimer);
	let killed = false;
	if (!exited) {
		worker.child.kill("SIGKILL");
		killed = true;
		await worker.exitPromise;
	}
	worker.logStream.end();
	return {
		stopMs: reply?.stopMs ?? null,
		stopError: reply?.error ?? null,
		exitMs: exited ? round(exited.at - sentAt) : null,
		code: worker.exited?.code ?? null,
		signal: worker.exited?.signal ?? null,
		killed,
		error,
		errors: reply?.errors ?? null,
		errorSamples: reply?.errorSamples ?? [],
		activeResourcesAfterStop: reply?.activeResources ?? null,
	};
};

const main = async () => {
	if (CPU_MARK_MS >= OBSERVE_MIN_MS) {
		log(
			`warning: --cpu-mark-ms ${CPU_MARK_MS} is not below --observe-min-ms ${OBSERVE_MIN_MS}; a rest observation can end before the CPU mark`,
		);
	}
	if (WAIT_LOAD !== null) {
		const startedAt = wall();
		while (os.loadavg()[0] >= WAIT_LOAD) {
			if (wall() - startedAt > WAIT_LOAD_TIMEOUT_MS) {
				throw new Error(`load1 stayed at or above ${WAIT_LOAD}`);
			}
			await sleep(5_000);
			assertBeforeDeadline("the workers started");
		}
		report.setup.waitedForLoadMs = round(wall() - startedAt);
	}
	assertBeforeDeadline("the workers started");
	report.load1AtStart = os.loadavg()[0];
	A = new Worker("A");
	B = new Worker("B");
	await Promise.all([A.ready, B.ready]);
	await both("load");
	const setup = report.setup;
	let startedAt = wall();
	const peerA = await A.call("create", { dir: path.join(OUT, "peer-A") });
	const peerB = await B.call("create", { dir: path.join(OUT, "peer-B") });
	addressA = peerA.multiaddrs.find((address) => address.includes("/tcp/")) ?? peerA.multiaddrs[0];
	await A.call("setOther", { peerId: peerB.peerId, hash: peerB.hash });
	await B.call("setOther", { peerId: peerA.peerId, hash: peerA.hash });
	setup.createMs = round(wall() - startedAt);
	const openA = await A.call("open", { min: MIN });
	setup.presence = openA.presence;
	setup.fields = openA.fields;
	setup.logTimings = { A: openA.logTimings };
	setup.droppedFixtureTimings = { A: openA.droppedFixtureTimings };
	startedAt = wall();
	log(`seeding ${N} entries on A`);
	const seed = await A.call(
		"seed",
		{ count: N, batch: 500, linked: LINKED },
		CONVERGE_TIMEOUT_MS,
	);
	setup.seedMs = seed.ms;
	startedAt = wall();
	const dial = await B.call("dial", { address: addressA }, 120_000);
	setup.dialMs = dial.ms;
	const openB = await B.call(
		"open",
		{ address: openA.address, id: openA.id, min: MIN },
		300_000,
	);
	setup.openBMs = openB.ms;
	setup.logTimings.B = openB.logTimings;
	setup.droppedFixtureTimings.B = openB.droppedFixtureTimings;
	setup.store = {
		address: openA.address,
		B: openB.constructedWithId ? "constructed with A's id" : "opened by address",
		addressMatches: openB.address === openA.address,
	};
	const [convergedB, convergedA] = await Promise.all([
		B.call("waitConverged", { count: N, timeoutMs: CONVERGE_TIMEOUT_MS }, CONVERGE_TIMEOUT_MS + 60_000),
		A.call("waitConverged", { count: N, timeoutMs: CONVERGE_TIMEOUT_MS }, CONVERGE_TIMEOUT_MS + 60_000),
	]);
	setup.convergeMs = round(wall() - startedAt);
	setup.lengths = { A: convergedA.length, B: convergedB.length };
	log(`converged: B has ${convergedB.length} entries after ${setup.convergeMs} ms`);
	for (const phase of PHASES) {
		assertBeforeDeadline(`phase ${phase}`);
		const result = await runPhase(phase);
		assertBeforeDeadline(`recording phase ${phase}`);
		report.phaseResults.push(result);
		writeReport();
	}
	report.ok = true;
};

let deadlineTimer = null;
const deadline =
	RUN_TIMEOUT_MS > 0
		? new Promise((_resolve, reject) => {
				deadlineTimer = setTimeout(() => {
					report.timedOut = true;
					emit("run:timeout", { runTimeoutMs: RUN_TIMEOUT_MS });
					reject(new Error(`run timed out after ${RUN_TIMEOUT_MS} ms (--run-timeout-ms)`));
				}, RUN_TIMEOUT_MS);
			})
		: null;
const running = main();
// After the deadline main() is abandoned: its pending worker calls reject once
// the workers stop below. Handle that late rejection here.
running.catch(() => {});
const exitCode = await (deadline ? Promise.race([running, deadline]) : running).then(
	() => 0,
	(error) => {
		report.failures.push({
			error: String(error?.message ?? error),
			stack: error?.stack ?? null,
			workerStack: error?.workerStack ?? null,
		});
		log(`FAILED: ${error?.message ?? error}`);
		return 1;
	},
);
clearTimeout(deadlineTimer);
const [stopA, stopB] = await Promise.all([stopWorker(A), stopWorker(B)]);
if (report.timedOut) report.ok = false;
report.stop = { A: stopA, B: stopB };
report.errors = {
	A: stopA?.errors ?? null,
	B: stopB?.errors ?? null,
	driver: { unhandled: driverErrors.unhandled, uncaught: driverErrors.uncaught },
	driverSamples: driverErrors.samples,
};
report.endedAt = new Date().toISOString();
if (!KEEP_PEERS) {
	for (const side of ["A", "B"]) {
		fs.rmSync(path.join(OUT, `peer-${side}`), { recursive: true, force: true });
	}
}
writeReport();
log(
	`stopped: A exit ${stopA?.exitMs ?? "-"} ms${stopA?.killed ? " (killed)" : ""}, B exit ${stopB?.exitMs ?? "-"} ms${stopB?.killed ? " (killed)" : ""}; report ${path.join(OUT, "report.json")}`,
);
eventsStream.end();
process.exitCode = exitCode;
