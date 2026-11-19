#!/usr/bin/env node
/**
 * rc-worker.mjs: one Peerbit peer per process, driven over IPC by rc-driver.mjs.
 *
 * Measurement only (JUDGED_PLAN.md section 5). It wraps SharedLog instance
 * methods from outside and reads fields; it never changes product code. The
 * one argument it changes is the test EventStore's: the store's log.open is
 * wrapped to drop the fixture's fast test timings, so SharedLog runs with its
 * production defaults (checked after open). Every counter is "since the last
 * reset". Functions or fields a variant does not have are reported as absent
 * (null / present:false), never as zero.
 *
 * Arguments: --root <tree> --side <A|B> --events <file.ndjson>
 * Node 22 or later; imports only the tree's own dist files.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";

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

const argv = parseArgs(process.argv.slice(2));
if (!argv.root || !argv.side || !argv.events) {
	console.error("usage: rc-worker.mjs --root <tree> --side <A|B> --events <file>");
	process.exit(2);
}
const ROOT = path.resolve(argv.root);
const SIDE = argv.side;
/** Optional (diagnostics): profile events whose name matches go to the events log. */
const PROFILE_LOG = argv["profile-log"] ? new RegExp(argv["profile-log"]) : null;

/** Wall clock in ms with sub-ms precision; the same host clock in every process. */
const wall = () => performance.timeOrigin + performance.now();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const fmt = (error) =>
	error instanceof Error
		? `${error.name}: ${error.message}`
		: typeof error === "string"
			? error
			: (() => {
					try {
						return JSON.stringify(error);
					} catch {
						return String(error);
					}
				})();
const jsonSafe = (_key, value) =>
	typeof value === "bigint"
		? value.toString()
		: value instanceof Map
			? Object.fromEntries(value)
			: value instanceof Set
				? [...value]
				: value;

// ---------------------------------------------------------------------------
// State

const state = {
	phase: "setup",
	resetAt: wall(),
	partitioned: false,
	/** { peerId, hash } of the other peer. */
	other: undefined,
	trackArrivals: false,
};
let peer;
let store;
let log;

const eventsStream = fs.createWriteStream(argv.events, { flags: "a" });
let eventsOpen = true;
const emit = (type, data = {}) => {
	if (!eventsOpen) return;
	try {
		eventsStream.write(
			`${JSON.stringify(
				{
					...data,
					// The standard fields win over same-named record fields.
					t: Math.round(wall() * 1000) / 1000,
					rel: Math.round((wall() - state.resetAt) * 10) / 10,
					side: SIDE,
					phase: state.phase,
					type,
				},
				jsonSafe,
			)}\n`,
		);
	} catch {
		// The events log is diagnostics only.
	}
};

// ---------------------------------------------------------------------------
// Counters since the last reset

const freshCounters = () => ({
	errors: { unhandled: 0, uncaught: 0 },
	passes: [],
	passOverlapMax: 0,
	coord: {
		iterateCalls: 0,
		pages: 0,
		pages1048: 0,
		pages1048OutsidePass: 0,
		pages1048InRefOtherPass: 0,
		nextBySize: {},
		unwrappable: 0,
	},
	sweeps: [],
	joinRepairs: [],
	unchangedSuccessorRepairs: [],
	inv: {
		passes: [],
		pageRequests: 0,
		pageHashes: 0,
		pageResults: { true: 0, false: 0, error: 0 },
		pageMs: 0,
		handled: {
			requests: 0,
			hashes: 0,
			confirmed: 0,
			undefinedReplies: 0,
			errors: 0,
			ms: 0,
		},
	},
	park: [],
	parkDiag: { create: [], restore: [] },
	profileByName: {},
	/** sharedLog.placement.pass profile events by details.phase. */
	placementPassProfiles: {},
	membership: [],
	arrivals: new Map(),
	polledArrivals: new Map(),
	writes: [],
});
let counters = freshCounters();

/** Process-lifetime error tallies (also counted per phase). */
const errorTotals = { unhandled: 0, uncaught: 0, samples: [] };
const recordError = (kind, error) => {
	const msg = fmt(error);
	const stack = error instanceof Error ? error.stack : undefined;
	errorTotals[kind] += 1;
	if (errorTotals.samples.length < 20) {
		errorTotals.samples.push({ kind, phase: state.phase, msg, stack });
	}
	counters.errors[kind] += 1;
	emit(kind, { msg, stack });
};
process.on("unhandledRejection", (reason) =>
	recordError("unhandled", reason),
);
process.on("uncaughtException", (error) => recordError("uncaught", error));

/** Process-lifetime marks the partition waits read. */
const marks = {
	unreachableAt: 0,
	leaveAt: 0,
	joinAt: 0,
	parkLog: [],
};

let cpuBase = process.cpuUsage();
/**
 * The CPU snapshot the driver arms for a fixed time after the phase's anchor
 * (the dial, or the first write in control): { label, plannedAt, timer, taken }.
 */
let cpuMark = null;
const eld = monitorEventLoopDelay({ resolution: 10 });
eld.enable();
let loadAtReset = os.loadavg()[0];

const presence = {};
const fields = {};
const inFlightPasses = new Set();
let lastPassEventAt = 0;
let passSeq = 0;
let invSeq = 0;

const relNow = () => Math.round((wall() - state.resetAt) * 10) / 10;

const flattenChanges = (argument) =>
	Array.isArray(argument?.[0]) ? argument.flat() : (argument ?? []);

const peersOf = (iterable) => {
	if (!iterable) return [];
	if (Array.isArray(iterable)) return iterable;
	if (iterable instanceof Set) return [...iterable];
	return []; // never consume a one-shot iterable
};

// ---------------------------------------------------------------------------
// Instrumentation (wrappers on the instance only)

const wrapMethod = (name, make) => {
	const original = log[name];
	if (typeof original !== "function") {
		presence[name] = false;
		return false;
	}
	presence[name] = true;
	log[name] = make(original);
	return true;
};

const installWrappers = () => {
	const otherHash = () => state.other?.hash;

	// Placement passes. On the fix, the debouncer callback and rebalanceAll both
	// reach this through runPlacementPass; on base they call it directly.
	wrapMethod(
		"onReplicationChange",
		(original) =>
			function (changeOrChanges, ...rest) {
				const changes = flattenChanges(changeOrChanges);
				const other = otherHash();
				const types = { added: 0, removed: 0, replaced: 0 };
				const otherTypes = new Set();
				const owners = new Set();
				for (const change of changes) {
					types[change?.type] = (types[change?.type] ?? 0) + 1;
					const owner = change?.range?.hash;
					owners.add(owner);
					if (owner === other) otherTypes.add(change.type);
				}
				const record = {
					seq: ++passSeq,
					start: relNow(),
					end: null,
					ms: null,
					changes: changes.length,
					types,
					refOther: otherTypes.size > 0,
					otherTypes: [...otherTypes].sort(),
					owners: owners.size,
					pages: 0,
					pages1048: 0,
					overlap: inFlightPasses.size,
					outcome: "running",
					// From the pass's own sharedLog.placement.pass profile event:
					// outcome (completed, stale, error), examined entries and mode.
					// The return value cannot tell a completed pass from a stale one.
					profile: null,
				};
				counters.passes.push(record);
				inFlightPasses.add(record);
				counters.passOverlapMax = Math.max(
					counters.passOverlapMax,
					inFlightPasses.size,
				);
				lastPassEventAt = wall();
				const startedAt = performance.now();
				emit("pass:start", {
					seq: record.seq,
					changes: record.changes,
					types,
					refOther: record.refOther,
					otherTypes: record.otherTypes,
				});
				const finish = (settled, error) => {
					record.ms = Math.round((performance.now() - startedAt) * 100) / 100;
					record.end = relNow();
					record.outcome =
						settled === "resolved"
							? (record.profile?.outcome ?? "unprofiled")
							: settled;
					inFlightPasses.delete(record);
					lastPassEventAt = wall();
					emit("pass:end", {
						seq: record.seq,
						ms: record.ms,
						outcome: record.outcome,
						examined: record.profile?.entries ?? null,
						pages: record.pages,
						pages1048: record.pages1048,
						error: error ? fmt(error) : undefined,
					});
				};
				let result;
				try {
					result = original.call(this, changeOrChanges, ...rest);
				} catch (error) {
					finish("throw", error);
					throw error;
				}
				return Promise.resolve(result).then(
					(value) => {
						finish("resolved");
						return value;
					},
					(error) => {
						finish("error", error);
						throw error;
					},
				);
			},
	);

	// Entry-coordinate pages. The rebalance scan reads next(1048)
	// (ranges.ts REBALANCE_ITERATOR_BATCH_SIZE); repair sweeps read next(1000).
	const coordinateIndex = log._entryCoordinatesIndex;
	if (coordinateIndex && typeof coordinateIndex.iterate === "function") {
		presence["entryCoordinatesIndex.iterate"] = true;
		const originalIterate = coordinateIndex.iterate;
		coordinateIndex.iterate = function (...args) {
			const iterator = originalIterate.apply(this, args);
			counters.coord.iterateCalls += 1;
			if (iterator && typeof iterator.next === "function") {
				const originalNext = iterator.next;
				try {
					iterator.next = function (amount, ...rest) {
						const coord = counters.coord;
						coord.pages += 1;
						const key = String(amount);
						coord.nextBySize[key] = (coord.nextBySize[key] ?? 0) + 1;
						for (const record of inFlightPasses) record.pages += 1;
						if (amount === 1048) {
							coord.pages1048 += 1;
							if (inFlightPasses.size === 0) coord.pages1048OutsidePass += 1;
							let inRefOther = false;
							for (const record of inFlightPasses) {
								record.pages1048 += 1;
								if (record.refOther) inRefOther = true;
							}
							if (inRefOther) coord.pages1048InRefOtherPass += 1;
						}
						return originalNext.call(this, amount, ...rest);
					};
				} catch {
					counters.coord.unwrappable += 1;
				}
			}
			return iterator;
		};
	} else {
		presence["entryCoordinatesIndex.iterate"] = false;
	}

	// Repair scheduling. A call without a controller argument is the delayed
	// join-authoritative timer (scheduleJoinAuthoritativeRepair's callback);
	// every direct caller passes one, on both variants.
	wrapMethod(
		"scheduleRepairSweep",
		(original) =>
			function (...args) {
				const options = args[0];
				const peers = peersOf(options?.peers);
				const record = {
					t: relNow(),
					mode: options?.mode,
					peers: peers.length,
					refOther: peers.includes(otherHash()),
					delayed: args.length < 2 || args[1] === undefined,
				};
				counters.sweeps.push(record);
				emit("repair:sweep", record);
				return original.apply(this, args);
			},
	);
	wrapMethod(
		"scheduleJoinAuthoritativeRepair",
		(original) =>
			function (...args) {
				const peers = peersOf(args[0]);
				const record = {
					t: relNow(),
					peers: peers.length,
					refOther: peers.includes(otherHash()),
				};
				counters.joinRepairs.push(record);
				emit("repair:join-authoritative", record);
				return original.apply(this, args);
			},
	);
	// Exists only on the fix (on base the same statements are inline).
	wrapMethod(
		"scheduleUnchangedSuccessorRepair",
		(original) =>
			function (...args) {
				const record = { t: relNow(), refOther: args[0] === otherHash() };
				counters.unchangedSuccessorRepairs.push(record);
				emit("repair:unchanged-successor", record);
				return original.apply(this, args);
			},
	);

	// Entry inventory.
	wrapMethod(
		"recoverPeerEntryInventory",
		(original) =>
			function (...args) {
				const pass = original.apply(this, args);
				const record = {
					id: ++invSeq,
					peer: args[0],
					refOther: args[0] === otherHash(),
					created: relNow(),
					pages: 0,
					results: { more: 0, complete: 0, retry: 0, error: 0 },
					completedAt: null,
					ms: 0,
				};
				counters.inv.passes.push(record);
				emit("inventory:pass", { id: record.id, refOther: record.refOther });
				if (pass && typeof pass.next === "function") {
					const originalNext = pass.next;
					pass.next = async function (...nextArgs) {
						const startedAt = performance.now();
						try {
							const result = await originalNext.apply(this, nextArgs);
							record.pages += 1;
							record.results[result] = (record.results[result] ?? 0) + 1;
							if (result === "complete") {
								record.completedAt = relNow();
								emit("inventory:complete", {
									id: record.id,
									refOther: record.refOther,
									pages: record.pages,
									results: record.results,
								});
							} else if (result === "retry") {
								emit("inventory:retry", { id: record.id, pages: record.pages });
							}
							return result;
						} catch (error) {
							record.results.error += 1;
							throw error;
						} finally {
							record.ms += performance.now() - startedAt;
						}
					};
				}
				return pass;
			},
	);
	wrapMethod(
		"recoverEntryInventoryPage",
		(original) =>
			async function (...args) {
				const inv = counters.inv;
				inv.pageRequests += 1;
				inv.pageHashes += Array.isArray(args[2]) ? args[2].length : 0;
				const startedAt = performance.now();
				try {
					const result = await original.apply(this, args);
					inv.pageResults[result === true ? "true" : "false"] += 1;
					return result;
				} catch (error) {
					inv.pageResults.error += 1;
					throw error;
				} finally {
					inv.pageMs += performance.now() - startedAt;
				}
			},
	);
	wrapMethod(
		"handleRequestEntryInventoryV1",
		(original) =>
			async function (...args) {
				const handled = counters.inv.handled;
				handled.requests += 1;
				handled.hashes += args[0]?.hashes?.length ?? 0;
				const startedAt = performance.now();
				try {
					const result = await original.apply(this, args);
					if (result === undefined) handled.undefinedReplies += 1;
					else handled.confirmed += result?.hashes?.length ?? 0;
					return result;
				} catch (error) {
					handled.errors += 1;
					throw error;
				} finally {
					handled.ms += performance.now() - startedAt;
				}
			},
	);

	// Hold diagnostics (fix only; the outcomes themselves come from the profile).
	wrapMethod(
		"tryCreateTransportLossPark",
		(original) =>
			async function (...args) {
				const result = await original.apply(this, args);
				const record = {
					t: relNow(),
					refOther: args[0] === otherHash(),
					rows: Array.isArray(args[1]) ? args[1].length : null,
					created: result !== undefined,
				};
				counters.parkDiag.create.push(record);
				emit("park:create", record);
				return result;
			},
	);
	wrapMethod(
		"tryRestoreTransportLossPark",
		(original) =>
			async function (...args) {
				const result = await original.apply(this, args);
				const record = {
					t: relNow(),
					refOther: args[0] === otherHash(),
					diffs: Array.isArray(args[2]) ? args[2].length : null,
					restored: result === true,
				};
				counters.parkDiag.restore.push(record);
				emit("park:restore", record);
				return result;
			},
	);
	presence.releaseTransportLossPark =
		typeof log.releaseTransportLossPark === "function";
	presence.runPlacementPass = typeof log.runPlacementPass === "function";

	// Arrival time per hash: SharedLog.onChange runs when the lower log commits
	// an append or a join (the lower log calls this.onChange on the instance).
	wrapMethod(
		"onChange",
		(original) =>
			function (change, ...rest) {
				if (state.trackArrivals) {
					const t = wall();
					for (const added of change?.added ?? []) {
						const hash = added?.entry?.hash ?? added?.hash;
						if (hash && !counters.arrivals.has(hash)) {
							counters.arrivals.set(hash, t);
						}
					}
				}
				return original.call(this, change, ...rest);
			},
	);

	fields._transportLossParks = log._transportLossParks instanceof Map;
	fields._placementPassesInFlight = log._placementPassesInFlight instanceof Set;
	fields._entryInventoryRecovery =
		!!log._entryInventoryRecovery?.pending && !!log._entryInventoryRecovery?.active;
	fields.replicationChangeDebounceFn =
		typeof log.replicationChangeDebounceFn?.size === "function";
	fields._checkedPrune = !!log._checkedPrune?.pendingDeletes;
	fields._joinAuthoritativeRepairTimersByDelay =
		log._joinAuthoritativeRepairTimersByDelay instanceof Map;
	fields._repairSweepRunning = typeof log._repairSweepRunning === "boolean";
	fields._repairFrontierActiveTargetsByMode =
		log._repairFrontierActiveTargetsByMode instanceof Map;

	// Membership.
	log.events.addEventListener("replicator:leave", (event) => {
		const hash = event.detail?.publicKey?.hashcode?.();
		const refOther = hash === state.other?.hash;
		if (refOther) marks.leaveAt = wall();
		counters.membership.push({ t: relNow(), type: "leave", refOther });
		emit("replicator:leave", { refOther });
	});
	log.events.addEventListener("replicator:join", (event) => {
		const hash = event.detail?.publicKey?.hashcode?.();
		const refOther = hash === state.other?.hash;
		if (refOther) marks.joinAt = wall();
		counters.membership.push({ t: relNow(), type: "join", refOther });
		emit("replicator:join", { refOther });
	});
	peer.services.pubsub.addEventListener("unsubscribe", ({ detail }) => {
		if (!detail?.topics?.includes(log.topic)) return;
		const refOther = detail.from?.hashcode?.() === state.other?.hash;
		if (refOther && detail.reason === "peer-unreachable") {
			marks.unreachableAt = wall();
		}
		counters.membership.push({
			t: relNow(),
			type: "unsubscribe",
			reason: detail.reason ?? null,
			refOther,
		});
		emit("pubsub:unsubscribe", { reason: detail.reason ?? null, refOther });
	});
	peer.services.pubsub.addEventListener("subscribe", ({ detail }) => {
		if (!detail?.topics?.includes(log.topic)) return;
		const refOther = detail.from?.hashcode?.() === state.other?.hash;
		counters.membership.push({ t: relNow(), type: "subscribe", refOther });
		emit("pubsub:subscribe", { refOther });
	});
};

/** The sync profile sink. Park outcomes come from here. */
const profile = (event) => {
	try {
		const byName = counters.profileByName;
		byName[event.name] = (byName[event.name] ?? 0) + 1;
		if (event.name === "sharedLog.transportLoss.park") {
			const record = {
				t: relNow(),
				wall: wall(),
				refOther: event.peer === state.other?.hash,
				outcome: event.details?.outcome,
				reason: event.details?.reason ?? null,
				rows: event.count,
				component: event.component,
			};
			counters.park.push(record);
			marks.parkLog.push(record);
			emit("park", record);
		} else if (event.name === "sharedLog.placement.pass") {
			const phase = event.details?.phase ?? "none";
			const byPhase = counters.placementPassProfiles;
			byPhase[phase] = (byPhase[phase] ?? 0) + 1;
			// onReplicationChange emits phase "range-change" in its finally, before
			// its promise settles: attach that to the pass still in flight (by
			// change count when several overlap). runRepairSweep emits the same
			// name with phase "repair-sweep"; it belongs to no pass.
			const running =
				phase === "range-change"
					? [...inFlightPasses].filter((record) => !record.profile)
					: [];
			const target =
				running.length <= 1
					? running[0]
					: (running.find((record) => record.changes === event.details?.changes) ??
						running.at(-1));
			if (target) {
				target.profile = {
					outcome: event.details?.outcome ?? null,
					entries: event.entries ?? null,
					repairCandidates: event.count ?? null,
					forceFresh: event.details?.forceFreshDelivery ?? null,
					joinWarmup: event.details?.joinWarmupFastPath ?? null,
					durationMs:
						typeof event.durationMs === "number"
							? Math.round(event.durationMs * 100) / 100
							: null,
				};
			}
		}
		if (PROFILE_LOG?.test(event.name)) {
			emit("profile", {
				name: event.name,
				refOther: event.peer === undefined ? null : event.peer === state.other?.hash,
				entries: event.entries ?? null,
				count: event.count ?? null,
				targets: event.targets ?? null,
				durationMs:
					typeof event.durationMs === "number"
						? Math.round(event.durationMs * 100) / 100
						: null,
				details: event.details ?? null,
			});
		}
	} catch {
		// Diagnostics never change behaviour.
	}
};

// ---------------------------------------------------------------------------
// Readers

const otherConnections = () =>
	peer.libp2p
		.getConnections()
		.filter((connection) => connection.remotePeer.toString() === state.other.peerId);

const otherDials = () =>
	peer.libp2p
		.getDialQueue()
		.filter((dial) => dial.peerId?.toString() === state.other.peerId);

const handshakeOk = () =>
	log._v2Receive?._receiveStates?.get(state.other.hash)?.phase === "active" &&
	log._v2Send?._sendStates?.get(state.other.hash)?.established === true;

const idleState = () => {
	const s = {};
	s.debouncer = fields.replicationChangeDebounceFn
		? log.replicationChangeDebounceFn.size()
		: null;
	s.passesInFlight = inFlightPasses.size;
	s.placementPassesInFlight = log._placementPassesInFlight?.size ?? null;
	s.parks = log._transportLossParks?.size ?? null;
	const inventory = log._entryInventoryRecovery;
	s.inventoryPending = inventory?.pending?.size ?? null;
	s.inventoryActive = inventory?.active?.size ?? null;
	s.pruneDebouncer = log.pruneDebouncedFn?.size?.() ?? null;
	const checked = log._checkedPrune;
	s.checkedPrune = checked
		? (checked.pendingDeletes?.size ?? 0) +
			(checked.sessions?.size ?? 0) +
			(checked.retries?.size ?? 0) +
			(checked.restartReservations?.size ?? 0) +
			(checked.candidateTokens?.size ?? 0)
		: null;
	s.repairSweepRunning = log._repairSweepRunning ?? null;
	s.joinRepairTimers = log._joinAuthoritativeRepairTimersByDelay?.size ?? null;
	// A frontier runner keeps retrying its sends on its own schedule (1, 3, 7,
	// 15, 30, 60 s, then every 30 s) after the sweep that started it, for as
	// long as its frontier holds unconfirmed hashes.
	const frontier = log._repairFrontierActiveTargetsByMode;
	s.frontierRunners =
		frontier instanceof Map
			? [...frontier.values()].reduce((total, targets) => total + (targets?.size ?? 0), 0)
			: null;
	s.idle =
		(s.debouncer ?? 0) === 0 &&
		s.passesInFlight === 0 &&
		(s.placementPassesInFlight ?? 0) === 0 &&
		(s.parks ?? 0) === 0 &&
		(s.inventoryPending ?? 0) === 0 &&
		(s.inventoryActive ?? 0) === 0 &&
		(s.pruneDebouncer ?? 0) === 0 &&
		(s.checkedPrune ?? 0) === 0 &&
		s.repairSweepRunning !== true &&
		(s.joinRepairTimers ?? 0) === 0 &&
		(s.frontierRunners ?? 0) === 0;
	return s;
};

const inventoryStatus = () => {
	const passes = counters.inv.passes;
	const completed = passes.filter((pass) => pass.completedAt !== null);
	const scheduler = log._entryInventoryRecovery;
	return {
		passes: presence.recoverPeerEntryInventory ? passes.length : null,
		completed: presence.recoverPeerEntryInventory ? completed.length : null,
		lastCompleteAt: completed.length
			? Math.max(...completed.map((pass) => pass.completedAt))
			: null,
		pending: scheduler?.pending?.size ?? null,
		active: scheduler?.active?.size ?? null,
	};
};

const waitFor = async (check, { timeoutMs, intervalMs = 50, label }) => {
	const startedAt = wall();
	for (;;) {
		let ok = false;
		try {
			ok = await check();
		} catch {
			ok = false;
		}
		if (ok) return Math.round((wall() - startedAt) * 10) / 10;
		if (wall() - startedAt > timeoutMs) {
			throw new Error(`timed out after ${timeoutMs} ms: ${label}`);
		}
		await sleep(intervalMs);
	}
};

// ---------------------------------------------------------------------------
// Report

const quantile = (sorted, q) => {
	if (sorted.length === 0) return null;
	const index = Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1);
	return sorted[Math.max(0, index)];
};
const round = (value, digits = 2) =>
	value === null || value === undefined
		? value
		: Math.round(value * 10 ** digits) / 10 ** digits;

const summarizePasses = () => {
	const records = counters.passes;
	const refOther = records.filter((record) => record.refOther);
	const shape = (record) => record.otherTypes.join("+");
	const sum = (list) => round(list.reduce((total, r) => total + (r.ms ?? 0), 0));
	const examined = (list) =>
		list.reduce((total, r) => total + (r.profile?.entries ?? 0), 0);
	return {
		present: presence.onReplicationChange,
		calls: presence.onReplicationChange ? records.length : null,
		ms: sum(records),
		examined: examined(records),
		pages1048: records.reduce((total, r) => total + r.pages1048, 0),
		outcomes: countBy(records, "outcome"),
		overlapMax: counters.passOverlapMax,
		refOther: {
			calls: refOther.length,
			ms: sum(refOther),
			examined: examined(refOther),
			pages1048: refOther.reduce((total, r) => total + r.pages1048, 0),
			outcomes: countBy(refOther, "outcome"),
			shapes: refOther.map(shape),
			removedOnly: refOther.filter((r) => shape(r) === "removed").length,
			addedOnly: refOther.filter((r) => shape(r) === "added").length,
			mixed: refOther.filter(
				(r) => shape(r) !== "removed" && shape(r) !== "added",
			).length,
			removedOnlyWithPage: refOther.filter(
				(r) => shape(r) === "removed" && r.pages1048 > 0,
			).length,
			addedOnlyWithPage: refOther.filter(
				(r) => shape(r) === "added" && r.pages1048 > 0,
			).length,
		},
		records,
	};
};

const countBy = (list, key) => {
	const out = {};
	for (const item of list) {
		const k = typeof key === "function" ? key(item) : item[key];
		out[k] = (out[k] ?? 0) + 1;
	}
	return out;
};

const summarizeRepair = () => {
	const byMode = {};
	for (const sweep of counters.sweeps) {
		const entry = (byMode[sweep.mode] ??= {
			calls: 0,
			direct: 0,
			delayed: 0,
			refOther: 0,
			directRefOther: 0,
		});
		entry.calls += 1;
		entry[sweep.delayed ? "delayed" : "direct"] += 1;
		if (sweep.refOther) entry.refOther += 1;
		if (sweep.refOther && !sweep.delayed) entry.directRefOther += 1;
	}
	return {
		present: {
			scheduleRepairSweep: presence.scheduleRepairSweep,
			scheduleJoinAuthoritativeRepair: presence.scheduleJoinAuthoritativeRepair,
			scheduleUnchangedSuccessorRepair: presence.scheduleUnchangedSuccessorRepair,
		},
		sweepByMode: presence.scheduleRepairSweep ? byMode : null,
		sweeps: presence.scheduleRepairSweep ? counters.sweeps : null,
		joinAuthoritative: presence.scheduleJoinAuthoritativeRepair
			? {
					calls: counters.joinRepairs.length,
					refOther: counters.joinRepairs.filter((r) => r.refOther).length,
					records: counters.joinRepairs,
				}
			: null,
		unchangedSuccessor: presence.scheduleUnchangedSuccessorRepair
			? {
					calls: counters.unchangedSuccessorRepairs.length,
					refOther: counters.unchangedSuccessorRepairs.filter((r) => r.refOther)
						.length,
				}
			: null,
	};
};

const summarizeInventory = () => {
	const inv = counters.inv;
	const passes = inv.passes;
	const pageResults = { more: 0, complete: 0, retry: 0, error: 0 };
	for (const pass of passes) {
		for (const [key, value] of Object.entries(pass.results)) {
			pageResults[key] = (pageResults[key] ?? 0) + value;
		}
	}
	const completed = passes.filter((pass) => pass.completedAt !== null);
	return {
		present: {
			recoverPeerEntryInventory: presence.recoverPeerEntryInventory,
			recoverEntryInventoryPage: presence.recoverEntryInventoryPage,
			handleRequestEntryInventoryV1: presence.handleRequestEntryInventoryV1,
		},
		passes: presence.recoverPeerEntryInventory
			? {
					count: passes.length,
					refOther: passes.filter((pass) => pass.refOther).length,
					completed: completed.length,
					firstCreatedAt: passes.length ? passes[0].created : null,
					lastCompleteAt: completed.length
						? Math.max(...completed.map((pass) => pass.completedAt))
						: null,
					pages: passes.reduce((total, pass) => total + pass.pages, 0),
					pageResults,
					ms: round(passes.reduce((total, pass) => total + pass.ms, 0)),
					records: passes.map((pass) => ({ ...pass, ms: round(pass.ms) })),
				}
			: null,
		pages: presence.recoverEntryInventoryPage
			? {
					requests: inv.pageRequests,
					hashes: inv.pageHashes,
					results: inv.pageResults,
					ms: round(inv.pageMs),
				}
			: null,
		handled: presence.handleRequestEntryInventoryV1
			? { ...inv.handled, ms: round(inv.handled.ms) }
			: null,
		scheduler: {
			pending: log._entryInventoryRecovery?.pending?.size ?? null,
			active: log._entryInventoryRecovery?.active?.size ?? null,
		},
	};
};

const summarizePark = () => {
	const events = counters.park;
	const refOther = events.filter((event) => event.refOther);
	const label = (event) =>
		event.reason ? `${event.outcome}:${event.reason}` : event.outcome;
	return {
		supported: fields._transportLossParks,
		events,
		refOther: {
			sequence: refOther.map(label),
			parked: refOther.filter((e) => e.outcome === "parked").length,
			restored: refOther.filter((e) => e.outcome === "restored").length,
			released: refOther.filter((e) => e.outcome === "released").length,
			releasedByReason: countBy(
				refOther.filter((e) => e.outcome === "released"),
				(e) => e.reason ?? "none",
			),
		},
		diag: {
			create: presence.tryCreateTransportLossPark ? counters.parkDiag.create : null,
			restore: presence.tryRestoreTransportLossPark
				? counters.parkDiag.restore
				: null,
		},
	};
};

/**
 * CPU since the reset at the armed time, with the pass and inventory work done
 * by then. A pass still in flight counts its time so far.
 */
const takeCpuMark = () => {
	if (!cpuMark || cpuMark.taken) return;
	clearTimeout(cpuMark.timer);
	cpuMark.timer = null;
	const cpu = process.cpuUsage(cpuBase);
	const now = wall();
	const rel = relNow();
	const invPasses = counters.inv.passes;
	cpuMark.taken = {
		at: now,
		rel: round(now - state.resetAt, 1),
		lateMs: round(now - cpuMark.plannedAt, 1),
		cpuMs: round((cpu.user + cpu.system) / 1000, 1),
		cpuUserMs: round(cpu.user / 1000, 1),
		cpuSystemMs: round(cpu.system / 1000, 1),
		passCalls: counters.passes.length,
		passMs: round(
			counters.passes.reduce(
				(total, record) => total + (record.ms ?? Math.max(0, rel - record.start)),
				0,
			),
		),
		passesInFlight: inFlightPasses.size,
		invPasses: presence.recoverPeerEntryInventory ? invPasses.length : null,
		invCompleted: presence.recoverPeerEntryInventory
			? invPasses.filter((pass) => pass.completedAt !== null).length
			: null,
		invActive: log._entryInventoryRecovery?.active?.size ?? null,
		invMs: presence.recoverPeerEntryInventory
			? round(invPasses.reduce((total, pass) => total + pass.ms, 0))
			: null,
		invHashes: presence.recoverEntryInventoryPage ? counters.inv.pageHashes : null,
	};
	emit("cpu:mark", { label: cpuMark.label, ...cpuMark.taken });
};

const cpuMarkReport = () => {
	if (!cpuMark) return null;
	// A timer the event loop has not reached yet is taken now, and says how late.
	if (!cpuMark.taken && wall() >= cpuMark.plannedAt) takeCpuMark();
	return {
		label: cpuMark.label,
		plannedAt: cpuMark.plannedAt,
		plannedRel: round(cpuMark.plannedAt - state.resetAt, 1),
		taken: cpuMark.taken !== null,
		...(cpuMark.taken ?? {}),
	};
};

const clearCpuMark = () => {
	if (cpuMark?.timer) clearTimeout(cpuMark.timer);
	cpuMark = null;
};

const buildReport = () => {
	const mark = cpuMarkReport();
	const cpu = process.cpuUsage(cpuBase);
	const memory = process.memoryUsage();
	return {
		side: SIDE,
		phase: state.phase,
		resetAt: state.resetAt,
		reportedAt: wall(),
		durationMs: round(wall() - state.resetAt, 1),
		cpuMs: round((cpu.user + cpu.system) / 1000, 1),
		cpuUserMs: round(cpu.user / 1000, 1),
		cpuSystemMs: round(cpu.system / 1000, 1),
		cpuMark: mark,
		eventLoopDelayMs: {
			p50: round(eld.percentile(50) / 1e6, 3),
			p99: round(eld.percentile(99) / 1e6, 3),
			max: round(eld.max / 1e6, 3),
			mean: round(eld.mean / 1e6, 3),
		},
		loadavg1: { start: loadAtReset, end: os.loadavg()[0] },
		rssMB: round(memory.rss / 1048576, 1),
		heapUsedMB: round(memory.heapUsed / 1048576, 1),
		errors: { ...counters.errors },
		presence: { ...presence },
		fields: { ...fields },
		passes: summarizePasses(),
		coordinatePages: presence["entryCoordinatesIndex.iterate"]
			? { ...counters.coord }
			: null,
		repair: summarizeRepair(),
		inventory: summarizeInventory(),
		park: summarizePark(),
		profileEventsByName: counters.profileByName,
		placementPassProfiles: counters.placementPassProfiles,
		membership: counters.membership,
		idle: idleState(),
		logLength: store?.log?.log?.length ?? null,
		arrivals: Object.fromEntries(counters.arrivals),
		polledArrivals: Object.fromEntries(counters.polledArrivals),
		writes: counters.writes,
	};
};

// ---------------------------------------------------------------------------
// Commands

let EventStore;
let Peerbit;

/**
 * Each entry is its own head (no parents), as a document put is, so every
 * entry has its own coordinate row. Linked appends (the EventStore default)
 * chain onto the previous head and leave a single coordinate row however many
 * entries there are, which leaves the rebalance scan and the inventory walk
 * nothing to measure.
 */
const appendOptions = (linked) => (linked ? undefined : { meta: { next: [] } });

/**
 * The plan measures SharedLog's production defaults, but the test EventStore's
 * open() passes log.open distributionDebounceTime 50 and timeUntilRoleMaturity
 * 3000 ("to make tests fast"). The store instance's log.open is therefore
 * wrapped before the store opens, to drop both keys so SharedLog applies its
 * own defaults; open() then checks the values SharedLog ended up with. The
 * fixture's waitForPruneDelay 0 already equals WAIT_FOR_PRUNE_DELAY. Setting
 * the fields after open would not do: the change debouncer captures
 * distributionDebounceTime when open() creates it.
 */
const FIXTURE_TIMING_KEYS = ["distributionDebounceTime", "timeUntilRoleMaturity"];
/**
 * SharedLog's defaults, the same on both variants (src/index.ts
 * DEFAULT_DISTRIBUTION_DEBOUNCE_TIME, WAIT_FOR_ROLE_MATURITY,
 * WAIT_FOR_PRUNE_DELAY).
 */
const PRODUCTION_TIMINGS = {
	distributionDebounceTime: 500,
	timeUntilRoleMaturity: 5_000,
	waitForPruneDelay: 0,
};

/** Wraps program.log.open on the instance; returns what it dropped (filled when open runs). */
const dropFixtureTimings = (program) => {
	const sharedLog = program.log;
	if (typeof sharedLog?.open !== "function" || Object.hasOwn(sharedLog, "open")) {
		throw new Error("the store's log has no prototype open() to wrap");
	}
	const original = sharedLog.open;
	const dropped = {};
	sharedLog.open = function (options, ...rest) {
		const next = { ...(options ?? {}) };
		for (const key of FIXTURE_TIMING_KEYS) {
			if (Object.hasOwn(next, key)) {
				dropped[key] = next[key];
				delete next[key];
			}
		}
		return original.call(this, next, ...rest);
	};
	return dropped;
};

const readLogTimings = () =>
	Object.fromEntries(Object.keys(PRODUCTION_TIMINGS).map((key) => [key, log[key] ?? null]));

const commands = {
	async load() {
		const peerbitUrl = pathToFileURL(
			path.join(ROOT, "packages/clients/peerbit/dist/src/index.js"),
		).href;
		const storeUrl = pathToFileURL(
			path.join(
				ROOT,
				"packages/programs/data/shared-log/dist/test/utils/stores/event-store.js",
			),
		).href;
		({ Peerbit } = await import(peerbitUrl));
		({ EventStore } = await import(storeUrl));
		return { node: process.version, pid: process.pid };
	},

	async create({ dir }) {
		peer = await Peerbit.create({
			directory: dir,
			libp2p: {
				addresses: { listen: ["/ip4/127.0.0.1/tcp/0"] },
				connectionGater: {
					denyDialPeer: async () => state.partitioned,
					denyInboundConnection: async () => state.partitioned,
				},
			},
		});
		return {
			peerId: peer.libp2p.peerId.toString(),
			hash: peer.identity.publicKey.hashcode(),
			multiaddrs: peer.getMultiaddrs().map((address) => address.toString()),
		};
	},

	async setOther({ peerId, hash }) {
		state.other = { peerId, hash };
		return true;
	},

	/**
	 * A constructs the store with a fresh id. B constructs it with A's id (hex)
	 * instead of opening A's address, so that its log.open can be wrapped before
	 * it runs, and checks that it got A's address. Both open with SharedLog's
	 * production timings (see dropFixtureTimings) and fail unless SharedLog
	 * reports them.
	 */
	async open({ address, id, min }) {
		if (address && typeof id !== "string") {
			throw new Error("B's open needs A's store id");
		}
		const args = { replicas: { min }, replicate: 1, sync: { profile } };
		const startedAt = wall();
		const program = address
			? new EventStore({ id: Uint8Array.from(Buffer.from(id, "hex")) })
			: new EventStore();
		const droppedFixtureTimings = dropFixtureTimings(program);
		store = await peer.open(program, { args });
		log = store.log;
		if (address && String(store.address) !== address) {
			throw new Error(`B's store has address ${store.address}, A's is ${address}`);
		}
		const logTimings = readLogTimings();
		const wrong = Object.keys(PRODUCTION_TIMINGS).filter(
			(key) => logTimings[key] !== PRODUCTION_TIMINGS[key],
		);
		if (wrong.length > 0) {
			throw new Error(
				`SharedLog ${wrong.join(", ")} not at the production default: got ${JSON.stringify(logTimings)}, expected ${JSON.stringify(PRODUCTION_TIMINGS)} (fixture values dropped: ${JSON.stringify(droppedFixtureTimings)})`,
			);
		}
		installWrappers();
		emit("open", { presence, fields, logTimings, droppedFixtureTimings });
		return {
			address: String(store.address),
			id: Buffer.from(store.id).toString("hex"),
			constructedWithId: Boolean(address),
			topic: log.topic,
			ms: round(wall() - startedAt, 1),
			logTimings,
			droppedFixtureTimings,
			presence,
			fields,
		};
	},

	async seed({ count, batch = 500, linked = false }) {
		const startedAt = wall();
		for (let i = 0; i < count; i++) {
			await store.add(`seed ${i}`, appendOptions(linked));
			if ((i + 1) % batch === 0) {
				process.send?.({ event: "progress", side: SIDE, what: "seed", done: i + 1, count });
				await new Promise((resolve) => setImmediate(resolve));
			}
		}
		return { ms: round(wall() - startedAt, 1), length: store.log.log.length };
	},

	async dial({ address, attempts = 3, timeoutMs = 20_000 }) {
		const startedAt = wall();
		const failures = [];
		for (let attempt = 1; attempt <= attempts; attempt++) {
			try {
				await peer.dial(address);
				return {
					ms: round(wall() - startedAt, 1),
					startedAt,
					resolvedAt: wall(),
					attempts: attempt,
					failures,
				};
			} catch (error) {
				failures.push(fmt(error));
				emit("dial:failed", { attempt, error: fmt(error) });
				// A gated job may still be retiring; let it leave before retrying.
				await waitFor(() => otherDials().length === 0, {
					timeoutMs,
					label: "dial queue drained before retry",
				}).catch(() => {});
			}
		}
		throw new Error(`dial failed ${attempts} times: ${failures.join(" | ")}`);
	},

	async waitConverged({ count, timeoutMs = 1_800_000 }) {
		const ms = await waitFor(
			async () =>
				store.log.log.length >= count &&
				handshakeOk() &&
				(await log.getReplicators()).has(state.other.hash),
			{ timeoutMs, intervalMs: 250, label: `converged at ${count} entries` },
		);
		return { ms, length: store.log.log.length };
	},

	idle() {
		return idleState();
	},

	/**
	 * Debouncer empty, no pass in flight, no inventory pass running or queued,
	 * no prune work, no repair sweep running or delayed join-authoritative sweep
	 * pending, no repair-frontier runner active; then quietMs without a new pass.
	 */
	async quiesce({ quietMs = 5_000, timeoutMs = 300_000 }) {
		const startedAt = wall();
		let stableSince = null;
		let seenPassAt = lastPassEventAt;
		let last;
		for (;;) {
			last = idleState();
			if (!last.idle || lastPassEventAt !== seenPassAt) {
				stableSince = null;
				seenPassAt = lastPassEventAt;
			} else if (stableSince === null) {
				stableSince = wall();
			}
			if (stableSince !== null && wall() - stableSince >= quietMs) {
				return { ok: true, waitedMs: round(wall() - startedAt, 1), state: last };
			}
			if (wall() - startedAt > timeoutMs) {
				return { ok: false, waitedMs: round(wall() - startedAt, 1), state: last };
			}
			await sleep(100);
		}
	},

	reset({ phase }) {
		clearCpuMark();
		state.phase = phase;
		state.resetAt = wall();
		counters = freshCounters();
		cpuBase = process.cpuUsage();
		eld.reset();
		loadAtReset = os.loadavg()[0];
		state.trackArrivals = true;
		emit("reset", { loadavg1: loadAtReset });
		return { resetAt: state.resetAt, loadavg1: loadAtReset };
	},

	/**
	 * Arm the phase's CPU snapshot for wall time `at` (the same host clock in
	 * both workers). The report carries it as cpuMark; a reset discards it.
	 */
	armCpuMark({ at, label }) {
		clearCpuMark();
		cpuMark = { label, plannedAt: at, timer: null, taken: null };
		cpuMark.timer = setTimeout(takeCpuMark, Math.max(0, at - wall()));
		return { armedAt: wall(), inMs: round(at - wall(), 1) };
	},

	/** Close the gate first on both sides, so no redial can slip through. */
	gate() {
		state.partitioned = true;
		emit("gate");
		return { at: wall() };
	},

	/**
	 * Hang up the other peer while the gater denies redials, then wait for its
	 * peer-unreachable unsubscribe on the log topic, no connection, and its
	 * replicator:leave. Where holds exist, also wait (bounded) for this
	 * departure's hold outcome: parked, or released (declined at the queue site).
	 */
	async partition({ timeoutMs = 10_000, parkWaitMs = 5_000 }) {
		state.partitioned = true;
		const startedAt = wall();
		const remotes = otherConnections().map((connection) => connection.remotePeer);
		if (remotes.length > 0) {
			await peer.libp2p.hangUp(remotes[0]).catch((error) => {
				emit("hangUp:error", { error: fmt(error) });
			});
		}
		await waitFor(
			() =>
				otherConnections().length === 0 &&
				marks.unreachableAt >= startedAt &&
				marks.leaveAt >= startedAt,
			{ timeoutMs, label: "peer-unreachable unsubscribe, no connection and leave" },
		);
		const out = {
			startedAt,
			unreachableAt: marks.unreachableAt,
			leaveAt: marks.leaveAt,
			park: null,
			parkWaitMs: null,
		};
		if (fields._transportLossParks) {
			const hold = () =>
				marks.parkLog.find(
					(event) =>
						event.refOther &&
						event.wall >= startedAt &&
						(event.outcome === "parked" || event.outcome === "released"),
				);
			out.parkWaitMs = await waitFor(() => hold() !== undefined, {
				timeoutMs: parkWaitMs,
				label: "hold outcome",
			}).catch(() => null);
			const event = hold();
			out.park = event
				? { outcome: event.outcome, reason: event.reason, at: event.wall }
				: null;
		}
		out.ms = round(wall() - startedAt, 1);
		emit("partition:done", out);
		return out;
	},

	async drainDials({ timeoutMs = 10_000 }) {
		const ms = await waitFor(() => otherDials().length === 0, {
			timeoutMs,
			label: "partition-era dials retired",
		});
		return { ms };
	},

	/** Lift the gate in the same turn that sees no dial to the other peer queued. */
	async heal({ timeoutMs = 10_000 }) {
		const ms = await waitFor(
			() => {
				if (otherDials().length !== 0) return false;
				state.partitioned = false;
				return true;
			},
			{ timeoutMs, label: "dial queue empty at heal" },
		);
		emit("heal");
		return { ms, at: wall() };
	},

	async waitHandshake({ timeoutMs = 30_000 }) {
		const ms = await waitFor(
			async () =>
				handshakeOk() && (await log.getReplicators()).has(state.other.hash),
			{ timeoutMs, label: "renewed replication-info handshake" },
		);
		return { ms, at: wall() };
	},

	async write({ count, intervalMs = 1_000, label = "write", linked = false }) {
		const startedAt = wall();
		const records = [];
		for (let i = 0; i < count; i++) {
			const due = startedAt + i * intervalMs;
			const wait = due - wall();
			if (wait > 0) await sleep(wait);
			const t0 = wall();
			const { entry } = await store.add(
				`${label} ${i} ${Math.round(t0)}`,
				appendOptions(linked),
			);
			const t1 = wall();
			const record = { i, label, hash: entry.hash, t0, t1, appendMs: round(t1 - t0) };
			records.push(record);
			counters.writes.push(record);
		}
		emit("write:done", { label, count });
		return records;
	},

	async status({ hashes } = {}) {
		let arrived = null;
		if (Array.isArray(hashes)) {
			arrived = 0;
			for (const hash of hashes) {
				if (counters.arrivals.has(hash) || counters.polledArrivals.has(hash)) {
					arrived += 1;
				} else if (await store.log.log.has(hash)) {
					counters.polledArrivals.set(hash, wall());
					arrived += 1;
				}
			}
		}
		return {
			at: wall(),
			idle: idleState(),
			inventory: inventoryStatus(),
			arrived,
			connections: otherConnections().length,
			handshake: handshakeOk(),
		};
	},

	report() {
		return buildReport();
	},

	/**
	 * Diagnostics only (the driver never calls it): what an inventory pass for
	 * the other peer would see now. Reads the coordinate rows and plans leaders
	 * the way recoverPeerEntryInventory does.
	 */
	async probe({ limit = 2000 } = {}) {
		const { decodeReplicas } = await import(
			pathToFileURL(
				path.join(ROOT, "packages/programs/data/shared-log/dist/src/replication.js"),
			).href
		);
		const scan = log._coordinates.scanAuthoritativeCoordinateKeys({ pageSize: 128 });
		const out = {
			logLength: store.log.log.length,
			coordinateRows: await log.entryCoordinatesIndex.count({}),
			keys: 0,
			ledByOther: 0,
			leaderSetSizes: {},
			replicasSeen: {},
			samples: [],
		};
		if (!scan) return { ...out, scan: "unsupported" };
		try {
			for (;;) {
				const page = await scan.next();
				for (const key of page.keys) {
					if (out.keys >= limit) break;
					out.keys += 1;
					const coordinate =
						await log._coordinates.getAuthoritativeCoordinateEntryForInventory(key);
					if (!coordinate) continue;
					const replicas = decodeReplicas(coordinate).getValue(log);
					out.replicasSeen[replicas] = (out.replicasSeen[replicas] ?? 0) + 1;
					const leaders = await log.findLeadersFromEntry(coordinate, replicas, {
						roleAge: 0,
						freshLeaderPlan: true,
					});
					out.leaderSetSizes[leaders.size] = (out.leaderSetSizes[leaders.size] ?? 0) + 1;
					if (leaders.has(state.other.hash)) out.ledByOther += 1;
					if (out.samples.length < 3) {
						out.samples.push({
							key,
							replicas,
							leaders: [...leaders.keys()].map((hash) =>
								hash === state.other.hash ? "other" : "self",
							),
						});
					}
				}
				if (page.status !== "more" || out.keys >= limit) {
					out.status = page.status;
					break;
				}
			}
		} finally {
			await scan.close();
		}
		return out;
	},

	async stop() {
		clearCpuMark();
		const startedAt = wall();
		let error = null;
		try {
			await peer?.stop();
		} catch (stopError) {
			error = fmt(stopError);
		}
		const stopMs = round(wall() - startedAt, 1);
		eld.disable();
		state.trackArrivals = false;
		emit("stopped", { stopMs, error });
		return {
			stopMs,
			error,
			errors: { unhandled: errorTotals.unhandled, uncaught: errorTotals.uncaught },
			errorSamples: errorTotals.samples,
			activeResources:
				typeof process.getActiveResourcesInfo === "function"
					? process.getActiveResourcesInfo()
					: null,
		};
	},
};

process.on("message", async (message) => {
	if (!message || typeof message !== "object" || !("id" in message)) return;
	const { id, cmd, args } = message;
	const handler = commands[cmd];
	let reply;
	try {
		if (!handler) throw new Error(`unknown command ${cmd}`);
		const result = await handler(args ?? {});
		reply = { id, ok: true, result };
	} catch (error) {
		reply = { id, ok: false, error: fmt(error), stack: error?.stack };
		emit("command:error", { cmd, error: fmt(error) });
	}
	try {
		process.send?.(JSON.parse(JSON.stringify(reply, jsonSafe)));
	} catch (sendError) {
		console.error(`[${SIDE}] reply failed for ${cmd}: ${fmt(sendError)}`);
	}
	if (cmd === "stop") {
		// Let the process exit on its own: no forced exit, so a leaked handle in
		// the product shows up as a late exit.
		eventsOpen = false;
		eventsStream.end();
		process.removeAllListeners("message");
		process.disconnect?.();
	}
});

process.send?.({ ready: true, side: SIDE, pid: process.pid });
