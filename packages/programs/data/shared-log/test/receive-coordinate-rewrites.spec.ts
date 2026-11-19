import { deserialize, getSchema, serialize } from "@dao-xyz/borsh";
import { keys } from "@libp2p/crypto";
import { createStore } from "@peerbit/any-store";
import { Or, StringMatch, toId } from "@peerbit/indexer-interface";
import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import type { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer from "p-defer";
import path from "path";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import {
	NATIVE_COORDINATE_WRITERS,
	PERSISTED_COORDINATE_ROW_FIELDS,
	coordinateRowsEqual,
	sameStoredInteger,
} from "../src/coordinate-persistence.js";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	RawEntryWithRefs,
	RawExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import {
	type EntryReplicated,
	EntryReplicatedU32,
	EntryReplicatedU64,
} from "../src/ranges.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

type Resolution = "u32" | "u64";
type Store = EventStore<string, any>;
type Row = EntryReplicated<any>;

// Heads per copy, and copies of them delivered to one receiver.
const K = 8;
const COPIES = 3;

const storeArgs = (resolution: Resolution): any => ({
	replicate: false,
	keep: () => true,
	timeUntilRoleMaturity: 0,
	nativeGraph: true,
	// The skip is off with a native backbone; these receivers keep their
	// coordinate rows in SQLite only.
	nativeBackbone: false,
	setup: {
		domain: createReplicationDomainHash(resolution),
		type: resolution,
		syncronizer: SimpleSyncronizer,
		name: "receive-coordinate-rewrites",
	},
});

const openReceivers = async (
	session: TestSession,
	resolution: Resolution,
	receivers: number,
	profile?: (event: { name: string; details?: any }) => void,
) => {
	const source: Store = await session.peers[0].open(
		new EventStore<string, any>(),
		{ args: storeArgs(resolution) },
	);
	const targets: Store[] = [];
	for (let i = 1; i <= receivers; i++) {
		targets.push(
			await session.peers[i].open(source.clone(), {
				args: {
					...storeArgs(resolution),
					// Force the independent lower batch, not prepared-facts admission.
					canAppend: () => true,
					...(profile ? { sync: { profile } } : {}),
				},
			}),
		);
	}
	return { source, targets };
};

const addIndependent = async (source: Store, count: number) => {
	const entries: Entry<any>[] = [];
	for (let i = 0; i < count; i++) {
		entries.push((await source.add(`head-${i}`, { meta: { next: [] } })).entry);
	}
	return entries;
};

const rawCopy = async (source: Store, entries: Entry<any>[]) =>
	new RawExchangeHeadsMessage({
		heads: await Promise.all(
			entries.map(async (entry) => {
				const bytes = await source.log.log.blocks.get(entry.hash);
				expect(bytes).to.be.instanceOf(Uint8Array);
				return new RawEntryWithRefs({
					hash: entry.hash,
					bytes: bytes!.slice(),
					gidRefrences: [],
				});
			}),
		),
	});

const deliver = (source: Store, target: Store, message: TransportMessage) =>
	target.log.onMessage(message, {
		from: source.node.identity.publicKey,
	} as any);

const coordinatorOf = (target: Store) => (target.log as any)._coordinates;

/** The coordinate rows a receiver writes, in call order. */
const recordRowWrites = (sandbox: sinon.SinonSandbox, target: Store) => {
	const index = target.log.entryCoordinatesIndex as any;
	const put = sandbox.spy(index, "put");
	const putBatch = sandbox.spy(index, "putBatch");
	return {
		rows: (hashes: string[]): Row[] =>
			[
				...put.getCalls().map((call: any) => ({
					id: call.callId,
					rows: [call.args[0] as Row],
				})),
				...putBatch.getCalls().map((call: any) => ({
					id: call.callId,
					rows: call.args[0] as Row[],
				})),
			]
				.sort((a, b) => a.id - b.id)
				.flatMap((call) => call.rows)
				.filter((row) => hashes.includes(row.hash)),
	};
};

const storedRow = async (
	target: Store,
	hash: string,
): Promise<Row | undefined> =>
	(await target.log.entryCoordinatesIndex.get(toId(hash)))?.value;

const storedRows = async (target: Store) =>
	new Map<string, Row>(
		(await target.log.entryCoordinatesIndex.iterate({}).all()).map(
			({ value }) => [value.hash, value],
		),
	);

const expectStoredAsWritten = async (target: Store, written: Row[]) => {
	for (const row of written) {
		expect(
			coordinateRowsEqual(await storedRow(target, row.hash), row),
			row.hash,
		).to.equal(true);
	}
};

/** A persist item that writes exactly `row` again for `entry`. */
const itemFromRow = (entry: Entry<any>, row: Row) => ({
	entry,
	coordinates: [...row.coordinates],
	leaders: false as const,
	replicas: row.coordinates.length,
	prepared: {
		assignedToRangeBoundary: row.assignedToRangeBoundary,
		fields: {
			hash: row.hash,
			hashNumber: row.hashNumber,
			gid: row.gid,
			coordinates: [...row.coordinates],
			wallTime: row.wallTime,
			assignedToRangeBoundary: row.assignedToRangeBoundary,
			metaBytes: row.getMetaBytes().slice(),
		},
	},
});

/** The hashes that a delete names; none for other queries. */
const queryHashes = (options?: { query?: unknown }): string[] => {
	const query = options?.query;
	if (query instanceof Or) {
		return query.or.map((term) => (term as StringMatch).value);
	}
	const hash = (query as { hash?: unknown } | undefined)?.hash;
	return typeof hash === "string" ? [hash] : [];
};

/** Resolves when `signal` does, or fails if `work` settles first. */
const awaitBefore = async (
	signal: Promise<void>,
	work: Promise<unknown>,
	message: string,
) => {
	const first = await Promise.race([
		signal.then(() => "signal" as const),
		work.then(
			() => "work" as const,
			() => "work" as const,
		),
	]);
	if (first === "work") {
		throw new Error(message);
	}
};

/**
 * Stops the first batch join on `target` at its entry-index batch write.
 * `entered` resolves when it stops there, `waiting` when COPIES - 1 later
 * copies wait on that join, and `release` lets it go on.
 */
const holdFirstBatchJoin = (sandbox: sinon.SinonSandbox, target: Store) => {
	const lower = target.log.log.entryIndex as any;
	const putAppendBatch = lower.putAppendBatch.bind(lower);
	const entered = pDefer<void>();
	const gate = pDefer<void>();
	let held = false;
	sandbox
		.stub(lower, "putAppendBatch")
		.callsFake(async (...args: unknown[]) => {
			if (!held) {
				held = true;
				entered.resolve();
				await gate.promise;
			}
			return putAppendBatch(...args);
		});
	const log = target.log.log as any;
	const waitForBorrowedJoin = log.waitForBorrowedJoin.bind(log);
	const waiting = pDefer<void>();
	let borrowedWaits = 0;
	sandbox.stub(log, "waitForBorrowedJoin").callsFake((...args: unknown[]) => {
		if (++borrowedWaits === COPIES - 1) {
			waiting.resolve();
		}
		return waitForBorrowedJoin(...args);
	});
	return {
		entered: entered.promise,
		waiting: waiting.promise,
		release: () => gate.resolve(),
	};
};

/**
 * Delivers COPIES raw copies of the same heads. The first copy stops at its
 * entry-index batch write, inside its batch join, while the later copies
 * decode every head as missing and wait on its join. Then it goes on.
 */
const deliverWhileFirstCopyJoins = async (
	sandbox: sinon.SinonSandbox,
	source: Store,
	target: Store,
	entries: Entry<any>[],
	beforeRelease?: () => void,
) => {
	const join = holdFirstBatchJoin(sandbox, target);
	const receives: Promise<unknown>[] = [];
	try {
		receives.push(deliver(source, target, await rawCopy(source, entries)));
		await awaitBefore(
			join.entered,
			receives[0],
			"The first copy finished before its entry-index batch write",
		);
		for (let i = 1; i < COPIES; i++) {
			receives.push(deliver(source, target, await rawCopy(source, entries)));
		}
		await awaitBefore(
			join.waiting,
			Promise.race(receives.slice(1)),
			"A later copy finished without waiting on the first copy's join",
		);
		beforeRelease?.();
		join.release();
		await Promise.all(receives);
	} finally {
		join.release();
		await Promise.allSettled(receives);
	}
	await target.log.log.entryIndex.flushPendingWrites();
};

/**
 * Holds every check for stored rows on `target` until `release` is called.
 * The check runs inside the entry-index hash locks its call has taken, so a
 * held check keeps those locks. `held` resolves when the first check is held
 * (or on `release`), and `finished` counts the checks that have returned.
 */
const holdStoredRowChecks = (sandbox: sinon.SinonSandbox, target: Store) => {
	const coordinator = coordinatorOf(target);
	const check =
		coordinator.findStoredUnchangedCoordinateWrites.bind(coordinator);
	const held = pDefer<void>();
	const gate = pDefer<void>();
	let finished = 0;
	sandbox
		.stub(coordinator, "findStoredUnchangedCoordinateWrites")
		.callsFake(async (...args: unknown[]) => {
			held.resolve();
			await gate.promise;
			try {
				return await check(...args);
			} finally {
				finished++;
			}
		});
	return {
		held: held.promise,
		release: () => {
			gate.resolve();
			held.resolve();
		},
		finished: () => finished,
	};
};

describe("receive coordinate rewrites", function () {
	this.timeout(60_000);

	for (const resolution of ["u32", "u64"] as const) {
		it(`writes no row for copies that wait on an in-flight copy (${resolution})`, async () => {
			const session = await TestSession.disconnected(2, {
				indexer: createSQLiteIndices,
			});
			const sandbox = sinon.createSandbox();
			try {
				const unchangedRows: unknown[] = [];
				const {
					source,
					targets: [target],
				} = await openReceivers(session, resolution, 1, (event) => {
					if (event.name === "sharedLog.receive.coordinatePersist") {
						unchangedRows.push(event.details?.unchangedRows);
					}
				});
				const entries = await addIndependent(source, K);
				const hashes = entries.map((entry) => entry.hash);
				const writes = recordRowWrites(sandbox, target);
				const persist = sandbox.spy(
					coordinatorOf(target),
					"persistCoordinatesBatch",
				);

				await deliverWhileFirstCopyJoins(sandbox, source, target, entries);

				// The first copy's join writes each row; the copies that waited on
				// it find them stored and write nothing.
				const rows = writes.rows(hashes);
				expect(rows, "coordinate rows written").to.have.length(K);
				expect(rows.map((row) => row.hash)).to.have.members(hashes);
				expect(persist.callCount).to.equal(COPIES - 1);
				for (const call of persist.getCalls()) {
					expect(
						(call.args[0] as any[]).map((item) => item.entry.hash),
					).to.have.members(hashes);
					expect(call.args[3]).to.deep.equal({ skipStoredUnchanged: true });
					expect(await call.returnValue).to.deep.equal(hashes.map(() => false));
				}
				expect(unchangedRows).to.have.members([0, K, K]);
				await expectStoredAsWritten(target, rows);
				expect(target.log.log.length).to.equal(K);
			} finally {
				sandbox.restore();
				await session.stop();
			}
		});
	}

	it("writes each row once for plain copies delivered in the same tick", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: createSQLiteIndices,
		});
		const sandbox = sinon.createSandbox();
		try {
			const {
				source,
				targets: [target],
			} = await openReceivers(session, "u32", 1);
			const entries = await addIndependent(source, K);
			const hashes = entries.map((entry) => entry.hash);
			const bytes = serialize(
				new ExchangeHeadsMessage({
					heads: entries.map(
						(entry) => new EntryWithRefs({ entry, gidRefrences: [] }),
					),
				}),
			);
			const copies = Array.from({ length: COPIES }, () =>
				deserialize(bytes, TransportMessage),
			);
			const writes = recordRowWrites(sandbox, target);

			await Promise.all(copies.map((copy) => deliver(source, target, copy)));
			await target.log.log.entryIndex.flushPendingWrites();

			const rows = writes.rows(hashes);
			expect(rows, "coordinate rows written").to.have.length(K);
			expect(rows.map((row) => row.hash)).to.have.members(hashes);
			await expectStoredAsWritten(target, rows);
			expect(target.log.log.length).to.equal(K);
		} finally {
			sandbox.restore();
			await session.stop();
		}
	});

	it("writes each row once for copies delivered one after another", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: createSQLiteIndices,
		});
		const sandbox = sinon.createSandbox();
		try {
			const {
				source,
				targets: [target],
			} = await openReceivers(session, "u32", 1);
			const entries = await addIndependent(source, K);
			const hashes = entries.map((entry) => entry.hash);
			const writes = recordRowWrites(sandbox, target);
			const persist = sandbox.spy(
				coordinatorOf(target),
				"persistCoordinatesBatch",
			);

			await deliver(source, target, await rawCopy(source, entries));
			const persistCallsAfterFirstCopy = persist.callCount;
			for (let i = 1; i < COPIES; i++) {
				await deliver(source, target, await rawCopy(source, entries));
			}
			await target.log.log.entryIndex.flushPendingWrites();

			const rows = writes.rows(hashes);
			expect(rows, "coordinate rows written").to.have.length(K);
			expect(rows.map((row) => row.hash)).to.have.members(hashes);
			// The later copies find every head present and persist nothing.
			expect(persist.callCount).to.equal(persistCallsAfterFirstCopy);
			expect(target.log.log.length).to.equal(K);
		} finally {
			sandbox.restore();
			await session.stop();
		}
	});

	it("issues every stored-row read of a step before awaiting any", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: createSQLiteIndices,
		});
		const sandbox = sinon.createSandbox();
		try {
			const {
				targets: [target],
			} = await openReceivers(session, "u32", 1);
			const index = target.log.entryCoordinatesIndex as any;
			// Rows for 130 heads, stored as an earlier join would have left them.
			const rows = Array.from(
				{ length: 130 },
				(_, i) =>
					new EntryReplicatedU32({
						hash: `stored-${i}`,
						hashNumber: i,
						gid: `gid-${i}`,
						coordinates: [i],
						wallTime: BigInt(i),
						assignedToRangeBoundary: false,
						metaBytes: new Uint8Array([i]),
					}),
			);
			await index.putBatch(rows);
			const item = (row: Row, deleteHashes?: string[]) => ({
				...itemFromRow({ hash: row.hash, meta: { next: [] } } as any, row),
				deleteHashes,
			});
			const persist = (items: ReturnType<typeof item>[]) =>
				coordinatorOf(target).persistCoordinatesBatch(
					items,
					undefined,
					undefined,
					{ skipStoredUnchanged: true },
				) as Promise<boolean[]>;
			// Every read and every settled read, in order. A step is a run of
			// reads issued before any read settles, so reads that each wait for
			// the one before them would make one step per row.
			let events: { read?: string; settled?: true }[] = [];
			const get = index.get.bind(index);
			sandbox.stub(index, "get").callsFake((id: any, ...rest: unknown[]) => {
				events.push({ read: String(id.primitive) });
				return Promise.resolve(get(id, ...rest)).finally(() =>
					events.push({ settled: true }),
				);
			});
			const steps = () => {
				const result: string[][] = [];
				let previous: (typeof events)[number] | undefined;
				for (const event of events) {
					if (event.read) {
						if (!previous?.read) {
							result.push([]);
						}
						result[result.length - 1]!.push(event.read);
					}
					previous = event;
				}
				return result;
			};
			const count = sandbox.spy(index, "count");
			const iterate = sandbox.spy(index, "iterate");
			const writes = ["put", "putBatch", "del"].map((name) =>
				sandbox.spy(index, name),
			);

			expect(await persist(rows.map((row) => item(row)))).to.deep.equal(
				rows.map(() => false),
			);
			expect(steps()).to.deep.equal([rows.map((row) => row.hash)]);

			// With a delete hash each: the rows first, then the delete-hash rows of
			// the writes whose stored row is identical.
			events = [];
			const chained = rows.slice(0, 70);
			const deleteHash = (row: Row) => `absent-${row.hash}`;
			expect(
				await persist(chained.map((row) => item(row, [deleteHash(row)]))),
			).to.deep.equal(chained.map(() => false));
			expect(steps()).to.deep.equal([
				chained.map((row) => row.hash),
				chained.map(deleteHash),
			]);

			expect(count.callCount, "count").to.equal(0);
			expect(iterate.callCount, "iterate").to.equal(0);
			for (const write of writes) {
				expect(write.callCount).to.equal(0);
			}
		} finally {
			sandbox.restore();
			await session.stop();
		}
	});

	describe("skipping a stored row is equivalent to writing it", () => {
		let session: TestSession | undefined;
		let skipping: Store;
		let writing: Store;
		const entries: Record<string, Entry<any>> = {};
		let reference: Map<string, Row>;

		// A row for a head's predecessor, as an earlier layout could have left.
		const predecessorRow = (entry: Entry<any>) =>
			new EntryReplicatedU32({
				coordinates: [1],
				hash: entry.hash,
				meta: entry.meta,
				assignedToRangeBoundary: false,
				hashNumber: 1,
			});

		/** `row` with the other range boundary, as a changed layout leaves it. */
		const withOtherBoundary = (row: Row) =>
			new EntryReplicatedU32({
				hash: row.hash,
				hashNumber: row.hashNumber as number,
				gid: row.gid,
				coordinates: [...(row.coordinates as number[])],
				wallTime: row.wallTime,
				assignedToRangeBoundary: !row.assignedToRangeBoundary,
				metaBytes: row.getMetaBytes(),
			});

		const itemFor = (name: string) => {
			const entry = entries[name]!;
			const row = reference.get(entry.hash);
			// A non-head has no stored row; the coordinator prepares one.
			return row
				? itemFromRow(entry, row)
				: { entry, coordinates: [1], leaders: false as const, replicas: 1 };
		};

		/** Runs the same call on both receivers, with and without the skip, and
		 * checks that they end in the same state: stored rows and every mirror
		 * (coordinate-to-hash, resident and native). The rows the call deletes
		 * start out in the resident and native mirrors on both receivers, so a
		 * delete that one of them leaves out shows. `items` makes the call's
		 * items for `names`, once for each receiver. */
		const persistOnBoth = async (
			names: string[],
			items: () => unknown[] = () => names.map(itemFor),
		) => {
			const deleted = [
				...new Set(names.flatMap((name) => entries[name]!.meta.next)),
			];
			// In call order, a write puts its head in the mirrors and takes out
			// the hashes it deletes.
			const kept = new Set<string>();
			const removed = new Set<string>();
			for (const name of names) {
				const entry = entries[name]!;
				kept.add(entry.hash);
				removed.delete(entry.hash);
				for (const hash of entry.meta.next) {
					kept.delete(hash);
					removed.add(hash);
				}
			}
			const outcomes = [];
			for (const [target, options] of [
				[skipping, { skipStoredUnchanged: true }],
				[writing, undefined],
			] as const) {
				const local = sinon.createSandbox();
				const shared = target.log as any;
				const coordinator = coordinatorOf(target);
				const native = shared._nativeSharedLogState;
				try {
					shared.coordinateToHash.clear();
					coordinator._residentEntryCoordinatesByHash = new Map(
						deleted.map((hash) => [hash, predecessorRow(entryOf(hash))]),
					);
					native.clearEntryCoordinates();
					for (const hash of deleted) {
						native.putEntryCoordinates(hash, entryOf(hash).meta.gid, [1]);
					}
					const index = target.log.entryCoordinatesIndex as any;
					const put = local.spy(index, "put");
					const putBatch = local.spy(index, "putBatch");
					const del = local.spy(index, "del");
					const result = await coordinator.persistCoordinatesBatch(
						items(),
						undefined,
						undefined,
						options,
					);
					outcomes.push({
						result: result as boolean[],
						put: put.getCalls().map((call: any) => call.args[0].hash as string),
						putBatch: putBatch
							.getCalls()
							.flatMap((call: any) =>
								(call.args[0] as Row[]).map((row) => row.hash),
							),
						del: del
							.getCalls()
							.flatMap((call: any) => queryHashes(call.args[0])),
						coordinateToHash: [...shared.coordinateToHash.map]
							.map(([key, value]: [unknown, { value: unknown }]) =>
								[String(key), value.value].join(" "),
							)
							.sort(),
						native: (native.getEntryCoordinateHashes() as string[])
							.sort()
							.map((hash) => [hash, native.getEntryCoordinates(hash)]),
						resident: new Map<string, Row>(
							[...coordinator._residentEntryCoordinatesByHash].map(
								([hash, entry]: [string, unknown]) => [
									hash,
									coordinator.materializeResidentCoordinateEntry(entry),
								],
							),
						),
						rows: await storedRows(target),
					});
				} finally {
					coordinator._residentEntryCoordinatesByHash = undefined;
					local.restore();
				}
			}
			const [skip, write] = outcomes;
			expect(skip.coordinateToHash).to.deep.equal(write.coordinateToHash);
			expect(skip.native, "native").to.deep.equal(write.native);
			// Both mirrors hold the heads the call leaves in place, and neither
			// holds a row that it deletes and does not write again.
			const nativeHashes = write.native.map(([hash]) => hash);
			expect(nativeHashes, "native").to.include.members([...kept]);
			for (const hash of removed) {
				expect(nativeHashes, "native").to.not.include(hash);
				expect(write.resident.has(hash), "resident").to.equal(false);
			}
			for (const state of ["resident", "rows"] as const) {
				expect([...skip[state].keys()], state).to.have.members([
					...write[state].keys(),
				]);
				for (const [hash, row] of write[state]) {
					expect(
						coordinateRowsEqual(skip[state].get(hash), row),
						`${state} ${hash}`,
					).to.equal(true);
				}
			}
			return { skip, write };
		};

		const hashesOf = (...names: string[]) =>
			names.map((name) => entries[name]!.hash);

		const entryOf = (hash: string) =>
			Object.values(entries).find((entry) => entry.hash === hash)!;

		before(async () => {
			session = await TestSession.disconnected(3, {
				indexer: createSQLiteIndices,
			});
			const opened = await openReceivers(session, "u32", 2);
			[skipping, writing] = opened.targets;
			for (const target of opened.targets) {
				expect(
					(target.log as any)._nativeSharedLogState,
					"native shared-log state",
				).to.exist;
			}
			const add = async (name: string, next: Entry<any>[] = []) => {
				entries[name] = (
					await opened.source.add(name, { meta: { next } })
				).entry;
			};
			for (const name of [
				"u1",
				"u2",
				"u3",
				"u4",
				"u5",
				"u6",
				"d",
				"f",
				"a0",
				"a1",
				"a2",
				"a3",
				"x0",
				"x1",
				"x2",
			]) {
				await add(name);
			}
			await add("b0", [entries.a0!]);
			await add("b1", [entries.a1!]);
			await add("b2", [entries.a2!]);
			await add("c0", [entries.x0!]);
			await add("c1", [entries.x1!]);
			await add("c2", [entries.x2!]);
			for (const target of [skipping, writing]) {
				await deliver(
					opened.source,
					target,
					await rawCopy(opened.source, Object.values(entries)),
				);
				await target.log.log.entryIndex.flushPendingWrites();
			}
			// b3 reaches neither receiver, so a3 stays a head with a stored row.
			await add("b3", [entries.a3!]);

			// Both receivers stored the same row for every head.
			reference = await storedRows(skipping);
			expect([...reference.keys()]).to.have.members(
				hashesOf(
					"u1",
					"u2",
					"u3",
					"u4",
					"u5",
					"u6",
					"d",
					"f",
					"a3",
					"b0",
					"b1",
					"b2",
					"c0",
					"c1",
					"c2",
				),
			);
			const other = await storedRows(writing);
			expect([...other.keys()]).to.have.members([...reference.keys()]);
			for (const [hash, row] of reference) {
				expect(coordinateRowsEqual(other.get(hash), row), hash).to.equal(true);
			}

			for (const target of [skipping, writing]) {
				const index = target.log.entryCoordinatesIndex;
				await index.del({ query: { hash: entries.d!.hash } });
				for (const name of ["f", "c2"]) {
					await index.put(
						withOtherBoundary(reference.get(entries[name]!.hash)!),
					);
				}
				await index.put(predecessorRow(entries.a0!));
				await index.put(predecessorRow(entries.a2!));
			}
		});

		after(async () => {
			await session?.stop();
			session = undefined;
		});

		it("skips rows that are stored unchanged", async () => {
			const { skip, write } = await persistOnBoth(["u1", "u2", "u3"]);
			expect(skip.putBatch).to.deep.equal([]);
			expect(skip.put).to.deep.equal([]);
			expect(skip.result).to.deep.equal([false, false, false]);
			expect(write.putBatch).to.deep.equal(hashesOf("u1", "u2", "u3"));
			expect(write.result).to.deep.equal([true, true, true]);
			expect(skip.del).to.deep.equal([]);
			expect(write.del).to.deep.equal([]);
		});

		it("writes a row that was deleted", async () => {
			const { skip, write } = await persistOnBoth(["d"]);
			for (const outcome of [skip, write]) {
				expect(outcome.putBatch).to.deep.equal(hashesOf("d"));
				expect(outcome.put).to.deep.equal([]);
				expect(outcome.result).to.deep.equal([true]);
			}
			expect(
				coordinateRowsEqual(
					skip.rows.get(entries.d!.hash),
					reference.get(entries.d!.hash),
				),
			).to.equal(true);
		});

		it("writes a row stored with another range boundary", async () => {
			const { skip, write } = await persistOnBoth(["f"]);
			for (const outcome of [skip, write]) {
				expect(outcome.putBatch).to.deep.equal(hashesOf("f"));
				expect(outcome.result).to.deep.equal([true]);
			}
			expect(
				coordinateRowsEqual(
					skip.rows.get(entries.f!.hash),
					reference.get(entries.f!.hash),
				),
			).to.equal(true);
		});

		it("writes a head whose predecessor still has a row, and deletes that row", async () => {
			const { skip, write } = await persistOnBoth(["b0"]);
			for (const outcome of [skip, write]) {
				expect(outcome.put).to.deep.equal(hashesOf("b0"));
				expect(outcome.putBatch).to.deep.equal([]);
				expect(outcome.del).to.deep.equal(hashesOf("a0"));
				expect(outcome.result).to.deep.equal([true]);
				expect(outcome.rows.has(entries.a0!.hash)).to.equal(false);
			}
		});

		it("writes both rows when one write in the call deletes the other", async () => {
			const { skip, write } = await persistOnBoth(["a1", "b1"]);
			for (const outcome of [skip, write]) {
				expect(outcome.put).to.deep.equal(hashesOf("a1", "b1"));
				expect(outcome.del).to.deep.equal(hashesOf("a1"));
				expect(outcome.result).to.deep.equal([true, true]);
			}
		});

		it("writes a stored head that an earlier write in the call deletes", async () => {
			// b3's write deletes a3's row, so a3 is written again although its
			// stored row is identical.
			const { skip, write } = await persistOnBoth(["b3", "a3"]);
			for (const outcome of [skip, write]) {
				expect(outcome.put).to.deep.equal(hashesOf("b3", "a3"));
				expect(outcome.del).to.deep.equal(hashesOf("a3"));
				expect(outcome.result).to.deep.equal([true, true]);
				expect(
					coordinateRowsEqual(
						outcome.rows.get(entries.a3!.hash),
						reference.get(entries.a3!.hash),
					),
				).to.equal(true);
			}
		});

		it("writes both rows when the call carries one head twice", async () => {
			const stored = reference.get(entries.u6!.hash)!;
			const changed = withOtherBoundary(stored);
			const { skip, write } = await persistOnBoth(["u6", "u6"], () => [
				itemFromRow(entries.u6!, stored),
				itemFromRow(entries.u6!, changed),
			]);
			for (const outcome of [skip, write]) {
				expect(outcome.putBatch).to.deep.equal(hashesOf("u6", "u6"));
				expect(outcome.result).to.deep.equal([true, true]);
				// The later write is the one stored.
				expect(
					coordinateRowsEqual(outcome.rows.get(entries.u6!.hash), changed),
				).to.equal(true);
			}
		});

		it("writes only the changed row when the call takes the per-row path", async () => {
			const { skip, write } = await persistOnBoth(["b2", "u4", "u5"]);
			expect(skip.put).to.deep.equal(hashesOf("b2"));
			expect(skip.result).to.deep.equal([true, false, false]);
			expect(write.put).to.deep.equal(hashesOf("b2", "u4", "u5"));
			expect(write.result).to.deep.equal([true, true, true]);
			for (const outcome of [skip, write]) {
				expect(outcome.putBatch).to.deep.equal([]);
				expect(outcome.del).to.deep.equal(hashesOf("a2"));
			}
		});

		it("skips a stored head whose predecessor has no row", async () => {
			const { skip, write } = await persistOnBoth(["c0"]);
			expect(skip.put).to.deep.equal([]);
			expect(skip.putBatch).to.deep.equal([]);
			expect(skip.del).to.deep.equal([]);
			expect(skip.result).to.deep.equal([false]);
			expect(write.put).to.deep.equal(hashesOf("c0"));
			expect(write.del).to.deep.equal(hashesOf("x0"));
			expect(write.result).to.deep.equal([true]);
		});

		it("writes only the changed head of two chained heads", async () => {
			const { skip, write } = await persistOnBoth(["c1", "c2"]);
			expect(skip.put).to.deep.equal(hashesOf("c2"));
			expect(skip.del).to.deep.equal(hashesOf("x2"));
			expect(skip.result).to.deep.equal([false, true]);
			expect(write.put).to.deep.equal(hashesOf("c1", "c2"));
			expect(write.del).to.deep.equal(hashesOf("x1", "x2"));
			expect(write.result).to.deep.equal([true, true]);
			for (const outcome of [skip, write]) {
				expect(outcome.putBatch).to.deep.equal([]);
			}
		});

		it("writes a stored head whose predecessor's row cannot be read", async () => {
			const index = skipping.log.entryCoordinatesIndex as any;
			const get = index.get.bind(index);
			const predecessor = entries.x0!.hash;
			let failedReads = 0;
			const stub = sinon
				.stub(index, "get")
				.callsFake(async (id: any, ...rest: unknown[]) => {
					if (String(id.primitive) === predecessor) {
						failedReads++;
						throw new Error("coordinate row read failed");
					}
					return get(id, ...rest);
				});
			try {
				const { skip, write } = await persistOnBoth(["c0"]);
				expect(failedReads).to.equal(1);
				for (const outcome of [skip, write]) {
					expect(outcome.put).to.deep.equal(hashesOf("c0"));
					expect(outcome.del).to.deep.equal(hashesOf("x0"));
					expect(outcome.result).to.deep.equal([true]);
				}
			} finally {
				stub.restore();
			}
		});
	});

	it("keeps one durable row per head through a reopen", async () => {
		const directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "receive-coordinate-rewrites-"),
		);
		const diskOptions = {
			directory,
			storage: {
				storeFactory: (storeDirectory?: string) => createStore(storeDirectory),
			},
			indexer: (indexDirectory?: string) => createSQLiteIndices(indexDirectory),
			libp2p: { privateKey: await keys.generateKeyPair("Ed25519") },
		};
		let session: TestSession | undefined;
		let reopened: Peerbit | undefined;
		const sandbox = sinon.createSandbox();
		let failure: { error: unknown } | undefined;
		try {
			session = await TestSession.disconnected(2, [
				{ indexer: createSQLiteIndices },
				diskOptions,
			]);
			const {
				source,
				targets: [target],
			} = await openReceivers(session, "u32", 1);
			expect(
				target.log.entryCoordinatesIndex.crashSafeDurability,
				"crash-safe coordinate rows",
			).to.exist;
			const clone = source.clone();
			const entries = await addIndependent(source, K);
			const hashes = entries.map((entry) => entry.hash);
			const writes = recordRowWrites(sandbox, target);

			await deliverWhileFirstCopyJoins(sandbox, source, target, entries);

			const rows = writes.rows(hashes);
			expect(rows, "coordinate rows written").to.have.length(K);
			sandbox.restore();
			await session.stop();
			session = undefined;

			reopened = await Peerbit.create(diskOptions);
			const restored = await reopened.open(clone, {
				args: storeArgs("u32"),
			});
			expect(restored.log.log.length).to.equal(K);
			const coordinates = coordinatorOf(restored);
			for (const row of rows) {
				expect(
					coordinateRowsEqual(await storedRow(restored, row.hash), row),
					row.hash,
				).to.equal(true);
				expect(
					coordinateRowsEqual(
						await coordinates.getAuthoritativeCoordinateEntryForReceipt(
							row.hash,
						),
						row,
					),
					row.hash,
				).to.equal(true);
			}
		} catch (error) {
			failure = { error };
		}
		// Every cleanup runs, whatever failed before it. A cleanup error fails
		// the test only when the test itself passed, so it cannot hide the test's
		// own error, such as a setup failure that left a peer writing here.
		sandbox.restore();
		const cleanupErrors: unknown[] = [];
		for (const cleanup of [
			() => reopened?.stop(),
			() => session?.stop(),
			() => fs.rm(directory, { recursive: true, force: true, maxRetries: 3 }),
		]) {
			try {
				await cleanup();
			} catch (error) {
				cleanupErrors.push(error);
			}
		}
		if (failure) {
			throw failure.error;
		}
		if (cleanupErrors.length > 0) {
			throw cleanupErrors[0];
		}
	});

	it("writes a row whose stored copy cannot be read", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: createSQLiteIndices,
		});
		const sandbox = sinon.createSandbox();
		try {
			const {
				source,
				targets: [target],
			} = await openReceivers(session, "u32", 1);
			const entries = await addIndependent(source, K);
			const hashes = entries.map((entry) => entry.hash);
			const writes = recordRowWrites(sandbox, target);
			const persist = sandbox.spy(
				coordinatorOf(target),
				"persistCoordinatesBatch",
			);
			const index = target.log.entryCoordinatesIndex as any;
			const get = index.get.bind(index);
			let unreadable: string | undefined;

			await deliverWhileFirstCopyJoins(sandbox, source, target, entries, () => {
				sandbox
					.stub(index, "get")
					.callsFake(async (id: any, ...rest: unknown[]) => {
						// The first stored-row read fails.
						if (unreadable === undefined) {
							unreadable = String(id.primitive);
							throw new Error("coordinate row read failed");
						}
						return get(id, ...rest);
					});
			});

			expect(unreadable, "a stored row was read").to.be.oneOf(hashes);
			// Only that row is written again: the copy whose read failed writes it,
			// and every other read of both copies finds its row unchanged.
			const persisted: string[] = [];
			for (const call of persist.getCalls()) {
				const result: boolean[] = await call.returnValue;
				(call.args[0] as any[]).forEach((item, i) => {
					if (result[i]) persisted.push(item.entry.hash);
				});
			}
			expect(persisted).to.deep.equal([unreadable]);
			const rows = writes.rows(hashes);
			expect(rows).to.have.length(K + 1);
			expect(rows.filter((row) => row.hash === unreadable)).to.have.length(2);
			await expectStoredAsWritten(target, rows);
			expect(target.log.log.length).to.equal(K);
		} finally {
			sandbox.restore();
			await session.stop();
		}
	});

	describe("while a check holds its locks", () => {
		it("answers a persisted receipt that waits for those locks", async () => {
			const directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "receive-coordinate-rewrites-"),
			);
			const crashSafe = (name: string) => ({
				directory: path.join(directory, name),
				storage: {
					storeFactory: (storeDirectory?: string) =>
						createStore(storeDirectory),
				},
				indexer: (indexDirectory?: string) =>
					createSQLiteIndices(indexDirectory),
			});
			let session: TestSession | undefined;
			const sandbox = sinon.createSandbox();
			const releases: (() => void)[] = [];
			try {
				session = await TestSession.connected(2, [
					crashSafe("writer"),
					crashSafe("receiver"),
				]);
				const args = {
					replicas: { min: 2 },
					replicate: { offset: 0, factor: 1 },
					timeUntilRoleMaturity: 0,
				};
				const writer: Store = await session.peers[0].open(
					new EventStore<string, any>(),
					{ args },
				);
				const receiver = await EventStore.open<Store>(
					writer.address!,
					session.peers[1],
					// Force the independent lower batch, as openReceivers does.
					{ args: { ...args, canAppend: () => true } },
				);
				await writer.log.waitForReplicators({
					coverageThreshold: 1,
					roleAge: 0,
					timeout: 15_000,
				});
				await writer.log.waitForReplicator(receiver.node.identity.publicKey, {
					roleAge: 0,
					timeout: 15_000,
				});
				await writer.log.waitForPersistedReceiptPeerReadiness(
					receiver.node.identity.publicKey,
					{ timeout: 15_000 },
				);
				const entries: Entry<any>[] = [];
				for (let i = 0; i < K; i++) {
					entries.push(
						(
							await writer.add(`receipt-${i}`, {
								target: "none",
								meta: { next: [] },
							})
						).entry,
					);
				}
				const hashes = entries.map((entry) => entry.hash);
				const receiverLog = receiver.log as any;
				const persist = sandbox.spy(
					coordinatorOf(receiver),
					"persistCoordinatesBatch",
				);
				const checks = holdStoredRowChecks(sandbox, receiver);
				releases.push(checks.release);

				// Receipts start once a check holds its locks, so a receipt's flush
				// of the entries' index writes has to wait for those locks.
				const handle =
					receiverLog.handleRequestPersistedEntriesV1.bind(receiverLog);
				const confirmed = new Set<string>();
				sandbox
					.stub(receiverLog, "handleRequestPersistedEntriesV1")
					.callsFake(async (...args: unknown[]) => {
						await checks.held;
						const response = await handle(...args);
						for (const hash of response?.hashes ?? []) {
							confirmed.add(hash);
						}
						return response;
					});
				const entryIndex = receiverLog.log.entryIndex;
				const flush = entryIndex.flushPendingWrites.bind(entryIndex);
				const receiptFlushing = pDefer<void>();
				let checksFinishedWhenFlushed: number | undefined;
				sandbox
					.stub(entryIndex, "flushPendingWrites")
					.callsFake(async (...args: unknown[]) => {
						const requested = args[0] as Iterable<string> | undefined;
						if (!requested || ![...requested].some((h) => hashes.includes(h))) {
							return flush(...args);
						}
						receiptFlushing.resolve();
						const result = await flush(...args);
						checksFinishedWhenFlushed ??= checks.finished();
						return result;
					});

				// The first heads message that carries the entries becomes COPIES
				// raw copies with the same context. The first copy stops inside its
				// batch join until the others wait on it, and the copies go on after
				// the message handler returns.
				const join = holdFirstBatchJoin(sandbox, receiver);
				releases.push(join.release);
				const onMessage = receiverLog.onMessage.bind(receiverLog);
				const copies: Promise<unknown>[] = [];
				let replaced = false;
				sandbox
					.stub(receiverLog, "onMessage")
					.callsFake(async (message: unknown, context: unknown) => {
						const carried =
							message instanceof ExchangeHeadsMessage
								? message.heads.map((head) => head.entry.hash)
								: message instanceof RawExchangeHeadsMessage
									? message.heads.map((head) => head.hash)
									: [];
						if (replaced || !carried.some((hash) => hashes.includes(hash))) {
							return onMessage(message, context);
						}
						replaced = true;
						copies.push(onMessage(await rawCopy(writer, entries), context));
						await awaitBefore(
							join.entered,
							copies[0],
							"The first copy finished before its entry-index batch write",
						);
						for (let i = 1; i < COPIES; i++) {
							copies.push(onMessage(await rawCopy(writer, entries), context));
						}
						await awaitBefore(
							join.waiting,
							Promise.race(copies.slice(1)),
							"A later copy finished without waiting on the first copy's join",
						);
						join.release();
					});

				const delivery = writer.log.deliverPersistedEntries(entries, {
					target: "replicators",
					delivery: { reliability: "persisted", minAcks: 1, timeout: 30_000 },
				});
				await awaitBefore(
					checks.held,
					delivery,
					"The delivery settled before a check held its locks",
				);
				await awaitBefore(
					receiptFlushing.promise,
					delivery,
					"The delivery settled before a receipt flushed the entries",
				);
				checks.release();
				await delivery;
				await Promise.all(copies);

				expect([...confirmed]).to.include.members(hashes);
				// The receipt's flush got the locks only after the held check.
				expect(checksFinishedWhenFlushed).to.be.at.least(1);
				const postJoin = persist
					.getCalls()
					.filter((call: any) => call.args[3]?.skipStoredUnchanged);
				expect(postJoin.length).to.be.at.least(COPIES - 1);
				for (const call of postJoin) {
					expect(await call.returnValue).to.deep.equal(
						(call.args[0] as unknown[]).map(() => false),
					);
				}
			} finally {
				for (const release of releases) {
					release();
				}
				sandbox.restore();
				try {
					await session?.stop();
				} finally {
					await fs.rm(directory, {
						recursive: true,
						force: true,
						maxRetries: 3,
					});
				}
			}
		});

		it("settles a close that comes while a check holds its locks", async () => {
			const session = await TestSession.disconnected(2, {
				indexer: createSQLiteIndices,
			});
			const sandbox = sinon.createSandbox();
			const releases: (() => void)[] = [];
			const unhandled: unknown[] = [];
			const onUnhandled = (reason: unknown) => unhandled.push(reason);
			process.on("unhandledRejection", onUnhandled);
			try {
				const {
					source,
					targets: [target],
				} = await openReceivers(session, "u32", 1);
				const entries = await addIndependent(source, K);
				const persist = sandbox.spy(
					coordinatorOf(target),
					"persistCoordinatesBatch",
				);
				const checks = holdStoredRowChecks(sandbox, target);
				const join = holdFirstBatchJoin(sandbox, target);
				releases.push(checks.release, join.release);
				const receives = [
					deliver(source, target, await rawCopy(source, entries)),
				];
				await awaitBefore(
					join.entered,
					receives[0],
					"The first copy finished before its entry-index batch write",
				);
				for (let i = 1; i < COPIES; i++) {
					receives.push(
						deliver(source, target, await rawCopy(source, entries)),
					);
				}
				await awaitBefore(
					join.waiting,
					Promise.race(receives.slice(1)),
					"A later copy finished without waiting on the first copy's join",
				);
				join.release();
				await awaitBefore(
					checks.held,
					Promise.all(receives),
					"The copies finished before a check held its locks",
				);

				// Close aborts the ownership lifecycle before it waits for the
				// receives. The held check goes on only after that.
				const lifecycle: AbortController = (target.log as any)
					._instanceLifecycle.ownershipLifecycleController;
				const aborted = new Promise<void>((resolve) =>
					lifecycle.signal.addEventListener("abort", () => resolve(), {
						once: true,
					}),
				);
				const closing = target.close();
				await awaitBefore(
					aborted,
					closing,
					"The close finished without aborting the ownership lifecycle",
				);
				checks.release();
				await closing;
				await Promise.allSettled(receives);

				const calls = persist.getCalls();
				expect(calls.length).to.be.at.least(1);
				for (const call of calls) {
					const error = await (call.returnValue as Promise<unknown>).then(
						() => undefined,
						(error: unknown) => error,
					);
					expect((error as Error | undefined)?.message).to.equal(
						"Replication ownership lifecycle is no longer active",
					);
				}
				// A rejection that nothing handled is reported after this turn.
				await new Promise((resolve) => setImmediate(resolve));
				expect(unhandled).to.deep.equal([]);
			} finally {
				for (const release of releases) {
					release();
				}
				process.off("unhandledRejection", onUnhandled);
				sandbox.restore();
				await session.stop();
			}
		});
	});

	describe("coordinate row comparison", () => {
		it("lists every persisted column of the row schema", () => {
			for (const schema of [EntryReplicatedU32, EntryReplicatedU64]) {
				expect(
					getSchema(schema).fields.map((field) => field.key),
					schema.name,
				).to.deep.equal([...PERSISTED_COORDINATE_ROW_FIELDS]);
			}
		});

		for (const resolution of ["u32", "u64"] as const) {
			it(`finds a change in any one column (${resolution})`, () => {
				const int = (value: number) =>
					resolution === "u32" ? value : BigInt(value);
				const RowClass: new (properties: any) => Row =
					resolution === "u32" ? EntryReplicatedU32 : EntryReplicatedU64;
				const row = (change: Record<string, unknown> = {}): Row =>
					new RowClass({
						hash: "hash",
						hashNumber: int(7),
						gid: "gid",
						coordinates: [int(1), int(2), int(3)],
						wallTime: 5n,
						assignedToRangeBoundary: false,
						metaBytes: new Uint8Array([1, 2, 3]),
						...change,
					} as any);
				const changes: Record<
					(typeof PERSISTED_COORDINATE_ROW_FIELDS)[number],
					Record<string, unknown>[]
				> = {
					hash: [{ hash: "other" }],
					hashNumber: [{ hashNumber: int(8) }],
					gid: [{ gid: "other" }],
					coordinates: [
						{ coordinates: [int(1), int(2), int(4)] },
						{ coordinates: [int(1), int(3), int(2)] },
						{ coordinates: [int(1), int(2)] },
					],
					wallTime: [{ wallTime: 6n }],
					assignedToRangeBoundary: [{ assignedToRangeBoundary: true }],
					_meta: [
						{ metaBytes: new Uint8Array([1, 2, 4]) },
						{ metaBytes: new Uint8Array([1, 2]) },
					],
				};
				expect(coordinateRowsEqual(row(), row())).to.equal(true);
				for (const field of PERSISTED_COORDINATE_ROW_FIELDS) {
					expect(changes[field], field).to.not.be.empty;
					for (const change of changes[field]) {
						expect(coordinateRowsEqual(row(), row(change)), field).to.equal(
							false,
						);
						expect(coordinateRowsEqual(row(change), row()), field).to.equal(
							false,
						);
					}
				}
			});
		}

		it("treats a number and a bigint holding the same integer as equal", () => {
			expect(sameStoredInteger(7, 7n)).to.equal(true);
			expect(sameStoredInteger(7n, 7)).to.equal(true);
			expect(sameStoredInteger(0, -0)).to.equal(true);
			expect(sameStoredInteger(7, 8n)).to.equal(false);
			const written = new EntryReplicatedU32({
				hash: "hash",
				hashNumber: 7,
				gid: "gid",
				coordinates: [1, 2, 3],
				wallTime: 5n,
				assignedToRangeBoundary: false,
				metaBytes: new Uint8Array([1, 2, 3]),
			});
			// An index may read the integers of a row back as bigints.
			const readBack = Object.assign(
				Object.create(EntryReplicatedU32.prototype),
				{
					hash: "hash",
					hashNumber: 7n,
					gid: "gid",
					coordinates: [1n, 2n, 3n],
					wallTime: 5n,
					assignedToRangeBoundary: false,
					_meta: new Uint8Array([1, 2, 3]),
				},
			);
			expect(coordinateRowsEqual(readBack, written)).to.equal(true);
			expect(coordinateRowsEqual(written, readBack)).to.equal(true);
		});

		it("never treats a missing or inexact value as equal", () => {
			expect(sameStoredInteger(undefined, undefined)).to.equal(false);
			expect(sameStoredInteger(null, null)).to.equal(false);
			expect(sameStoredInteger(NaN, NaN)).to.equal(false);
			expect(sameStoredInteger(1.5, 1.5)).to.equal(false);
			expect(sameStoredInteger(2 ** 53, 2 ** 53)).to.equal(false);
			expect(sameStoredInteger(2 ** 53, 2n ** 53n)).to.equal(false);
			expect(sameStoredInteger("7", 7)).to.equal(false);
			expect(coordinateRowsEqual(undefined, undefined)).to.equal(false);
			const missing = () =>
				Object.assign(Object.create(EntryReplicatedU32.prototype), {
					hash: "hash",
					gid: "gid",
					coordinates: [1],
					wallTime: 5n,
					assignedToRangeBoundary: false,
					_meta: new Uint8Array([1]),
				});
			// hashNumber is undefined on both rows.
			expect(coordinateRowsEqual(missing(), missing())).to.equal(false);
		});
	});

	describe("native coordinate writers", () => {
		let session: TestSession | undefined;
		let target: Store;
		let entries: Entry<any>[];
		let reference: Map<string, Row>;

		before(async () => {
			session = await TestSession.disconnected(2, {
				indexer: createSQLiteIndices,
			});
			const opened = await openReceivers(session, "u32", 1);
			[target] = opened.targets;
			entries = await addIndependent(opened.source, K);
			await deliver(
				opened.source,
				target,
				await rawCopy(opened.source, entries),
			);
			await target.log.log.entryIndex.flushPendingWrites();
			reference = await storedRows(target);
			expect([...reference.keys()]).to.have.members(
				entries.map((entry) => entry.hash),
			);
		});

		after(async () => {
			await session?.stop();
			session = undefined;
		});

		const persistUnchanged = () =>
			coordinatorOf(target).persistCoordinatesBatch(
				entries.map((entry) => itemFromRow(entry, reference.get(entry.hash)!)),
				undefined,
				undefined,
				{ skipStoredUnchanged: true },
			) as Promise<boolean[]>;

		it("reads and skips nothing while a native backbone is open", async () => {
			const local = sinon.createSandbox();
			try {
				const index = target.log.entryCoordinatesIndex as any;
				const backbone = { commitEntryCoordinatesBatch: local.spy() };
				local
					.stub(coordinatorOf(target).deps, "nativeBackbone")
					.returns(backbone);
				const get = local.spy(index, "get");
				const iterate = local.spy(index, "iterate");
				const putBatch = local.spy(index, "putBatch");

				const result = await persistUnchanged();

				expect(get.callCount, "get").to.equal(0);
				expect(iterate.callCount, "iterate").to.equal(0);
				expect(result).to.deep.equal(entries.map(() => true));
				expect(
					(putBatch.firstCall.args[0] as Row[]).map((row) => row.hash),
				).to.deep.equal(entries.map((entry) => entry.hash));
				expect(backbone.commitEntryCoordinatesBatch.callCount).to.equal(1);
			} finally {
				local.restore();
			}
		});

		// The list is checked against PutAndDeleteIndex when the source compiles.
		for (const writer of NATIVE_COORDINATE_WRITERS) {
			it(`reads and skips nothing when the index has ${writer}`, async () => {
				const local = sinon.createSandbox();
				const index = target.log.entryCoordinatesIndex as any;
				const native = local.stub().resolves(undefined);
				index[writer] = native;
				try {
					const get = local.spy(index, "get");
					const iterate = local.spy(index, "iterate");
					const putBatch = local.spy(index, "putBatch");

					const result = await persistUnchanged();

					expect(get.callCount, "get").to.equal(0);
					expect(iterate.callCount, "iterate").to.equal(0);
					expect(result).to.deep.equal(entries.map(() => true));
					// Batch writers take the rows; with the others putBatch does.
					const written = [
						...putBatch
							.getCalls()
							.flatMap((call: any) =>
								(call.args[0] as Row[]).map((row) => row.hash),
							),
						...native
							.getCalls()
							.flatMap((call: any) =>
								(call.args[0] as any[]).map(
									(value) => value.fields?.hash ?? value.value?.hash,
								),
							),
					];
					expect(written).to.deep.equal(entries.map((entry) => entry.hash));
				} finally {
					delete index[writer];
					local.restore();
				}
			});
		}
	});
});
