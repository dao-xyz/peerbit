import { Cache } from "@peerbit/cache";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import sinon from "sinon";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import {
	MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
	PENDING_SIMPLE_SYNC_KEY_TTL_MS,
	RequestMaybeSyncCoordinate,
	ResponseMaybeSync,
	ResponseMaybeSyncCapabilities,
	SimpleSyncronizer,
} from "../src/sync/simple.js";

// Stage-4 sync-unification pins. These freeze the observable behavior of the
// pending-sync structures (the syncInFlightQueue lockstep family) before the
// record-store fold so every remove path can be proven equivalent:
// TTL expiry, entry-added, per-peer claim removal (disconnect), coordinate
// alias reconciliation, and close().
describe("sync-chunking memory pins", () => {
	let peerA: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];
	let peerB: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];
	let peerC: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];

	before(async () => {
		[peerA, peerB, peerC] = await Promise.all([
			Ed25519Keypair.create().then((keypair) => keypair.publicKey),
			Ed25519Keypair.create().then((keypair) => keypair.publicKey),
			Ed25519Keypair.create().then((keypair) => keypair.publicKey),
		]);
	});

	const createSync = (overrides?: {
		log?: any;
		coordinateToHash?: Cache<string>;
		send?: sinon.SinonStub;
	}) => {
		const send = overrides?.send ?? sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: overrides?.log ?? ({ has: async () => false } as any),
			coordinateToHash:
				overrides?.coordinateToHash ?? new Cache<string>({ max: 1_000 }),
		});
		return { sync, send };
	};

	// The full census of the pending-sync key family. Every remove path must
	// leave all of these empty; any structure that survives one of the five
	// paths is a leak.
	const expectPendingSyncCensusEmpty = (sync: SimpleSyncronizer<"u64">) => {
		const anySync = sync as any;
		expect(sync.syncInFlightQueue.size, "syncInFlightQueue").to.equal(0);
		expect(
			sync.syncInFlightQueueInverted.size,
			"syncInFlightQueueInverted",
		).to.equal(0);
		expect(
			anySync.syncInFlightQueueExpiresAt.size,
			"syncInFlightQueueExpiresAt",
		).to.equal(0);
		expect(
			anySync.pendingSyncExpiryHeap.length,
			"pendingSyncExpiryHeap",
		).to.equal(0);
		expect(
			anySync.pendingSyncKeyExpiryNodes.size,
			"pendingSyncKeyExpiryNodes",
		).to.equal(0);
		expect(
			anySync.pendingSyncAdmissionExpiryNodes.size,
			"pendingSyncAdmissionExpiryNodes",
		).to.equal(0);
		expect(
			anySync.syncInFlightQueueClaimants.size,
			"syncInFlightQueueClaimants",
		).to.equal(0);
		expect(
			anySync.syncInFlightQueueClaimantIndexes.size,
			"syncInFlightQueueClaimantIndexes",
		).to.equal(0);
		expect(
			anySync.syncInFlightQueueRoundRobinCursor.size,
			"syncInFlightQueueRoundRobinCursor",
		).to.equal(0);
		expect(
			anySync.syncInFlightQueuedCoordinates.size,
			"syncInFlightQueuedCoordinates",
		).to.equal(0);
		expect(
			anySync.syncInFlightQueuedHashByCoordinate.size,
			"syncInFlightQueuedHashByCoordinate",
		).to.equal(0);
		expect(
			anySync.syncInFlightQueuedCoordinatesByHash.size,
			"syncInFlightQueuedCoordinatesByHash",
		).to.equal(0);
		expect(anySync.pendingSyncClaimCount, "pendingSyncClaimCount").to.equal(0);
		expect(
			anySync.pendingSyncAdmissionCount,
			"pendingSyncAdmissionCount",
		).to.equal(0);
		expect(
			anySync.pendingSyncAdmissionReservations.size,
			"pendingSyncAdmissionReservations",
		).to.equal(0);
		expect(
			anySync.pendingSyncAdmissionReservationsByPeer.size,
			"pendingSyncAdmissionReservationsByPeer",
		).to.equal(0);
		expect(
			anySync.pendingSyncAdmissionReservationsByIdentity.size,
			"pendingSyncAdmissionReservationsByIdentity",
		).to.equal(0);
		expect(
			anySync.pendingSyncAdmissionCountByPeer.size,
			"pendingSyncAdmissionCountByPeer",
		).to.equal(0);
		expect(
			anySync.pendingSyncAdmissionIdentitiesByPeer.size,
			"pendingSyncAdmissionIdentitiesByPeer",
		).to.equal(0);
		expect(sync.syncInFlight.size, "syncInFlight").to.equal(0);
		expect(
			anySync.syncInFlightTargetsByKey.size,
			"syncInFlightTargetsByKey",
		).to.equal(0);
	};

	const expectClaimCountMatchesClaimants = (sync: SimpleSyncronizer<"u64">) => {
		const anySync = sync as any;
		let sum = 0;
		for (const claimants of anySync.syncInFlightQueueClaimants.values()) {
			sum += (claimants as Set<string>).size;
		}
		expect(anySync.pendingSyncClaimCount).to.equal(sum);
	};

	it("empties every pending-sync structure on TTL expiry", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const { sync } = createSync();
		try {
			await sync.queueSync(["expiry-hash", 5n], peerA, { skipCheck: true });
			expect(sync.syncInFlightQueue.size).to.equal(2);
			expectClaimCountMatchesClaimants(sync);

			await clock.tickAsync(PENDING_SIMPLE_SYNC_KEY_TTL_MS - 1);
			expect(sync.syncInFlightQueue.size).to.equal(2);

			await clock.tickAsync(1);
			expectPendingSyncCensusEmpty(sync);
			expect((sync as any).syncInFlightQueueExpiryTimer).to.equal(undefined);
		} finally {
			await sync.close();
			clock.restore();
		}
	});

	it("empties every pending-sync structure when entries arrive", async () => {
		const { sync } = createSync();
		try {
			await sync.queueSync(["added-one", "added-two"], peerA, {
				skipCheck: true,
			});
			await sync.queueSync(["added-one"], peerB, { skipCheck: true });
			expectClaimCountMatchesClaimants(sync);

			sync.onEntryAddedHash("added-one");
			expect(sync.syncInFlightQueue.has("added-one")).to.equal(false);
			expect(sync.syncInFlightQueue.has("added-two")).to.equal(true);
			expectClaimCountMatchesClaimants(sync);

			sync.onEntryAddedHashes(["added-two"]);
			expectPendingSyncCensusEmpty(sync);
		} finally {
			await sync.close();
		}
	});

	it("empties every pending-sync structure through per-peer claim removal", async () => {
		const { sync } = createSync();
		try {
			await sync.queueSync(["shared-key"], peerA, { skipCheck: true });
			await sync.queueSync(["shared-key"], peerB, { skipCheck: true });
			expect(sync.syncInFlightQueue.get("shared-key")!.length).to.equal(2);

			sync.onPeerDisconnected(peerA);
			expect(
				sync.syncInFlightQueue
					.get("shared-key")!
					.map((peer) => peer.hashcode()),
			).to.deep.equal([peerB.hashcode()]);
			expect(sync.syncInFlightQueueInverted.has(peerA.hashcode())).to.equal(
				false,
			);
			expect((sync as any).pendingSyncClaimCount).to.equal(1);
			expectClaimCountMatchesClaimants(sync);

			sync.onPeerDisconnected(peerB);
			expectPendingSyncCensusEmpty(sync);
		} finally {
			await sync.close();
		}
	});

	it("empties every pending-sync structure on close", async () => {
		const { sync } = createSync();
		try {
			await sync.queueSync(["close-hash", 9n], peerA, { skipCheck: true });
			await sync.queueSync(["close-hash"], peerB, { skipCheck: true });
			expect(sync.pending).to.be.greaterThan(0);

			await sync.close();
			expectPendingSyncCensusEmpty(sync);
		} finally {
			await sync.close();
		}
	});

	it("preserves the earliest deadline when a coordinate alias transplants claims", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const coordinateToHash = new Cache<string>({ max: 1_000 });
		const { sync } = createSync({ coordinateToHash });
		const coordinate = 7n;
		const lateHash = "late-alias-hash";
		try {
			await sync.queueSync([coordinate], peerA, { skipCheck: true });
			const coordinateDeadline = (sync as any).syncInFlightQueueExpiresAt.get(
				coordinate,
			);
			expect(coordinateDeadline).to.equal(
				100_000 + PENDING_SIMPLE_SYNC_KEY_TTL_MS,
			);

			await clock.tickAsync(10_000);
			await sync.queueSync([lateHash], peerB, { skipCheck: true });
			expect(sync.syncInFlightQueue.has(lateHash)).to.equal(true);

			coordinateToHash.add(coordinate, lateHash);
			await sync.queueSync([lateHash], peerB, { skipCheck: true });

			// The hash record folded into the coordinate record.
			expect(sync.syncInFlightQueue.has(lateHash)).to.equal(false);
			expect(
				sync.syncInFlightQueue.get(coordinate)!.map((peer) => peer.hashcode()),
			).to.deep.equal([peerA.hashcode(), peerB.hashcode()]);
			// The transplanted claim inherits the earlier (coordinate) deadline;
			// repeated claims and additional peers must not slide it.
			expect((sync as any).syncInFlightQueueExpiresAt.get(coordinate)).to.equal(
				coordinateDeadline,
			);
			expectClaimCountMatchesClaimants(sync);

			await clock.tickAsync(PENDING_SIMPLE_SYNC_KEY_TTL_MS - 10_000);
			expectPendingSyncCensusEmpty(sync);
		} finally {
			await sync.close();
			clock.restore();
		}
	});

	it("keeps exact round-robin cursor semantics on swap-remove of a middle claimant", async () => {
		const { sync } = createSync();
		try {
			await sync.queueSync(["rr-key"], peerA, { skipCheck: true });
			await sync.queueSync(["rr-key"], peerB, { skipCheck: true });
			await sync.queueSync(["rr-key"], peerC, { skipCheck: true });
			expect(
				sync.syncInFlightQueue.get("rr-key")!.map((peer) => peer.hashcode()),
			).to.deep.equal([peerA.hashcode(), peerB.hashcode(), peerC.hashcode()]);

			// Cursor at the last slot, middle claimant removed: the last claimant
			// swaps into the removed slot and the cursor follows it.
			(sync as any).syncInFlightQueueRoundRobinCursor.set("rr-key", 2);
			(sync as any).removePendingSyncClaim("rr-key", peerB.hashcode());
			expect(
				sync.syncInFlightQueue.get("rr-key")!.map((peer) => peer.hashcode()),
			).to.deep.equal([peerA.hashcode(), peerC.hashcode()]);
			expect(
				(sync as any).syncInFlightQueueRoundRobinCursor.get("rr-key"),
			).to.equal(1);
			expectClaimCountMatchesClaimants(sync);

			// Cursor before the removed slot stays put (modulo the new length).
			await sync.queueSync(["rr-key"], peerB, { skipCheck: true });
			(sync as any).syncInFlightQueueRoundRobinCursor.set("rr-key", 0);
			(sync as any).removePendingSyncClaim("rr-key", peerC.hashcode());
			expect(
				sync.syncInFlightQueue.get("rr-key")!.map((peer) => peer.hashcode()),
			).to.deep.equal([peerA.hashcode(), peerB.hashcode()]);
			expect(
				(sync as any).syncInFlightQueueRoundRobinCursor.get("rr-key"),
			).to.equal(0);
		} finally {
			await sync.close();
		}
	});

	it("hydrates directly seeded public queue maps and keeps counts exact", async () => {
		const { sync } = createSync();
		try {
			// Tests and integrations seed the public maps directly; the defensive
			// hydration branches must keep working through the record-store fold.
			sync.syncInFlightQueue.set("seeded", [peerA]);
			sync.syncInFlightQueueInverted.set(peerA.hashcode(), new Set(["seeded"]));
			expect(
				(sync as any).hasPendingSyncClaim("seeded", peerA.hashcode()),
			).to.equal(true);
			expect((sync as any).pendingSyncClaimCount).to.equal(1);

			await sync.queueSync(["seeded"], peerB, { skipCheck: true });
			expect(
				sync.syncInFlightQueue.get("seeded")!.map((peer) => peer.hashcode()),
			).to.deep.equal([peerA.hashcode(), peerB.hashcode()]);
			expect((sync as any).pendingSyncClaimCount).to.equal(2);
			expectClaimCountMatchesClaimants(sync);

			sync.onEntryAddedHash("seeded");
			expectPendingSyncCensusEmpty(sync);
		} finally {
			await sync.close();
		}
	});

	it("sweeps a directly seeded in-flight row through the defensive scan", async () => {
		const { sync } = createSync();
		try {
			// A directly seeded in-flight row has no reverse-index entry; the
			// defensive scan must still find and remove it.
			sync.syncInFlight.set(
				peerA.hashcode(),
				new Map([["only-inflight", { timestamp: Date.now() }]]),
			);
			sync.onEntryAddedHash("only-inflight");
			expectPendingSyncCensusEmpty(sync);
		} finally {
			await sync.close();
		}
	});

	it("does not dispatch claims captured under a rotated dispatch epoch", async () => {
		let releaseHasMany!: (hashes: string[]) => void;
		const hasMany = sinon.stub().returns(
			new Promise<string[]>((resolve) => {
				releaseHasMany = resolve;
			}),
		);
		const { sync, send } = createSync({
			log: { has: async () => false, hasMany } as any,
		});
		try {
			const handling = sync.queueSync(["epoch-hash"], peerA);
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(hasMany.calledOnce).to.equal(true);

			// The peer disconnects while its admission lookup is still blocked:
			// the captured dispatch epoch is deleted, so the resumed queueSync
			// must not dispatch or retain claims for the stale epoch.
			sync.onPeerDisconnected(peerA);
			releaseHasMany([]);
			await handling;

			expect(send.called).to.equal(false);
			expect(sync.syncInFlightQueue.size).to.equal(0);
			expectPendingSyncCensusEmpty(sync);
		} finally {
			await sync.close();
		}
	});
});

// Stage-4 memory-growth pins (the leak fix). Storage resolvers and transport
// sends are not universally abortable: a call that never settles after its
// peer disconnected must not pin the peer's (or the global) slot quota. The
// release closures capture per-peer slot rows; disconnect detaches the row.
describe("sync-chunking slot-quota pins", () => {
	let peerA: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];

	before(async () => {
		peerA = (await Ed25519Keypair.create()).publicKey;
	});

	const waitFor = async (condition: () => boolean) => {
		for (let i = 0; i < 1_000; i++) {
			if (condition()) {
				return;
			}
			await Promise.resolve();
		}
		throw new Error("condition was not reached");
	};

	it("releases a disconnected peer's mid-lookup slots and restores the full quota", async () => {
		const resolveHashesForSymbols = sinon.stub().returns(new Promise(() => {}));
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 100 }),
			resolveHashesForSymbols,
		});
		try {
			void sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [1n] }),
				{ from: peerA } as any,
			);
			await waitFor(() => resolveHashesForSymbols.callCount === 1);
			expect((sync as any).pendingCoordinateLookupCount).to.equal(1);
			expect(
				(sync as any).pendingCoordinateLookupCountByPeer.get(peerA.hashcode()),
			).to.equal(1);

			sync.onPeerDisconnected(peerA);
			expect((sync as any).pendingCoordinateLookupCount).to.equal(0);
			expect((sync as any).pendingCoordinateLookupCountByPeer.size).to.equal(0);
			expect((sync as any).syncResponseSlotRows.size).to.equal(0);

			// The reconnected peer gets its full lookup quota back even though
			// the old resolver calls never settle.
			for (
				let index = 0;
				index < MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER;
				index += 1
			) {
				void sync.onMessage(
					new RequestMaybeSyncCoordinate({
						hashNumbers: [BigInt(10 + index)],
					}),
					{ from: peerA } as any,
				);
			}
			await waitFor(
				() =>
					resolveHashesForSymbols.callCount ===
					1 + MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);
			expect((sync as any).pendingCoordinateLookupCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);

			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [99n] }),
				{ from: peerA } as any,
			);
			expect(resolveHashesForSymbols.callCount).to.equal(
				1 + MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);
		} finally {
			await sync.close();
		}
	});

	it("nets slot accounting to zero and drops the idle row when work settles", async () => {
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 100 }),
			resolveHashListForSymbols: sinon.stub().returns(["settled-hash"]),
		});
		const ship = sinon.stub(sync as any, "shipExchangeHeads").resolves({
			messages: 1,
			fused: false,
		});
		try {
			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [1n] }),
				{ from: peerA } as any,
			);
			expect(ship.calledOnce).to.equal(true);
			expect((sync as any).pendingCoordinateLookupCount).to.equal(0);
			expect((sync as any).pendingCoordinateResponseCount).to.equal(0);
			expect((sync as any).pendingCoordinateLookupCountByPeer.size).to.equal(0);
			expect((sync as any).pendingCoordinateResponseCountByPeer.size).to.equal(
				0,
			);
			expect((sync as any).syncResponseSlotRows.size).to.equal(0);
		} finally {
			await sync.close();
		}
	});

	it("releases a blocked response-ship slot when the peer disconnects", async () => {
		let releaseShip!: () => void;
		const blockedShip = new Promise<{ messages: number; fused: boolean }>(
			(resolve) => {
				releaseShip = () => resolve({ messages: 1, fused: false });
			},
		);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 100 }),
			resolveHashListForSymbols: sinon.stub().returns(["ship-hash"]),
		});
		const ship = sinon
			.stub(sync as any, "shipExchangeHeads")
			.returns(blockedShip);
		try {
			const handling = sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [1n] }),
				{ from: peerA } as any,
			);
			await waitFor(() => ship.callCount === 1);
			expect((sync as any).pendingCoordinateLookupCount).to.equal(0);
			expect((sync as any).pendingCoordinateResponseCount).to.equal(1);

			sync.onPeerDisconnected(peerA);
			expect((sync as any).pendingCoordinateResponseCount).to.equal(0);
			expect((sync as any).pendingCoordinateResponseCountByPeer.size).to.equal(
				0,
			);
			expect((sync as any).syncResponseSlotRows.size).to.equal(0);

			// The blocked ship settling late is aggregate-neutral.
			releaseShip();
			await handling;
			expect((sync as any).pendingCoordinateResponseCount).to.equal(0);
			expect((sync as any).pendingCoordinateResponseCountByPeer.size).to.equal(
				0,
			);
		} finally {
			releaseShip();
			await sync.close();
		}
	});

	it("releases a disconnected peer's active response slots without touching a successor", async () => {
		const releases: (() => void)[] = [];
		const sendRawExchangeHeads = sinon.stub().callsFake(
			() =>
				new Promise<number>((resolve) => {
					releases.push(() => resolve(1));
				}),
		);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});
		const active: Promise<boolean>[] = [];
		try {
			const first = sync.expectMaybeSyncResponse({
				hashes: ["slot-hash-0"],
				targets: [peerA.hashcode()],
			});
			active.push(
				sync.onMessage(
					new ResponseMaybeSyncCapabilities({ hashes: ["slot-hash-0"] }),
					{ from: peerA } as any,
				),
			);
			await waitFor(() => sendRawExchangeHeads.callCount === 1);
			expect((sync as any).activeMaybeSyncResponseCount).to.equal(1);

			sync.onPeerDisconnected(peerA);
			expect((sync as any).activeMaybeSyncResponseCount).to.equal(0);
			expect((sync as any).activeMaybeSyncResponseCountByPeer.size).to.equal(0);

			// The reconnected peer's fresh response work uses a fresh row.
			const second = sync.expectMaybeSyncResponse({
				hashes: ["slot-hash-1"],
				targets: [peerA.hashcode()],
			});
			active.push(
				sync.onMessage(
					new ResponseMaybeSyncCapabilities({ hashes: ["slot-hash-1"] }),
					{ from: peerA } as any,
				),
			);
			await waitFor(() => sendRawExchangeHeads.callCount === 2);
			expect((sync as any).activeMaybeSyncResponseCount).to.equal(1);

			// The pre-disconnect ship settling late never decrements the
			// successor's accounting.
			releases.shift()!();
			await active.shift();
			expect((sync as any).activeMaybeSyncResponseCount).to.equal(1);
			expect(
				(sync as any).activeMaybeSyncResponseCountByPeer.get(peerA.hashcode()),
			).to.equal(1);

			releases.shift()!();
			await active.shift();
			expect((sync as any).activeMaybeSyncResponseCount).to.equal(0);
			expect((sync as any).activeMaybeSyncResponseCountByPeer.size).to.equal(0);
			first?.release();
			second?.release();
		} finally {
			for (const release of releases) {
				release();
			}
			await Promise.all(active);
			await sync.close();
		}
	});
});

describe("rateless-iblt-syncronizer slot-quota pins", () => {
	const waitFor = async (condition: () => boolean) => {
		for (let i = 0; i < 1_000; i++) {
			if (condition()) {
				return;
			}
			await Promise.resolve();
		}
		throw new Error("condition was not reached");
	};

	const createRateless = () =>
		new RatelessIBLTSynchronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		} as any);

	const createEntries = (count = 400) => {
		const entries = new Map<string, any>();
		for (let index = 0; index < count; index += 1) {
			const hash = `hash-${index}`;
			entries.set(hash, {
				hash,
				hashNumber: BigInt(index + 1),
				assignedToRangeBoundary: false,
			});
		}
		return entries;
	};

	it("frees a disconnected peer's response slots while open keeps accounting", async () => {
		const sync = createRateless();
		const releases: (() => void)[] = [];
		let blockShipments = true;
		const ship = sinon
			.stub(sync.simple, "shipAuthorizedMaybeSyncResponse")
			.callsFake(async () => {
				if (blockShipments) {
					await new Promise<void>((resolve) => releases.push(resolve));
				}
				return { messages: 1, fused: false, entries: 1 };
			});
		const from = { hashcode: () => "peer-a" } as any;
		const handlings: Promise<boolean>[] = [];
		try {
			await sync.onMaybeMissingEntries({
				entries: createEntries(),
				targets: ["peer-a"],
			});
			handlings.push(
				sync.onMessage(new ResponseMaybeSync({ hashes: ["hash-0"] }), {
					from,
				} as any),
			);
			await waitFor(() => ship.callCount === 1);
			expect((sync as any).activeRatelessResponseCount).to.equal(1);

			sync.onPeerDisconnected("peer-a");
			expect((sync as any).activeRatelessResponseCount).to.equal(0);
			expect((sync as any).activeRatelessResponseCountByPeer.size).to.equal(0);
			expect((sync as any).outgoingSyncProcessByTarget.size).to.equal(0);
			expect((sync as any).ratelessResponseSlotRows.size).to.equal(0);

			// The reconnected peer's fresh process gets a fresh row.
			await sync.onMaybeMissingEntries({
				entries: createEntries(),
				targets: ["peer-a"],
			});
			handlings.push(
				sync.onMessage(new ResponseMaybeSync({ hashes: ["hash-1"] }), {
					from,
				} as any),
			);
			await waitFor(() => ship.callCount === 2);
			expect((sync as any).activeRatelessResponseCount).to.equal(1);

			// open() rotation deliberately does NOT clear slot accounting:
			// cross-generation ship work is charged until it settles.
			await sync.open();
			expect((sync as any).activeRatelessResponseCount).to.equal(1);

			blockShipments = false;
			for (const release of releases) {
				release();
			}
			await Promise.all(handlings);
			// The pre-disconnect lease settles aggregate-neutrally; the live
			// lease returns its slot.
			expect((sync as any).activeRatelessResponseCount).to.equal(0);
			expect((sync as any).activeRatelessResponseCountByPeer.size).to.equal(0);
		} finally {
			blockShipments = false;
			for (const release of releases) {
				release();
			}
			await sync.close();
		}
	});
});
