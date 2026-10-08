import { deserialize, serialize } from "@dao-xyz/borsh";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { toId } from "@peerbit/indexer-interface";
import { SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	SYNC_CAPABILITY_ENTRY_INVENTORY,
	SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
} from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import {
	ConfirmEntriesMessage,
	ENTRY_INVENTORY_PAGE_SIZE,
	RequestEntryInventoryV1,
	RequestPersistedEntriesV1,
} from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

describe("entry inventory correlated presence", function () {
	this.timeout(60_000);
	let session: TestSession | undefined;
	let sandbox: sinon.SinonSandbox;
	let releases: (() => void)[];
	let pending: Promise<unknown>[];

	beforeEach(() => {
		sandbox = sinon.createSandbox();
		releases = [];
		pending = [];
	});

	afterEach(async () => {
		for (const release of releases) release();
		await Promise.allSettled(pending);
		sandbox.restore();
		await session?.stop();
		session = undefined;
	});

	const track = <T>(promise: Promise<T>) => {
		void promise.catch(() => {});
		pending.push(promise);
		return promise;
	};
	const gate = () => {
		const deferred = pDefer<void>();
		releases.push(() => deferred.resolve());
		return deferred;
	};
	const request = (hashes: string[], expectedReceiverSession = 1n) =>
		new RequestEntryInventoryV1({ hashes, expectedReceiverSession });
	const context = { message: { header: { session: 1n } } };
	const lane = { fromHash: "inventory-peer" };
	const newHash = async (index: number) =>
		(await calculateRawCid(new TextEncoder().encode(`inventory-${index}`))).cid;

	const openStore = async () => {
		session = await TestSession.disconnected(1);
		return session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
	};
	// Isolate storage/ingress assertions from transport setup. The wire tests below
	// retain the real session gate and negotiated capabilities.
	const allowInventorySession = (log: any) => {
		log._peerSyncCapabilities.set(
			lane.fromHash,
			SYNC_CAPABILITY_ENTRY_INVENTORY,
		);
		return sandbox
			.stub(log, "isEntryPresenceRequestSessionCurrent")
			.returns(true);
	};
	const storageSpies = (log: any) => [
		sandbox.spy(log.remoteBlocks.localStore, "hasMany"),
		sandbox.spy(log.log.entryIndex, "getShallow"),
		sandbox.spy(
			log._coordinates,
			"getAuthoritativeCoordinateEntryForInventory",
		),
		sandbox.spy(log, "withReplicationRangeMutationQueue"),
	];
	const expectNoStorage = (spies: sinon.SinonSpy[]) => {
		for (const spy of spies) expect(spy.notCalled).to.equal(true);
	};

	const openPair = async () => {
		session = await TestSession.connected(2);
		const args = {
			replicate: 1,
			replicas: { min: 2 },
			timeUntilRoleMaturity: 0,
		};
		const writer = await session.peers[0].open(new EventStore<string, any>(), {
			args,
		});
		const receiver = await EventStore.open<EventStore<string, any>>(
			writer.address!,
			session.peers[1],
			{ args },
		);
		await waitForResolved(
			() => {
				for (const [host, remote] of [
					[writer, receiver],
					[receiver, writer],
				]) {
					const log = host.log as any;
					const peerHash = remote.node.identity.publicKey.hashcode();
					expect(
						log._peerSyncCapabilities.get(peerHash) &
							SYNC_CAPABILITY_ENTRY_INVENTORY,
					).to.equal(SYNC_CAPABILITY_ENTRY_INVENTORY);
					expect(
						log._peerSyncCapabilities.get(peerHash) &
							SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
					).to.equal(0);
					expect(
						log._v2Receive.isCurrentActive({
							peerHash,
							peerSession: log._peerSessions.current(peerHash),
							receiveEpoch: log._peerSessions.receiveEpoch(peerHash),
							senderTransportSession:
								log._peerSyncCapabilitySessions.get(peerHash),
						}),
					).to.equal(true);
				}
			},
			{ timeout: 15_000 },
		);
		return { writer, receiver };
	};

	it("pins the bounded inventory capability and request variant [0,14]", () => {
		expect(SYNC_CAPABILITY_ENTRY_INVENTORY).to.equal(1 << 7);
		expect(ENTRY_INVENTORY_PAGE_SIZE).to.equal(128);
		const original = request(["a", "bc"], 0x0102030405060708n);
		const bytes = serialize(original);
		expect(Buffer.from(bytes).toString("hex")).to.equal(
			"00000e0807060504030201020000000100000061020000006263",
		);
		const decoded = deserialize(bytes, TransportMessage);
		expect(decoded).to.be.instanceOf(RequestEntryInventoryV1);
		expect(
			(decoded as RequestEntryInventoryV1).expectedReceiverSession,
		).to.equal(original.expectedReceiverSession);
		expect((decoded as RequestEntryInventoryV1).hashes).to.deep.equal(
			original.hashes,
		);
	});

	it("reports only the block/lower/authoritative-coordinate intersection, without durability work", async () => {
		const store = await openStore();
		const log = store.log as any;
		const lower = log.log.entryIndex;
		// Hold only background projection scheduling: these are genuinely accepted
		// lower entries, but their raw index rows have not been flushed yet.
		const schedule = sandbox.stub(lower, "schedulePendingIndexWriteFlush");
		const hashes: string[] = [];
		for (let index = 0; index < 5; index++) {
			hashes.push(
				(await store.add(`present-${index}`, { meta: { next: [] } })).entry
					.hash,
			);
		}
		allowInventorySession(log);
		await waitForResolved(() =>
			expect(lower.captureMutationGeneration()).to.not.equal(undefined),
		);
		const get = lower.getShallow.bind(lower);
		const coordinate =
			log._coordinates.getAuthoritativeCoordinateEntryForInventory.bind(
				log._coordinates,
			);
		for (const hash of hashes) {
			expect(await log.remoteBlocks.localStore.has(hash)).to.equal(true);
			expect(await lower.properties.index.get(toId(hash))).to.equal(undefined);
			expect(lower.pendingIndexWrites.has(hash)).to.equal(true);
			expect(await get(hash)).to.exist;
			expect(await coordinate(hash)).to.exist;
		}
		expect(schedule.callCount).to.equal(5);
		const blocks = sandbox
			.stub(log.remoteBlocks.localStore, "hasMany")
			.resolves([true, false, true, true, true]);
		const rows = sandbox
			.stub(lower, "getShallow")
			.callsFake((hash) =>
				hash === hashes[2] ? Promise.resolve(undefined) : get(hash),
			);
		const coordinates = sandbox
			.stub(log._coordinates, "getAuthoritativeCoordinateEntryForInventory")
			.callsFake((hash) =>
				hash === hashes[3] ? Promise.resolve(undefined) : coordinate(hash),
			);
		sandbox
			.stub(log._checkedPrune, "hasActiveWork")
			.callsFake((hash) => hash === hashes[4]);
		const flush = sandbox.spy(lower, "flushPendingWrites");
		const journal = sandbox.spy(
			log._coordinates,
			"flushNativeBackboneCoordinateJournal",
		);
		const durable = sandbox.spy(log, "resolvePersistedReceiptStorage");
		const mutation = sandbox.spy(log, "withReplicationRangeMutationQueue");
		expect(log._persistedReceiptStorage).to.equal(undefined);

		const response = await log.handleRequestEntryInventoryV1(
			request(hashes),
			context,
			lane,
		);
		expect(response).to.be.instanceOf(ConfirmEntriesMessage);
		expect(response.hashes).to.deep.equal([hashes[0]]);
		expect(blocks.calledOnceWithExactly(hashes)).to.equal(true);
		expect(rows.callCount).to.equal(4);
		expect(coordinates.callCount).to.equal(4);
		for (const spy of [flush, journal, durable, mutation])
			expect(spy.notCalled).to.equal(true);
		expect(log._persistedReceiptRequestsInFlightTotal).to.equal(0);
	});

	it("rejects empty, duplicate, malformed and oversized hashes before reads or locks", async () => {
		const log = (await openStore()).log as any;
		allowInventorySession(log);
		const hash = await newHash(0);
		const spies = storageSpies(log);
		const ingress = sandbox.spy(log, "admitPersistedReceiptIngress");
		const oversized = await Promise.all(
			Array.from({ length: 129 }, (_, i) => newHash(i)),
		);
		for (const hashes of [
			[],
			[hash, hash],
			["not-a-cid"],
			[""],
			["x".repeat(16_385)],
			oversized,
		]) {
			expect(
				await log.handleRequestEntryInventoryV1(request(hashes), context, lane),
			).to.equal(undefined);
		}
		expect(ingress.callCount).to.equal(6);
		expectNoStorage(spies);
		expect(log._persistedReceiptRequestsInFlightTotal).to.equal(0);
	});

	it("accepts exactly one 128-hash page and skips lower reads for absent blocks", async () => {
		const log = (await openStore()).log as any;
		allowInventorySession(log);
		const hashes = await Promise.all(
			Array.from({ length: 128 }, (_, i) => newHash(i)),
		);
		const blocks = sandbox
			.stub(log.remoteBlocks.localStore, "hasMany")
			.resolves(hashes.map(() => false));
		const rows = sandbox.spy(log.log.entryIndex, "getShallow");
		const coordinates = sandbox.spy(
			log._coordinates,
			"getAuthoritativeCoordinateEntryForInventory",
		);
		const response = await log.handleRequestEntryInventoryV1(
			request(hashes),
			context,
			lane,
		);
		expect(response.hashes).to.deep.equal([]);
		expect(blocks.calledOnceWithExactly(hashes)).to.equal(true);
		expect(rows.notCalled && coordinates.notCalled).to.equal(true);
	});

	it("does not admit work for a peer without the inventory capability", async () => {
		const log = (await openStore()).log as any;
		allowInventorySession(log);
		log._peerSyncCapabilities.delete(lane.fromHash);
		const spies = storageSpies(log);
		const ingress = sandbox.spy(log, "admitPersistedReceiptIngress");
		expect(
			await log.handleRequestEntryInventoryV1(
				request([await newHash(0)]),
				context,
				lane,
			),
		).to.equal(undefined);
		expect(ingress.notCalled).to.equal(true);
		expectNoStorage(spies);
	});

	it("shares ingress credits with persisted receipts instead of creating a second quota", async () => {
		const log = (await openStore()).log as any;
		allowInventorySession(log);
		sandbox.stub(log, "isPersistedReceiptRequestSessionCurrent").returns(true);
		sandbox.stub(Date, "now").returns(1_000_000);
		const spies = storageSpies(log);
		for (let index = 0; index < 16; index++) {
			expect(
				await log.handleRequestEntryInventoryV1(
					request(["not-a-cid"]),
					context,
					lane,
				),
			).to.equal(undefined);
		}
		expect(
			await log.handleRequestPersistedEntriesV1(
				new RequestPersistedEntriesV1({
					expectedReceiverSession: 1n,
					hashes: [await newHash(0)],
				}),
				context,
				lane,
			),
		).to.equal(undefined);
		expectNoStorage(spies);
	});

	it("does not read when the lower mutation generation cannot be captured", async () => {
		const log = (await openStore()).log as any;
		allowInventorySession(log);
		sandbox
			.stub(log.log.entryIndex, "captureMutationGeneration")
			.returns(undefined);
		const spies = storageSpies(log);
		const response = await log.handleRequestEntryInventoryV1(
			request([await newHash(0)]),
			context,
			lane,
		);
		expect(response.hashes).to.deep.equal([]);
		expectNoStorage(spies);
	});

	it("discards presence if a real lower mutation crosses the awaited block read", async () => {
		const store = await openStore();
		const log = store.log as any;
		const hash = (await store.add("before-read")).entry.hash;
		allowInventorySession(log);
		const lower = log.log.entryIndex;
		await waitForResolved(() =>
			expect(lower.captureMutationGeneration()).to.not.equal(undefined),
		);
		const readGate = gate();
		const original = log.remoteBlocks.localStore.hasMany.bind(
			log.remoteBlocks.localStore,
		);
		const blocks = sandbox
			.stub(log.remoteBlocks.localStore, "hasMany")
			.callsFake(async (value) => {
				const hashes = value as string[];
				if (hashes.length === 1 && hashes[0] === hash) await readGate.promise;
				return original(hashes);
			});
		const response = track(
			log.handleRequestEntryInventoryV1(request([hash]), context, lane),
		);
		await waitForResolved(() => expect(blocks.called).to.equal(true));
		await store.add("intervening-write");
		readGate.resolve();
		expect(((await response) as ConfirmEntriesMessage).hashes).to.deep.equal(
			[],
		);
		expect(log._persistedReceiptRequestsInFlightTotal).to.equal(0);
	});

	it("drops a reply when its current-session fence becomes stale and releases the in-flight slot", async () => {
		const log = (await openStore()).log as any;
		const current = allowInventorySession(log);
		const readGate = gate();
		const blocks = sandbox
			.stub(log.remoteBlocks.localStore, "hasMany")
			.callsFake(async () => {
				await readGate.promise;
				return [true];
			});
		const response = track(
			log.handleRequestEntryInventoryV1(
				request([await newHash(0)]),
				context,
				lane,
			),
		);
		await waitForResolved(() => expect(blocks.calledOnce).to.equal(true));
		expect(log._persistedReceiptRequestsInFlightTotal).to.equal(1);
		current.returns(false);
		readGate.resolve();
		expect(((await response) as ConfirmEntriesMessage).hashes).to.deep.equal(
			[],
		);
		expect(log._persistedReceiptRequestsInFlightTotal).to.equal(0);
		expect(log._persistedReceiptRequestsInFlight.has(lane.fromHash)).to.equal(
			false,
		);
	});

	it("rejects a stale receiver session before ingress or local reads", async () => {
		const { writer, receiver } = await openPair();
		const log = receiver.log as any;
		const fromHash = writer.node.identity.publicKey.hashcode();
		const realContext = {
			from: writer.node.identity.publicKey,
			message: {
				header: { session: (writer.log as any).ownTransportSession() },
			},
		};
		const realLane = {
			fromHash,
			session: log._peerSessions.current(fromHash),
			receiveEpoch: log._peerSessions.receiveEpoch(fromHash),
			ownershipLifecycleController: log.captureReplicationOwnershipLifecycle(),
		};
		const hash = await newHash(0);
		expect(
			log.isEntryPresenceRequestSessionCurrent(
				request([hash], log.ownTransportSession()),
				realContext,
				realLane,
			),
		).to.equal(true);
		const spies = storageSpies(log);
		const ingress = sandbox.spy(log, "admitPersistedReceiptIngress");
		expect(
			await log.handleRequestEntryInventoryV1(
				request([hash], log.ownTransportSession() + 1n),
				realContext,
				realLane,
			),
		).to.equal(undefined);
		expect(ingress.notCalled).to.equal(true);
		expectNoStorage(spies);
	});

	it("correlates out-of-order RPC replies and ignores an uncorrelated confirmation", async () => {
		const { writer, receiver } = await openPair();
		const receiverLog = receiver.log as any;
		const writerLog = writer.log as any;
		const hashes = [
			(await writer.add("first-inventory", { meta: { next: [] } })).entry.hash,
			(await writer.add("second-inventory", { meta: { next: [] } })).entry.hash,
		];
		await waitForResolved(
			async () => {
				for (const hash of hashes) {
					expect(await receiverLog.log.entryIndex.getShallow(hash)).to.exist;
					expect(
						await receiverLog._coordinates.getAuthoritativeCoordinateEntryForInventory(
							hash,
						),
					).to.exist;
				}
				expect(
					receiverLog.log.entryIndex.captureMutationGeneration(),
				).to.not.equal(undefined);
			},
			{ timeout: 15_000 },
		);
		expect(receiverLog._persistedReceiptStorage).to.equal(undefined);
		const gates = [gate(), gate()];
		const entered = new Map<string, ConfirmEntriesMessage | undefined>();
		const original =
			receiverLog.handleRequestEntryInventoryV1.bind(receiverLog);
		sandbox
			.stub(receiverLog, "handleRequestEntryInventoryV1")
			.callsFake(async (...args: any[]) => {
				const result = await original(...args);
				const hash = args[0].hashes[0];
				const index = hashes.indexOf(hash);
				if (args[0].hashes.length !== 1 || index < 0) return result;
				entered.set(hash, result);
				await gates[index].promise;
				return result;
			});
		let unsolicitedSeen = false;
		const onMessage = writerLog.onMessage.bind(writerLog);
		sandbox.stub(writerLog, "onMessage").callsFake(async (message, ctx) => {
			const result = await onMessage(message, ctx);
			if (
				message instanceof ConfirmEntriesMessage &&
				message.hashes.join() === [...hashes].reverse().join()
			) {
				unsolicitedSeen = true;
			}
			return result;
		});
		let firstResolved = false;
		const ask = (hash: string) =>
			track(
				writer.log.rpc.request(
					request([hash], receiverLog.ownTransportSession()),
					{
						mode: new SilentDelivery({
							to: [receiver.node.identity.publicKey],
							redundancy: 1,
						}),
						amount: 1,
						timeout: 10_000,
					},
				),
			);
		const first = ask(hashes[0]).then((result) => {
			firstResolved = true;
			return result;
		});
		track(first);
		const second = ask(hashes[1]);
		await waitForResolved(() => expect(entered.size).to.equal(2), {
			timeout: 5_000,
		});
		for (const hash of hashes)
			expect(entered.get(hash)?.hashes).to.deep.equal([hash]);
		await receiver.log.rpc.send(
			new ConfirmEntriesMessage({ hashes: [...hashes].reverse() }),
			{
				mode: new SilentDelivery({
					to: [writer.node.identity.publicKey],
					redundancy: 1,
				}),
			},
		);
		await waitForResolved(() => expect(unsolicitedSeen).to.equal(true), {
			timeout: 5_000,
		});
		expect(firstResolved).to.equal(false);
		gates[1].resolve();
		const secondReplies = await second;
		expect(firstResolved).to.equal(false);
		expect(secondReplies).to.have.length(1);
		expect(
			(secondReplies[0].response as ConfirmEntriesMessage).hashes,
		).to.deep.equal([hashes[1]]);
		expect(
			secondReplies[0].from?.equals(receiver.node.identity.publicKey),
		).to.equal(true);
		gates[0].resolve();
		const firstReplies = await first;
		expect(firstReplies).to.have.length(1);
		expect(
			(firstReplies[0].response as ConfirmEntriesMessage).hashes,
		).to.deep.equal([hashes[0]]);
	});
});
