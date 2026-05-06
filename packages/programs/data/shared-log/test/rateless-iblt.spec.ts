import { deserialize, serialize } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
} from "../src/index.js";
import { TransportMessage } from "../src/message.js";
import {
	MoreSymbols,
	RatelessIBLTSynchronizer,
	RequestAll,
	StartSync,
} from "../src/sync/rateless-iblt.js";
import { RequestMaybeSyncCoordinate } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

const setup = {
	domain: createReplicationDomainHash("u64"),
	type: "u64" as const,
	syncronizer: RatelessIBLTSynchronizer,
	name: "u64-iblt",
	coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
};

describe("rateless-iblt-syncronizer", () => {
	let session: TestSession | undefined;
	let db1: EventStore<string, ReplicationDomainHash<"u64">>,
		db2: EventStore<string, ReplicationDomainHash<"u64">>;

	// Helper to capture messages from a log instance
	const collectMessages = async (
		log: EventStore<string, ReplicationDomainHash<"u64">>,
	) => {
		const messages: TransportMessage[] = [];
		const onMessage = log.log.rpc["_responseHandler"];
		log.log.rpc["_responseHandler"] = async (msg: any, context: any) => {
			messages.push(msg);
			return onMessage(msg, context);
		};
		return {
			get calls(): TransportMessage[] {
				return messages;
			},
		};
	};

	const countMessages = (
		messages: TransportMessage[],
		type: new (...args: any[]) => TransportMessage,
	) => {
		return messages.filter((x) => x instanceof type).length;
	};

	it("roundtrips coded symbol batches through existing transport variants", () => {
		const symbols = [
			{ count: 1n, hash: 2n, symbol: 3n },
			{ count: 4n, hash: 5n, symbol: 6n },
		];
		const expectedFlat = [1n, 2n, 3n, 4n, 5n, 6n];

		const startSyncBytes = serialize(
			new StartSync({ from: 7n, to: 11n, symbols }),
		);
		const emptyStartSyncBytes = serialize(
			new StartSync({ from: 7n, to: 11n, symbols: [] }),
		);
		expect(startSyncBytes.length - emptyStartSyncBytes.length).to.equal(48);
		const startSync = deserialize(startSyncBytes, TransportMessage);
		expect(startSync).to.be.instanceOf(StartSync);
		expect((startSync as StartSync).symbols.length).to.equal(symbols.length);
		expect(Array.from((startSync as StartSync).symbols.toFlat())).to.deep.equal(
			expectedFlat,
		);

		const moreSymbolsBytes = serialize(
			new MoreSymbols({
				syncId: new Uint8Array(32).fill(1),
				lastSeqNo: 1n,
				symbols,
			}),
		);
		const emptyMoreSymbolsBytes = serialize(
			new MoreSymbols({
				syncId: new Uint8Array(32).fill(1),
				lastSeqNo: 1n,
				symbols: [],
			}),
		);
		expect(moreSymbolsBytes.length - emptyMoreSymbolsBytes.length).to.equal(48);
		const moreSymbols = deserialize(moreSymbolsBytes, TransportMessage);
		expect(moreSymbols).to.be.instanceOf(MoreSymbols);
		expect((moreSymbols as MoreSymbols).symbols.length).to.equal(
			symbols.length,
		);
		expect(
			Array.from((moreSymbols as MoreSymbols).symbols.toFlat()),
		).to.deep.equal(expectedFlat);
	});

	const setupLogs = async (
		syncedCount: number,
		unsyncedCount: number,
		oneSided = false,
	) => {
		session = await TestSession.disconnected(2);
		db1 = await session.peers[0].open(
			new EventStore<string, ReplicationDomainHash<"u64">>(),
			{
				args: {
					replicate: { factor: 1 },
					setup,
				},
			},
		);

		db2 = await session.peers[1].open(db1.clone(), {
			args: {
				replicate: { factor: 1 },
				setup,
			},
		});

		// Add synced entries (present on both logs)
		for (let i = 0; i < syncedCount; i++) {
			const out = await db1.add("test", { meta: { next: [] } });
			await db2.log.join([out.entry]);
		}

		// Add unsynced entries (present on one or both logs)
		for (let i = 0; i < unsyncedCount; i++) {
			await db1.add("test", { meta: { next: [] } });
			if (!oneSided) {
				await db2.add("test", { meta: { next: [] } });
			}
		}

		expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount);
		expect(db2.log.log.length).to.equal(
			syncedCount + (oneSided ? 0 : unsyncedCount),
		);
	};

	afterEach(async () => {
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	it("already synced", async function () {
		this.timeout(120_000);
		const syncedCount = 1000;
		await setupLogs(syncedCount, 0);

		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();

		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(syncedCount),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0);
	});

	it("all missing will skip iblt syncing", async () => {
		const syncedCount = 0;
		const unsyncedCount = 1000;
		const oneSided = true;

		await setupLogs(syncedCount, unsyncedCount, oneSided);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();
		await Promise.all([
			db1.log.waitForReplicator(session!.peers[1].identity.publicKey, {
				timeout: 15_000,
				roleAge: 0,
			}),
			db2.log.waitForReplicator(session!.peers[0].identity.publicKey, {
				timeout: 15_000,
				roleAge: 0,
			}),
		]);
		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(unsyncedCount),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(unsyncedCount),
		);

		await waitForResolved(() => {
			const totalMoreSymbols =
				countMessages(db1Messages.calls, MoreSymbols) +
				countMessages(db2Messages.calls, MoreSymbols);
			const totalRequestAll =
				countMessages(db1Messages.calls, RequestAll) +
				countMessages(db2Messages.calls, RequestAll);
			const totalStartSync =
				countMessages(db1Messages.calls, StartSync) +
				countMessages(db2Messages.calls, StartSync);
			// Direction can vary with scheduling, but behavior should remain:
			// no incremental IBLT symbol exchange and at least one fallback/full-sync trigger.
			expect(totalMoreSymbols).to.equal(0);
			expect(totalRequestAll).to.be.greaterThan(0);
			expect(totalStartSync).to.be.greaterThan(0);
		});
	});

	it("one missing", async () => {
		const syncedCount = 1000;
		const unsyncedCount = 1;

		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();

		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0);
	});

	it("large missing sets dispatch with rateless IBLT", async () => {
		const sentMessages: TransportMessage[] = [];
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: {
				send: async (message: TransportMessage) => {
					sentMessages.push(message);
				},
			} as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});

		try {
			const entries = new Map<string, any>();
			for (let i = 0; i < 3000; i++) {
				const hash = `hash-${i}`;
				entries.set(hash, {
					hash,
					hashNumber: BigInt(i + 1),
					assignedToRangeBoundary: false,
				});
			}

			await sync.onMaybeMissingEntries({
				entries,
				targets: ["target"],
			});

			const startSyncMessages = sentMessages.filter(
				(message) => message instanceof StartSync,
			);
			expect(sentMessages).to.have.length(1);
			expect(startSyncMessages).to.have.length(1);
			expect(
				(startSyncMessages[0] as StartSync).symbols.length,
			).to.be.greaterThan(0);
		} finally {
			await sync.close();
		}
	});

	it("skips decoded remote symbols that are already present locally", async () => {
		const sentMessages: TransportMessage[] = [];
		let localPresenceChecks = 0;
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: {
				send: async (message: TransportMessage) => {
					sentMessages.push(message);
				},
			} as any,
			rangeIndex: {} as any,
			entryIndex: {
				count: async () => {
					localPresenceChecks += 1;
					return 1;
				},
			} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});

		(sync as any).getLocalDecoderForRange = async () => ({
			add_coded_symbol: () => {},
			try_decode: () => {},
			decoded: () => true,
			get_remote_symbols: () => [42n],
			free: () => {},
		});

		try {
			await sync.onMessage(
				new StartSync({
					from: 0n,
					to: 100n,
					symbols: [{ count: 0n, hash: 0n, symbol: 0n } as any],
				}),
				{
					from: {
						hashcode: () => "peer-a",
						equals: () => false,
					},
				} as any,
			);

			await waitForResolved(() => expect(localPresenceChecks).to.equal(1));
			expect(
				sentMessages.some(
					(message) => message instanceof RequestMaybeSyncCoordinate,
				),
			).to.equal(false);
		} finally {
			await sync.close();
		}
	});

	it("many missing", async function () {
		this.timeout(120_000);
		const syncedCount = 3000;
		const unsyncedCount = 3000;

		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();

		const expectedCount = syncedCount + unsyncedCount * 2;
		await Promise.all([
			waitForResolved(
				() => expect(db1.log.log.length).to.equal(expectedCount),
				{ timeout: 60_000 },
			),
			waitForResolved(
				() => expect(db2.log.log.length).to.equal(expectedCount),
				{ timeout: 60_000 },
			),
		]);

		const totalRequestAll =
			countMessages(db1Messages.calls, RequestAll) +
			countMessages(db2Messages.calls, RequestAll);
		// Depending on exchange-head timing, the peers may converge before rateless
		// repair needs to send StartSync. The deterministic dispatch test above
		// covers large-set IBLT selection; this integration path must not fall back
		// to full RequestAll transfer.
		expect(totalRequestAll).to.equal(0);
	});
});
