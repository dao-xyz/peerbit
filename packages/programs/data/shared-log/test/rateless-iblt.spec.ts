import { Cache } from "@peerbit/cache";
import type { RequestContext } from "@peerbit/rpc";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
} from "../src/index.js";
import type { TransportMessage } from "../src/message.js";
import {
	MoreSymbols,
	RatelessIBLTSynchronizer,
	RequestAll,
	StartSync,
} from "../src/sync/rateless-iblt.js";
import { EventStore } from "./utils/stores/index.js";

const setup = {
	domain: createReplicationDomainHash("u64"),
	type: "u64" as const,
	syncronizer: RatelessIBLTSynchronizer,
	name: "u64-iblt",
	coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
};

describe("rateless-iblt-syncronizer", () => {
	let session: TestSession;
	let db1: EventStore<string, ReplicationDomainHash<"u64">>,
		db2: EventStore<string, ReplicationDomainHash<"u64">>;

	// Helper to capture messages from a log instance
	const collectMessages = async (
		log: EventStore<string, ReplicationDomainHash<"u64">>,
	) => {
		const onMessageSpy = sinon.spy(log.log, "onMessage");
		log.log.onMessage = onMessageSpy;
		return {
			get calls(): TransportMessage[] {
				const calls = onMessageSpy.getCalls() as Array<
					sinon.SinonSpyCall<[TransportMessage, RequestContext], Promise<void>>
				>;
				return calls.map((call) => call.args[0]);
			},
		};
	};

	const countMessages = (
		messages: TransportMessage[],
		type: new (...args: any[]) => TransportMessage,
	) => {
		return messages.filter((x) => x instanceof type).length;
	};

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
		await session.stop();
	});

	it("already synced", async () => {
		const syncedCount = 1000;
		await setupLogs(syncedCount, 0);

		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await db1.node.dial(db2.node.getMultiaddrs());

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

		await db1.node.dial(db2.node.getMultiaddrs());
		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(unsyncedCount),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(unsyncedCount),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db1Messages.calls, RequestAll)).to.equal(1);
		expect(countMessages(db1Messages.calls, StartSync)).to.equal(0);

		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db2Messages.calls, RequestAll)).to.equal(0);
		expect(countMessages(db2Messages.calls, StartSync)).to.equal(1);
	});

	it("one missing", async () => {
		const syncedCount = 1000;
		const unsyncedCount = 1;

		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await db1.node.dial(db2.node.getMultiaddrs());

		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0);
	});

	it("many missing", async () => {
		const syncedCount = 3000;
		const unsyncedCount = 3000;

		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await db1.node.dial(db2.node.getMultiaddrs());

		await waitForResolved(
			() =>
				expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
			{ timeout: 20000 },
		);
		await waitForResolved(
			() =>
				expect(db2.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
			{ timeout: 20000 },
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.be.greaterThan(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.be.greaterThan(0);
	});
});
