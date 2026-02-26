import { TestSession } from "@peerbit/test-utils";
import { TimeoutError, delay } from "@peerbit/time";
import { expect } from "chai";
import { RequestReplicationInfoMessage } from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("waitForReplicator", () => {
	let session: TestSession;
	let db: EventStore<string, any>;

	afterEach(async () => {
		if (db && db.closed === false) {
			await db.drop();
		}
		await session?.stop();
	});

	it("respects configured request retry limits", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
				waitForReplicatorRequestMaxAttempts: 2,
			},
		});

		const originalSend = db.log.rpc.send.bind(db.log.rpc);
		let requestCount = 0;
		db.log.rpc.send = async (message: any, options: any) => {
			if (message instanceof RequestReplicationInfoMessage) {
				requestCount++;
				return;
			}
			return originalSend(message, options);
		};

		// `open()` may schedule a best-effort replication info request to recently seen peers.
		// We only want to count the retries issued by `waitForReplicator()`.
		await delay(100);
		const baseline = requestCount;

		try {
			await db.log.waitForReplicator(session.peers[1].identity.publicKey, {
				timeout: 300,
				eager: true,
			});
			throw new Error("Expected waitForReplicator() to time out");
		} catch (error) {
			expect(error).to.be.instanceOf(TimeoutError);
		} finally {
			db.log.rpc.send = originalSend;
		}

		expect(requestCount - baseline).to.equal(2);
	});

	it("rejects waitForReplicators when internal leader check throws", async () => {
		session = await TestSession.connected(1);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
			},
		});

		const originalFindLeaders = db.log.findLeaders.bind(db.log);
		(db.log as any).findLeaders = async () => {
			throw new Error("forced-findLeaders-error");
		};

		try {
			await expect(
				(db.log as any)._waitForReplicators(
					[0n],
					{
						hash: "bafkreif4wi7jfhqqlvgyj7a5z2fi6zt2fx5b5h3h3rfwjz2wco6n2w2k7u",
						meta: { next: [] },
					},
					[],
					{ timeout: 200 },
				),
			).to.be.rejectedWith("forced-findLeaders-error");
		} finally {
			(db.log as any).findLeaders = originalFindLeaders;
		}
	});

	it("ignores persistCoordinate for missing or invalid hashes", async () => {
		session = await TestSession.connected(1);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
			},
		});

		const persistCoordinate = (db.log as any).persistCoordinate.bind(db.log);

		await expect(
			persistCoordinate({
				coordinates: [0n],
				entry: { hash: undefined, meta: { next: [] } },
				leaders: new Map(),
				replicas: 1,
			}),
		).to.not.be.rejected;

		await expect(
			persistCoordinate({
				coordinates: [0n],
				entry: { hash: "not-a-cid", meta: { next: [] } },
				leaders: new Map(),
				replicas: 1,
			}),
		).to.not.be.rejected;
	});
});
