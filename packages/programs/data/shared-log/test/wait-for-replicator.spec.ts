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
});
