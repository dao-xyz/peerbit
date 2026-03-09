import { randomBytes, toBase64 } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import { EventStore } from "./utils/stores/event-store.js";

describe("refresh rejoin recovery", function () {
	let session: TestSession;
	let db0: EventStore<string, any> | undefined;
	let db1: EventStore<string, any> | undefined;
	let db2: EventStore<string, any> | undefined;

	afterEach(async () => {
		await session?.stop();
		if (db0 && db0.closed === false) await db0.drop();
		if (db1 && db1.closed === false) await db1.drop();
		if (db2 && db2.closed === false) await db2.drop();
	});

	it("reconverges after a full peer restart during large catch-up", async function () {
		this.timeout(180_000);

		const baseDir = `./tmp/shared-log/refresh-rejoin/${uuid()}`;
		session = await TestSession.connected(3, [
			{ directory: `${baseDir}/0` },
			{ directory: `${baseDir}/1` },
			{ directory: `${baseDir}/2` },
		]);

		const args = {
			replicate: { factor: 1 },
			replicas: { min: 3 },
			timeUntilRoleMaturity: 0,
			waitForReplicatorTimeout: 30_000,
		};

		const store = new EventStore<string, any>();
		db0 = await session.peers[0].open(store, { args });
		db1 = await session.peers[1].open(store.clone(), { args });

		await waitForResolved(
			async () => expect((await db0!.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const entryCount = 24;
		const payload = toBase64(randomBytes(256 * 1024));
		for (let i = 0; i < entryCount; i++) {
			await db0.add(`${i}:${payload}`);
		}

		await waitForResolved(
			() => expect(db1!.log.log.length).to.equal(entryCount),
			{ timeout: 60_000, delayInterval: 200 },
		);

		const allReplicators = [
			session.peers[0].identity.publicKey.hashcode(),
			session.peers[1].identity.publicKey.hashcode(),
			session.peers[2].identity.publicKey.hashcode(),
		].sort();

		const waitForDb2 = async () => {
			await Promise.all([
				db0!.waitFor(session.peers[2].peerId),
				db1!.waitFor(session.peers[2].peerId),
				db2!.waitFor(session.peers[0].peerId),
				db2!.waitFor(session.peers[1].peerId),
				db0!.log.waitForReplicator(session.peers[2].identity.publicKey, {
					eager: true,
					timeout: 60_000,
				}),
				db1!.log.waitForReplicator(session.peers[2].identity.publicKey, {
					eager: true,
					timeout: 60_000,
				}),
			]);
		};

		const expectReplicators = async () => {
			const sets = await Promise.all([
				db0!.log.getReplicators(),
				db1!.log.getReplicators(),
				db2!.log.getReplicators(),
			]);
			for (const replicators of sets) {
				expect([...replicators].sort()).to.deep.equal(allReplicators);
			}
		};

		const expectCaughtUp = () => {
			expect(db0!.log.log.length).to.equal(entryCount);
			expect(db1!.log.log.length).to.equal(entryCount);
			expect(db2!.log.log.length).to.equal(entryCount);
		};

		db2 = await session.peers[2].open<EventStore<string, any>>(db0.address!, {
			args,
		});
		await waitForDb2();
		await Promise.all([
			db0.log.rebalanceAll({ clearCache: true }),
			db1.log.rebalanceAll({ clearCache: true }),
			db2.log.rebalanceAll({ clearCache: true }),
		]);
		await waitForResolved(expectReplicators, {
			timeout: 90_000,
			delayInterval: 500,
		});
		await waitForResolved(expectCaughtUp, {
			timeout: 90_000,
			delayInterval: 500,
		});

		await session.peers[2].stop();
		await session.peers[2].start();

		db2 = await session.peers[2].open<EventStore<string, any>>(db0.address!, {
			args,
		});
		await waitForDb2();
		await Promise.all([
			db0.log.rebalanceAll({ clearCache: true }),
			db1.log.rebalanceAll({ clearCache: true }),
			db2.log.rebalanceAll({ clearCache: true }),
		]);
		await waitForResolved(expectReplicators, {
			timeout: 90_000,
			delayInterval: 500,
		});
		await waitForResolved(expectCaughtUp, {
			timeout: 90_000,
			delayInterval: 500,
		});
	});
});
