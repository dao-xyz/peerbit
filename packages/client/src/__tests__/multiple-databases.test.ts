import { Peerbit } from "../peer";
import { EventStore } from "./utils/stores";
import { jest } from "@jest/globals";
import { v4 as uuid } from "uuid";
import { waitForPeers, LSession } from "@dao-xyz/peerbit-test-utils";
import { delay } from "@dao-xyz/peerbit-time";

describe(`Multiple Databases`, function () {
	jest.setTimeout(60000);

	let session: LSession;
	let client1: Peerbit, client2: Peerbit, client3: Peerbit;

	let localDatabases: EventStore<string>[] = [];
	let remoteDatabasesA: EventStore<string>[] = [];
	let remoteDatabasesB: EventStore<string>[] = [];

	const dbCount = 2;

	// Create two IPFS instances and two client instances (2 nodes/peers)
	beforeAll(async () => {
		session = await LSession.connected(3);

		client1 = await Peerbit.create(session.peers[0], {});
		client2 = await Peerbit.create(session.peers[1], {});
		client3 = await Peerbit.create(session.peers[2], {});
		client2._minReplicas = 3;
		client3._minReplicas = 3;
		client1._minReplicas = 3;
	});

	afterAll(async () => {
		if (client1) await client1.stop();

		if (client2) await client2.stop();

		if (client3) await client3.stop();
		await session.stop();
	});

	beforeEach(async () => {
		// Set write access for both clients

		// Open the databases on the first node
		const options = {};

		// Open the databases on the first node
		for (let i = 0; i < dbCount; i++) {
			const db = await client1.open(
				new EventStore<string>({ id: "local-" + i }),
				{ ...options }
			);
			localDatabases.push(db);
		}
		for (let i = 0; i < dbCount; i++) {
			const db = await client2.open<EventStore<string>>(
				await EventStore.load<EventStore<string>>(
					client2.libp2p.directblock,
					localDatabases[i].address!
				),
				{ ...options }
			);
			remoteDatabasesA.push(db);
		}

		for (let i = 0; i < dbCount; i++) {
			const db = await client3.open<EventStore<string>>(
				await EventStore.load<EventStore<string>>(
					client3.libp2p.directblock,
					localDatabases[i].address!
				),
				{ ...options }
			);
			remoteDatabasesB.push(db);
		}

		// Wait for the peers to connect
		for (const db of localDatabases) {
			await waitForPeers(session.peers[0], [client2.id], db.address.toString());
			await waitForPeers(session.peers[1], [client1.id], db.address.toString());
			await waitForPeers(session.peers[2], [client1.id], db.address.toString());
		}

	});

	afterEach(async () => {
		/*  for (let db of remoteDatabasesA)
	 await db.drop()

   for (let db of remoteDatabasesB)
	 await db.drop()

   for (let db of localDatabases)
	 await db.drop() */
	});

	it("replicates multiple open databases", async () => {
		const entryCount = 1;
		const entryArr: number[] = [];

		// Create an array that we use to create the db entries
		for (let i = 1; i < entryCount + 1; i++) entryArr.push(i);

		// Write entries to each database
		for (let index = 0; index < dbCount; index++) {
			const db = localDatabases[index];
			entryArr.forEach((val) => db.add("hello-" + val));
		}

		// Function to check if all databases have been replicated
		const allReplicated = () => {
			return (
				remoteDatabasesA.every(
					(db) => db.store._oplog.length === entryCount
				) &&
				remoteDatabasesB.every(
					(db) => db.store._oplog.length === entryCount
				)
			);
		};

		// check data
		await new Promise((resolve, reject) => {
			const interval = setInterval(async () => {
				if (allReplicated()) {
					clearInterval(interval);

					await delay(3000); // add some delay, so that we absorb any extra (unwanted) replication

					// Verify that the databases contain all the right entries
					remoteDatabasesA.forEach((db) => {
						try {
							const result = db
								.iterator({ limit: -1 })
								.collect().length;
							expect(result).toEqual(entryCount);
							expect(db.store._oplog.length).toEqual(entryCount);
						} catch (error) {
							reject(error);
						}
					});

					remoteDatabasesB.forEach((db) => {
						try {
							const result = db
								.iterator({ limit: -1 })
								.collect().length;
							expect(result).toEqual(entryCount);
							expect(db.store._oplog.length).toEqual(entryCount);
						} catch (error) {
							reject(error);
						}
					});
					resolve(true);
				}
			}, 200);
		});

		// check gracefully shut down (with no leak)
		const subscriptions = client3.libp2p.directsub.topics;
		expect(subscriptions.size).toEqual(dbCount);
		for (let i = 0; i < dbCount; i++) {
			await remoteDatabasesB[i].drop();
			await delay(3000);
			expect(client3.libp2p.directsub.topics.size).toEqual(dbCount - i - 1)
		}
	});
});
