import { LSession } from "@peerbit/test-utils";
import { delay } from "@peerbit/time";
import { EventStore } from "./utils/event-store";

describe(`Multiple Databases`, function () {
	let session: LSession;

	let localDatabases: EventStore[] = [];
	let remoteDatabasesA: EventStore[] = [];
	let remoteDatabasesB: EventStore[] = [];

	const dbCount = 2;

	// Create two IPFS instances and two client instances (2 nodes/peers)
	beforeAll(async () => {
		session = await LSession.connected(3);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		// Set write access for both clients
		// Open the databases on the first node
		/* for (let i = 0; i < dbCount; i++) {
			const db = await session.peers[0].open(new EventStore<string>());
			localDatabases.push(db);
		}
		for (let i = 0; i < dbCount; i++) {
			const db = await session.peers[1].open<EventStore<string>>(
				(await EventStore.load<EventStore<string>>(
					localDatabases[i].address!,
					session.peers[2].services.blocks
				))!
			);
			remoteDatabasesA.push(db);
		}

		for (let i = 0; i < dbCount; i++) {
			const db = await client3.open<EventStore<string>>(
				(await EventStore.load<EventStore<string>>(
					localDatabases[i].address!,
					session.peers[2].services.blocks
				))!
			);
			remoteDatabasesB.push(db);
		}

		// Wait for the peers to connect
		for (const db of localDatabases) {
			await db.waitFor(session.peers[1].peerId);
			await db.waitFor(session.peers[2].peerId);
		} */
	});

	afterEach(async () => {
		/*  for (let db of remoteDatabasesA)
	 await db.drop()
	
	for (let db of remoteDatabasesB)
	 await db.drop()
	
	for (let db of localDatabases)
	 await db.drop() */
	});

	/* it("replicates multiple open databases", async () => {
		const entryCount = 1;
		const entryArr: number[] = [];

		// Create an array that we use to create the db entries
		for (let i = 1; i < entryCount + 1; i++) entryArr.push(i);

		// Write entries to each database
		for (let index = 0; index < dbCount; index++) {
			const db = localDatabases[index];
			entryArr.forEach((val) => db.add(Buffer.from("hello-" + val)));
		}

		// Function to check if all databases have been replicated
		const allReplicated = () => {
			return (
				remoteDatabasesA.every((db) => db.log.length === entryCount) &&
				remoteDatabasesB.every((db) => db.log.length === entryCount)
			);
		};

		// check data
		await new Promise((resolve, reject) => {
			const interval = setInterval(async () => {
				if (allReplicated()) {
					clearInterval(interval);

					await delay(3000); // add some delay, so that we absorb any extra (unwanted) replication

					// Verify that the databases contain all the right entries
					for (const db of remoteDatabasesA) {
						try {
							const result = db.log.length
							expect(result).toEqual(entryCount);
							expect(db.log.length).toEqual(entryCount);
						} catch (error) {
							reject(error);
						}
					}

					for (const db of remoteDatabasesB) {
						try {
							const result = db.log.length
							expect(result).toEqual(entryCount);
							expect(db.log.length).toEqual(entryCount);
						} catch (error) {
							reject(error);
						}
					}
					resolve(true);
				}
			}, 200);
		});

		// check gracefully shut down (with no leak)
		const subscriptions = session.peers[2].services.pubsub.topics;
		expect(subscriptions.size).toEqual(dbCount);
		for (let i = 0; i < dbCount; i++) {
			await remoteDatabasesB[i].drop();
			expect(session.peers[2].services.pubsub.topics.size).toEqual(
				dbCount - i - 1
			);
		}
	}); */
});
