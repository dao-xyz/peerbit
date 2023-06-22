import assert from "assert";
import path from "path";
import { Peerbit } from "../peer.js";

// @ts-ignore
import { v4 as uuid } from "uuid";

// Include test utilities
import { Program } from "@peerbit/program";
import { LevelBlockStore } from "@peerbit/blocks";
import { EventStore } from "./utils/event-store.js";

const dbPath = path.join("./peerbit", "tests", "create-open");
describe("Open", function () {
	let client: Peerbit;
	//jest.retryTimes(1); // TODO Side effects may cause failures

	beforeAll(async () => {
		client = await Peerbit.create({
			directory: dbPath + uuid(),
		});
	});
	afterAll(async () => {
		if (client) {
			await client.stop();
		}
	});

	describe("Open", () => {
		it("opens a database - name only", async () => {
			const db = await client.open(new EventStore());
			assert.equal(db.address!.toString().indexOf("/peerbit"), 0);
			assert.equal(db.address!.toString().indexOf("zb"), 9);
			await db.drop();
		});

		it("can open multiple times", async () => {
			const db = await client.open(new EventStore());
			db.save = () => {
				throw new Error("Did not expect resave");
			};
			await client.open(db.address!);
		});

		/* TODO feat 
		it("opens a database - with a different identity", async () => {
			const signKey = await Ed25519Keypair.create();
			const topic = uuid();
			const db = await client.open(new EventStore(), {
				identity: signKey,
			});
			assert.equal(db.address!.toString().indexOf("/peerbit"), 0);
			assert.equal(db.address!.toString().indexOf("zb"), 9);
			expect(db.log.identity.publicKey.equals(signKey.publicKey));
			await db.drop();
		}); */

		it("opens the same database - from an address", async () => {
			const db = await client.open(new EventStore());
			const db2 = await client.open(
				(await Program.load(db.address!, client.libp2p.services.blocks))!
			);
			assert.equal(db2.address!.toString().indexOf("/peerbit"), 0);
			assert.equal(db2.address!.toString().indexOf("zb"), 9);
			await db.drop();
			await db2.drop();
		});

		/* 	const address = new Address({
				cid: "zdpuAnmza2vimH3drqNJji1rckA7x8jUfvi7miWzpePDtvHKJ" // a random cid
			}); */
		it("doesn't open a database if we don't have it locally", async () => {
			const db = await client.open(new EventStore());
			await db.drop();
			await (
				client.libp2p.services.blocks["_localStore"] as LevelBlockStore
			).idle();
			const dbToLoad = await Program.load(
				db.address!,
				client.libp2p.services.blocks,
				{ timeout: 3000 }
			);
			expect(dbToLoad).toBeUndefined();
		});

		it("saves database manifest file locally", async () => {
			const db = await client.open(new EventStore());
			const loaded = (await Program.load(
				db.address!,
				client.libp2p.services.blocks
			)) as EventStore;
			expect(loaded).toBeDefined();
			expect(loaded.log).toBeDefined();

			await db.drop();
		});

		/* 
	  TODO feat
	  it("can pass local database directory as an option", async () => {
		  const dir = "./peerbit/tests/another-feed-" + uuid();
		  const db2 = await client.open(new EventStore({ id: randomBytes(32) }), {
			  directory: dir,
		  });
		  expect(fs.existsSync(dir)).toEqual(true);
		  await db2.close();
	  }); */
	});

	/*  TODO, this test throws error, but not the expected one
	it('throws an error if trying to open a database locally and we don\'t have it', async () => {
	const db = await client.open(new EventStore({ id: 'abc' }), { replicationTopic })
	const address = new Address(db.address.cid.slice(0, -1) + 'A')
	await db.drop()
	try {
	 await client.open(address, { replicationTopic, localOnly: true, timeout: 3000 })
	 throw new Error('Shouldn\'t open the database')
	} catch (error: any) {
	 expect(error.toString()).toEqual(`Error: Database '${address}' doesn't exist!`)
	}
	}) */

	describe("Close", function () {
		let client: Peerbit;

		beforeEach(async () => {
			client = await Peerbit.create({
				directory: dbPath + uuid(),
			});
		});
		afterEach(async () => {
			if (client) {
				await client.stop();
			}
		});

		/* it("closes when disconnecting", async () => {
			const db = await client.open(new EventStore());
			await client.stop();
			expect(db.log.headsIndex.headsCache?.cache?.["_store"].status).toEqual(
				"closed"
			);
		});

		it("closes a custom store", async () => {
			const db = await client.open(new EventStore());
			await db.close();
			expect(db.log.headsIndex.headsCache?.cache?.["_store"].status).toEqual(
				"closed"
			);
		}); */

		/* TODO fix
		
	it("close load close sets status to 'closed'", async () => {
	  const directory = path.join(dbPath, "custom-store")
	  const db = await client.open(new EventStore(), { replicationTopic, directory })
	  await db.close()
	  await db.load()
	  await db.close()
	  expect(db.store._cache._store.status).toEqual('closed')
	})
	*/
		/* 
			TODO feat:
		it("successfully manages multiple caches", async () => {
			// Cleaning up cruft from other tests
			const directory = path.join(dbPath, "custom-store");
			const directory2 = path.join(dbPath, "custom-store2");
		
			const db1 = await client.open(new EventStore());
			const db2 = await client.open(new EventStore(), {
				directory,
			});
			const db3 = await client.open(new EventStore(), {
				directory,
			});
			const db4 = await client.open(new EventStore(), {
				directory: directory2,
			});
			const db5 = await client.open(new EventStore());
		
			expect(db1.log.headsIndex.headsCache?.cache.status).toEqual("open");
			expect(db2.log.headsIndex.headsCache?.cache.status).toEqual("open");
			expect(db3.log.headsIndex.headsCache?.cache.status).toEqual("open");
			expect(db4.log.headsIndex.headsCache?.cache.status).toEqual("open");
		
			await db1.close();
			await db2.close();
			await db4.close();
		
			expect(client.cache._store.status).toEqual("open");
			expect(db2.log.headsIndex.headsCache?.cache.status).toEqual("closed");
			expect(db3.log.headsIndex.headsCache?.cache.status).toEqual("open");
			expect(db4.log.headsIndex.headsCache?.cache.status).toEqual("closed");
		
			await db3.close();
			await db5.close();
		
			expect(client.cache.status).toEqual("open"); // TODO should this be open or closed now? Assume open, no-op is prefered if not certain
			expect(db2.log.headsIndex.headsCache?.cache.status).toEqual("closed");
			expect(db3.log.headsIndex.headsCache?.cache.status).toEqual("closed");
			expect(db4.log.headsIndex.headsCache?.cache.status).toEqual("closed");
			expect(db5.log.headsIndex.headsCache?.cache.status).toEqual("closed");
		}); */
	});
});
