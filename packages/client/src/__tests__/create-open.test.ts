import assert from "assert";
import fs from "fs-extra";
import path from "path";
import { Peerbit } from "../peer.js";
import { KeyBlocks } from "./utils/stores/key-value-store";

import { EventStore } from "./utils/stores";

// @ts-ignore
import { v4 as uuid } from "uuid";

// Include test utilities
import { Observer, Program } from "@dao-xyz/peerbit-program";
import { waitForAsync } from "@dao-xyz/peerbit-time";
import { LevelBlockStore } from "@dao-xyz/libp2p-direct-block";
import { createEd25519PeerId } from "@libp2p/peer-id-factory";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";

const dbPath = path.join("./peerbit", "tests", "create-open");

describe(`Create & Open`, function () {
	describe("Create", function () {
		describe("with db", function () {
			let db: KeyBlocks<string>;
			let client: Peerbit;
			let clientDirectory: string;
			beforeAll(async () => {
				clientDirectory = dbPath + uuid();
				client = await Peerbit.create({
					directory: clientDirectory,
				});
			});
			afterAll(async () => {
				await client.stop();
			});

			beforeEach(async () => {
				db = await client.open(new KeyBlocks<string>(), {
					role: new Observer(),
				});
			});
			afterEach(async () => {
				await db.drop();
			});
			it("directory exist", async () => {
				expect(client.directory).toEqual(clientDirectory);
			});

			it("creates a feed database", async () => {
				expect(db).toBeDefined();
			});

			it("block storage exist at path", async () => {
				const location = (
					client.libp2p.services.blocks._localStore as LevelBlockStore
				)._level._store["location"];
				expect(location).toEndWith(
					path.join(client.directory!, "blocks").toString()
				);
			});

			it("saves the database locally", async () => {
				expect(fs.existsSync(clientDirectory)).toEqual(true);
			});

			it("saves database manifest file locally", async () => {
				const loaded = (await Program.load(
					client.libp2p.services.blocks,
					db.address!
				)) as KeyBlocks<string>;
				expect(loaded).toBeDefined();
				expect(loaded.log).toBeDefined();
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

		it("can create with peerId", async () => {
			const peerId = await createEd25519PeerId();
			const client = await Peerbit.create({
				libp2p: { peerId },
			});
			expect(client.libp2p.peerId.equals(peerId)).toBeTrue();
			await client.stop();
		});
	});

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
				(await Program.load(client.libp2p.services.blocks, db.address!))!
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
				client.libp2p.services.blocks._localStore as LevelBlockStore
			).idle();
			const dbToLoad = await Program.load(
				client.libp2p.services.blocks,
				db.address,
				{ timeout: 3000 }
			);
			expect(dbToLoad).toBeUndefined();
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

		it("open the database and it has the added entries", async () => {
			const db = await client.open(new EventStore());
			await db.add("hello1");
			await db.add("hello2");
			await db.close();
			await db.load();
			await waitForAsync(
				async () => (await db.iterator({ limit: -1 })).collect().length == 2
			);
			const res = (await db.iterator({ limit: -1 })).collect();
			expect(res.length).toEqual(2);
			expect(res[0].payload.getValue().value).toEqual("hello1");
			expect(res[1].payload.getValue().value).toEqual("hello2");
			await db.drop();
		});

		it("opens and resets", async () => {
			const path = dbPath + uuid();
			let db = await client.open(new EventStore());
			await db.add("hello1");
			await db.add("hello2");
			await db.close();
			await db.load();
			expect((await db.iterator({ limit: -1 })).collect().length).toEqual(2);
			await db.close();
			db = await client.open(new EventStore(), {
				reset: true,
			});
			await db.load();
			expect((await db.iterator({ limit: -1 })).collect().length).toEqual(0);
			await db.drop();
		});
	});

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

		it("closes when disconnecting", async () => {
			const db = await client.open(new EventStore());
			await client.stop();
			expect(db.log.headsIndex.headsCache?.cache?._store.status).toEqual(
				"closed"
			);
		});

		it("closes a custom store", async () => {
			const db = await client.open(new EventStore());
			await db.close();
			expect(db.log.headsIndex.headsCache?.cache?._store.status).toEqual(
				"closed"
			);
		});

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
