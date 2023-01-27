import assert from "assert";
import fs from "fs-extra";
import path from "path";
// @ts-ignore
import { Peerbit } from "../peer";
import { KeyBlocks } from "./utils/stores/key-value-store";

import { Address } from "@dao-xyz/peerbit-program";
import { EventStore } from "./utils/stores";

// @ts-ignore
import { v4 as uuid } from "uuid";
import { jest } from "@jest/globals";

// Include test utilities
import { LSession } from "@dao-xyz/libp2p-test-utils";
import { Program } from "@dao-xyz/peerbit-program";
import { waitFor } from "@dao-xyz/peerbit-time";
import { LevelBlockStore } from "@dao-xyz/libp2p-direct-block";

const dbPath = path.join("./peerbit", "tests", "create-open");

describe(`Create & Open`, function () {
	//   jest.retryTimes(1); // TODO Side effects may cause failures

	let session: LSession;

	beforeAll(async () => {
		session = await LSession.connected(1);
	});

	afterAll(async () => {
		if (session) {
			await session.stop();
		}
	});

	describe("Create", function () {
		describe("Success", function () {
			let db: KeyBlocks<string>;
			let localDataPath: string, client: Peerbit;

			beforeAll(async () => {
				client = await Peerbit.create({
					directory: dbPath + uuid(),
					libp2p: session.peers[0]
				});
			});
			afterAll(async () => {
				if (client) {
					await client.stop();
				}
			});

			beforeEach(async () => {
				localDataPath = path.join(
					dbPath,
					client.id.toString(),
					"cache"
				);

				db = await client.open(
					new KeyBlocks<string>({ id: "second" }),
					{
						directory: localDataPath,
						replicate: false,
					}
				);
				await db.close();
			});
			afterEach(async () => {
				await db.drop();
			});

			it("creates a feed database", async () => {
				assert.notEqual(db, null);
			});

			it("block storage exist at path", async () => {

				const location = (client.libp2p.directblock._localStore as LevelBlockStore)._level["location"]
				expect(location).toEndWith(
					path.join(client.directory!, "blocks").toString()
				);
			});

			it("saves the database locally", async () => {
				expect(fs.existsSync(localDataPath)).toEqual(true);
			});

			/*       it('saves database manifest reference locally', async () => {
			  const address = db.address!.toString();
			  const manifestHash = address.split('/')[2]
			  await client.cache.open()
			  const value = await client.cache.get(path.join(db.address?.toString(), '/_manifest'))
			  expect(value).toEqual(manifestHash)
			}) */

			it("saves database manifest file locally", async () => {
				const loaded = (await Program.load(
					client.libp2p.directblock,
					db.address!
				)) as KeyBlocks<string>;
				expect(loaded).toBeDefined();
				expect(loaded.store).toBeDefined();
			});

			it("can pass local database directory as an option", async () => {
				const dir = "./peerbit/tests/another-feed-" + uuid();
				const db2 = await client.open(new EventStore({ id: "third" }), {
					directory: dir,
				});
				expect(fs.existsSync(dir)).toEqual(true);
				await db2.close();
			});
		});
	});

	describe("Open", function () {
		let client: Peerbit;
		jest.retryTimes(1); // TODO Side effects may cause failures

		beforeAll(async () => {
			client = await Peerbit.create({
				directory: dbPath + uuid(),
				libp2p: session.peers[0]
			});
		});
		afterAll(async () => {
			if (client) {
				await client.stop();
			}
		});

		it("opens a database - name only", async () => {
			const topic = uuid();
			const db = await client.open(new EventStore({}));
			assert.equal(db.address!.toString().indexOf("/peerbit"), 0);
			assert.equal(db.address!.toString().indexOf("zd"), 9);
			await db.drop();
		});

		it("opens a database - with a different identity", async () => {
			const signKey = await client.keystore.createEd25519Key();
			const topic = uuid();
			const db = await client.open(new EventStore({}), {
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
			});
			assert.equal(db.address!.toString().indexOf("/peerbit"), 0);
			assert.equal(db.address!.toString().indexOf("zd"), 9);
			expect(
				db.store.identity.publicKey.equals(signKey.keypair.publicKey)
			);
			await db.drop();
		});

		it("opens the same database - from an address", async () => {
			const signKey = await client.keystore.createEd25519Key();
			const topic = uuid();
			const db = await client.open(new EventStore({}), {
				identity: {
					...signKey.keypair,
					sign: (data) => signKey.keypair.sign(data),
				},
			});
			const db2 = await client.open(
				await Program.load(client.libp2p.directblock, db.address!)
			);
			assert.equal(db2.address!.toString().indexOf("/peerbit"), 0);
			assert.equal(db2.address!.toString().indexOf("zd"), 9);
			await db.drop();
			await db2.drop();
		});

		it("doesn't open a database if we don't have it locally", async () => {
			const topic = uuid();
			const db = await client.open(new EventStore({}));
			const address = new Address({
				cid: db.address!.cid.slice(0, -1) + "A",
			});
			await db.drop();
			const dbToLoad = await Program.load(client.libp2p.directblock, address);
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
			const db = await client.open(new EventStore({ id: uuid() }), {
				directory: dbPath + uuid(),
			});
			await db.add("hello1");
			await db.add("hello2");
			await db.close();
			await db.load();
			await waitFor(
				() => db.iterator({ limit: -1 }).collect().length == 2
			);
			const res = db.iterator({ limit: -1 }).collect();
			expect(res.length).toEqual(2);
			expect(res[0].payload.getValue().value).toEqual("hello1");
			expect(res[1].payload.getValue().value).toEqual("hello2");
			await db.drop();
		});
	});

	describe("Close", function () {
		let client: Peerbit;

		beforeAll(async () => {
			client = await Peerbit.create({
				directory: dbPath + uuid(),
				libp2p: session.peers[0],
			});
		});
		afterAll(async () => {
			if (client) {
				await client.stop();
			}
		});

		it("closes a custom store", async () => {
			const directory = path.join(dbPath, "custom-store");
			const replicationTopic = uuid();
			const db = await client.open(new EventStore({}), {
				directory,
			});
			try {
				await db.close();
				expect(db.store._cache._store.status).toEqual("closed");
				const x = 123;
			} catch (error) {
				const x = 123;
			}
		});

		/* TODO fix
    
	it("close load close sets status to 'closed'", async () => {
	  const directory = path.join(dbPath, "custom-store")
	  const db = await client.open(new EventStore({}), { replicationTopic, directory })
	  await db.close()
	  await db.load()
	  await db.close()
	  expect(db.store._cache._store.status).toEqual('closed')
	})
 */
		it("successfully manages multiple caches", async () => {
			// Cleaning up cruft from other tests
			const directory = path.join(dbPath, "custom-store");
			const directory2 = path.join(dbPath, "custom-store2");

			const topic = uuid();
			const db1 = await client.open(new EventStore({ id: "xyz1" }));
			const db2 = await client.open(new EventStore({ id: "xyz2" }), {
				directory,
			});
			const db3 = await client.open(new EventStore({ id: "xyz3" }), {
				directory,
			});
			const db4 = await client.open(new EventStore({ id: "xyz4" }), {
				directory: directory2,
			});
			const db5 = await client.open(new EventStore({ id: "xyz5" }));
			try {
				await db1.close();
				await db2.close();
				await db4.close();

				expect(client.cache._store.status).toEqual("open");
				expect(db2.store._cache._store.status).toEqual("open");
				expect(db3.store._cache._store.status).toEqual("open");
				expect(db4.store._cache._store.status).toEqual("closed");

				await db3.close();
				await db5.close();

				expect(client.cache._store.status).toEqual("closed");
				expect(db2.store._cache._store.status).toEqual("closed");
				expect(db3.store._cache._store.status).toEqual("closed");
				expect(db4.store._cache._store.status).toEqual("closed");
				expect(db5.store._cache._store.status).toEqual("closed");
			} catch (error) {
				const x = 123;
			}
		});
	});
});
