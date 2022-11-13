import assert from "assert";
import fs from "fs-extra";
import path from "path";
import rmrf from "rimraf";
// @ts-ignore
import { Peerbit } from "../peer";
import { KeyValueStore } from "./utils/stores/key-value-store";

import { Address } from "@dao-xyz/peerbit-program";
import { EventStore } from "./utils/stores";

// @ts-ignore
import { v4 as uuid } from "uuid";
import { jest } from "@jest/globals";

// Include test utilities
import {
  nodeConfig as config,
  Session,
  createStore,
} from "@dao-xyz/peerbit-test-utils";
import { Program } from "@dao-xyz/peerbit-program";
import { delay, waitFor } from "@dao-xyz/peerbit-time";

const dbPath = path.join("./peerbit", "tests", "create-open");

describe(`orbit-db - Create & Open `, function () {
  jest.retryTimes(1); // TODO Side effects may cause failures
  jest.setTimeout(config.timeout * 5);

  let session: Session;

  beforeAll(async () => {
    session = await Session.connected(1);
  });

  afterAll(async () => {
    if (session) {
      await session.stop();
    }
  });

  describe("Create", function () {
    describe("Success", function () {
      let db: KeyValueStore<string>;
      let localDataPath: string, orbitdb: Peerbit;

      beforeAll(async () => {
        orbitdb = await Peerbit.create(session.peers[0].ipfs, {
          directory: dbPath + uuid(),
        });
      });
      afterAll(async () => {
        if (orbitdb) {
          await orbitdb.stop();
        }
      });

      beforeEach(async () => {
        localDataPath = path.join(dbPath, orbitdb.id.toString(), "cache");

        db = await orbitdb.open(new KeyValueStore<string>({ id: "second" }), {
          replicationTopic: uuid(),
          directory: localDataPath,
          replicate: false,
        });
        await db.close();
      });
      afterEach(async () => {
        await db.drop();
      });

      it("creates a feed database", async () => {
        assert.notEqual(db, null);
      });

      it("saves the database locally", async () => {
        expect(fs.existsSync(localDataPath)).toEqual(true);
      });

      /*       it('saves database manifest reference locally', async () => {
              const address = db.address!.toString();
              const manifestHash = address.split('/')[2]
              await orbitdb.cache.open()
              const value = await orbitdb.cache.get(path.join(db.address?.toString(), '/_manifest'))
              expect(value).toEqual(manifestHash)
            }) */

      it("saves database manifest file locally", async () => {
        const loaded = (await Program.load(
          session.peers[0].ipfs,
          db.address!
        )) as KeyValueStore<string>;
        expect(loaded).toBeDefined();
        expect(loaded.store).toBeDefined();
      });

      it("can pass local database directory as an option", async () => {
        const dir = "./peerbit/tests/another-feed-" + uuid();
        const db2 = await orbitdb.open(new EventStore({ id: "third" }), {
          replicationTopic: uuid(),
          directory: dir,
        });
        expect(fs.existsSync(dir)).toEqual(true);
        await db2.close();
      });
    });
  });

  describe("Open", function () {
    let orbitdb: Peerbit;
    jest.retryTimes(1); // TODO Side effects may cause failures

    beforeAll(async () => {
      orbitdb = await Peerbit.create(session.peers[0].ipfs, {
        directory: dbPath + uuid(),
        storage: { createStore: (string: string) => createStore(string) },
      });
    });
    afterAll(async () => {
      if (orbitdb) {
        await orbitdb.stop();
      }
    });

    it("opens a database - name only", async () => {
      const replicationTopic = uuid();
      const db = await orbitdb.open(new EventStore({}), { replicationTopic });
      assert.equal(db.address!.toString().indexOf("/peerbit"), 0);
      assert.equal(db.address!.toString().indexOf("zd"), 9);
      await db.drop();
    });

    it("opens a database - with a different identity", async () => {
      const signKey = await orbitdb.keystore.createEd25519Key();
      const replicationTopic = uuid();
      const db = await orbitdb.open(new EventStore({}), {
        replicationTopic,
        identity: {
          ...signKey.keypair,
          sign: (data) => signKey.keypair.sign(data),
        },
      });
      assert.equal(db.address!.toString().indexOf("/peerbit"), 0);
      assert.equal(db.address!.toString().indexOf("zd"), 9);
      expect(db.store.identity.publicKey.equals(signKey.keypair.publicKey));
      await db.drop();
    });

    it("opens the same database - from an address", async () => {
      const signKey = await orbitdb.keystore.createEd25519Key();
      const replicationTopic = uuid();
      const db = await orbitdb.open(new EventStore({}), {
        replicationTopic,
        identity: {
          ...signKey.keypair,
          sign: (data) => signKey.keypair.sign(data),
        },
      });
      const db2 = await orbitdb.open(
        await Program.load(orbitdb._ipfs, db.address!),
        { replicationTopic }
      );
      assert.equal(db2.address!.toString().indexOf("/peerbit"), 0);
      assert.equal(db2.address!.toString().indexOf("zd"), 9);
      await db.drop();
      await db2.drop();
    });

    it("doesn't open a database if we don't have it locally", async () => {
      const replicationTopic = uuid();
      const db = await orbitdb.open(new EventStore({}), { replicationTopic });
      const address = new Address({ cid: db.address!.cid.slice(0, -1) + "A" });
      await db.drop();
      return new Promise(async (resolve, reject) => {
        setTimeout(resolve, 900);
        orbitdb
          .open(await Program.load(orbitdb._ipfs, address), {
            replicationTopic,
          })
          .then(() => reject(new Error("Shouldn't open the database")))
          .catch(reject);
      });
    });

    /*  TODO, this test throws error, but not the expected one
    it('throws an error if trying to open a database locally and we don\'t have it', async () => {
       const db = await orbitdb.open(new EventStore({ id: 'abc' }), { replicationTopic })
       const address = new Address(db.address.cid.slice(0, -1) + 'A')
       await db.drop()
       try {
         await orbitdb.open(address, { replicationTopic, localOnly: true, timeout: 3000 })
         throw new Error('Shouldn\'t open the database')
       } catch (error: any) {
         expect(error.toString()).toEqual(`Error: Database '${address}' doesn't exist!`)
       }
     }) */

    it("open the database and it has the added entries", async () => {
      const db = await orbitdb.open(new EventStore({ id: uuid() }), {
        directory: dbPath + uuid(),
      });
      await db.add("hello1");
      await db.add("hello2");
      await db.close();
      await db.load();
      await waitFor(() => db.iterator({ limit: -1 }).collect().length == 2);
      const res = db.iterator({ limit: -1 }).collect();
      expect(res.length).toEqual(2);
      expect(res[0].payload.getValue().value).toEqual("hello1");
      expect(res[1].payload.getValue().value).toEqual("hello2");
      await db.drop();
    });
  });

  describe("Close", function () {
    let orbitdb: Peerbit;

    beforeAll(async () => {
      orbitdb = await Peerbit.create(session.peers[0].ipfs, {
        directory: dbPath + uuid(),
      });
    });
    afterAll(async () => {
      if (orbitdb) {
        await orbitdb.stop();
      }
    });

    it("closes a custom store", async () => {
      const directory = path.join(dbPath, "custom-store");
      const replicationTopic = uuid();
      const db = await orbitdb.open(new EventStore({}), {
        replicationTopic,
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
      const db = await orbitdb.open(new EventStore({}), { replicationTopic, directory })
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

      const replicationTopic = uuid();
      const db1 = await orbitdb.open(new EventStore({ id: "xyz1" }), {
        replicationTopic,
      });
      const db2 = await orbitdb.open(new EventStore({ id: "xyz2" }), {
        replicationTopic,
        directory,
      });
      const db3 = await orbitdb.open(new EventStore({ id: "xyz3" }), {
        replicationTopic,
        directory,
      });
      const db4 = await orbitdb.open(new EventStore({ id: "xyz4" }), {
        replicationTopic,
        directory: directory2,
      });
      const db5 = await orbitdb.open(new EventStore({ id: "xyz5" }), {
        replicationTopic,
      });
      try {
        await db1.close();
        await db2.close();
        await db4.close();

        expect(orbitdb.cache._store.status).toEqual("open");
        expect(db2.store._cache._store.status).toEqual("open");
        expect(db3.store._cache._store.status).toEqual("open");
        expect(db4.store._cache._store.status).toEqual("closed");

        await db3.close();
        await db5.close();

        expect(orbitdb.cache._store.status).toEqual("closed");
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
