import { EntryIO } from "../entry-io.js";
import { Log } from "../log.js";
import assert from "assert";
import rmrf from "rimraf";
import fs from "fs-extra";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";

// Test utils
import {
  nodeConfig as config,
  testAPIs,
  startIpfs,
  stopIpfs,
} from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { IPFS } from "ipfs-core-types";
import { Controller } from "ipfsd-ctl";
import { jest } from "@jest/globals";
import { dirname } from "path";
import { fileURLToPath } from "url";
import path from "path";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

let ipfsd: Controller,
  ipfs: IPFS,
  signKey: KeyWithMeta<Ed25519Keypair>,
  signKey2: KeyWithMeta<Ed25519Keypair>,
  signKey3: KeyWithMeta<Ed25519Keypair>,
  signKey4: KeyWithMeta<Ed25519Keypair>;

const last = (arr: any[]) => arr[arr.length - 1];

Object.keys(testAPIs).forEach((IPFS) => {
  describe("Entry - Persistency", function () {
    jest.setTimeout(config.timeout);

    const { signingKeyFixtures, signingKeysPath } = config;

    let options, keystore: Keystore;

    beforeAll(async () => {
      rmrf.sync(signingKeysPath(__filenameBase));
      await fs.copy(
        signingKeyFixtures(__dirname),
        signingKeysPath(__filenameBase)
      );
      const defaultOptions = { signingKeysPath };

      keystore = new Keystore(
        await createStore(signingKeysPath(__filenameBase))
      );

      const users = ["userA", "userB", "userC", "userD"];
      options = users.map((user) => {
        return Object.assign({}, defaultOptions, { id: user, keystore });
      });
      await keystore.waitForOpen();
      signKey = (await keystore.getKey(
        new Uint8Array([0])
      )) as KeyWithMeta<Ed25519Keypair>;
      signKey2 = (await keystore.getKey(
        new Uint8Array([1])
      )) as KeyWithMeta<Ed25519Keypair>;
      signKey3 = (await keystore.getKey(
        new Uint8Array([2])
      )) as KeyWithMeta<Ed25519Keypair>;
      signKey4 = (await keystore.getKey(
        new Uint8Array([3])
      )) as KeyWithMeta<Ed25519Keypair>;
      ipfsd = await startIpfs(IPFS, config.defaultIpfsConfig);
      ipfs = ipfsd.api;
    });

    afterAll(async () => {
      await stopIpfs(ipfsd);
      rmrf.sync(signingKeysPath(__filenameBase));
      await keystore?.close();
    });

    it("log with one entry", async () => {
      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      await log.append("one");
      const hash = log.values[0].hash;
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 1 });
      expect(res.length).toEqual(1);
    });

    it("log with 2 entries", async () => {
      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      await log.append("one");
      await log.append("two");
      const hash = last(log.values).hash;
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 2 });
      expect(res.length).toEqual(2);
    });

    it("loads max 1 entry from a log of 2 entry", async () => {
      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      await log.append("one");
      await log.append("two");
      const hash = last(log.values).hash;
      const res = await EntryIO.fetchAll(ipfs, hash, { length: 1 });
      expect(res.length).toEqual(1);
    });

    it("log with 100 entries", async () => {
      const count = 100;
      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      for (let i = 0; i < count; i++) {
        await log.append("hello" + i);
      }
      const hash = await log.toMultihash();
      const result = await Log.fromMultihash(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        hash,
        {}
      );
      expect(result.length).toEqual(count);
    });

    it("load only 42 entries from a log with 100 entries", async () => {
      const count = 100;
      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      let log2 = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      for (let i = 1; i <= count; i++) {
        await log.append("hello" + i);
        if (i % 10 === 0) {
          log2 = new Log(
            ipfs,
            {
              ...signKey.keypair,
              sign: async (data: Uint8Array) =>
                await signKey.keypair.sign(data),
            },
            {
              logId: log2._id,
              entries: log2.values,
              heads: log2.heads.concat(log.heads),
            }
          );
          await log2.append("hi" + i);
        }
      }

      const hash = await log.toMultihash();
      const result = await Log.fromMultihash(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        hash,
        { length: 42 }
      );
      expect(result.length).toEqual(42);
    });

    it("load only 99 entries from a log with 100 entries", async () => {
      const count = 100;
      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      let log2 = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      for (let i = 1; i <= count; i++) {
        await log.append("hello" + i);
        if (i % 10 === 0) {
          log2 = new Log(
            ipfs,
            {
              ...signKey.keypair,
              sign: async (data: Uint8Array) =>
                await signKey.keypair.sign(data),
            },
            { logId: log2._id, entries: log2.values }
          );
          await log2.append("hi" + i);
          await log2.join(log);
        }
      }

      const hash = await log2.toMultihash();
      const result = await Log.fromMultihash(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        hash,
        { length: 99 }
      );
      expect(result.length).toEqual(99);
    });

    it("load only 10 entries from a log with 100 entries", async () => {
      const count = 100;
      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      let log2 = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      let log3 = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      for (let i = 1; i <= count; i++) {
        await log.append("hello" + i);
        if (i % 10 === 0) {
          log2 = new Log(
            ipfs,
            {
              ...signKey.keypair,
              sign: async (data: Uint8Array) =>
                await signKey.keypair.sign(data),
            },
            { logId: log2._id, entries: log2.values, heads: log2.heads }
          );
          await log2.append("hi" + i);
          await log2.join(log);
        }
        if (i % 25 === 0) {
          log3 = new Log(
            ipfs,
            {
              ...signKey.keypair,
              sign: async (data: Uint8Array) =>
                await signKey.keypair.sign(data),
            },
            {
              logId: log3._id,
              entries: log3.values,
              heads: log3.heads.concat(log2.heads),
            }
          );
          await log3.append("--" + i);
        }
      }

      await log3.join(log2);
      const hash = await log3.toMultihash();
      const result = await Log.fromMultihash(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        hash,
        { length: 10 }
      );
      expect(result.length).toEqual(10);
    });

    it("load only 10 entries and then expand to max from a log with 100 entries", async () => {
      const count = 30;

      const log = new Log(
        ipfs,
        {
          ...signKey.keypair,
          sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
        },
        { logId: "X" }
      );
      const log2 = new Log(
        ipfs,
        {
          ...signKey2.keypair,
          sign: async (data: Uint8Array) => await signKey2.keypair.sign(data),
        },
        { logId: "X" }
      );
      let log3 = new Log(
        ipfs,
        {
          ...signKey3.keypair,
          sign: async (data: Uint8Array) => await signKey3.keypair.sign(data),
        },
        { logId: "X" }
      );
      for (let i = 1; i <= count; i++) {
        await log.append("hello" + i);
        if (i % 10 === 0) {
          await log2.append("hi" + i);
          await log2.join(log);
        }
        if (i % 25 === 0) {
          log3 = new Log(
            ipfs,
            {
              ...signKey3.keypair,
              sign: async (data: Uint8Array) =>
                await signKey3.keypair.sign(data),
            },
            {
              logId: log3._id,
              entries: log3.values,
              heads: log3.heads.concat(log2.heads),
            }
          );
          await log3.append("--" + i);
        }
      }

      await log3.join(log2);

      const log4 = new Log(
        ipfs,
        {
          ...signKey4.keypair,
          sign: async (data: Uint8Array) => await signKey4.keypair.sign(data),
        },
        { logId: "X" }
      );
      await log4.join(log2);
      await log4.join(log3);

      const values3 = log3.values.map((e) => e.payload.getValue());
      const values4 = log4.values.map((e) => e.payload.getValue());

      assert.deepStrictEqual(values3, values4);
    });
  });
});
