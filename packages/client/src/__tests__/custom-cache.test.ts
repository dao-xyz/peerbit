import assert from "assert";
import rmrf from "rimraf";
import path from "path";
import { Peerbit } from "../peer";
import { createStore } from "./storage.js";
import CustomCache from "@dao-xyz/peerbit-cache";
import { jest } from "@jest/globals";
import { databases } from "./utils";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { DEFAULT_BLOCK_TRANSPORT_TOPIC } from "@dao-xyz/peerbit-block";

const dbPath = "./orbitdb/tests/customKeystore";

describe(`Use a Custom Cache`, function () {
    jest.setTimeout(20000);

    let session: LSession, orbitdb1: Peerbit, store;

    beforeAll(async () => {
        session = await LSession.connected(1, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
        store = await createStore("local");
        const cache = new CustomCache(store);

        rmrf.sync(dbPath);

        orbitdb1 = await Peerbit.create(session.peers[0], {
            directory: path.join(dbPath, "1"),
            cache: cache,
        });
    });

    afterAll(async () => {
        await orbitdb1.stop();
        await session.stop();
    });

    describe("allows orbit to use a custom cache with different store types", function () {
        for (let database of databases) {
            it(database.type + " allows custom cache", async () => {
                const db1 = await database.create(orbitdb1, "custom-keystore");
                await database.tryInsert(db1);

                assert.deepEqual(
                    database.getTestValue(db1),
                    database.expectedValue
                );
                await db1.store.close();
            });
        }
    });
});
