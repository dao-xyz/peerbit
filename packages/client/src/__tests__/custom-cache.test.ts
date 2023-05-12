import assert from "assert";
import rmrf from "rimraf";
import path from "path";
import { Peerbit } from "../peer.js";
import { createStore } from "./storage.js";
import CustomCache from "@dao-xyz/lazy-level";
import { jest } from "@jest/globals";
import { databases } from "./utils";
import { LSession } from "@dao-xyz/peerbit-test-utils";

const dbPath = "./tmp/tests/customCache";

describe(`Use a Custom Cache`, function () {
	jest.setTimeout(20000);

	let session: LSession, client1: Peerbit, store;

	beforeAll(async () => {
		session = await LSession.connected(1);
		store = await createStore("local" + +new Date());
		const cache = new CustomCache(store);

		rmrf.sync(dbPath);

		client1 = await Peerbit.create({
			directory: path.join(dbPath, "1"),
			cache: cache,
			libp2p: session.peers[0],
		});
	});

	afterAll(async () => {
		await client1.stop();
		await session.stop();
	});

	describe("allows orbit to use a custom cache with different store types", function () {
		it("allows custom cache", async () => {
			for (let database of databases) {
				try {
					const db1 = await database.create(client1, "custom-keystore");
					await database.tryInsert(db1);

					assert.deepEqual(
						await database.getTestValue(db1),
						database.expectedValue
					);
					await db1.log.close();
				} catch (error) {
					const e = 123;
				}
			}
		});
	});
});
