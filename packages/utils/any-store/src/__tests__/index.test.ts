import { createStore as createStoreFn } from "../store.js";
import path from "path";
import { v4 as uuid } from "uuid";
import { AnyStore } from "../interface.js";
import { fileURLToPath } from "url";
import { dirname } from "path";
import { delay } from "@peerbit/time";
import { randomBytes } from "@peerbit/crypto";

const __dirname = dirname(fileURLToPath(import.meta.url));

describe(`index`, function () {
	const testType = ["disc", "memory"];
	testType.map((type) => {
		const createStore = () => {
			return createStoreFn(
				type === "disc"
					? path.join(__dirname, "tmp", "anystore", "index", uuid())
					: undefined
			);
		};

		describe(type, () => {
			const data = [{ key: "boolean", value: new Uint8Array(1e3) }];
			let store: AnyStore;
			beforeAll(async () => {
				store = await createStore();
				await store.open();
			});

			afterAll(async () => {
				await store.close();
			});

			it(`set, get, delete, size`, async () => {
				for (const d of data) {
					const value = Buffer.from(JSON.stringify(d.value));
					await store.put(d.key, value);

					await store.close(); // force writes/size updates to take place
					await store.open();

					expect(await store.size()).toBeGreaterThan(0);

					const retrievedValue = await store.get(d.key);
					expect(new Uint8Array(retrievedValue!)).toEqual(
						new Uint8Array(value)
					);

					expect(await store.get("fooKey")).toBeUndefined();
					await store.del(d.key);

					await store.close(); // force writes/size updates to take place
					await store.open();

					expect(await store.size()).toEqual(0);

					expect(await store.get(d.key)).toBeUndefined();
					expect(await store.del("fooKey")).toBeUndefined();
				}
			});

			describe("sublevel", () => {
				it("create from sublevel", async () => {
					expect(store.status()).toEqual("open");
					const sublevel = await store.sublevel("sublevel");
					await sublevel.close();
					expect(sublevel.status()).toEqual("closed");
					expect(store.status()).toEqual("open");
				});

				it("clear sublevel", async () => {
					await store.put("a", new Uint8Array([1]));
					const sublevel = await store.sublevel("sublevel");
					await sublevel.put("a", new Uint8Array([2]));
					expect(new Uint8Array((await store.get("a"))!)).toEqual(
						new Uint8Array([1])
					);
					expect(new Uint8Array((await sublevel.get("a"))!)).toEqual(
						new Uint8Array([2])
					);
					expect(new Uint8Array((await store.get("a"))!)).toEqual(
						new Uint8Array([1])
					);
					await store.clear();
					expect(await sublevel.get("a")).toBeUndefined();
				});
			});
		});
	});
});
