import { createStore as createStoreFn } from "../store.js";
import path from "path";
import { v4 as uuid } from "uuid";
import { AnyStore } from "../interface.js";
import { fileURLToPath } from "url";
import { dirname } from "path";

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
			const data = [{ key: "boolean", value: true }];
			let store: AnyStore;
			beforeAll(async () => {
				store = await createStore();
				store.open();
			});

			afterAll(async () => {
				await store.close();
			});

			it(`set, get, delete`, async () => {
				for (const d of data) {
					await store.put(d.key, Buffer.from(JSON.stringify(d.value)));
					const val = await store.get(d.key);
					expect(new Uint8Array(val!)).toEqual(
						new Uint8Array(Buffer.from(JSON.stringify(d.value)))
					);

					expect(await store.get("fooKey")).toBeUndefined();

					await store.put(d.key, Buffer.from(JSON.stringify(d.value)));
					await store.del(d.key);

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
