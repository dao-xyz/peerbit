import { type AnyStore } from "@peerbit/any-store-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import path, { dirname } from "path";
import { fileURLToPath } from "url";
import { v4 as uuid } from "uuid";
import { createStore as createStoreFn } from "../src/store.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;
// eslint-disable-next-line @typescript-eslint/naming-convention
const __dirname = isNode ? dirname(fileURLToPath(import.meta.url)) : "/tmp";

(globalThis as any)["__playwright_test__"] = isNode;

describe(`index`, function () {
	const testType = ["disc", "memory"];
	testType.map((type) => {
		const createStore = (id = uuid()) => {
			return createStoreFn(
				type === "disc"
					? path.join(__dirname, "tmp", "anystore", "index", id)
					: undefined,
			);
		};

		describe(type, () => {
			const data = { key: "some-value", value: new Uint8Array([123]) };

			let store: AnyStore;
			let id: string;
			beforeEach(async () => {
				id = uuid();
				store = await createStore(id);
				await store.open();

				await store.put(data.key, data.value);

				await store.close(); // force writes/size updates to take place (only needed for classic level). TODO make the code work without this
				await store.open();
			});

			afterEach(async () => {
				await store.close();
			});

			/* it(`set, get, delete, size`, async () => {

				await store.close(); // force writes/size updates to take place
				await store.open();

				expect(await store.size()).greaterThan(0);

				const retrievedValue = await store.get(data.key);
				expect(new Uint8Array(retrievedValue!)).equal(
					new Uint8Array(value)
				);

				expect(await store.get("fooKey")).equal(undefined);
				await store.del(d.key);

				await store.close(); // force writes/size updates to take place
				await store.open();

				expect(await store.size()).equal(0);

				expect(await store.get(data.key)).equal(undefined);
				expect(await store.del("fooKey")).equal(undefined);
			});
 */

			it("persisted", () => {
				expect(store.persisted()).equal(type === "disc");
			});

			it("exposes crash-safe barriers only for LevelStore and propagates them to sublevels", async () => {
				const sublevel = await store.sublevel("durability-capability");

				if (store.constructor.name === "LevelStore") {
					expect(store.crashSafeDurability?.crashSafe).to.equal(true);
					expect(sublevel.crashSafeDurability?.crashSafe).to.equal(true);
					expect(store.crashSafeDurability?.atomicReplace).to.be.a("function");
					expect(sublevel.crashSafeDurability?.atomicReplace).to.be.a(
						"function",
					);

					await store.put("durability-root", new Uint8Array([1]));
					await sublevel.put("durability-sublevel", new Uint8Array([2]));
					await store.crashSafeDurability!.atomicReplace!(
						"durability-root",
						new Uint8Array([3]),
					);
					await sublevel.crashSafeDurability!.atomicReplace!(
						"durability-sublevel",
						new Uint8Array([4]),
					);
					await store.crashSafeDurability!.barrier();
					await sublevel.crashSafeDurability!.barrier();
					expect(await store.get("durability-root")).to.deep.equal(
						new Uint8Array([3]),
					);
					expect(await sublevel.get("durability-sublevel")).to.deep.equal(
						new Uint8Array([4]),
					);
				} else {
					expect(store.crashSafeDurability).to.equal(undefined);
					expect(sublevel.crashSafeDurability).to.equal(undefined);
				}
			});

			it("captures exact atomic replacement bytes on root and sublevels", async () => {
				const sublevel = await store.sublevel("atomic-replacement-capture");
				if (store.constructor.name !== "LevelStore") return;

				class HostileBytes extends Uint8Array {
					static get [Symbol.species]() {
						throw new Error("species must not run");
					}
					[Symbol.iterator](): ArrayIterator<number> {
						throw new Error("iterator must not run");
					}
				}

				const targets = [
					{ name: "root", store },
					{ name: "sublevel", store: sublevel },
				];
				for (const target of targets) {
					const input = new HostileBytes([1, 2, 3]);
					Object.defineProperty(input, "byteLength", {
						get: () => {
							throw new Error("shadowed byteLength must not run");
						},
					});
					const key = `atomic-replacement-${target.name}`;
					const replacing = target.store.crashSafeDurability!.atomicReplace!(
						key,
						input,
					);
					input.fill(9);
					await replacing;
					expect(await target.store.get(key)).to.deep.equal(
						new Uint8Array([1, 2, 3]),
					);

					const detached = new Uint8Array([1]);
					structuredClone(detached.buffer, { transfer: [detached.buffer] });
					const invalid = [
						new Proxy(new Uint8Array([1]), {}) as Uint8Array,
						new Uint16Array([1]) as unknown as Uint8Array,
						detached,
					];
					for (const [index, value] of invalid.entries()) {
						const invalidKey = `${key}-invalid-${index}`;
						let thrown: unknown;
						try {
							await target.store.crashSafeDurability!.atomicReplace!(
								invalidKey,
								value,
							);
						} catch (error) {
							thrown = error;
						}
						expect(thrown).to.be.instanceOf(TypeError);
						expect(await target.store.get(invalidKey)).to.equal(undefined);
					}
				}
			});

			it("get", async () => {
				const result = await store.get(data.key);
				expect(result).to.deep.equal(new Uint8Array([123]));
			});

			it("get missing", async () => {
				const result = await store.get("_missing_");
				expect(result).equal(undefined);
			});

			it("size", async () => {
				const result = await store.size();
				expect(result).to.be.greaterThanOrEqual(1); // classic level might calculate with true stored size

				await store.close();
				store = createStore(id);
				await store.open();
				if (type === "disc") {
					expect(await store.size()).to.equal(result);
				} else {
					expect(await store.size()).to.equal(0);
				}
			});

			it("size multiple levels", async () => {
				let levelA = "a";
				let storeA = await store.sublevel(levelA);
				await storeA.put("a", new Uint8Array([1]));
				await storeA.put("b", new Uint8Array([2]));
				let levelB = "b";
				let storeB = await store.sublevel(levelB);
				await storeB.put("a", new Uint8Array([4]));

				let rootSize = await store.size();
				expect(rootSize).to.be.greaterThan(0);

				const resultA = await storeA.size();
				expect(resultA).to.be.greaterThanOrEqual(2);
				const resultB = await storeB.size();
				expect(resultB).to.be.greaterThanOrEqual(1);

				await store.close();
				store = createStore(id);
				await store.open();

				storeA = await store.sublevel(levelA);
				storeB = await store.sublevel(levelB);

				if (type === "disc") {
					if (store.constructor.name === "LevelStore") {
						// we can not do instanceof becauser this will run in the browser and we can not import LevelStore because it depends on fs
						expect(await store.size()).to.greaterThanOrEqual(rootSize); // Classic level approximates size until restart
					} else {
						expect(await store.size()).to.equal(rootSize);
					}
					expect(await storeA.size()).to.equal(resultA);
					expect(await storeB.size()).to.equal(resultB);
				} else {
					expect(await store.size()).to.equal(0);
					expect(await storeA.size()).to.equal(0);
					expect(await storeB.size()).to.equal(0);
				}
			});

			it("del", async () => {
				expect(await store.size()).greaterThanOrEqual(1);
				await store.del(data.key);
				const result = await store.get(data.key);
				expect(result).equal(undefined);
				expect(await store.size()).equal(0);
			});

			/* TODO make this one work on FireFox (we got "Not modifications allowed" error) */

			/* it("del+put concurrently", async () => {
				// dont await put
				await handle.evaluate((store) => {
					store.put("cx", new Uint8Array([123]));
				});
				await handle.evaluate(async (store) => {
					await store.del("cx");
				});

				const result = await handle.evaluate((store) => store.get("cx"));
				expect(result).equal(undefined);
			}); */

			it("del missing", async () => {
				expect(await store.size()).greaterThanOrEqual(1);

				await store.del("_missing_");
				expect(await store.size()).greaterThanOrEqual(1);
			});

			it("replace-delete", async () => {
				await store.put(data.key, new Uint8Array([2, 3]));

				await store.close(); // force writes/size updates to take place (only needed for classic level). TODO make the code work without this
				await store.open();

				const s0 = await store.size();
				expect(s0).greaterThanOrEqual(2);
				await store.del(data.key);
				const result = await store.get(data.key);
				expect(result).equal(undefined);
				expect(await store.size()).to.be.lessThan(s0);
			});

			it("clear", async () => {
				let result = await store.get(data.key);
				expect(result).to.exist;
				await store.clear();
				result = await store.get(data.key);
				expect(result).equal(undefined);
				expect(await store.size()).equal(0);
			});

			it("status", async () => {
				let status = await store.status();
				expect(status).equal("open");

				await store.close();

				status = await store.status();
				expect(status).equal("closed");

				await store.open();

				status = await store.status();
				expect(status).equal("open");
			});

			it("iterate", async () => {
				await store.put("y", new Uint8Array([124]));
				await store.put("z", new Uint8Array([125]));

				let result: string[] = [];
				for await (const [k, _v] of store.iterator()) {
					result.push(k);
				}
				expect(result.sort()).to.deep.equal([data.key, "y", "z"]);
			});
			/* 
			// this seems to fail in Node sometimes. Is this worth to assert?
			// when we want sequential writes, we should just await prev
				it("concurrent put", async () => {
				store.put("y", new Uint8Array([1]));
				store.put("y", new Uint8Array([2]));
				store.put("y", new Uint8Array([3]));
				store.put("y", new Uint8Array([4]));
				const last = store.put("y", new Uint8Array([5]));
				await last;

				const result = await store.get("y")
				expect(new Uint8Array(result!)).to.deep.equal(new Uint8Array([5]));

				expect(await store.size()).to.be.greaterThanOrEqual(2);
			});  
			*/

			it("concurrent delete", async () => {
				store.del(data.key);
				store.del(data.key);
				store.del(data.key);
				store.del(data.key);
				store.del(data.key);
				const last = store.del(data.key);
				await last;
				const result = await store.get(data.key);
				expect(result).equal(undefined);
				expect(await store.size()).equal(0);
			});

			it("special characters", async () => {
				await store.put("* _ /", new Uint8Array([123]));
				const result = await store.get("* _ /");
				expect(result).to.deep.equal(new Uint8Array([123]));
			});

			describe("sublevel", () => {
				it("create from sublevel", async () => {
					await waitForResolved(async () =>
						expect(await store.status()).equal("open"),
					);
					const sublevel = await store.sublevel("sublevel");
					await waitForResolved(async () =>
						expect(await sublevel.status()).equal("open"),
					);
					await sublevel.close();
					expect(await sublevel.status()).equal("closed");
					expect(await store.status()).equal("open");
				});

				it("clear sublevel", async () => {
					await store.put("a", new Uint8Array([1]));
					const sublevel = await store.sublevel("sublevel");
					await sublevel.put("a", new Uint8Array([2]));
					expect(new Uint8Array((await store.get("a"))!)).to.deep.equal(
						new Uint8Array([1]),
					);
					expect(new Uint8Array((await sublevel.get("a"))!)).to.deep.equal(
						new Uint8Array([2]),
					);
					expect(new Uint8Array((await store.get("a"))!)).to.deep.equal(
						new Uint8Array([1]),
					);
					await store.clear();
					expect(await sublevel.get("a")).to.be.undefined;
				});
			});
		});
	});
});

// 		"test": "aegir clean && aegir build --no-bundle && playwright-test  'dist/test/index.spec.js' --runner mocha --assets ../../../../node_modules/@peerbit/any-store-opfs/dist --timeout 60000",
