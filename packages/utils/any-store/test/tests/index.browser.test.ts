import { test, expect, JSHandle } from "@playwright/test";
import { AnyStore } from "../../src/index.js";
import { delay, waitForResolved } from "@peerbit/time";

const asObject = (any: any) => JSON.parse(JSON.stringify(any));

test.describe("AnyLevel", () => {
	let types = ["disc", "memory"];

	types.map((type) => {
		return test.describe(type, () => {
			let handle: JSHandle<AnyStore>;

			let isInDirectoryTest: (string | undefined)[] = [undefined, "sub/*& +"];

			return isInDirectoryTest.map((directory) => {
				return test.describe(
					"directory: " + (directory ? JSON.stringify(directory) : "root"),
					() => {
						test.beforeEach(async ({ page }) => {
							await page.goto("http://localhost:5205");
							await page.waitForFunction(() => window["create"]);
							await page.evaluateHandle(
								async (args) => {
									await window["create"](args[0], args[1]);
								},
								[type, directory]
							);

							await page.waitForFunction(() => window["store"]);
							handle = await page.evaluateHandle(async () => {
								const store = window["store"] as AnyStore;
								await store.put("x", new Uint8Array([123]));
								return store;
							});
						});

						test("get", async () => {
							const result = await handle.evaluate((store) => store.get("x"));
							expect(result).toEqual(asObject(new Uint8Array([123])));
						});

						test("get missing", async () => {
							const result = await handle.evaluate((store) => store.get("y"));
							expect(result).toEqual(undefined);
						});

						test("size", async () => {
							const result = await handle.evaluate((store) => store.size());
							expect(result).toEqual(1);
						});

						test("del", async () => {
							await handle.evaluate((store) => store.del("x"));
							const result = await handle.evaluate((store) => store.get("x"));
							expect(result).toBeUndefined();
						});

						/* TODO make this one work on FireFox (we got "Not modifications allowed" error) */

						/* test("del+put concurrently", async () => {
							// dont await put
							await handle.evaluate((store) => {
								store.put("cx", new Uint8Array([123]));
							});
							await handle.evaluate(async (store) => {
								await store.del("cx");
							});

							const result = await handle.evaluate((store) => store.get("cx"));
							expect(result).toBeUndefined();
						}); */

						test("del missing", async () => {
							await handle.evaluate((store) => store.del("y"));
						});

						test("clear", async () => {
							let result = await handle.evaluate((store) => store.get("x"));
							expect(result).toBeDefined();
							await handle.evaluate((store) => store.clear());
							result = await handle.evaluate((store) => store.get("x"));
							expect(result).toBeUndefined();
						});

						test("status", async () => {
							let status = await handle.evaluate((store) => store.status());
							expect(status).toEqual("open");

							await handle.evaluate((store) => store.close());

							status = await handle.evaluate((store) => store.status());
							expect(status).toEqual("closed");

							await handle.evaluate((store) => store.open());

							status = await handle.evaluate((store) => store.status());
							expect(status).toEqual("open");
						});

						test("iterate", async () => {
							await handle.evaluate((store) =>
								store.put("y", new Uint8Array([124]))
							);
							await handle.evaluate((store) =>
								store.put("z", new Uint8Array([125]))
							);

							await waitForResolved(async () => {
								const result = await handle.evaluate(async (store) => {
									let ret: string[] = [];
									for await (const [k, v] of store.iterator()) {
										ret.push(k);
									}
									return ret;
								});
								expect(result.sort()).toEqual(["x", "y", "z"]);
							});
						});

						test("concurrent put", async () => {
							await handle.evaluate(async (store) => {
								store.put("y", new Uint8Array([1]));
								store.put("y", new Uint8Array([2]));
								store.put("y", new Uint8Array([3]));
								store.put("y", new Uint8Array([4]));
								const last = store.put("y", new Uint8Array([5]));
								await last;
							});

							const result = await handle.evaluate((store) => store.get("y"));
							expect(result).toEqual(asObject(new Uint8Array([5])));
						});

						test("sublevel", async () => {
							await handle.evaluate(async (store) => {
								const sub = await store.sublevel("sub");
								await sub.put("a", new Uint8Array([1]));
							});

							// put
							await waitForResolved(async () => {
								const result = await handle.evaluate(async (store) => {
									const sub = await store.sublevel("sub");
									return await sub.get("a");
								});
								expect(result).toEqual(asObject(new Uint8Array([1])));
							});

							// status
							const result = await handle.evaluate(async (store) => {
								const sub = await store.sublevel("sub");
								await sub.close();
								return sub.status();
							});
							expect(result).toEqual("closed");
						});

						test("special characters", async () => {
							await handle.evaluate((store) =>
								store.put("* _ /", new Uint8Array([123]))
							);
							const result = await handle.evaluate((store) =>
								store.get("* _ /")
							);
							expect(result).toEqual(asObject(new Uint8Array([123])));
						});
					}
				);
			});
		});
	});
});
