import { test, expect, JSHandle } from "@playwright/test";
import { AnyStore } from "../../src/index.js";
import { waitForResolved } from "@peerbit/time";

const asObject = (any: any) => JSON.parse(JSON.stringify(any));

test.describe("AnyLevel", () => {
	let types = ["disc", "memory"];

	types.map((type) => {
		return test.describe(type, () => {
			let handle: JSHandle<AnyStore>;

			let isSubLevelTest: string[][] = [[], ["sub/*& +"]];

			return isSubLevelTest.map((testLevel) => {
				return test.describe("sublevel: " + JSON.stringify(testLevel), () => {
					test.beforeEach(async ({ page }) => {
						await page.goto("http://localhost:5205");
						await page.waitForFunction(() => window["create"]);
						await page.evaluateHandle(async (type) => {
							window["create"](type);
						}, type);
						await page.waitForFunction(() => window["store"]);
						handle = await page.evaluateHandle(
							async (args) => {
								let store = window["store"] as AnyStore;
								for (const level of args[0]) {
									store = await store.sublevel(level);
								}
								return store;
							},
							[testLevel]
						);
						await handle.evaluate((store) =>
							store.put("x", new Uint8Array([123]))
						);
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

					test("del missing", async () => {
						await handle.evaluate((store) => store.del("y"));
					});

					test("clear", async () => {
						await handle.evaluate((store) => store.clear());
						const result = await handle.evaluate((store) => store.get("x"));
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
						const result = await handle.evaluate((store) => store.get("* _ /"));
						expect(result).toEqual(asObject(new Uint8Array([123])));
					});
				});
			});
		});
	});
});
