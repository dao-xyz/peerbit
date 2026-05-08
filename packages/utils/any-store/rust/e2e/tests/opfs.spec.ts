import { expect, test } from "@playwright/test";

type CheckpointManifest = {
	epoch: number;
	snapshot: string;
	journal: string;
};

const checksumString = (value: string): string => {
	let hash = 0x811c9dc5;
	for (let i = 0; i < value.length; i++) {
		hash ^= value.charCodeAt(i);
		hash = Math.imul(hash, 0x01000193);
	}
	return (hash >>> 0).toString(16).padStart(8, "0");
};

const encodeManifest = (manifest: CheckpointManifest): string => {
	const payload = JSON.stringify(manifest);
	return JSON.stringify({
		payload: JSON.parse(payload),
		checksum: checksumString(payload),
	});
};

const testDirectory = (workerIndex: number): string =>
	`rust-opfs-${Date.now()}-${workerIndex}-${Math.random()
		.toString(16)
		.slice(2)}`;

test.describe("any-store-rust OPFS", () => {
	test("persists WAL and sublevels across worker and page reloads", async ({
		page,
	}, testInfo) => {
		await page.goto("/");
		await expect(page.getByTestId("status")).toHaveText("ready");

		const directory = testDirectory(testInfo.workerIndex);

		await page.evaluate(async (directory) => {
			const api = (window as any).__rustAnyStoreTest;
			await api.request({ op: "open", directory });
			await api.request({ op: "put", key: "a", value: "alpha" });
			await api.request({
				op: "subPut",
				level: "sub/level",
				key: "b",
				value: "beta",
			});
			await api.request({ op: "close" });
			await api.restartWorker();
		}, directory);

		await page.evaluate(async (directory) => {
			const api = (window as any).__rustAnyStoreTest;
			await api.request({ op: "open", directory });
		}, directory);

		await expect
			.poll(() =>
				page.evaluate(() =>
					(window as any).__rustAnyStoreTest.request({
						op: "get",
						key: "a",
					}),
				),
			)
			.toBe("alpha");
		await expect
			.poll(() =>
				page.evaluate(() =>
					(window as any).__rustAnyStoreTest.request({
						op: "subGet",
						level: "sub/level",
						key: "b",
					}),
				),
			)
			.toBe("beta");

		await page.evaluate(() =>
			(window as any).__rustAnyStoreTest.request({ op: "close" }),
		);
		await page.reload();
		await expect(page.getByTestId("status")).toHaveText("ready");

		await page.evaluate(async (directory) => {
			const api = (window as any).__rustAnyStoreTest;
			await api.request({ op: "open", directory });
		}, directory);
		expect(
			await page.evaluate(() =>
				(window as any).__rustAnyStoreTest.request({
					op: "get",
					key: "a",
				}),
			),
		).toBe("alpha");
		expect(
			await page.evaluate(() =>
				(window as any).__rustAnyStoreTest.request({
					op: "subGet",
					level: "sub/level",
					key: "b",
				}),
			),
		).toBe("beta");

		await page.evaluate(async () => {
			const api = (window as any).__rustAnyStoreTest;
			await api.request({ op: "clear" });
			await api.request({ op: "close" });
		});
	});

	test("falls back when the newest manifest points at an incomplete checkpoint", async ({
		page,
	}, testInfo) => {
		await page.goto("/");
		await expect(page.getByTestId("status")).toHaveText("ready");

		const directory = testDirectory(testInfo.workerIndex);
		const incompleteManifest = encodeManifest({
			epoch: 2,
			snapshot: "missing-snapshot.bin",
			journal: "missing-journal.wal",
		});

		await page.evaluate(
			async ({ directory, incompleteManifest }) => {
				const api = (window as any).__rustAnyStoreTest;
				await api.request({ op: "open", directory });
				await api.request({ op: "put", key: "a", value: "alpha" });
				await api.request({ op: "close" });
				await api.request({
					op: "writeOpfsFile",
					directory,
					key: "manifest-b.json",
					value: incompleteManifest,
				});
				await api.restartWorker();
			},
			{ directory, incompleteManifest },
		);

		await page.evaluate(async (directory) => {
			const api = (window as any).__rustAnyStoreTest;
			await api.request({ op: "open", directory });
		}, directory);
		expect(
			await page.evaluate(() =>
				(window as any).__rustAnyStoreTest.request({
					op: "get",
					key: "a",
				}),
			),
		).toBe("alpha");

		await page.evaluate(async () => {
			const api = (window as any).__rustAnyStoreTest;
			await api.request({ op: "clear" });
			await api.request({ op: "close" });
		});
	});
});
