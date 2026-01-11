import { expect, test } from "@playwright/test";

const getLogStats = async (page: { evaluate: any }) => {
	const stats = await page.evaluate(() =>
		(window as any).__canonicalTest.getHostStats(),
	);
	const entries = stats?.sharedLogs?.entries ?? [];
	const total =
		typeof stats?.sharedLogs?.total === "number"
			? stats.sharedLogs.total
			: entries.length;
	const refs = entries.reduce(
		(sum: number, entry: { refs?: number }) => sum + (entry.refs ?? 0),
		0,
	);
	return { total, refs };
};

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("canonical lifetime", () => {
	test("releases shared-log refs when tabs close", async ({
		page,
	}, testInfo) => {
		const session = sessionParam(`lifetime-${testInfo.title}`);
		await page.goto(`/?scenario=shared-log&label=tab-1&session=${session}`);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const page2 = await page.context().newPage();
		await page2.goto(`/?scenario=shared-log&label=tab-2&session=${session}`);
		await expect(page2.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const page3 = await page.context().newPage();
		await page3.goto(`/?scenario=stats`);
		await expect(page3.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		await expect
			.poll(() => getLogStats(page3), { timeout: 20_000 })
			.toEqual({ total: 1, refs: 2 });

		await page.evaluate(() => (window as any).__canonicalTest.close());
		await page.close();

		await expect
			.poll(() => getLogStats(page3), { timeout: 20_000 })
			.toEqual({ total: 1, refs: 1 });

		await page2.evaluate(() => (window as any).__canonicalTest.close());
		await page2.close();

		await expect
			.poll(() => getLogStats(page3), { timeout: 20_000 })
			.toEqual({ total: 0, refs: 0 });
		await page3.close();
	});

	test("releases shared-log refs after abrupt tab close", async ({
		page,
	}, testInfo) => {
		const session = sessionParam(`lifetime-abrupt-${testInfo.title}`);
		await page.goto(
			`/?scenario=shared-log&label=tab-1&session=${session}&autoClose=false`,
		);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const page2 = await page.context().newPage();
		await page2.goto(
			`/?scenario=shared-log&label=tab-2&session=${session}&autoClose=false`,
		);
		await expect(page2.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const page3 = await page.context().newPage();
		await page3.goto(`/?scenario=stats`);
		await expect(page3.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		await expect
			.poll(() => getLogStats(page3), { timeout: 20_000 })
			.toEqual({ total: 1, refs: 2 });

		await page.close();

		await expect
			.poll(() => getLogStats(page3), { timeout: 20_000 })
			.toEqual({ total: 1, refs: 1 });

		await page2.close();

		await expect
			.poll(() => getLogStats(page3), { timeout: 20_000 })
			.toEqual({ total: 0, refs: 0 });

		await page3.close();
	});
});
