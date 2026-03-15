import { expect, test } from "@playwright/test";

test("runs the OPFS protocol benchmark", async ({ page }) => {
	await page.goto("/?opfsbench=1&bytes=262144&count=12");

	await expect(page.getByTestId("opfs-benchmark-status")).toHaveText("ready", {
		timeout: 120_000,
	});

	const resultsText =
		(await page.getByTestId("opfs-benchmark-results").textContent()) ?? "";
	const results = JSON.parse(resultsText) as {
		protocol: "clone" | "legacy";
		putMs: number;
		getMs: number;
		size: number;
		bytes: number;
		count: number;
	}[];

	expect(results).toHaveLength(2);
	expect(results.map((result) => result.protocol).sort()).toEqual([
		"clone",
		"legacy",
	]);
	for (const result of results) {
		expect(result.putMs).toBeGreaterThan(0);
		expect(result.getMs).toBeGreaterThan(0);
		expect(result.size).toBe(result.bytes * result.count);
	}
});
