import { expect, test } from "@playwright/test";

const getEntries = async (page: { $$eval: any }) => {
	return page.$$eval('[data-testid="entries"] li', (elements: Element[]) =>
		elements.map((el) => el.textContent?.trim() || ""),
	);
};

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("shared-log canonical shared worker", () => {
	test("shares shared-log across tabs", async ({ page }, testInfo) => {
		const session = sessionParam(`shared-log-${testInfo.title}`);
		await page.goto(`/?scenario=shared-log&label=tab-1&session=${session}`);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const page2 = await page.context().newPage();
		await page2.goto(`/?scenario=shared-log&label=tab-2&session=${session}`);
		await expect(page2.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const peerId1 = (await page.getByTestId("peer-id").textContent())?.trim();
		const peerId2 = (await page2.getByTestId("peer-id").textContent())?.trim();
		expect(peerId1).toBeTruthy();
		expect(peerId1).toEqual(peerId2);

		await page.evaluate(() => (window as any).__canonicalTest.append("hello"));

		await expect
			.poll(() => getEntries(page2 as any), { timeout: 20_000 })
			.toContain("tab-1: hello");

		await page2.close();
	});
});
