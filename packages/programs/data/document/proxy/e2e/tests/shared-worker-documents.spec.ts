import { expect, test } from "@playwright/test";

const getMessages = async (page: { $$eval: any }) => {
	return page.$$eval('[data-testid="messages"] li', (elements: Element[]) =>
		elements.map((el) => el.textContent?.trim() || ""),
	);
};

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("document canonical shared worker", () => {
	test("shares documents across tabs", async ({ page }, testInfo) => {
		const session = sessionParam(`documents-${testInfo.title}`);
		await page.goto(`/?scenario=documents&label=tab-1&session=${session}`);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		// SharedWorker instances are scoped to a browser context.
		const page2 = await page.context().newPage();
		await page2.goto(`/?scenario=documents&label=tab-2&session=${session}`);
		await expect(page2.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const peerId1 = (await page.getByTestId("peer-id").textContent())?.trim();
		const peerId2 = (await page2.getByTestId("peer-id").textContent())?.trim();
		expect(peerId1).toBeTruthy();
		expect(peerId1).toEqual(peerId2);

		await page.evaluate(() => (window as any).__canonicalTest.put("hello"));

		await expect
			.poll(
				async () => {
					const messages = await getMessages(page2 as any);
					return messages;
				},
				{ timeout: 20_000 },
			)
			.toContain("tab-1: hello");

		await expect
			.poll(
				async () => {
					const info = await page.evaluate(() =>
						(window as any).__canonicalTest.getLastLogEntryHash(),
					);
					return !!info?.head && info?.head === info?.entryHash;
				},
				{ timeout: 20_000 },
			)
			.toBe(true);

		await page2.close();
	});
});
