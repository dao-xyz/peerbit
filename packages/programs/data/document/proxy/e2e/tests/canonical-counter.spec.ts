import { expect, test } from "@playwright/test";

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("canonical program", () => {
	test("shares counter across tabs", async ({ page }, testInfo) => {
		const session = sessionParam(`counter-${testInfo.title}`);
		await page.goto(`/?scenario=counter&label=tab-1&session=${session}`);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const page2 = await page.context().newPage();
		await page2.goto(`/?scenario=counter&label=tab-2&session=${session}`);
		await expect(page2.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		const peerId1 = (await page.getByTestId("peer-id").textContent())?.trim();
		const peerId2 = (await page2.getByTestId("peer-id").textContent())?.trim();
		expect(peerId1).toBeTruthy();
		expect(peerId1).toEqual(peerId2);

		await page.evaluate(() => (window as any).__canonicalTest.increment(1));

		await expect
			.poll(
				async () => {
					const text = await page2.getByTestId("message-count").textContent();
					return text?.trim();
				},
				{ timeout: 20_000 },
			)
			.toBe("1");

		await page2.close();
	});
});
