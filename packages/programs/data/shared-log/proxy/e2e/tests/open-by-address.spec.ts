import { expect, test } from "@playwright/test";

const getEntries = async (page: { $$eval: any }) => {
	return page.$$eval('[data-testid="entries"] li', (elements: Element[]) =>
		elements.map((el) => el.textContent?.trim() || ""),
	);
};

const sessionParam = (value: string) => encodeURIComponent(value);

test.describe("canonical open(address)", () => {
	test("opens shared-log by address after host save", async ({
		page,
	}, testInfo) => {
		const session = sessionParam(`open-by-address-${testInfo.title}`);

		await page.goto(`/?scenario=shared-log&label=tab-1&session=${session}`);
		await expect(page.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		await page.evaluate(() => (window as any).__canonicalTest.append("hello"));
		const address = await page.evaluate(() =>
			(window as any).__canonicalTest.saveAddress(),
		);
		expect(address).toBeTruthy();

		const page2 = await page.context().newPage();
		await page2.goto(
			`/?scenario=shared-log&label=tab-2&session=${session}&address=${encodeURIComponent(
				address,
			)}`,
		);
		await expect(page2.getByTestId("status")).toHaveText("ready", {
			timeout: 20_000,
		});

		await expect
			.poll(() => getEntries(page2 as any), { timeout: 20_000 })
			.toContain("tab-1: hello");

		await page2.close();
	});
});
