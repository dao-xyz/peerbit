import { expect, test } from "@playwright/test";

test.describe("iframe", () => {
	test("appends cross frames", async ({ page }) => {
		let frames = 5;
		await page.goto("http://localhost:5202/?frames=" + frames);
		const locator = await page.getByTestId("pb0");
		await locator.waitFor({ state: "visible" });
		const iframe = page.frameLocator("#pb0");
		const counter = await iframe.getByTestId("counter");
		await counter.waitFor({ state: "visible" });
		await expect(counter).toHaveText(String(frames), { timeout: 5000 });
	});
});
