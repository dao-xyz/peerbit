import { expect, test } from "@playwright/test";

test.describe("sharedworker tabs", () => {
	test("shares single host across tabs", async ({ browser }) => {
		const context = await browser.newContext();
		const page1 = await context.newPage();
		const page2 = await context.newPage();

		// page1 writes, page2 reads only
		await page1.goto("http://localhost:5210/");
		await page2.goto("http://localhost:5210/?read=true");

		const c1 = page1.getByTestId("counter");
		const c2 = page2.getByTestId("counter");

		await c1.waitFor({ state: "visible" });
		await c2.waitFor({ state: "visible" });

		// Wait until both see at least 1 (writer appends once)
		await expect(c1).toHaveText(/^[1-9][0-9]*$/, { timeout: 10000 });
		const v = await c1.textContent();
		await expect(c2).toHaveText(v ?? "1", { timeout: 10000 });

		await context.close();
	});
});
