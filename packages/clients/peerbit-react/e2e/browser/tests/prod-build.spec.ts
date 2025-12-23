import { expect, test } from "@playwright/test";

test("serves a production Vite build", async ({ page }) => {
	await page.goto("/");

	const entry = page.locator('script[type="module"]').first();
	await expect(entry).toHaveAttribute("src", /\/assets\//);

	const src = await entry.getAttribute("src");
	expect(src).not.toContain("/src/");

	await expect(page.getByTestId("peer-hash")).not.toHaveText("no-peer", {
		timeout: 20_000,
	});
});
