import {
	/* expect, */
	test,
} from "@playwright/test";

test.describe("iframe", () => {
	test("appends cross frames", async ({ page }) => {
		console.warn("TODO: re-enable appends cross frames");
		// we need to rework how we proxy programs, shared-log and its backend is not parallelizable as we want right now.
		return;
		/* page.on("console", (msg) => {
			console.log("[console]", msg.type(), msg.text());
		});
		let frames = 5;
		await page.goto("http://localhost:5202/?frames=" + frames);
		const iframe = page.frameLocator("#pb0");
		await expect(iframe.getByTestId("counter")).toHaveText(String(frames), {
			timeout: 15_000,
		}); */
	});
});
