import { expect, test } from "@playwright/test";

test("loads sqlite3 wasm from /peerbit/sqlite3", async ({ page }) => {
	page.on("console", (message) => {
		console.log(`[browser:${message.type()}] ${message.text()}`);
	});
	page.on("pageerror", (error) => {
		console.log(`[browser:error] ${error.message}`);
	});

	const responsePromise = page.waitForResponse((res) =>
		res.url().includes("/peerbit/sqlite3/sqlite3.wasm"),
	);

	await page.goto("/?sqlite=1");

	await expect(page.getByTestId("sqlite-status")).toBeVisible();

	const response = await responsePromise;
	expect(new URL(response.url()).pathname).toBe(
		"/peerbit/sqlite3/sqlite3.wasm",
	);
	expect(response.ok()).toBeTruthy();
});
