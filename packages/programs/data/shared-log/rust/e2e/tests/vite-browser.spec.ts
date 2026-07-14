import { expect, test } from "@playwright/test";

const isSharedLogRustAsset = (url: string): boolean =>
	/shared_log_rust[^/]*\.(?:js|wasm)(?:\?|$)/.test(url);

test("loads Vite-emitted glue and WASM with the native planner active", async ({
	page,
}) => {
	const responses: Array<{ status: number; url: string }> = [];
	const failedRequests: string[] = [];
	const pageErrors: string[] = [];

	page.on("response", (response) => {
		if (isSharedLogRustAsset(response.url())) {
			responses.push({ status: response.status(), url: response.url() });
		}
	});
	page.on("requestfailed", (request) => {
		if (isSharedLogRustAsset(request.url())) {
			failedRequests.push(request.url());
		}
	});
	page.on("pageerror", (error) => pageErrors.push(error.message));

	await page.goto("/");
	await expect(page.getByTestId("status")).toHaveText("native-ready");

	await expect
		.poll(() =>
			responses.some(({ url }) => new URL(url).pathname.endsWith(".wasm")),
		)
		.toBe(true);

	const result = await page.evaluate(() => window.__sharedLogRustResult);
	expect(result).toEqual({
		nativePlannerActive: true,
		length: 1,
		samples: [["peer-a", { intersecting: true }]],
	});
	expect(failedRequests).toEqual([]);
	expect(pageErrors).toEqual([]);
	expect(
		responses.some(({ url }) => new URL(url).pathname.endsWith(".js")),
	).toBe(true);
	expect(responses.every(({ status }) => status === 200)).toBe(true);
	expect(
		responses.some(
			({ url }) => new URL(url).pathname === "/wasm/shared_log_rust.js",
		),
	).toBe(false);
});
