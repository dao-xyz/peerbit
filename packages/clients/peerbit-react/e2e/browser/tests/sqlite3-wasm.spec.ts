import { type Response, expect, test } from "@playwright/test";

const useViteDevFixture = process.env.PEERBIT_E2E_VITE_DEV === "1";

test("loads the sqlite3 module and wasm from /peerbit/sqlite3", async ({
	page,
}) => {
	page.on("console", (message) => {
		console.log(`[browser:${message.type()}] ${message.text()}`);
	});
	page.on("pageerror", (error) => {
		console.log(`[browser:error] ${error.message}`);
	});
	const moduleResponses: Response[] = [];
	page.on("response", (response) => {
		if (response.url().includes("/peerbit/sqlite3/sqlite3.mjs")) {
			moduleResponses.push(response);
		}
	});

	const responsePromise = page.waitForResponse((res) =>
		res.url().includes("/peerbit/sqlite3/sqlite3.wasm"),
	);
	const workerResponsePromise = useViteDevFixture
		? page.waitForResponse((res) =>
				res.url().includes("/peerbit/sqlite3/sqlite3.worker.min.js"),
			)
		: undefined;

	await page.goto("/?sqlite=1");

	await expect(page.getByTestId("sqlite-status")).toHaveText("ready", {
		timeout: 20_000,
	});

	const response = await responsePromise;
	const workerResponse = await workerResponsePromise;
	expect(new URL(response.url()).pathname).toBe(
		"/peerbit/sqlite3/sqlite3.wasm",
	);
	expect(response.ok()).toBeTruthy();
	expect(response.headers()["content-type"]).toContain("application/wasm");
	expect(moduleResponses.length).toBeGreaterThanOrEqual(
		useViteDevFixture ? 2 : 1,
	);
	for (const moduleResponse of moduleResponses) {
		expect(new URL(moduleResponse.url()).pathname).toBe(
			"/peerbit/sqlite3/sqlite3.mjs",
		);
		expect(new URL(moduleResponse.url()).search).toBe("");
		expect(moduleResponse.ok()).toBeTruthy();
		expect(moduleResponse.headers()["content-type"]).toContain("javascript");
	}
	if (workerResponse) {
		expect(new URL(workerResponse.url()).pathname).toBe(
			"/peerbit/sqlite3/sqlite3.worker.min.js",
		);
		expect(new URL(workerResponse.url()).search).toBe("");
		expect(workerResponse.ok()).toBeTruthy();
	}
});

test("loads direct sqlite under nonce and Trusted Types CSP", async ({
	page,
}) => {
	test.skip(!useViteDevFixture, "Dedicated Vite development regression");

	const errors: string[] = [];
	page.on("console", (message) => {
		if (message.type() === "error") errors.push(message.text());
	});
	page.on("pageerror", (error) => errors.push(error.message));
	await page.route("**/*", async (route) => {
		if (!route.request().isNavigationRequest()) {
			await route.continue();
			return;
		}
		const response = await route.fetch();
		await route.fulfill({
			response,
			headers: {
				...response.headers(),
				"content-security-policy": [
					"default-src 'none'",
					"script-src 'nonce-peerbit-sqlite-e2e' 'strict-dynamic' 'wasm-unsafe-eval'",
					"connect-src 'self' ws:",
					"style-src 'unsafe-inline'",
					"require-trusted-types-for 'script'",
				].join("; "),
			},
		});
	});

	const moduleResponsePromise = page.waitForResponse((response) =>
		response.url().includes("/peerbit/sqlite3/sqlite3.mjs"),
	);
	await page.goto("/?strict-csp=1");
	await expect(page.getByTestId("sqlite-status")).toHaveText("ready", {
		timeout: 20_000,
	});

	const moduleResponse = await moduleResponsePromise;
	expect(new URL(moduleResponse.url()).search).toBe("");
	expect(moduleResponse.ok()).toBeTruthy();
	expect(errors).toEqual([]);
});
