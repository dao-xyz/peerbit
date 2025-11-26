import { defineConfig, devices } from "@playwright/test";

const port = 4173;

export default defineConfig({
	testDir: "./tests",
	timeout: 120_000,
	expect: {
		timeout: 10_000,
	},
	reporter: process.env.PARTY_HTML_REPORT
		? [["list"], ["html", { open: "never" }]]
		: [["list"]],
	use: {
		baseURL: `http://localhost:${port}`,
		trace: "on-first-retry",
		video: "retain-on-failure",
		screenshot: "only-on-failure",
	},
	webServer: {
		command: "pnpm dev",
		url: `http://localhost:${port}`,
		reuseExistingServer: !process.env.CI,
		timeout: 120_000,
	},
	projects: [
		{
			name: "chromium",
			use: { ...devices["Desktop Chrome"] },
		},
	],
});
