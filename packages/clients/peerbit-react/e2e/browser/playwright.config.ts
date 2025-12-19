import { defineConfig, devices } from "@playwright/test";

const defaultPort = 4173;
const envPort = Number.parseInt(process.env.PLAYWRIGHT_PORT ?? "", 10);
const port = Number.isFinite(envPort) && envPort > 0 ? envPort : defaultPort;
const baseURL = `http://localhost:${port}`;

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
		baseURL,
		trace: "on-first-retry",
		video: "retain-on-failure",
		screenshot: "only-on-failure",
	},
	webServer: {
		command: `pnpm dev -- --port ${port}`,
		url: baseURL,
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
