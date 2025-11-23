import { defineConfig, devices } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.PARTY_PORT ?? 5255);
const BASE_URL = `http://localhost:${PORT}`;
const DIRNAME = fileURLToPath(new URL(".", import.meta.url));
const BROWSER_NODE_DIR = path.resolve(DIRNAME, "./browser-node");

export default defineConfig({
	testDir: "./tests",
	fullyParallel: false,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 1 : 0,
	workers: process.env.CI ? 1 : undefined,
	reporter: process.env.PARTY_HTML_REPORT
		? [
				["list"],
				["html", { open: "never" }],
			]
		: [["list"]],

	use: {
		trace: "on-first-retry",
		baseURL: BASE_URL,
	},
	projects: [
		{
			name: "chromium",
			use: { ...devices["Desktop Chrome"] },
		},
	],
	webServer: [
		{
			command: `pnpm --dir ${BROWSER_NODE_DIR} dev -- --host 0.0.0.0 --port ${PORT}`,
			url: BASE_URL,
			reuseExistingServer: !process.env.CI,
		},
	],
});
