import { defineConfig, devices } from "@playwright/test";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.CANONICAL_E2E_PORT ?? 5260);
const BASE_URL = `http://localhost:${PORT}`;
const DIRNAME = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
	testDir: "./tests",
	fullyParallel: false,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 1 : 0,
	workers: process.env.CI ? 1 : undefined,
	reporter: [["list"]],
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
			command: `pnpm --dir ${DIRNAME} dev -- --host 0.0.0.0 --port ${PORT}`,
			url: BASE_URL,
			reuseExistingServer: !process.env.CI,
		},
	],
});
