import { defineConfig, devices } from "@playwright/test";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.PEERBIT_SHARED_LOG_RUST_E2E_PORT ?? 5275);
const BASE_URL = `http://127.0.0.1:${PORT}`;
const PACKAGE_ROOT = fileURLToPath(new URL("..", import.meta.url));

export default defineConfig({
	testDir: "./tests",
	fullyParallel: false,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 1 : 0,
	workers: 1,
	reporter: [["list"]],
	use: {
		baseURL: BASE_URL,
		trace: "on-first-retry",
	},
	projects: [
		{
			name: "chromium",
			use: { ...devices["Desktop Chrome"] },
		},
	],
	webServer: [
		{
			command: `pnpm --dir ${PACKAGE_ROOT} e2e:preview --host 127.0.0.1 --port ${PORT}`,
			url: BASE_URL,
			reuseExistingServer: false,
		},
	],
});
