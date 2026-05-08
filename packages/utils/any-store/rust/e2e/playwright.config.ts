import { defineConfig, devices } from "@playwright/test";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.PEERBIT_ANY_STORE_RUST_E2E_PORT ?? 5274);
const BASE_URL = `http://localhost:${PORT}`;
const DIRNAME = fileURLToPath(new URL("..", import.meta.url));

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
			command: `pnpm --dir ${DIRNAME} e2e:dev -- --host 0.0.0.0 --port ${PORT}`,
			url: BASE_URL,
			reuseExistingServer: !process.env.CI,
		},
	],
});
