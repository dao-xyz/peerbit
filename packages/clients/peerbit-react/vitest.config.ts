import path from "node:path";
import { defineConfig } from "vitest/config";

const ROOT = path.resolve(__dirname, "../../..");

export default defineConfig({
	test: {
		name: "node",
		environment: "node",
		include: ["vitest/**/*.test.ts?(x)"],
		exclude: ["dist", "node_modules"],
		setupFiles: [path.join(ROOT, "vitest.setup.ts")],
		// These suites intentionally replace process-global localStorage and timers.
		// Keep them serial, and use a worker thread so a completed fork cannot get
		// stuck waiting for its final IPC teardown message.
		pool: "threads",
		fileParallelism: false,
		hookTimeout: 120_000,
		testTimeout: 120_000,
	},
});
