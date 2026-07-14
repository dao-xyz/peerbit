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
		// Running their coverage workers concurrently can leave a completed worker
		// waiting forever during teardown.
		fileParallelism: false,
		hookTimeout: 120_000,
		testTimeout: 120_000,
	},
});
