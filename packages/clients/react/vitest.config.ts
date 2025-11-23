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
		hookTimeout: 120_000,
		testTimeout: 120_000,
	},
});
