import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		environment: "node",
		include: ["vitest/**/*.test.ts"],
		exclude: ["dist/**", "node_modules/**"],
	},
});

