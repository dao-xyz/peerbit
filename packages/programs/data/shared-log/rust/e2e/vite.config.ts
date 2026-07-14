import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const E2E_ROOT = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
	root: E2E_ROOT,
	build: {
		emptyOutDir: true,
		outDir: "dist",
		target: "es2022",
	},
});
