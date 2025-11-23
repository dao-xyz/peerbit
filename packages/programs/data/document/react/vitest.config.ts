import path from "node:path";
import { defineConfig } from "vitest/config";

const ROOT = path.resolve(__dirname, "../../../../..");
const DOM_SETUP = [
	path.join(ROOT, "vitest.setup.ts"),
	path.join(ROOT, "vitest.setup.dom.ts"),
];

export default defineConfig({
	resolve: {
		preserveSymlinks: true,
	},
	server: {
		fs: {
			allow: [ROOT],
		},
	},
	esbuild: {
		target: "es2022",
	},
	test: {
		name: "dom",
		environment: "happy-dom",
		globals: true,
		include: ["src/**/*.dom.test.ts?(x)", "vitest/**/*.dom.test.ts?(x)"],
		exclude: ["node_modules", "dist"],
		setupFiles: DOM_SETUP,
		deps: {
			optimizer: {
				web: {
					include: ["@peerbit/document", "@peerbit/program"],
				},
			},
		},
	},
});
