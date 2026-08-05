import peerbit from "@peerbit/vite";
import path from "node:path";
import { defineConfig } from "vite";

const cspNonce = "peerbit-sqlite-e2e";

export default defineConfig({
	root: path.resolve(__dirname, "sqlite-dev"),
	plugins: [peerbit()],
	html: {
		cspNonce,
	},
	define: {
		global: "globalThis",
	},
	optimizeDeps: {
		include: ["buffer"],
	},
	resolve: {
		alias: {
			buffer: "buffer",
		},
	},
	server: {
		headers: {
			"Cross-Origin-Embedder-Policy": "require-corp",
			"Cross-Origin-Opener-Policy": "same-origin",
		},
	},
	worker: {
		format: "es",
	},
});
