import peerbit from "@peerbit/vite";
import react from "@vitejs/plugin-react";
import path from "node:path";
import { defineConfig } from "vite";

const PORT = Number(process.env.CANONICAL_DOCUMENT_REACT_E2E_PORT ?? 5263);
const ROOT = path.resolve(__dirname, "../../../../../../..");

export default defineConfig({
	plugins: [react(), peerbit()],
	optimizeDeps: {
		include: ["buffer"],
		esbuildOptions: {
			define: {
				global: "globalThis",
			},
			target: "esnext",
		},
	},
	define: {
		global: "globalThis",
	},
	resolve: {
		alias: {
			buffer: "buffer",
		},
	},
	build: {
		target: "esnext",
	},
	worker: {
		format: "es",
	},
	server: {
		host: "0.0.0.0",
		port: PORT,
		fs: {
			allow: [__dirname, ROOT],
		},
	},
});
