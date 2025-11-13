import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import peerbit from "@peerbit/vite";
import path from "node:path";

const ROOT = path.resolve(__dirname, "..");

export default defineConfig({
	plugins: [react(), peerbit({ assets: null })],
	optimizeDeps: {
		esbuildOptions: {
			target: "esnext",
		},
	},
	build: {
		target: "esnext",
	},
	server: {
		host: "0.0.0.0",
		port: Number(process.env.PORT ?? 5255),
		fs: {
			allow: [__dirname, ROOT],
		},
	},
	resolve: {
		alias: {
			"@party/shared": path.resolve(ROOT, "shared"),
		},
	},
});
