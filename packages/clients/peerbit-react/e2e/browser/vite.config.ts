import peerbit from "@peerbit/vite";
import react from "@vitejs/plugin-react";
import path from "node:path";
import { defineConfig } from "vite";

export default defineConfig({
	plugins: [peerbit(), react()],
	optimizeDeps: {
		include: ["buffer"],
		esbuildOptions: {
			define: {
				global: "globalThis",
			},
			target: "esnext",
		},
	},
	resolve: {
		alias: {
			buffer: "buffer",
		},
		// Ensure all workspace packages share a single React instance in the bundle.
		dedupe: ["react", "react-dom"],
	},
	define: {
		global: "globalThis",
	},
	build: {
		target: "es2022",
		rollupOptions: {
			input: [
				path.resolve(__dirname, "index.html"),
				path.resolve(__dirname, "service-worker.ts"),
			],
			output: {
				entryFileNames: (chunk) =>
					chunk.facadeModuleId?.endsWith("service-worker.ts")
						? "service-worker.js"
						: "assets/[name]-[hash].js",
				chunkFileNames: "assets/[name]-[hash].js",
				assetFileNames: "assets/[name]-[hash][extname]",
			},
		},
	},
	worker: {
		format: "es",
	},
	server: {
		port: 4173,
		host: true,
	},
});
