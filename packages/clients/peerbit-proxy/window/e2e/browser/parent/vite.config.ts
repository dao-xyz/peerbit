import peerbit from "@peerbit/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import includeAssetsPlugin from "./../plugin.js";

const wasmContentTypePlugin = {
	name: "wasm-content-type-plugin",
	configureServer(server) {
		server.middlewares.use((req, res, next) => {
			if (req.url.endsWith(".wasm")) {
				res.setHeader("Content-Type", "application/wasm");
			}
			next();
		});
	},
};

export default defineConfig({
	plugins: [
		react(),
		/* wasmContentTypePlugin,
		includeAssetsPlugin({
			assets: [
				{
					src: "../../../../../../../node_modules/@peerbit/any-store-opfs/dist/peerbit",
					dest: "peerbit/",
				},
				{
					src: "../../../../../../../node_modules/@peerbit/indexer-sqlite3/dist/peerbit",
					dest: "peerbit/",
				},
			],
		}), */
		peerbit(),
	],
	optimizeDeps: {
		/* exclude: ['@sqlite.org/sqlite-wasm', '@peerbit/any-store', '@peerbit/any-store-opfs'], */
		esbuildOptions: {
			target: "esnext",
		},
	},
	build: {
		target: "esnext",
	},
	server: {
		port: 5202,
	},
});
