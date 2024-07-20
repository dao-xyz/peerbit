import peerbit from "@peerbit/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import includeAssetsPlugin from "./../plugin.js";

// https://vitejs.dev/config/
export default defineConfig({
	plugins: [
		react(),
		includeAssetsPlugin({
			assets: [
				{
					src: "../../../../../../../node_modules/@peerbit/indexer-sqlite3/dist/peerbit",
					dest: "peerbit/",
				},
			],
		}),
		peerbit(),
	],
	optimizeDeps: {
		esbuildOptions: {
			target: "esnext",
		},
	},
	build: {
		target: "esnext",
	},
	server: {
		port: 5201,
	},
});
