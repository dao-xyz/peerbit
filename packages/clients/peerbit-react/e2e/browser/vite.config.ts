import peerbit from "@peerbit/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
	plugins: [peerbit(), react()],
	optimizeDeps: {
		esbuildOptions: {
			target: "esnext",
		},
	},
	resolve: {
		// Ensure all workspace packages share a single React instance in the bundle.
		dedupe: ["react", "react-dom"],
	},
	server: {
		port: 4173,
		host: true,
	},
});
