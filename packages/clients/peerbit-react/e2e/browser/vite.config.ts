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
	server: {
		port: 4173,
		host: true,
	},
});
