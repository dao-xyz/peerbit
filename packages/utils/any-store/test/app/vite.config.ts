import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vitejs.dev/config/
export default defineConfig({
	plugins: [react()],
	optimizeDeps: {
		esbuildOptions: {
			target: "esnext"
		},
		exclude: ["@peerbit/any-store"] // https://github.com/vitejs/vite/issues/11672
	},
	build: {
		target: "esnext"
	},
	server: {
		port: 5205
	}
});
