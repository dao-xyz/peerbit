import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import { docsContentPlugin } from "./scripts/docsContentPlugin.js";

const parsePort = (value: string | undefined, fallback: number) => {
	if (!value) return fallback;
	const parsed = Number(value);
	return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

// Use a non-default Vite port to reduce collisions with other local dev servers.
const devPort = parsePort(process.env.PEERBIT_ORG_PORT ?? process.env.PORT, 5193);
const previewPort = parsePort(process.env.PEERBIT_ORG_PREVIEW_PORT, devPort + 1);

export default defineConfig({
	base: "./",
	server: {
		port: devPort,
		strictPort: false,
	},
	preview: {
		port: previewPort,
		strictPort: false,
	},
	plugins: [
		tailwindcss(),
		react(),
		docsContentPlugin(),
	],
});
