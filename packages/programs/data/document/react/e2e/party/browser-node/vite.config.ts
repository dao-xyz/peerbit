import peerbit from "@peerbit/vite";
import react from "@vitejs/plugin-react";
import fs from "node:fs";
import path from "node:path";
import { defineConfig } from "vite";

const ROOT = path.resolve(__dirname, "..");
const PUBLIC_DIR = path.resolve(__dirname, "public");

if (!fs.existsSync(PUBLIC_DIR)) {
	fs.mkdirSync(PUBLIC_DIR, { recursive: true });
}

export default defineConfig({
	plugins: [react(), peerbit()],
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
});
