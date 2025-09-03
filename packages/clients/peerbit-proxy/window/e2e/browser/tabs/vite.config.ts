import peerbit from "@peerbit/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
	plugins: [react(), peerbit()],
	server: { port: 5210 },
	build: { target: "es2022" },
});
