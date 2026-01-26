import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import { docsContentPlugin } from "./scripts/docsContentPlugin.js";

export default defineConfig({
	base: process.env.VITE_BASE ?? "/",
	plugins: [
		tailwindcss(),
		react(),
		docsContentPlugin(),
	],
});
