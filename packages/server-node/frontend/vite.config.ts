import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { nodeResolve } from "@rollup/plugin-node-resolve";

// https://vitejs.dev/config/
export default defineConfig({
    plugins: [react()],
    build: {
        target: "esnext",
        rollupOptions: {
            plugins: [
                nodeResolve(), // Required until https://github.com/vitejs/vite/issues/7385 is solved
            ],
        },
    },
});
