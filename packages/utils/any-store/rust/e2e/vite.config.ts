import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const PORT = Number(process.env.PEERBIT_ANY_STORE_RUST_E2E_PORT ?? 5274);
const DIRNAME = fileURLToPath(new URL(".", import.meta.url));
const PACKAGE_ROOT = path.resolve(DIRNAME, "..");
const REPO_ROOT = path.resolve(DIRNAME, "../../../../..");

export default defineConfig({
	root: DIRNAME,
	build: {
		target: "esnext",
	},
	worker: {
		format: "es",
	},
	server: {
		host: "0.0.0.0",
		port: PORT,
		fs: {
			allow: [DIRNAME, PACKAGE_ROOT, REPO_ROOT],
		},
	},
});
