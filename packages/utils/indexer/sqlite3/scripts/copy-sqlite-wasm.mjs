import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SQLITE_PACKAGE_DIR = path.dirname(
	require.resolve("@sqlite.org/sqlite-wasm/package.json")
);
const SOURCE = path.resolve(SQLITE_PACKAGE_DIR, "sqlite-wasm/jswasm");
const TARGET = path.resolve(__dirname, "../dist/assets/sqlite3");

if (!existsSync(SOURCE)) {
	throw new Error(`Missing sqlite-wasm assets at ${SOURCE}. Did pnpm install?`);
}

rmSync(TARGET, { recursive: true, force: true });
mkdirSync(TARGET, { recursive: true });
cpSync(SOURCE, TARGET, { recursive: true });

console.log(
	`[@peerbit/indexer-sqlite3] Copied sqlite-wasm bundle from ${SOURCE} -> ${TARGET}`
);
