import { cpSync, rmSync, mkdirSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = fileURLToPath(new URL(".", import.meta.url));
const SOURCE = path.resolve(
	HERE,
	"../../../../../../../utils/indexer/sqlite3/dist/peerbit"
);
const TARGET = path.resolve(HERE, "../browser-node/public/peerbit");

if (!existsSync(SOURCE)) {
	throw new Error(`Missing sqlite assets at ${SOURCE}. Build @peerbit/indexer-sqlite3 first.`);
}

rmSync(TARGET, { recursive: true, force: true });
mkdirSync(TARGET, { recursive: true });
cpSync(SOURCE, TARGET, { recursive: true });
console.log(`[party] Copied sqlite assets -> ${TARGET}`);
