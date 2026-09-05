import { copyFileSync, mkdirSync, writeFileSync } from "node:fs";

// Keep the test-only Chai setup usable by npm/pnpm on Windows as well as POSIX.
const destination = new URL("../node_modules/chai-extend/", import.meta.url);
mkdirSync(destination, { recursive: true });
copyFileSync(
	new URL("../chai-global.js", import.meta.url),
	new URL("chai-global.js", destination),
);
writeFileSync(
	new URL("package.json", destination),
	`${JSON.stringify({ type: "module" })}\n`,
);
