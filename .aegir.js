// get monorepo root location using esm and .git folder
import * as findUp from "find-up";
import path from "path";
import { fileURLToPath } from "url";

const configDir = path.dirname(fileURLToPath(import.meta.url));
const gitEntry = await findUp.findUp(".git", { cwd: configDir });
if (!gitEntry) {
	throw new Error("Failed to locate repository root from .aegir.js");
}
const root = path.dirname(path.resolve(gitEntry));

export default {
	// global options
	debug: false,
	test: {
		/* concurrency: 2, */
		files: [],
		before: () => {
			return {
				env: { TS_NODE_PROJECT: path.join(root, "tsconfig.test.json") },
			};
		},
	},
};
