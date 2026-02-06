// Resolve monorepo root from the location of this config file.
// Note: in git worktrees `.git` is a file (not a directory), so we can't rely
// on `findUp(..., { type: "directory" })` always succeeding.
import * as findUp from "find-up";
import path from "path";
import { fileURLToPath } from "url";

const configDir = path.dirname(fileURLToPath(import.meta.url));
const gitPath =
	(await findUp.findUp(".git", { cwd: configDir, type: "directory" })) ??
	(await findUp.findUp(".git", { cwd: configDir, type: "file" }));
const root = gitPath ? path.dirname(gitPath) : configDir;

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
