// get monorepo root location using esm and .git folder
import * as findUp from "find-up";
import path from "path";

// In git worktrees, `.git` is a *file* (not a directory), so don't constrain the type.
// Fall back to workspace markers for non-git environments.
const rootMarker =
	(await findUp.findUp(".git")) ??
	(await findUp.findUp("pnpm-workspace.yaml")) ??
	(await findUp.findUp("package.json"));
if (!rootMarker) {
	throw new Error("Unable to locate repo root (no .git/workspace marker found)");
}
const root = path.dirname(rootMarker);

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
