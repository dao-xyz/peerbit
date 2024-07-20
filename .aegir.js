// get monorepo root location using esm and .git folder
import findUp from "find-up";
import path from "path";

const root = path.dirname(findUp.sync(".git", { type: "directory" }));

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
