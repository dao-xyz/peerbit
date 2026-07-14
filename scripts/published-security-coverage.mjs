import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

export const packageDirectories = Object.freeze([
	"packages/utils/crypto",
	"packages/utils/any-store/interface",
	"packages/utils/any-store/opfs",
	"packages/utils/any-store/any-store",
	"packages/utils/any-store/proxy",
	"packages/utils/indexer/interface",
	"packages/utils/indexer/sqlite3",
	"packages/utils/indexer/tests",
	"packages/log",
	"packages/programs/data/document/react",
	"packages/clients/peerbit-react",
	"packages/clients/peerbit-server/node",
	"packages/clients/vite",
]);

export const validatePublishedSecurityCoverage = async ({
	packageNames,
	changesetPath,
}) => {
	let changeset;
	try {
		changeset = await readFile(changesetPath, "utf8");
	} catch (error) {
		if (error?.code === "ENOENT") {
			// `changeset version` consumes this file before the guarded publish.
			return false;
		}
		throw error;
	}

	const changesetPackages = [
		...changeset.matchAll(/^"([^"]+)":\s+(?:patch|minor|major)\s*$/gm),
	].map((match) => match[1]);
	assert(changesetPackages.length > 0, "security changeset has no packages");
	assert.equal(
		new Set(changesetPackages).size,
		changesetPackages.length,
		"security changeset contains duplicate packages",
	);
	assert.deepEqual(
		[...packageNames].sort(),
		changesetPackages.sort(),
		"published-consumer coverage must exactly match the security changeset",
	);
	return true;
};
