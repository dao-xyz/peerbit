import { variant } from "@dao-xyz/borsh";
import { id, toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import { create } from "../src/index.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;
const describeNode = isNode ? describe : describe.skip;

const loadNodeFileHelpers = async () => {
	// Aegir bundles this test file for browsers too. Keep Node built-ins behind
	// non-literal dynamic imports so the browser bundle does not resolve them.
	const fsModule = "node:fs";
	const pathModule = "node:path";
	const urlModule = "node:url";
	const [fs, path, { fileURLToPath }] = await Promise.all([
		import(fsModule) as Promise<typeof import("node:fs")>,
		import(pathModule) as Promise<typeof import("node:path")>,
		import(urlModule) as Promise<typeof import("node:url")>,
	]);
	const testDirectory = path.dirname(fileURLToPath(import.meta.url));

	return {
		createDirectory: () =>
			path.join(testDirectory, "tmp", "durability", uuid()),
		removeDirectory: (directory: string) =>
			fs.rmSync(directory, { recursive: true, force: true }),
	};
};

@variant("sqlite_crash_safe_durability_test")
class DurableDocument {
	@id({ type: "string" })
	id: string;

	constructor(id: string) {
		this.id = id;
	}
}

describeNode("SQLite crash-safe durability", () => {
	it("exposes and propagates an awaitable barrier for directory-backed FULL mode", async () => {
		const { createDirectory, removeDirectory } = await loadNodeFileHelpers();
		const directory = createDirectory();
		const indices = await create(directory, {
			pragmas: { synchronous: "FULL" },
		});

		try {
			await indices.start();
			const index = await indices.init<DurableDocument, never>({
				schema: DurableDocument,
				indexBy: ["id"],
			});
			const durability = index.crashSafeDurability;
			expect(durability?.crashSafe).to.equal(true);

			await index.put(new DurableDocument("acknowledged"));
			await durability!.barrier();
		} finally {
			await indices.stop();
		}

		const reopened = await create(directory, {
			pragmas: { synchronous: "FULL" },
		});
		try {
			await reopened.start();
			const index = await reopened.init<DurableDocument, never>({
				schema: DurableDocument,
				indexBy: ["id"],
			});
			expect((await index.get(toId("acknowledged")))?.value.id).to.equal(
				"acknowledged",
			);
		} finally {
			await reopened.stop();
			removeDirectory(directory);
		}
	});

	it("uses FULL as the crash-safe directory default", async () => {
		const { createDirectory, removeDirectory } = await loadNodeFileHelpers();
		const directory = createDirectory();
		const indices = await create(directory);

		try {
			await indices.start();
			const index = await indices.init<DurableDocument, never>({
				schema: DurableDocument,
				indexBy: ["id"],
			});
			expect(index.crashSafeDurability?.crashSafe).to.equal(true);
			await index.crashSafeDurability!.barrier();
		} finally {
			await indices.stop();
			removeDirectory(directory);
		}
	});

	it("treats an empty directory as memory-only", async () => {
		const indices = await create("", {
			pragmas: { synchronous: "FULL" },
		});

		try {
			await indices.start();
			const index = await indices.init<DurableDocument, never>({
				schema: DurableDocument,
				indexBy: ["id"],
			});
			expect(indices.persisted()).to.equal(false);
			expect(indices.preservesDataOnStop()).to.equal(false);
			expect(index.crashSafeDurability).to.equal(undefined);
		} finally {
			await indices.stop();
		}
	});

	it("fails closed for memory databases and directory-backed NORMAL or OFF mode", async () => {
		const { createDirectory, removeDirectory } = await loadNodeFileHelpers();
		const cases = [
			{ directory: undefined, synchronous: "FULL" as const },
			{ directory: createDirectory(), synchronous: "NORMAL" as const },
			{ directory: createDirectory(), synchronous: "OFF" as const },
		];

		for (const testCase of cases) {
			const indices = await create(testCase.directory, {
				pragmas: { synchronous: testCase.synchronous },
			});
			try {
				await indices.start();
				const index = await indices.init<DurableDocument, never>({
					schema: DurableDocument,
					indexBy: ["id"],
				});
				expect(index.crashSafeDurability).to.equal(undefined);
			} finally {
				await indices.stop();
				if (testCase.directory) {
					removeDirectory(testCase.directory);
				}
			}
		}
	});
});
