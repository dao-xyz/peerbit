import { field, variant, vec } from "@dao-xyz/borsh";
import { id, toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import type { SQLiteIndex, SQLiteIndices } from "../src/engine.js";
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

@variant("sqlite_crash_safe_durability_nested_test")
class NestedDurableDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: vec("string") })
	tags: string[];

	constructor(id: string) {
		this.id = id;
		this.tags = [`${id}-child`];
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

	it("runs the barrier after another scope's open put transaction", async () => {
		const { createDirectory, removeDirectory } = await loadNodeFileHelpers();
		const directory = createDirectory();
		const indices = (await create(directory)) as SQLiteIndices;

		try {
			await indices.start();
			const documents = await (
				await indices.scope("documents")
			).init<NestedDurableDocument, never>({
				schema: NestedDurableDocument,
				indexBy: ["id"],
			});
			const entries = await (
				await indices.scope("entries")
			).init<DurableDocument, never>({
				schema: DurableDocument,
				indexBy: ["id"],
			});
			const durability = entries.crashSafeDurability!;
			expect(durability.crashSafe).to.equal(true);
			expect(documents.crashSafeDurability).to.equal(durability);

			// Prepare the one-tag child insert, then hold the next one after its
			// parent row has been written inside the put's transaction.
			await documents.put(new NestedDurableDocument("prepared"));
			const tags = [
				...(
					documents as unknown as SQLiteIndex<NestedDurableDocument>
				).tables.values(),
			].find((table) => table.parentPath?.at(-1) === "tags");
			const statement = indices.properties.db.statements.get(
				`${tags!.name}_put`,
			)!;
			expect(statement).not.to.equal(undefined);
			const originalGet = statement.get;
			let release!: () => void;
			const gate = new Promise<void>((resolve) => (release = resolve));
			let markStarted!: () => void;
			const childStarted = new Promise<void>(
				(resolve) => (markStarted = resolve),
			);
			statement.get = async (...args) => {
				markStarted();
				await gate;
				return originalGet.apply(statement, args);
			};

			try {
				const put = Promise.resolve(
					documents.put(new NestedDurableDocument("pending")),
				);
				await childStarted;
				let barrierSettled = false;
				const barrier = Promise.resolve(durability.barrier()).then(() => {
					barrierSettled = true;
				});
				await new Promise((resolve) => setTimeout(resolve, 20));
				expect(barrierSettled).to.equal(false);

				release();
				await put;
				await barrier;
			} finally {
				release();
				statement.get = originalGet;
			}
			expect(await documents.getSize()).to.equal(2);
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
