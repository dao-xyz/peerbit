import { type AnyStore } from "@peerbit/any-store-interface";
import { expect } from "chai";
import { mkdtemp, rm } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { createStore } from "../src/index.js";

const tempDirectory = async () => mkdtemp(join(tmpdir(), "peerbit-any-store-rust-"));

const collectKeys = async (store: AnyStore): Promise<string[]> => {
	const keys: string[] = [];
	for await (const [key] of store.iterator()) {
		keys.push(key);
	}
	return keys.sort();
};

describe("@peerbit/any-store-rust", () => {
	const cleanup: string[] = [];

	afterEach(async () => {
		await Promise.all(cleanup.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
	});

	it("stores transient values", async () => {
		const store = createStore();
		await store.open();
		await store.put("a", new Uint8Array([1, 2, 3]));
		await store.put("b", new Uint8Array([4]));

		expect(await store.get("a")).to.deep.equal(new Uint8Array([1, 2, 3]));
		expect(await store.size()).to.equal(4);
		expect(await collectKeys(store)).to.deep.equal(["a", "b"]);

		await store.del("a");
		expect(await store.get("a")).to.equal(undefined);
		expect(await store.size()).to.equal(1);
		await store.close();
	});

	it("stores transient values with the redb engine", async () => {
		const store = createStore(undefined, { engine: "redb" });
		await store.open();
		await store.put("a", new Uint8Array([1, 2, 3]));
		await store.put("b", new Uint8Array([4]));

		expect(await store.get("a")).to.deep.equal(new Uint8Array([1, 2, 3]));
		expect(await store.size()).to.equal(4);
		expect(await collectKeys(store)).to.deep.equal(["a", "b"]);

		await store.del("a");
		expect(await store.get("a")).to.equal(undefined);
		expect(await store.size()).to.equal(1);
		await store.close();
	});

	it("applies batched mutations", async () => {
		const store = createStore();
		await store.open();
		await store.putMany([
			["a", new Uint8Array([1])],
			["b", new Uint8Array([2, 3])],
		]);

		expect(await store.getMany(["a", "b", "c"])).to.deep.equal([
			new Uint8Array([1]),
			new Uint8Array([2, 3]),
			undefined,
		]);
		expect(await store.size()).to.equal(3);
		expect(await store.delMany(["a", "missing"])).to.equal(1);
		expect(await collectKeys(store)).to.deep.equal(["b"]);
		await store.close();
	});

	it("rejects persistent redb stores until a byte-range backend lands", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory, { engine: "redb" });
		await expect(store.open()).to.be.rejectedWith(/redb engine is transient/);
	});

	it("persists values across reopen", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory);
		await store.open();
		await store.put("a", new Uint8Array([1, 2, 3]));
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.persisted()).to.equal(true);
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1, 2, 3]));
		expect(await store.size()).to.equal(3);
		await store.close();
	});

	it("persists journaled deletes before compaction", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory, { compactOnClose: false });
		await store.open();
		await store.put("a", new Uint8Array([1]));
		await store.put("b", new Uint8Array([2]));
		await store.del("a");
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.get("a")).to.equal(undefined);
		expect(await store.get("b")).to.deep.equal(new Uint8Array([2]));
		await store.close();
	});

	it("keeps sublevels isolated and clears them from the parent", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory);
		await store.open();
		const sublevel = await store.sublevel("sub/level");
		await store.put("a", new Uint8Array([1]));
		await sublevel.put("a", new Uint8Array([2]));
		await store.close();

		store = createStore(directory);
		await store.open();
		const reopenedSublevel = await store.sublevel("sub/level");
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await reopenedSublevel.get("a")).to.deep.equal(new Uint8Array([2]));

		await store.clear();
		expect(await store.get("a")).to.equal(undefined);
		expect(await reopenedSublevel.get("a")).to.equal(undefined);
		await store.close();

		store = createStore(directory);
		await store.open();
		const clearedSublevel = await store.sublevel("sub/level");
		expect(await store.get("a")).to.equal(undefined);
		expect(await clearedSublevel.get("a")).to.equal(undefined);
		await store.close();
	});

	it("handles special-character keys and repeated deletes", async () => {
		const store = createStore();
		await store.open();
		const key = "* _ /";
		await store.put(key, new Uint8Array([123]));
		store.del(key);
		store.del(key);
		await store.del(key);
		expect(await store.get(key)).to.equal(undefined);
		expect(await store.size()).to.equal(0);
		await store.close();
	});
});
