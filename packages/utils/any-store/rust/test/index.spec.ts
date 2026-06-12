import { type AnyStore } from "@peerbit/any-store-interface";
import { expect } from "chai";
import { mkdtemp, readFile, rm, writeFile } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { createStore } from "../src/index.js";

const tempDirectory = async () => mkdtemp(join(tmpdir(), "peerbit-any-store-rust-"));

type StoreInternals = {
	persistence: {
		appendJournal(
			record: Uint8Array,
			durability: "normal" | "strict",
		): Promise<void>;
	};
	journalQueue: Promise<unknown>;
};

const internalsOf = (store: unknown): StoreInternals => store as StoreInternals;

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
		expect(store.put("a", new Uint8Array([1, 2, 3]))).to.equal(undefined);
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
		expect(store.putMany([
			["a", new Uint8Array([1])],
			["b", new Uint8Array([2, 3])],
		])).to.equal(undefined);

		expect(await store.getMany(["a", "b", "c"])).to.deep.equal([
			new Uint8Array([1]),
			new Uint8Array([2, 3]),
			undefined,
		]);
		expect(await store.hasMany(["c", "a", "b"])).to.deep.equal([
			false,
			true,
			true,
		]);
		expect(await store.size()).to.equal(3);
		expect(await store.delMany(["a", "missing"])).to.equal(1);
		expect(await collectKeys(store)).to.deep.equal(["b"]);
		await store.close();
	});

	it("stores immutable transient values without forcing an async boundary", async () => {
		const store = createStore();
		await store.open();
		expect(store.putImmutable("a", new Uint8Array([1, 2, 3]))).to.equal(
			undefined,
		);
		expect(
			store.putManyImmutable([
				["b", new Uint8Array([4])],
				["c", new Uint8Array([5, 6])],
			]),
		).to.equal(undefined);

		expect(await store.getMany(["a", "b", "c"])).to.deep.equal([
			new Uint8Array([1, 2, 3]),
			new Uint8Array([4]),
			new Uint8Array([5, 6]),
		]);
		await store.close();
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

	it("persists immutable values across reopen", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory);
		await store.open();
		expect(store.putImmutable("a", new Uint8Array([1, 2, 3]))).to.equal(
			undefined,
		);
		expect(
			store.putManyImmutable([
				["b", new Uint8Array([4])],
				["c", new Uint8Array([5, 6])],
			]),
		).to.equal(undefined);
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.getMany(["a", "b", "c"])).to.deep.equal([
			new Uint8Array([1, 2, 3]),
			new Uint8Array([4]),
			new Uint8Array([5, 6]),
		]);
		await store.close();
	});

	it("keeps strict immutable persistence on an async durability boundary", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory, { durability: "strict" });
		await store.open();
		expect(store.putImmutable("a", new Uint8Array([1]))).to.be.instanceOf(
			Promise,
		);
		expect(store.putManyImmutable([["b", new Uint8Array([2])]])).to.be.instanceOf(
			Promise,
		);
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

	it("recovers from a torn journal tail and keeps later writes", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory, { compactOnClose: false });
		await store.open();
		await store.put("a", new Uint8Array([1]));
		await store.put("b", new Uint8Array([2]));
		await store.close();

		// Simulate a crash mid-append: the last journal record is torn.
		const journalPath = join(directory, "store.wal");
		const journal = await readFile(journalPath);
		await writeFile(journalPath, journal.subarray(0, journal.byteLength - 3));

		store = createStore(directory, { compactOnClose: false });
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("b")).to.equal(undefined);
		await store.put("c", new Uint8Array([3]));
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("b")).to.equal(undefined);
		expect(await store.get("c")).to.deep.equal(new Uint8Array([3]));
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

	it("surfaces a failed journal append on the next mutation and clears it once reported", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory);
		await store.open();
		await store.put("a", new Uint8Array([1]));
		const internals = internalsOf(store);
		await internals.journalQueue;

		const failure = new Error("journal append failed");
		const persistence = internals.persistence;
		const originalAppend = persistence.appendJournal.bind(persistence);
		let failNext = true;
		persistence.appendJournal = (record, durability) => {
			if (failNext) {
				failNext = false;
				return Promise.reject(failure);
			}
			return originalAppend(record, durability);
		};

		// normal durability: the put resolves while the append fails behind it
		await store.put("b", new Uint8Array([2]));
		await internals.journalQueue;

		let reported: unknown;
		try {
			await store.put("c", new Uint8Array([3]));
		} catch (error) {
			reported = error;
		}
		expect(reported).to.equal(failure);

		// reported once: the store recovers and keeps serving writes
		await store.put("c", new Uint8Array([3]));
		expect(await store.get("c")).to.deep.equal(new Uint8Array([3]));
		await store.close();
		expect(store.status()).to.equal("closed");
	});

	it("reaches closed status when the journal flush fails during close", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory, { compactOnClose: false });
		await store.open();
		await store.put("a", new Uint8Array([1]));
		const internals = internalsOf(store);
		await internals.journalQueue;

		const failure = new Error("journal append failed");
		internals.persistence.appendJournal = () => Promise.reject(failure);
		await store.put("b", new Uint8Array([2]));

		let closeError: unknown;
		try {
			await store.close();
		} catch (error) {
			closeError = error;
		}
		expect(closeError).to.equal(failure);
		expect(store.status()).to.equal("closed");

		// fully released: the store can be reopened and read again
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		await store.close();
		expect(store.status()).to.equal("closed");
	});

	it("keeps the store closed when close() races open()", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory);
		const open = store.open();
		const close = store.close();
		await Promise.all([open, close]);
		expect(store.status()).to.equal("closed");

		// the interleaved open must not resurrect the store afterwards
		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(store.status()).to.equal("closed");

		await store.open();
		expect(store.status()).to.equal("open");
		await store.put("a", new Uint8Array([1]));
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		await store.close();
		expect(store.status()).to.equal("closed");
	});

	it("rejects mutations after close until reopened", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory);
		await store.open();
		await store.put("a", new Uint8Array([1]));
		const close = store.close();
		let duringClose: unknown;
		try {
			await store.put("b", new Uint8Array([2]));
		} catch (error) {
			duringClose = error;
		}
		await close;
		expect(duringClose).to.be.instanceOf(Error);
		expect((duringClose as Error).message).to.equal("RustAnyStore is closed");
		expect(store.status()).to.equal("closed");

		let afterClose: unknown;
		try {
			await store.put("c", new Uint8Array([3]));
		} catch (error) {
			afterClose = error;
		}
		expect(afterClose).to.be.instanceOf(Error);
		expect(store.status()).to.equal("closed");

		// explicit open() restores service, matching the level backend
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		await store.put("d", new Uint8Array([4]));
		expect(await store.get("d")).to.deep.equal(new Uint8Array([4]));
		await store.close();

		const transient = createStore();
		await transient.open();
		const transientClose = transient.close();
		let transientThrown: unknown;
		try {
			await transient.put("a", new Uint8Array([1]));
		} catch (error) {
			transientThrown = error;
		}
		await transientClose;
		expect(transientThrown).to.be.instanceOf(Error);
		expect(transient.status()).to.equal("closed");
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
