import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { type SQLiteIndices, create } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import sinon from "sinon";
import { createEntry } from "../src/entry-create.js";
import type { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

const outcome = <T>(promise: Promise<T>) =>
	promise.then(
		(value) => ({ value, error: undefined as unknown }),
		(error: unknown) => ({ value: undefined, error }),
	);

describe("fromEntry resource ownership", () => {
	let store: AnyBlockStore;
	let identity: Ed25519Keypair;
	let indexer: SQLiteIndices;
	let entry: Entry<Uint8Array>;
	let returned: Log<Uint8Array> | undefined;

	beforeEach(async () => {
		store = new AnyBlockStore();
		await store.start();
		identity = await Ed25519Keypair.create();
		indexer = await create();
		entry = await createEntry({
			store,
			identity,
			data: new Uint8Array([1]),
		});
	});

	afterEach(async () => {
		sinon.restore();
		await returned?.close();
		returned = undefined;
		// Also clean the supplied indexer when testing the unfixed implementation.
		await indexer.stop();
		await store.stop();
	});

	it("transfers an open log to the caller after successful replay", async () => {
		const stop = sinon.spy(indexer, "stop");
		returned = await Log.fromEntry(store, identity, entry.hash, { indexer });
		expect(returned.closed).to.equal(false);
		expect((await returned.toArray()).map((item) => item.hash)).to.deep.equal([
			entry.hash,
		]);
		expect(stop.callCount).to.equal(0);
		await returned.close();
		expect(stop.callCount).to.equal(1);
	});

	it("closes the indexer when a block read rejects without stopping the caller's store", async () => {
		const failure = new Error("entry fetch failed");
		const get = sinon.stub(store, "get").rejects(failure);
		const stop = sinon.spy(indexer, "stop");
		const result = await outcome(
			Log.fromEntry(store, identity, entry.hash, { indexer }),
		);
		expect(result.error).to.equal(failure);
		expect(stop.callCount).to.equal(1);
		expect(await indexer.properties.db.status()).to.equal("closed");
		get.restore();
		expect(await store.get(entry.hash)).to.not.equal(undefined);
	});

	it("preserves an authorization error after releasing the opened indexer", async () => {
		const failure = new Error("authorization failed");
		const stop = sinon.spy(indexer, "stop");
		const result = await outcome(
			Log.fromEntry(store, identity, entry, {
				indexer,
				canAppend: async () => {
					throw failure;
				},
			}),
		);
		expect(result.error).to.equal(failure);
		expect(stop.callCount).to.equal(1);
		expect(await indexer.properties.db.status()).to.equal("closed");
		expect(await store.get(entry.hash)).to.not.equal(undefined);
	});

	it("keeps committed blocks recoverable when a change callback rejects", async () => {
		const failure = new Error("change callback failed");
		const stop = sinon.spy(indexer, "stop");
		const drop = sinon.spy(indexer, "drop");
		const added: string[] = [];
		const result = await outcome(
			Log.fromEntry(store, identity, entry, {
				indexer,
				onChange: async (change) => {
					added.push(...(change.added ?? []).map((item) => item.entry.hash));
					throw failure;
				},
			}),
		);
		expect(added).to.deep.equal([entry.hash]);
		expect(result.error).to.equal(failure);
		expect(stop.callCount).to.equal(1);
		expect(drop.callCount).to.equal(0);
		expect(await indexer.properties.db.status()).to.equal("closed");
		expect(await store.get(entry.hash)).to.not.equal(undefined);
		returned = await Log.fromEntry(store, identity, entry.hash, { indexer });
		expect((await returned.toArray()).map((item) => item.hash)).to.deep.equal([
			entry.hash,
		]);
	});

	it("waits for cleanup before rejecting replay", async () => {
		const failure = new Error("authorization failed");
		let entered!: () => void;
		const closing = new Promise<void>((resolve) => (entered = resolve));
		let release!: () => void;
		const gate = new Promise<void>((resolve) => (release = resolve));
		const originalStop = indexer.stop.bind(indexer);
		sinon.stub(indexer, "stop").callsFake(async () => {
			entered();
			await gate;
			await originalStop();
		});
		let settled = false;
		const result = outcome(
			Log.fromEntry(store, identity, entry, {
				indexer,
				canAppend: async () => {
					throw failure;
				},
			}),
		).then((value) => {
			settled = true;
			return value;
		});
		try {
			expect(
				await Promise.race([
					closing.then(() => "closing"),
					result.then(() => "settled"),
				]),
			).to.equal("closing");
			expect(settled).to.equal(false);
		} finally {
			release();
			await result;
		}
		expect((await result).error).to.equal(failure);
		expect(await indexer.properties.db.status()).to.equal("closed");
	});

	it("reports both replay and cleanup failures", async () => {
		const failure = new Error("authorization failed");
		const cleanupFailure = new Error("post-apply indexer stop failed");
		const originalStop = indexer.stop.bind(indexer);
		sinon.stub(indexer, "stop").callsFake(async () => {
			await originalStop();
			throw cleanupFailure;
		});
		const result = await outcome(
			Log.fromEntry(store, identity, entry, {
				indexer,
				canAppend: async () => {
					throw failure;
				},
			}),
		);
		expect(result.error).to.be.instanceOf(AggregateError);
		expect((result.error as AggregateError).errors).to.deep.equal([
			failure,
			cleanupFailure,
		]);
		expect(await indexer.properties.db.status()).to.equal("closed");
	});
});
