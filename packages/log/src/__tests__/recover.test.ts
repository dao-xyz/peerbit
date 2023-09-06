import { MemoryLevelBlockStore } from "@peerbit/blocks";
import { Log } from "../log";
import { Ed25519Keypair } from "@peerbit/crypto";
import { createStore } from "@peerbit/test-utils";
import LazyLevel from "@peerbit/lazy-level";

describe("load", () => {
	let log: Log<Uint8Array>;
	let store: MemoryLevelBlockStore;
	beforeEach(async () => {
		log = new Log();
		store = new MemoryLevelBlockStore();
		await store.start();
		const cache = new LazyLevel(createStore());
		await cache.open();
		await log.open(store, await Ed25519Keypair.create(), { cache });
	});

	afterEach(async () => {
		await log.close();
		await store.stop();
	});
	it("recovers from empty heads", async () => {
		await log.append(new Uint8Array([1]));
		await log.append(new Uint8Array([2]));
		await log.append(new Uint8Array([3]), { meta: { next: [] } });

		await log.storage["_level"]["_store"].put("not a cid", new Uint8Array([4]));
		expect(log.length).toEqual(3);
		expect(await log.getHeads()).toHaveLength(2);

		await log.close();

		const cache = new LazyLevel(createStore());
		await cache.open();
		log = new Log();
		await log.open(store, await Ed25519Keypair.create(), { cache });
		await log.recover();

		expect(log.length).toEqual(3);
		expect(await log.getHeads()).toHaveLength(2);

		// now destroy heads and try to reload
	});

	it("recovers and merges current heads", async () => {
		await log.append(new Uint8Array([1]));
		await log.append(new Uint8Array([2]));
		await log.append(new Uint8Array([3]), { meta: { next: [] } });

		await log.storage["_level"]["_store"].put("not a cid", new Uint8Array([4]));
		expect(log.length).toEqual(3);
		expect(await log.getHeads()).toHaveLength(2);

		await log.close();

		const cache = new LazyLevel(createStore());
		await cache.open();
		log = new Log();
		await log.open(store, await Ed25519Keypair.create(), { cache });

		await log.append(new Uint8Array([4]), { meta: { next: [] } });
		await log.recover();

		expect(log.length).toEqual(4);
		expect(await log.getHeads()).toHaveLength(3);

		// now destroy heads and try to reload
	});
});
