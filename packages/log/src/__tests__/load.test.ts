import { AnyBlockStore } from "@peerbit/blocks";
import { Log } from "../log";
import { Ed25519Keypair } from "@peerbit/crypto";
import { createStore } from "@peerbit/any-store";

describe("load", () => {
	let log: Log<Uint8Array>;
	let store: AnyBlockStore;
	beforeEach(async () => {
		log = new Log();
		store = new AnyBlockStore();
		await store.start();
		const cache = createStore();
		await cache.open();
		await log.open(store, await Ed25519Keypair.create(), { cache });
	});

	afterEach(async () => {
		await log.close();
		await store.stop();
	});
	it("can reload", async () => {
		await log.append(new Uint8Array([1]));
		expect(log.length).toEqual(1);
		expect(await log.getHeads()).toHaveLength(1);
		await log.load();
		expect(log.length).toEqual(1);
		expect(await log.getHeads()).toHaveLength(1);
	});
});
