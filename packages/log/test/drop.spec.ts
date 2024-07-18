import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import { Log } from "../src/log.js";

describe("drop", () => {
	let log: Log<Uint8Array>;
	let store: AnyBlockStore;
	beforeEach(async () => {
		log = new Log();
		store = new AnyBlockStore();
		await store.start();
		await log.open(store, await Ed25519Keypair.create());
	});

	afterEach(async () => {
		await log.close();
		await store.stop();
	});
	it("drops entries", async () => {
		const e0 = await log.append(new Uint8Array([1]));
		expect(log.length).equal(1);
		let loadedEntry = await store.get(e0.entry.hash);
		expect(loadedEntry).to.exist;
		await log.drop();
		loadedEntry = await store.get(e0.entry.hash);
		expect(loadedEntry).equal(undefined);
	});
});
