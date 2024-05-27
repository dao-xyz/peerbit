import { AnyBlockStore } from "@peerbit/blocks";
import { Log } from "../src/log.js";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";

describe("recover", () => {
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
	it("recovers from empty heads", async () => {
		await log.append(new Uint8Array([1]));
		await log.append(new Uint8Array([2]));
		await log.append(new Uint8Array([3]), { meta: { next: [] } });

		await (log.blocks as any)["_store"].store.set("not a cid", new Uint8Array([4]));
		expect(log.length).equal(3);
		expect(await log.getHeads().all()).to.have.length(2);

		await log.close();

		log = new Log();
		await log.open(store, await Ed25519Keypair.create());
		await log.recover();

		expect(log.length).equal(3);
		expect(await log.getHeads().all()).to.have.length(2);

		// now destroy heads and try to reload
	});

	it("recovers and merges current heads", async () => {
		await log.append(new Uint8Array([1]));
		await log.append(new Uint8Array([2]));
		await log.append(new Uint8Array([3]), { meta: { next: [] } });

		await (log.blocks as any)["_store"].store.set("not a cid", new Uint8Array([4]));
		expect(log.length).equal(3);
		expect(await log.getHeads().all()).to.have.length(2);

		await log.close();

		log = new Log();
		await log.open(store, await Ed25519Keypair.create());

		await log.append(new Uint8Array([4]), { meta: { next: [] } });
		await log.recover();

		expect(log.length).equal(4);
		expect(await log.getHeads().all()).to.have.length(3);

		// now destroy heads and try to reload
	});
});
