import { AnyBlockStore } from "@peerbit/blocks";
import { Log } from "../src/log.js";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { expect } from 'chai'
import sinon from 'sinon'

describe("load", () => {
	let log: Log<Uint8Array>;
	let store: AnyBlockStore;
	let indexer: HashmapIndices;
	beforeEach(async () => {
		log = new Log();
		store = new AnyBlockStore();
		await store.start();
		indexer = new HashmapIndices();
		await log.open(store, await Ed25519Keypair.create(), { indexer });
	});

	afterEach(async () => {
		await log.close();
		await store.stop();
	});
	it("can reload", async () => {
		await log.append(new Uint8Array([1]));
		expect(log.length).equal(1);
		expect(await log.getHeads().all()).to.have.length(1);
		await log.load();
		expect(log.length).equal(1);
		expect(await log.getHeads().all()).to.have.length(1);
	});

	it("sets size on load", async () => {
		await log.append(new Uint8Array([1]));
		await log.close();
		await log.open(store, await Ed25519Keypair.create(), { indexer });
		await log.load();
		const [entry] = await log.toArray();
		expect(entry.size).equal(242);
	});

	it("load after delete", async () => {
		await log.append(new Uint8Array([1]), { meta: { next: [] } });
		const { entry: e2 } = await log.append(new Uint8Array([2]), {
			meta: { next: [] }
		});
		expect(log.length).equal(2);
		expect(await log.getHeads().all()).to.have.length(2);
		await log.deleteRecursively(e2);
		expect(log.length).equal(1);
		expect(await log.getHeads().all()).to.have.length(1);
		log = new Log({ id: log.id });
		await log.open(store, await Ed25519Keypair.create(), { indexer });
		await log.load();
		expect(log.length).equal(1);
	});

	it("does not update storage after loading local entries", async () => {
		await log.append(new Uint8Array([1]), { meta: { next: [] } });
		expect(log.length).equal(1);
		expect(await log.getHeads().all()).to.have.length(1);

		const putFn = sinon.spy(log.blocks.put);

		await log.close();

		await log.open(store, await Ed25519Keypair.create(), { indexer });

		await log.load();

		expect(putFn.notCalled).to.be.true
	});

	it("failing to load entry will not corrupt memory", async () => {
		const { entry: e1 } = await log.append(new Uint8Array([1]), {
			meta: { next: [] }
		});
		expect(log.length).equal(1);
		const getFn = log.blocks.get.bind(log.blocks);
		let skip = true;
		log.blocks.get = (hash, options) => {
			if (skip) {
				if (hash === e1.hash) {
					return undefined;
				}
			}
			return getFn(hash, options);
		};

		await log.close();
		await log.open(store, await Ed25519Keypair.create(), { indexer });
		await expect(log.load()).rejectedWith(
			"Failed to load entry from head with hash: " + e1.hash
		);
	});

	it("failing to load entry with ignoreMissing", async () => {
		const { entry: e1 } = await log.append(new Uint8Array([1]), {
			meta: { next: [] }
		});
		await log.append(new Uint8Array([2]), { meta: { next: [] } });

		expect(log.length).equal(2);
		const getFn = log.blocks.get.bind(log.blocks);
		let skip = true;
		log.blocks.get = (hash, options) => {
			if (skip) {
				if (hash === e1.hash) {
					return undefined;
				}
			}
			return getFn(hash, options);
		};

		await log.close();
		expect(() => log.length).to.throw("Closed");
		await log.open(store, await Ed25519Keypair.create(), { indexer });
		await log.load({ ignoreMissing: true, reload: true });

		expect(log.length).equal(2); // two entries still exist in the index because we don't want to corrupt the log
	});
});
