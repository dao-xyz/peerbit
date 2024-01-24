import { AnyBlockStore } from "@peerbit/blocks";
import { Log } from "../log";
import { Ed25519Keypair } from "@peerbit/crypto";
import { createStore } from "@peerbit/any-store";
import { jest } from "@jest/globals";
describe("load", () => {
	let log: Log<Uint8Array>;
	let store: AnyBlockStore;
	let cache: ReturnType<typeof createStore>;
	beforeEach(async () => {
		log = new Log();
		store = new AnyBlockStore();
		await store.start();
		cache = createStore();
		cache.open();
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

	it("sets size on load", async () => {
		await log.append(new Uint8Array([1]));
		await log.close();
		await log.open(store, await Ed25519Keypair.create(), { cache });
		await log.load();
		const [entry] = await log.toArray();
		expect(entry.size).toEqual(245);
	});

	it("load after delete", async () => {
		await log.append(new Uint8Array([1]), { meta: { next: [] } });
		const { entry: e2 } = await log.append(new Uint8Array([2]), {
			meta: { next: [] }
		});
		expect(log.length).toEqual(2);
		expect(await log.getHeads()).toHaveLength(2);
		await log.deleteRecursively(e2);
		expect(log.length).toEqual(1);
		expect(await log.getHeads()).toHaveLength(1);
		log = new Log();
		await log.open(store, await Ed25519Keypair.create(), { cache });
		await log.load();
		expect(log.length).toEqual(1);
	});

	it("does not update storage after loading local entries", async () => {
		await log.append(new Uint8Array([1]), { meta: { next: [] } });
		expect(log.length).toEqual(1);
		expect(await log.getHeads()).toHaveLength(1);

		const putFn = jest.fn(log.blocks.put);

		await log.close();

		await log.open(store, await Ed25519Keypair.create(), { cache });

		await log.load();

		expect(putFn).not.toHaveBeenCalled();
	});

	it("failing to load entry will not corrupt memory", async () => {
		const { entry: e1 } = await log.append(new Uint8Array([1]), {
			meta: { next: [] }
		});
		expect(log.length).toEqual(1);
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
		await log.open(store, await Ed25519Keypair.create(), { cache });
		await expect(log.load()).rejects.toThrow(
			"Failed to load entry from head with hash: " + e1.hash
		);
	});

	it("failing to load entry with ignoreMissing", async () => {
		const { entry: e1 } = await log.append(new Uint8Array([1]), {
			meta: { next: [] }
		});
		await log.append(new Uint8Array([2]), { meta: { next: [] } });

		expect(log.length).toEqual(2);
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
		expect(() => log.length).toThrow("Closed");
		await log.open(store, await Ed25519Keypair.create(), { cache });
		await log.load({ ignoreMissing: true, reload: true });
		expect(log.length).toEqual(1);
	});
});
