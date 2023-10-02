import assert from "assert";
import { Log } from "../log.js";
import { BlockStore, AnyBlockStore } from "@peerbit/blocks";
import { signKey, signKey3, signKey4 } from "./fixtures/privateKey.js";
import { Ed25519Keypair } from "@peerbit/crypto";
import { createStore } from "@peerbit/any-store";

const last = (arr: any[]) => {
	return arr[arr.length - 1];
};

interface Events {
	cacheUpdates: number;
	gidsRemoved: string[][];
}
const createEvents = async (
	log: Log<any>,
	store: BlockStore,
	signKey: Ed25519Keypair
) => {
	let events: Events = {
		cacheUpdates: 0,
		gidsRemoved: []
	};
	const cache = createStore();
	await log.open(store, signKey, {
		cache,
		onGidRemoved: (gids) => {
			events.gidsRemoved.push(gids);
		}
	});
	const queueFn = log.headsIndex.headsCache!.queue.bind(
		log.headsIndex.headsCache
	);
	log.headsIndex.headsCache!.queue = (change) => {
		events.cacheUpdates += 1;
		return queueFn(change);
	};
	const logClose = log.close.bind(log);
	log.close = async () => {
		await cache.close();
		return logClose();
	};
	return { log, events };
};
describe("head-tails", function () {
	let store: BlockStore;
	let log1Events: Events;
	let log2Events: Events;

	let log1: Log<Uint8Array>,
		log2: Log<Uint8Array>,
		log3: Log<Uint8Array>,
		log4: Log<Uint8Array>;

	beforeAll(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	afterAll(async () => {
		await store.stop();
	});

	beforeEach(async () => {
		const l1 = await createEvents(new Log<Uint8Array>(), store, signKey);
		log1 = l1.log;
		log1Events = l1.events;

		const l2 = await createEvents(new Log<Uint8Array>(), store, signKey);
		log2 = l2.log;
		log2Events = l2.events;

		log3 = new Log<Uint8Array>();
		await log3.open(store, signKey3);
		log4 = new Log<Uint8Array>();
		await log4.open(store, signKey4);
	});
	afterEach(async () => {
		await log1.close();
		await log2.close();
		await log3?.close();
		await log4?.close();
	});

	describe("heads", () => {
		it("finds one head after one entry", async () => {
			await log1.append(new Uint8Array([0, 0]));
			expect((await log1.getHeads()).length).toEqual(1);
		});

		it("finds one head after two entries", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			expect((await log1.getHeads()).length).toEqual(1);
		});

		it("log contains the head entry", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			assert.deepStrictEqual(
				(await log1.get((await log1.getHeads())[0].hash))?.hash,
				(await log1.getHeads())[0].hash
			);
		});

		it("finds head after a join and append", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			await log2.append(new Uint8Array([1, 0]));

			await log2.join(log1);

			expect(log2Events.cacheUpdates).toEqual(2); // initial load + join
			await log2.append(new Uint8Array([1, 1]));

			expect(log1Events.gidsRemoved).toHaveLength(0);
			expect(log2Events.gidsRemoved).toHaveLength(1); // because log2 had 2 different gis before last append

			expect(log2Events.cacheUpdates).toEqual(3);
			const expectedHead = last(await log2.toArray());

			expect((await log2.getHeads()).length).toEqual(1);
			assert.deepStrictEqual(
				(await log2.getHeads())[0].hash,
				expectedHead.hash
			);
		});

		it("finds two heads after a join", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			const expectedHead1 = last(await log1.toArray());

			await log2.append(new Uint8Array([1, 0]));
			await log2.append(new Uint8Array([1, 1]));
			const expectedHead2 = last(await log2.toArray());

			await log1.join(log2);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(2);
			expect(heads.map((x) => x.hash)).toContainAllValues([
				expectedHead1.hash,
				expectedHead2.hash
			]);
		});

		it("finds two heads after two joins", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));

			await log2.append(new Uint8Array([1, 0]));
			await log2.append(new Uint8Array([1, 1]));

			await log1.join(log2);

			await log2.append(new Uint8Array([1, 2]));

			await log1.append(new Uint8Array([0, 2]));
			await log1.append(new Uint8Array([0, 3]));
			const expectedHead2 = last(await log2.toArray());
			const expectedHead1 = last(await log1.toArray());

			await log1.join(log2);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(2);
			expect(heads[0].hash).toEqual(expectedHead1.hash);
			expect(heads[1].hash).toEqual(expectedHead2.hash);
		});

		it("finds two heads after three joins", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			await log2.append(new Uint8Array([1, 0]));
			await log2.append(new Uint8Array([1, 1]));
			await log1.join(log2);
			await log1.append(new Uint8Array([0, 2]));
			await log1.append(new Uint8Array([0, 3]));
			const expectedHead1 = last(await log1.toArray());
			await log3.append(new Uint8Array([2, 0]));
			await log3.append(new Uint8Array([2, 1]));
			await log2.join(log3);
			await log2.append(new Uint8Array([1, 2]));
			const expectedHead2 = last(await log2.toArray());
			await log1.join(log2);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(2);
			expect(heads.map((x) => x.hash)).toContainAllValues([
				expectedHead1.hash,
				expectedHead2.hash
			]);
		});

		it("finds three heads after three joins", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			await log2.append(new Uint8Array([1, 0]));
			await log2.append(new Uint8Array([1, 1]));
			await log1.join(log2);
			await log1.append(new Uint8Array([0, 2]));
			await log1.append(new Uint8Array([0, 3]));
			const expectedHead1 = last(await log1.toArray());
			await log3.append(new Uint8Array([2, 0]));
			await log2.append(new Uint8Array([1, 2]));
			await log3.append(new Uint8Array([2, 1]));
			const expectedHead2 = last(await log2.toArray());
			const expectedHead3 = last(await log3.toArray());
			await log1.join(log2);
			await log1.join(log3);

			const heads = await log1.getHeads();
			expect(heads.length).toEqual(3);
			expect(heads.map((x) => x.hash)).toContainAllValues([
				expectedHead1.hash,
				expectedHead2.hash,
				expectedHead3.hash
			]);
		});

		describe("onGidsRemoved", () => {
			it("it emits callback when gid is shadowed, triangle shape", async () => {
				/*  
				Either A or B shaded
				┌─┐┌─┐  
				│a││b│  
				└┬┘└┬┘  
				┌▽──▽──┐
				│a or b│
				└──────┘
				*/

				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]), {
					meta: { next: [] }
				});
				const { entry: b1 } = await log1.append(new Uint8Array([1, 0]), {
					meta: { next: [] }
				});
				const { entry: ab1 } = await log1.append(new Uint8Array([0]), {
					meta: { next: [a1, b1] }
				});
				expect(log1Events.cacheUpdates).toEqual(3);
				expect(log1Events.gidsRemoved).toHaveLength(1);
				expect(log1Events.gidsRemoved[0]).toHaveLength(1);
				expect(log1Events.gidsRemoved[0][0]).toEqual(
					ab1.gid === a1.gid ? b1.gid : a1.gid
				); // if ab1 has gid a then b will be shadowed
			});

			it("it emits callback when gid is shadowed, N shape", async () => {
				/*  
					No shadows
					┌──┐┌───┐ 
					│a0││b1 │ 
					└┬─┘└┬─┬┘ 
					┌▽─┐ │┌▽─┐
					│a1│ ││b2│
					└┬─┘ │└──┘
					┌▽───▽┐   
					│a2   │   
					└─────┘   
				*/

				const { entry: a0 } = await log1.append(new Uint8Array([0, 0]), {
					meta: { next: [], gidSeed: Buffer.from("b") }
				});
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]), {
					meta: { next: [a0] }
				});
				const { entry: b1 } = await log1.append(new Uint8Array([1, 0]), {
					meta: { next: [], gidSeed: Buffer.from("a") }
				});

				// b2
				const _b2 = await log1.append(new Uint8Array([1, 1]), {
					meta: { next: [b1] }
				});

				// a2
				const a2 = await log1.append(new Uint8Array([0, 2]), {
					meta: { next: [a1, b1] }
				});

				// this test only makes sense to do, we try to make b1 gid "removable", i.e. by using a1 gid instead for a2
				expect(a2.entry.gid).toEqual(a1.gid);

				// but b1 gid is used by b2, so now removals are done
				expect(log1Events.gidsRemoved).toEqual([]);
			});
		});
	});

	describe("tails", () => {
		it("returns a tail", async () => {
			await log1.append(new Uint8Array([0, 0]));
			expect((await log1.getTails()).length).toEqual(1);
		});

		it("tail is a Entry", async () => {
			await log1.append(new Uint8Array([0, 0]));
		});

		it("returns tail entries", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log2.append(new Uint8Array([1, 0]));
			await log1.join(log2);
			expect((await log1.getTails()).length).toEqual(2);
		});

		it("returns tail hashes", async () => {
			await log1.close();
			log1 = new Log();
			await log1.open(store, signKey, {
				trim: { type: "length", to: 2 }
			});
			await log2.close();
			log2 = new Log();
			await log2.open(store, signKey, {
				trim: { type: "length", to: 2 }
			});
			const { entry: a1 } = await log1.append(new Uint8Array([0, 0]));
			const { entry: b1 } = await log2.append(new Uint8Array([1, 0]));
			const { entry: a2 } = await log1.append(new Uint8Array([0, 1]));
			const { entry: b2 } = await log2.append(new Uint8Array([1, 1]));
			await log1.join(log2);

			// the joined log will only contain the last two entries a2, b2
			expect((await log1.toArray()).map((x) => x.hash)).toContainAllValues([
				a2.hash,
				b2.hash
			]);
			expect(await log1.getTailHashes()).toContainAllValues([a1.hash, b1.hash]);
		});

		it("returns no tail hashes if all entries point to empty nexts", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log2.append(new Uint8Array([1, 0]));
			await log1.join(log2);
			expect((await log1.getTailHashes()).length).toEqual(0);
		});

		/* TODO
		
		it("returns tails after loading a partial log", async () => {
			await log1.append(new Uint8Array([0,0]));
			await log1.append(new Uint8Array([0,1]));
			await log2.append(new Uint8Array([1,0]));
			await log2.append(new Uint8Array([1,1]));
			await log1.join(log2);
			await log4.join(await log1.getHeads(), { length: 2 });
			expect(log4.length).toEqual(2);
			expect((await log4.getTails()).length).toEqual(2);
			expect((await log4.getTails())[0].hash).toEqual(
				(await log4.toArray())[0].hash
			);
			expect((await log4.getTails())[1].hash).toEqual(
				(await log4.toArray())[1].hash
			);
		}); */

		/*  Feat?
		it("returns tails sorted by public key", async () => {
			await log1.append(new Uint8Array([3, 0]));
			await log2.append(new Uint8Array([1, 0]));
			await log3.append(new Uint8Array([0, 0]));
			await log3.join(log1);
			await log3.join(log2);
			await log4.join(log3);
			expect((await log4.getTails()).length).toEqual(3);

			let sortedKeys = [signKey.publicKey.bytes, signKey2.publicKey.bytes, signKey3.publicKey.bytes].sort()
			expect((await log4.getTails())[0].meta.clock.id).toEqual(
				sortedKeys[2]
			);
			expect((await log4.getTails())[1].meta.clock.id).toEqual(
				sortedKeys[1]
			);
			expect((await log4.getTails())[2].meta.clock.id).toEqual(
				sortedKeys[0]
			);
		}); */
	});
});
