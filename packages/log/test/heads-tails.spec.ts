import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import type { Ed25519Keypair } from "@peerbit/crypto";
import assert from "assert";
import { expect } from "chai";
import { Log } from "../src/log.js";
import { signKey, signKey3, signKey4 } from "./fixtures/privateKey.js";

const last = (arr: any[]) => {
	return arr[arr.length - 1];
};

interface Events {
	gidsRemoved: string[][];
}
const createEvents = async (
	log: Log<any>,
	store: BlockStore,
	signKey: Ed25519Keypair,
) => {
	let events: Events = {
		gidsRemoved: [],
	};
	await log.open(store, signKey, {
		onGidRemoved: (gids) => {
			events.gidsRemoved.push(gids);
		},
	});

	const logClose = log.close.bind(log);
	log.close = async () => {
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

	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
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
			expect((await log1.getHeads().all()).length).equal(1);
		});

		it("finds one head after two entries", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			expect((await log1.getHeads().all()).length).equal(1);
		});

		it("log contains the head entry", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			assert.deepStrictEqual(
				(await log1.get((await log1.getHeads().all())[0].hash))?.hash,
				(await log1.getHeads().all())[0].hash,
			);
		});

		it("finds head after a join and append", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));

			await log2.append(new Uint8Array([1, 0]));

			await log2.join(log1);

			await log2.append(new Uint8Array([1, 1]));

			expect(log1Events.gidsRemoved).to.be.empty;
			expect(log2Events.gidsRemoved).to.have.length(1); // because log2 had 2 different gis before last append

			const expectedHead = last(await log2.toArray());

			expect((await log2.getHeads().all()).length).equal(1);
			assert.deepStrictEqual(
				(await log2.getHeads().all())[0].hash,
				expectedHead.hash,
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

			const heads = await log1.getHeads(true).all();
			expect(heads.length).equal(2);
			expect(heads.map((x) => x.hash)).to.have.members([
				expectedHead1.hash,
				expectedHead2.hash,
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

			const heads = await log1.getHeads(true).all();
			expect(heads.length).equal(2);
			expect(heads[0].hash).equal(expectedHead2.hash);
			expect(heads[1].hash).equal(expectedHead1.hash);
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

			const heads = await log1.getHeads(true).all();
			expect(heads.length).equal(2);
			expect(heads.map((x) => x.hash)).to.have.members([
				expectedHead1.hash,
				expectedHead2.hash,
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

			const heads = await log1.getHeads(true).all();
			expect(heads.length).equal(3);
			expect(heads.map((x) => x.hash)).to.have.members([
				expectedHead1.hash,
				expectedHead2.hash,
				expectedHead3.hash,
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
					meta: { next: [] },
				});
				const { entry: b1 } = await log1.append(new Uint8Array([1, 0]), {
					meta: { next: [] },
				});
				const { entry: ab1 } = await log1.append(new Uint8Array([0]), {
					meta: { next: [a1, b1] },
				});
				expect(log1Events.gidsRemoved).to.have.length(1);
				expect(log1Events.gidsRemoved[0]).to.have.length(1);
				expect(log1Events.gidsRemoved[0][0]).equal(
					ab1.gid === a1.gid ? b1.gid : a1.gid,
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
					meta: { next: [], gidSeed: Buffer.from("b") },
				});
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]), {
					meta: { next: [a0] },
				});
				const { entry: b1 } = await log1.append(new Uint8Array([1, 0]), {
					meta: { next: [], gidSeed: Buffer.from("a") },
				});

				// b2
				// @ts-ignore unused
				const b2 = await log1.append(new Uint8Array([1, 1]), {
					meta: { next: [b1] },
				});

				// a2
				const a2 = await log1.append(new Uint8Array([0, 2]), {
					meta: { next: [a1, b1] },
				});

				// this test only makes sense to do, we try to make b1 gid "removable", i.e. by using a1 gid instead for a2
				expect(a2.entry.gid).equal(a1.gid);

				// but b1 gid is used by b2, so now removals are done
				expect(log1Events.gidsRemoved).to.be.empty;
			});
		});
	});

	describe("tails", () => {
		it("returns a tail", async () => {
			await log1.append(new Uint8Array([0, 0]));
			expect((await log1.getTails()).length).equal(1);
		});

		it("tail is a Entry", async () => {
			await log1.append(new Uint8Array([0, 0]));
		});

		it("returns tail entries", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log2.append(new Uint8Array([1, 0]));
			await log1.join(log2);
			expect((await log1.getTails()).length).equal(2);
		});

		it("returns tail hashes", async () => {
			await log1.close();
			log1 = new Log();
			await log1.open(store, signKey, {
				trim: { type: "length", to: 2 },
			});
			await log2.close();
			log2 = new Log();
			await log2.open(store, signKey, {
				trim: { type: "length", to: 2 },
			});
			const { entry: a1 } = await log1.append(new Uint8Array([0, 0]));
			const { entry: b1 } = await log2.append(new Uint8Array([1, 0]));
			const { entry: a2 } = await log1.append(new Uint8Array([0, 1]));
			const { entry: b2 } = await log2.append(new Uint8Array([1, 1]));
			await log1.join(log2);

			// the joined log will only contain the last two entries a2, b2
			expect((await log1.toArray()).map((x) => x.hash)).to.have.members([
				a2.hash,
				b2.hash,
			]);
			expect(await log1.getTailHashes()).to.have.members([a1.hash, b1.hash]);
		});

		it("returns no tail hashes if all entries point to empty nexts", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log2.append(new Uint8Array([1, 0]));
			await log1.join(log2);
			expect((await log1.getTailHashes()).length).equal(0);
		});

		/* TODO
		
		it("returns tails after loading a partial log", async () => {
			await log1.append(new Uint8Array([0,0]));
			await log1.append(new Uint8Array([0,1]));
			await log2.append(new Uint8Array([1,0]));
			await log2.append(new Uint8Array([1,1]));
			await log1.join(log2);
			await log4.join(await log1.getHeads(), { length: 2 });
			expect(log4.length).equal(2);
			expect((await log4.getTails()).length).equal(2);
			expect((await log4.getTails())[0].hash).equal(
				(await log4.toArray())[0].hash
			);
			expect((await log4.getTails())[1].hash).equal(
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
			expect((await log4.getTails()).length).equal(3);

			let sortedKeys = [signKey.publicKey.bytes, signKey2.publicKey.bytes, signKey3.publicKey.bytes].sort()
			expect((await log4.getTails())[0].meta.clock.id).to.deep.equal(
				sortedKeys[2]
			);
			expect((await log4.getTails())[1].meta.clock.id).to.deep.equal(
				sortedKeys[1]
			);
			expect((await log4.getTails())[2].meta.clock.id).to.deep.equal(
				sortedKeys[0]
			);
		}); */
	});

	describe("order", () => {
		it("can get oldest", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			await log1.append(new Uint8Array([0, 2]));
			expect((await log1.entryIndex.getOldest())!.hash).equal(
				(await log1.toArray())[0].hash,
			);
		});

		it("can get newest", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			await log1.append(new Uint8Array([0, 2]));
			expect((await log1.entryIndex.getNewest())!.hash).equal(
				(await log1.toArray())[2].hash,
			);
		});

		it("can get before", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			await log1.append(new Uint8Array([0, 2]));

			const entry = await log1.append(new Uint8Array([0, 3]));
			const before = await log1.entryIndex.getBefore(entry.entry);
			expect(before!.hash).equal((await log1.toArray())[2].hash);
		});

		it("can get after", async () => {
			await log1.append(new Uint8Array([0, 0]));
			await log1.append(new Uint8Array([0, 1]));
			const entry = await log1.append(new Uint8Array([0, 2]));
			await log1.append(new Uint8Array([0, 3]));
			const after = await log1.entryIndex.getAfter(entry.entry);
			expect(after!.hash).equal((await log1.toArray())[3].hash);
		});
	});
});
