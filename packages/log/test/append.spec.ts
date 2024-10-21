import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import { EntryType } from "../src/entry-type.js";
import { Log } from "../src/log.js";

describe("append", function () {
	let store: BlockStore;
	let signKey: Ed25519Keypair;

	const blockExists = async (hash: string): Promise<boolean> => {
		try {
			return !!(await store.get(hash, { remote: { timeout: 3000 } }));
		} catch (error) {
			return false;
		}
	};

	before(async () => {
		store = new AnyBlockStore();
		signKey = await Ed25519Keypair.create();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	describe("append one", () => {
		let log: Log<Uint8Array>;

		beforeEach(async () => {
			log = new Log();
			await log.open(store, signKey);
			await log.append(new Uint8Array([1]));
		});

		it("added the correct amount of items", () => {
			expect(log.length).equal(1);
		});

		it("added the correct values", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.payload.getValue()).to.deep.equal(new Uint8Array([1]));
			});
		});

		it("added the correct amount of next pointers", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.meta.next.length).equal(0);
			});
		});

		it("has the correct heads", async () => {
			for (const head of await log.getHeads().all()) {
				expect(head.hash).to.deep.equal((await log.toArray())[0].hash);
			}
		});

		it("updated the clocks correctly", async () => {
			(await log.toArray()).forEach((entry) => {
				expect(entry.meta.clock.id).to.deep.equal(signKey.publicKey.bytes);
				expect(entry.meta.clock.timestamp.logical).equal(0);
			});
		});
	});

	describe("reset", () => {
		it("append", async () => {
			const log = new Log();
			await log.open(store, signKey);
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2 } = await log.append(new Uint8Array([2]));
			expect(await blockExists(e1.hash)).to.be.true;
			expect(await blockExists(e2.hash)).to.be.true;
			const { entry: e3 } = await log.append(new Uint8Array([3]), {
				meta: { type: EntryType.CUT },
			});
			expect((await log.entryIndex.getHasNext(e1.hash).all()).length).equal(0);
			expect(await blockExists(e1.hash)).to.be.false;
			expect(await blockExists(e2.hash)).to.be.false;
			expect(await blockExists(e3.hash)).to.be.true;
		});
	});

	describe("append 100 items to a log", () => {
		const amount = 100;

		let log: Log<Uint8Array>;

		before(async () => {
			// Do sign function really need to returnr publcikey
			log = new Log();
			await log.open(store, signKey);
			let prev: any = undefined;
			for (let i = 0; i < amount; i++) {
				prev = (
					await log.append(new TextEncoder().encode("hello" + i), {
						meta: {
							next: prev ? [prev] : undefined,
						},
					})
				).entry;

				// Make sure the log has the right heads after each append
				const values = await log.toArray();
				const heads = await log.getHeads().all();
				expect(heads.length).equal(1);
				expect(heads[0].hash).equal(values[values.length - 1].hash);
			}
		});

		it("added the correct amount of items", () => {
			expect(log.length).equal(amount);
		});

		it("added the correct values", async () => {
			(await log.toArray()).forEach((entry, index) => {
				expect(entry.payload.getValue()).to.deep.equal(
					new TextEncoder().encode("hello" + index),
				);
			});
		});

		it("updated the clocks correctly", async () => {
			for (const [index, entry] of (await log.toArray()).entries()) {
				if (index > 0) {
					expect(
						entry.meta.clock.timestamp.compare(
							(await log.toArray())[index - 1].meta.clock.timestamp,
						),
					).greaterThan(0);
				}
				expect(entry.meta.clock.id).to.deep.equal(signKey.publicKey.bytes);
			}
		});

		/*    it('added the correct amount of refs pointers', async () => {
	   log.values.forEach((entry, index) => {
		 expect(entry.refs.length).equal(index > 0 ? Math.ceil(Math.log2(Math.min(nextPointerAmount, index))) : 0)
	   })
	 }) */
	});
});
