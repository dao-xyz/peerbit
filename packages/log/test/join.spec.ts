import { Ed25519Keypair } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import assert from "assert";
import { expect } from "chai";
import sinon from "sinon";
import { compare } from "uint8arrays";
import { createEntry } from "../src/entry-create.js";
import { EntryType } from "../src/entry-type.js";
import { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

const last = (arr: any[]) => {
	return arr[arr.length - 1];
};

const checkedStorage = async (log: Log<any>) => {
	for (const value of await log.toArray()) {
		expect(await log.blocks.has(value.hash)).to.be.true;
	}
};

describe("join", function () {
	let session: TestSession;

	let signKey: Ed25519Keypair,
		signKey2: Ed25519Keypair,
		signKey3: Ed25519Keypair,
		signKey4: Ed25519Keypair;
	before(async () => {
		const keys: Ed25519Keypair[] = [
			await Ed25519Keypair.create(),
			await Ed25519Keypair.create(),
			await Ed25519Keypair.create(),
			await Ed25519Keypair.create(),
		];
		keys.sort((a, b) => {
			return compare(a.publicKey.publicKey, b.publicKey.publicKey);
		});
		signKey = keys[0];
		signKey2 = keys[1];
		signKey3 = keys[2];
		signKey4 = keys[3];
		session = await TestSession.connected(3);
		await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
		await session.peers[2].services.blocks.waitFor(session.peers[0].peerId);
		await session.peers[2].services.blocks.waitFor(session.peers[1].peerId);
	});

	after(async () => {
		await session.stop();
	});

	describe("join", () => {
		let log1: Log<Uint8Array>,
			log2: Log<Uint8Array>,
			log3: Log<Uint8Array>,
			log4: Log<Uint8Array>;

		beforeEach(async () => {
			const logOptions = {};
			log1 = new Log<Uint8Array>();
			await log1.open(session.peers[0].services.blocks, signKey, logOptions);
			log2 = new Log<Uint8Array>();
			await log2.open(session.peers[1].services.blocks, signKey2, {
				...logOptions,
			});
			log3 = new Log<Uint8Array>();
			await log3.open(session.peers[2].services.blocks, signKey3, logOptions);
			log4 = new Log<Uint8Array>();
			await log4.open(
				session.peers[2].services.blocks, // [2] because we cannot create more than 3 peers when running tests in CI
				signKey4,
				logOptions,
			);
		});

		it("joins logs", async () => {
			const items1: Entry<Uint8Array>[] = [];
			const items2: Entry<Uint8Array>[] = [];
			const items3: Entry<Uint8Array>[] = [];
			const amount = 40;

			for (let i = 1; i <= amount; i++) {
				const prev1 = last(items1);
				const prev2 = last(items2);
				const prev3 = last(items3);
				const n1 = await createEntry({
					store: session.peers[0].services.blocks,
					identity: {
						...signKey,
						sign: (data: Uint8Array) => signKey.sign(data),
					},
					meta: {
						gidSeed: Buffer.from("X" + i),
						next: prev1 ? [prev1] : undefined,
					},
					data: new Uint8Array([0, i]),
				});
				const n2 = await createEntry({
					store: session.peers[0].services.blocks,
					identity: {
						...signKey2,
						sign: (data: Uint8Array) => signKey2.sign(data),
					},
					meta: {
						next: prev2 ? [prev2, n1] : [n1],
					},
					data: new Uint8Array([1, i]),
				});
				const n3 = await createEntry({
					store: session.peers[1].services.blocks,
					identity: {
						...signKey3,
						sign: (data: Uint8Array) => signKey3.sign(data),
					},
					data: new Uint8Array([2, i]),
					meta: {
						next: prev3 ? [prev3, n1, n2] : [n1, n2],
					},
				});

				items1.push(n1);
				items2.push(n2);
				items3.push(n3);
			}

			// Here we're creating a log from entries signed by A and B
			// but we accept entries from C too
			const logA = await Log.fromEntry(
				session.peers[0].services.blocks,
				signKey3,
				last(items2),
				{ timeout: 3000 },
			);

			// Here we're creating a log from entries signed by peer A, B and C
			// "logA" accepts entries from peer C so we can join logs A and B

			const logB = await Log.fromEntry(
				session.peers[1].services.blocks,
				signKey3,
				last(items3),
				{ timeout: 3000 },
			);
			expect(logA.length).equal(items2.length + items1.length);
			expect(logB.length).equal(items3.length + items2.length + items1.length);

			expect(amount).equal(items3.length);
			expect(amount).equal(items2.length);
			expect(amount).equal(items1.length);

			expect((await logA.getHeads().all()).length).equal(1);
			await logA.join(await logB.getHeads(true).all());

			expect(logA.length).equal(items3.length + items2.length + items1.length);
			// The last Entry<T>, 'entryC100', should be the only head
			// (it points to entryB100, entryB100 and entryC99)
			expect((await logA.getHeads().all()).length).equal(1);

			await checkedStorage(logA);
			await checkedStorage(logB);
		});

		it("will update cache", async () => {
			// Expect log2 to use memory cache
			await log1.append(new Uint8Array([0, 1]));
			await log2.join(await log1.getHeads(true).all());
			await log2.load();
			expect(await log2.getHeads().all()).to.have.length(1);
			expect(await log2.length).equal(1);
		});

		it("will no-refetch blocks when already joined by has", async () => {
			await log1.append(new Uint8Array([0, 1]));
			const blockGet = log2["_storage"].get.bind(log2.blocks);
			let fetched: string[] = [];
			log2["_storage"].get = (cid: any, options: any) => {
				fetched.push(cid);
				return blockGet(cid, options);
			};

			await log2.join((await log1.getHeads(true).all()).map((x) => x.hash));
			expect(log2.length).equal(1);
			expect(fetched).to.have.length(1);

			await log2.join((await log1.getHeads(true).all()).map((x) => x.hash));
			expect(log2.length).equal(1);
			expect(fetched).to.have.length(1);
		});

		it("will no-refetch blocks when already joined by shallow entry", async () => {
			await log1.append(new Uint8Array([0, 1]));
			const blockGet = log2["_storage"].get.bind(log2.blocks);
			let fetched: string[] = [];
			log2["_storage"].get = (cid: any, options: any) => {
				fetched.push(cid);
				return blockGet(cid, options);
			};

			await log2.join(
				(await log1.getHeads(true).all()).map((x) => x.toShallow(true)),
			);
			expect(log2.length).equal(1);
			expect(fetched).to.have.length(1);

			await log2.join(
				(await log1.getHeads(true).all()).map((x) => x.toShallow(true)),
			);
			expect(log2.length).equal(1);
			expect(fetched).to.have.length(1);
		});

		it("joins only unique items", async () => {
			await log1.append(new Uint8Array([0, 1]));
			let b0 = await log2.append(new Uint8Array([1, 0]));
			await log1.append(new Uint8Array([0, 2]));
			let b1 = await log2.append(new Uint8Array([1, 1]));
			let joinedFirst: { head: boolean; entry: Entry<any> }[] = [];
			await log1.join(await log2.getHeads(true).all(), {
				onChange: (change) => {
					joinedFirst.push(...change.added);
				},
			});

			expect(joinedFirst).to.have.length(2);
			expect(joinedFirst.map((x) => x.entry.hash)).to.deep.equal([
				b0.entry.hash,
				b1.entry.hash,
			]);
			expect(joinedFirst.map((x) => x.head)).to.deep.equal([false, true]);

			let joinedSecond: { head: boolean; entry: Entry<any> }[] = [];
			await log1.join(await log2.getHeads(true).all(), {
				onChange: (change) => {
					joinedSecond.push(...change.added);
				},
			});
			expect(joinedSecond).to.have.length(0);

			const expectedData = [
				new Uint8Array([0, 1]),
				new Uint8Array([1, 0]),
				new Uint8Array([0, 2]),
				new Uint8Array([1, 1]),
			];

			expect(log1.length).equal(4);
			expect(
				(await log1.toArray()).map(
					(e) => new Uint8Array(e.payload.getValue(log1.encoding)),
				),
			).to.deep.equal(expectedData);

			const item = last(await log1.toArray());
			expect(item.next.length).equal(1);
			expect((await log1.getHeads().all()).length).equal(2);
		});

		it("canAppend bottom first", async () => {
			await log1.append(new Uint8Array([1]));
			await log1.append(new Uint8Array([2]));

			let canAppendCheckedData: Uint8Array[] = [];

			const canAppend2 = log2["_canAppend"]!.bind(log2);
			log2["_canAppend"] = async (entry) => {
				const result = await canAppend2(entry);
				canAppendCheckedData.push(entry.payload.getValue());
				return result;
			};

			expect(await log1.getHeads().all()).to.have.length(1);
			await log2.join(await log1.getHeads(true).all());
			expect(canAppendCheckedData).to.deep.equal([
				new Uint8Array([1]),
				new Uint8Array([2]),
			]);
		});

		describe("cut", () => {
			let fetchEvents: number;
			let fetchHashes: Set<string>;
			let fromMultihash: any;
			before(() => {
				fetchEvents = 0;
				fetchHashes = new Set();
				fromMultihash = Entry.fromMultihash;

				// TODO monkeypatching might lead to sideeffects in other tests!
				Entry.fromMultihash = (s, h, o) => {
					fetchHashes.add(h);
					fetchEvents += 1;
					return fromMultihash(s, h, o);
				};
			});
			after(() => {
				fetchHashes = new Set();
				fetchEvents = 0;
				Entry.fromMultihash = fromMultihash;
			});

			it("joins cut", async () => {
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
				const { entry: b1 } = await log2.append(new Uint8Array([1, 0]), {
					meta: {
						next: [a1],
						type: EntryType.CUT,
					},
				});
				const { entry: a2 } = await log1.append(new Uint8Array([0, 2]));
				await log1.join(await log2.getHeads(true).all());
				expect((await log1.toArray()).map((e) => e.hash)).to.deep.equal([
					a1.hash,
					b1.hash,
					a2.hash,
				]);
				const { entry: b2 } = await log2.append(new Uint8Array([1, 0]), {
					meta: {
						next: [a2],
						type: EntryType.CUT,
					},
				});
				await log1.join(await log2.getHeads(true).all());
				expect((await log1.getHeads().all()).map((e) => e.hash)).to.deep.equal([
					b1.hash,
					b2.hash,
				]);
				expect((await log1.toArray()).map((e) => e.hash)).to.deep.equal([
					b1.hash,
					b2.hash,
				]);
			});

			it("ignores entry after cut", async () => {
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
				await log1.append(new Uint8Array([1, 0]), {
					meta: {
						next: [a1],
						type: EntryType.CUT,
					},
				});
				expect(log1.length).equal(1);
				await log1.join([a1]);
				expect(log1.length).equal(1);
			});

			it("concurrent append after cut", async () => {
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
				const b1 = await createEntry({
					data: new Uint8Array([1, 0]),
					meta: {
						type: EntryType.CUT,
						next: [a1],
					},
					identity: log1.identity,
					store: log1.blocks,
				});

				const b2 = await createEntry({
					data: new Uint8Array([1, 1]),
					meta: {
						type: EntryType.APPEND,
						next: [a1],
					},
					identity: log1.identity,
					store: log1.blocks,
				});

				// We need to store a1 somewhere else, becuse log1 will temporarely delete the block since due to the merge order
				// TODO make this work even though there is not a third party helping
				await log2.blocks.get(a1.hash, { remote: { replicate: true } });
				expect(await log2.blocks.get(a1.hash)).to.exist;
				await log1.join([b1, b2]);
				expect((await log1.toArray()).map((e) => e.hash)).to.deep.equal([
					b1.hash,
					b2.hash,
					// a1 is missing here, this is expected
				]);
			});

			it("concurrent append before cut", async () => {
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));

				const b2 = await createEntry({
					data: new Uint8Array([1, 1]),
					meta: {
						type: EntryType.APPEND,
						next: [a1],
					},
					identity: log1.identity,
					store: log1.blocks,
				});

				const b1 = await createEntry({
					data: new Uint8Array([1, 0]),
					meta: {
						type: EntryType.CUT,
						next: [a1],
					},
					identity: log1.identity,
					store: log1.blocks,
				});

				// We need to store a1 somewhere else, becuse log1 will temporarely delete the block since due to the merge order
				// TODO make this work even though there is not a third party helping
				await log2.blocks.get(a1.hash, { remote: { replicate: true } });
				expect(await log2.blocks.get(a1.hash)).to.exist;
				await log1.join([b1, b2]);
				expect((await log1.toArray()).map((e) => e.hash)).to.have.members([
					b1.hash,
					b2.hash,
					// a1 is missing here, this is expected
				]);
			});

			it("will not reset if joining conflicting (reversed)", async () => {
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
				const b1 = await createEntry({
					data: new Uint8Array([1, 0]),
					meta: {
						type: EntryType.APPEND,
						next: [a1],
					},
					identity: log1.identity,
					store: log1.blocks,
				});
				const b2 = await createEntry({
					data: new Uint8Array([1, 1]),
					meta: {
						type: EntryType.CUT,
						next: [a1],
					},
					identity: log1.identity,
					store: log1.blocks,
				});
				await log1.join([b1, b2]);
				expect((await log1.toArray()).map((e) => e.hash)).to.deep.equal([
					a1.hash,
					b1.hash,
					b2.hash,
				]);
			});

			it("joining multiple resets", async () => {
				const { entry: a1 } = await log2.append(new Uint8Array([0, 1]));
				const { entry: b1 } = await log2.append(new Uint8Array([1, 0]), {
					meta: {
						next: [a1],
						type: EntryType.CUT,
					},
				});
				const { entry: b2 } = await log2.append(new Uint8Array([1, 1]), {
					meta: {
						next: [a1],
						type: EntryType.CUT,
					},
				});

				expect((await log2.getHeads().all()).map((x) => x.hash)).to.deep.equal([
					b1.hash,
					b2.hash,
				]);
				fetchEvents = 0;
				await log1.join(await log2.getHeads(true).all());
				expect(fetchEvents).equal(0); // will not fetch a1 since b1 and b2 is CUT (no point iterating to nexts)
				expect((await log1.toArray()).map((e) => e.hash)).to.deep.equal([
					b1.hash,
					b2.hash,
				]);
			});
		});

		it("joins heads", async () => {
			const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
			const { entry: b1 } = await log2.append(new Uint8Array([1, 0]), {
				meta: { next: [a1] },
			});

			expect(log1.length).equal(1);
			expect(log2.length).equal(2);

			await log1.join(await log2.getHeads(true).all());
			const expectedData = [new Uint8Array([0, 1]), new Uint8Array([1, 0])];
			expect(log1.length).equal(2);
			expect(
				(await log1.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			).to.deep.equal(expectedData);

			const item = last(await log1.toArray());
			expect(item.next.length).equal(1);
			expect((await log1.getHeads().all()).map((x) => x.hash)).to.deep.equal([
				b1.hash,
			]);
		});

		it("joins unique concurrently", async () => {
			let expectedData: Uint8Array[] = [];
			let len = 2;
			let entries: Entry<any>[] = [];
			for (let i = 0; i < len; i++) {
				expectedData.push(new Uint8Array([i]));
				entries.push((await log2.append(new Uint8Array([i]))).entry);
			}
			let promises: Promise<any>[] = [];
			for (let i = 0; i < len; i++) {
				promises.push(log1.join([entries[i]]));
			}

			await Promise.all(promises);

			expect(log1.length).equal(len);
			expect(
				(await log1.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			).to.deep.equal(expectedData);

			const item = last(await log1.toArray());
			let allHeads = await log1.getHeads().all();
			expect(allHeads.length).equal(1);
			expect(item.next.length).equal(1);
			expect(log1.length).equal(len);
		});

		it("joins same concurrently", async () => {
			let joinCount = 1e3;
			let entry = (await log2.append(new Uint8Array([0]))).entry;
			let promises: Promise<any>[] = [];

			const fn = sinon.spy(log1.entryIndex, "getHeads");

			for (let i = 0; i < joinCount; i++) {
				promises.push(log1.join([entry]));
			}

			await Promise.all(promises);
			const arr = await log1.toArray();
			expect(arr.length).equal(1);
			expect(log1.length).equal(1);
			expect(fn.callCount).to.equal(1);
		});

		it("joins same sequence of entries concurrently", async () => {
			let entry1 = (await log2.append(new Uint8Array([0]))).entry;
			let entry2 = (await log2.append(new Uint8Array([0]))).entry;

			let promises: Promise<any>[] = [];

			const getFn = sinon.spy(log1.blocks, "get");

			promises.push(log1.join([entry1]));
			promises.push(log1.join([entry2]));

			await Promise.all(promises);
			const arr = await log1.toArray();
			expect(arr.length).equal(2);
			expect(log1.length).equal(2);
			expect(getFn.callCount).to.equal(0);
		});

		it("joins with extra references", async () => {
			const e1 = await log1.append(new Uint8Array([0, 1]));
			const e2 = await log1.append(new Uint8Array([0, 2]));
			const e3 = await log1.append(new Uint8Array([0, 3]));
			expect(log1.length).equal(3);
			await log2.join([e1.entry, e2.entry, e3.entry]);
			const expectedData = [
				new Uint8Array([0, 1]),
				new Uint8Array([0, 2]),
				new Uint8Array([0, 3]),
			];
			expect(log2.length).equal(3);
			expect(
				(await log1.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			).to.deep.equal(expectedData);
			const item = last(await log1.toArray());
			expect(item.next.length).equal(1);
			expect((await log1.getHeads().all()).length).equal(1);
		});

		it("joins logs two ways", async () => {
			const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
			const { entry: b1 } = await log2.append(new Uint8Array([1, 0]));
			const { entry: a2 } = await log1.append(new Uint8Array([0, 2]));
			const { entry: b2 } = await log2.append(new Uint8Array([1, 1]));
			await log1.join(await log2.getHeads(true).all());
			await log2.join(await log1.getHeads(true).all());

			const expectedData = [
				new Uint8Array([0, 1]),
				new Uint8Array([1, 0]),
				new Uint8Array([0, 2]),
				new Uint8Array([1, 1]),
			];

			expect(await log1.getHeads(true).all()).to.have.members([a2, b2]);
			expect(await log2.getHeads(true).all()).to.have.members([a2, b2]);
			expect(a2.meta.next).to.have.members([a1.hash]);
			expect(b2.meta.next).to.have.members([b1.hash]);

			expect((await log1.toArray()).map((e) => e.hash)).to.deep.equal(
				(await log2.toArray()).map((e) => e.hash),
			);
			expect(
				(await log1.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			).to.deep.equal(expectedData);
			expect(
				(await log2.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			).to.deep.equal(expectedData);
		});

		it("joins logs twice", async () => {
			const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
			const { entry: b1 } = await log2.append(new Uint8Array([1, 0]));
			await log2.join(await log1.getHeads(true).all());
			expect(log2.length).equal(2);
			expect((await log2.getHeads().all()).map((x) => x.hash)).to.have.members([
				a1.hash,
				b1.hash,
			]);

			const { entry: a2 } = await log1.append(new Uint8Array([0, 2]));
			const { entry: b2 } = await log2.append(new Uint8Array([1, 1]));
			await log2.join(await log1.getHeads(true).all());

			const expectedData = [
				new Uint8Array([0, 1]),
				new Uint8Array([1, 0]),
				new Uint8Array([0, 2]),
				new Uint8Array([1, 1]),
			];

			expect(
				(await log2.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			).to.deep.equal(expectedData);
			expect(log2.length).equal(4);

			expect((await log1.getHeads().all()).map((x) => x.hash)).to.have.members([
				a2.hash,
			]);
			expect((await log2.getHeads().all()).map((x) => x.hash)).to.have.members([
				a2.hash,
				b2.hash,
			]);
		});

		it("joins 2 logs two ways", async () => {
			await log1.append(new Uint8Array([0, 1]));
			await log2.append(new Uint8Array([1, 0]));
			await log2.join(await log1.getHeads(true).all());
			await log1.join(await log2.getHeads(true).all());
			const { entry: a2 } = await log1.append(new Uint8Array([0, 2]));
			const { entry: b2 } = await log2.append(new Uint8Array([1, 1]));
			await log2.join(await log1.getHeads(true).all());

			const expectedData = [
				new Uint8Array([0, 1]),
				new Uint8Array([1, 0]),
				new Uint8Array([0, 2]),
				new Uint8Array([1, 1]),
			];

			expect(log2.length).equal(4);
			assert.deepStrictEqual(
				(await log2.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
				expectedData,
			);

			expect(
				(await log1.getHeads(true).all()).map((x) => x.hash),
			).to.have.members([a2.hash]);
			expect((await log2.getHeads().all()).map((x) => x.hash)).to.have.members([
				a2.hash,
				b2.hash,
			]);
		});

		it("joins 2 logs two ways and has the right heads at every step", async () => {
			await log1.append(new Uint8Array([0, 1]));
			expect((await log1.getHeads().all()).length).equal(1);
			expect(
				(await log1.getHeads(true).all())[0].payload.getValue(),
			).to.deep.equal(new Uint8Array([0, 1]));

			await log2.append(new Uint8Array([1, 0]));
			expect((await log2.getHeads().all()).length).equal(1);
			expect(
				(await log2.getHeads(true).all())[0].payload.getValue(),
			).to.deep.equal(new Uint8Array([1, 0]));

			await log2.join(await log1.getHeads(true).all());
			expect(
				(await log2.getHeads(true).all()).map((x) => x.payload.getValue()),
			).to.deep.equal([new Uint8Array([0, 1]), new Uint8Array([1, 0])]);

			await log1.join(await log2.getHeads(true).all());
			expect((await log1.getHeads().all()).length).equal(2);
			expect(
				(await log1.getHeads(true).all())[1].payload.getValue(),
			).to.deep.equal(new Uint8Array([1, 0]));
			expect(
				(await log1.getHeads(true).all())[0].payload.getValue(),
			).to.deep.equal(new Uint8Array([0, 1]));

			await log1.append(new Uint8Array([0, 2]));
			expect((await log1.getHeads().all()).length).equal(1);
			expect(
				(await log1.getHeads(true).all())[0].payload.getValue(),
			).to.deep.equal(new Uint8Array([0, 2]));

			await log2.append(new Uint8Array([1, 1]));
			expect((await log2.getHeads().all()).length).equal(1);
			expect(
				(await log2.getHeads(true).all())[0].payload.getValue(),
			).to.deep.equal(new Uint8Array([1, 1]));

			await log2.join(await log1.getHeads(true).all());
			expect(
				(await log2.getHeads(true).all()).map((x) => x.payload.getValue()),
			).to.deep.equal([new Uint8Array([0, 2]), new Uint8Array([1, 1])]);
		});

		it("joins 4 logs to one", async () => {
			// order determined by identity's publicKey
			await log1.append(new Uint8Array([0, 1]));
			await log2.append(new Uint8Array([1, 0]));
			await log3.append(new Uint8Array([2, 0]));
			await log4.append(new Uint8Array([3, 0]));
			const { entry: a2 } = await log1.append(new Uint8Array([0, 2]));
			const { entry: b2 } = await log2.append(new Uint8Array([1, 1]));
			const { entry: c2 } = await log3.append(new Uint8Array([2, 1]));
			const { entry: d2 } = await log4.append(new Uint8Array([3, 1]));
			await log1.join(await log2.getHeads(true).all());
			await log1.join(await log3.getHeads(true).all());
			await log1.join(await log4.getHeads(true).all());

			const expectedData = [
				new Uint8Array([0, 1]),
				new Uint8Array([1, 0]),
				new Uint8Array([2, 0]),
				new Uint8Array([3, 0]),
				new Uint8Array([0, 2]),
				new Uint8Array([1, 1]),
				new Uint8Array([2, 1]),
				new Uint8Array([3, 1]),
			];

			expect(log1.length).equal(8);
			expect(
				(await log1.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			).to.deep.equal(expectedData);

			expect((await log1.getHeads().all()).map((x) => x.hash)).to.have.members(
				[a2, b2, c2, d2].map((x) => x.hash),
			);
		});

		it("joins 4 logs to one is commutative", async () => {
			await log1.append(new Uint8Array([0, 1]));
			await log1.append(new Uint8Array([0, 2]));
			await log2.append(new Uint8Array([1, 0]));
			await log2.append(new Uint8Array([1, 1]));
			await log3.append(new Uint8Array([2, 0]));
			await log3.append(new Uint8Array([2, 1]));
			await log4.append(new Uint8Array([3, 0]));
			await log4.append(new Uint8Array([3, 1]));
			await log1.join(await log2.getHeads(true).all());
			await log1.join(await log3.getHeads(true).all());
			await log1.join(await log4.getHeads(true).all());
			await log2.join(await log1.getHeads(true).all());
			await log2.join(await log3.getHeads(true).all());
			await log2.join(await log4.getHeads(true).all());

			expect(log1.length).equal(8);
			assert.deepStrictEqual(
				(await log1.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
				(await log2.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
			);
		});

		it("joins logs and updates clocks", async () => {
			const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
			const { entry: b1 } = await log2.append(new Uint8Array([1, 0]));
			await log2.join(await log1.getHeads(true).all());
			const { entry: a2 } = await log1.append(new Uint8Array([0, 2]));
			const { entry: b2 } = await log2.append(new Uint8Array([1, 1]));

			expect(a2.meta.clock.id).to.deep.equal(signKey.publicKey.bytes);
			expect(b2.meta.clock.id).to.deep.equal(signKey2.publicKey.bytes);
			expect(
				a2.meta.clock.timestamp.compare(a1.meta.clock.timestamp),
			).greaterThan(0);
			expect(
				b2.meta.clock.timestamp.compare(b1.meta.clock.timestamp),
			).greaterThan(0);

			await log3.join(await log1.getHeads(true).all());

			await log3.append(new Uint8Array([2, 0]));
			const { entry: c2 } = await log3.append(new Uint8Array([2, 1]));
			await log1.join(await log3.getHeads(true).all());
			await log1.join(await log2.getHeads(true).all());
			await log4.append(new Uint8Array([3, 0]));
			const { entry: d2 } = await log4.append(new Uint8Array([3, 1]));
			await log4.join(await log2.getHeads(true).all());
			await log4.join(await log1.getHeads(true).all());
			await log4.join(await log3.getHeads(true).all());
			const { entry: d3 } = await log4.append(new Uint8Array([3, 2]));
			expect(d3.meta.gid).equal(
				[
					a1.meta.gid,
					a2.meta.gid,
					b2.meta.gid,
					c2.meta.gid,
					d2.meta.gid,
				].sort()[0],
			);
			await log4.append(new Uint8Array([3, 3]));
			await log1.join(await log4.getHeads(true).all());
			await log4.join(await log1.getHeads(true).all());
			const { entry: d5 } = await log4.append(new Uint8Array([3, 4]));
			expect(d5.meta.gid).equal(
				[
					a1.meta.gid,
					a2.meta.gid,
					b2.meta.gid,
					c2.meta.gid,
					d2.meta.gid,
					d3.meta.gid,
					d5.meta.gid,
				].sort()[0],
			);

			const { entry: a5 } = await log1.append(new Uint8Array([0, 4]));
			expect(a5.meta.gid).equal(
				[
					a1.meta.gid,
					a2.meta.gid,
					b2.meta.gid,
					c2.meta.gid,
					d2.meta.gid,
					d3.meta.gid,
					d5.meta.gid,
				].sort()[0],
			);

			await log4.join(await log1.getHeads(true).all());
			const { entry: d6 } = await log4.append(new Uint8Array([3, 5]));
			expect(d5.meta.gid).equal(a5.meta.gid);
			expect(d6.meta.gid).equal(a5.meta.gid);

			const expectedData = [
				{
					payload: new Uint8Array([0, 1]),
					gid: a1.meta.gid,
				},
				{
					payload: new Uint8Array([1, 0]),
					gid: b1.meta.gid,
				},

				{
					payload: new Uint8Array([0, 2]),
					gid: a2.meta.gid,
				},
				{
					payload: new Uint8Array([1, 1]),
					gid: b2.meta.gid,
				},
				{
					payload: new Uint8Array([2, 0]),
					gid: a1.meta.gid,
				},
				{
					payload: new Uint8Array([2, 1]),
					gid: c2.meta.gid,
				},
				{
					payload: new Uint8Array([3, 0]),
					gid: d2.meta.gid,
				},
				{
					payload: new Uint8Array([3, 1]),
					gid: d2.meta.gid,
				},
				{
					payload: new Uint8Array([3, 2]),
					gid: d3.meta.gid,
				},
				{
					payload: new Uint8Array([3, 3]),
					gid: d3.meta.gid,
				},
				{
					payload: new Uint8Array([3, 4]),
					gid: d5.meta.gid,
				},
				{
					payload: new Uint8Array([0, 4]),
					gid: a5.meta.gid,
				},
				{
					payload: new Uint8Array([3, 5]),
					gid: d6.meta.gid,
				},
			];

			const transformed = (await log4.toArray()).map((e) => {
				return {
					payload: new Uint8Array(e.payload.getValue()),
					gid: e.meta.gid,
				};
			});

			expect(log4.length).equal(13);
			expect(transformed).to.deep.equal(expectedData);
		});

		it("joins logs from 4 logs", async () => {
			const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
			await log1.join(await log2.getHeads(true).all());
			// @ts-ignore unused
			const { entry: b1 } = await log2.append(new Uint8Array([1, 0]));
			await log2.join(await log1.getHeads(true).all());
			const { entry: a2 } = await log1.append(new Uint8Array([0, 2]));
			await log2.append(new Uint8Array([1, 1]));

			await log1.join(await log3.getHeads(true).all());
			// Sometimes failes because of clock ids are random TODO Fix
			expect(
				(await log1.getHeads().all())[(await log1.getHeads().all()).length - 1]
					.meta.gid,
			).equal(a1.meta.gid);
			expect(a2.meta.clock.id).to.deep.equal(signKey.publicKey.bytes);
			expect(
				a2.meta.clock.timestamp.compare(a1.meta.clock.timestamp),
			).greaterThan(0);

			await log3.join(await log1.getHeads(true).all());
			expect(
				(await log3.getHeads().all())[(await log3.getHeads().all()).length - 1]
					.meta.gid,
			).equal(a1.meta.gid); // because longest

			await log3.append(new Uint8Array([2, 0]));
			await log3.append(new Uint8Array([2, 1]));
			await log1.join(await log3.getHeads(true).all());
			await log1.join(await log2.getHeads(true).all());
			await log4.append(new Uint8Array([3, 0]));
			await log4.append(new Uint8Array([3, 1]));
			await log4.join(await log2.getHeads(true).all());
			await log4.join(await log1.getHeads(true).all());
			await log4.join(await log3.getHeads(true).all());
			await log4.append(new Uint8Array([3, 2]));
			const { entry: d4 } = await log4.append(new Uint8Array([3, 3]));

			expect(d4.meta.clock.id).to.deep.equal(signKey4.publicKey.bytes);

			const expectedData = [
				new Uint8Array([0, 1]),
				new Uint8Array([1, 0]),
				new Uint8Array([0, 2]),
				new Uint8Array([1, 1]),
				new Uint8Array([2, 0]),
				new Uint8Array([2, 1]),
				new Uint8Array([3, 0]),
				new Uint8Array([3, 1]),
				new Uint8Array([3, 2]),
				new Uint8Array([3, 3]),
			];

			expect(log4.length).equal(10);
			assert.deepStrictEqual(
				(await log4.toArray()).map((e) => new Uint8Array(e.payload.getValue())),
				expectedData,
			);
		});

		describe("entry-with-references", () => {
			let fetchCounter = 0;
			let joinEntryCounter = 0;
			let fromMultihashOrg: any;
			before(() => {
				fromMultihashOrg = Entry.fromMultihash;
				Entry.fromMultihash = (s, h, o) => {
					fetchCounter += 1;
					return fromMultihashOrg(s, h, o);
				};
			});
			after(() => {
				Entry.fromMultihash = fromMultihashOrg;
			});

			beforeEach(() => {
				const joinEntryFn = log2["joinRecursively"].bind(log2);
				log2["joinRecursively"] = (e: any, o: any) => {
					joinEntryCounter += 1;
					return joinEntryFn(e, o);
				};
				fetchCounter = 0;
				joinEntryCounter = 0;
			});

			it("joins with references", async () => {
				const { entry: a1 } = await log1.append(new Uint8Array([0, 1]));
				const { entry: a2 } = await log1.append(new Uint8Array([0, 2]), {
					meta: { next: [a1] },
				});
				await log2.join([{ entry: a2, references: [a1] }]);
				expect(log2.length).equal(2);
				expect(fetchCounter).equal(0); // no fetches since all entries where passed
				expect(joinEntryCounter).equal(2);
			});
		});

		// TODO move this into the prune test file
		describe("join and prune", () => {
			beforeEach(async () => {
				await log1.append(new Uint8Array([0, 1]));
				await log2.append(new Uint8Array([1, 0]));
				await log1.append(new Uint8Array([0, 2]));
				await log2.append(new Uint8Array([1, 1]));
			});

			it("joins only specified amount of entries - one entry", async () => {
				const log2Heads = await log2.getHeads(true).all();
				await log1.join(log2Heads);

				await log1.trim({ type: "length", to: 1 });

				const expectedData = [new Uint8Array([1, 1])];
				const lastEntry = last(await log1.toArray());
				expect(log1.length).equal(1);
				assert.deepStrictEqual(
					(await log1.toArray()).map(
						(e) => new Uint8Array(e.payload.getValue()),
					),
					expectedData,
				);
				expect(lastEntry.next.length).equal(1);
			});

			it("joins only specified amount of entries - two entries", async () => {
				await log1.join(await log2.getHeads(true).all());
				await log1.trim({ type: "length", to: 2 });

				const expectedData = [new Uint8Array([0, 2]), new Uint8Array([1, 1])];
				const lastEntry = last(await log1.toArray());

				expect(log1.length).equal(2);
				expect(
					(await log1.toArray()).map(
						(e) => new Uint8Array(e.payload.getValue()),
					),
				).to.deep.equal(expectedData);
				expect(lastEntry.next.length).equal(1);
			});

			it("joins only specified amount of entries - three entries", async () => {
				await log1.join(await log2.getHeads(true).all());
				await log1.trim({ type: "length", to: 3 });

				const expectedData = [
					new Uint8Array([1, 0]),
					new Uint8Array([0, 2]),
					new Uint8Array([1, 1]),
				];
				const lastEntry = last(await log1.toArray());

				expect(log1.length).equal(3);
				expect(
					(await log1.toArray()).map(
						(e) => new Uint8Array(e.payload.getValue()),
					),
				).to.deep.equal(expectedData);
				expect(lastEntry.next.length).equal(1);
			});

			it("joins only specified amount of entries - (all) four entries", async () => {
				await log1.join(await log2.getHeads(true).all());
				await log1.trim({ type: "length", to: 4 });

				const expectedData = [
					new Uint8Array([0, 1]),
					new Uint8Array([1, 0]),
					new Uint8Array([0, 2]),
					new Uint8Array([1, 1]),
				];
				const lastEntry = last(await log1.toArray());

				expect(log1.length).equal(4);
				expect(
					(await log1.toArray()).map(
						(e) => new Uint8Array(e.payload.getValue()),
					),
				).to.deep.equal(expectedData);
				expect(lastEntry.next.length).equal(1);
			});
		});

		it("sets size on join", async () => {
			const n1 = await createEntry({
				store: session.peers[0].services.blocks,
				identity: {
					...signKey,
					sign: (data: Uint8Array) => signKey.sign(data),
				},
				data: new Uint8Array([0]),
			});
			n1.size = undefined as any;
			await log1.join([n1]);
			const [entry] = await log1.toArray();
			expect(entry.size).equal(245);
		});
	});
});
