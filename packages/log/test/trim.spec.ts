import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { waitFor } from "@peerbit/time";
import { expect } from "chai";
import type { ShallowEntry } from "../src/entry-shallow.js";
import { Log } from "../src/log.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("trim", function () {
	let store: BlockStore;

	before(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	let log: Log<Uint8Array>;
	beforeEach(async () => {
		log = new Log<Uint8Array>();
		await log.open(store, signKey);
	});

	it("cut back to max oplog length", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			trim: {
				type: "length",
				from: 1,
				to: 1,
				filter: { canTrim: () => true },
			},
		});
		await log.append(new Uint8Array([1]));
		await log.trim();
		await log.append(new Uint8Array([2]));
		await log.trim();
		await log.append(new Uint8Array([3]));
		await log.trim();
		expect(log.length).equal(1);
		expect((await log.toArray())[0].payload.getValue()).to.deep.equal(
			new Uint8Array([3]),
		);
	});

	it("respect canTrim for length type", async () => {
		let canTrimInvocations = 0;
		// @ts-ignore
		const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // set nexts [] so all get unique gids
		// @ts-ignore
		const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // set nexts [] so all get unique gids
		// @ts-ignore
		const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // set nexts [] so all get unique gids

		await log.trim({
			type: "length",
			from: 2,
			to: 2,
			filter: {
				canTrim: (entry) => {
					canTrimInvocations += 1;
					return Promise.resolve(entry.meta.gid !== e1.entry.meta.gid);
				},
			},
		});
		expect(log.length).equal(2);
		expect((await log.toArray())[0].payload.getValue()).to.deep.equal(
			new Uint8Array([1]),
		);
		expect((await log.toArray())[1].payload.getValue()).to.deep.equal(
			new Uint8Array([3]),
		);
		expect(canTrimInvocations).equal(2);
	});

	it("not recheck untrimmable gid", async () => {
		let canTrimInvocations = 0;
		// @ts-ignore
		const e1 = await log.append(new Uint8Array([1]));
		// @ts-ignore
		const e2 = await log.append(new Uint8Array([2]));
		// @ts-ignore
		const e3 = await log.append(new Uint8Array([3]));
		await log.trim({
			type: "length",
			from: 2,
			to: 2,
			filter: {
				canTrim: (gid) => {
					canTrimInvocations += 1;
					return Promise.resolve(false);
				},
			},
		});
		expect(log.length).equal(3);
		expect(canTrimInvocations).equal(1);
	});

	it("cut back to cut length", async () => {
		const log = new Log<Uint8Array>();
		await log.open(
			store,
			signKey,
			{ trim: { type: "length", from: 3, to: 1 } }, // when length > 3 cut back to 1
		);
		const { entry: a1 } = await log.append(new Uint8Array([1]));
		const { entry: a2 } = await log.append(new Uint8Array([2]));
		expect(await log.trim()).to.be.empty;
		expect(await log.blocks.get(a1.hash)).to.exist;
		expect(await log.blocks.get(a2.hash)).to.exist;
		expect(log.length).equal(2);
		const { entry: a3, removed } = await log.append(new Uint8Array([3]));
		expect(removed.map((x) => x.hash)).to.have.members([a1.hash, a2.hash]);
		expect(log.length).equal(1);
		expect(await log.blocks.get(a1.hash)).equal(undefined);
		expect(await log.blocks.get(a2.hash)).equal(undefined);
		expect(await log.blocks.get(a3.hash)).to.exist;
		expect((await log.toArray())[0].payload.getValue()).to.deep.equal(
			new Uint8Array([3]),
		);
	});

	describe("concurrency", () => {
		it("append", async () => {
			/**
			 * In this test we test, that even if the commits are concurrent the output is determenistic if we are trimming
			 */

			// TODO is this test really neccessary
			let canTrimInvocations = 0;
			const log = new Log<string>();
			await log.open(
				store,
				signKey,
				{
					encoding: JSON_ENCODING,
					trim: {
						type: "length",
						from: 1,
						to: 1,
						filter: {
							canTrim: () => {
								canTrimInvocations += 1;
								return true;
							},
						},
					},
				}, // when length > 3 cut back to 1
			);
			let size = 3;
			let promises: Promise<any>[] = [];
			for (let i = 0; i < size; i++) {
				promises.push(log.append(String(i)));
			}
			await Promise.all(promises);
			expect(canTrimInvocations).lessThan(size); // even though concurrently trimming is sync
			expect(log.length).equal(1);
		});
	});

	it("cut back to bytelength", async () => {
		const log = new Log<Uint8Array>();
		await log.open(
			store,
			signKey,
			{
				trim: { type: "bytelength", to: 15, filter: { canTrim: () => true } },
				encoding: JSON_ENCODING,
			}, // bytelength is 15 so for every new helloX we hav eto delete the previous helloY
		);

		const { entry: a1, removed: r1 } = await log.append(new Uint8Array([1]), {
			meta: { next: [] },
		});
		expect(r1).to.be.empty;
		expect(await log.blocks.get(a1.hash)).to.exist;
		expect(
			(await log.toArray()).map((x) => x.payload.getValue()),
		).to.deep.equal([new Uint8Array([1])]);
		const { entry: a2, removed: r2 } = await log.append(new Uint8Array([2]), {
			meta: { next: [] },
		});
		expect(r2.map((x) => x.hash)).to.have.members([a1.hash]);
		expect(await log.blocks.get(a2.hash)).to.exist;
		expect(
			(await log.toArray()).map((x) => x.payload.getValue()),
		).to.deep.equal([new Uint8Array([2])]);
		const { entry: a3, removed: r3 } = await log.append(new Uint8Array([3]), {
			meta: { next: [] },
		});
		expect(r3.map((x) => x.hash)).to.have.members([a2.hash]);
		expect(await log.blocks.get(a3.hash)).to.exist;
		expect(
			(await log.toArray()).map((x) => x.payload.getValue()),
		).to.deep.equal([new Uint8Array([3])]);
		const { entry: a4, removed: r4 } = await log.append(new Uint8Array([4]), {
			meta: { next: [] },
		});
		expect(r4.map((x) => x.hash)).to.have.members([a3.hash]);
		expect(
			(await log.toArray()).map((x) => x.payload.getValue()),
		).to.deep.equal([new Uint8Array([4])]);
		expect(await log.blocks.get(a1.hash)).equal(undefined);
		expect(await log.blocks.get(a2.hash)).equal(undefined);
		expect(await log.blocks.get(a3.hash)).equal(undefined);
		expect(await log.blocks.get(a4.hash)).to.exist;
	});

	it("trim to time", async () => {
		const maxAge = 3000;
		const log = new Log<Uint8Array>();
		await log.open(
			store,
			signKey,
			{
				trim: { type: "time", maxAge },
			}, // bytelength is 15 so for every new helloX we hav eto delete the previous helloY
		);

		let t0 = +new Date();
		const { entry: a1, removed: r1 } = await log.append(new Uint8Array([1]));
		expect(r1).to.be.empty;
		expect(await log.blocks.get(a1.hash)).to.exist;
		expect(
			(await log.toArray()).map((x) => x.payload.getValue()),
		).to.deep.equal([new Uint8Array([1])]);
		const { entry: a2, removed: r2 } = await log.append(new Uint8Array([2]));
		expect(r2.map((x) => x.hash)).to.have.members([]);

		await waitFor(() => +new Date() - t0 > maxAge);
		// @ts-ignore
		const { entry: a3, removed: r3 } = await log.append(new Uint8Array([2]));
		expect(r3.map((x) => x.hash)).to.have.members([a1.hash, a2.hash]);
	});

	describe("cache", () => {
		it("not recheck gid in cache", async () => {
			let canTrimInvocations = 0;
			const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			const canTrim = (entry: ShallowEntry) => {
				canTrimInvocations += 1;
				return Promise.resolve(entry.meta.gid !== e1.entry.meta.gid); // can not trim
			};
			const cacheId = () => "";
			await log.trim({
				type: "length",
				from: 2,
				to: 2,
				filter: {
					canTrim,
					cacheId,
				},
			});
			expect(log.length).equal(2);
			expect(canTrimInvocations).equal(2); // checks e1 then e2 (e2 we can delete)

			await log.trim({
				type: "length",
				from: 1,
				to: 1,
				filter: {
					canTrim,
					cacheId,
				},
			});

			expect(log.length).equal(1);
			expect(canTrimInvocations).equal(3); // Will start at e3 (and not loop around because tail and head is the same)
		});

		it("ignores invalid trim cache", async () => {
			let canTrimInvocations = 0;
			const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // meta: { next: [] } means unique gid

			// @ts-ignore
			const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const e4 = await log.append(new Uint8Array([4]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			const canTrim = (entry: ShallowEntry) => {
				canTrimInvocations += 1;
				return Promise.resolve(entry.meta.gid !== e1.entry.meta.gid); // can not trim
			};

			const cacheId = () => "";

			await log.trim({
				type: "length",
				from: 3,
				to: 3,
				filter: {
					canTrim,
					cacheId,
				},
			});

			expect(canTrimInvocations).equal(2); // checks e1 then e2 (e2 we can delete)
			await log.delete(e3.entry.hash); // e3 is also cached as the next node to trim

			await log.trim({
				type: "length",
				from: 1,
				to: 1,
				filter: {
					canTrim,
					cacheId,
				},
			});

			expect(log.length).equal(1);

			expect(canTrimInvocations).equal(3); // Will start at e4 because e3 is cache is gone
		});

		it("uses trim cache cross sessions", async () => {
			let canTrimInvocations: string[] = [];
			const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			const canTrim = (entry: ShallowEntry) => {
				canTrimInvocations.push(entry.meta.gid);
				return Promise.resolve(false); // can not trim
			};

			const cacheId = () => "id";

			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});

			expect(canTrimInvocations).to.deep.equal([
				e1.entry.meta.gid,
				e2.entry.meta.gid,
				e3.entry.meta.gid,
			]); // checks e1, e2, e3
			canTrimInvocations = [];
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});

			expect(canTrimInvocations).to.be.empty; // no more checks since nothing has changed

			const e4 = await log.append(new Uint8Array([4]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const result = await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});
			expect(canTrimInvocations).to.deep.equal([
				e3.entry.meta.gid,
				e4.entry.meta.gid,
			]); // starts at e1 then e2, but ignored because of cache
		});

		it("can first when new entries are added", async () => {
			let canTrimInvocations: string[] = [];
			let trimmableGids = new Set();

			const canTrim = (entry: ShallowEntry) => {
				canTrimInvocations.push(entry.meta.gid);
				return trimmableGids.has(entry.meta.gid);
			};

			const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // meta: { next: [] } means unique gid

			trimmableGids.add(e1.entry.meta.gid);

			const cacheId = () => "id";
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.entry.hash,
			]);
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});

			expect(canTrimInvocations).to.deep.equal([e1.entry.meta.gid]); // checks e1
			expect((await log.toArray()).map((x) => x.hash)).to.be.empty;

			canTrimInvocations = [];
			const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			trimmableGids.add(e2.entry.meta.gid);
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e2.entry.hash,
			]);
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});
			expect(canTrimInvocations).to.deep.equal([e2.entry.meta.gid]); // e1 checked again (?), e2 checked and trimmed
			expect((await log.toArray()).map((x) => x.hash)).to.be.empty;

			canTrimInvocations = [];
			const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e3.entry.hash,
			]);
			trimmableGids.add(e3.entry.meta.gid);
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});
			expect(canTrimInvocations).to.deep.equal([e3.entry.meta.gid]);
			expect((await log.toArray()).map((x) => x.hash)).to.be.empty;
		});

		it("can trim later new entries are added", async () => {
			let canTrimInvocations: string[] = [];
			let trimmableGids = new Set();
			const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			const canTrim = (entry: ShallowEntry) => {
				canTrimInvocations.push(entry.meta.gid);
				return trimmableGids.has(entry.meta.gid);
			};

			const cacheId = () => "id";
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});

			expect(canTrimInvocations).to.deep.equal([e1.entry.meta.gid]); // checks e1

			canTrimInvocations = [];
			const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});
			expect(canTrimInvocations).to.deep.equal([
				e1.entry.meta.gid,
				e2.entry.meta.gid,
			]); // e1 checked again (?), e2 checked and trimmed
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.entry.hash,
				e2.entry.hash,
			]);

			canTrimInvocations = [];
			const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.entry.hash,
				e2.entry.hash,
				e3.entry.hash,
			]);
			trimmableGids.add(e3.entry.meta.gid);
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});
			expect(canTrimInvocations).to.deep.equal([
				e2.entry.meta.gid,
				e3.entry.meta.gid,
			]);
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.entry.hash,
				e2.entry.hash,
			]);

			canTrimInvocations = [];
			const e4 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.entry.hash,
				e2.entry.hash,
				e4.entry.hash,
			]);
			trimmableGids.add(e4.entry.meta.gid);
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId,
				},
			});
			expect(canTrimInvocations).to.deep.equal([
				e2.entry.meta.gid,
				e4.entry.meta.gid,
			]);
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.entry.hash,
				e2.entry.hash,
			]);
		});

		it("drops cache if canTrim function changes", async () => {
			let canTrimInvocations = 0;
			// @ts-ignore
			const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim: (gid) => {
						canTrimInvocations += 1;
						return Promise.resolve(false); // can not trim
					},
				},
			});
			expect(canTrimInvocations).equal(3); // checks e1 then e2 (e2 we can delete)
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim: (gid) => {
						canTrimInvocations += 1;
						return Promise.resolve(false); // can not trim
					},
				},
			});

			expect(canTrimInvocations).equal(6);
		});

		it("changing cacheId will reset cache", async () => {
			let canTrimInvocations = 0;
			const e1 = await log.append(new Uint8Array([1]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const e2 = await log.append(new Uint8Array([2]), { meta: { next: [] } }); // meta: { next: [] } means unique gid
			// @ts-ignore
			const e3 = await log.append(new Uint8Array([3]), { meta: { next: [] } }); // meta: { next: [] } means unique gid

			let trimGid: string | undefined = undefined;
			const canTrim = (entry: ShallowEntry) => {
				canTrimInvocations += 1;
				return Promise.resolve(entry.meta.gid === trimGid); // can not trim
			};
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId: () => "a",
				},
			});

			trimGid = e1.entry.meta.gid;
			expect(canTrimInvocations).equal(3);
			expect(log.length).equal(3);

			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId: () => "a",
				},
			});

			expect(canTrimInvocations).equal(3);
			expect(log.length).equal(3);
			await log.trim({
				type: "length",
				from: 0,
				to: 0,
				filter: {
					canTrim,
					cacheId: () => "b",
				},
			});
			expect(log.length).equal(2);
			expect(canTrimInvocations).equal(6); // cache resets, so will go through all entries
		});

		it("trims on middle insertion", async () => {
			const log2 = new Log<Uint8Array>();
			await log2.open(store, signKey);
			const e1 = await log.append(new Uint8Array([1]));
			const e2 = await log2.append(new Uint8Array([2]));
			const e3 = await log.append(new Uint8Array([3]));
			const canTrim = () => true;
			await log.trim({
				type: "length",
				from: 2,
				to: 2,
				filter: {
					canTrim,
					cacheId: () => "b",
				},
			});
			await log.join([e2.entry]);
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.entry.hash,
				e2.entry.hash,
				e3.entry.hash,
			]);
			await log.trim({
				type: "length",
				from: 2,
				to: 2,
				filter: {
					canTrim,
					cacheId: () => "b",
				},
			});
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([
				e2.entry.hash,
				e3.entry.hash,
			]);
		});
	});
});
