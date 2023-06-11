import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { Entry } from "../entry";
import { EntryIndex } from "../entry-index";
import { LastWriteWins } from "../log-sorting";
import { Values } from "../values";
import { Cache } from "@dao-xyz/cache";
import { signKey } from "./fixtures/privateKey";

describe("values", () => {
	let e1: Entry<string>, e2: Entry<string>, e3: Entry<string>;
	let store: BlockStore;
	let entryIndex: EntryIndex<string>;
	beforeEach(async () => {
		const identity = signKey;
		store = new MemoryLevelBlockStore();
		await store.open();
		e1 = await Entry.create({
			store,
			identity,
			gidSeed: Buffer.from("a"),
			data: "1",
			next: [],
		});

		e2 = await Entry.create({
			store,
			identity,
			gidSeed: Buffer.from("a"),
			data: "2",
			next: [e1],
		});

		e3 = await Entry.create({
			store,
			identity,
			gidSeed: Buffer.from("a"),
			data: "3",
			next: [e2],
		});
		entryIndex = new EntryIndex({
			store,
			init: (e) => {},
			cache: new Cache({ max: 1000 }),
		});
		await entryIndex.set(e1);
		await entryIndex.set(e2);
		await entryIndex.set(e3);
	});
	afterEach(async () => {
		await store.close();
	});
	it("put last", async () => {
		const values = new Values<string>(entryIndex, LastWriteWins, []);
		await values.put(e1);
		await values.put(e2);
		await values.put(e3);
		expect(values.head!.value.hash).toEqual(e3.hash);
		expect((await values.toArray()).map((x) => x.hash)).toEqual([
			e1.hash,
			e2.hash,
			e3.hash,
		]);
	});

	it("put middle", async () => {
		const values = new Values<string>(entryIndex, LastWriteWins, []);
		await values.put(e1);
		await values.put(e3);
		await values.put(e2);
		expect(values.head!.value.hash).toEqual(e3.hash);
		expect((await values.toArray()).map((x) => x.hash)).toEqual([
			e1.hash,
			e2.hash,
			e3.hash,
		]);
	});

	it("put first", async () => {
		const values = new Values<string>(entryIndex, LastWriteWins, []);
		await values.put(e2);
		await values.put(e3);
		await values.put(e1);
		expect(values.head!.value.hash).toEqual(e3.hash);
		expect((await values.toArray()).map((x) => x.hash)).toEqual([
			e1.hash,
			e2.hash,
			e3.hash,
		]);
	});

	it("put concurrently", async () => {
		const values = new Values<string>(entryIndex, LastWriteWins, []);
		let promises: Promise<any>[] = [];
		for (let i = 0; i < 100; i++) {
			promises.push(values.put(e1));
		}

		await Promise.all(promises);
		expect(values.head!.value.hash).toEqual(e1.hash);
		expect((await values.toArray()).length).toEqual(1);
	});
	it("delete", async () => {
		const values = new Values<string>(entryIndex, LastWriteWins, []);
		await values.put(e1);
		await values.put(e2);
		await values.put(e3);
		expect(values.head!.value.hash).toEqual(e3.hash);
		expect((await values.toArray()).map((x) => x.hash)).toEqual([
			e1.hash,
			e2.hash,
			e3.hash,
		]);
		await values.delete(e2);
		expect((await values.toArray()).map((x) => x.hash)).toEqual([
			e1.hash,
			e3.hash,
		]);
		expect(values.head!.value.hash).toEqual(e3.hash);
		expect(values.tail!.value.hash).toEqual(e1.hash);
		await values.delete(e1);
		expect((await values.toArray()).map((x) => x.hash)).toEqual([e3.hash]);
		expect(values.head!.value.hash).toEqual(e3.hash);
		expect(values.tail!.value.hash).toEqual(e3.hash);
		await values.delete(e3);
		expect(await values.toArray()).toEqual([]);
		expect(values.head).toBeNull();
		expect(values.tail).toBeNull();
	});
});
