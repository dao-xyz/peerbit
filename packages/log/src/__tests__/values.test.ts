import {
	BlockStore,
	MemoryLevelBlockStore,
} from "@dao-xyz/libp2p-direct-block";
import { Ed25519Keypair, Keypair } from "@dao-xyz/peerbit-crypto";
import { KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { Entry } from "../entry";
import { LastWriteWins } from "../log-sorting";
import { Values } from "../values";
import { identityFromSignKey } from "./utils";

describe("values", () => {
	let e1: Entry<string>, e2: Entry<string>, e3: Entry<string>;
	let store: BlockStore;
	beforeEach(async () => {
		const identity = identityFromSignKey(
			new KeyWithMeta({
				group: "",
				keypair: Ed25519Keypair.create(),
				timestamp: 0n,
			})
		);
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
	});
	afterEach(async () => {
		await store.close();
	});
	it("put last", () => {
		const values = new Values<string>(LastWriteWins, []);
		values.put(e1);
		values.put(e2);
		values.put(e3);
		expect(values.head!.value).toEqual(e3);
		expect(values.toArray()).toEqual([e1, e2, e3]);
	});

	it("put middle", () => {
		const values = new Values<string>(LastWriteWins, []);
		values.put(e1);
		values.put(e3);
		values.put(e2);
		expect(values.head!.value).toEqual(e3);
		expect(values.toArray()).toEqual([e1, e2, e3]);
	});

	it("put first", () => {
		const values = new Values<string>(LastWriteWins, []);
		values.put(e2);
		values.put(e3);
		values.put(e1);
		expect(values.head!.value).toEqual(e3);
		expect(values.toArray()).toEqual([e1, e2, e3]);
	});

	it("delete", () => {
		const values = new Values<string>(LastWriteWins, []);
		values.put(e1);
		values.put(e2);
		values.put(e3);
		expect(values.head!.value).toEqual(e3);
		expect(values.toArray()).toEqual([e1, e2, e3]);
		values.delete(e2);
		expect(values.toArray()).toEqual([e1, e3]);
		expect(values.head!.value).toEqual(e3);
		expect(values.tail!.value).toEqual(e1);
		values.delete(e1);
		expect(values.toArray()).toEqual([e3]);
		expect(values.head!.value).toEqual(e3);
		expect(values.tail!.value).toEqual(e3);
		values.delete(e3);
		expect(values.toArray()).toEqual([]);
		expect(values.head).toBeNull();
		expect(values.tail).toBeNull();
	});
});
