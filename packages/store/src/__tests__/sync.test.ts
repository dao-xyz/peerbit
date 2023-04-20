import { default as Cache } from "@dao-xyz/lazy-level";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { LSession, createStore } from "@dao-xyz/peerbit-test-utils";
import { DefaultOptions, Store } from "../store.js";
import { SimpleIndex } from "./utils.js";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { jest } from "@jest/globals";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AbstractLevel } from "abstract-level";
import { Entry } from "@dao-xyz/peerbit-log";

// Tests timeout
const timeout = 30000;

describe(`Sync`, () => {
	jest.setTimeout(timeout);

	let session: LSession,
		signKey: KeyWithMeta<Ed25519Keypair>,
		store: Store<any>,
		store2: Store<any>,
		keystore: Keystore,
		cacheStore: AbstractLevel<any, string, Uint8Array>;

	let index: SimpleIndex<string>;
	let fromMultihash: any;
	let fromMultiHashCounter: number;

	beforeAll(() => {
		fromMultihash = Entry.fromMultihash;
		// TODO monkeypatching might lead to sideeffects in other tests!
		Entry.fromMultihash = (s, h, o) => {
			fromMultiHashCounter += 1;
			return fromMultihash(s, h, o);
		};
	});
	afterAll(() => {
		Entry.fromMultihash = fromMultihash;
	});

	beforeEach(async () => {
		fromMultiHashCounter = 0;
		cacheStore = await createStore();
		keystore = new Keystore(await createStore());
		session = await LSession.connected(2);
		signKey = await keystore.createEd25519Key();

		const cache = new Cache(cacheStore);
		store = new Store({ storeIndex: 0 });
		index = new SimpleIndex(store);
		await store.init(
			session.peers[0].directblock,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				replicationConcurrency: 123,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index.updateIndex.bind(index),
				cacheId: "id",
			}
		);
	});

	afterEach(async () => {
		await store.close();
		await store2.close();
		await session.stop();
		await keystore?.close();
	});

	it("syncs normally", async () => {
		const cache = new Cache(createStore());
		const index2 = new SimpleIndex(store);
		store2 = new Store({ storeIndex: 1 });

		await store2.init(
			session.peers[1].directblock,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				replicationConcurrency: 123,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index2.updateIndex.bind(index2),
				cacheId: "id",
			}
		);

		const entryCount = 10;
		for (let i = 0; i < entryCount; i++) {
			await store.append("i: " + i);
		}

		expect((await store.oplog.getHeads()).length).toEqual(1);
		expect(store.oplog.values.length).toEqual(entryCount);
		await store2.sync(await store.oplog.getHeads());
		await waitFor(() => store2.oplog.values.length == entryCount);
		expect(await store2.oplog.getHeads()).toHaveLength(1);
	});

	it("syncs with references", async () => {
		const cache = new Cache(createStore());
		const index2 = new SimpleIndex(store);
		store2 = new Store({ storeIndex: 1 });
		await store2.init(
			session.peers[1].directblock,
			{
				...signKey.keypair,
				sign: async (data: Uint8Array) => await signKey.keypair.sign(data),
			},
			{
				...DefaultOptions,
				replicationConcurrency: 123,
				resolveCache: () => Promise.resolve(cache),
				onUpdate: index2.updateIndex.bind(index2),
				cacheId: "id",
			}
		);

		const entryCount = 10;
		for (let i = 0; i < entryCount; i++) {
			await store.append(i);
		}

		expect((await store.oplog.getHeads()).length).toEqual(1);
		expect(store2.oplog.length).toEqual(0);
		expect(store.oplog.values.length).toEqual(entryCount);
		await store2.sync([
			{
				entry: (await store.oplog.getHeads())[0],
				references: [
					(await store.oplog.values.toArray())[3],
					(await store.oplog.values.toArray())[6],
				],
			},
		]);
		expect(store2.oplog.values.length).toEqual(entryCount);
		expect(fromMultiHashCounter).toEqual(7); // since passing 3 references only 10 - 3 = 7 needs to be loaded from disc or network
		expect(await store2.oplog.getHeads()).toHaveLength(1);
	});
});
