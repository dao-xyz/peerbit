import { TestSession } from "@peerbit/test-utils";
import { DString } from "../string-store.js";
import {
	SearchRequest,
	StringResult,
	StringMatch,
	RangeMetadatas,
	RangeMetadata,
	StringOperation,
	AbstractSearchResult
} from "../index.js";
import { Range } from "../range.js";
import { ProgramClient } from "@peerbit/program";
import { Change } from "@peerbit/log";
import { waitForResolved } from "@peerbit/time";

describe("query", () => {
	let session: TestSession,
		observer: ProgramClient,
		writer: ProgramClient,
		writeStore: DString,
		observerStore: DString;

	beforeEach(async () => {
		// we reinit sesion for every test since DString does always have same address
		// and that might lead to sideeffects running all tests in one go
		session = await TestSession.connected(2);
		observer = session.peers[0];
		writer = session.peers[1];

		// Create store
		writeStore = new DString({});
		await writer.open(writeStore);

		observerStore = (await DString.load(
			writeStore.address!,
			writer.services.blocks
		)) as DString;

		await observer.open(observerStore, {
			args: {
				log: {
					role: "observer"
				}
			}
		});

		await observerStore.waitFor(writer.identity.publicKey);
	});

	afterEach(async () => {
		await writeStore.drop();
		await observerStore.drop();
		await session.stop();
	});

	it("empty", async () => {
		const string = await writeStore.getValue();
		expect(string).toEqual("");
	});

	it("match all", async () => {
		await writeStore.add(
			"hello",
			new Range({ offset: 0n, length: "hello".length })
		);
		await writeStore.add(
			"world",
			new Range({
				offset: BigInt("hello ".length),
				length: "world".length
			})
		);
		let responses: AbstractSearchResult[] = (
			await observerStore.query.request(
				new SearchRequest({
					query: []
				}),

				{ amount: 1 }
			)
		).map((x) => x.response);

		expect(responses[0]).toBeDefined();
		expect(responses[0]).toMatchObject(
			new StringResult({
				string: "hello world",
				metadatas: undefined //  because we are matching without any specific query
			})
		);
	});

	it("match part", async () => {
		await writeStore.add(
			"hello",
			new Range({ offset: 0n, length: "hello".length })
		);
		await writeStore.add(
			"world",
			new Range({
				offset: BigInt("hello ".length),
				length: "world".length
			})
		);

		let response: AbstractSearchResult[] = (
			await observerStore.query.request(
				new SearchRequest({
					query: [
						new StringMatch({
							exactMatch: true,
							value: "o w"
						}),
						new StringMatch({
							exactMatch: true,
							value: "orld"
						})
					]
				}),
				{ amount: 1 }
			)
		).map((x) => x.response);
		expect(response[0]).toBeDefined();
		expect(response[0]).toMatchObject(
			new StringResult({
				string: "hello world",
				metadatas: new RangeMetadatas({
					metadatas: [
						new RangeMetadata({
							length: BigInt("o w".length),
							offset: BigInt("hell".length)
						}),
						new RangeMetadata({
							length: BigInt("orld".length),
							offset: BigInt("hello w".length)
						})
					]
				})
			})
		);
	});

	it("toString remote", async () => {
		await writeStore.add(
			"hello",
			new Range({ offset: 0n, length: "hello".length })
		);
		await writeStore.add(
			"world",
			new Range({
				offset: BigInt("hello ".length),
				length: "world".length
			})
		);

		let callbackValues: string[] = [];
		const string = await observerStore.getValue({
			remote: {
				callback: (s) => {
					callbackValues.push(s);
				},
				queryOptions: { amount: 1 }
			}
		});
		expect(string).toEqual("hello world");
		expect(callbackValues).toEqual(["hello world"]);
	});
});

describe("concurrency", () => {
	let session: TestSession,
		peer1: ProgramClient,
		peer2: ProgramClient,
		peer3: ProgramClient,
		store1: DString,
		store2: DString,
		store3: DString;

	beforeEach(async () => {
		// we reinit sesion for every test since DString does always have same address
		// and that might lead to sideeffects running all tests in one go
		session = await TestSession.connected(3);
		peer1 = session.peers[0];
		peer2 = session.peers[1];
		peer3 = session.peers[2];

		store1 = new DString({});

		await peer1.open(store1);

		store2 = (await DString.load(
			store1.address!,
			peer2.services.blocks
		)) as DString;

		store3 = (await DString.load(
			store1.address!,
			peer3.services.blocks
		)) as DString;

		await peer2.open(store2);
		await peer3.open(store3);
	});

	afterEach(async () => {
		await store1.drop();
		await store2.drop();
		await store3.drop();
		await session.stop();
	});
	it("can replicate state", async () => {
		await store1.add("1", new Range({ offset: 0n, length: 1 }));
		await store2.add("2", new Range({ offset: 1n, length: 1 }));
		await store3.add("3", new Range({ offset: 2n, length: 1 }));

		await waitForResolved(async () =>
			expect(await store1.getValue()).toEqual("123")
		);
		await waitForResolved(async () =>
			expect(await store2.getValue()).toEqual("123")
		);
		await waitForResolved(async () =>
			expect(await store3.getValue()).toEqual("123")
		);
	});
});

describe("events", () => {
	let session: TestSession,
		peer1: ProgramClient,
		peer2: ProgramClient,
		store1: DString,
		store2: DString;

	beforeEach(async () => {
		// we reinit sesion for every test since DString does always have same address
		// and that might lead to sideeffects running all tests in one go
		session = await TestSession.connected(2);
		peer1 = session.peers[0];
		peer2 = session.peers[1];

		// Create store
		store1 = new DString({});
		await peer2.open(store1);

		store2 = (await DString.load(
			store1.address!,
			peer2.services.blocks
		)) as DString;

		await peer1.open(store2, {
			args: {
				log: {
					role: "observer"
				}
			}
		});
	});

	afterEach(async () => {
		await store1.drop();
		await store2.drop();
		await session.stop();
	});
	it("emits events on join and append", async () => {
		let events1: Change<StringOperation>[] = [];
		let events2: Change<StringOperation>[] = [];

		store1.events.addEventListener("change", (e) => {
			events1.push(e.detail);
		});
		store2.events.addEventListener("change", (e) => {
			events2.push(e.detail);
		});

		await store1.add(
			"hello",
			new Range({ offset: 0n, length: "hello".length })
		);

		await waitForResolved(() => expect(events1).toHaveLength(1));
		expect(events1[0].added).toHaveLength(1);
		expect((await events1[0].added[0].getPayloadValue()).value).toEqual(
			"hello"
		);

		await waitForResolved(() => expect(events2).toHaveLength(1));
		expect(events2[0].added).toHaveLength(1);
		expect((await events2[0].added[0].getPayloadValue()).value).toEqual(
			"hello"
		);

		await store2.add(
			"world",
			new Range({
				offset: BigInt("hello ".length),
				length: "world".length
			})
		);

		await waitForResolved(() => expect(events1).toHaveLength(2));
		expect(events1[1].added).toHaveLength(1);
		expect((await events1[1].added[0].getPayloadValue()).value).toEqual(
			"world"
		);

		await waitForResolved(() => expect(events2).toHaveLength(2));
		expect(events2[1].added).toHaveLength(1);
		expect((await events2[1].added[0].getPayloadValue()).value).toEqual(
			"world"
		);
		expect(await store1.getValue()).toEqual("hello world");
		expect(await store2.getValue()).toEqual("hello world");
	});
});

describe("load", () => {
	let session: TestSession, store: DString;

	beforeEach(async () => {
		// we reinit sesion for every test since DString does always have same address
		// and that might lead to sideeffects running all tests in one go
		session = await TestSession.connected(1);

		// Create store
		store = new DString({});
		await session.peers[0].open(store);
	});

	afterEach(async () => {
		await store.drop();
		await session.stop();
	});

	it("loads on open", async () => {
		let data = "hello";
		await store.add(data, new Range({ offset: 0, length: data.length }));
		await store.close();
		expect(store._index.string).toEqual("");
		expect(store._index._log).toBeUndefined();
		await session.peers[0].open(store);
		expect(await store.getValue()).toEqual(data);
	});
});
