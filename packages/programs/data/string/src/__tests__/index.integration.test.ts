import { LSession } from "@peerbit/test-utils";
import { DString } from "../string-store.js";
import {
	StringQueryRequest,
	StringResult,
	StringMatch,
	RangeMetadatas,
	RangeMetadata,
} from "../index.js";
import { Range } from "../range.js";
import { Observer } from "@peerbit/shared-log";
import { ProgramClient } from "@peerbit/program";

describe("query", () => {
	let session: LSession,
		observer: ProgramClient,
		writer: ProgramClient,
		writeStore: DString,
		observerStore: DString;

	beforeEach(async () => {
		// we reinit sesion for every test since DString does always have same address
		// and that might lead to sideeffects running all tests in one go
		session = await LSession.connected(2);
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
				role: new Observer(),
			},
		});
	});

	afterEach(async () => {
		await writeStore.drop();
		await observerStore.drop();
		await session.stop();
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
				length: "world".length,
			})
		);

		let responses: StringResult[] = (
			await observerStore.query.request(
				new StringQueryRequest({
					query: [],
				}),

				{ amount: 1 }
			)
		).map((x) => x.response);
		expect(responses[0]).toBeDefined();
		expect(responses[0]).toMatchObject(
			new StringResult({
				string: "hello world",
				metadatas: undefined, //  because we are matching without any specific query
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
				length: "world".length,
			})
		);

		let response: StringResult[] = (
			await observerStore.query.request(
				new StringQueryRequest({
					query: [
						new StringMatch({
							exactMatch: true,
							value: "o w",
						}),
						new StringMatch({
							exactMatch: true,
							value: "orld",
						}),
					],
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
							offset: BigInt("hell".length),
						}),
						new RangeMetadata({
							length: BigInt("orld".length),
							offset: BigInt("hello w".length),
						}),
					],
				}),
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
				length: "world".length,
			})
		);

		let callbackValues: string[] = [];
		const string = await observerStore.getValue({
			remote: {
				callback: (s) => {
					callbackValues.push(s);
				},
				queryOptions: { amount: 1 },
			},
		});
		expect(string).toEqual("hello world");
		expect(callbackValues).toEqual(["hello world"]);
	});
});

describe("load", () => {
	let session: LSession, store: DString;

	beforeEach(async () => {
		// we reinit sesion for every test since DString does always have same address
		// and that might lead to sideeffects running all tests in one go
		session = await LSession.connected(1);

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
		await store.open();
		expect(await store.getValue()).toEqual(data);
	});
});
