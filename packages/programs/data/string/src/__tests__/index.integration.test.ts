import { createStore, LSession } from "@peerbit/test-utils";
import Cache from "@dao-xyz/lazy-level";
import { X25519Keypair, X25519PublicKey } from "@peerbit/crypto";
import { delay, waitFor } from "@peerbit/time";

import { DString } from "../string-store.js";
import {
	StringQueryRequest,
	StringResult,
	StringMatch,
	RangeMetadatas,
	RangeMetadata,
} from "../index.js";
import { Range } from "../range.js";
import { Peerbit } from "@peerbit/interface";
import { Observer, Replicator } from "@peerbit/shared-log";

describe("query", () => {
	let session: LSession,
		observer: Peerbit,
		writer: Peerbit,
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
		await writeStore.open(writer);

		observerStore = (await DString.load(
			writeStore.address!,
			writer.services.blocks
		)) as DString;

		await observerStore.open(observer, {
			setup: (p) => p.setup(new Observer()),
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

	it("handles AccessError gracefully", async () => {
		const store = new DString({});
		await store.open(writer);

		await store.add(
			"hello",
			new Range({ offset: 0n, length: "hello".length }),
			{
				encryption: {
					keypair: await X25519Keypair.create(),
					reciever: {
						metadata: undefined,
						signatures: undefined,
						next: undefined,
						payload: [await X25519PublicKey.create()],
					},
				},
			}
		);
		await store.close();
		await delay(1000); // TODO store is async?
		await store.open(writer);
		await waitFor(() => store._log.log.values.length === 1);
		await store.close();
	});
});
