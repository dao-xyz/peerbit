import { createStore, LSession } from "@dao-xyz/peerbit-test-utils";
import Cache from "@dao-xyz/lazy-level";
import {
	Ed25519Keypair,
	X25519Keypair,
	X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { Libp2pExtended } from "@dao-xyz/peerbit-libp2p";
import { Observer, Replicator } from "@dao-xyz/peerbit-program";

import { DString } from "../string-store.js";
import {
	StringQueryRequest,
	StringResult,
	StringMatch,
	RangeMetadatas,
	RangeMetadata,
} from "../index.js";
import { Range } from "../range.js";

describe("query", () => {
	let session: LSession,
		observer: Libp2pExtended,
		writer: Libp2pExtended,
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
		await writeStore.init(writer, {
			role: new Replicator(),
			log: {
				replication: {
					replicators: () => [],
				},
				encryption: {
					getAnyKeypair: (_) => Promise.resolve(undefined),
					getEncryptionKeypair: () => X25519Keypair.create(),
				},
				cache: () => new Cache(createStore()),
			},
		});

		observerStore = (await DString.load(
			writer.services.blocks,
			writeStore.address!
		)) as DString;

		await observerStore.init(observer, {
			role: new Observer(),
			log: {
				replication: {
					replicators: () => [],
				},
				cache: () => new Cache(createStore()),
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
		const string = await observerStore.toString({
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
		await store.init(writer, {
			role: new Replicator(),
			log: {
				replication: {
					replicators: () => [],
				},
				encryption: {
					getAnyKeypair: (_) => Promise.resolve(undefined),
					getEncryptionKeypair: () => X25519Keypair.create(),
				},
				cache: () => new Cache(createStore()),
			},
		});

		await store.add(
			"hello",
			new Range({ offset: 0n, length: "hello".length }),
			{
				reciever: {
					metadata: undefined,
					signatures: undefined,
					next: undefined,
					payload: [await X25519PublicKey.create()],
				},
			}
		);
		await store.close();
		await delay(1000); // TODO store is async?
		await store.load();
		await waitFor(() => store._log.values.length === 1);
		await store.close();
	});
});
