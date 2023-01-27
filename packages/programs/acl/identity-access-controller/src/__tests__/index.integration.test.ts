import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import {
	createStore,
	LSession,
	waitForPeers,
} from "@dao-xyz/peerbit-test-utils";
import { Access, AccessType } from "../access";
import { AnyAccessCondition, PublicKeyAccessCondition } from "../condition";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { AccessError, Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import {
	Documents,
	DocumentIndex,
	DocumentQueryRequest,
	FieldStringMatchQuery,
	Results,
} from "@dao-xyz/peerbit-document";
import type { CanAppend, Identity } from "@dao-xyz/peerbit-log";
import { AbstractLevel } from "abstract-level";
import { CachedValue, DefaultOptions } from "@dao-xyz/peerbit-store";
import Cache from "@dao-xyz/peerbit-cache";
import { CanRead, RPC } from "@dao-xyz/peerbit-rpc";
import { Program } from "@dao-xyz/peerbit-program";
import { IdentityAccessController } from "../acl-db";

@variant("document")
class Document {
	@field({ type: "string" })
	id: string;

	constructor(props?: { id: string }) {
		if (props) {
			this.id = props.id;
		}
	}
}

const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		sign: (data) => ed.sign(data),
	} as Identity;
};

@variant("test_store")
class TestStore extends Program {
	@field({ type: Documents })
	store: Documents<Document>;

	@field({ type: IdentityAccessController })
	accessController: IdentityAccessController;

	constructor(properties: {
		id?: string;
		identity: Identity;
		accessControllerName?: string;
	}) {
		super(properties);
		if (properties) {
			this.store = new Documents({
				index: new DocumentIndex({
					indexBy: "id",
					query: new RPC(),
				}),
			});
			this.accessController = new IdentityAccessController({
				id: properties.accessControllerName || "test-acl",
				rootTrust: properties.identity?.publicKey,
			});
		}
	}

	async setup() {
		await this.accessController.setup();
		await this.store.setup({
			type: Document,
			canRead: this.accessController.canRead.bind(this.accessController),
			canAppend: (entry) => this.accessController.canAppend(entry),
		});
	}
}


describe("index", () => {
	let session: LSession,
		programs: Program[],
		identites: Identity[],
		cacheStore: AbstractLevel<any, string, Uint8Array>[];

	const identity = (i: number) => identites[i];
	const init = async <T extends Program>(
		store: T,
		i: number,
		options: {
			replicate: boolean;
			store: {};
			canRead?: CanRead;
			canAppend?: CanAppend<T>;
		}
	) => {
		programs.push(store);
		const result = await store.init(session.peers[i], identites[i], {
			...options,
			store: {
				...DefaultOptions,
				resolveCache: async () => new Cache<CachedValue>(cacheStore[i]),
				...options.store,
			},
		});
		return result;
	};

	beforeAll(async () => {
		session = await LSession.connected(3);
		identites = [];
		programs = [];
		cacheStore = [];
		for (let i = 0; i < session.peers.length; i++) {
			identites.push(await createIdentity());
			cacheStore.push(
				await createStore()
			);
		}
	});

	afterAll(async () => {
		await Promise.all(programs?.map((c) => c.close()));
		await session.stop();
		await Promise.all(cacheStore?.map((c) => c.close()));
	});

	it("can be deterministic", async () => {
		const key = (await Ed25519Keypair.create()).publicKey;
		const t1 = new IdentityAccessController({ id: "x", rootTrust: key });
		const t2 = new IdentityAccessController({ id: "x", rootTrust: key });
		t1.setupIndices();
		t2.setupIndices();

		expect(serialize(t1)).toEqual(serialize(t2));
	});

	it("can write from trust web", async () => {
		const s = new TestStore({ identity: identity(0) });;
		const options = {

			replicate: true,
			store: {},
		};
		const l0a = await init(s, 0, options);

		await l0a.store.put(
			new Document({
				id: "1",
			})
		);

		const l0b = (await init(
			await TestStore.load(session.peers[1].directblock, l0a.address!),
			1,
			options
		)) as TestStore;

		await expect(
			l0b.store.put(
				new Document({
					id: "id",
				})
			)
		).rejects.toBeInstanceOf(AccessError); // Not trusted
		await l0a.accessController.trustedNetwork.add(identity(1).publicKey);

		await l0b.accessController.trustedNetwork.trustGraph.store.sync(
			l0a.accessController.trustedNetwork.trustGraph.store.oplog.heads
		);

		await waitFor(
			() =>
				l0b.accessController.trustedNetwork.trustGraph.store.oplog
					.length === 1
		);
		await waitFor(
			() =>
				l0b.accessController.trustedNetwork.trustGraph._index.size === 1
		);

		await l0b.store.put(
			new Document({
				id: "2",
			})
		); // Now trusted

		await l0a.store.store.sync(l0b.store.store.oplog.heads);
		await l0b.store.store.sync(l0a.store.store.oplog.heads);

		await waitFor(() => l0a.store.index.size === 2);
		await waitFor(() => l0b.store.index.size === 2);
	});

	describe("conditions", () => {
		it("publickey", async () => {
			const options = {

				replicate: true,
				store: {},
			};

			const l0a = await init(
				new TestStore({ identity: identity(0) }),
				0,
				options
			);

			await l0a.store.put(
				new Document({
					id: "1",
				})
			);

			const l0b = (await init(
				await TestStore.load(session.peers[1].directblock, l0a.address!),
				1,
				options
			)) as TestStore;

			await l0b.store.store.sync(l0a.store.store.oplog.heads);
			await waitFor(() => l0b.store.index.size === 1);
			await expect(
				l0b.store.put(
					new Document({
						id: "id",
					})
				)
			).rejects.toBeInstanceOf(AccessError); // Not trusted

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new PublicKeyAccessCondition({
						key: identity(1).publicKey,
					}),
					accessTypes: [AccessType.Any],
				})
			);

			await l0b.accessController.access.store.sync(
				l0a.accessController.access.store.oplog.heads
			);
			await waitFor(() => l0b.accessController.access.index.size === 1);
			await l0b.store.put(
				new Document({
					id: "2",
				})
			); // Now trusted
		});

		it("through trust chain", async () => {
			const options = {

				replicate: true,
				store: {},
			};

			const l0a = await init(
				new TestStore({ identity: identity(0) }),
				0,
				options
			);

			await l0a.store.put(
				new Document({
					id: "1",
				})
			);

			const l0b = (await init(
				await TestStore.load(session.peers[1].directblock, l0a.address!),
				1,
				options
			)) as TestStore;
			const l0c = (await init(
				await TestStore.load(session.peers[2].directblock, l0a.address!),
				2,
				options
			)) as TestStore;

			/* await waitForPeers(
				session.peers[1],
				session.peers[0],
				l0a.address.toString()
			);
			await waitForPeers(
				session.peers[2],
				session.peers[0],
				l0a.address.toString()
			); */
			await expect(
				l0c.store.put(
					new Document({
						id: "id",
					})
				)
			).rejects.toBeInstanceOf(AccessError); // Not trusted

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new PublicKeyAccessCondition({
						key: identity(1).publicKey,
					}),
					accessTypes: [AccessType.Any],
				})
			);

			await l0b.accessController.access.store.sync(
				l0a.accessController.access.store.oplog.heads
			);
			await l0c.accessController.access.store.sync(
				l0a.accessController.access.store.oplog.heads
			);

			await expect(
				l0c.store.put(
					new Document({
						id: "id",
					})
				)
			).rejects.toBeInstanceOf(AccessError); // Not trusted

			await waitFor(() => l0b.accessController.access.index.size == 1);
			await l0b.accessController.identityGraphController.addRelation(
				identity(2).publicKey
			);
			await l0c.accessController.identityGraphController.relationGraph.store.sync(
				l0b.accessController.identityGraphController.relationGraph.store
					.oplog.heads
			);

			await waitFor(
				() =>
					l0c.accessController.identityGraphController.relationGraph
						.index.size === 1
			);
			await l0c.store.put(
				new Document({
					id: "2",
				})
			); // Now trusted
		});

		it("any access", async () => {
			const options = {
				replicate: true,
				store: {},
			};

			const l0a = await init(
				new TestStore({ identity: identity(0) }),
				0,
				options
			);
			await l0a.store.put(
				new Document({
					id: "1",
				})
			);

			const l0b = (await init(
				await TestStore.load(session.peers[1].directblock, l0a.address!),
				1,
				options
			)) as TestStore;
			/* 		await waitForPeers(
						session.peers[1],
						session.peers[0],
						l0a.address.toString()
					); */

			await expect(
				l0b.store.put(
					new Document({
						id: "id",
					})
				)
			).rejects.toBeInstanceOf(AccessError); // Not trusted

			const access = new Access({
				accessCondition: new AnyAccessCondition(),
				accessTypes: [AccessType.Any],
			});
			expect(access.id).toBeDefined();
			await l0a.accessController.access.put(access);
			await l0b.accessController.access.store.sync(
				l0a.accessController.access.store.oplog.heads
			);

			await waitFor(() => l0b.accessController.access.index.size === 1);
			await l0b.store.put(
				new Document({
					id: "2",
				})
			); // Now trusted
		});

		it("read access", async () => {
			const options = {

				replicate: true,
				store: {},
			};

			const l0a = await init(
				new TestStore({ identity: identity(0) }),
				0,
				options
			);

			await l0a.store.put(
				new Document({
					id: "1",
				})
			);
			const l0b = await init<TestStore>(
				deserialize(serialize(l0a), TestStore),
				1,
				options
			);
			/* 	await waitForPeers(
					session.peers[1],
					session.peers[0],
					l0a.address.toString()
				);
	 */
			const q = async (): Promise<Results<Document>> => {
				let results: Results<Document> = undefined as any;

				l0b.store.index.query(
					new DocumentQueryRequest({
						queries: [
							new FieldStringMatchQuery({
								key: "id",
								value: "1",
							}),
						],
					}),
					(response) => {
						results = response;
					},
					{
						remote: {
							signer: identity(1),
							timeout: 3000,
						},
						local: false,
					}
				);
				try {
					await waitFor(() => !!results);
				} catch (error) { }
				return results;
			};

			expect(await q()).toBeUndefined(); // Because no read access

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new AnyAccessCondition(),
					accessTypes: [AccessType.Read],
				}).initialize()
			);

			expect(await q()).toBeDefined(); // Because read access
		});
	});

	it("manifests are unique", async () => {
		const options = {

			replicate: true,
			store: {},
		};

		const l0a = await init(
			new TestStore({ identity: identity(0) }),
			0,
			options
		);
		const l0b = await init(
			new TestStore({ identity: identity(0) }),
			0,
			options
		);
		expect(l0a.address).not.toEqual(l0b.address);
	});

	it("can query", async () => {
		const options = {

			replicate: true,
			store: {},
		};

		const l0a = await init(new TestStore({ identity: identity(0) }), 0, {
			...options,
			canRead: () => Promise.resolve(true),
		});
		await l0a.accessController.access.put(
			new Access({
				accessCondition: new AnyAccessCondition(),
				accessTypes: [AccessType.Any],
			}).initialize()
		);

		const dbb = (await TestStore.load(
			session.peers[0].directblock,
			l0a.address!
		)) as TestStore;

		const l0b = await init(dbb, 1, {
			...options,
			replicate: false,
			store: {},
			canRead: () => Promise.resolve(true),
		});

		// Allow all for easy query
		l0b.accessController.access.store.sync(
			l0a.accessController.access.store.oplog.heads
		);
		await waitFor(() => l0a.accessController.access.index.size === 1);
		await waitFor(() => l0b.accessController.access.index.size === 1);

		let results: Results<Document> = undefined as any;
		l0b.accessController.access.index.query(
			new DocumentQueryRequest({
				queries: [],
			}),
			(response) => {
				results = response;
			},
			{
				remote: {
					signer: identity(1),
					amount: 1,
				},
				local: false,
			}
		);

		await waitFor(() => !!results);

		// Now trusted because append all is 'true'c
	});
});
