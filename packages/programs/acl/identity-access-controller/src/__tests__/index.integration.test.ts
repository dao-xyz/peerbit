import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { Access, AccessType } from "../access";
import { AnyAccessCondition, PublicKeyAccessCondition } from "../condition";
import { waitFor } from "@dao-xyz/peerbit-time";
import {
	AccessError,
	Ed25519Keypair,
	randomBytes,
} from "@dao-xyz/peerbit-crypto";
import {
	Documents,
	DocumentIndex,
	SearchRequest,
	StringMatch,
} from "@dao-xyz/peerbit-document";
import type { CanAppend, Identity } from "@dao-xyz/peerbit-log";
import { CanRead, RPC } from "@dao-xyz/peerbit-rpc";
import {
	ObserverType,
	Program,
	ReplicatorType,
	SubscriptionType,
} from "@dao-xyz/peerbit-program";
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

	constructor(properties: { identity: Identity }) {
		super();
		if (properties) {
			this.store = new Documents({
				index: new DocumentIndex({
					indexBy: "id",
					query: new RPC(),
				}),
			});
			this.accessController = new IdentityAccessController({
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
	let session: LSession, programs: Program[], identites: Identity[];
	let replicators: string[][];

	const identity = (i: number) => identites[i];
	const init = async <T extends Program>(
		store: T,
		i: number,
		options: {
			role: SubscriptionType;
			canRead?: CanRead;
			canAppend?: CanAppend<T>;
		}
	) => {
		programs.push(store);
		const result = await store.init(session.peers[i], identites[i], {
			...options,
			log: {
				replication: {
					replicators: () => replicators,
				},
			},
		});
		return result;
	};

	beforeAll(async () => {
		session = await LSession.connected(3);
		identites = [];
		programs = [];
		for (let i = 0; i < session.peers.length; i++) {
			identites.push(await createIdentity());
		}
	});
	beforeEach(() => {
		replicators = [];
	});

	afterEach(async () => {
		await Promise.all(programs?.map((c) => c.close()));
	});

	afterAll(async () => {
		await session.stop();
	});

	it("can be deterministic", async () => {
		const key = (await Ed25519Keypair.create()).publicKey;
		let id = randomBytes(32);
		const t1 = new IdentityAccessController({ rootTrust: key });
		const t2 = new IdentityAccessController({ rootTrust: key });
		await t1.initializeIds();
		await t2.initializeIds();
		expect(serialize(t1)).toEqual(serialize(t2));
	});

	it("can write from trust web", async () => {
		const s = new TestStore({ identity: identity(0) });
		const options = {
			role: new ReplicatorType(),
			log: {},
		};
		const l0a = await init(s, 0, options);

		const l0b = (await init(
			(await TestStore.load(session.peers[1].services.blocks, l0a.address!))!,
			1,
			options
		)) as TestStore;

		replicators = [];

		await l0a.store.put(
			new Document({
				id: "1",
			})
		);

		await expect(
			l0b.store.put(
				new Document({
					id: "id",
				})
			)
		).rejects.toBeInstanceOf(AccessError); // Not trusted

		await l0a.accessController.trustedNetwork.add(identity(1).publicKey);
		await l0a.accessController.trustedNetwork.add(
			session.peers[1].services.blocks.publicKey
		);

		await l0b.accessController.trustedNetwork.trustGraph.log.join(
			await l0a.accessController.trustedNetwork.trustGraph.log.getHeads()
		);

		replicators = [
			[session.peers[0].services.blocks.publicKeyHash],
			[session.peers[1].services.blocks.publicKeyHash],
		];

		await waitFor(
			() => l0b.accessController.trustedNetwork.trustGraph.log.length === 2
		);
		await l0b.store.put(
			new Document({
				id: "2",
			})
		); // Now trusted

		await l0a.store.log.join(await l0b.store.log.getHeads());
		await l0b.store.log.join(await l0a.store.log.getHeads());

		await waitFor(() => l0a.store.index.size === 2);
		await waitFor(() => l0b.store.index.size === 2);
	});

	describe("conditions", () => {
		it("publickey", async () => {
			const options = {
				role: new ReplicatorType(),
				log: {},
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
				(await TestStore.load(session.peers[1].services.blocks, l0a.address!))!,
				1,
				options
			)) as TestStore;

			await l0b.store.log.join(await l0a.store.log.getHeads());

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

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new PublicKeyAccessCondition({
						key: session.peers[1].services.blocks.publicKey,
					}),
					accessTypes: [AccessType.Any],
				})
			);

			await l0b.accessController.access.log.join(
				await l0a.accessController.access.log.getHeads()
			);
			await waitFor(() => l0b.accessController.access.index.size === 2);
			await l0b.store.put(
				new Document({
					id: "2",
				})
			); // Now trusted
		});

		it("through trust chain", async () => {
			const options = {
				role: new ReplicatorType(),
				log: {},
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
				(await TestStore.load(session.peers[1].services.blocks, l0a.address!))!,
				1,
				options
			)) as TestStore;
			programs.push(l0a);

			const l0c = (await init(
				(await TestStore.load(session.peers[2].services.blocks, l0a.address!))!,
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

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new PublicKeyAccessCondition({
						key: session.peers[1].services.blocks.publicKey,
					}),
					accessTypes: [AccessType.Any],
				})
			);

			await l0b.accessController.access.log.join(
				await l0a.accessController.access.log.getHeads()
			);
			await l0c.accessController.access.log.join(
				await l0a.accessController.access.log.getHeads()
			);

			await expect(
				l0c.store.put(
					new Document({
						id: "id",
					})
				)
			).rejects.toBeInstanceOf(AccessError); // Not trusted

			await waitFor(() => l0b.accessController.access.index.size == 2);
			await l0b.accessController.identityGraphController.addRelation(
				identity(2).publicKey
			);
			await l0c.accessController.identityGraphController.relationGraph.log.join(
				await l0b.accessController.identityGraphController.relationGraph.log.getHeads()
			);

			await waitFor(
				() =>
					l0c.accessController.identityGraphController.relationGraph.index
						.size === 1
			);
			await l0c.store.put(
				new Document({
					id: "2",
				})
			); // Now trusted
		});

		it("any access", async () => {
			const options = {
				role: new ReplicatorType(),
				log: {},
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
				(await TestStore.load(session.peers[1].services.blocks, l0a.address!))!,
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
			await l0b.accessController.access.log.join(
				await l0a.accessController.access.log.getHeads()
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
				role: new ReplicatorType(),
				log: {},
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

			replicators = [[session.peers[0].services.blocks.publicKeyHash]];

			const q = async (): Promise<Document[]> => {
				return l0b.store.index.query(
					new SearchRequest({
						queries: [
							new StringMatch({
								key: "id",
								value: "1",
							}),
						],
					}),
					{
						remote: {
							amount: 1,
							timeout: 3000,
						},
						local: false,
					}
				);
			};

			//expect(await q()).toBeUndefined(); // Because no read access

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new AnyAccessCondition(),
					accessTypes: [AccessType.Read],
				}).initialize()
			);
			await l0b.accessController.access.log.join(
				await l0a.accessController.access.log.getHeads()
			);
			await waitFor(() => l0b.accessController.access.index.size === 1);

			const result = await q();
			expect(result.length).toBeGreaterThan(0); // Because read access
		});
	});

	it("manifests are not unique", async () => {
		const options = {
			role: new ReplicatorType(),
			log: {},
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
		expect(l0a.address).toEqual(l0b.address);
	});

	it("can query", async () => {
		const options = {
			role: new ReplicatorType(),
			log: {},
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

		replicators = [[session.peers[0].services.pubsub.publicKeyHash]];

		const dbb = (await TestStore.load(
			session.peers[0].services.blocks,
			l0a.address!
		)) as TestStore;

		const l0b = await init(dbb, 1, {
			...options,
			role: new ObserverType(),
			canRead: () => Promise.resolve(true),
		});

		// Allow all for easy query
		l0b.accessController.access.log.join(
			await l0a.accessController.access.log.getHeads()
		);
		await waitFor(() => l0a.accessController.access.index.size === 1);
		await waitFor(() => l0b.accessController.access.index.size === 1);

		let results: Document[] = await l0b.accessController.access.index.query(
			new SearchRequest({
				queries: [],
			}),
			{
				remote: {
					amount: 1,
				},
				local: false,
			}
		);

		await waitFor(() => results.length > 0);

		// Now trusted because append all is 'true'c
	});
});
