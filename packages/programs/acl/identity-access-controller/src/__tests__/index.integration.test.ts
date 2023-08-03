import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { LSession } from "@peerbit/test-utils";
import { Access, AccessType } from "../access";
import { AnyAccessCondition, PublicKeyAccessCondition } from "../condition";
import { delay, waitFor } from "@peerbit/time";
import {
	AccessError,
	Ed25519Keypair,
	Identity,
	PublicSignKey,
	randomBytes,
} from "@peerbit/crypto";
import {
	Documents,
	DocumentIndex,
	SearchRequest,
	StringMatch,
	Observer,
	Role,
} from "@peerbit/document";
import { RPC } from "@peerbit/rpc";
import { Program } from "@peerbit/program";
import { IdentityAccessController } from "../acl-db";
import { PeerId } from "@libp2p/interface-peer-id";

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
class TestStore extends Program<{ role: Role }> {
	@field({ type: Documents })
	store: Documents<Document>;

	@field({ type: IdentityAccessController })
	accessController: IdentityAccessController;

	constructor(properties: { publicKey: PublicSignKey | PeerId }) {
		super();
		if (properties) {
			this.store = new Documents({
				index: new DocumentIndex({
					query: new RPC(),
				}),
			});
			this.accessController = new IdentityAccessController({
				rootTrust: properties.publicKey,
			});
		}
	}

	async open(properties?: { role: Role }) {
		await this.accessController.open(properties);
		await this.store.open({
			type: Document,
			canRead: this.accessController.canRead.bind(this.accessController),
			canAppend: (entry) => this.accessController.canWrite(entry),
			role: properties?.role,
		});
	}
}

describe("index", () => {
	let session: LSession;

	beforeAll(async () => {
		session = await LSession.connected(3);
	});

	afterAll(async () => {
		await session.stop();
	});

	it("can be deterministic", async () => {
		const key = (await Ed25519Keypair.create()).publicKey;
		let id = randomBytes(32);
		const t1 = new IdentityAccessController({
			id: key.publicKey,
			rootTrust: key,
		});
		const t2 = new IdentityAccessController({
			id: key.publicKey,
			rootTrust: key,
		});
		expect(serialize(t1)).toEqual(serialize(t2));
	});

	it("can write from trust web", async () => {
		const s = new TestStore({ publicKey: session.peers[0].peerId });

		const l0a = await session.peers[0].open(s);
		const l0b = await TestStore.open(l0a.address!, session.peers[1]);

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

		await l0a.accessController.trustedNetwork.add(session.peers[1].peerId);

		await l0b.accessController.trustedNetwork.trustGraph.log.log.join(
			await l0a.accessController.trustedNetwork.trustGraph.log.log.getHeads()
		);

		await waitFor(
			() => l0b.accessController.trustedNetwork.trustGraph.log.log.length === 1
		);
		await l0b.store.put(
			new Document({
				id: "2",
			})
		); // Now trusted

		await l0a.store.log.log.join(await l0b.store.log.log.getHeads());
		await l0b.store.log.log.join(await l0a.store.log.log.getHeads());

		await waitFor(() => l0a.store.index.size === 2);
		await waitFor(() => l0b.store.index.size === 2);
	});

	describe("conditions", () => {
		it("publickey", async () => {
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				})
			);

			await l0a.store.put(
				new Document({
					id: "1",
				})
			);

			const l0b = await TestStore.open(l0a.address!, session.peers[1]);

			await l0b.store.log.log.join(await l0a.store.log.log.getHeads());

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
						key: session.peers[1].peerId,
					}),
					accessTypes: [AccessType.Any],
				})
			);

			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads()
			);
			await waitFor(() => l0b.accessController.access.index.size === 1);
			await l0b.store.put(
				new Document({
					id: "2",
				})
			); // Now trusted
		});

		it("through trust chain", async () => {
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				})
			);

			await l0a.store.put(
				new Document({
					id: "1",
				})
			);

			const l0b = await TestStore.open(l0a.address!, session.peers[1]);
			const l0c = await TestStore.open(l0a.address!, session.peers[2]);

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
						key: session.peers[1].peerId,
					}),
					accessTypes: [AccessType.Any],
				})
			);

			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads()
			);
			await l0c.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads()
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
				session.peers[2].peerId
			);
			await l0c.accessController.identityGraphController.relationGraph.log.log.join(
				await l0b.accessController.identityGraphController.relationGraph.log.log.getHeads()
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
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				})
			);

			await l0a.store.put(
				new Document({
					id: "1",
				})
			);

			const l0b = await TestStore.open(l0a.address!, session.peers[1]);

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
			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads()
			);

			await waitFor(() => l0b.accessController.access.index.size === 1);
			await l0b.store.put(
				new Document({
					id: "2",
				})
			); // Now trusted
		});

		it("read access", async () => {
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				})
			);

			await l0a.store.put(
				new Document({
					id: "1",
				})
			);
			const l0b = await TestStore.open(l0a.address!, session.peers[1], {
				args: { role: new Observer() },
			});

			await l0b.waitFor(session.peers[0].peerId);

			const q = async (): Promise<Document[]> => {
				return l0b.store.index.search(
					new SearchRequest({
						query: [
							new StringMatch({
								key: "id",
								value: "1",
							}),
						],
					}),
					{
						remote: true,
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
			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads()
			);
			await waitFor(() => l0b.accessController.access.index.size === 1);

			const abc = await l0a.store.index.search(
				new SearchRequest({
					query: [
						new StringMatch({
							key: "id",
							value: "1",
						}),
					],
				})
			);

			const result = await q();
			expect(result.length).toBeGreaterThan(0); // Because read access
		});
	});

	it("manifests are not unique", async () => {
		const l0a = await session.peers[0].open(
			new TestStore({
				publicKey: session.peers[0].peerId,
			})
		);

		const l0b = await TestStore.open(l0a.address!, session.peers[1]);

		expect(l0a.address).toEqual(l0b.address);
	});

	it("can query", async () => {
		const l0a = await session.peers[0].open(
			new TestStore({
				publicKey: session.peers[0].peerId,
			})
		);

		await l0a.accessController.access.put(
			new Access({
				accessCondition: new AnyAccessCondition(),
				accessTypes: [AccessType.Any],
			}).initialize()
		);

		const l0b = await TestStore.open(l0a.address!, session.peers[1], {
			args: {
				role: new Observer(),
			},
		});

		// Allow all for easy query
		await l0b.waitFor(session.peers[0].peerId);
		await l0b.accessController.access.log.log.join(
			await l0a.accessController.access.log.log.getHeads()
		);
		await waitFor(() => l0a.accessController.access.index.size === 1);
		await waitFor(() => l0b.accessController.access.index.size === 1);

		let results: Document[] = await l0b.accessController.access.index.search(
			new SearchRequest({
				query: [],
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
