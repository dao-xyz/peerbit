import { field, serialize, variant } from "@dao-xyz/borsh";
import { type PeerId } from "@libp2p/interface";
import { AccessError, Ed25519Keypair, PublicSignKey } from "@peerbit/crypto";
import { Documents, SearchRequest } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { type ReplicationOptions, SharedLog } from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Access, AccessType } from "../src/access.js";
import { IdentityAccessController } from "../src/acl-db.js";
import {
	AnyAccessCondition,
	PublicKeyAccessCondition,
} from "../src/condition.js";

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

@variant("test_store")
class TestStore extends Program<{ replicate: ReplicationOptions }> {
	@field({ type: Documents })
	store: Documents<Document>;

	@field({ type: IdentityAccessController })
	accessController: IdentityAccessController;

	constructor(properties: { publicKey: PublicSignKey | PeerId }) {
		super();
		this.store = new Documents();
		this.accessController = new IdentityAccessController({
			rootTrust: properties.publicKey,
		});
	}

	async open(properties?: { replicate: ReplicationOptions }) {
		await this.accessController.open();
		await this.store.open({
			type: Document,
			canPerform: (properties) => this.accessController.canPerform(properties),
			index: {
				canRead: this.accessController.canRead.bind(this.accessController),
			},
			replicate: properties?.replicate,
		});
	}
}

describe("index", () => {
	let session: TestSession;

	before(async () => {
		session = await TestSession.connected(3);
	});

	after(async () => {
		await session.stop();
	});

	it("can be deterministic", async () => {
		const key = (await Ed25519Keypair.create()).publicKey;
		const t1 = new IdentityAccessController({
			id: key.publicKey,
			rootTrust: key,
		});
		const t2 = new IdentityAccessController({
			id: key.publicKey,
			rootTrust: key,
		});
		expect(serialize(t1)).to.deep.equal(serialize(t2));
	});

	it("replicates by default", async () => {
		const s = new TestStore({ publicKey: session.peers[0].peerId });
		const l0a = await session.peers[0].open(s);
		const checkRole = async (log: SharedLog<any>) => {
			expect(await log.isReplicating()).to.be.true;
			expect(
				(await log.getMyReplicationSegments()).reduce(
					(a, b) => a + b.widthNormalized,
					0,
				),
			).to.equal(1);
		};
		await checkRole(l0a.accessController.access.log);
		await checkRole(
			l0a.accessController.identityGraphController.relationGraph.log,
		);
		await checkRole(l0a.accessController.trustedNetwork.trustGraph.log);
	});

	it("can write from trust web", async () => {
		const s = new TestStore({ publicKey: session.peers[0].peerId });

		const l0a = await session.peers[0].open(s);
		const l0b = await TestStore.open(l0a.address!, session.peers[1]);

		await l0a.store.put(
			new Document({
				id: "1",
			}),
		);

		await expect(
			l0b.store.put(
				new Document({
					id: "id",
				}),
			),
		).eventually.rejectedWith(AccessError); // Not trusted

		await l0a.accessController.trustedNetwork.add(session.peers[1].peerId);

		await l0b.accessController.trustedNetwork.trustGraph.log.log.join(
			await l0a.accessController.trustedNetwork.trustGraph.log.log
				.getHeads()
				.all(),
		);

		await waitFor(
			() => l0b.accessController.trustedNetwork.trustGraph.log.log.length === 1,
		);
		await l0b.store.put(
			new Document({
				id: "2",
			}),
		); // Now trusted

		await l0a.store.log.log.join(await l0b.store.log.log.getHeads().all());
		await l0b.store.log.log.join(await l0a.store.log.log.getHeads().all());

		await waitForResolved(async () =>
			expect(await l0a.store.index.getSize()).equal(2),
		);
		await waitForResolved(async () =>
			expect(await l0b.store.index.getSize()).equal(2),
		);
	});

	describe("conditions", () => {
		it("publickey", async () => {
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				}),
			);

			await l0a.store.put(
				new Document({
					id: "1",
				}),
			);

			const l0b = await TestStore.open(l0a.address!, session.peers[1]);

			await l0b.store.log.log.join(await l0a.store.log.log.getHeads().all());

			await waitForResolved(async () =>
				expect(await l0b.store.index.getSize()).equal(1),
			);

			await expect(
				l0b.store.put(
					new Document({
						id: "id",
					}),
				),
			).eventually.rejectedWith(AccessError); // Not trusted

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new PublicKeyAccessCondition({
						key: session.peers[1].peerId,
					}),
					accessTypes: [AccessType.Any],
				}),
			);

			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads().all(),
			);
			await waitForResolved(async () =>
				expect(await l0b.store.index.getSize()).equal(1),
			);

			await l0b.store.put(
				new Document({
					id: "2",
				}),
			);
			// Now trusted
		});

		it("through trust chain", async () => {
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				}),
			);

			await l0a.store.put(
				new Document({
					id: "1",
				}),
			);

			const l0b = await TestStore.open(l0a.address!, session.peers[1]);
			const l0c = await TestStore.open(l0a.address!, session.peers[2]);

			await expect(
				l0c.store.put(
					new Document({
						id: "id",
					}),
				),
			).eventually.rejectedWith(AccessError); // Not trusted

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new PublicKeyAccessCondition({
						key: session.peers[1].peerId,
					}),
					accessTypes: [AccessType.Any],
				}),
			);

			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads().all(),
			);
			await l0c.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads().all(),
			);

			await expect(
				l0c.store.put(
					new Document({
						id: "id",
					}),
				),
			).eventually.rejectedWith(AccessError); // Not trusted

			await waitForResolved(async () =>
				expect(await l0b.accessController.access.index.getSize()).equal(1),
			);

			await l0b.accessController.identityGraphController.addRelation(
				session.peers[2].peerId,
			);
			await l0c.accessController.identityGraphController.relationGraph.log.log.join(
				await l0b.accessController.identityGraphController.relationGraph.log.log
					.getHeads()
					.all(),
			);

			await waitForResolved(async () =>
				expect(
					await l0c.accessController.identityGraphController.relationGraph.index.getSize(),
				).equal(1),
			);

			await l0c.store.put(
				new Document({
					id: "2",
				}),
			);
		});

		/* it("any access", async () => {
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				}),
			);

			await l0a.store.put(
				new Document({
					id: "1",
				}),
			);

			const l0b = await TestStore.open(l0a.address!, session.peers[1]);

			await expect(
				l0b.store.put(
					new Document({
						id: "id",
					}),
				),
			).eventually.rejectedWith(AccessError); // Not trusted

			const access = new Access({
				accessCondition: new AnyAccessCondition(),
				accessTypes: [AccessType.Any],
			});
			expect(access.id).to.exist;
			await l0a.accessController.access.put(access);
			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads().all(),
			);

			await waitForResolved(async () =>
				expect(await l0b.accessController.access.index.getSize()).equal(1),
			);
			await l0b.store.put(
				new Document({
					id: "2",
				}),
			); // Now trusted
		});

		it("read access", async () => {
			const l0a = await session.peers[0].open(
				new TestStore({
					publicKey: session.peers[0].peerId,
				}),
			);

			await l0a.store.put(
				new Document({
					id: "1",
				}),
			);
			const l0b = await TestStore.open(l0a.address!, session.peers[1], {
				args: { replicate: false },
			});

			await l0b.store.log.waitForReplicator(
				session.peers[0].identity.publicKey,
			);

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
					},
				);
			};

			//expect(await q()).equal(undefined); // Because no read access

			await l0a.accessController.access.put(
				new Access({
					accessCondition: new AnyAccessCondition(),
					accessTypes: [AccessType.Read],
				}).initialize(),
			);
			await l0b.accessController.access.log.log.join(
				await l0a.accessController.access.log.log.getHeads().all(),
			);
			await waitForResolved(async () =>
				expect(await l0b.accessController.access.index.getSize()).equal(1),
			);

			const result = await q();
			expect(result.length).greaterThan(0); // Because read access
		}); */
	});

	it("manifests are not unique", async () => {
		const l0a = await session.peers[0].open(
			new TestStore({
				publicKey: session.peers[0].peerId,
			}),
		);

		const l0b = await TestStore.open(l0a.address!, session.peers[1]);

		expect(l0a.address).equal(l0b.address);
	});

	it("can query", async () => {
		const l0a = await session.peers[0].open(
			new TestStore({
				publicKey: session.peers[0].peerId,
			}),
		);

		await l0a.accessController.access.put(
			new Access({
				accessCondition: new AnyAccessCondition(),
				accessTypes: [AccessType.Any],
			}).initialize(),
		);

		const l0b = await TestStore.open(l0a.address!, session.peers[1], {
			args: {
				replicate: false,
			},
		});

		// Allow all for easy query
		await l0b.accessController.access.log.waitForReplicator(
			session.peers[0].identity.publicKey,
		);

		await waitForResolved(async () =>
			expect(await l0a.accessController.access.index.getSize()).equal(1),
		);
		await waitForResolved(async () =>
			expect(await l0b.accessController.access.index.getSize()).equal(1),
		);

		await l0b.accessController.access.log.waitForReplicator(
			l0a.node.identity.publicKey,
		);

		// since we are replicator by default of the access index (even though opened the db as observer)
		// we will be able to query ourselves
		// TODO should we really be replicator of the access index?
		// can we create a solution where this is not the case?
		let results: Document[] = await l0b.accessController.access.index.search(
			new SearchRequest({
				query: [],
			}),
		);

		expect(results.length).greaterThan(0);

		// Now trusted because append all is 'true'c
	});
});
