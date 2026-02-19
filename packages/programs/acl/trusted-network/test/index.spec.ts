import { serialize, variant } from "@dao-xyz/borsh";
import { Wallet } from "@ethersproject/wallet";
import { AccessError, Ed25519Keypair, type Identity } from "@peerbit/crypto";
import { Secp256k1PublicKey } from "@peerbit/crypto";
import { Documents, SearchRequest } from "@peerbit/document";
import { Program } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { equals } from "uint8arrays";
import {
	FromTo,
	IdentityGraph,
	IdentityRelation,
	TrustedNetwork,
	createIdentityGraphStore,
	getFromByTo,
	getPathGenerator,
	getToByFrom,
} from "../src/index.js";

const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		sign: (data: any) => ed.sign(data),
	};
};

// Tests in this workspace run in parallel across packages; allow extra headroom for
// network/replication warmup under CI load.
const REPLICATOR_WAIT_TIMEOUT = 60_000;

@variant("any_identity_graph")
class AnyCanAppendIdentityGraph extends IdentityGraph {
	constructor(props?: {
		id?: Uint8Array;
		relationGraph?: Documents<IdentityRelation, FromTo>;
	}) {
		super(props);
	}
	async canPerform(_props: any): Promise<boolean> {
		return true;
	}
}
describe("index", () => {
	describe("identity-graph", () => {
		let session: TestSession, identites: Identity[], programs: Program[];

		before(async () => {
			session = await TestSession.connected(2);
			identites = [];
			programs = [];

			for (let i = 0; i < session.peers.length; i++) {
				identites.push(await createIdentity());
			}
		});

		afterEach(async () => {
			await Promise.all(programs.map((p) => p.close()));
		});

		after(async () => {
			await session.stop();
		});

		it("path", async () => {
			const a = (await Ed25519Keypair.create()).publicKey;
			const b = await Secp256k1PublicKey.recover(await Wallet.createRandom());
			const c = (await Ed25519Keypair.create()).publicKey;

			const store = new AnyCanAppendIdentityGraph({
				relationGraph: createIdentityGraphStore(),
			});
			await session.peers[0].open(store);

			const ab = new IdentityRelation({
				to: b,
				from: a,
			});
			const bc = new IdentityRelation({
				to: c,
				from: b,
			});
			await store.relationGraph.put(ab);
			await store.relationGraph.put(bc);

			// Get relations one by one
			const trustingC = await getFromByTo.resolve(c, store.relationGraph);
			expect(trustingC).to.have.length(1);
			expect(trustingC[0].id).to.deep.equal(bc.id);

			const bIsTrusting = await getToByFrom.resolve(b, store.relationGraph);
			expect(bIsTrusting).to.have.length(1);
			expect(bIsTrusting[0].id).to.deep.equal(bc.id);

			const trustingB = await getFromByTo.resolve(b, store.relationGraph);
			expect(trustingB).to.have.length(1);
			expect(trustingB[0].id).to.deep.equal(ab.id);

			const aIsTrusting = await getToByFrom.resolve(a, store.relationGraph);
			expect(aIsTrusting).to.have.length(1);
			expect(aIsTrusting[0].id).to.deep.equal(ab.id);

			// Test generator
			const relationsFromGeneratorFromByTo: IdentityRelation[] = [];
			for await (const relation of getPathGenerator(
				c,
				store.relationGraph,
				getFromByTo,
			)) {
				relationsFromGeneratorFromByTo.push(relation);
			}
			expect(relationsFromGeneratorFromByTo).to.have.length(2);
			expect(relationsFromGeneratorFromByTo[0].id).to.deep.equal(bc.id);
			expect(relationsFromGeneratorFromByTo[1].id).to.deep.equal(ab.id);

			const relationsFromGeneratorToByFrom: IdentityRelation[] = [];
			for await (const relation of getPathGenerator(
				a,
				store.relationGraph,
				getToByFrom,
			)) {
				relationsFromGeneratorToByFrom.push(relation);
			}
			expect(relationsFromGeneratorToByFrom).to.have.length(2);
			expect(relationsFromGeneratorToByFrom[0].id).to.deep.equal(ab.id);
			expect(relationsFromGeneratorToByFrom[1].id).to.deep.equal(bc.id);
		});

		it("can revoke", async () => {
			const a = (await Ed25519Keypair.create()).publicKey;
			const b = await Secp256k1PublicKey.recover(await Wallet.createRandom());

			const store = new AnyCanAppendIdentityGraph({
				relationGraph: createIdentityGraphStore(),
			});
			await session.peers[0].open(store);

			const ab = new IdentityRelation({
				to: b,
				from: a,
			});

			await store.relationGraph.put(ab);

			let trustingB = await getFromByTo.resolve(b, store.relationGraph);
			expect(trustingB).to.have.length(1);
			expect(trustingB[0].id).to.deep.equal(ab.id);

			await store.relationGraph.del(ab.id);
			trustingB = await getFromByTo.resolve(b, store.relationGraph);
			expect(trustingB).to.be.empty;
		});

		it("can get path as observer", async () => {
			const replicator = new AnyCanAppendIdentityGraph({
				relationGraph: createIdentityGraphStore(),
			});
			await session.peers[0].open(replicator);
			const ab = new IdentityRelation({
				to: (await Ed25519Keypair.create()).publicKey,
				from: (await Ed25519Keypair.create()).publicKey,
			});
			await replicator.relationGraph.put(ab);

			const observer = await AnyCanAppendIdentityGraph.open(
				replicator.address,
				session.peers[1],
				{ args: { replicate: false } },
			);
			await (
				observer.relationGraph as Documents<IdentityRelation, FromTo>
			).log.waitForReplicator(session.peers[0].identity.publicKey);

			let pathFrom = await getFromByTo.resolve(ab.to, observer.relationGraph);
			expect(pathFrom).to.have.length(1);

			let pathTo = await getToByFrom.resolve(ab.from, observer.relationGraph);
			expect(pathTo).to.have.length(1);
		});
	});

		describe("TrustedNetwork", () => {
			let session: TestSession;
			// Create a fresh session per test to avoid cross-test state/leaks causing flakiness
			// when the full workspace runs many packages in parallel.
			beforeEach(async () => {
				session = await TestSession.connected(4);
			});

			afterEach(async () => {
				await session.stop();
			});

		it("can be deterministic", async () => {
			const key = (await Ed25519Keypair.create()).publicKey;
			const t1 = new TrustedNetwork({ id: key.publicKey, rootTrust: key });
			const t2 = new TrustedNetwork({ id: key.publicKey, rootTrust: key });

			expect(equals(serialize(t1), serialize(t2))).to.be.true;
		});

			it("replicates by default", async () => {
				const l0a = new TrustedNetwork({ rootTrust: session.peers[0].peerId });
				try {
					await session.peers[0].open(l0a);
					expect(
						await (
							l0a.trustGraph as Documents<IdentityRelation, FromTo>
						).log.isReplicating(),
					).to.be.true;
					expect(
						(
							await (
								l0a.trustGraph as Documents<IdentityRelation, FromTo>
							).log.getMyReplicationSegments()
						).reduce((a, b) => a + b.widthNormalized, 0),
					).to.equal(1);
				} finally {
					await l0a.close();
				}
			});

				it("trusted by chain", async function () {
					// This test performs multiple networked operations and is sensitive to overall CI load.
					this.timeout(180_000);

				const l0a = new TrustedNetwork({ rootTrust: session.peers[0].peerId });
				let l0b: TrustedNetwork | undefined;
				let l0c: TrustedNetwork | undefined;
				let l0d: TrustedNetwork | undefined;
				try {
					await session.peers[0].open(l0a);

					await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
					l0b = await TrustedNetwork.open(l0a.address!, session.peers[1]);

					await session.peers[2].services.blocks.waitFor(session.peers[0].peerId);
					l0c = await TrustedNetwork.open(l0a.address!, session.peers[2], {
						args: { replicate: false },
					});

					await session.peers[3].services.blocks.waitFor(session.peers[0].peerId);
					l0d = await TrustedNetwork.open(l0a.address!, session.peers[3]);

					await l0c.waitFor([session.peers[0].peerId, session.peers[1].peerId]);

					await l0a.add(session.peers[1].peerId);

					await (l0b.trustGraph as Documents<IdentityRelation, FromTo>).log.log.join(
						await (l0a.trustGraph as Documents<IdentityRelation, FromTo>).log.log
							.getHeads()
							.all(),
					);

					await waitForResolved(async () =>
						expect(await l0b.trustGraph.index.getSize()).equal(1),
					);

					await l0b.add(session.peers[2].peerId); // Will only work if peer2 is trusted

					await (l0a.trustGraph as Documents<IdentityRelation, FromTo>).log.log.join(
						await (l0b.trustGraph as Documents<IdentityRelation, FromTo>).log.log
							.getHeads()
							.all(),
					);

					await waitForResolved(async () =>
						expect(await l0b.trustGraph.index.getSize()).equal(2),
					);
					await waitForResolved(async () =>
						expect(await l0a.trustGraph.index.getSize()).equal(2),
					);

					// Wait for all expected replicators concurrently to minimize total wall time.
					await Promise.all([
						(
							l0c.trustGraph as Documents<IdentityRelation, FromTo>
						).log.waitForReplicator(session.peers[0].identity.publicKey, {
							timeout: REPLICATOR_WAIT_TIMEOUT,
						}),
						(
							l0c.trustGraph as Documents<IdentityRelation, FromTo>
						).log.waitForReplicator(session.peers[1].identity.publicKey, {
							timeout: REPLICATOR_WAIT_TIMEOUT,
						}),
						(
							l0c.trustGraph as Documents<IdentityRelation, FromTo>
						).log.waitForReplicator(session.peers[3].identity.publicKey, {
							timeout: REPLICATOR_WAIT_TIMEOUT,
						}),
					]);

					await waitForResolved(
						async () => expect(await l0c.trustGraph.index.getSize()).equal(2),
						{ timeout: REPLICATOR_WAIT_TIMEOUT },
					);

					// Try query with trusted
					const responses: IdentityRelation[] = await l0c.trustGraph.index.search(
						new SearchRequest({ query: [] }),
					);

					expect(responses).to.have.length(2);
				} finally {
					await l0d?.close();
					await l0c?.close();
					await l0b?.close();
					await l0a.close();
				}

				// Try query with untrusted
				// TODO we are not using read access control on the trust graph anymore, but should we?
				/* let untrustedResponse: Results<IdentityRelation>[] =
				await l0d.trustGraph.index.search(
					new SearchRequest({
						query: [],
					}),
					{
						remote: { timeout: 10 * 1000, signer: identity(3) },
					}
				);

			expect(untrustedResponse).to.be.empty;

			// now check if peer3 is trusted from peer perspective
			expect(await l0a.isTrusted(identity(2).publicKey)).to.be.true;

			// check if peer3 is trusted from someone else
			expect(await l0a.isTrusted(identity(2).publicKey)).to.be.true;
			expect(await l0a.isTrusted(identity(3).publicKey)).to.be.false;

			const trusted = await l0a.getTrusted();
			expect(trusted.map((k) => k.bytes)).to.have.members([
				identity(0).publicKey.bytes,
				identity(1).publicKey.bytes,
				identity(2).publicKey.bytes,
			]); */
		});

			it("has relation", async () => {
				const l0a = new TrustedNetwork({ rootTrust: session.peers[0].peerId });
				try {
					await session.peers[0].open(l0a);

					await l0a.add(session.peers[1].peerId);
					expect(
						await l0a.hasRelation(
							session.peers[1].peerId,
							session.peers[0].peerId,
						),
					).to.be.false;
					expect(
						await l0a.hasRelation(
							session.peers[0].peerId,
							session.peers[1].peerId,
						),
					).to.be.true;
				} finally {
					await l0a.close();
				}
			});

			it("can not append with wrong truster", async () => {
				const l0a = new TrustedNetwork({ rootTrust: session.peers[0].peerId });
				try {
					await session.peers[0].open(l0a);

					await expect(
						l0a.trustGraph.put(
							new IdentityRelation({
								to: await Secp256k1PublicKey.recover(
									await Wallet.createRandom(),
								),
								from: await Secp256k1PublicKey.recover(
									await Wallet.createRandom(),
								),
							}),
						),
					).eventually.rejectedWith(AccessError);
				} finally {
					await l0a.close();
				}
			});

			it("untrusteed by chain", async () => {
				const l0a = new TrustedNetwork({ rootTrust: session.peers[0].peerId });
				let l0b: TrustedNetwork | undefined;
				try {
					await session.peers[0].open(l0a);

					l0b = await TrustedNetwork.open(l0a.address!, session.peers[1]);

					// Can not append peer3Key since its not trusted by the root
					await expect(l0b.add(session.peers[2].peerId)).eventually.rejectedWith(
						AccessError,
					);
				} finally {
					await l0b?.close();
					await l0a.close();
				}
			});
	});
});
