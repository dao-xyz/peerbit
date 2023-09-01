import { LSession } from "@peerbit/test-utils";
import {
	IdentityRelation,
	createIdentityGraphStore,
	getFromByTo,
	getPathGenerator,
	getToByFrom,
	TrustedNetwork,
	IdentityGraph
} from "..";
import { waitFor, waitForResolved } from "@peerbit/time";
import { AccessError, Ed25519Keypair, Identity } from "@peerbit/crypto";
import { Secp256k1PublicKey } from "@peerbit/crypto";
import { Entry } from "@peerbit/log";
import { Wallet } from "@ethersproject/wallet";
import { serialize, variant } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Documents, SearchRequest, Operation } from "@peerbit/document";
import { Observer } from "@peerbit/shared-log";

const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		sign: (data) => ed.sign(data)
	};
};

@variant("any_identity_graph")
class AnyCanAppendIdentityGraph extends IdentityGraph {
	constructor(props?: {
		id?: Uint8Array;
		relationGraph?: Documents<IdentityRelation>;
	}) {
		super(props);
	}
	async canPerform(operation, context): Promise<boolean> {
		return true;
	}
}
describe("index", () => {
	describe("identity-graph", () => {
		let session: LSession, identites: Identity[], programs: Program[];

		beforeAll(async () => {
			session = await LSession.connected(1);
			identites = [];
			programs = [];

			for (let i = 0; i < session.peers.length; i++) {
				identites.push(await createIdentity());
			}
		});

		afterEach(async () => {
			await Promise.all(programs.map((p) => p.close()));
		});

		afterAll(async () => {
			await session.stop();
		});

		it("path", async () => {
			const a = (await Ed25519Keypair.create()).publicKey;
			const b = await Secp256k1PublicKey.recover(await Wallet.createRandom());
			const c = (await Ed25519Keypair.create()).publicKey;

			const store = new AnyCanAppendIdentityGraph({
				relationGraph: createIdentityGraphStore()
			});
			await session.peers[0].open(store);

			const ab = new IdentityRelation({
				to: b,
				from: a
			});
			const bc = new IdentityRelation({
				to: c,
				from: b
			});
			await store.relationGraph.put(ab);
			await store.relationGraph.put(bc);

			// Get relations one by one
			const trustingC = await getFromByTo.resolve(c, store.relationGraph);
			expect(trustingC).toHaveLength(1);
			expect(trustingC[0].id).toEqual(bc.id);

			const bIsTrusting = await getToByFrom.resolve(b, store.relationGraph);
			expect(bIsTrusting).toHaveLength(1);
			expect(bIsTrusting[0].id).toEqual(bc.id);

			const trustingB = await getFromByTo.resolve(b, store.relationGraph);
			expect(trustingB).toHaveLength(1);
			expect(trustingB[0].id).toEqual(ab.id);

			const aIsTrusting = await getToByFrom.resolve(a, store.relationGraph);
			expect(aIsTrusting).toHaveLength(1);
			expect(aIsTrusting[0].id).toEqual(ab.id);

			// Test generator
			const relationsFromGeneratorFromByTo: IdentityRelation[] = [];
			for await (const relation of getPathGenerator(
				c,
				store.relationGraph,
				getFromByTo
			)) {
				relationsFromGeneratorFromByTo.push(relation);
			}
			expect(relationsFromGeneratorFromByTo).toHaveLength(2);
			expect(relationsFromGeneratorFromByTo[0].id).toEqual(bc.id);
			expect(relationsFromGeneratorFromByTo[1].id).toEqual(ab.id);

			const relationsFromGeneratorToByFrom: IdentityRelation[] = [];
			for await (const relation of getPathGenerator(
				a,
				store.relationGraph,
				getToByFrom
			)) {
				relationsFromGeneratorToByFrom.push(relation);
			}
			expect(relationsFromGeneratorToByFrom).toHaveLength(2);
			expect(relationsFromGeneratorToByFrom[0].id).toEqual(ab.id);
			expect(relationsFromGeneratorToByFrom[1].id).toEqual(bc.id);
		});

		it("can revoke", async () => {
			const a = (await Ed25519Keypair.create()).publicKey;
			const b = await Secp256k1PublicKey.recover(await Wallet.createRandom());

			const store = new AnyCanAppendIdentityGraph({
				relationGraph: createIdentityGraphStore()
			});
			await session.peers[0].open(store);

			const ab = new IdentityRelation({
				to: b,
				from: a
			});

			await store.relationGraph.put(ab);

			let trustingB = await getFromByTo.resolve(b, store.relationGraph);
			expect(trustingB).toHaveLength(1);
			expect(trustingB[0].id).toEqual(ab.id);

			await store.relationGraph.del(ab.id);
			trustingB = await getFromByTo.resolve(b, store.relationGraph);
			expect(trustingB).toHaveLength(0);
		});
	});

	describe("TrustedNetwork", () => {
		let session: LSession;
		beforeAll(async () => {
			session = await LSession.connected(4);
		});
		beforeEach(async () => {});

		afterEach(async () => {});

		afterAll(async () => {
			await session.stop();
		});

		it("can be deterministic", async () => {
			const key = (await Ed25519Keypair.create()).publicKey;
			const t1 = new TrustedNetwork({ id: key.publicKey, rootTrust: key });
			const t2 = new TrustedNetwork({ id: key.publicKey, rootTrust: key });

			expect(serialize(t1)).toEqual(serialize(t2));
		});

		it("trusted by chain", async () => {
			const l0a = new TrustedNetwork({
				rootTrust: session.peers[0].peerId
			});
			await session.peers[0].open(l0a);

			await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
			let l0b: TrustedNetwork = await TrustedNetwork.open(
				l0a.address!,
				session.peers[1]
			);

			await session.peers[2].services.blocks.waitFor(session.peers[0].peerId);
			let l0c: TrustedNetwork = await TrustedNetwork.open(
				l0a.address!,
				session.peers[2],
				{
					args: {
						role: new Observer()
					}
				}
			);

			await session.peers[3].services.blocks.waitFor(session.peers[0].peerId);
			let l0d: TrustedNetwork = await TrustedNetwork.open(
				l0a.address!,
				session.peers[3]
			);

			await l0c.waitFor(session.peers[0].peerId, session.peers[1].peerId);

			await l0a.add(session.peers[1].peerId);

			await l0b.trustGraph.log.log.join(
				await l0a.trustGraph.log.log.getHeads()
			);

			await waitForResolved(() => expect(l0b.trustGraph.index.size).toEqual(1));

			await l0b.add(session.peers[2].peerId); // Will only work if peer2 is trusted

			await l0a.trustGraph.log.log.join(
				await l0b.trustGraph.log.log.getHeads()
			);

			await waitFor(() => l0b.trustGraph.index.size == 2);
			await waitFor(() => l0a.trustGraph.index.size == 2);
			await l0c.waitFor(session.peers[0].peerId);
			await l0c.waitFor(session.peers[1].peerId);

			// Try query with trusted
			let responses: IdentityRelation[] = await l0c.trustGraph.index.search(
				new SearchRequest({
					query: []
				})
			);

			// TODO test this properly!
			expect(responses).toHaveLength(2);

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

			expect(untrustedResponse).toHaveLength(0);

			// now check if peer3 is trusted from peer perspective
			expect(await l0a.isTrusted(identity(2).publicKey)).toBeTrue();

			// check if peer3 is trusted from someone else
			expect(await l0a.isTrusted(identity(2).publicKey)).toBeTrue();
			expect(await l0a.isTrusted(identity(3).publicKey)).toBeFalse();

			const trusted = await l0a.getTrusted();
			expect(trusted.map((k) => k.bytes)).toContainAllValues([
				identity(0).publicKey.bytes,
				identity(1).publicKey.bytes,
				identity(2).publicKey.bytes,
			]); */
		});

		it("has relation", async () => {
			const l0a = new TrustedNetwork({
				rootTrust: session.peers[0].peerId
			});
			await session.peers[0].open(l0a);

			await l0a.add(session.peers[1].peerId);
			expect(
				await l0a.hasRelation(session.peers[0].peerId, session.peers[1].peerId)
			).toBeFalse();
			expect(
				await l0a.hasRelation(session.peers[1].peerId, session.peers[0].peerId)
			).toBeTrue();
		});

		it("can not append with wrong truster", async () => {
			let l0a = new TrustedNetwork({
				rootTrust: session.peers[0].peerId
			});
			await session.peers[0].open(l0a);

			expect(
				l0a.trustGraph.put(
					new IdentityRelation({
						to: await Secp256k1PublicKey.recover(await Wallet.createRandom()),
						from: await Secp256k1PublicKey.recover(await Wallet.createRandom())
					})
				)
			).rejects.toBeInstanceOf(AccessError);
		});

		it("untrusteed by chain", async () => {
			let l0a = new TrustedNetwork({
				rootTrust: session.peers[0].peerId
			});

			await session.peers[0].open(l0a);

			let l0b: TrustedNetwork = await TrustedNetwork.open(
				l0a.address!,
				session.peers[1]
			);

			// Can not append peer3Key since its not trusted by the root
			await expect(l0b.add(session.peers[2].peerId)).rejects.toBeInstanceOf(
				AccessError
			);
		});
	});
});
