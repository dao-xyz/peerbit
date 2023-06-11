import { LSession } from "@dao-xyz/peerbit-test-utils";
import {
	IdentityRelation,
	createIdentityGraphStore,
	getFromByTo,
	getPathGenerator,
	getToByFrom,
	TrustedNetwork,
	IdentityGraph,
} from "..";
import { waitFor } from "@dao-xyz/peerbit-time";
import { AccessError, Ed25519Keypair, Identity } from "@dao-xyz/peerbit-crypto";
import { Secp256k1PublicKey } from "@dao-xyz/peerbit-crypto";
import { Entry } from "@dao-xyz/peerbit-log";
import { Wallet } from "@ethersproject/wallet";
import { serialize, variant } from "@dao-xyz/borsh";
import {
	Program,
	ReplicatorType,
	SubscriptionType,
} from "@dao-xyz/peerbit-program";
import { Documents, SearchRequest, Operation } from "@dao-xyz/peerbit-document";
import { v4 as uuid } from "uuid";
import { waitForPeers as waitForPeersBlock } from "@dao-xyz/libp2p-direct-stream";
import { waitForSubscribers } from "@dao-xyz/libp2p-direct-sub";

const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		sign: (data) => ed.sign(data),
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
	async canAppend(entry: Entry<Operation<IdentityRelation>>): Promise<boolean> {
		return true;
	}
}
describe("index", () => {
	describe("identity-graph", () => {
		let session: LSession, identites: Identity[], programs: Program[];

		const init = async (
			store: Program,
			i: number,
			options: {
				topic: string;
				role?: SubscriptionType;
			}
		) => {
			store.init &&
				(await store.init(session.peers[i], identites[i], {
					...options,
					log: {
						replication: {
							replicators: () => [],
						},
					},
					role: options.role || new ReplicatorType(),
				}));
			programs.push(store);
			return store;
		};
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
				relationGraph: createIdentityGraphStore(),
			});
			await init(store, 0, { topic: uuid() });

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
				relationGraph: createIdentityGraphStore(),
			});
			const topic = uuid();
			await init(store, 0, { topic });

			const ab = new IdentityRelation({
				to: b,
				from: a,
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
		let session: LSession, identites: Identity[], programs: Program[];

		let replicators: string[][];

		const identity = (i: number) => identites[i];
		const init = async (
			store: Program,
			i: number,
			options: {
				topic: string;
				role?: SubscriptionType;
			}
		) => {
			store.init &&
				(await store.init(session.peers[i], identites[i], {
					...options,
					log: {
						replication: {
							replicators: () => replicators,
						},
					},
					role: options.role ?? new ReplicatorType(),
				}));
			programs.push(store);
			return store;
		};

		beforeAll(async () => {
			session = await LSession.connected(4);
			await waitForPeersBlock(...session.peers.map((x) => x.services.blocks));
		});
		beforeEach(async () => {
			identites = [];
			programs = [];
			for (let i = 0; i < session.peers.length; i++) {
				identites.push(await createIdentity());
			}

			replicators = session.peers.map((x) => [
				x.services.pubsub.publicKey.hashcode(),
			]);
		});

		afterEach(async () => {
			await Promise.all(programs.map((p) => p.close()));
		});

		afterAll(async () => {
			await session.stop();
		});

		it("can be deterministic", async () => {
			const key = (await Ed25519Keypair.create()).publicKey;
			const t1 = new TrustedNetwork({ rootTrust: key });
			const t2 = new TrustedNetwork({ rootTrust: key });
			await t1.initializeIds();
			await t2.initializeIds();
			expect(serialize(t1)).toEqual(serialize(t2));
		});

		it("trusted by chain", async () => {
			// TODO make this test in parts instead (very bloaty atm)
			const topic = uuid();

			const l0a = new TrustedNetwork({
				rootTrust: identity(0).publicKey,
			});
			await init(l0a, 0, { topic });

			let l0b: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[1].services.blocks,
				l0a.address!
			)) as any;
			await init(l0b, 1, { topic });

			let l0c: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[2].services.blocks,
				l0a.address!
			)) as any;
			await init(l0c, 2, { topic });

			let l0d: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[3].services.blocks,
				l0a.address!
			)) as any;
			await init(l0d, 3, { topic });

			await waitForSubscribers(
				session.peers[2],
				[session.peers[0], session.peers[1]],
				l0b.trustGraph.index._query.rpcTopic
			);

			await l0a.add(identity(1).publicKey);

			await l0b.trustGraph.log.join(await l0a.trustGraph.log.getHeads());

			await waitFor(() => l0b.trustGraph.index.size == 1);

			await l0b.add(identity(2).publicKey); // Will only work if peer2 is trusted

			await l0a.trustGraph.log.join(await l0b.trustGraph.log.getHeads());

			await waitFor(() => l0b.trustGraph.index.size == 2);
			await waitFor(() => l0a.trustGraph.index.size == 2);

			// Try query with trusted
			let responseCount = 0;
			let responses: IdentityRelation[] = await l0c.trustGraph.index.query(
				new SearchRequest({
					queries: [],
				}),
				{
					remote: {
						onResponse: () => {
							responseCount++;
						},
						timeout: 20000,
					},
					local: false,
				}
			);

			expect(responseCount).toEqual(3);
			expect(responses).toHaveLength(2);

			// Try query with untrusted
			// TODO we are not using read access control on the trust graph anymore, but should we?
			/* let untrustedResponse: Results<IdentityRelation>[] =
				await l0d.trustGraph.index.query(
					new SearchRequest({
						queries: [],
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
				rootTrust: identity(0).publicKey,
			});
			await init(l0a, 0, { topic: uuid() });
			replicators = [];

			await l0a.add(identity(1).publicKey);
			expect(
				await l0a.hasRelation(identity(0).publicKey, identity(1).publicKey)
			).toBeFalse();
			expect(
				await l0a.hasRelation(identity(1).publicKey, identity(0).publicKey)
			).toBeTrue();
		});

		it("can not append with wrong truster", async () => {
			let l0a = new TrustedNetwork({
				rootTrust: identity(0).publicKey,
			});
			await init(l0a, 0, { topic: uuid() });
			replicators = [];

			expect(
				l0a.trustGraph.put(
					new IdentityRelation({
						to: await Secp256k1PublicKey.recover(await Wallet.createRandom()),
						from: await Secp256k1PublicKey.recover(await Wallet.createRandom()),
					})
				)
			).rejects.toBeInstanceOf(AccessError);
		});

		it("untrusteed by chain", async () => {
			let l0a = new TrustedNetwork({
				rootTrust: identity(0).publicKey,
			});
			const topic = uuid();

			await init(l0a, 0, { topic });

			let l0b: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[1].services.blocks,
				l0a.address!
			)) as any;
			await init(l0b, 1, { topic });

			replicators = [
				[session.peers[0].services.pubsub.publicKey.hashcode()],
				[session.peers[1].services.pubsub.publicKey.hashcode()],
			];

			// Can not append peer3Key since its not trusted by the root
			await expect(l0b.add(identity(2).publicKey)).rejects.toBeInstanceOf(
				AccessError
			);
		});
	});
});
