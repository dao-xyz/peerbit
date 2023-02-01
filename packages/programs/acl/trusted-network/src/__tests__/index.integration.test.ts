import { LSession, waitForPeers } from "@dao-xyz/peerbit-test-utils";
import {
	IdentityRelation,
	createIdentityGraphStore,
	getFromByTo,
	getPathGenerator,
	getToByFrom,
	TrustedNetwork,
	KEY_OFFSET,
	OFFSET_TO_KEY,
} from "..";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import {
	AccessError,
	Ed25519Keypair,
	PeerIdAddress,
} from "@dao-xyz/peerbit-crypto";
import { Secp256k1Keccak256PublicKey } from "@dao-xyz/peerbit-crypto";
import { Identity } from "@dao-xyz/peerbit-log";
import { Wallet } from "@ethersproject/wallet";
import { createStore } from "@dao-xyz/peerbit-test-utils";
import { AbstractLevel } from "abstract-level";
import {
	CachedValue,
	DefaultOptions,
	IStoreOptions,
} from "@dao-xyz/peerbit-store";
import Cache from "@dao-xyz/lazy-level";
import { field, serialize, variant } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import {
	Documents,
	DocumentQueryRequest,
	Results,
} from "@dao-xyz/peerbit-document";
import { v4 as uuid } from "uuid";

const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		sign: (data) => ed.sign(data),
	} as Identity;
};

@variant("identity_graph")
class IdentityGraph extends Program {
	@field({ type: Documents })
	store: Documents<IdentityRelation>;

	constructor(properties?: { store: Documents<IdentityRelation> }) {
		super();
		if (properties) {
			this.store = properties.store;
		}
	}
	async setup(): Promise<void> {
		await this.store.setup({ type: IdentityRelation });
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
				replicate?: boolean;
				store?: IStoreOptions<any>;
			}
		) => {
			store.init &&
				(await store.init(session.peers[i], identites[i], {
					...options,
					replicate: options.replicate ?? true,
					store: {
						...DefaultOptions,
						resolveCache: async () => new Cache(createStore()),
						...options.store,
					},
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

		it("serializes relation with right padding ed25519", async () => {
			const from = (await Ed25519Keypair.create()).publicKey;
			const to = (await Ed25519Keypair.create()).publicKey;
			const relation = new IdentityRelation({ from, to });
			const serRelation = serialize(relation);
			const serFrom = serialize(from);
			const serTo = serialize(to);

			expect(serRelation.slice(KEY_OFFSET + OFFSET_TO_KEY)).toEqual(serTo); // To key has a fixed offset from 0
			expect(
				serRelation.slice(KEY_OFFSET, KEY_OFFSET + serFrom.length)
			).toEqual(serFrom); // From key has a fixed offset from 0
		});

		it("serializes relation with right padding sepc256k1", async () => {
			const from = new Secp256k1Keccak256PublicKey({
				address: await Wallet.createRandom().getAddress(),
			});
			const to = (await Ed25519Keypair.create()).publicKey;
			const relation = new IdentityRelation({ from, to });
			const serRelation = serialize(relation);
			const serFrom = serialize(from);
			const serTo = serialize(to);

			expect(
				serRelation.slice(KEY_OFFSET, KEY_OFFSET + serFrom.length)
			).toEqual(serFrom); // From key has a fixed offset from 0
			const sliceTo = serRelation.slice(KEY_OFFSET + OFFSET_TO_KEY);
			expect(sliceTo).toEqual(serTo); // To key has a fixed offset from 0
		});

		it("path", async () => {
			const a = (await Ed25519Keypair.create()).publicKey;
			const b = new Secp256k1Keccak256PublicKey({
				address: await Wallet.createRandom().getAddress(),
			});
			const c = (await Ed25519Keypair.create()).publicKey;

			const store = new IdentityGraph({
				store: createIdentityGraphStore({
					id: session.peers[0].peerId.toString(),
				}),
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
			await store.store.put(ab);
			await store.store.put(bc);

			// Get relations one by one
			const trustingC = await getFromByTo.resolve(c, store.store);
			expect(trustingC).toHaveLength(1);
			expect(trustingC[0].id).toEqual(bc.id);

			const bIsTrusting = await getToByFrom.resolve(b, store.store);
			expect(bIsTrusting).toHaveLength(1);
			expect(bIsTrusting[0].id).toEqual(bc.id);

			const trustingB = await getFromByTo.resolve(b, store.store);
			expect(trustingB).toHaveLength(1);
			expect(trustingB[0].id).toEqual(ab.id);

			const aIsTrusting = await getToByFrom.resolve(a, store.store);
			expect(aIsTrusting).toHaveLength(1);
			expect(aIsTrusting[0].id).toEqual(ab.id);

			// Test generator
			const relationsFromGeneratorFromByTo: IdentityRelation[] = [];
			for await (const relation of getPathGenerator(
				c,
				store.store,
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
				store.store,
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
			const b = new Secp256k1Keccak256PublicKey({
				address: await Wallet.createRandom().getAddress(),
			});

			const store = new IdentityGraph({
				store: createIdentityGraphStore({
					id: session.peers[0].peerId.toString(),
				}),
			});
			const topic = uuid();
			await init(store, 0, { topic });

			const ab = new IdentityRelation({
				to: b,
				from: a,
			});

			await store.store.put(ab);

			let trustingB = await getFromByTo.resolve(b, store.store);
			expect(trustingB).toHaveLength(1);
			expect(trustingB[0].id).toEqual(ab.id);

			await store.store.del(ab.id);
			trustingB = await getFromByTo.resolve(b, store.store);
			expect(trustingB).toHaveLength(0);
		});
	});

	describe("TrustedNetwork", () => {
		let session: LSession,
			identites: Identity[],
			cacheStore: AbstractLevel<any, string, Uint8Array>[],
			programs: Program[];

		const identity = (i: number) => identites[i];
		const init = async (
			store: Program,
			i: number,
			options: {
				topic: string;
				replicate?: boolean;
				store?: IStoreOptions<any>;
			}
		) => {
			store.init &&
				(await store.init(session.peers[i], identites[i], {
					...options,
					replicate: options.replicate ?? true,
					store: {
						...DefaultOptions,
						resolveCache: async () => new Cache(cacheStore[i]),
						...options.store,
					},
				}));
			programs.push(store);
			return store;
		};
		beforeAll(async () => {
			session = await LSession.connected(5);
			identites = [];
			cacheStore = [];
			programs = [];

			for (let i = 0; i < session.peers.length; i++) {
				identites.push(await createIdentity());
				cacheStore.push(await createStore());
			}
		});

		afterAll(async () => {
			await Promise.all(programs.map((p) => p.close()));
			await session.stop();
			await Promise.all(cacheStore?.map((c) => c.close()));
		});

		it("can be deterministic", async () => {
			const key = (await Ed25519Keypair.create()).publicKey;
			const t1 = new TrustedNetwork({ id: "x", rootTrust: key });
			const t2 = new TrustedNetwork({ id: "x", rootTrust: key });
			t1.setupIndices();
			t2.setupIndices();
			expect(serialize(t1)).toEqual(serialize(t2));
		});

		it("trusted by chain", async () => {
			// TODO make this test in parts instead (very bloaty atm)

			const l0a = new TrustedNetwork({
				rootTrust: identity(0).publicKey,
			});

			const topic = uuid();

			await init(l0a, 0, { topic });
			await l0a.add(identity(1).publicKey);

			await delay(1000);
			let l0b: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[1].directblock,
				l0a.address!
			)) as any;
			await init(l0b, 1, { topic });

			await l0b.trustGraph.store.sync(l0a.trustGraph.store.oplog.heads);

			await waitFor(() => l0b.trustGraph.index.size == 1);

			await l0b.add(identity(2).publicKey); // Will only work if peer2 is trusted

			await l0a.trustGraph.store.sync(l0b.trustGraph.store.oplog.heads);

			await waitFor(() => l0b.trustGraph.index.size == 2);
			await waitFor(() => l0a.trustGraph.index.size == 2);

			await waitForPeers(
				session.peers[2],
				[session.peers[0], session.peers[1]],
				l0b.trustGraph.index._query.rpcTopic
			);

			// Try query with trusted
			let responses: Results<IdentityRelation>[] = [];
			let l0c: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[2].directblock,
				l0a.address!
			)) as any;
			await init(l0c, 2, { topic });

			await delay(3000); // with github ci this fails for some reason, hence this delay. TODO identify what proecss to wait for

			await l0c.trustGraph.index.query(
				new DocumentQueryRequest({
					queries: [],
				}),
				(response) => {
					responses.push(response);
				},
				{
					remote: {
						signer: identity(2),
						timeout: 20000,
						amount: 2, // response from peer and peer2
					},
					local: false,
				}
			);

			expect(responses).toHaveLength(2);

			// Try query with untrusted
			let l0d: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[3].directblock,
				l0a.address!
			)) as any;
			await init(l0d, 3, { topic });

			let untrustedResponse: any = undefined;
			await l0d.trustGraph.index.query(
				new DocumentQueryRequest({
					queries: [],
				}),
				(response) => {
					untrustedResponse = response;
				},
				{
					remote: { timeout: 3000, signer: identity(3) },
				}
			);

			expect(untrustedResponse).toBeUndefined();

			// now check if peer3 is trusted from peer perspective
			expect(await l0a.isTrusted(identity(2).publicKey)).toBeTrue();

			// check if peer3 is trusted from a peer that is not replicating
			let l0observer: TrustedNetwork = (await TrustedNetwork.load(
				session.peers[4].directblock,
				l0a.address!
			)) as any;
			await init(l0observer, 4, {
				topic,
				replicate: false,
				store: {},
			});
			expect(await l0observer.isTrusted(identity(2).publicKey)).toBeTrue();
			expect(await l0observer.isTrusted(identity(3).publicKey)).toBeFalse();

			const trusted = await l0a.getTrusted();
			expect(trusted.map((k) => k.bytes)).toContainAllValues([
				identity(0).publicKey.bytes,
				identity(1).publicKey.bytes,
				identity(2).publicKey.bytes,
			]);
		});

		it("has relation", async () => {
			const l0a = new TrustedNetwork({
				rootTrust: identity(0).publicKey,
			});

			await init(l0a, 0, { topic: uuid() });

			await l0a.add(identity(1).publicKey);
			expect(
				l0a.hasRelation(identity(0).publicKey, identity(1).publicKey)
			).toBeFalse();
			expect(
				l0a.hasRelation(identity(1).publicKey, identity(0).publicKey)
			).toBeTrue();
		});

		it("can not append with wrong truster", async () => {
			let l0a = new TrustedNetwork({
				rootTrust: identity(0).publicKey,
			});
			await init(l0a, 0, { topic: uuid() });

			expect(
				l0a.trustGraph.put(
					new IdentityRelation({
						to: new Secp256k1Keccak256PublicKey({
							address: await Wallet.createRandom().getAddress(),
						}),
						from: new Secp256k1Keccak256PublicKey({
							address: await Wallet.createRandom().getAddress(),
						}),
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
				session.peers[1].directblock,
				l0a.address!
			)) as any;
			await init(l0b, 1, { topic });

			// Can not append peer3Key since its not trusted by the root
			await expect(l0b.add(identity(2).publicKey)).rejects.toBeInstanceOf(
				AccessError
			);
		});
	});
});
