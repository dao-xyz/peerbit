import { deserialize, serialize } from "@dao-xyz/borsh";
import { type PeerId } from "@libp2p/interface";
import {
	AccessError,
	Ed25519Keypair,
	type Identity,
	type PublicSignKey,
	Secp256k1Keypair,
	Secp256k1PublicKey,
} from "@peerbit/crypto";
import {
	BORSH_ENCODING_OPERATION,
	DeleteOperation,
	Documents,
	PutOperation,
	toId,
} from "@peerbit/document";
import { type Entry, EntryType, createEntry } from "@peerbit/log";
import { AbsoluteReplicas, encodeReplicas } from "@peerbit/shared-log";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import { Wallet } from "ethers";
import {
	FromTo,
	IdentityGraph,
	IdentityRelation,
	TrustedNetwork,
} from "../src/index.js";

const asStructuralPublicKey = (key: PublicSignKey): PublicSignKey =>
	({ bytes: key.bytes.slice() }) as unknown as PublicSignKey;

const canPut = (
	controller: TrustedNetwork | IdentityGraph,
	value: unknown,
	signer: PublicSignKey,
) =>
	controller.canPerform({
		type: "put",
		value,
		entry: {
			meta: { type: EntryType.APPEND, next: [] },
			getPublicKeys: async () => [signer],
		},
	} as any);

// Every way a caller can name a key when a relation is built.
const createKeyInputs = async (): Promise<
	{ name: string; input: PublicSignKey | PeerId; key: PublicSignKey }[]
> => {
	const ed25519 = (await Ed25519Keypair.create()).publicKey;
	const secp256k1 = await Secp256k1PublicKey.recover(
		await Wallet.createRandom(),
	);
	const ed25519Peer = (await Ed25519Keypair.create()).publicKey;
	const secp256k1Peer = (await Secp256k1Keypair.create()).publicKey;
	const structural = (await Ed25519Keypair.create()).publicKey;
	return [
		{ name: "Ed25519 key", input: ed25519, key: ed25519 },
		{ name: "secp256k1 key", input: secp256k1, key: secp256k1 },
		{
			name: "Ed25519 peer id",
			input: ed25519Peer.toPeerId(),
			key: ed25519Peer,
		},
		{
			name: "secp256k1 peer id",
			input: secp256k1Peer.toPeerId(),
			key: secp256k1Peer,
		},
		{
			name: "structural key",
			input: asStructuralPublicKey(structural),
			key: structural,
		},
	];
};

const identityOf = (publicKey: PublicSignKey | PeerId) =>
	({ publicKey }) as unknown as Identity;

const addThroughNetwork = async (
	from: PublicSignKey | PeerId,
	to: PublicSignKey | PeerId,
): Promise<IdentityRelation> => {
	let stored: IdentityRelation | undefined;
	const network = new TrustedNetwork({
		rootTrust: (await Ed25519Keypair.create()).publicKey,
	});
	network.trustGraph = {
		index: { get: async (): Promise<undefined> => undefined },
		put: async (relation: IdentityRelation) => {
			stored = relation;
		},
	} as any;
	await network.add(to, { identity: identityOf(from) });
	return stored!;
};

const addThroughGraph = async (
	from: PublicSignKey | PeerId,
	to: PublicSignKey | PeerId,
): Promise<IdentityRelation> => {
	let stored: IdentityRelation | undefined;
	const graph = new IdentityGraph({
		relationGraph: {
			put: async (relation: IdentityRelation) => {
				stored = relation;
			},
		} as any,
	});
	await graph.addRelation(to, { identity: identityOf(from) });
	return stored!;
};

describe("index trusted-network relation ids", () => {
	describe("canPerform", () => {
		it("accepts relations built by every library path for every supported key", async () => {
			const inputs = await createKeyInputs();
			const network = new TrustedNetwork({
				rootTrust: (await Ed25519Keypair.create()).publicKey,
			});
			network.isTrusted = async () => true;
			const graph = new IdentityGraph();

			for (const from of inputs) {
				for (const to of inputs) {
					const built = [
						new IdentityRelation({ from: from.key, to: to.key }),
						await addThroughNetwork(from.input, to.input),
						await addThroughGraph(from.input, to.input),
					];
					for (const relation of built) {
						expect(relation.id).to.deep.equal(
							new IdentityRelation({ from: from.key, to: to.key }).id,
						);
						// Remote peers check the decoded relation.
						const decoded = deserialize(serialize(relation), IdentityRelation);
						for (const value of [relation, decoded]) {
							const label = `${from.name} -> ${to.name}`;
							expect(await canPut(network, value, from.key), label).to.be.true;
							expect(await canPut(graph, value, from.key), label).to.be.true;
						}
					}
				}
			}
		});

		it("refuses a relation put whose id is not the id of its own pair", async () => {
			const network = new TrustedNetwork({
				rootTrust: (await Ed25519Keypair.create()).publicKey,
			});
			network.isTrusted = async () => true;
			const graph = new IdentityGraph();
			const root = (await Ed25519Keypair.create()).publicKey;
			const member = (await Ed25519Keypair.create()).publicKey;
			const trustee = await Secp256k1PublicKey.recover(
				await Wallet.createRandom(),
			);
			const other = (await Secp256k1Keypair.create()).publicKey;
			const withId = (id: Uint8Array) => {
				const relation = new IdentityRelation({ from: member, to: other });
				relation.id = id;
				return relation;
			};
			const retargeted = new IdentityRelation({ from: member, to: other });
			retargeted._to = trustee;

			for (const value of [
				// Claims the row of root -> trustee.
				withId(new IdentityRelation({ from: root, to: trustee }).id),
				// Claims the reverse pair.
				withId(new IdentityRelation({ from: other, to: member }).id),
				withId(new Uint8Array(32)),
				retargeted,
			]) {
				const decoded = deserialize(serialize(value), IdentityRelation);
				for (const relation of [value, decoded]) {
					expect(await canPut(network, relation, member)).to.be.false;
					expect(await canPut(graph, relation, member)).to.be.false;
				}
			}
		});

		it("lets the owner of a stored row delete it whatever id the row has", async () => {
			const root = await Ed25519Keypair.create();
			const member = await Ed25519Keypair.create();
			const trustee = (await Ed25519Keypair.create()).publicKey;
			// A row admitted before ids were bound: it holds root -> trustee's id.
			const stored = new IdentityRelation({
				from: member.publicKey,
				to: (await Ed25519Keypair.create()).publicKey,
			});
			stored.id = new IdentityRelation({
				from: root.publicKey,
				to: trustee,
			}).id;
			const network = new TrustedNetwork({ rootTrust: root.publicKey });
			network.isTrusted = async () => true;
			network.trustGraph = {
				index: { get: async () => stored },
			} as any;

			expect(
				await network.canPerform({
					type: "delete",
					operation: { key: toId(stored.id) },
					entry: { getPublicKeys: async () => [member.publicKey] },
				} as any),
			).to.be.true;
		});
	});

	describe("network", () => {
		let session: TestSession;
		// The root, a member and a replica.
		beforeEach(async () => {
			session = await TestSession.disconnected(3);
		});
		afterEach(async () => {
			await session.stop();
		});

		// The root trusts the member and the trustee. The member then claims the
		// row of root -> trustee for a relation of its own to another key.
		const setup = async (
			network = new TrustedNetwork({
				rootTrust: session.peers[0].identity.publicKey,
			}),
		) => {
			const [rootPeer, memberPeer] = session.peers;
			const trustee = await Secp256k1PublicKey.recover(
				await Wallet.createRandom(),
			);
			const other = (await Ed25519Keypair.create()).publicKey;
			const root = await rootPeer.open(network);
			await root.add(memberPeer.identity.publicKey);
			const rootToTrustee = await root.add(trustee);
			const rootGraph = root.trustGraph as Documents<IdentityRelation, FromTo>;

			const member = await memberPeer.open(root.clone());
			const memberGraph = member.trustGraph as Documents<
				IdentityRelation,
				FromTo
			>;
			await memberGraph.log.log.join(
				await rootGraph.log.log.getHeads(true).all(),
			);
			expect(await member.isTrusted(memberPeer.identity.publicKey)).to.be.true;
			const rootToTrusteeEntry = (await memberGraph.log.log.get(
				(await rootGraph.index.get(rootToTrustee.id, {
					local: true,
					remote: false,
				}))!.__context.head,
			))!;

			const forged = new IdentityRelation({
				from: memberPeer.identity.publicKey,
				to: other,
			});
			forged.id = rootToTrustee.id;
			return {
				rootPeer,
				memberPeer,
				trustee,
				other,
				root,
				rootGraph,
				rootToTrustee,
				rootToTrusteeEntry,
				member,
				forged,
			};
		};

		// Signed on the member's peer without the member's own access check, as a
		// modified client could write it.
		const createMemberEntry = (
			peer: TestSession["peers"][number],
			data: PutOperation | DeleteOperation,
			meta?: { next: Entry<any>[]; type?: EntryType },
		) =>
			createEntry({
				store: peer.services.blocks,
				identity: peer.identity,
				data,
				encoding: BORSH_ENCODING_OPERATION,
				meta: {
					...meta,
					// Shared logs refuse entries without a replication degree.
					data: encodeReplicas(new AbsoluteReplicas(1)),
				},
			});

		const expectRootStillTrustsTrustee = async ({
			root,
			rootGraph,
			rootPeer,
			rootToTrustee,
			trustee,
		}: Awaited<ReturnType<typeof setup>>) => {
			expect(await root.isTrusted(trustee)).to.be.true;
			const row = await rootGraph.index.get(rootToTrustee.id, {
				local: true,
				remote: false,
			});
			expect(row?.from.equals(rootPeer.identity.publicKey)).to.be.true;
			expect(row?.to.equals(trustee)).to.be.true;
		};

		it("refuses a trusted member's relation that claims another relation's id", async () => {
			const context = await setup();
			const put = await createMemberEntry(
				context.memberPeer,
				new PutOperation({ data: serialize(context.forged) }),
			);

			await context.rootGraph.log.log.join([put]);
			await expectRootStillTrustsTrustee(context);
			expect(await context.rootGraph.log.log.has(put.hash)).to.be.false;

			await expect(
				context.member.trustGraph.put(context.forged),
			).eventually.rejectedWith(AccessError);
			// The member is still trusted to add a relation of its own.
			await context.member.add(context.other);
			expect(await context.member.isTrusted(context.other)).to.be.true;
		});

		it("refuses a trusted member's delete of a relation it overwrote by id", async () => {
			const context = await setup();
			const put = await createMemberEntry(
				context.memberPeer,
				new PutOperation({ data: serialize(context.forged) }),
			);
			const del = await createMemberEntry(
				context.memberPeer,
				new DeleteOperation({ key: toId(context.rootToTrustee.id) }),
				{ next: [put], type: EntryType.CUT },
			);

			await context.rootGraph.log.log.join([put]);
			await context.rootGraph.log.log.join([del]);
			await expectRootStillTrustsTrustee(context);
			expect(await context.rootGraph.log.log.has(put.hash)).to.be.false;
			expect(await context.rootGraph.log.log.has(del.hash)).to.be.false;
		});

		// Stands in for a root that admitted any put before this check. Peers
		// that open a clone of it run the check.
		const preFixNetwork = () => {
			let admitsAnyPut = true;
			const network = new TrustedNetwork({
				rootTrust: session.peers[0].identity.publicKey,
			});
			network.canPerform = (properties) =>
				admitsAnyPut && properties.type === "put"
					? Promise.resolve(true)
					: TrustedNetwork.prototype.canPerform.call(network, properties);
			return {
				network,
				upgrade: () => {
					admitsAnyPut = false;
				},
			};
		};

		it("lets the truster replace a relation whose row another pair took before ids were bound", async () => {
			const preFix = preFixNetwork();
			const context = await setup(preFix.network);
			const rootKey = context.rootPeer.identity.publicKey;
			const put = await createMemberEntry(
				context.memberPeer,
				new PutOperation({ data: serialize(context.forged) }),
			);
			await context.rootGraph.log.log.join([put]);
			const row = await context.rootGraph.index.get(context.rootToTrustee.id, {
				local: true,
				remote: false,
			});
			expect(row?.from.equals(context.memberPeer.identity.publicKey)).to.be
				.true;
			expect(await context.root.isTrusted(context.trustee)).to.be.false;
			preFix.upgrade();

			// The row at root -> trustee's id holds another pair, so the root has
			// no relation to the trustee.
			expect(await context.root.getRelation(rootKey, context.trustee)).to.be
				.undefined;
			expect(await context.root.hasRelation(rootKey, context.trustee)).to.be
				.false;
			expect(await context.root.revoke(context.trustee)).to.be.undefined;

			const relation = await context.root.add(context.trustee);
			expect(relation.from.equals(rootKey)).to.be.true;
			expect(relation.to.equals(context.trustee)).to.be.true;
			await expectRootStillTrustsTrustee(context);
			expect(await context.root.isTrusted(context.other)).to.be.false;
		});

		it("lets a replica that refused the overwriting put apply the truster's repair and revoke", async () => {
			const preFix = preFixNetwork();
			const context = await setup(preFix.network);
			// A client from before this check gives the forged put the root ->
			// trustee entry as its previous version, as Documents.put does for an
			// existing row.
			const put = await createMemberEntry(
				context.memberPeer,
				new PutOperation({ data: serialize(context.forged) }),
				{ next: [context.rootToTrusteeEntry] },
			);
			await context.rootGraph.log.log.join([put]);
			expect(await context.root.isTrusted(context.trustee)).to.be.false;
			preFix.upgrade();

			const replica = await session.peers[2].open(context.root.clone());
			const replicaGraph = replica.trustGraph as Documents<
				IdentityRelation,
				FromTo
			>;
			const syncFromRoot = async () =>
				replicaGraph.log.log.join(await context.rootGraph.log.log.toArray());
			await syncFromRoot();
			expect(await replicaGraph.log.log.has(put.hash)).to.be.false;

			// The repair names the forged entry, which the replica refused, as its
			// previous version.
			const repaired = await context.root.add(context.trustee);
			const repair = (await context.rootGraph.index.get(repaired.id, {
				local: true,
				remote: false,
			}))!.__context.head;
			expect(
				(await context.rootGraph.log.log.get(repair))!.meta.next,
			).to.deep.equal([put.hash]);
			await syncFromRoot();
			expect(await replicaGraph.log.log.has(repair)).to.be.true;
			expect(await replica.isTrusted(context.trustee)).to.be.true;

			await context.root.revoke(context.trustee);
			await syncFromRoot();
			expect(await context.root.isTrusted(context.trustee)).to.be.false;
			expect(await replica.isTrusted(context.trustee)).to.be.false;
			// On the root, the revoke also removed the forged entry and the grant
			// it pointed to.
			expect(await context.rootGraph.log.log.has(put.hash)).to.be.false;
			expect(
				await context.rootGraph.log.log.has(context.rootToTrusteeEntry.hash),
			).to.be.false;
		});
	});
});
