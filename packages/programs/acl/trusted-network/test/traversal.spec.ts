import { deserialize, serialize } from "@dao-xyz/borsh";
import { AccessError, Ed25519Keypair, Ed25519PublicKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import {
	IdentityRelation,
	type RelationResolver,
	TrustedNetwork,
	createIdentityGraphStore,
	getFromByToLocalOnly,
	getPathGenerator,
	hasPath,
} from "../src/index.js";

const key = (value: number) =>
	new Ed25519PublicKey({ publicKey: new Uint8Array(32).fill(value) });
const edgeId = (relation: IdentityRelation) =>
	Buffer.from(relation.id).toString("hex");
const relation = (from: Ed25519PublicKey, to: Ed25519PublicKey) =>
	new IdentityRelation({ from, to });

describe("trust graph traversal", () => {
	it("visits the full diamond and preserves both converging edges", async () => {
		const [root, a, b, c, d, e] = [1, 2, 3, 4, 5, 6].map(key);
		const relations = [
			relation(root, a),
			relation(root, b),
			relation(a, c),
			relation(b, c),
			relation(c, d),
			relation(d, e),
		];
		const resolver: RelationResolver = {
			resolve: async (from) =>
				relations.filter((edge) => edge.from.equals(from)),
			next: (edge) => edge.to,
		};
		const found: IdentityRelation[] = [];
		for await (const edge of getPathGenerator(
			root,
			createIdentityGraphStore(),
			resolver,
		)) {
			found.push(edge);
		}
		expect(found.map(edgeId)).to.deep.equal(relations.map(edgeId));
	});

	for (const fresh of [false, true]) {
		it(`terminates a cycle and follows its exit with ${fresh ? "freshly decoded" : "cached"} relations`, async () => {
			const [root, a, b, c, d] = [1, 2, 3, 4, 5].map(key);
			const relations = [
				relation(root, a),
				relation(a, b),
				relation(b, a),
				relation(b, c),
				relation(c, d),
			];
			const resolved: string[] = [];
			const resolver: RelationResolver = {
				resolve: async (from) => {
					resolved.push(from.hashcode());
					// Bound the regression even when a broken walk keeps decoding a cycle.
					expect(resolved.length).to.be.at.most(5);
					return relations
						.filter((edge) => edge.from.equals(from))
						.map((edge) =>
							fresh ? deserialize(serialize(edge), IdentityRelation) : edge,
						);
				},
				next: (edge) => edge.to,
			};
			const found: IdentityRelation[] = [];
			for await (const edge of getPathGenerator(
				root,
				createIdentityGraphStore(),
				resolver,
			)) {
				found.push(edge);
			}
			expect(found.map(edgeId)).to.deep.equal(relations.map(edgeId));
			expect(resolved).to.deep.equal(
				[root, a, b, c, d].map((k) => k.hashcode()),
			);
		});
	}

	it("checks targets in the resolver's direction, including the terminal node", async () => {
		const [a, b, c] = [1, 2, 3].map(key);
		const relations = [relation(a, b), relation(b, c)];
		const store = createIdentityGraphStore();
		const forward: RelationResolver = {
			resolve: async (from) =>
				relations.filter((edge) => edge.from.equals(from)),
			next: (edge) => edge.to,
		};
		const reverse: RelationResolver = {
			resolve: async (to) => relations.filter((edge) => edge.to.equals(to)),
			next: (edge) => edge.from,
		};
		expect(await hasPath(a, c, store, forward)).to.be.true;
		expect(await hasPath(c, a, store, reverse)).to.be.true;
		expect(await hasPath(c, a, store, forward)).to.be.false;
		expect(await hasPath(a, c, store, reverse)).to.be.false;
	});

	describe("signed Documents grants", () => {
		let session: TestSession;
		let network: TrustedNetwork;

		beforeEach(async () => {
			session = await TestSession.connected(1);
			network = await session.peers[0].open(
				new TrustedNetwork({ rootTrust: session.peers[0].identity.publicKey }),
			);
		});

		afterEach(async () => {
			sinon.restore();
			await session.stop();
		});

		it("returns every trusted diamond participant exactly once", async () => {
			const [a, b, c, d, e] = await Promise.all(
				Array.from({ length: 5 }, () => Ed25519Keypair.create()),
			);
			await network.add(a.publicKey);
			await network.add(b.publicKey);
			await network.add(c.publicKey, { identity: a });
			await network.add(c.publicKey, { identity: b });
			await network.add(d.publicKey, { identity: c });
			await network.add(e.publicKey, { identity: d });

			const trusted = (await network.getTrusted()).map((k) => k.hashcode());
			expect(trusted).to.have.length(6);
			expect(trusted).to.have.members(
				[network.rootTrust, ...[a, b, c, d, e].map((k) => k.publicKey)].map(
					(k) => k.hashcode(),
				),
			);
			expect(await network.isTrusted(e.publicKey)).to.be.true;
		});

		it("authorizes a writer past an earlier cycle without weakening relation ownership", async () => {
			const [a, b, c, x, e, outsider] = await Promise.all(
				Array.from({ length: 6 }, () => Ed25519Keypair.create()),
			);
			await network.add(c.publicKey);
			await network.add(b.publicKey, { identity: c });
			await network.add(x.publicKey, { identity: b });
			await network.add(a.publicKey, { identity: x });
			await network.add(x.publicKey, { identity: a });

			// Retain real indexed Documents results, but always explore A -> X before
			// B -> X. The cycle must not end the walk before it reaches root -> C.
			const resolve = getFromByToLocalOnly.resolve;
			sinon.stub(getFromByToLocalOnly, "resolve").callsFake(async (...args) => {
				const found = await resolve(...args);
				return found.sort(
					(left, right) =>
						Number(right.from.equals(a.publicKey)) -
						Number(left.from.equals(a.publicKey)),
				);
			});

			const grant = await network.add(e.publicKey, { identity: x });
			expect(await network.isTrusted(x.publicKey)).to.be.true;
			expect(await network.isTrusted(e.publicKey)).to.be.true;
			const trusted = (await network.getTrusted()).map((k) => k.hashcode());
			expect(trusted).to.have.length(6);
			expect(new Set(trusted).size).to.equal(trusted.length);
			expect(trusted).to.include(e.publicKey.hashcode());

			expect(await network.isTrusted(outsider.publicKey)).to.be.false;
			await expect(
				network.add(e.publicKey, { identity: outsider }),
			).eventually.rejectedWith(AccessError);
			await expect(
				network.trustGraph.put(
					new IdentityRelation({ from: x.publicKey, to: outsider.publicKey }),
					{ identity: a },
				),
			).eventually.rejectedWith(AccessError);
			await expect(
				network.trustGraph.del(grant.id, { identity: a }),
			).eventually.rejectedWith(AccessError);
			await network.revoke(e.publicKey, { identity: x });
			expect(await network.getRelation(x.publicKey, e.publicKey)).to.equal(
				undefined,
			);
		});
	});
});
