import { AccessError, Ed25519Keypair } from "@peerbit/crypto";
import { Documents } from "@peerbit/document";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import {
	FromTo,
	IdentityGraph,
	IdentityRelation,
	TrustedNetwork,
	createIdentityGraphStore,
} from "../src/index.js";

describe("index trusted-network reopen", () => {
	let session: TestSession;
	beforeEach(async () => {
		session = await TestSession.disconnected(1);
	});
	afterEach(async () => {
		sinon.restore();
		await session.stop();
	});

	it("reopens the same TrustedNetwork instance with its retained trust graph", async () => {
		const peer = session.peers[0];
		const trustee = (await Ed25519Keypair.create()).publicKey;
		const network = await peer.open(
			new TrustedNetwork({ rootTrust: peer.identity.publicKey }),
		);
		await network.add(trustee);
		expect(await network.isTrusted(trustee)).to.be.true;
		const graph = network.trustGraph as Documents<IdentityRelation, FromTo>;
		await network.close();
		expect(graph.log.log.closed).to.be.true;

		expect(await peer.open(network, { existing: "reuse" })).to.equal(network);
		expect(network.trustGraph).to.equal(graph);
		expect(graph.log.log.closed).to.be.false;
		expect(await network.isTrusted(trustee)).to.be.true;
		const next = (await Ed25519Keypair.create()).publicKey;
		await network.add(next);
		expect(await network.isTrusted(next)).to.be.true;
		await network.revoke(trustee);
		expect(await network.isTrusted(trustee)).to.be.false;
	});

	it("reopens the same IdentityGraph instance without losing owner authorization", async () => {
		const peer = session.peers[0];
		const graph = await peer.open(
			new IdentityGraph({ relationGraph: createIdentityGraphStore() }),
		);
		const trustee = (await Ed25519Keypair.create()).publicKey;
		await graph.addRelation(trustee);
		await graph.close();
		expect(await peer.open(graph, { existing: "reuse" })).to.equal(graph);
		await graph.addRelation((await Ed25519Keypair.create()).publicKey);
		expect(await graph.relationGraph.index.getSize()).to.equal(2);
		await expect(
			graph.relationGraph.put(
				new IdentityRelation({ from: trustee, to: peer.identity.publicKey }),
			),
		).to.be.rejectedWith(AccessError);
	});

	it("reuses a genuinely live shared child without reopening it", async () => {
		const peer = session.peers[0];
		const documents = createIdentityGraphStore();
		const open = sinon.spy(documents, "open");
		const first = new TrustedNetwork({ rootTrust: peer.identity.publicKey });
		first.trustGraph = documents;
		await peer.open(first);
		const other = new TrustedNetwork({
			rootTrust: (await Ed25519Keypair.create()).publicKey,
		});
		other.trustGraph = documents;
		const second = await peer.open(other, {
			existing: "reuse",
		});
		expect(second).not.to.equal(first);
		expect(second.address).not.to.equal(first.address);
		expect(open.callCount).to.equal(1);
		expect(second.trustGraph).to.equal(first.trustGraph);
		await first.close();
		expect(documents.log.log.closed).to.be.false;
		await second.add((await Ed25519Keypair.create()).publicKey);

		expect(await peer.open(first, { existing: "reuse" })).to.equal(first);
		expect(open.callCount).to.equal(1);
		await first.add((await Ed25519Keypair.create()).publicKey);
		expect(await documents.index.getSize()).to.equal(2);
	});

	it("cleans up a failed child reopen before the next attempt", async () => {
		const peer = session.peers[0];
		const network = await peer.open(
			new TrustedNetwork({ rootTrust: peer.identity.publicKey }),
		);
		const trustee = (await Ed25519Keypair.create()).publicKey;
		await network.add(trustee);
		const documents = network.trustGraph as Documents<IdentityRelation, FromTo>;
		await network.close();
		const failure = new Error("injected child open failure");
		const original = documents.open.bind(documents);
		const open = sinon.stub(documents, "open").callsFake(async (args) => {
			await original(args);
			throw failure;
		});

		await expect(peer.open(network, { existing: "reuse" })).to.be.rejectedWith(
			failure,
		);
		expect(open.callCount).to.equal(1);
		expect(documents.closed).to.be.true;
		expect(documents.log.log.closed).to.be.true;
		open.restore();

		expect(await peer.open(network, { existing: "reuse" })).to.equal(network);
		expect(await network.isTrusted(trustee)).to.be.true;
		await network.add((await Ed25519Keypair.create()).publicKey);
	});
});
