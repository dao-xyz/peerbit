import { deserialize, serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import sinon from "sinon";
import { TrustedNetwork } from "../src/index.js";

describe("index trusted-network warmup", () => {
	afterEach(() => sinon.restore());

	for (const decoded of [false, true]) {
		it(`warms an untrusted local miss after ${decoded ? "deserialization" : "construction"} without granting trust`, async () => {
			const root = await Ed25519Keypair.create();
			const trustee = await Ed25519Keypair.create();
			const created = new TrustedNetwork({ rootTrust: root.publicKey });
			const network = decoded
				? deserialize(serialize(created), TrustedNetwork)
				: created;
			const search = sinon.stub().resolves([]);
			network.trustGraph = { index: { search } } as any;
			const local = sinon.stub(network, "_isTrustedLocal").resolves(false);
			const now = sinon.stub(Date, "now").returns(100_000);
			await network.open();

			expect(await network.isTrusted(trustee.publicKey)).to.be.false;
			expect(search.callCount).to.equal(1);
			expect(search.firstCall.args[1]).to.deep.equal({
				remote: { replicate: true },
			});
			expect(await network.isTrusted(trustee.publicKey)).to.be.false;
			expect(search.callCount).to.equal(1);

			// Re-entering open must not turn a recent miss into another remote query.
			await network.open();
			expect(await network.isTrusted(trustee.publicKey)).to.be.false;
			expect(search.callCount).to.equal(1);
			now.returns(102_001);
			expect(await network.isTrusted(trustee.publicKey)).to.be.false;
			expect(search.callCount).to.equal(2);

			local.resolves(true);
			expect(await network.isTrusted(trustee.publicKey)).to.be.true;
			expect(search.callCount).to.equal(2);
		});
	}

	it("keeps failed remote warmup default-deny and throttled", async () => {
		const root = await Ed25519Keypair.create();
		const trustee = await Ed25519Keypair.create();
		const network = deserialize(
			serialize(new TrustedNetwork({ rootTrust: root.publicKey })),
			TrustedNetwork,
		);
		const search = sinon.stub().rejects(new Error("unavailable"));
		network.trustGraph = { index: { search } } as any;
		sinon.stub(network, "_isTrustedLocal").resolves(false);
		sinon.stub(Date, "now").returns(100_000);
		await network.open();

		expect(await network.isTrusted(trustee.publicKey)).to.be.false;
		expect(await network.isTrusted(trustee.publicKey)).to.be.false;
		expect(search.callCount).to.equal(1);
	});
});
