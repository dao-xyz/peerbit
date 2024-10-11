import { variant } from "@dao-xyz/borsh";
import { generateKeyPair } from "@libp2p/crypto/keys";
import { Program } from "@peerbit/program";
import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

@variant("test-start-stop")
class TestP extends Program {
	async open(_args?: any): Promise<void> {
		await (await this.node.storage.sublevel("test")).open();
	}
}
describe("start-stop", () => {
	let client: Peerbit;

	after(async () => {
		await client.stop();
	});
	it("can create with peerId", async () => {
		const privateKey = await generateKeyPair("Ed25519");
		client = await Peerbit.create({
			libp2p: { privateKey },
		});
		expect(client.peerId.publicKey!.equals(privateKey.publicKey)).to.be.true;
		const addressA = (await client.open(new TestP())).address;

		await client.stop();
		await client.start();
		const addressB = (await client.open(new TestP())).address;

		expect(addressA).equal(addressB);
		expect(addressA).to.exist;
	});
});
