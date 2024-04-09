import { Program } from "@peerbit/program";
import { Peerbit } from "../src/peer.js";
import { createEd25519PeerId } from "@libp2p/peer-id-factory";
import { variant } from "@dao-xyz/borsh";
import { expect } from 'chai';

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
		const peerId = await createEd25519PeerId();
		client = await Peerbit.create({
			libp2p: { peerId }
		});
		expect(client.peerId.equals(peerId)).to.be.true;
		const addressA = (await client.open(new TestP())).address;

		await client.stop();
		await client.start();
		const addressB = (await client.open(new TestP())).address;

		expect(addressA).equal(addressB);
		expect(addressA).to.exist;
	});
});
