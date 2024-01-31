import { Program } from "@peerbit/program";
import { Peerbit } from "../peer";
import { createEd25519PeerId } from "@libp2p/peer-id-factory";
import { variant } from "@dao-xyz/borsh";

@variant("test-start-stop")
class TestP extends Program {
	async open(args?: any): Promise<void> {
		await (await this.node.storage.sublevel("test")).open();
	}
}
describe("start-stop", () => {
	let client: Peerbit;

	afterAll(async () => {
		await client.stop();
	});
	it("can create with peerId", async () => {
		const peerId = await createEd25519PeerId();
		client = await Peerbit.create({
			libp2p: { peerId }
		});
		expect(client.peerId.equals(peerId)).toBeTrue();
		const addressA = (await client.open(new TestP())).address;

		await client.stop();
		await client.start();
		const addressB = (await client.open(new TestP())).address;

		expect(addressA).toEqual(addressB);
		expect(addressA).toBeDefined();
	});
});
