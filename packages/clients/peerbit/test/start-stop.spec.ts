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
		expect(client.libp2p.status).to.equal("started");
		await client.stop();
		expect(client.libp2p.status).to.equal("stopped");

		await client.start();
		const addressB = (await client.open(new TestP())).address;

		expect(addressA).equal(addressB);
		expect(addressA).to.exist;
	});

	it("can create with directory", async () => {
		let directory = `tmp/peerbit/tests/start-stop-${Math.random()}`;
		client = await Peerbit.create({
			directory,
		});
		expect(client.directory).to.equal(directory);

		// put block
		const cid = await client.services.blocks.put(new Uint8Array([1, 2, 3]));
		expect(cid).to.exist;

		await client.stop();
		client = await Peerbit.create({
			directory,
		});
		expect(client.directory).to.equal(directory);
		expect(client.libp2p.status).to.equal("started");
		const bytes = await client.services.blocks.get(cid);
		expect(bytes).to.deep.equal(new Uint8Array([1, 2, 3]));
	});
});
