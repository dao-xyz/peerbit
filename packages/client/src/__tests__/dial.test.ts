import assert from "assert";
import { Peerbit } from "../peer";
import { databases } from "./utils";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { createBlock, getBlockValue } from "@dao-xyz/libp2p-direct-block";
import { delay, waitFor } from "@dao-xyz/peerbit-time";

describe(`dial`, function () {
	let session: LSession, client1: Peerbit, client2: Peerbit;

	beforeEach(async () => {
		session = await LSession.disconnected(2);
		client1 = await Peerbit.create({
			libp2p: session.peers[0],
		});
		client2 = await Peerbit.create({
			libp2p: session.peers[1],
		});
	});

	afterEach(async () => {
		await client1.stop();
		await client2.stop();
		await session.stop();
	});

	it("waits for directblock", async () => {
		const cid = await client2.libp2p.directblock.put(
			await createBlock(new Uint8Array([1]), "raw")
		);
		await client1.dial(client2.libp2p.getMultiaddrs()[0]);
		expect(
			await getBlockValue<Uint8Array>(
				(await client1.libp2p.directblock.get(cid))!
			)
		).toEqual(new Uint8Array([1]));
	});

	it("waits for directsub", async () => {
		let topic = "topic";
		await client2.libp2p.directsub.subscribe(topic);
		let data: Uint8Array | undefined = undefined;
		client2.libp2p.directsub.addEventListener("data", (d) => {
			data = d.detail.data;
		});
		await client1.dial(client2.libp2p.getMultiaddrs()[0]);
		await client1.libp2p.directsub.publish(new Uint8Array([1]), {
			topics: [topic],
		});
		await waitFor(() => !!data);
		expect(data && new Uint8Array(data)).toEqual(new Uint8Array([1]));
	});
});
