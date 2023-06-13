// This more like a playground as of now
// No real tests yet,
// But there are ways here to generate base64 string for programs

import { serialize, deserialize } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { Peerbit } from "@dao-xyz/peerbit";
import { DString } from "@dao-xyz/peerbit-string";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { jest } from "@jest/globals";
import { PermissionedString } from "..";

describe("server", () => {
	let session: LSession, peer: Peerbit;
	jest.setTimeout(60 * 1000);

	beforeAll(async () => {
		session = await LSession.connected(1);
		peer = await Peerbit.create({
			libp2p: session.peers[0],
			directory: "./tmp/peerbit/" + +new Date(),
		});
	});

	afterAll(async () => {
		await peer.stop();
		await session.stop();
	});

	it("_", async () => {
		const program = new PermissionedString({
			store: new DString({}),
			trusted: [peer.identity.publicKey],
		});
		await program.initializeIds();
		const base54 = Buffer.from(serialize(program)).toString("base64");
		const pr = deserialize(Buffer.from(base54, "base64"), Program);
	});
});
