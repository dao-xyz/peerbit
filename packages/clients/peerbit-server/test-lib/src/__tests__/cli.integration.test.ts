// This more like a playground as of now
// No real tests yet,
// But there are ways here to generate base64 string for programs

import { serialize, deserialize } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { Peerbit } from "@peerbit/interface";
import { DString } from "@peerbit/string";
import { LSession } from "@peerbit/test-utils";
import { jest } from "@jest/globals";
import { PermissionedString } from "..";

describe("server", () => {
	let session: LSession, peer: Peerbit;
	jest.setTimeout(60 * 1000);

	beforeAll(async () => {
		session = await LSession.connected(1, {
			directory: "./tmp/peerbit/" + +new Date(),
		});
		peer = session.peers[0];
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
		const base54 = Buffer.from(serialize(program)).toString("base64");
		const pr = deserialize(Buffer.from(base54, "base64"), Program);
	});
});
