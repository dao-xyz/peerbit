// This more like a playground as of now
// No real tests yet,
// But there are ways here to generate base64 string for programs
import { deserialize, serialize } from "@dao-xyz/borsh";
import { Program, type ProgramClient } from "@peerbit/program";
import { DString } from "@peerbit/string";
import { TestSession } from "@peerbit/test-utils";
import { PermissionedString } from "../src/index.js";

describe("server", () => {
	let session: TestSession, peer: ProgramClient;

	before(async () => {
		session = await TestSession.connected(1, {
			directory: "./tmp/peerbit/" + +new Date(),
		});
		peer = session.peers[0];
	});

	after(async () => {
		await peer.stop();
		await session.stop();
	});

	it("_", async () => {
		const program = new PermissionedString({
			store: new DString({}),
			trusted: [peer.identity.publicKey],
		});
		const base54 = Buffer.from(serialize(program)).toString("base64");
		deserialize(Buffer.from(base54, "base64"), Program);
	});
});
