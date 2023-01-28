// This more like a playground as of now
// No tests yet,
// But there are ways here to generate base64 string for programs
import { Peerbit } from "@dao-xyz/peerbit";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import http from "http";
import { jest } from "@jest/globals";

describe("server", () => {
	let session: LSession, peer: Peerbit, server: http.Server;
	jest.setTimeout(60 * 1000);

	beforeAll(async () => {
		session = await LSession.connected(1);
		peer = await Peerbit.create({
			libp2p: session.peers[0],
			directory: "./peerbit/" + +new Date(),
		});
	});

	afterAll(async () => {
		await session.stop();
	});
	it("_", () => {
		expect(1).toEqual(1);
	});
	/*     it("x", async () => {
			const program = new PermissionedString({
				store: new DString({}),
				network: new TrustedNetwork({ rootTrust: peer.identity.publicKey }),
			});
			program.setupIndices();
			const base542 = Buffer.from(serialize(program)).toString("base64");
			const t = 123;
		}); */
});
