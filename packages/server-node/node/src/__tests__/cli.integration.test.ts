// This more like a playground as of now
// No tests yet,
// But there are ways here to generate base64 string for programs
import { serialize } from "@dao-xyz/borsh";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { Peerbit } from "@dao-xyz/peerbit";
import { DString } from "@dao-xyz/peerbit-string";
import { Session } from "@dao-xyz/peerbit-test-utils";
import http from "http";
import { jest } from "@jest/globals";
import { PermissionedString } from "@dao-xyz/peerbit-node-test-lib";

describe("server", () => {
    let session: Session, peer: Peerbit, server: http.Server;
    jest.setTimeout(60 * 1000);

    beforeAll(async () => {
        session = await Session.connected(1);
        peer = await Peerbit.create(session.peers[0].ipfs, {
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
