// This more like a playground as of now
// No real tests yet,
// But there are ways here to generate base64 string for programs

import { serialize, deserialize } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { Peerbit } from "@dao-xyz/peerbit";
import { DString } from "@dao-xyz/peerbit-string";
import { Session } from "@dao-xyz/peerbit-test-utils";
import { jest } from "@jest/globals";
import { PermissionedString } from "..";

describe("server", () => {
    let session: Session, peer: Peerbit;
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

    it("_", async () => {
        const program = new PermissionedString({
            store: new DString({}),
            network: new TrustedNetwork({ rootTrust: peer.identity.publicKey }),
        });
        program.setupIndices();
        const base54 = Buffer.from(serialize(program)).toString("base64");
        const pr = deserialize(Buffer.from(base54, "base64"), Program);
    });
});
