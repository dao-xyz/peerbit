import {
    TrustedNetwork,
    IdentityRelation,
} from "@dao-xyz/peerbit-trusted-network";
import { Peerbit } from "@dao-xyz/peerbit";
import { DString } from "@dao-xyz/peerbit-string";
import { Session } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import http from "http";
import { client, startServer } from "../api.js";
import { jest } from "@jest/globals";
import { PermissionedString } from "@dao-xyz/peerbit-node-test-lib";

describe("server", () => {
    let session: Session, peer: Peerbit, server: http.Server;
    jest.setTimeout(60 * 1000);

    beforeAll(async () => {
        session = await Session.connected(1);
    });

    beforeEach(async () => {
        peer = await Peerbit.create(session.peers[0].ipfs, {
            directory: "./peerbit/" + +new Date(),
        });
        server = await startServer(peer);
    });
    afterEach(() => {
        server.close();
    });

    afterAll(async () => {
        await session.stop();
    });

    describe("ipfs", () => {
        it("id", async () => {
            const c = await client();
            expect(await c.ipfs.id.get()).toEqual(
                (await peer.ipfs.id()).id.toString()
            );
        });
        it("addresses", async () => {
            const c = await client();
            expect(
                (await c.ipfs.addresses.get()).map((x) => x.toString())
            ).toEqual(
                (await peer.ipfs.id()).addresses.map((x) => x.toString())
            );
        });
    });

    it("topics", async () => {
        const c = await client();
        expect(await c.topics.get()).toHaveLength(0);
        await c.topic.put("1");
        await c.topic.put("2");
        await c.topic.put("3");
        expect(await c.topics.get()).toHaveLength(3);
    });

    it("program", async () => {
        const c = await client();
        const program = new PermissionedString({
            store: new DString({}),
            network: new TrustedNetwork({ rootTrust: peer.identity.publicKey }),
        });
        program.setupIndices();
        const address = await c.program.put(program, "topic");
        expect(await c.program.get(address)).toBeInstanceOf(PermissionedString);
    });
    it("library", async () => {
        const c = await client();
        await c.library.put("@dao-xyz/peerbit-node-test-lib");
    });

    it("network", async () => {
        const c = await client();
        const program = new PermissionedString({
            store: new DString({}),
            network: new TrustedNetwork({ rootTrust: peer.identity.publicKey }),
        });
        program.setupIndices();
        const address = await c.program.put(program, "topic");
        expect(await c.program.get(address)).toBeInstanceOf(PermissionedString);
        expect(await c.network.peers.get(address)).toHaveLength(0);
        const pk = (await Ed25519Keypair.create()).publicKey;
        await c.network.peer.put(address, pk);
        const peers = await c.network.peers.get(address);
        expect(peers).toHaveLength(1);
        expect(
            (peers?.[0] as IdentityRelation).from.equals(
                peer.identity.publicKey
            )
        );
        expect((peers?.[0] as IdentityRelation).to.equals(pk));
    });
});
