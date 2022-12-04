import {
    TrustedNetwork,
    IdentityRelation,
} from "@dao-xyz/peerbit-trusted-network";
import { Peerbit } from "@dao-xyz/peerbit";
import { DString } from "@dao-xyz/peerbit-string";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import http from "http";
import { client, startServer } from "../api.js";
import { jest } from "@jest/globals";
import { PermissionedString } from "@dao-xyz/peerbit-node-test-lib";

describe("libp2p only", () => {
    let session: LSession, server: http.Server;
    jest.setTimeout(60 * 1000);

    beforeAll(async () => {
        session = await LSession.connected(1);
    });

    beforeEach(async () => {
        server = await startServer(session.peers[0], 7676);
    });
    afterEach(() => {
        server.close();
    });

    afterAll(async () => {
        await session.stop();
    });

    it("use cli as libp2p cli", async () => {
        const c = await client("http://localhost:" + 7676);
        await c.topic.put("1", false);
        await c.topic.put("2", false);
        try {
            await c.topic.put("3", true);
            fail();
        } catch (error) {
            // not peerbit, so should not succeed
        }
        expect(await c.topics.get(false)).toContainAllValues([
            "_block",
            "1",
            "2",
        ]);
    });
});
describe("server", () => {
    let session: LSession, peer: Peerbit, server: http.Server;
    jest.setTimeout(60 * 1000);

    beforeAll(async () => {
        session = await LSession.connected(1);
    });

    beforeEach(async () => {
        peer = await Peerbit.create(session.peers[0], {
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
            expect(await c.peer.id.get()).toEqual(
                peer.libp2p.peerId.toString()
            );
        });
        it("addresses", async () => {
            const c = await client();
            expect(
                (await c.peer.addresses.get()).map((x) => x.toString())
            ).toEqual(
                (await peer.libp2p.getMultiaddrs()).map((x) => x.toString())
            );
        });
    });

    it("topics", async () => {
        const c = await client();
        expect(await c.topics.get(false)).toHaveLength(1); // _block topic
        await c.topic.put("1", true); // 2 pubsub topics
        await c.topic.put("2", true); // 2 pubsub topics
        await c.topic.put("3", false); // 1 pubsub topic
        expect(await c.topics.get(true)).toHaveLength(2); // two topic we are replicating
        expect(await c.topics.get(false)).toHaveLength(6); // not 3 but 5 + _blockTopic because extra topics as replciator
    });

    it("program", async () => {
        const c = await client();
        const program = new PermissionedString({
            store: new DString({}),
            network: new TrustedNetwork({ rootTrust: peer.identity.publicKey }),
        });
        program.setupIndices();
        const address = await c.program.put(program, "topic");
        const programInstance = await c.program.get(address);
        expect(programInstance).toBeInstanceOf(PermissionedString);
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
