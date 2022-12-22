import path from "path";
import assert from "assert";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { DirectChannel as Channel } from "../direct-channel.js";
import { v1 as PROTOCOL } from "../protocol.js";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { waitForPeers } from "../wait-for-peers.js";

describe(`DirectChannel`, function () {
    let expectedPeerIDs: string[] = [];
    let session: LSession;
    beforeEach(async () => {
        session = await LSession.connected(3);

        // Note, we only create channels between peer1 and peer2 in these test,
        // peer3 is used for "external actor" tests
        expectedPeerIDs = Array.from(
            [session.peers[0].peerId, session.peers[1].peerId].map((x) =>
                x.toString()
            )
        ).sort();
    });

    afterEach(async () => {
        await session.stop();
    });

    describe("create a channel", function () {
        it("has two participants", async () => {
            const c = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                () => {}
            );
            expect(c.peers).toContainAllValues(expectedPeerIDs);
            c.close();
        });

        it("has correct ID", async () => {
            const expectedID = path.join(
                "/",
                PROTOCOL,
                expectedPeerIDs.join("/")
            );
            const c = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                () => {}
            );
            assert.deepEqual(c.id, expectedID);
            c.close();
        });

        it("has two peers", async () => {
            const c1 = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                () => {}
            );
            const c2 = await Channel.open(
                session.peers[1],
                session.peers[0].peerId,
                () => {}
            );
            expect(c1.peers).toContainAllValues(expectedPeerIDs);
            expect(c2.peers).toContainAllValues(expectedPeerIDs);
            expect(c1.id).toEqual(
                path.join("/", PROTOCOL, expectedPeerIDs.join("/"))
            );
            expect(c2.id).toEqual(
                path.join("/", PROTOCOL, expectedPeerIDs.join("/"))
            );
            c1.close();
            c2.close();
        });

        it("can be created with one line", async () => {
            const c = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                () => {}
            );
            const topics = await session.peers[0].pubsub.getTopics();
            const channelID = topics.find((e) => e === c.id);
            expect(channelID).toEqual(c.id);
            c.close();
        });
    });

    describe("properties", function () {
        let c: Channel;

        beforeEach(async () => {
            c = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                () => {}
            );
        });

        afterEach(() => {
            if (c) {
                c.close();
            }
        });

        it("has an id", async () => {
            expect(c.id).toEqual(
                path.join("/", PROTOCOL, expectedPeerIDs.join("/"))
            );
        });

        it("has two peers", async () => {
            expect(c.peers.length).toEqual(2);
            expect(c.peers).toContainAllValues(expectedPeerIDs);
        });
    });

    describe("messaging", function () {
        it("sends and receives messages", async () => {
            let c1Response: boolean | undefined = undefined;
            const c1 = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                (m) => {
                    /*  if (m.type === "signed"
                     ) { */
                    /*    expect(
                           m.from.equals(session.peers[1].peerId.toString())
                       ); */
                    expect(new Uint8Array(m.data)).toEqual(new Uint8Array([1]));
                    expect(m.topic).toEqual(c1.id);
                    expect(m.topic).toEqual(c2.id);
                    /*   } else {
                          expect(false);
                      } */

                    c1.close();
                    c2.close();
                    c1Response = true;
                }
            );
            const c2 = await Channel.open(
                session.peers[1],
                session.peers[0].peerId,
                (m) => {
                    /* if (m.type === "signed") { */
                    /*    expect(
                           m.from.equals(session.peers[0].peerId.toString())
                       ); */
                    expect(new Uint8Array(m.data)).toEqual(new Uint8Array([0]));
                    expect(m.topic).toEqual(c1.id);
                    expect(m.topic).toEqual(c2.id);
                    /*  } else {
                         expect(false);
                     } */

                    c2.send(new Uint8Array([1]));
                }
            );

            await c1.connect();
            await c2.connect();

            await c1.send(new Uint8Array([0]));
            await waitFor(() => !!c1Response);
        });

        it("can share a channel for multiple handlers", async () => {
            let c1Response: boolean | undefined = undefined;
            const c1 = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                (m) => {
                    c1Response = true;
                }
            );
            const c2 = await Channel.open(
                session.peers[1],
                session.peers[0].peerId,
                (m) => {
                    c2.send(new Uint8Array([1]));
                }
            );
            const c1x = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                (m) => {}
            );
            const c2x = await Channel.open(
                session.peers[1],
                session.peers[0].peerId,
                (m) => {}
            );

            await c1.connect();
            await c2.connect();

            await c1x.connect();
            await c2x.connect();
            await c1x.close();
            await c2x.close();

            await c1.send(new Uint8Array([0]));
            await waitFor(() => !!c1Response);
        });
    });

    describe("connect", function () {
        it("connects the peers", async () => {
            let c1: Channel, c2: Channel;

            let callbackJoinedCounter: number = 0;
            let callbackLeavingCounter: number = 0;

            c1 = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                () => {},
                {
                    onNewPeerCallback: (channel) => {
                        callbackJoinedCounter += 1;
                    },
                    onPeerLeaveCallback: (channel) => {
                        callbackLeavingCounter += 1;
                    },
                }
            );
            c2 = await Channel.open(
                session.peers[1],
                session.peers[0].peerId,
                () => {}
            );

            let peers = await session.peers[0].pubsub.getSubscribers(c1.id);
            // assert.deepEqual(peers, [])

            await c1.connect();

            peers = await session.peers[0].pubsub.getSubscribers(c1.id);
            expect(peers.map((x) => x.toString())).toContainAllValues([
                session.peers[1].peerId.toString(),
            ]);
            await delay(2000); // wait for all callbacks
            expect(callbackJoinedCounter).toEqual(1);
            await c2.close({ subscription: true });
            await delay(2000); // wait for all callbacks
            expect(callbackLeavingCounter).toEqual(1);
            await c1.close({ subscription: true });
        });
    });

    describe("disconnecting", function () {
        it("closes a channel", async () => {
            const c1 = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                () => {}
            );
            const c2 = await Channel.open(
                session.peers[1],
                session.peers[0].peerId,
                () => {}
            );

            await c1.connect();
            await c2.connect();

            return new Promise(async (resolve, reject) => {
                expect(c1._closed).toEqual(false);
                expect(c1._isClosed()).toEqual(false);
                c1.close({ subscription: true });
                const topics1 = await session.peers[0].pubsub.getTopics();
                assert.deepEqual(topics1, []);
                expect(c1._closed).toEqual(true);
                expect(c1._isClosed()).toEqual(true);

                expect(c2._closed).toEqual(false);
                expect(c2._isClosed()).toEqual(false);
                c2.close({ subscription: true });
                const topics2 = await session.peers[1].pubsub.getTopics();
                assert.deepEqual(topics2, []);
                expect(c2._closed).toEqual(true);
                expect(c2._isClosed()).toEqual(true);

                setTimeout(async () => {
                    const peers1 = await session.peers[0].pubsub.getSubscribers(
                        c1.id
                    );
                    const peers2 = await session.peers[1].pubsub.getSubscribers(
                        c1.id
                    );
                    assert.deepEqual(peers1, []);
                    assert.deepEqual(peers2, []);
                    resolve(true);
                }, 200);
            });
        });
    });

    describe("non-participant peers can't send messages", function () {
        it("doesn't receive unwanted messages", async () => {
            const c1 = await Channel.open(
                session.peers[0],
                session.peers[1].peerId,
                (m) => {
                    /*  if (m.type === "signed") {
                         expect(
                             m.from.equals(session.peers[1].peerId.toString())
                         ); */
                    expect(new Uint8Array(m.data)).toEqual(new Uint8Array([0]));
                    expect(m.topic).toEqual(c1.id);
                    expect(m.topic).toEqual(c2.id);
                    /* } else {
                        expect(false);
                    } */
                }
            );
            const c2 = await Channel.open(
                session.peers[1],
                session.peers[0].peerId,
                () => {}
            );

            await c1.connect();
            await c2.connect();

            await session.peers[2].pubsub.subscribe(c1.id);
            await waitForPeers(
                session.peers[0],
                [session.peers[2].peerId.toString()],
                c1.id,
                c1._isClosed.bind(c1)
            );
            await session.peers[2].pubsub.publish(c1.id, Buffer.from("OMG!"));

            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    c2.send(new Uint8Array([0]));
                    setTimeout(() => {
                        c1.close();
                        c2.close();
                        resolve(true);
                    }, 1000);
                }, 1000);
            });
        });
    });
});
