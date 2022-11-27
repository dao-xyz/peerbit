import { v4 as uuid } from "uuid";
import type { Message } from "@libp2p/interface-pubsub";
import { waitFor } from "@dao-xyz/peerbit-time";
import { Session, waitForPeers } from "@dao-xyz/peerbit-test-utils";
import {
    AccessError,
    decryptVerifyInto,
    Ed25519Keypair,
    Ed25519PublicKey,
    X25519Keypair,
    X25519PublicKey,
    X25519SecretKey,
} from "@dao-xyz/peerbit-crypto";
import { RequestV0, ResponseV0, send, respond, RPC, RPCMessage } from "../";
import { Ed25519Identity } from "@dao-xyz/ipfs-log";
import { Program } from "@dao-xyz/peerbit-program";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        privateKey: ed.privateKey,
        sign: (data) => ed.sign(data),
    } as Ed25519Identity;
};

@variant("payload")
class Body {
    @field(UInt8ArraySerializer)
    arr: Uint8Array;
    constructor(properties?: { arr: Uint8Array }) {
        if (properties) {
            this.arr = properties.arr;
        }
    }
}

@variant("rpc-test")
class RPCTest extends Program {
    @field({ type: RPC })
    query: RPC<Body, Body>;

    async setup(): Promise<void> {
        await this.query.setup({
            responseType: Body,
            queryType: Body,
            context: this,
            responseHandler: (query, from) => {
                const resp = query;
                return resp;
            },
        });
    }
}

describe("rpc", () => {
    let session: Session, responder: RPCTest, reader: RPCTest;
    beforeAll(async () => {
        session = await Session.connected(3);

        responder = new RPCTest();
        responder.query = new RPC();
        const topic = uuid();
        await responder.init(session.peers[0].ipfs, await createIdentity(), {
            topic,
            store: { replicate: true } as any,
        } as any);
        reader = deserialize(serialize(responder), RPCTest);
        await reader.init(session.peers[1].ipfs, await createIdentity(), {
            topic,
            store: {} as any,
        } as any);

        await waitForPeers(
            session.peers[1].ipfs,
            [session.peers[0].id],
            responder.query.rpcTopic
        );
    });
    afterAll(async () => {
        await session.stop();
    });

    it("any", async () => {
        let results: Body[] = [];
        await reader.query.send(
            new Body({
                arr: new Uint8Array([0, 1, 2]),
            }),
            (resp) => {
                results.push(resp);
            },
            { waitForAmount: 1 }
        );

        await waitFor(() => results.length === 1);
    });

    it("context", async () => {
        let results: Body[] = [];

        // Unknown context (expect no results)
        await reader.query.send(
            new Body({
                arr: new Uint8Array([0, 1, 2]),
            }),
            (resp) => {
                results.push(resp);
            },
            { maxAggregationTime: 3000, context: "wrong context" }
        );

        // Explicit
        await reader.query.send(
            new Body({
                arr: new Uint8Array([0, 1, 2]),
            }),
            (resp) => {
                results.push(resp);
            },
            { waitForAmount: 1, context: reader.address.toString() }
        );
        expect(results).toHaveLength(1);

        // Implicit
        await reader.query.send(
            new Body({
                arr: new Uint8Array([0, 1, 2]),
            }),
            (resp) => {
                results.push(resp);
            },
            { waitForAmount: 1 }
        );
        expect(results).toHaveLength(2);
    });

    it("timeout", async () => {
        let maxAggregationTime = 5000;

        let results: Body[] = [];
        const t0 = +new Date();
        await reader.query.send(
            new Body({
                arr: new Uint8Array([0, 1, 2]),
            }),
            (resp) => {
                results.push(resp);
            },
            {
                maxAggregationTime,
            }
        );
        const t1 = +new Date();
        expect(Math.abs(t1 - t0 - maxAggregationTime)).toBeLessThan(200); // some threshold
        expect(results).toHaveLength(1);
    });

    it("waitForAmount", async () => {
        let waitForAmount = 2;
        let maxAggregationTime = 2000;

        const topic = uuid();
        for (let i = 1; i < 3; i++) {
            await session.peers[i].ipfs.pubsub.subscribe(
                topic,
                async (msg: Message) => {
                    let { result: request } = await decryptVerifyInto(
                        msg.data,
                        RPCMessage,
                        () => Promise.resolve(undefined)
                    );
                    if (request instanceof RequestV0) {
                        await respond(
                            session.peers[i].ipfs,
                            topic,
                            request,
                            new ResponseV0({
                                response: serialize(
                                    new Body({ arr: new Uint8Array([0, 1, 2]) })
                                ),
                                context: "context",
                            })
                        );
                    }
                }
            );
        }

        await waitForPeers(
            session.peers[0].ipfs,
            [session.peers[1].id, session.peers[2].id],
            topic
        );

        let results: Uint8Array[] = [];
        await send(
            session.peers[0].ipfs,
            topic,
            topic,
            new RequestV0({
                request: serialize(
                    new Body({ arr: new Uint8Array([0, 1, 2]) })
                ),
            }),
            (resp) => {
                results.push(resp.response);
            },
            {
                maxAggregationTime,
                waitForAmount,
            }
        );

        await waitFor(() => results.length == waitForAmount);
    });

    it("signed", async () => {
        let waitForAmount = 1;

        let maxAggregationTime = 3000;

        const sender = await createIdentity();
        const responder = await createIdentity();
        const topic = uuid();
        await session.peers[1].ipfs.pubsub.subscribe(
            topic,
            async (msg: Message) => {
                let { result: request, from } = await decryptVerifyInto(
                    msg.data,
                    RPCMessage,
                    () => Promise.resolve(undefined)
                );
                if (request instanceof RequestV0) {
                    // Check that it was signed by the sender
                    expect(from).toBeInstanceOf(Ed25519PublicKey);
                    expect(
                        (from as Ed25519PublicKey).equals(sender.publicKey)
                    ).toBeTrue();

                    await respond(
                        session.peers[1].ipfs,
                        topic,
                        request,
                        new ResponseV0({
                            response: new Uint8Array([0, 1, 2]),
                            context: "context",
                        }),
                        { signer: responder }
                    );
                }
            }
        );

        await waitForPeers(session.peers[0].ipfs, [session.peers[1].id], topic);

        let results: Uint8Array[] = [];
        await send(
            session.peers[0].ipfs,
            topic,
            topic,
            new RequestV0({
                request: new Uint8Array([0, 1, 2]),
            }),
            (resp, from) => {
                // Check that it was signed by the responder
                expect(from).toBeInstanceOf(Ed25519PublicKey);
                expect(
                    (from as Ed25519PublicKey).equals(responder.publicKey)
                ).toBeTrue();

                results.push(resp.response);
            },
            {
                maxAggregationTime,
                waitForAmount,
                signer: sender,
            }
        );

        await waitFor(() => results.length == waitForAmount);
    });

    it("encrypted", async () => {
        // query encrypted and respond encrypted
        let waitForAmount = 1;
        let maxAggregationTime = 3000;

        const responder = await createIdentity();
        const requester = await createIdentity();
        const topic = uuid();
        await session.peers[1].ipfs.pubsub.subscribe(
            topic,
            async (msg: Message) => {
                try {
                    let { result: request } = await decryptVerifyInto(
                        msg.data,
                        RequestV0,
                        async (keys) => {
                            return {
                                index: 0,
                                keypair: await X25519Keypair.from(
                                    new Ed25519Keypair({ ...responder })
                                ),
                            };
                        }
                    );
                    if (request instanceof RequestV0) {
                        await respond(
                            session.peers[1].ipfs,
                            topic,
                            request,
                            new ResponseV0({
                                response: new Uint8Array([0, 1, 2]),
                                context: "context",
                            })
                        );
                    }
                } catch (error) {
                    if (error instanceof AccessError) {
                        return;
                    }
                    throw error;
                }
            }
        );
        await waitForPeers(session.peers[0].ipfs, [session.peers[1].id], topic);

        let results: Uint8Array[] = [];
        await send(
            session.peers[0].ipfs,
            topic,
            topic,
            new RequestV0({
                request: new Uint8Array([0, 1, 2]),
                respondTo: await X25519PublicKey.from(requester.publicKey),
            }),
            (resp) => {
                results.push(resp.response);
            },
            {
                maxAggregationTime,
                waitForAmount,
                signer: requester,
                sendKey: await X25519Keypair.from(
                    new Ed25519Keypair({ ...requester })
                ),
                /* keyResolver: async () => {
                    return {
                        index: 0,
                        keypair: await X25519Keypair.from(
                            new Ed25519Keypair({ ...requester })
                        ),
                    };
                }, */
                encryption: {
                    key: () => new Ed25519Keypair({ ...requester }),
                    responders: [
                        await X25519PublicKey.from(responder.publicKey),
                    ],
                },
            }
        );

        await waitFor(() => results.length == waitForAmount);
    });
});
