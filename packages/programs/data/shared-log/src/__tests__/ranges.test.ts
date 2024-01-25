import yallist from "yallist";
import { ReplicatorRect } from "../replication.js";
import { Ed25519Keypair } from "@peerbit/crypto";

import { Replicator } from "../role.js";
import { getCover } from "../ranges.js";
const a = (await Ed25519Keypair.create()).publicKey;
const b = (await Ed25519Keypair.create()).publicKey;
const c = (await Ed25519Keypair.create()).publicKey;

// prettier-ignore
describe("ranges", () => {
    describe('getCover', () => {


        const rotations = [0, 0.333, 0.5, 0.8]
        rotations.forEach((rotation) => {
            describe('rotation: ' + String(rotation), () => {
                let peers: yallist<ReplicatorRect>
                let create = (...rects: ReplicatorRect[]) => {
                    const sorted = rects.sort((a, b) => a.role.offset - b.role.offset)
                    peers = yallist.create(sorted);
                }

                beforeEach(() => {
                    peers = undefined!;
                })
                describe("skip", () => {
                    it('next', async () => {


                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 0.34, offset: (0.1 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 0.5, offset: (0.3 + rotation) % 1, timestamp: BigInt(+new Date) }) }
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...getCover(0.5, peers, 1e5, a)]).toContainValues([a.hashcode(), b.hashcode()])
                    })
                    it('between', async () => {


                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 0.5, offset: (0.2 + rotation) % 1, timestamp: BigInt(+new Date) }) },
                            { publicKey: b, role: new Replicator({ factor: 0.34, offset: (0.3 + rotation) % 1, timestamp: 0n }) }
                        );


                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...getCover(0.5, peers, 1e5, a)]).toContainValues([a.hashcode(), b.hashcode()])

                    })
                })

                describe("boundary", () => {

                    it('after', () => {

                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 0.5, offset: (0.5 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n }) }
                        );

                        expect([...getCover(0.6, peers, 0, b)]).toContainValues([b.hashcode()])
                    })

                    it('all if non matured', () => {
                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 0.5, offset: (0.5 + rotation) % 1, timestamp: BigInt(+new Date) }) },
                            { publicKey: c, role: new Replicator({ factor: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n }) }
                        );
                        // starting from b, we need both a and c since b is not mature to cover the width
                        expect([...getCover(0.5, peers, 1e5, b)]).toContainValues([a.hashcode(), b.hashcode(), c.hashcode()])
                    })
                })
            })

        })
    })
})
