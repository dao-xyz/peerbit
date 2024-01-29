import yallist from "yallist";
import { ReplicatorRect } from "../replication.js";
import { Ed25519Keypair } from "@peerbit/crypto";

import { ReplicationSegment, Replicator } from "../role.js";
import { containsPoint, getCover, getSamples } from "../ranges.js";
const a = (await Ed25519Keypair.create()).publicKey;
const b = (await Ed25519Keypair.create()).publicKey;
const c = (await Ed25519Keypair.create()).publicKey;

// prettier-ignore
describe("ranges", () => {
    let peers: yallist<ReplicatorRect>
    let create = (...rects: ReplicatorRect[]) => {
        const sorted = rects.sort((a, b) => a.role.offset - b.role.offset)
        peers = yallist.create(sorted);
    }
    beforeEach(() => {
        peers = undefined!;
    })

    describe('getCover', () => {

        const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
        rotations.forEach((rotation) => {
            describe('rotation: ' + String(rotation), () => {

                describe('underflow', () => {
                    it('includes all', () => {


                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.1, offset: (0 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 0.1, offset: (0.333 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 0.1, offset: (0.666 + rotation) % 1, timestamp: 0n }) }
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...getCover(1, peers, 1e5, a)]).toContainAllValues([a.hashcode(), b.hashcode(), c.hashcode()])
                    })
                })

                describe("overflow", () => {
                    it("local first", () => {


                        create(
                            { publicKey: a, role: new Replicator({ factor: 1, offset: (0 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 1, offset: (0.333 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 1, offset: (0.666 + rotation) % 1, timestamp: 0n }) }
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...getCover(1, peers, 1e5, a)]).toContainAllValues([a.hashcode()])
                    })
                })

                describe("unmature", () => {

                    it('all unmature', () => {
                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.34, offset: (0 + rotation) % 1, timestamp: BigInt(+new Date) }) },
                            { publicKey: b, role: new Replicator({ factor: 0.34, offset: (0.333 + rotation) % 1, timestamp: BigInt(+new Date) }) },
                            { publicKey: c, role: new Replicator({ factor: 0.34, offset: (0.666 + rotation) % 1, timestamp: BigInt(+new Date) }) }
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...getCover(1, peers, 1e5, a)]).toContainAllValues([a.hashcode(), b.hashcode(), c.hashcode()])

                    })
                })
                describe("skip", () => {
                    it('next', async () => {


                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 0.41, offset: (0.1 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 0.5, offset: (0.3 + rotation) % 1, timestamp: BigInt(+new Date) }) }
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...getCover(0.5, peers, 1e5, a)]).toContainAllValues([a.hashcode(), b.hashcode()])
                    })
                    it('between', async () => {


                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 0.5, offset: (0.2 + rotation) % 1, timestamp: BigInt(+new Date) }) },
                            { publicKey: b, role: new Replicator({ factor: 0.34, offset: (0.3 + rotation) % 1, timestamp: 0n }) }
                        );


                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...getCover(0.5, peers, 1e5, a)]).toContainAllValues([a.hashcode(), b.hashcode()])

                    })
                })

                describe("boundary", () => {

                    it('after', () => {

                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 0.5, offset: (0.5 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: c, role: new Replicator({ factor: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n }) }
                        );

                        expect([...getCover(0.6, peers, 0, b)]).toContainAllValues([b.hashcode()])
                    })

                    it('all if non matured', () => {
                        create(
                            { publicKey: a, role: new Replicator({ factor: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n }) },
                            { publicKey: b, role: new Replicator({ factor: 0.5, offset: (0.5 + rotation) % 1, timestamp: BigInt(+new Date) }) },
                            { publicKey: c, role: new Replicator({ factor: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n }) }
                        );
                        // starting from b, we need both a and c since b is not mature to cover the width
                        expect([...getCover(0.5, peers, 1e5, b)]).toContainAllValues([a.hashcode(), b.hashcode(), c.hashcode()])
                    })
                })
            })

        })
    })

    describe("getSamples", () => {
        const rotations = [0, 0.333, 0.5, 0.8]
        rotations.forEach((rotation) => {
            it("will get at least amount of samples: " + rotation, async () => {
                create(
                    { publicKey: a, role: new Replicator({ factor: 0.2625, offset: (0.367 + rotation) % 1, timestamp: 0n }) },
                    { publicKey: b, role: new Replicator({ factor: 1, offset: (0.847 + rotation) % 1, timestamp: 0n }) }
                );
                expect(getSamples(0.78, peers, 2, 0)).toHaveLength(2)
            })

        })



        it("factor 0 ", async () => {
            create(
                { publicKey: a, role: new Replicator({ factor: 0, offset: (0.367) % 1, timestamp: 0n }) },
                { publicKey: b, role: new Replicator({ factor: 1, offset: (0.567) % 1, timestamp: 0n }) },
                { publicKey: c, role: new Replicator({ factor: 1, offset: (0.847) % 1, timestamp: 0n }) }

            );
            expect(getSamples(0.37, peers, 2, 0)).toContainAllValues([b, c].map(x => x.hashcode()))
        })

        it("factor 0 with 3 peers factor 1", async () => {
            create(
                { publicKey: a, role: new Replicator({ factor: 1, offset: 0.145, timestamp: 0n }) },
                { publicKey: b, role: new Replicator({ factor: 0, offset: 0.367, timestamp: 0n }) },
                { publicKey: c, role: new Replicator({ factor: 1, offset: 0.8473, timestamp: 0n }) }

            );
            expect(getSamples(0.937, peers, 2, 0)).toContainAllValues([a, c].map(x => x.hashcode()))
        })

        it("factor 0 with 3 peers short", async () => {
            create(
                { publicKey: a, role: new Replicator({ factor: 0.2, offset: 0.145, timestamp: 0n }) },
                { publicKey: b, role: new Replicator({ factor: 0, offset: 0.367, timestamp: 0n }) },
                { publicKey: c, role: new Replicator({ factor: 0.2, offset: 0.8473, timestamp: 0n }) }

            );
            expect(getSamples(0.937, peers, 2, 0)).toContainAllValues([a, c].map(x => x.hashcode()))
        })



        describe('maturity', () => {
            it("starting at unmatured", async () => {
                create(
                    { publicKey: a, role: new Replicator({ factor: 0.333, offset: (0.333) % 1, timestamp: 0n }) },
                    { publicKey: b, role: new Replicator({ factor: 0.333, offset: (0.666) % 1, timestamp: BigInt(+new Date) }) },
                    { publicKey: c, role: new Replicator({ factor: 0.3333, offset: (0.999) % 1, timestamp: 0n }) },

                );
                expect(getSamples(0.7, peers, 2, 1e5)).toContainAllValues([a, b, c].map(x => x.hashcode()))
            })

            it("starting at matured", async () => {
                create(
                    { publicKey: a, role: new Replicator({ factor: 0.333, offset: (0.333) % 1, timestamp: 0n }) },
                    { publicKey: b, role: new Replicator({ factor: 0.333, offset: (0.666) % 1, timestamp: BigInt(+new Date) }) },
                    { publicKey: c, role: new Replicator({ factor: 0.3333, offset: (0.999) % 1, timestamp: 0n }) },

                );
                // the offset jump will be 0.5 (a) and 0.5 + 0.5 = 1 which will intersect (c)
                expect(getSamples(0.5, peers, 2, 1e5)).toContainAllValues([a, c].map(x => x.hashcode()))
            })

            it("starting at matured-2", async () => {
                create(
                    { publicKey: a, role: new Replicator({ factor: 0.333, offset: (0.333) % 1, timestamp: 0n }) },
                    { publicKey: b, role: new Replicator({ factor: 0.333, offset: (0.666) % 1, timestamp: BigInt(+new Date) }) },
                    { publicKey: c, role: new Replicator({ factor: 0.3333, offset: (0.999) % 1, timestamp: 0n }) },

                );
                // the offset jump will be 0.2 (a) and 0.2 + 0.5 = 0.7 which will intersect (b) (unmatured)
                expect(getSamples(0, peers, 2, 1e5)).toContainAllValues([a, c].map(x => x.hashcode()))
            })
        })
    })




    describe("containsPoint", () => {
        it("length 0", () => {
            expect(containsPoint({ factor: 0, offset: 0 }, 0)).toBeFalse();
            expect(containsPoint({ factor: 0, offset: 0 }, 0.1)).toBeFalse();
            expect(containsPoint({ factor: 0, offset: 0.1 }, 0)).toBeFalse();
        });

        it("length 1", () => {
            expect(containsPoint({ factor: 1, offset: 0 }, 0)).toBeTrue();
            expect(containsPoint({ factor: 1, offset: 0 }, 0.1)).toBeTrue();
            expect(containsPoint({ factor: 1, offset: 0.1 }, 0)).toBeTrue();
        });


        it("wrapped", () => {
            expect(containsPoint({ factor: 0.3, offset: 0.8 }, 0.1)).toBeTrue();
            expect(containsPoint({ factor: 0.3, offset: 0.8 }, 0.2)).toBeFalse();
        });

        it("unwrapped", () => {
            expect(containsPoint({ factor: 0.1, offset: 0.8 }, 0.89)).toBeTrue();
            expect(containsPoint({ factor: 0.1, offset: 0.8 }, 0.91)).toBeFalse();
        });
    });
})
