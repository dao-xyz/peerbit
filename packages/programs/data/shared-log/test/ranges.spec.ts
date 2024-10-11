import {
	Ed25519Keypair,
	type Ed25519PublicKey,
	randomBytes,
} from "@peerbit/crypto";
import type { Index } from "@peerbit/indexer-interface";
import { create as createIndices } from "@peerbit/indexer-sqlite3";
import { LamportClock, Meta } from "@peerbit/log";
import { expect } from "chai";
import {
	EntryReplicated,
	ReplicationIntent,
	ReplicationRangeIndexable,
	getCoverSet,
	getDistance,
	getEvenlySpacedU32,
	getSamples as getSamplesMap,
	hasCoveringRange,
	toRebalance,
} from "../src/ranges.js";
import { HALF_MAX_U32, MAX_U32, scaleToU32 } from "../src/role.js";

const getSamples = async (
	offset: number,
	peers: Index<ReplicationRangeIndexable>,
	count: number,
	roleAge: number,
) => {
	const map = await getSamplesMap(
		getEvenlySpacedU32(offset, count),
		peers,
		roleAge,
	);
	return [...map.keys()];
};

// prettier-ignore
describe("ranges", () => {
    let peers: Index<ReplicationRangeIndexable>
    let a: Ed25519PublicKey, b: Ed25519PublicKey, c: Ed25519PublicKey;

    let create = async (...rects: ReplicationRangeIndexable[]) => {
        const indices = (await createIndices())
        await indices.start()
        const index = await indices.init({ schema: ReplicationRangeIndexable })
        for (const rect of rects) {
            await index.put(rect)
        }
        peers = index
    }
    before(async () => {
        a = (await Ed25519Keypair.create()).publicKey;
        b = (await Ed25519Keypair.create()).publicKey;
        c = (await Ed25519Keypair.create()).publicKey;

        // sort keys by hash to make test assertions easier
        if (a.hashcode() > b.hashcode()) {
            const tmp = a;
            a = b;
            b = tmp;
        }
        if (b.hashcode() > c.hashcode()) {
            const tmp = b;
            b = c;
            c = tmp;
        }
        if (a.hashcode() > b.hashcode()) {
            const tmp = a;
            a = b;
            b = tmp;
        }

    })
    beforeEach(() => {
        peers = undefined!;
    })

    describe('getCover', () => {

        const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
        rotations.forEach((rotation) => {
            describe('rotation: ' + String(rotation), () => {

                describe('underflow', () => {
                    it('includes all', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.1, offset: (0 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.1, offset: (0.333 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.1, offset: (0.666 + rotation) % 1, timestamp: 0n })
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32 })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])

                    })
                })

                describe("overflow", () => {
                    it("local first", async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 1, offset: (0 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 1, offset: (0.333 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 1, offset: (0.666 + rotation) % 1, timestamp: 0n }))

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32 })]).to.have.members([a.hashcode()])
                    })
                })

                describe("unmature", () => {

                    it('all unmature', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.34, offset: (0 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.34, offset: (0.333 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.34, offset: (0.666 + rotation) % 1, timestamp: BigInt(+new Date) })
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32 })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])

                    })


                    it('full width all unmature', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 1, offset: (0 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 1, offset: (0.333 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 1, offset: (0.666 + rotation) % 1, timestamp: BigInt(+new Date) })
                        );

                        // special case, assume we only look into selef
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32 })]).to.have.members([a.hashcode()])

                    })

                    it('two unmature', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.34, offset: (0.333 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.34, offset: (0.666 + rotation) % 1, timestamp: BigInt(+new Date) })
                        );


                        // should not be included. TODO is this always expected behaviour?
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32 })]).to.have.members([a.hashcode()])

                    })


                })

                describe('eager', () => {
                    it('all unmature', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.34, offset: (0 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.34, offset: (0.333 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.34, offset: (0.666 + rotation) % 1, timestamp: BigInt(+new Date) })
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32, eager: true })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])
                    })
                    it('full width all mature', async () => {

                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 1, offset: (0 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 1, offset: (0.333 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 1, offset: (0.666 + rotation) % 1, timestamp: 0n })
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32, eager: true })]).to.have.members([a.hashcode()])
                    })


                    it('full width all unmature', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 1, offset: (0 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 1, offset: (0.333 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 1, offset: (0.666 + rotation) % 1, timestamp: BigInt(+new Date) })
                        );

                        // special case, assume we only look into selef
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32, eager: true })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])

                    })

                    it('two unmature', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.34, offset: (0.333 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.34, offset: (0.666 + rotation) % 1, timestamp: BigInt(+new Date) })
                        );


                        // should not be included. TODO is this always expected behaviour?
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32, eager: true })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: b, widthToCoverScaled: MAX_U32, eager: true })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: c, widthToCoverScaled: MAX_U32, eager: true })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])

                    })
                })


                describe("skip", () => {
                    it('next', async () => {


                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.41, offset: (0.1 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.5, offset: (0.3 + rotation) % 1, timestamp: BigInt(+new Date) })
                        );

                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32 / 2 })]).to.have.members([a.hashcode(), b.hashcode()])
                    })
                    it('between', async () => {


                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.34, offset: (0 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.5, offset: (0.2 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.34, offset: (0.3 + rotation) % 1, timestamp: 0n })
                        );


                        // we try to cover 0.5 starting from a
                        // this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: MAX_U32 / 2 })]).to.have.members([a.hashcode(), b.hashcode()])

                    })
                })

                describe("boundary", () => {

                    it('exact', async () => {

                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.5, offset: (0.2 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.5, offset: (0.5 + rotation) % 1, timestamp: 0n })
                        );

                        // because of rounding errors, a cover width of 0.5 might yield unecessary results
                        expect([...await getCoverSet({ peers, roleAge: 0, start: a, widthToCoverScaled: 0.499 * MAX_U32 })]).to.have.members([a.hashcode()])
                    })

                    it('after', async () => {

                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.1, offset: (0.21 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.5, offset: (0.5 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n })
                        );

                        expect([...await getCoverSet({ peers, roleAge: 0, start: b, widthToCoverScaled: scaleToU32(0.6) })]).to.have.members([b.hashcode()])
                    })

                    it('skip matured', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.5, offset: (0.5 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n })
                        );
                        // starting from b, we need both a and c since b is not mature to cover the width
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: a, widthToCoverScaled: scaleToU32(0.5) })]).to.have.members([a.hashcode(), c.hashcode()])
                    })

                    it('include start node identity', async () => {
                        await create(
                            new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.5, offset: (0.5 + rotation) % 1, timestamp: BigInt(+new Date) }),
                            new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n })
                        );
                        // starting from b, we need both a and c since b is not mature to cover the width
                        expect([...await getCoverSet({ peers, roleAge: 1e5, start: b, widthToCoverScaled: scaleToU32(0.5) })]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()])
                    })

                    describe('strict', () => {
                        it('no boundary', async () => {
                            await create(
                                new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n, mode: ReplicationIntent.Strict }),
                                new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.5, offset: (0.5 + rotation) % 1, timestamp: 0n, mode: ReplicationIntent.Strict }),
                                new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n, mode: ReplicationIntent.Strict })
                            );
                            // starting from b, we need both a and c since b is not mature to cover the width
                            expect([...await getCoverSet({ peers, roleAge: 1e5, start: b, widthToCoverScaled: scaleToU32(0.51) })]).to.have.members([b.hashcode()])
                        })

                        it('empty set boundary', async () => {
                            await create(
                                new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n, mode: ReplicationIntent.Strict }),
                                new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.1, offset: (0.81 + rotation) % 1, timestamp: 0n, mode: ReplicationIntent.Strict })
                            );
                            // starting from b, we need both a and c since b is not mature to cover the width
                            expect([...await getCoverSet({ peers, roleAge: 1e5, start: scaleToU32((0.5 + rotation) % 1), widthToCoverScaled: scaleToU32(0.3) })]).to.have.members([])
                        })

                        it('overlapping', async () => {
                            await create(
                                new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.1, offset: (0.2 + rotation) % 1, timestamp: 0n, mode: ReplicationIntent.Strict }),
                            );
                            // starting from b, we need both a and c since b is not mature to cover the width
                            expect([...await getCoverSet({ peers, roleAge: 1e5, start: scaleToU32((0 + rotation) % 1), widthToCoverScaled: scaleToU32(0.6) })]).to.have.members([a.hashcode()])
                        })
                    })
                })


            })

        })
    })

    describe("getSamples", () => {
        const rotations = [0, 0.333, 0.5, 0.8]
        rotations.forEach((rotation) => {
            describe('samples correctly: ' + rotation, () => {
                it("1 and less than 1", async () => {
                    await create(
                        new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.2625, offset: (0.367 + rotation) % 1, timestamp: 0n }),
                        new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 1, offset: (0.847 + rotation) % 1, timestamp: 0n }))
                    expect(await getSamples(scaleToU32(0.78), peers, 2, 0)).to.have.length(2)
                })

                it("1 sample but overlapping yield two matches", async () => {
                    await create(
                        new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 1, offset: (0.367 + rotation) % 1, timestamp: 0n }),
                        new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 1, offset: (0.847 + rotation) % 1, timestamp: 0n }))
                    expect(await getSamples(scaleToU32(0.78), peers, 1, 0)).to.have.length(2)
                })

                it("closest to", async () => {
                    await create(
                        new ReplicationRangeIndexable({ normalized: false, publicKey: a, length: 1, offset: scaleToU32((0.367 + rotation) % 1), timestamp: 0n }),
                        new ReplicationRangeIndexable({ normalized: false, publicKey: b, length: 1, offset: scaleToU32((0.847 + rotation) % 1), timestamp: 0n }))
                    expect(await getSamples(scaleToU32((0.78 + rotation) % 1), peers, 1, 0)).to.deep.eq([b.hashcode()])
                })

                it("closest to oldest", async () => {

                    // two exactly the same, but one is older
                    await create(
                        new ReplicationRangeIndexable({ normalized: false, publicKey: a, length: 1, offset: scaleToU32((0.367 + rotation) % 1), timestamp: 1n }),
                        new ReplicationRangeIndexable({ normalized: false, publicKey: b, length: 1, offset: scaleToU32((0.367 + rotation) % 1), timestamp: 0n }))

                    expect(await getSamples(scaleToU32((0.78 + rotation) % 1), peers, 1, 0)).to.deep.eq([b.hashcode()])
                })

                it("closest to hash", async () => {

                    // two exactly the same, but one is older
                    await create(
                        new ReplicationRangeIndexable({ normalized: false, publicKey: a, length: 1, offset: scaleToU32((0.367 + rotation) % 1), timestamp: 0n }),
                        new ReplicationRangeIndexable({ normalized: false, publicKey: b, length: 1, offset: scaleToU32((0.367 + rotation) % 1), timestamp: 0n }))

                    expect(a.hashcode() < b.hashcode()).to.be.true
                    expect(await getSamples(scaleToU32((0.78 + rotation) % 1), peers, 1, 0)).to.deep.eq([a.hashcode()])
                })

                it("interescting", async () => {

                    // two exactly the same, but one is older
                    await create(
                        new ReplicationRangeIndexable({ normalized: false, publicKey: a, length: HALF_MAX_U32, offset: scaleToU32((0 + rotation) % 1), timestamp: 0n }),
                        new ReplicationRangeIndexable({ normalized: false, publicKey: b, length: 1, offset: scaleToU32((0.5 + rotation) % 1), timestamp: 0n }))

                    const samples1 = await getSamplesMap(getEvenlySpacedU32(scaleToU32((0.25 + rotation) % 1), 1), peers, 0)
                    expect([...samples1.values()].filter(x => x.intersecting).length).to.eq(1)
                    expect(samples1.size).to.eq(1)

                    const samples2 = await getSamplesMap(getEvenlySpacedU32(scaleToU32((0.75 + rotation) % 1), 2), peers, 0)
                    expect([...samples2.values()].filter(x => x.intersecting).length).to.eq(1)
                    expect(samples2.size).to.eq(2)

                })


                // TODO add breakeven test to make sure it is sorted by hash

            })

        })



        it("factor 0 ", async () => {
            await create(
                new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0, offset: (0.367) % 1, timestamp: 0n }),
                new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 1, offset: (0.567) % 1, timestamp: 0n }),
                new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 1, offset: (0.847) % 1, timestamp: 0n })
            );
            expect(await getSamples(scaleToU32(0.3701), peers, 2, 0)).to.have.members([b, c].map(x => x.hashcode()))
        })


        it("factor 0 with 3 peers factor 1", async () => {
            await create(
                new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 1, offset: 0.145, timestamp: 0n }),
                new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0, offset: 0.367, timestamp: 0n }),
                new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 1, offset: 0.8473, timestamp: 0n })
            );
            expect(await getSamples(scaleToU32(0.937), peers, 2, 0)).to.have.members([a, c].map(x => x.hashcode()))
        })

        it("factor 0 with 3 peers short", async () => {
            await create(
                new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.2, offset: 0.145, timestamp: 0n }),
                new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0, offset: 0.367, timestamp: 0n }),
                new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.2, offset: 0.8473, timestamp: 0n })
            );
            expect(await getSamples(scaleToU32(0.937), peers, 2, 0)).to.have.members([a, c].map(x => x.hashcode()))
        })

        rotations.forEach((rotation) => {

            it("evenly distributed: " + rotation, async () => {
                await create(
                    new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.2, offset: (0.2333 + rotation) % 1, timestamp: 0n }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.2, offset: (0.56666 + rotation) % 1, timestamp: 0n }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.2, offset: (0.9 + rotation) % 1, timestamp: 0n })
                );


                let ac = 0, bc = 0, cc = 0;
                let count = 1000;
                for (let i = 0; i < count; i++) {
                    const leaders = await getSamplesMap([scaleToU32(i / count)], peers, 0)
                    if (leaders.has(a.hashcode())) { ac++; }
                    if (leaders.has(b.hashcode())) { bc++; }
                    if (leaders.has(c.hashcode())) { cc++; }
                }

                // check ac, bc and cc are all close to 1/3
                expect(ac / count).to.be.closeTo(1 / 3, 0.1)
                expect(bc / count).to.be.closeTo(1 / 3, 0.1)
                expect(cc / count).to.be.closeTo(1 / 3, 0.1)
            })
        })

        describe('maturity', () => {
            it("starting at unmatured", async () => {
                await create(
                    new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.333, offset: (0.333) % 1, timestamp: 0n }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.333, offset: (0.666) % 1, timestamp: BigInt(+new Date) }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.3333, offset: (0.999) % 1, timestamp: 0n }),
                );
                expect(await getSamples(scaleToU32(0.7), peers, 2, 1e5)).to.have.members([a, b, c].map(x => x.hashcode()))
            })

            it("starting at matured", async () => {
                await create(
                    new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.333, offset: (0.333) % 1, timestamp: 0n }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.333, offset: (0.666) % 1, timestamp: BigInt(+new Date) }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.3333, offset: (0.999) % 1, timestamp: 0n })
                );
                // the offset jump will be 0.5 (a) and 0.5 + 0.5 = 1 which will intersect (c)
                expect(await getSamples(scaleToU32(0.5), peers, 2, 1e5)).to.have.members([a, c].map(x => x.hashcode()))
            })

            it("starting at matured-2", async () => {
                await create(
                    new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.333, offset: (0.333) % 1, timestamp: 0n }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.333, offset: (0.666) % 1, timestamp: BigInt(+new Date) }),
                    new ReplicationRangeIndexable({ normalized: true, publicKey: c, length: 0.3333, offset: (0.999) % 1, timestamp: 0n })
                );
                // the offset jump will be 0.2 (a) and 0.2 + 0.5 = 0.7 which will intersect (b) (unmatured)
                expect(await getSamples(0, peers, 2, 1e5)).to.have.members([a, c].map(x => x.hashcode()))
            })
        })


        describe('strict', async () => {

            rotations.forEach((rotation) => {

                it("only includes strict segments when intersecting: " + rotation, async () => {

                    const offsetNonStrict = (0 + rotation) % 1
                    await create(
                        new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.2, offset: offsetNonStrict, timestamp: 0n }),
                        new ReplicationRangeIndexable({ normalized: true, publicKey: b, length: 0.2, offset: (0.3 + rotation) % 1, timestamp: 0n, mode: ReplicationIntent.Strict }),
                    );

                    const leaders = await getSamples(scaleToU32(offsetNonStrict + 0.001), peers, 2, 0)
                    expect(leaders).to.have.members([a].map(x => x.hashcode()))
                })
            })


        })
    })

    describe("getDistance", () => {

        describe('above', () => {
            it("immediate", () => {
                expect(getDistance(0.5, 0.4, 'above', 1)).to.be.closeTo(0.1, 0.0001)
            })

            it('wrap', () => {
                expect(getDistance(0.1, 0.9, 'above', 1)).to.be.closeTo(0.2, 0.0001)
            })
        })

        describe('below', () => {

            it("immediate", () => {
                expect(getDistance(0.5, 0.6, 'below', 1)).to.be.closeTo(0.1, 0.0001)
            })

            it('wrap', () => {
                expect(getDistance(0.9, 0.1, 'below', 1)).to.be.closeTo(0.2, 0.0001)
            })

        })

        describe('closest', () => {
            it('immediate', () => {
                expect(getDistance(0.5, 0.6, 'closest', 1)).to.be.closeTo(0.1, 0.0001)
            })

            it('wrap', () => {
                expect(getDistance(0.9, 0.1, 'closest', 1)).to.be.closeTo(0.2, 0.0001)
            })

            it('wrap 2', () => {
                expect(getDistance(0.1, 0.9, 'closest', 1)).to.be.closeTo(0.2, 0.0001)
            })
        })
    })

    describe("hasOneOverlapping", () => {
        const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
        rotations.forEach((rotation) => {
            describe('rotation: ' + String(rotation), () => {

                it('includes all', async () => {
                    const cmp = new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.5, offset: (0 + rotation) % 1, timestamp: 0n })
                    await create(cmp);

                    const inside = new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.4, offset: (0.05 + rotation) % 1, timestamp: 0n });
                    expect(await hasCoveringRange(peers, inside)).to.be.true

                    const outside1 = new ReplicationRangeIndexable({ normalized: true, publicKey: a, length: 0.4, offset: (0.2 + rotation) % 1, timestamp: 0n });
                    expect(await hasCoveringRange(peers, outside1)).to.be.false

                    const outside2 = new ReplicationRangeIndexable({
                        normalized: true, publicKey: a, length: 0.51, offset: (0.1 + rotation) % 1, timestamp: 0n
                    });
                    expect(await hasCoveringRange(peers, outside2)).to.be.false

                })
            })
        })

    })


    /*  describe("removeRange", () => {
 
 
         it('remove outside', () => {
             const from = new ReplicationRangeIndexable({ normalized: false, publicKey: a, offset: 1, length: 1, timestamp: 0n })
             const toRemove = new ReplicationRangeIndexable({ normalized: false, publicKey: a, offset: 0, length: 1, timestamp: 0n })
             const result = from.removeRange(toRemove)
             expect(result).to.equal(from)
 
         })
 
         it('remove all', () => {
             const from = new ReplicationRangeIndexable({ normalized: false, publicKey: a, offset: 1, length: 1, timestamp: 0n })
             const toRemove = new ReplicationRangeIndexable({ normalized: false, publicKey: a, offset: 1, length: 1, timestamp: 0n })
             const result = from.removeRange(toRemove)
             expect(result).to.have.length(0)
         })
 
         const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
         rotations.forEach((rotation) => {
             describe('rotation: ' + String(rotation), () => {
 
                 it('removes end', () => {
                     const from = new ReplicationRangeIndexable({ normalized: true, publicKey: a, offset: rotation, length: 0.3, timestamp: 0n })
                     const toRemove = new ReplicationRangeIndexable({ normalized: true, publicKey: a, offset: rotation + 0.2, length: 0.2, timestamp: 0n })
                     const result = from.removeRange(toRemove)
                     expect(result).to.have.length(2)
                     const arr = result as ReplicationRangeIndexable[]
                     expect(arr[0].start1).to.equal(from.start1)
                     expect(arr[0].end1).to.equal(toRemove.start1)
                     expect(arr[1].start2).to.equal(toRemove.start2)
                     expect(arr[1].end2).to.equal(toRemove.end2)
                 })
             })
         })
 
     }) */
})
describe("entry replicated", () => {
	let index: Index<EntryReplicated>;

	let create = async (...rects: EntryReplicated[]) => {
		const indices = await createIndices();
		await indices.start();
		index = await indices.init({ schema: EntryReplicated });
		for (const rect of rects) {
			await index.put(rect);
		}
	};
	let a: Ed25519PublicKey;

	beforeEach(async () => {
		a = (await Ed25519Keypair.create()).publicKey;
		index = undefined!;
	});

	describe("toRebalance", () => {
		const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1];

		const consumeAllFromAsyncIterator = async (
			iter: AsyncIterable<{ gid: string; entries: EntryReplicated[] }>,
		) => {
			const result = [];
			for await (const entry of iter) {
				result.push(entry);
			}
			return result;
		};

		rotations.forEach((rotation) => {
			const rotate = (from: number) => (from + rotation) % 1;
			describe("rotation: " + String(rotation), () => {
				it("empty change set", async () => {
					await create(
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0)),
							assignedToRangeBoundary: false,
							hash: "a",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "a",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0.3)),
							assignedToRangeBoundary: false,
							hash: "b",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "b",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
					);

					const result = await consumeAllFromAsyncIterator(
						toRebalance([], index),
					);
					expect(result).to.have.length(0);
				});

				describe("update", () => {
					it("matches prev", async () => {
						await create(
							new EntryReplicated({
								coordinate: scaleToU32(rotate(0)),
								assignedToRangeBoundary: false,
								hash: "a",
								meta: new Meta({
									clock: new LamportClock({ id: randomBytes(32) }),
									gid: "a",
									next: [],
									type: 0,
									data: undefined,
								}),
							}),
							new EntryReplicated({
								coordinate: scaleToU32(rotate(0.3)),
								assignedToRangeBoundary: false,
								hash: "b",
								meta: new Meta({
									clock: new LamportClock({ id: randomBytes(32) }),
									gid: "b",
									next: [],
									type: 0,
									data: undefined,
								}),
							}),
						);

						const prev = new ReplicationRangeIndexable({
							normalized: true,
							publicKey: a,
							offset: rotate(0.2),
							length: 0.2,
						});
						const updated = new ReplicationRangeIndexable({
							id: prev.id,
							normalized: true,
							publicKey: a,
							offset: rotate(0.5),
							length: 0.2,
						});

						const result = await consumeAllFromAsyncIterator(
							toRebalance(
								[
									{
										prev,
										range: updated,
										type: "updated",
									},
								],
								index,
							),
						);
						expect(result.map((x) => x.gid)).to.deep.equal(["b"]);
					});

					it("matches next", async () => {
						await create(
							new EntryReplicated({
								coordinate: scaleToU32(rotate(0)),
								assignedToRangeBoundary: false,
								hash: "a",
								meta: new Meta({
									clock: new LamportClock({ id: randomBytes(32) }),
									gid: "a",
									next: [],
									type: 0,
									data: undefined,
								}),
							}),
							new EntryReplicated({
								coordinate: scaleToU32(rotate(0.3)),
								assignedToRangeBoundary: false,
								hash: "b",
								meta: new Meta({
									clock: new LamportClock({ id: randomBytes(32) }),
									gid: "b",
									next: [],
									type: 0,
									data: undefined,
								}),
							}),
						);

						const prev = new ReplicationRangeIndexable({
							normalized: true,
							publicKey: a,
							offset: rotate(0.5),
							length: 0.2,
						});
						const updated = new ReplicationRangeIndexable({
							id: prev.id,
							normalized: true,
							publicKey: a,
							offset: rotate(0.2),
							length: 0.2,
						});

						const result = await consumeAllFromAsyncIterator(
							toRebalance(
								[
									{
										prev,
										range: updated,
										type: "updated",
									},
								],
								index,
							),
						);
						expect(result.map((x) => x.gid)).to.deep.equal(["b"]);
					});
				});

				it("not enoughly replicated after change", async () => {
					await create(
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0)),
							assignedToRangeBoundary: false,
							hash: "a",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "a",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0.3)),
							assignedToRangeBoundary: false,
							hash: "b",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "b",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
					);

					const prev = new ReplicationRangeIndexable({
						normalized: true,
						publicKey: a,
						offset: rotate(0.2),
						length: 0.2,
					});
					const updated = new ReplicationRangeIndexable({
						id: prev.id,
						normalized: true,
						publicKey: a,
						offset: rotate(0.4),
						length: 0.2,
					});

					const result = await consumeAllFromAsyncIterator(
						toRebalance(
							[
								{
									prev,
									range: updated,
									type: "updated",
								},
							],
							index,
						),
					);
					expect(result.map((x) => x.gid)).to.deep.eq(["b"]);
				});

				it("not enoughly replicated after removed", async () => {
					await create(
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0)),
							assignedToRangeBoundary: false,
							hash: "a",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "a",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0.3)),
							assignedToRangeBoundary: false,
							hash: "b",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "b",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
					);

					const updated = new ReplicationRangeIndexable({
						normalized: true,
						publicKey: a,
						offset: rotate(0.2),
						length: 0.2,
					});

					const result = await consumeAllFromAsyncIterator(
						toRebalance(
							[
								{
									range: updated,
									type: "removed",
								},
							],
							index,
						),
					);
					expect(result.map((x) => x.gid)).to.deep.eq(["b"]);
				});

				it("boundary assigned are always included", async () => {
					await create(
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0)),
							assignedToRangeBoundary: false,
							hash: "a",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "a",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
						new EntryReplicated({
							coordinate: scaleToU32(rotate(0)),
							assignedToRangeBoundary: true,
							hash: "b",
							meta: new Meta({
								clock: new LamportClock({ id: randomBytes(32) }),
								gid: "b",
								next: [],
								type: 0,
								data: undefined,
							}),
						}),
					);
					const result = await consumeAllFromAsyncIterator(
						toRebalance([], index),
					);
					expect(result.map((x) => x.gid)).to.deep.eq(["b"]);
				});
			});
		});
	});
});
