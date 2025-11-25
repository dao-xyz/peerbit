import { Cache } from "@peerbit/cache";
import {
	Ed25519Keypair,
	type Ed25519PublicKey,
	type PublicSignKey,
	randomBytes,
} from "@peerbit/crypto";
import type { Index } from "@peerbit/indexer-interface";
import { create as createIndices } from "@peerbit/indexer-sqlite3";
import { LamportClock, Meta } from "@peerbit/log";
import { expect } from "chai";
import {
	type NumberFromType,
	createNumbers,
	denormalizer,
} from "../src/integers.js";
import {
	type EntryReplicated,
	EntryReplicatedU32,
	EntryReplicatedU64,
	ReplicationIntent,
	type ReplicationRangeIndexable,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
	appromixateCoverage,
	calculateCoverage,
	countCoveringRangesSameOwner,
	getAdjecentSameOwner,
	getCoverSet as getCoverSetGeneric,
	getDistance,
	getSamples as getSamplesMap,
	mergeRanges,
	toRebalance,
} from "../src/ranges.js";

// prettier-ignore
type R = 'u32' | 'u64'
const resolutions: [R, R] = ["u32", "u64"];

resolutions.forEach((resolution) => {
	describe("ranges: " + resolution, () => {
		const rangeClass =
			resolution === "u32"
				? ReplicationRangeIndexableU32
				: ReplicationRangeIndexableU64;
		const coerceNumber = (number: number | bigint): NumberFromType<R> =>
			resolution === "u32" ? number : BigInt(number);
		const numbers = createNumbers(resolution);
		const denormalizeFn = denormalizer(resolution);
		const getCoverSet = async <R extends "u32" | "u64">(properties: {
			peers: Index<ReplicationRangeIndexable<R>>;
			start: NumberFromType<R> | PublicSignKey | undefined;
			widthToCoverScaled: NumberFromType<R>;
			roleAge: number;
			eager?:
				| {
						unmaturedFetchCoverSize?: number;
				  }
				| boolean;
		}): Promise<Set<string>> => {
			return getCoverSetGeneric<R>({ ...properties, numbers });
		};
		const getSamples = async (
			offset: NumberFromType<R>,
			peers: Index<ReplicationRangeIndexable<R>>,
			count: number,
			roleAge: number,
		) => {
			const map = await getSamplesMap(
				numbers.getGrid(offset, count),
				peers,
				roleAge,
				numbers,
			);
			return [...map.keys()];
		};

		const createReplicationRangeFromNormalized = (properties: {
			id?: Uint8Array;
			publicKey: PublicSignKey;
			width: number;
			offset: number;
			timestamp?: bigint;
			mode?: ReplicationIntent;
		}) => {
			return new rangeClass({
				id: properties.id,
				publicKey: properties.publicKey,
				mode: properties.mode,
				// @ts-ignore
				width: denormalizeFn(properties.width),
				// @ts-ignore
				offset: denormalizeFn(properties.offset),
				timestamp: properties.timestamp,
			});
		};

		const createReplicationRange = (properties: {
			id?: Uint8Array;
			publicKey: PublicSignKey;
			width: number | bigint;
			offset: number | bigint;
			timestamp?: bigint;
			mode?: ReplicationIntent;
		}) => {
			// @ts-ignore
			return new rangeClass({
				id: properties.id,
				publicKey: properties.publicKey,
				mode: properties.mode,
				// @ts-ignore
				width: coerceNumber(properties.width),
				// @ts-ignore
				offset: coerceNumber(properties.offset),
				timestamp: properties.timestamp,
			});
		};

		describe("ReplicationRangeIndexable", () => {
			let peers: Index<ReplicationRangeIndexable<R>>;
			let a: Ed25519PublicKey, b: Ed25519PublicKey, c: Ed25519PublicKey;

			let create = async (...rects: ReplicationRangeIndexable<R>[]) => {
				const indices = await createIndices();
				await indices.start();
				const index = await indices.init({ schema: rangeClass as any });
				for (const rect of rects) {
					await index.put(rect);
				}
				peers = index as Index<ReplicationRangeIndexable<R>>;
			};

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
			});
			beforeEach(() => {
				peers = undefined!;
			});

			describe("getCover", () => {
				const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1];
				rotations.forEach((rotation) => {
					describe("rotation: " + String(rotation), () => {
						describe("underflow", () => {
							it("includes all", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.1,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.1,
										offset: (0.333 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.1,
										offset: (0.666 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
							});
						});

						describe("overflow", () => {
							it("local first", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 1,
										offset: (0.333 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 1,
										offset: (0.666 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
									})),
								]).to.have.members([a.hashcode()]);
							});
						});

						describe("unmature", () => {
							it("partially overlapping all unmature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.34,
										offset: (0 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.34,
										offset: (0.333 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.34,
										offset: (0.666 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
								);

								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
							});

							it("full width all unmature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 1,
										offset: (0.333 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 1,
										offset: (0.666 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
								);

								// special case, assume we only look into selef
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
									})),
								]).to.have.members([a.hashcode()]);
							});

							it("full one unmature one mature same offset", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// special case, assume we only look into selef
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
									})),
								]).to.have.members([a.hashcode(), b.hashcode()]);
							});

							it("full one unmature one mature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 1,
										offset: (0.333 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// special case, assume we only look into selef
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
									})),
								]).to.have.members([a.hashcode(), b.hashcode()]);
							});

							it("two unmature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.34,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.34,
										offset: (0.333 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.34,
										offset: (0.666 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
								);

								// should not be included. TODO is this always expected behaviour?
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
									})),
								]).to.have.members([a.hashcode()]);
							});
						});

						describe("eager", () => {
							it("all unmature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.34,
										offset: (0 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.34,
										offset: (0.333 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.34,
										offset: (0.666 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
										eager: true,
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
							});
							it("full width all mature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 1,
										offset: (0.333 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 1,
										offset: (0.666 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
										eager: true,
									})),
								]).to.have.members([a.hashcode()]);
							});

							it("full width all unmature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 1,
										offset: (0.333 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 1,
										offset: (0.666 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
								);

								// special case, assume we only look into selef
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
										eager: true,
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
							});

							it("two unmature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.34,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.34,
										offset: (0.333 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.34,
										offset: (0.666 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
								);

								// should not be included. TODO is this always expected behaviour?
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.maxValue,
										eager: true,
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: b,
										widthToCoverScaled: numbers.maxValue,
										eager: true,
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: c,
										widthToCoverScaled: numbers.maxValue,
										eager: true,
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
							});
						});

						describe("skip", () => {
							it("next", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.34,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.41,
										offset: (0.1 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.5,
										offset: (0.3 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.divRound(numbers.maxValue, 2),
									})),
								]).to.have.members([a.hashcode(), b.hashcode()]);
							});
							it("between", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.34,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.5,
										offset: (0.2 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.34,
										offset: (0.3 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: numbers.divRound(numbers.maxValue, 2),
									})),
								]).to.have.members([a.hashcode(), b.hashcode()]);
							});
						});

						describe("boundary", () => {
							it("exact", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.5,
										offset: (0.2 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.5,
										offset: (0.5 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// because of rounding errors, a cover width of 0.5 might yield unecessary results
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 0,
										start: a,
										widthToCoverScaled: numbers.divRound(numbers.maxValue, 2),
									})),
								]).to.have.members([a.hashcode()]);
							});

							it("after", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.1,
										offset: (0.21 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.5,
										offset: (0.5 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.1,
										offset: (0.81 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								expect([
									...(await getCoverSet({
										peers,
										roleAge: 0,
										start: b,
										widthToCoverScaled: denormalizeFn(0.6),
									})),
								]).to.have.members([b.hashcode()]);
							});

							it("skip unmature", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.1,
										offset: (0.2 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.5,
										offset: (0.5 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.1,
										offset: (0.81 + rotation) % 1,
										timestamp: 0n,
									}),
								);
								// starting from b, we need both a and c since b is not mature to cover the width
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: a,
										widthToCoverScaled: denormalizeFn(0.5),
									})),
								]).to.have.members([a.hashcode(), c.hashcode()]);
							});

							it("include start node identity", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.1,
										offset: (0.2 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.5,
										offset: (0.5 + rotation) % 1,
										timestamp: BigInt(+new Date()),
									}),
									createReplicationRangeFromNormalized({
										publicKey: c,
										width: 0.1,
										offset: (0.81 + rotation) % 1,
										timestamp: 0n,
									}),
								);
								// starting from b, we need both a and c since b is not mature to cover the width
								expect([
									...(await getCoverSet({
										peers,
										roleAge: 1e5,
										start: b,
										widthToCoverScaled: denormalizeFn(0.5),
									})),
								]).to.have.members([a.hashcode(), b.hashcode(), c.hashcode()]);
							});

							describe("strict", () => {
								it("no boundary", async () => {
									await create(
										createReplicationRangeFromNormalized({
											publicKey: a,
											width: 0.1,
											offset: (0.2 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
										createReplicationRangeFromNormalized({
											publicKey: b,
											width: 0.5,
											offset: (0.5 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
										createReplicationRangeFromNormalized({
											publicKey: c,
											width: 0.1,
											offset: (0.81 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
									);
									// starting from b, we need both a and c since b is not mature to cover the width
									expect([
										...(await getCoverSet({
											peers,
											roleAge: 1e5,
											start: b,
											widthToCoverScaled: denormalizeFn(0.51),
										})),
									]).to.have.members([b.hashcode()]);
								});

								it("empty set boundary", async () => {
									await create(
										createReplicationRangeFromNormalized({
											publicKey: a,
											width: 0.1,
											offset: (0.2 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
										createReplicationRangeFromNormalized({
											publicKey: c,
											width: 0.1,
											offset: (0.81 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
									);
									expect([
										...(await getCoverSet({
											peers,
											roleAge: 1e5,
											start: denormalizeFn((0.5 + rotation) % 1),
											widthToCoverScaled: denormalizeFn(0.3),
										})),
									]).to.have.members([]);
								});

								it("overlapping", async () => {
									await create(
										createReplicationRangeFromNormalized({
											publicKey: a,
											width: 0.1,
											offset: (0.2 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
									);

									expect([
										...(await getCoverSet({
											peers,
											roleAge: 1e5,
											start: denormalizeFn((0 + rotation) % 1),
											widthToCoverScaled: denormalizeFn(0.6),
										})),
									]).to.have.members([a.hashcode()]);
								});

								it("inside one", async () => {
									await create(
										createReplicationRangeFromNormalized({
											publicKey: a,
											width: 0.1,
											offset: (0 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
										createReplicationRangeFromNormalized({
											publicKey: b,
											width: 0.1,
											offset: (0.2 + rotation) % 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
									);

									expect([
										...(await getCoverSet({
											peers,
											roleAge: 1e5,
											start: denormalizeFn((0.21 + rotation) % 1),
											widthToCoverScaled: denormalizeFn(0.01),
										})),
									]).to.have.members([b.hashcode()]);
								});

								it("starting at", async () => {
									await create(
										createReplicationRange({
											publicKey: a,
											width: 1,
											offset: 1,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
										createReplicationRange({
											publicKey: b,
											width: 2,
											offset: 2,
											timestamp: 0n,
											mode: ReplicationIntent.Strict,
										}),
									);

									expect([
										...(await getCoverSet({
											peers,
											roleAge: 1e5,
											start: coerceNumber(2),
											widthToCoverScaled: coerceNumber(1),
										})),
									]).to.have.members([b.hashcode()]);
								});
							});
						});
					});
				});
			});

			describe("getAdjecentSameOwner", () => {
				const rotations = [0, 0.333, 0.5, 0.8];
				rotations.forEach((rotation) => {
					describe("rotation: " + rotation, () => {
						it("no adjecent", async () => {
							const range = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.2625,
								offset: (0 + rotation) % 1,
								timestamp: 0n,
							});
							await create(
								range,
								createReplicationRangeFromNormalized({
									publicKey: b,
									width: 1,
									offset: (0.5 + rotation) % 1,
									timestamp: 0n,
								}),
							);

							const adjecent = await getAdjecentSameOwner(
								peers,
								range,
								numbers,
							);
							expect(adjecent.below).to.be.undefined;
							expect(adjecent.above).to.be.undefined;
						});

						it("one adjecent", async () => {
							const from = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.001,
								offset: (0.4 + rotation) % 1,
								timestamp: 0n,
							});
							const below = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.2625,
								offset: (0 + rotation) % 1,
								timestamp: 0n,
							});
							await create(
								below,
								createReplicationRangeFromNormalized({
									publicKey: b,
									width: 1,
									offset: (0.5 + rotation) % 1,
									timestamp: 0n,
								}),
							);

							const adjecent = await getAdjecentSameOwner(peers, from, numbers);
							expect(adjecent.below?.idString).to.eq(below.idString);
							expect(adjecent.above).to.be.undefined;
						});

						it("two adjecent", async () => {
							const from = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.001,
								offset: (0.4 + rotation) % 1,
								timestamp: 0n,
							});
							const below = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.2625,
								offset: (0 + rotation) % 1,
								timestamp: 0n,
							});
							const above = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.2625,
								offset: (0.5 + rotation) % 1,
								timestamp: 0n,
							});

							await create(
								below,
								above,
								createReplicationRangeFromNormalized({
									publicKey: b,
									width: 1,
									offset: (0.5 + rotation) % 1,
									timestamp: 0n,
								}),
							);

							const adjecent = await getAdjecentSameOwner(peers, from, numbers);
							expect(adjecent.below?.idString).to.eq(below.idString);
							expect(adjecent.above?.idString).to.eq(above.idString);
						});
					});
				});
			});

			describe("getSamples", () => {
				const rotations = [0, 0.333, 0.5, 0.8];
				rotations.forEach((rotation) => {
					describe("samples correctly: " + rotation, () => {
						it("1 and less than 1", async () => {
							await create(
								createReplicationRangeFromNormalized({
									publicKey: a,
									width: 0.2625,
									offset: (0.367 + rotation) % 1,
									timestamp: 0n,
								}),
								createReplicationRangeFromNormalized({
									publicKey: b,
									width: 1,
									offset: (0.847 + rotation) % 1,
									timestamp: 0n,
								}),
							);
							expect(
								// 0.78 is choosen to not interesect with a
								// also (0.75 + 0.5) % 1 = 0.25 which also does not intersect with a
								// this means a need to be included though the non interesecting sampling method
								await getSamples(denormalizeFn(0.78), peers, 2, 0),
							).to.have.length(2);
						});

						it("1 sample but overlapping yield two matches", async () => {
							await create(
								createReplicationRangeFromNormalized({
									publicKey: a,
									width: 1,
									offset: (0.367 + rotation) % 1,
									timestamp: 0n,
								}),
								createReplicationRangeFromNormalized({
									publicKey: b,
									width: 1,
									offset: (0.847 + rotation) % 1,
									timestamp: 0n,
								}),
							);
							expect(
								await getSamples(denormalizeFn(0.78), peers, 1, 0),
							).to.have.length(2);
						});

						it("3 adjecent ranges", async () => {
							await create(
								createReplicationRangeFromNormalized({
									publicKey: a,
									width: 0.3333,
									offset: (0 + rotation) % 1,
									timestamp: 0n,
								}),
								createReplicationRangeFromNormalized({
									publicKey: b,
									width: 0.3333,
									offset: (0.3333 + rotation) % 1,
									timestamp: 0n,
								}),
								createReplicationRangeFromNormalized({
									publicKey: c,
									width: 0.3333,
									offset: (0.6666 + rotation) % 1,
									timestamp: 0n,
								}),
							);
							expect(
								await getSamples(denormalizeFn(0.1), peers, 2, 0),
							).to.have.length(2);
						});

						it("closest to", async () => {
							await create(
								createReplicationRange({
									publicKey: a,
									width: 1,
									offset: denormalizeFn((0.367 + rotation) % 1),
									timestamp: 0n,
								}),
								createReplicationRange({
									publicKey: b,
									width: 1,
									offset: denormalizeFn((0.847 + rotation) % 1),
									timestamp: 0n,
								}),
							);
							expect(
								await getSamples(
									denormalizeFn((0.78 + rotation) % 1),
									peers,
									1,
									0,
								),
							).to.deep.eq([b.hashcode()]);
						});

						it("closest to oldest", async () => {
							// two exactly the same, but one is older
							await create(
								createReplicationRange({
									publicKey: a,
									width: 1,
									offset: denormalizeFn((0.367 + rotation) % 1),
									timestamp: 1n,
								}),
								createReplicationRange({
									publicKey: b,
									width: 1,
									offset: denormalizeFn((0.367 + rotation) % 1),
									timestamp: 0n,
								}),
							);

							expect(
								await getSamples(
									denormalizeFn((0.78 + rotation) % 1),
									peers,
									1,
									0,
								),
							).to.deep.eq([b.hashcode()]);
						});

						it("closest to hash", async () => {
							// two exactly the same, but one is older
							await create(
								createReplicationRange({
									publicKey: a,
									width: 1,
									offset: denormalizeFn((0.367 + rotation) % 1),
									timestamp: 0n,
								}),
								createReplicationRange({
									publicKey: b,
									width: 1,
									offset: denormalizeFn((0.367 + rotation) % 1),
									timestamp: 0n,
								}),
							);

							expect(a.hashcode() < b.hashcode()).to.be.true;
							expect(
								await getSamples(
									denormalizeFn((0.78 + rotation) % 1),
									peers,
									1,
									0,
								),
							).to.deep.eq([a.hashcode()]);
						});

						it("interescting one", async () => {
							await create(
								createReplicationRange({
									publicKey: a,
									width: numbers.divRound(numbers.maxValue, 2),
									offset: denormalizeFn((0 + rotation) % 1),
									timestamp: 0n,
								}),
								createReplicationRange({
									publicKey: b,
									width: 1,
									offset: denormalizeFn((0.5 + rotation) % 1),
									timestamp: 0n,
								}),
							);

							const samples1 = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.25 + rotation) % 1), 1),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples1.values()].filter((x) => x.intersecting).length,
							).to.eq(1);
							expect(samples1.size).to.eq(1);

							const samples2 = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.75 + rotation) % 1), 2),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples2.values()].filter((x) => x.intersecting).length,
							).to.eq(1);
							expect(samples2.size).to.eq(2);
						});

						it("interescting overlapping", async () => {
							await create(
								createReplicationRange({
									publicKey: a,
									width: numbers.divRound(numbers.maxValue, 2),
									offset: denormalizeFn((0 + rotation) % 1),
									timestamp: 0n,
								}),
								createReplicationRange({
									publicKey: b,
									width: numbers.maxValue,
									offset: denormalizeFn((0.5 + rotation) % 1),
									timestamp: 0n,
								}),
							);

							const samples1 = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.25 + rotation) % 1), 2),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples1.values()].filter((x) => x.intersecting).length,
							).to.eq(2);
							expect(samples1.size).to.eq(2);

							const samples2 = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.25 + rotation) % 1), 2),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples2.values()].filter((x) => x.intersecting).length,
							).to.eq(2);
							expect(samples2.size).to.eq(2);
						});

						it("interescting overlapping reversed", async () => {
							await create(
								// reversed insertion order
								createReplicationRange({
									publicKey: b,
									width: numbers.maxValue,
									offset: denormalizeFn((0.5 + rotation) % 1),
									timestamp: 0n,
								}),
								createReplicationRange({
									publicKey: a,
									width: numbers.divRound(numbers.maxValue, 2),
									offset: denormalizeFn((0 + rotation) % 1),
									timestamp: 0n,
								}),
							);

							const samples1 = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.25 + rotation) % 1), 2),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples1.values()].filter((x) => x.intersecting).length,
							).to.eq(2);
							expect(samples1.size).to.eq(2);

							const samples2 = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.25 + rotation) % 1), 2),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples2.values()].filter((x) => x.intersecting).length,
							).to.eq(2);
							expect(samples2.size).to.eq(2);
						});

						it("intersecting half range", async () => {
							await create(
								// reversed insertion order

								createReplicationRange({
									publicKey: a,
									width: numbers.divRound(numbers.maxValue, 2),
									offset: denormalizeFn((0.5 + rotation) % 1),
									timestamp: 0n,
								}),
								createReplicationRange({
									publicKey: b,
									width: numbers.divRound(numbers.maxValue, 2),
									offset: denormalizeFn((0.5 + rotation) % 1),
									timestamp: 0n,
								}),
							);
							const samples = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.25 + rotation) % 1), 2),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples.values()].filter((x) => x.intersecting).length,
							).to.eq(2);
						});

						it("intersecting half range overlapping", async () => {
							await create(
								// reversed insertion order

								createReplicationRange({
									publicKey: a,
									width: numbers.divRound(numbers.maxValue, 2),
									offset: denormalizeFn((0.4 + rotation) % 1),
									timestamp: 0n,
								}),
								createReplicationRange({
									publicKey: b,
									width: 1,
									offset: denormalizeFn((0.5 + rotation) % 1),
									timestamp: 0n,
								}),
							);
							const samples = await getSamplesMap(
								numbers.getGrid(denormalizeFn((0.3 + rotation) % 1), 2),
								peers,
								0,
								numbers,
							);
							expect(
								[...samples.values()].filter((x) => x.intersecting).length,
							).to.eq(1);
							expect(samples.size).to.eq(2);
						});

						// TODO add breakeven test to make sure it is sorted by hash
					});
				});

				it("factor 0 ", async () => {
					await create(
						createReplicationRangeFromNormalized({
							publicKey: a,
							width: 0,
							offset: 0.367 % 1,
							timestamp: 0n,
						}),
						createReplicationRangeFromNormalized({
							publicKey: b,
							width: 1,
							offset: 0.567 % 1,
							timestamp: 0n,
						}),
						createReplicationRangeFromNormalized({
							publicKey: c,
							width: 1,
							offset: 0.847 % 1,
							timestamp: 0n,
						}),
					);
					expect(
						await getSamples(denormalizeFn(0.3701), peers, 2, 0),
					).to.have.members([b, c].map((x) => x.hashcode()));
				});

				it("factor 0 with 3 peers factor 1", async () => {
					await create(
						createReplicationRangeFromNormalized({
							publicKey: a,
							width: 1,
							offset: 0.145,
							timestamp: 0n,
						}),
						createReplicationRangeFromNormalized({
							publicKey: b,
							width: 0,
							offset: 0.367,
							timestamp: 0n,
						}),
						createReplicationRangeFromNormalized({
							publicKey: c,
							width: 1,
							offset: 0.8473,
							timestamp: 0n,
						}),
					);
					expect(
						await getSamples(denormalizeFn(0.937), peers, 2, 0),
					).to.have.members([a, c].map((x) => x.hashcode()));
				});

				it("factor 0 with 3 peers short", async () => {
					await create(
						createReplicationRangeFromNormalized({
							publicKey: a,
							width: 0.2,
							offset: 0.145,
							timestamp: 0n,
						}),
						createReplicationRangeFromNormalized({
							publicKey: b,
							width: 0,
							offset: 0.367,
							timestamp: 0n,
						}),
						createReplicationRangeFromNormalized({
							publicKey: c,
							width: 0.2,
							offset: 0.8473,
							timestamp: 0n,
						}),
					);
					expect(
						await getSamples(denormalizeFn(0.937), peers, 2, 0),
					).to.have.members([a, c].map((x) => x.hashcode()));
				});

				rotations.forEach((rotation) => {
					it("evenly distributed: " + rotation, async () => {
						await create(
							createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.2,
								offset: (0.2333 + rotation) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								width: 0.2,
								offset: (0.56666 + rotation) % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								width: 0.2,
								offset: (0.9 + rotation) % 1,
								timestamp: 0n,
							}),
						);

						let ac = 0,
							bc = 0,
							cc = 0;
						let count = 1000;
						for (let i = 0; i < count; i++) {
							const leaders = await getSamplesMap(
								[denormalizeFn(i / count)],
								peers,
								0,
								numbers,
							);
							if (leaders.has(a.hashcode())) {
								ac++;
							}
							if (leaders.has(b.hashcode())) {
								bc++;
							}
							if (leaders.has(c.hashcode())) {
								cc++;
							}
						}

						// check ac, bc and cc are all close to 1/3
						expect(ac / count).to.be.closeTo(1 / 3, 0.1);
						expect(bc / count).to.be.closeTo(1 / 3, 0.1);
						expect(cc / count).to.be.closeTo(1 / 3, 0.1);
					});
				});

				describe("maturity", () => {
					it("starting at unmatured", async () => {
						await create(
							createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.333,
								offset: 0.333 % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								width: 0.333,
								offset: 0.666 % 1,
								timestamp: BigInt(+new Date()),
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								width: 0.3333,
								offset: 0.999 % 1,
								timestamp: 0n,
							}),
						);
						expect(
							await getSamples(denormalizeFn(0.7), peers, 2, 1e5),
						).to.have.members([a, b, c].map((x) => x.hashcode()));
					});

					it("starting at matured", async () => {
						await create(
							createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.333,
								offset: 0.333 % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								width: 0.333,
								offset: 0.666 % 1,
								timestamp: BigInt(+new Date()),
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								width: 0.3333,
								offset: 0.999 % 1,
								timestamp: 0n,
							}),
						);
						// the offset jump will be 0.5 (a) and 0.5 + 0.5 = 1 which will intersect (c)
						expect(
							await getSamples(denormalizeFn(0.5), peers, 2, 1e5),
						).to.have.members([a, c].map((x) => x.hashcode()));
					});

					it("starting at matured-2", async () => {
						await create(
							createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.333,
								offset: 0.333 % 1,
								timestamp: 0n,
							}),
							createReplicationRangeFromNormalized({
								publicKey: b,
								width: 0.333,
								offset: 0.666 % 1,
								timestamp: BigInt(+new Date()),
							}),
							createReplicationRangeFromNormalized({
								publicKey: c,
								width: 0.3333,
								offset: 0.999 % 1,
								timestamp: 0n,
							}),
						);
						// the offset jump will be 0.2 (a) and 0.2 + 0.5 = 0.7 which will intersect (b) (unmatured)
						expect(
							await getSamples(numbers.zero, peers, 2, 1e5),
						).to.have.members([a, c].map((x) => x.hashcode()));
					});
				});

				describe("strict", async () => {
					rotations.forEach((rotation) => {
						it(
							"only includes strict segments when intersecting: " + rotation,
							async () => {
								const offsetNonStrict = (0 + rotation) % 1;
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.2,
										offset: offsetNonStrict,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: b,
										width: 0.2,
										offset: (0.3 + rotation) % 1,
										timestamp: 0n,
										mode: ReplicationIntent.Strict,
									}),
								);

								const leaders = await getSamples(
									denormalizeFn(offsetNonStrict + 0.001),
									peers,
									2,
									0,
								);
								expect(leaders).to.have.members([a].map((x) => x.hashcode()));
							},
						);
					});
				});
			});

			describe("getDistance", () => {
				describe("above", () => {
					it("immediate", () => {
						expect(getDistance(0.5, 0.4, "above", 1)).to.be.closeTo(
							0.1,
							0.0001,
						);
					});

					it("wrap", () => {
						expect(getDistance(0.1, 0.9, "above", 1)).to.be.closeTo(
							0.2,
							0.0001,
						);
					});
				});

				describe("below", () => {
					it("immediate", () => {
						expect(getDistance(0.5, 0.6, "below", 1)).to.be.closeTo(
							0.1,
							0.0001,
						);
					});

					it("wrap", () => {
						expect(getDistance(0.9, 0.1, "below", 1)).to.be.closeTo(
							0.2,
							0.0001,
						);
					});
				});

				describe("closest", () => {
					it("immediate", () => {
						expect(getDistance(0.5, 0.6, "closest", 1)).to.be.closeTo(
							0.1,
							0.0001,
						);
					});

					it("wrap", () => {
						expect(getDistance(0.9, 0.1, "closest", 1)).to.be.closeTo(
							0.2,
							0.0001,
						);
					});

					it("wrap 2", () => {
						expect(getDistance(0.1, 0.9, "closest", 1)).to.be.closeTo(
							0.2,
							0.0001,
						);
					});
				});
			});

			describe("countCoveringRangesSameOwner", () => {
				const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1];
				rotations.forEach((rotation) => {
					describe("rotation: " + String(rotation), () => {
						it("includes all", async () => {
							const cmp = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.5,
								offset: (0 + rotation) % 1,
								timestamp: 0n,
							});
							await create(cmp);

							const inside = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.4,
								offset: (0.05 + rotation) % 1,
								timestamp: 0n,
							});
							expect(await countCoveringRangesSameOwner(peers, inside)).to.be
								.true;

							const outside1 = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.4,
								offset: (0.2 + rotation) % 1,
								timestamp: 0n,
							});
							expect(await countCoveringRangesSameOwner(peers, outside1)).to.be
								.false;

							const outside2 = createReplicationRangeFromNormalized({
								publicKey: a,
								width: 0.51,
								offset: (0.1 + rotation) % 1,
								timestamp: 0n,
							});
							expect(await countCoveringRangesSameOwner(peers, outside2)).to.be
								.false;
						});
					});
				});
			});

			describe("merge", () => {
				const rotations = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1];
				rotations.forEach((rotation) => {
					describe("rotation: " + String(rotation), () => {
						describe("2 ranges", () => {
							it("gap", async () => {
								const offset1 = denormalizeFn(0.2 + rotation);
								const offset2 = denormalizeFn(0.3 + rotation);

								//@ts-ignore
								const diff = numbers.abs(offset1 - offset2);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset1 % numbers.maxValue,
									timestamp: 0n,
								});

								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset2 % numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2], numbers);

								expect(merged.width).to.eq(
									diff + ((typeof diff === "number" ? 1 : 1n) as any),
								); // + 1 for the length of the last range
								expect(merged.start1).to.equal(range1.start1);
							});

							it("adjecent", async () => {
								const offset = denormalizeFn(0.2 + rotation);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset % numbers.maxValue,
									timestamp: 0n,
								});

								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 1,
									offset:
										// @ts-ignore
										(offset + (typeof offset === "bigint" ? 1n : 1)) %
										numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2], numbers);
								expect(Number(merged.width)).to.eq(2);
								expect(merged.start1).to.equal(range1.start1);
							});

							it("duplicates", async () => {
								const offset = denormalizeFn(0.2 + rotation);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset % numbers.maxValue,
									timestamp: 0n,
								});
								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset % numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2], numbers);
								expect(Number(merged.width)).to.eq(1);
								//  expect(merged.start1).to.equal(range1.start1)
							});

							it("overlapping", async () => {
								const offset1 = denormalizeFn(rotation);

								//@ts-ignore
								const offset2 =
									//@ts-ignore
									offset1 + (typeof offset1 === "number" ? 1 : 1n);

								//@ts-ignore
								const _diff = numbers.abs(offset1 - offset2);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 1e3,
									// @ts-ignore
									offset: offset1 % numbers.maxValue,
									timestamp: 0n,
								});

								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 10,
									// @ts-ignore
									offset: offset2 % numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2], numbers);

								expect(Number(merged.width)).to.eq(1e3); // + 1 for the length of the last range
								expect(merged.start1).to.equal(range1.start1);
								expect(
									merged.idString === range1.idString ||
										merged.idString === range2.idString,
								).to.be.true;
							});

							it("overlapping with a gap", async () => {
								const offset = denormalizeFn(0.2 + rotation);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 100,
									// @ts-ignore
									offset: offset % numbers.maxValue,
									timestamp: 0n,
								});
								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 100,
									// @ts-ignore
									offset:
										// @ts-ignore
										(offset + (typeof offset === "number" ? 60 : 60n)) %
										numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2], numbers);
								expect(Number(merged.width)).to.eq(160);
							});

							it("three overlapping", async () => {
								const offset = denormalizeFn(0.2 + rotation);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 100,
									// @ts-ignore
									offset: offset % numbers.maxValue,
									timestamp: 0n,
								});

								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 100,
									// @ts-ignore
									offset:
										// @ts-ignore
										(offset + (typeof offset === "number" ? 50 : 50n)) %
										numbers.maxValue,
									timestamp: 0n,
								});

								//@ts-ignore
								const range3 = createReplicationRange({
									publicKey: a,
									width: 100,
									// @ts-ignore
									offset:
										// @ts-ignore
										(offset + (typeof offset === "number" ? 150 : 150n)) %
										numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2, range3], numbers);
								expect(Number(merged.width)).to.eq(250);
							});

							it("different lengths", async () => {
								const offset = denormalizeFn(0.2 + rotation);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 10,
									// @ts-ignore
									offset: offset % numbers.maxValue,
									timestamp: 0n,
								});
								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 100,
									// @ts-ignore
									offset: (offset + coerceNumber(20)) % numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2], numbers);
								expect(Number(merged.width)).to.eq(120);
								expect(merged.start1).to.equal(range1.start1);
							});

							describe("mode", () => {
								it("equal ranges but different mode", async () => {
									const offset = denormalizeFn(0.2 + rotation);

									//@ts-ignore
									const range1 = createReplicationRange({
										publicKey: a,
										width: 1,
										// @ts-ignore
										offset: offset % numbers.maxValue,
										timestamp: 0n,
										mode: ReplicationIntent.NonStrict,
									});
									//@ts-ignore
									const range2 = createReplicationRange({
										publicKey: a,
										width: 1,
										// @ts-ignore
										offset: offset % numbers.maxValue,
										timestamp: 0n,
										mode: ReplicationIntent.Strict,
									});

									const merged = mergeRanges([range1, range2], numbers);
									expect(merged.mode).to.eq(ReplicationIntent.Strict);
								});

								it("different ranges different modes", async () => {
									const offset = denormalizeFn(0.2 + rotation);

									//@ts-ignore
									const range1 = createReplicationRange({
										publicKey: a,
										width: 1,
										// @ts-ignore
										offset,
										timestamp: 0n,
										mode: ReplicationIntent.NonStrict,
									});
									//@ts-ignore
									const range2 = createReplicationRange({
										publicKey: a,
										width: 1,
										// @ts-ignore
										offset,
										timestamp: 0n,
										mode: ReplicationIntent.Strict,
									});

									const merged = mergeRanges([range1, range2], numbers);
									expect(merged.mode).to.eq(ReplicationIntent.Strict);
								});

								it("same mode", async () => {
									const offset = denormalizeFn(0.2 + rotation);

									//@ts-ignore
									const range1 = createReplicationRange({
										publicKey: a,
										width: 1,
										// @ts-ignore
										offset,
										timestamp: 0n,
										mode: ReplicationIntent.NonStrict,
									});
									//@ts-ignore
									const range2 = createReplicationRange({
										publicKey: a,
										width: 1,
										// @ts-ignore
										offset,
										timestamp: 0n,
										mode: ReplicationIntent.NonStrict,
									});

									const merged = mergeRanges([range1, range2], numbers);
									expect(merged.mode).to.eq(ReplicationIntent.NonStrict);
								});
							});
						});

						describe("3 ranges", () => {
							it("gap", async () => {
								const offset1 = denormalizeFn(0.2 + rotation);
								const offset2 = denormalizeFn(0.3 + rotation);
								const offset3 = denormalizeFn(0.4 + rotation);

								// @ts-ignore
								const diff = numbers.abs(offset1 - offset3);

								// @ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset1 % numbers.maxValue,
									timestamp: 0n,
								});
								// @ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset2 % numbers.maxValue,
									timestamp: 0n,
								});
								const range3 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset3 % numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2, range3], numbers);
								// @ts-ignore
								expect(merged.width).to.eq(
									// @ts-ignore
									diff + (typeof diff === "number" ? 1 : 1n),
								); // + 1 for the length of the last range
							});

							it("adjecent", async () => {
								const offset1 = denormalizeFn(0.2 + rotation);
								const offset2 =
									// @ts-ignore
									offset1 + (typeof offset1 === "number" ? 1 : 1n);
								// @ts-ignore
								const offset3 =
									offset2 + (typeof offset2 === "number" ? 1 : 1n);

								// @ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset1 % numbers.maxValue,
									timestamp: 0n,
								});
								// @ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset2 % numbers.maxValue,
									timestamp: 0n,
								});

								// @ts-ignore
								const range3 = createReplicationRange({
									publicKey: a,
									width: 1,
									// @ts-ignore
									offset: offset3 % numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2, range3], numbers);
								expect(Number(merged.width)).to.eq(3);
							});

							it("different lengths", async () => {
								const offset = denormalizeFn(0.2 + rotation);

								//@ts-ignore
								const range1 = createReplicationRange({
									publicKey: a,
									width: 10,
									// @ts-ignore
									offset: offset % numbers.maxValue,
									timestamp: 0n,
								});

								//@ts-ignore
								const range2 = createReplicationRange({
									publicKey: a,
									width: 100,
									// @ts-ignore
									offset: (offset + coerceNumber(20)) % numbers.maxValue,
									timestamp: 0n,
								});

								//@ts-ignore
								const range3 = createReplicationRange({
									publicKey: a,
									width: 5,
									// @ts-ignore
									offset: (offset + coerceNumber(130)) % numbers.maxValue,
									timestamp: 0n,
								});

								const merged = mergeRanges([range1, range2, range3], numbers);
								expect(Number(merged.width)).to.eq(135);
								expect(merged.start1).to.equal(range1.start1);
							});
						});
					});
				});
			});

			describe("approximateCoverage", () => {
				[0, 0.3, 0.6, 0.9].forEach((rotation) => {
					describe("rotation: " + rotation, () => {
						const samples = 20;
						describe("100%", () => {
							it("one range", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await appromixateCoverage({
										peers,
										samples,
										numbers,
										normalized: true,
									}),
								).to.eq(1);
							});

							it("two ranges", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.5,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.5,
										offset: (0.5 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await appromixateCoverage({
										peers,
										samples,
										numbers,
										normalized: true,
									}),
								).to.be.closeTo(1, 1 / (samples - 1));
							});
						});

						describe("50%", () => {
							it("one range", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.5,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await appromixateCoverage({
										peers,
										samples,
										numbers,
										normalized: true,
									}),
								).to.be.closeTo(0.5, 1 / (samples - 1));
							});

							it("two ranges", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.25,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.25,
										offset: (0.5 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await appromixateCoverage({
										peers,
										samples,
										numbers,
										normalized: true,
									}),
								).to.be.closeTo(0.5, 1 / (samples - 1));
							});
						});
					});
				});
			});

			describe("calculateCoverage", () => {
				[0, 0.3, 0.6, 0.9].forEach((rotation) => {
					describe("rotation: " + rotation, () => {
						describe("100%", () => {
							it("one range", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await calculateCoverage({
										peers,
										numbers,
									}),
								).to.eq(1);
							});

							it("two ranges", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.51,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.51,
										offset: (0.4999 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await calculateCoverage({
										peers,
										numbers,
									}),
								).to.eq(1);
							});
						});

						describe("200%", () => {
							it("two ranges", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (0.5 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await calculateCoverage({
										peers,
										numbers,
									}),
								).to.eq(2);
							});

							it("three ranges", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.51,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.51,
										offset: (0.4999 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 1,
										offset: (1 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await calculateCoverage({
										peers,
										numbers,
									}),
								).to.eq(2);
							});
						});

						describe("50%", () => {
							it("one range", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.5,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await calculateCoverage({
										peers,
										numbers,
									}),
								).to.be.eq(0);
							});

							it("two ranges", async () => {
								await create(
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.25,
										offset: (0 + rotation) % 1,
										timestamp: 0n,
									}),
									createReplicationRangeFromNormalized({
										publicKey: a,
										width: 0.25,
										offset: (0.5 + rotation) % 1,
										timestamp: 0n,
									}),
								);

								// we try to cover 0.5 starting from a
								// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
								expect(
									await calculateCoverage({
										peers,
										numbers,
									}),
								).to.be.eq(0);

								expect(
									await calculateCoverage({
										peers,
										numbers,
									}),
								).to.be.eq(0);
							});
						});

						describe("partial range", () => {
							describe("100%", () => {
								it("one range", async () => {
									const offset = denormalizeFn((0 + rotation) % 1);
									await create(
										createReplicationRange({
											publicKey: a,
											width: denormalizeFn(0.5),
											// @ts-ignore
											offset,
											timestamp: 0n,
										}),
									);

									// we try to cover 0.5 starting from a
									// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
									expect(
										await calculateCoverage({
											peers,
											numbers,
											start: coerceNumber(offset),
											// @ts-ignore
											end: (offset + coerceNumber(1)) % numbers.maxValue,
										}),
									).to.be.eq(1);
								});

								it("two ranges", async () => {
									const offset1 = denormalizeFn((0 + rotation) % 1);
									const offset2 = denormalizeFn((0.23 + rotation) % 1);
									const width = denormalizeFn(0.25);
									await create(
										createReplicationRange({
											publicKey: a,
											width,
											offset: offset1,
											timestamp: 0n,
										}),
										createReplicationRange({
											publicKey: a,
											width,
											offset: offset2,
											timestamp: 0n,
										}),
									);

									// we try to cover 0.5 starting from a
									// this should mean that we would want a and b, because c is not mature enough, even though it would cover a wider set
									expect(
										await calculateCoverage({
											peers,
											numbers,
											start: offset1,
											end:
												// @ts-ignore
												(offset1 + width - coerceNumber(1)) % numbers.maxValue,
										}),
									).to.be.eq(1);

									expect(
										await calculateCoverage({
											peers,
											numbers,
											start: offset2,
											end:
												// @ts-ignore
												(offset2 + width + coerceNumber(1)) % numbers.maxValue, // +1 to be outside a replicate range
										}),
									).to.be.eq(0);
								});
							});
						});
					});
				});
			});

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
		});

		describe("entry replicated", () => {
			let index: Index<EntryReplicated<R>>;
			const entryClass =
				resolution === "u32" ? EntryReplicatedU32 : EntryReplicatedU64;

			let create = async (...rects: EntryReplicated<R>[]) => {
				const indices = await createIndices();
				await indices.start();
				index = await indices.init({ schema: entryClass as any });
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
					iter: AsyncIterable<EntryReplicated<R>>,
				): Promise<EntryReplicated<R>[]> => {
					const result = [];
					for await (const entry of iter) {
						result.push(entry);
					}
					return result;
				};

				const createEntryReplicated = (properties: {
					coordinate: NumberFromType<R> | NumberFromType<R>[];
					hash: string;
					meta: Meta;
					assignedToRangeBoundary: boolean;
				}) => {
					return new entryClass({
						coordinates: Array.isArray(properties.coordinate)
							? properties.coordinate
							: [properties.coordinate],
						assignedToRangeBoundary: properties.assignedToRangeBoundary,
						hash: properties.hash,
						meta: properties.meta,
					} as any);
				};

				rotations.forEach((rotation) => {
					const rotate = (from: number) => (from + rotation) % 1;
					describe("rotation: " + String(rotation), () => {
						it("empty change set", async () => {
							await create(
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
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
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0.3)),
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
							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							const result = await consumeAllFromAsyncIterator(
								toRebalance([], index, cache),
							);
							expect(result).to.have.length(0);
						});

						describe("update", () => {
							it("matches prev", async () => {
								await create(
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0)),
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
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.3)),
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

								const prev = createReplicationRangeFromNormalized({
									publicKey: a,
									offset: rotate(0.2),
									width: 0.2,
								});
								const updated = createReplicationRangeFromNormalized({
									id: prev.id,
									publicKey: a,
									offset: rotate(0.5),
									width: 0.2,
								});

								const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

								const result = await consumeAllFromAsyncIterator(
									toRebalance(
										[
											{
												range: prev,
												type: "replaced",
												timestamp: 0n,
											},

											{
												range: updated,
												type: "added",
												timestamp: 1n,
											},
										],
										index,
										cache,
									),
								);
								expect(result.map((x) => x.gid)).to.deep.equal(["b"]);
							});

							it("matches next", async () => {
								await create(
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0)),
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
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.3)),
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

								const prev = createReplicationRangeFromNormalized({
									publicKey: a,
									offset: rotate(0.5),
									width: 0.2,
								});
								const updated = createReplicationRangeFromNormalized({
									id: prev.id,
									publicKey: a,
									offset: rotate(0.2),
									width: 0.2,
								});
								const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

								const result = await consumeAllFromAsyncIterator(
									toRebalance(
										[
											{
												range: prev,
												type: "replaced",
												timestamp: 0n,
											},

											{
												range: updated,
												type: "added",
												timestamp: 1n,
											},
										],
										index,
										cache,
									),
								);
								expect(result.map((x) => x.gid)).to.deep.equal(["b"]);
							});
						});

						describe("replace", () => {
							it("differential between added and removed", async () => {
								await create(
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.05)),
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
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.15)),
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

									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.29)),
										assignedToRangeBoundary: false,
										hash: "c",
										meta: new Meta({
											clock: new LamportClock({ id: randomBytes(32) }),
											gid: "c",
											next: [],
											type: 0,
											data: undefined,
										}),
									}),
								);

								const first = createReplicationRangeFromNormalized({
									publicKey: a,
									offset: rotate(0),
									width: 0.2,
								});

								// second covers first and a little bit more
								const second = createReplicationRangeFromNormalized({
									publicKey: a,
									offset: rotate(0),
									width: 0.3,
								});

								// the differential makes it so that only range:
								// (0,0.1)
								// (0.2, 0.3)
								// needs to be considered
								const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

								let result = await consumeAllFromAsyncIterator(
									toRebalance(
										[
											{
												range: first,
												type: "added",
												timestamp: 0n,
											},
										],
										index,
										cache,
									),
								);
								expect(result.map((x) => x.gid)).to.deep.equal(["a", "b"]);

								result = await consumeAllFromAsyncIterator(
									toRebalance(
										[
											{
												range: first,
												type: "removed",
												timestamp: 1n,
											},

											{
												range: second,
												type: "added",
												timestamp: 2n,
											},
										],
										index,
										cache,
									),
								);
								expect(result.map((x) => x.gid)).to.deep.equal(["c"]);
							});

							it("differential between added and replaced", async () => {
								await create(
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.05)),
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
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.15)),
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

									createEntryReplicated({
										coordinate: denormalizeFn(rotate(0.29)),
										assignedToRangeBoundary: false,
										hash: "c",
										meta: new Meta({
											clock: new LamportClock({ id: randomBytes(32) }),
											gid: "c",
											next: [],
											type: 0,
											data: undefined,
										}),
									}),
								);

								const first = createReplicationRangeFromNormalized({
									publicKey: a,
									offset: rotate(0),
									width: 0.2,
								});

								// second covers first and a little bit more
								const second = createReplicationRangeFromNormalized({
									publicKey: a,
									offset: rotate(0),
									width: 0.3,
								});

								// the differential makes it so that only range:
								// (0,0.1)
								// (0.2, 0.3)
								// needs to be considered
								const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

								let result = await consumeAllFromAsyncIterator(
									toRebalance(
										[
											{
												range: first,
												type: "added",
												timestamp: 0n,
											},
										],
										index,
										cache,
									),
								);
								expect(result.map((x) => x.gid)).to.deep.equal(["a", "b"]);

								result = await consumeAllFromAsyncIterator(
									toRebalance(
										[
											{
												range: first,
												type: "replaced",
												timestamp: 1n,
											},

											{
												range: second,
												type: "added",
												timestamp: 2n,
											},
										],
										index,
										cache,
									),
								);
								expect(result.map((x) => x.gid)).to.deep.equal(["c"]);
							});
						});

						it("not enoughly replicated after change", async () => {
							await create(
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
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
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0.3)),
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

							const prev = createReplicationRangeFromNormalized({
								publicKey: a,
								offset: rotate(0.2),
								width: 0.2,
							});
							const updated = createReplicationRangeFromNormalized({
								id: prev.id,
								publicKey: a,
								offset: rotate(0.4),
								width: 0.2,
							});
							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							const result = await consumeAllFromAsyncIterator(
								toRebalance(
									[
										{
											range: prev,
											type: "replaced",
											timestamp: 0n,
										},

										{
											range: updated,
											type: "added",
											timestamp: 1n,
										},
									],
									index,
									cache,
								),
							);
							expect(result.map((x) => x.gid)).to.deep.eq(["b"]);
						});

						it("not enoughly replicated after removed", async () => {
							await create(
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
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
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0.3)),
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

							const updated = createReplicationRangeFromNormalized({
								publicKey: a,
								offset: rotate(0.2),
								width: 0.2,
							});

							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							const result = await consumeAllFromAsyncIterator(
								toRebalance(
									[
										{
											range: updated,
											type: "removed",
											timestamp: 0n,
										},
									],
									index,
									cache,
								),
							);
							expect(result.map((x) => x.gid)).to.deep.eq(["b"]);
						});

						it("boundary assigned are always included for empty set", async () => {
							await create(
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
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
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
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

							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							const result = await consumeAllFromAsyncIterator(
								toRebalance([], index, cache),
							);
							expect(result.map((x) => x.gid)).to.deep.eq(["b"]);
						});

						it("boundary assigned excluded when strict intent and not overlapping", async () => {
							await create(
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
									assignedToRangeBoundary: true,
									hash: "a",
									meta: new Meta({
										clock: new LamportClock({ id: randomBytes(32) }),
										gid: "a",
										next: [],
										type: 0,
										data: undefined,
									}),
								}),
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0.51)),
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

							const range = createReplicationRangeFromNormalized({
								publicKey: a,
								offset: rotate(0.5),
								width: 0.2,
								mode: ReplicationIntent.Strict,
							});
							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							const result = await consumeAllFromAsyncIterator(
								toRebalance(
									[{ range: range, type: "added", timestamp: 0n }],
									index,
									cache,
								),
							);
							expect(result.map((x) => x.gid)).to.deep.eq(["b"]);
						});

						it("boundary assigned included when mixed strictness", async () => {
							await create(
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
									assignedToRangeBoundary: true,
									hash: "a",
									meta: new Meta({
										clock: new LamportClock({ id: randomBytes(32) }),
										gid: "a",
										next: [],
										type: 0,
										data: undefined,
									}),
								}),
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0.51)),
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

							const range = createReplicationRangeFromNormalized({
								publicKey: a,
								offset: rotate(0.5),
								width: 0.2,
								mode: ReplicationIntent.Strict,
							});

							const rangeNonStrict = createReplicationRangeFromNormalized({
								publicKey: a,
								offset: rotate(0.5),
								width: 0.2,
								mode: ReplicationIntent.NonStrict,
							});

							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							const result = await consumeAllFromAsyncIterator(
								toRebalance(
									[
										{ range: range, type: "added", timestamp: 0n },
										{ range: rangeNonStrict, type: "added", timestamp: 1n },
									],
									index,
									cache,
								),
							);
							expect(result.map((x) => x.gid)).to.deep.eq(["a", "b"]);
						});

						it("boundary assigned included when updated mixed strictness", async () => {
							await create(
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0)),
									assignedToRangeBoundary: true,
									hash: "a",
									meta: new Meta({
										clock: new LamportClock({ id: randomBytes(32) }),
										gid: "a",
										next: [],
										type: 0,
										data: undefined,
									}),
								}),
								createEntryReplicated({
									coordinate: denormalizeFn(rotate(0.51)),
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

							const range = createReplicationRangeFromNormalized({
								publicKey: a,
								offset: rotate(0.5),
								width: 0.2,
								mode: ReplicationIntent.Strict,
							});

							const rangeNonStrict = createReplicationRangeFromNormalized({
								publicKey: a,
								offset: rotate(0.5),
								width: 0.2,
								mode: ReplicationIntent.NonStrict,
							});

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range: range,
													type: "replaced",
													timestamp: 0n,
												},

												{
													range: rangeNonStrict,
													type: "added",
													timestamp: 1n,
												},
											],
											index,
											new Cache<string>({ max: 1000, ttl: 1e5 }),
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["a", "b"]);

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range: range,
													type: "replaced",
													timestamp: 0n,
												},

												{
													range: rangeNonStrict,
													type: "added",
													timestamp: 1n,
												},
											],
											index,
											new Cache<string>({ max: 1000, ttl: 1e5 }),
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["a", "b"]);

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range: rangeNonStrict,
													type: "replaced",
													timestamp: 0n,
												},

												{
													range: rangeNonStrict,
													type: "added",
													timestamp: 1n,
												},
											],
											index,
											new Cache<string>({ max: 1000, ttl: 1e5 }),
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["a", "b"]);

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range: range,
													type: "replaced",
													timestamp: 0n,
												},

												{
													range: range,
													type: "added",
													timestamp: 1n,
												},
											],
											index,
											new Cache<string>({ max: 1000, ttl: 1e5 }),
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["b"]);
						});

						it("many items", async () => {
							let count = 1500;
							const entries: EntryReplicated<R>[] = [];
							for (let i = 0; i < count; i++) {
								entries.push(
									createEntryReplicated({
										coordinate: denormalizeFn(rotate(i / count)),
										assignedToRangeBoundary: true, // needs to be true, so this item always is returned
										hash: i.toString(),
										meta: new Meta({
											clock: new LamportClock({ id: randomBytes(32) }),
											gid: i.toString(),
											next: [],
											type: 0,
											data: undefined,
										}),
									}),
								);
							}
							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							await create(...entries);
							expect(
								await consumeAllFromAsyncIterator(
									toRebalance([], index, cache),
								).then((x) => x.length),
							).to.eq(count);
						});

						it("between coordinates", async () => {
							await create(
								createEntryReplicated({
									coordinate: [0, 10],
									assignedToRangeBoundary: true,
									hash: "a",
									meta: new Meta({
										clock: new LamportClock({ id: randomBytes(32) }),
										gid: "a",
										next: [],
										type: 0,
										data: undefined,
									}),
								}),
								createEntryReplicated({
									coordinate: [5, 15],
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
							const rangeIncludingB = createReplicationRange({
								publicKey: a,
								offset: 3,
								width: 3,
								mode: ReplicationIntent.Strict,
							});
							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range: rangeIncludingB,
													type: "added",
													timestamp: 0n,
												},
											],
											index,
											cache,
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["b"]);

							const rangeIncludingA = createReplicationRange({
								publicKey: a,
								offset: 8,
								width: 3,
								mode: ReplicationIntent.Strict,
							});

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range: rangeIncludingA,
													type: "added",
													timestamp: 0n,
												},
											],
											index,
											cache,
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["a"]);
						});

						it("multiple ranges", async () => {
							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							await create(
								createEntryReplicated({
									coordinate: [0, 10],
									assignedToRangeBoundary: true,
									hash: "a",
									meta: new Meta({
										clock: new LamportClock({ id: randomBytes(32) }),
										gid: "a",
										next: [],
										type: 0,
										data: undefined,
									}),
								}),
								createEntryReplicated({
									coordinate: [5, 13],
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
								createEntryReplicated({
									coordinate: [15, 20],
									assignedToRangeBoundary: false,
									hash: "c",
									meta: new Meta({
										clock: new LamportClock({ id: randomBytes(32) }),
										gid: "c",
										next: [],
										type: 0,
										data: undefined,
									}),
								}),
							);

							const rangeIncludingA = createReplicationRange({
								publicKey: a,
								offset: 8,
								width: 3,
								mode: ReplicationIntent.Strict,
							});

							const rangeIncludingC = createReplicationRange({
								publicKey: a,
								offset: 14,
								width: 2,
								mode: ReplicationIntent.Strict,
							});

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range: rangeIncludingA,
													type: "added",
													timestamp: 0n,
												},
												{
													range: rangeIncludingC,
													type: "added",
													timestamp: 1n,
												},
											],
											index,
											cache,
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["a", "c"]);
						});

						it("multiple ranges (many)", async () => {
							await create(
								createEntryReplicated({
									coordinate: [3000],
									assignedToRangeBoundary: false,
									hash: "c",
									meta: new Meta({
										clock: new LamportClock({ id: randomBytes(32) }),
										gid: "c",
										next: [],
										type: 0,
										data: undefined,
									}),
								}),
							);

							let ranges: (
								| ReplicationRangeIndexableU32
								| ReplicationRangeIndexableU64
							)[] = [];
							for (let i = 0; i < 300; i += 10) {
								ranges.push(
									createReplicationRange({
										publicKey: a,
										offset: 0,
										width: 10,
										mode: ReplicationIntent.Strict,
									}),
								);
							}
							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											ranges.map((range) => ({
												range,
												type: "added",
												timestamp: 0n,
											})),
											index,
											cache,
										),
									)
								).map((x) => x.gid),
							).to.deep.eq([]);
						});

						it("maturity will retrigger rebalance", async () => {
							await create(
								createEntryReplicated({
									coordinate: [0],
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
							);

							const range = createReplicationRange({
								publicKey: a,
								offset: 0,
								width: 1,
								mode: ReplicationIntent.Strict,
							});

							const cache = new Cache<string>({ max: 1000, ttl: 1e5 });

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range,
													type: "added",
													timestamp: 0n,
												},
											],
											index,
											cache,
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["a"]);

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range,
													type: "removed",
													timestamp: 0n,
												},
												{
													range,
													type: "added",
													timestamp: 1n,
												},
											],
											index,
											cache,
										),
									)
								).map((x) => x.gid),
							).to.deep.eq([]);

							expect(
								(
									await consumeAllFromAsyncIterator(
										toRebalance(
											[
												{
													range,
													type: "added",
													timestamp: 0n,
													matured: true,
												},
											],
											index,
											cache,
										),
									)
								).map((x) => x.gid),
							).to.deep.eq(["a"]);
						});
					});
				});
			});
		});
	});
});
