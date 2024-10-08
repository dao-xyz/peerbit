import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair, randomBytes, toBase64 } from "@peerbit/crypto";
// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { AbsoluteReplicas, maxReplicas } from "../src/replication.js";
import { waitForConverged } from "./utils.js";
import { EventStore } from "./utils/stores/event-store.js";

const checkReplicas = async (
	dbs: EventStore<string>[],
	minReplicas: number,
	entryCount: number,
) => {
	await waitForResolved(async () => {
		const map = new Map<string, number>();
		for (const db of dbs) {
			for (const value of await db.log.log.toArray()) {
				expect(await db.log.log.blocks.has(value.hash)).to.be.true;
				map.set(value.hash, (map.get(value.hash) || 0) + 1);
			}
		}
		for (const [_k, v] of map) {
			expect(v).greaterThanOrEqual(minReplicas);
			expect(v).lessThanOrEqual(dbs.length);
		}
		expect(map.size).equal(entryCount);
	});
};

const checkBounded = async (
	entryCount: number,
	lower: number,
	higher: number,
	...dbs: EventStore<string>[]
) => {
	for (const [_i, db] of dbs.entries()) {
		await waitForResolved(
			() => expect(db.log.log.length).greaterThanOrEqual(entryCount * lower),
			{
				timeout: 25 * 1000,
			},
		);
	}

	const checkConverged = async (db: EventStore<any>) => {
		const a = db.log.log.length;
		await delay(100); // arb delay
		return a === db.log.log.length;
	};

	for (const [_i, db] of dbs.entries()) {
		await waitFor(() => checkConverged(db), {
			timeout: 25000,
			delayInterval: 2500,
		});
	}

	for (const [_i, db] of dbs.entries()) {
		await waitForResolved(() =>
			expect(db.log.log.length).greaterThanOrEqual(entryCount * lower),
		);
		await waitForResolved(() =>
			expect(db.log.log.length).lessThanOrEqual(entryCount * higher),
		);
	}

	await checkReplicas(
		dbs,
		maxReplicas(dbs[0].log, [...(await dbs[0].log.log.toArray())]),
		entryCount,
	);
};

describe(`sharding`, () => {
	let session: TestSession;
	let db1: EventStore<string>,
		db2: EventStore<string>,
		db3: EventStore<string>,
		db4: EventStore<string>;

	before(async () => {
		session = await TestSession.connected(4, [
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 89, 189, 223, 17, 89, 221, 173, 81, 113, 69, 226, 180, 190,
							119, 201, 16, 59, 208, 95, 19, 142, 231, 71, 166, 43, 90, 10, 250,
							109, 68, 89, 118, 0, 27, 51, 234, 79, 160, 31, 81, 189, 54, 105,
							205, 202, 34, 30, 101, 16, 64, 52, 113, 222, 160, 31, 73, 148,
							161, 240, 201, 36, 71, 121, 134, 83,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
		]);
	});

	afterEach(async () => {
		try {
			await Promise.allSettled([
				db1?.drop(),
				db2?.drop(),
				db3?.drop(),
				db4?.drop(),
			]);
		} catch (error) {}
		db1 = undefined as any;
		db2 = undefined as any;
		db3 = undefined as any;
		db4 = undefined as any;
	});

	after(async () => {
		await session.stop();
	});

	const sampleSize = 200; // must be < 255

	it("2 peers", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicas: {
					min: 1,
				},
			},
		});
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 1,
					},
				},
			},
		);

		await waitForResolved(async () =>
			expect(await db1.log.replicationIndex?.getSize()).equal(2),
		);

		const entryCount = 200;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
			);
		}
		await Promise.all(promises);
		await checkBounded(entryCount, 0.35, 0.65, db1, db2);
	});

	it("2 peers write while joining", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicas: {
					min: 1,
				},
			},
		});
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 1,
					},
				},
			},
		);

		const entryCount = 200;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(toBase64(new Uint8Array([i]))), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
			);
		}

		await waitForResolved(async () =>
			expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(0.05),
		);
		await waitForResolved(async () =>
			expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(0.05),
		);
		await checkBounded(entryCount, 0.3, 0.7, db1, db2);
	});

	it("3 peers", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store);
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
		);
		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
		);

		const entryCount = sampleSize;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(toBase64(new Uint8Array([i]))), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
			);
		}

		await Promise.all(promises);

		await waitForResolved(async () =>
			expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(0.05),
		);
		await waitForResolved(async () =>
			expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(0.05),
		);
		await waitForResolved(async () =>
			expect((await db3.log.calculateTotalParticipation()) - 1).lessThan(0.05),
		);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
	});

	it("write while joining peers", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store);
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
		);

		const entryCount = 200;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(toBase64(new Uint8Array([i]))), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
			);
		}

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
		);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
	});

	// TODO add tests for late joining and leaving peers
	it("distributes to joining peers", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
		);

		await waitForResolved(async () =>
			expect(await db2.log.replicationIndex?.getSize()).equal(2),
		);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), {
					meta: { next: [], gidSeed: new Uint8Array([i]) },
				}),
			);
		}
		await waitFor(() => db1.log.log.length === entryCount);
		await waitFor(() => db2.log.log.length === entryCount);

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
		);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
	});
	it("distributes to leaving peers", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
		);
		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
		);

		const entryCount = sampleSize;

		await waitForResolved(async () =>
			expect(await db1.log.replicationIndex?.getSize()).equal(3),
		);
		await waitForResolved(async () =>
			expect(await db2.log.replicationIndex?.getSize()).equal(3),
		);
		await waitForResolved(async () =>
			expect(await db3.log.replicationIndex?.getSize()).equal(3),
		);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), {
					meta: { next: [], gidSeed: new Uint8Array([i]) },
				}),
			);
		}

		await Promise.all(promises);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

		const distribute = sinon.spy(db1.log.distribute);
		db1.log.distribute = distribute;

		await db3.close();
		await checkBounded(entryCount, 1, 1, db1, db2);
	});

	it("handles peer joining and leaving multiple times", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
		);
		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
		);

		const entryCount = sampleSize * 2;

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array(i)), {
					meta: { next: [], gidSeed: new Uint8Array([i]) },
				}),
			);
		}

		await Promise.all(promises);
		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

		await db3.close();
		await session.peers[2].open(db3);
		await db3.close();
		// adding some delay seems to make CI tests also fail here
		// Specifically is .pendingDeletes is used to resuse safelyDelete requests,
		// which would make this test break since reopen, would/should invalidate pending deletes
		// TODO make this more well defined

		await delay(100);

		await session.peers[2].open(db3);
		await db3.close();
		await session.peers[2].open(db3);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

		await waitForResolved(async () =>
			expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(0.1),
		);
		await waitForResolved(async () =>
			expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(0.1),
		);
		await waitForResolved(async () =>
			expect((await db3.log.calculateTotalParticipation()) - 1).lessThan(0.1),
		);

		await db3.close();

		await checkBounded(entryCount, 1, 1, db1, db2);

		await waitForResolved(async () =>
			expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(0.1),
		);
		await waitForResolved(async () =>
			expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(0.1),
		);
	});

	it("drops when no longer replicating as observer", async () => {
		let COUNT = 10;
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		);

		for (let i = 0; i < COUNT; i++) {
			await db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
		}

		await waitForResolved(() => expect(db2.log.log.length).equal(COUNT));

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		);

		await db2.log.replicate(false);

		await waitForResolved(() => expect(db3.log.log.length).equal(COUNT));
		await waitForResolved(() => expect(db2.log.log.length).equal(0));
	});

	it("drops when no longer replicating with factor 0", async () => {
		let COUNT = 100;

		const evtStore = new EventStore<string>();
		const db1p = await session.peers[0].open(evtStore, {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		const db2p = session.peers[1].open(evtStore.clone(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db1 = await db1p;
		db2 = await db2p;

		for (let i = 0; i < COUNT; i++) {
			await db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
		}

		await waitForResolved(() => expect(db2.log.log.length).equal(COUNT));

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		);
		await db2.log.replicate({ factor: 0 });
		await waitForResolved(() => expect(db3.log.log.length).equal(COUNT));
		await waitForResolved(() => expect(db2.log.log.length).equal(0)); // min replicas is set to 2 so, if there are 2 dbs still replicating, this nod should not store any data
	});

	describe("distribution", () => {
		describe("objectives", () => {
			describe("cpu", () => {
				it("no cpu usage allowed", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: true,
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										cpu: {
											max: 0,
											monitor: {
												value: () => 0.5, // fixed 50% usage
											},
										}, // 100kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					await waitForConverged(async () => {
						const diff = await db2.log.getMyTotalParticipation();
						return Math.round(diff * 100);
					});

					expect(await db2.log.getMyTotalParticipation()).equal(0); // because the CPU error from fixed usage (0.5) is always greater than max (0)
				});

				it("below limit", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: true,
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										cpu: {
											max: 0.4,
											monitor: {
												value: () => 0.3, // fixed 50% usage
											},
										}, // 100kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					await waitForConverged(async () => {
						const diff = await db1.log.getMyTotalParticipation();
						return Math.round(diff * 100);
					});
					await waitForConverged(async () => {
						const diff = await db2.log.getMyTotalParticipation();
						return Math.round(diff * 100);
					});

					expect(await db1.log.getMyTotalParticipation()).to.be.within(
						0.45,
						0.55,
					); // because the CPU error from fixed usage (0.5) is always greater than max (0)
					expect(await db2.log.getMyTotalParticipation()).to.be.within(
						0.45,
						0.55,
					); // because the CPU error from fixed usage (0.5) is always greater than max (0)
				});
			});
			describe("memory", () => {
				it("inserting half limited", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: true,
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					const memoryLimit = 100 * 1e3;
					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										storage: memoryLimit, // 100kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db1.add(data, { meta: { next: [] } });
					}

					await delay(db1.log.timeUntilRoleMaturity + 1000);

					await waitForConverged(async () => {
						const diff = Math.abs(
							(await db2.log.getMyTotalParticipation()) -
								(await db1.log.getMyTotalParticipation()),
						);
						return Math.round(diff * 50);
					});

					await waitForResolved(
						async () => {
							const memoryUsage = await db2.log.getMemoryUsage();
							expect(Math.abs(memoryLimit - memoryUsage)).lessThan(
								(memoryLimit / 100) * 5,
							);
						},
						{ timeout: 30 * 1000 },
					);
				});

				it("joining half limited", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					const memoryLimit = 100 * 1e3;
					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										storage: memoryLimit, // 100kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForConverged(async () => {
						const diff = Math.abs(
							(await db2.log.getMyTotalParticipation()) -
								(await db1.log.getMyTotalParticipation()),
						);

						return Math.round(diff * 100);
					});

					await waitForResolved(
						async () =>
							expect(
								Math.abs(memoryLimit - (await db2.log.getMemoryUsage())),
							).lessThan((memoryLimit / 100) * 10), // 10% error at most
						{ timeout: 30 * 1000, delayInterval: 1000 },
					); // 10% error at most
				});

				it("underflow limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: {
								limits: {
									storage: memoryLimit, // 100kb
								},
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										storage: memoryLimit, // 100kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb
					let entryCount = 150;
					for (let i = 0; i < entryCount; i++) {
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(
						async () => {
							expect(await db1.log.getMyTotalParticipation()).to.be.within(
								0.43,
								0.57,
							);
							expect(await db2.log.getMyTotalParticipation()).to.be.within(
								0.43,
								0.57,
							);
						},
						{ timeout: 20 * 1000 },
					);

					// allow 10% error
					await waitForResolved(async () => {
						expect(await db1.log.getMemoryUsage()).lessThan(memoryLimit * 1.1);
						expect(await db2.log.getMemoryUsage()).lessThan(memoryLimit * 1.1);
					});
				});

				it("overflow limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: {
								limits: {
									storage: memoryLimit, // 100kb
								},
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										storage: memoryLimit, // 100kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForConverged(async () =>
						Math.round((await db1.log.getMyTotalParticipation()) * 500),
					);
					await waitForConverged(async () =>
						Math.round((await db2.log.getMyTotalParticipation()) * 500),
					);
					expect(await db1.log.getMyTotalParticipation()).to.be.within(
						0.03,
						0.1,
					);
					expect(await db1.log.getMyTotalParticipation()).to.be.within(
						0.03,
						0.1,
					);
				});

				it("evenly if limited when not constrained", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: {
								limits: {
									storage: memoryLimit, // 100kb
								},
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										storage: memoryLimit * 3, // 300kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 100; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(async () => {
						expect(await db1.log.getMyTotalParticipation()).to.be.within(
							0.45,
							0.55,
						);
						expect(await db2.log.getMyTotalParticipation()).to.be.within(
							0.45,
							0.55,
						);
					});
				});

				it("unequally limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: {
								limits: {
									storage: memoryLimit, // 100kb
								},
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										storage: memoryLimit * 2, // 200kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 300; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(
						async () =>
							expect(
								Math.abs(memoryLimit - (await db1.log.getMemoryUsage())),
							).lessThan((memoryLimit / 100) * 10),
						{
							timeout: 20 * 1000,
						},
					); // 10% error at most

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit * 2 - (await db2.log.getMemoryUsage())),
						).lessThan(((memoryLimit * 2) / 100) * 10),
					); // 10% error at most

					await delay(5000);

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit - (await db1.log.getMemoryUsage())),
						).lessThan((memoryLimit / 100) * 10),
					); // 10% error at most

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit * 2 - (await db2.log.getMemoryUsage())),
						).lessThan(((memoryLimit * 2) / 100) * 10),
					); // 10% error at most
				});

				it("greatly limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: {
								limits: {
									storage: 0, // 0kb
								},
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									limits: {
										storage: memoryLimit, // 100kb
									},
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 100; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}
					await delay(db1.log.timeUntilRoleMaturity);
					try {
						await waitForResolved(async () =>
							expect(await db1.log.getMemoryUsage()).lessThan(10 * 1e3),
						); // 10% error at most

						await waitForResolved(async () =>
							expect(
								Math.abs(memoryLimit - (await db2.log.getMemoryUsage())),
							).lessThan((memoryLimit / 100) * 10),
						); // 10% error at most
					} catch (error) {
						const db1Memory = await db1.log.getMemoryUsage();
						const db2Memory = await db2.log.getMemoryUsage();
						const db1Factor = await db1.log.getMyTotalParticipation();
						const db2Factor = await db2.log.getMyTotalParticipation();
						console.log("db1 factor", db1Factor);
						console.log("db2 factor", db2Factor);
						console.log("db1 memory", db1Memory);
						console.log("db2 memory", db2Memory);
						const [_a, _b] = [db1, db2].map(
							(x) => (x.log as any)["_pendingDeletes"].size,
						);
						throw error;
					}
				});

				it("even if unlimited", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicate: true,
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: true,
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
							},
						},
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(async () => {
						expect(await db1.log.getMyTotalParticipation()).to.be.within(
							0.45,
							0.55,
						);
						expect(await db2.log.getMyTotalParticipation()).to.be.within(
							0.45,
							0.55,
						);
					});
				});
			});
		});

		describe("mixed", () => {
			it("1 limited, 2 factor", async () => {
				db1 = await session.peers[0].open(new EventStore<string>(), {
					args: {
						replicate: true,
					},
				});

				db2 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
						},
					},
				);

				db3 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								factor: 1,
							},
						},
					},
				);

				await waitForResolved(async () =>
					expect(await db1.log.getMyTotalParticipation()).equal(0),
				);
			});
		});

		describe("fixed", () => {
			it("can weight by factor", async () => {
				db1 = await session.peers[0].open(new EventStore<string>(), {
					args: {
						replicate: { offset: 0, factor: 0.05 },
						replicas: {
							min: new AbsoluteReplicas(1),
							max: new AbsoluteReplicas(1),
						},
					},
				});

				db2 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: { offset: 0.5, factor: 0.5 },
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1),
							},
						},
					},
				);
				const data = toBase64(randomBytes(5.5e2)); // about 1kb

				for (let i = 0; i < 100; i++) {
					// insert 100kb
					await db1.add(data, { meta: { next: [] } });
				}
				await waitForResolved(
					() => expect(db2.log.log.length).greaterThan(db1.log.log.length + 30),
					{
						timeout: 3e4,
					},
				);
			});
		});
	});

	// TODO test untrusted filtering
});
