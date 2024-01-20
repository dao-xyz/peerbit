import { EventStore } from "./utils/stores/event-store";

// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { AbsoluteReplicas, maxReplicas } from "../replication.js";
import { Replicator } from "../role";
import { Ed25519Keypair, randomBytes, toBase64 } from "@peerbit/crypto";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { jest } from "@jest/globals";
import { SharedLog } from "../index.js";

const checkReplicas = async (
	dbs: EventStore<string>[],
	minReplicas: number,
	entryCount: number
) => {
	await waitForResolved(async () => {
		const map = new Map<string, number>();
		for (const db of dbs) {
			for (const value of await db.log.log.values.toArray()) {
				expect(await db.log.log.blocks.has(value.hash)).toBeTrue();
				map.set(value.hash, (map.get(value.hash) || 0) + 1);
			}
		}
		for (const [k, v] of map) {
			expect(v).toBeGreaterThanOrEqual(minReplicas);
			expect(v).toBeLessThanOrEqual(dbs.length);
		}
		expect(map.size).toEqual(entryCount);
	});
};

const countIsLeader = async (log: SharedLog<any>, prune: boolean = true) => {
	let q = 0;
	let q2 = 0;
	let e = 0;
	let lengthFirst = log.log.length;
	const t0 = +new Date();
	for (const entry of await log.log.toArray()) {
		if (await log.isLeader(entry.gid, 2)) {
			q += 1;
		} else {
			if (prune) {
				try {
					await log.prune([entry]);
				} catch (error) {
					e += 1;
				}
			}
		}
		if (prune && (await log.isLeader(entry.gid, 2))) {
			q2 += 1;
		}
	}

	return {
		start: lengthFirst,
		leaderStart: q,
		leaderEnd: q2,
		end: log.log.length,
		err: e
	};
};

const checkBounded = async (
	entryCount: number,
	lower: number,
	higher: number,
	...dbs: EventStore<string>[]
) => {
	for (const [i, db] of dbs.entries()) {
		await waitForResolved(
			() =>
				expect(db.log.log.values.length).toBeGreaterThanOrEqual(
					entryCount * lower
				),
			{
				timeout: 25 * 1000
			}
		);
	}

	const checkConverged = async (db: EventStore<any>) => {
		const a = db.log.log.values.length;
		await delay(100); // arb delay
		return a === db.log.log.values.length;
	};

	for (const [i, db] of dbs.entries()) {
		await waitFor(() => checkConverged(db), {
			timeout: 25000,
			delayInterval: 2500
		});
	}

	for (const [i, db] of dbs.entries()) {
		await waitForResolved(() =>
			expect(db.log.log.values.length).toBeGreaterThanOrEqual(
				entryCount * lower
			)
		);
		await waitForResolved(() =>
			expect(db.log.log.values.length).toBeLessThanOrEqual(entryCount * higher)
		);
	}

	await checkReplicas(
		dbs,
		maxReplicas(dbs[0].log, [...(await dbs[0].log.log.values.toArray())]),
		entryCount
	);
};

const waitForConverged = async (
	fn: () => any,
	options: { timeout: number; tests: number } = { tests: 3, timeout: 20 * 1000 }
) => {
	let lastResult = undefined;
	let c = 0;
	let ok = 0;
	while (true) {
		const current = await fn();
		if (lastResult == current) {
			ok += 1;
			if (options.tests <= ok) {
				break;
			}
		} else {
			ok = 0;
		}
		lastResult = current;
		await delay(1000);
		c++;
		if (c * 1000 > options.timeout) {
			throw new Error("Timeout");
		}
	}
};

describe(`sharding`, () => {
	let session: TestSession;
	let db1: EventStore<string>,
		db2: EventStore<string>,
		db3: EventStore<string>,
		db4: EventStore<string>;

	beforeAll(async () => {
		session = await TestSession.connected(4, [
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215
						]),
						Ed25519Keypair
					).toPeerId()
				}
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15
						]),
						Ed25519Keypair
					).toPeerId()
				}
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242
						]),
						Ed25519Keypair
					).toPeerId()
				}
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 89, 189, 223, 17, 89, 221, 173, 81, 113, 69, 226, 180, 190,
							119, 201, 16, 59, 208, 95, 19, 142, 231, 71, 166, 43, 90, 10, 250,
							109, 68, 89, 118, 0, 27, 51, 234, 79, 160, 31, 81, 189, 54, 105,
							205, 202, 34, 30, 101, 16, 64, 52, 113, 222, 160, 31, 73, 148,
							161, 240, 201, 36, 71, 121, 134, 83
						]),
						Ed25519Keypair
					).toPeerId()
				}
			}
		]);
	});

	afterEach(async () => {
		await Promise.all([db1?.drop(), db2?.drop(), db3?.drop(), db4?.drop()]);
		db1 = undefined as any;
		db2 = undefined as any;
		db3 = undefined as any;
		db4 = undefined as any;
	});

	afterAll(async () => {
		await session.stop();
	});

	const sampleSize = 200; // must be < 255

	it("2 peers", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicas: {
					min: 1
				}
			}
		});
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 1
					}
				}
			}
		);

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()?.length).toEqual(2)
		);

		const entryCount = 200;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } })
			);
		}
		await Promise.all(promises);
		return checkBounded(entryCount, 0.4, 0.6, db1, db2);
	});

	it("2 peers write while joining", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicas: {
					min: 1
				}
			}
		});
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 1
					}
				}
			}
		);

		const entryCount = 200;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(toBase64(new Uint8Array([i]))), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } })
			);
		}

		await waitForResolved(() =>
			expect(db1.log.totalParticipation - 1).toBeLessThan(0.05)
		);
		await waitForResolved(() =>
			expect(db2.log.totalParticipation - 1).toBeLessThan(0.05)
		);
		await checkBounded(entryCount, 0.4, 0.6, db1, db2);
	});

	it("3 peers", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store);
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		);
		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2]
		);

		const entryCount = sampleSize;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(toBase64(new Uint8Array([i]))), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } })
			);
		}

		await Promise.all(promises);

		await waitForResolved(() =>
			expect(db1.log.totalParticipation - 1).toBeLessThan(0.05)
		);
		await waitForResolved(() =>
			expect(db2.log.totalParticipation - 1).toBeLessThan(0.05)
		);
		await waitForResolved(() =>
			expect(db3.log.totalParticipation - 1).toBeLessThan(0.05)
		);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
	});

	it("write while joining peers", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store);
		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		);

		const entryCount = 200;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(toBase64(new Uint8Array([i]))), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } })
			);
		}

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2]
		);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
	});

	// TODO add tests for late joining and leaving peers
	it("distributes to joining peers", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		);

		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()?.length).toEqual(2)
		);

		const entryCount = sampleSize;
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } })
			);
		}
		await waitFor(() => db1.log.log.values.length === entryCount);
		await waitFor(() => db2.log.log.values.length === entryCount);

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2]
		);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
	});
	it("distributes to leaving peers", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		);
		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2]
		);

		const entryCount = sampleSize;

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()?.length).toEqual(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()?.length).toEqual(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()?.length).toEqual(3)
		);

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } })
			);
		}

		await Promise.all(promises);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

		const distribute = jest.fn(db1.log.distribute);
		db1.log.distribute = distribute;

		await db3.close();
		await checkBounded(entryCount, 1, 1, db1, db2);
	});

	it("handles peer joining and leaving multiple times", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1]
		);
		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2]
		);

		const entryCount = sampleSize * 2;

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array(i)), { meta: { next: [] } })
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

		await waitForResolved(() =>
			expect(db1.log.totalParticipation - 1).toBeLessThan(0.1)
		);
		await waitForResolved(() =>
			expect(db2.log.totalParticipation - 1).toBeLessThan(0.1)
		);
		await waitForResolved(() =>
			expect(db3.log.totalParticipation - 1).toBeLessThan(0.1)
		);

		await db3.close();

		await checkBounded(entryCount, 1, 1, db1, db2);

		await waitForResolved(() =>
			expect(db1.log.totalParticipation - 1).toBeLessThan(0.1)
		);
		await waitForResolved(() =>
			expect(db2.log.totalParticipation - 1).toBeLessThan(0.1)
		);
	});

	it("drops when no longer replicating as observer", async () => {
		let COUNT = 10;
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				role: {
					type: "replicator",
					factor: 1
				}
			}
		});

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					role: {
						type: "replicator",
						factor: 1
					}
				}
			}
		);

		for (let i = 0; i < COUNT; i++) {
			await db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
		}

		await waitForResolved(() => expect(db2.log.log.length).toEqual(COUNT));

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					role: {
						type: "replicator",
						factor: 1
					}
				}
			}
		);

		await db2.log.updateRole("observer");

		await waitForResolved(() => expect(db3.log.log.length).toEqual(COUNT));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(0));
	});

	it("drops when no longer replicating with factor 0", async () => {
		let COUNT = 100;

		const evtStore = new EventStore<string>();
		const db1p = await session.peers[0].open(evtStore, {
			args: {
				role: {
					type: "replicator",
					factor: 1
				}
			}
		});

		const db2p = session.peers[1].open(evtStore.clone(), {
			args: {
				role: {
					type: "replicator",
					factor: 1
				}
			}
		});

		db1 = await db1p;
		db2 = await db2p;

		for (let i = 0; i < COUNT; i++) {
			await db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
		}

		await waitForResolved(() => expect(db2.log.log.length).toEqual(COUNT));

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					role: {
						type: "replicator",
						factor: 1
					}
				}
			}
		);

		await db2.log.updateRole({ type: "replicator", factor: 0 });

		await waitForResolved(() => expect(db3.log.log.length).toEqual(COUNT));
		await waitForResolved(() => expect(db2.log.log.length).toEqual(0));
	});

	describe("distribution", () => {
		describe("objectives", () => {
			describe("cpu", () => {
				it("no cpu usage allowed", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator"
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										cpu: {
											max: 0,
											monitor: {
												value: () => 0.5 // fixed 50% usage
											}
										} // 100kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					await waitForConverged(() => {
						const diff = Math.abs((db2.log.role as Replicator).factor);
						return Math.round(diff * 100);
					});

					expect((db2.log.role as Replicator).factor).toEqual(0); // because the CPU error from fixed usage (0.5) is always greater than max (0)
				});

				it("below limit", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator"
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										cpu: {
											max: 0.4,
											monitor: {
												value: () => 0.3 // fixed 50% usage
											}
										} // 100kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					await waitForConverged(() => {
						const diff = Math.abs((db1.log.role as Replicator).factor);
						return Math.round(diff * 100);
					});
					await waitForConverged(() => {
						const diff = Math.abs((db2.log.role as Replicator).factor);
						return Math.round(diff * 100);
					});

					expect((db1.log.role as Replicator).factor).toBeWithin(0.45, 0.55); // because the CPU error from fixed usage (0.5) is always greater than max (0)
					expect((db2.log.role as Replicator).factor).toBeWithin(0.45, 0.55); // because the CPU error from fixed usage (0.5) is always greater than max (0)
				});
			});
			describe("memory", () => {
				it("inserting half limited", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator"
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					const memoryLimit = 100 * 1e3;
					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										memory: memoryLimit // 100kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db1.add(data, { meta: { next: [] } });
					}

					await delay(db1.log.timeUntilRoleMaturity + 1000);

					await waitForConverged(() => {
						const diff = Math.abs(
							(db2.log.role as Replicator).factor -
								(db1.log.role as Replicator).factor
						);
						return Math.round(diff * 50);
					});

					await waitForResolved(
						async () => {
							const memoryUsage = await db2.log.getMemoryUsage();
							expect(Math.abs(memoryLimit - memoryUsage)).toBeLessThan(
								(memoryLimit / 100) * 5
							);
						},
						{ timeout: 20 * 1000 }
					);
				});

				it("joining half limited", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							},
							role: {
								type: "replicator"
							}
						}
					});

					const memoryLimit = 100 * 1e3;
					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										memory: memoryLimit // 100kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForConverged(() => {
						const diff = Math.abs(
							(db2.log.role as Replicator).factor -
								(db1.log.role as Replicator).factor
						);
						return Math.round(diff * 100);
					});
					await waitForResolved(
						async () =>
							expect(
								Math.abs(memoryLimit - (await db2.log.getMemoryUsage()))
							).toBeLessThan((memoryLimit / 100) * 10),
						{ timeout: 30 * 1000 }
					); // 10% error at most
				});

				it("underflow limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator",
								limits: {
									memory: memoryLimit // 100kb
								}
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										memory: memoryLimit // 100kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb
					let entryCount = 150;
					for (let i = 0; i < entryCount; i++) {
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(
						async () => {
							expect((db1.log.role as Replicator).factor).toBeWithin(
								0.45,
								0.55
							);
							expect((db2.log.role as Replicator).factor).toBeWithin(
								0.45,
								0.55
							);
						},
						{ timeout: 20 * 1000 }
					);

					// allow 10% error
					await waitForResolved(async () => {
						expect(await db1.log.getMemoryUsage()).toBeLessThan(
							memoryLimit * 1.1
						);
						expect(await db2.log.getMemoryUsage()).toBeLessThan(
							memoryLimit * 1.1
						);
					});
				});

				it("overflow limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator",
								limits: {
									memory: memoryLimit // 100kb
								}
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										memory: memoryLimit // 100kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForConverged(() =>
						Math.round((db1.log.role as Replicator).factor * 500)
					);
					await waitForConverged(() =>
						Math.round((db2.log.role as Replicator).factor * 500)
					);
					expect((db1.log.role as Replicator).factor).toBeWithin(0.03, 0.05);
					expect((db1.log.role as Replicator).factor).toBeWithin(0.03, 0.05);
				});

				it("evenly if limited when not constrained", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator",
								limits: {
									memory: memoryLimit // 100kb
								}
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										memory: memoryLimit * 3 // 300kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 100; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(() => {
						expect((db1.log.role as Replicator).factor).toBeWithin(0.45, 0.55);
						expect((db2.log.role as Replicator).factor).toBeWithin(0.45, 0.55);
					});
				});

				it("unequally limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator",
								limits: {
									memory: memoryLimit // 100kb
								}
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										memory: memoryLimit * 2 // 200kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 300; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(
						async () =>
							expect(
								Math.abs(memoryLimit - (await db1.log.getMemoryUsage()))
							).toBeLessThan((memoryLimit / 100) * 10),
						{
							timeout: 20 * 1000
						}
					); // 10% error at most

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit * 2 - (await db2.log.getMemoryUsage()))
						).toBeLessThan(((memoryLimit * 2) / 100) * 10)
					); // 10% error at most

					await delay(5000);

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit - (await db1.log.getMemoryUsage()))
						).toBeLessThan((memoryLimit / 100) * 10)
					); // 10% error at most

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit * 2 - (await db2.log.getMemoryUsage()))
						).toBeLessThan(((memoryLimit * 2) / 100) * 10)
					); // 10% error at most
				});

				it("greatly limited", async () => {
					const memoryLimit = 100 * 1e3;

					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator",
								limits: {
									memory: 0 // 0kb
								}
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator",
									limits: {
										memory: memoryLimit // 100kb
									}
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 100; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}
					await delay(db1.log.timeUntilRoleMaturity);

					await waitForResolved(async () =>
						expect(await db1.log.getMemoryUsage()).toBeLessThan(10 * 1e3)
					); // 10% error at most

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit - (await db2.log.getMemoryUsage()))
						).toBeLessThan((memoryLimit / 100) * 10)
					); // 10% error at most
				});

				it("even if unlimited", async () => {
					db1 = await session.peers[0].open(new EventStore<string>(), {
						args: {
							role: {
								type: "replicator"
							},
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					});

					db2 = await EventStore.open<EventStore<string>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								role: {
									type: "replicator"
								},
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1)
								}
							}
						}
					);

					const data = toBase64(randomBytes(5.5e2)); // about 1kb

					for (let i = 0; i < 1000; i++) {
						// insert 1mb
						await db2.add(data, { meta: { next: [] } });
					}

					await waitForResolved(() => {
						expect((db1.log.role as Replicator).factor).toBeWithin(0.45, 0.55);
						expect((db2.log.role as Replicator).factor).toBeWithin(0.45, 0.55);
					});
				});
			});
		});

		describe("mixed", () => {
			it("1 limited, 2 factor", async () => {
				db1 = await session.peers[0].open(new EventStore<string>(), {
					args: {
						role: {
							type: "replicator"
						}
					}
				});

				db2 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							role: {
								type: "replicator",
								factor: 1
							}
						}
					}
				);

				db3 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							role: {
								type: "replicator",
								factor: 1
							}
						}
					}
				);

				await waitForResolved(() =>
					expect((db1.log.role as Replicator).factor).toEqual(0)
				);
			});
		});

		describe("fixed", () => {
			it("can weight by factor", async () => {
				db1 = await session.peers[0].open(new EventStore<string>(), {
					args: {
						role: { type: "replicator", factor: 0.05 },
						replicas: {
							min: new AbsoluteReplicas(1),
							max: new AbsoluteReplicas(1)
						}
					}
				});

				db2 = await EventStore.open<EventStore<string>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							role: { type: "replicator", factor: 0.5 },
							replicas: {
								min: new AbsoluteReplicas(1),
								max: new AbsoluteReplicas(1)
							}
						}
					}
				);
				const data = toBase64(randomBytes(5.5e2)); // about 1kb

				for (let i = 0; i < 100; i++) {
					// insert 100kb
					await db1.add(data, { meta: { next: [] } });
				}
				await waitForResolved(() =>
					expect(db2.log.log.length).toBeGreaterThan(db1.log.log.length + 50)
				);
			});
		});
	});

	describe("union", () => {
		it("local first", async () => {
			const store = new EventStore<string>();
			db1 = await session.peers[0].open(store, {
				args: {
					role: {
						type: "replicator",
						factor: 0.5
					},
					replicas: {
						min: 2
					},
					timeUntilRoleMaturity: 10 * 1000
				}
			});
			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						role: {
							type: "replicator",
							factor: 0.5
						},
						replicas: {
							min: 2
						},
						timeUntilRoleMaturity: 10 * 1000
					}
				}
			);

			await waitForResolved(() => {
				expect(db1.log.getReplicatorsSorted()?.length).toEqual(2);
			});

			await waitForResolved(() => {
				expect(db2.log.getReplicatorsSorted()?.length).toEqual(2);
			});

			// expect either db1 to replicate more than 50% or db2 to replicate more than 50%
			// for these
			expect(db1.log.getReplicatorUnion(0)).toEqual([
				session.peers[0].identity.publicKey.hashcode()
			]);

			expect(db2.log.getReplicatorUnion(0)).toEqual([
				session.peers[1].identity.publicKey.hashcode()
			]);
		});

		it("sets replicators groups correctly", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store, {
				args: {
					replicas: {
						min: 1
					}
				}
			});
			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicas: {
							min: 1
						}
					}
				}
			);

			db3 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicas: {
							min: 1
						}
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
			);

			await waitForResolved(() =>
				expect(
					Math.abs(
						db1.log
							.getReplicatorsSorted()
							?.toArray()
							?.reduce((prev, current) => {
								return prev + current.role.factor;
							}, 0) || 0
					) - 1
				).toBeLessThan(0.01)
			);

			for (let i = 1; i < 3; i++) {
				db1.log.replicas.min = { getValue: () => i };

				// min replicas 3 only need to query 1 (every one have all the data)
				// min replicas 2 only need to query 2
				// min replicas 1 only need to query 3 (data could end up at any of the 3 nodes)
				await waitForResolved(() =>
					expect(db1.log.getReplicatorUnion(0)).toHaveLength(3 - i + 1)
				);
			}
		});

		it("all non-mature, all included", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store, {
				args: {
					replicas: {
						min: 1
					}
				}
			});

			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicas: {
							min: 1
						}
					}
				}
			);

			db3 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicas: {
							min: 1
						}
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
			);

			for (let i = 1; i < 3; i++) {
				db3.log.replicas.min = { getValue: () => i };

				// Should always include all nodes since no is mature
				expect(
					db3.log.getReplicatorUnion(Number.MAX_SAFE_INTEGER)
				).toHaveLength(3);
			}
		});

		it("one mature, all included", async () => {
			const store = new EventStore<string>();

			const MATURE_TIME = 3000;
			db1 = await session.peers[0].open(store, {
				args: {
					replicas: {
						min: 1
					},
					role: {
						type: "replicator",
						factor: 0.1
					}
				}
			});

			await delay(MATURE_TIME);

			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicas: {
							min: 1
						},
						role: {
							type: "replicator",
							factor: 1
						}
					}
				}
			);

			db3 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[2],
				{
					args: {
						replicas: {
							min: 1
						},
						role: {
							type: "replicator",
							factor: 1
						}
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).toHaveLength(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).toHaveLength(3)
			);

			for (let i = 1; i < 3; i++) {
				db3.log.replicas.min = { getValue: () => i };

				// Should always include all nodes since no is mature
				expect(db3.log.getReplicatorUnion(MATURE_TIME)).toHaveLength(3);
			}
			await delay(MATURE_TIME);

			for (let i = 1; i < 3; i++) {
				db3.log.replicas.min = { getValue: () => i };

				// Should always include all nodes since no is mature
				expect(db3.log.getReplicatorUnion(MATURE_TIME)).toHaveLength(1); // since I am replicating with factor 1 and is mature
			}
		});
	});

	// TODO test untrusted filtering
});
