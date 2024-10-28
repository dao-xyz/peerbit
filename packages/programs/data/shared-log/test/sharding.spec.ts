import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import { randomBytes, toBase64 } from "@peerbit/crypto";
// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { AbsoluteReplicas } from "../src/replication.js";
import { checkBounded, waitForConverged } from "./utils.js";
import { EventStore } from "./utils/stores/event-store.js";

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
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54, 162,
							197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197, 8, 203,
							18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41, 178, 86,
							159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90, 212, 16, 251,
							206, 54, 49, 141, 91, 171,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							113, 203, 231, 235, 7, 120, 3, 194, 138, 113, 131, 40, 251, 158,
							121, 38, 190, 114, 116, 252, 100, 202, 107, 97, 119, 184, 24, 56,
							27, 76, 150, 62, 132, 22, 246, 177, 200, 6, 179, 117, 218, 216,
							120, 235, 147, 249, 48, 157, 232, 161, 145, 3, 63, 158, 217, 111,
							65, 105, 99, 83, 4, 113, 62, 15,
						]),
					),
				},
			},

			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							215, 31, 167, 188, 121, 226, 67, 218, 96, 8, 55, 233, 34, 68, 9,
							147, 11, 157, 187, 43, 39, 43, 25, 95, 184, 227, 137, 56, 4, 69,
							120, 214, 182, 163, 41, 82, 248, 210, 213, 22, 179, 112, 251, 219,
							52, 114, 102, 110, 6, 60, 216, 135, 218, 60, 196, 128, 251, 85,
							167, 121, 179, 136, 114, 83,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							176, 30, 32, 212, 227, 61, 222, 213, 141, 55, 56, 33, 95, 29, 21,
							143, 15, 130, 94, 221, 124, 176, 12, 225, 198, 214, 83, 46, 114,
							69, 187, 104, 51, 28, 15, 14, 240, 27, 110, 250, 130, 74, 127,
							194, 243, 32, 169, 162, 109, 127, 172, 232, 208, 152, 149, 108,
							74, 52, 229, 109, 23, 50, 249, 249,
						]),
					),
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

	it("will not have any prunable after balance", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicas: {
					min: 1,
				},
				/* 	timeUntilRoleMaturity: 0 */
			},
		});
		const entryCount = 200;

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: {
						min: 1,
					},
					/* 	timeUntilRoleMaturity: 0 */
				},
			},
		);

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			// db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
			);
		}

		await Promise.all(promises);

		await waitForConverged(() => db1.log.log.length);
		await waitForConverged(() => db2.log.log.length);

		await waitForResolved(async () => {
			const prunable1 = await db1.log.getPrunable();
			const prunable2 = await db2.log.getPrunable();
			expect(prunable1).length(0);
			expect(prunable2).length(0);
		});

		expect(db1.log.log.length).to.be.greaterThan(30);
		expect(db2.log.log.length).to.be.greaterThan(30);

		expect(db1.log.log.length + db2.log.log.length).to.be.greaterThanOrEqual(
			entryCount,
		);
	});

	it("2 peers", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicas: {
					min: 1,
				},
				/* 	timeUntilRoleMaturity: 0 */
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
					/* 	timeUntilRoleMaturity: 0 */
				},
			},
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

		const entryCount = sampleSize;

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
			);
		}

		await Promise.all(promises);

		db2 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
		);
		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
		);

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

	it("3 peers prune all", async () => {
		const store = new EventStore<string>();

		db1 = await session.peers[0].open(store, {
			args: {
				replicate: false,
				replicas: {
					min: 1,
				},
			},
		});

		const promises: Promise<any>[] = [];
		for (let i = 0; i < 500; i++) {
			// db1.add(toBase64(toBase64(new Uint8Array([i]))), { meta: { next: [] } });
			promises.push(
				db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
			);
		}

		await Promise.all(promises);

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
		await delay(3e3);

		db3 = await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[2],
			{
				args: {
					replicas: {
						min: 1,
					},
				},
			},
		);

		// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator

		await waitForResolved(() => expect(db1.log.log.length).equal(0));
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
					meta: { next: [] },
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

		const entryCount = sampleSize * 6;

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
					meta: { next: [] },
				}),
			);
		}

		await Promise.all(promises);

		await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

		const distribute = sinon.spy(db1.log.onReplicationChange);
		db1.log.onReplicationChange = distribute;

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

		const entryCount = sampleSize * 5;

		const promises: Promise<any>[] = [];
		for (let i = 0; i < entryCount; i++) {
			promises.push(
				db1.add(toBase64(new Uint8Array(i)), {
					meta: { next: [] },
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

					await delay(3e3);

					await waitForResolved(async () =>
						expect(await db2.log.getMyTotalParticipation()).equal(0),
					); // because the CPU error from fixed usage (0.5) is always greater than max (0)
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
					try {
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
							{ timeout: 20 * 1000, delayInterval: 1000 },
						); // 10% error at most
					} catch (error) {
						const weight1 = await db2.log.getMemoryUsage();

						const weight2 = await db2.log.getMemoryUsage();
						console.log("weight", weight1, weight2);
						throw error;
					}
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
					await waitForResolved(
						async () =>
							expect(await db1.log.getMemoryUsage()).lessThan(10 * 1e3),
						{
							timeout: 2e4,
						},
					); // 10% error at most

					await waitForResolved(async () =>
						expect(
							Math.abs(memoryLimit - (await db2.log.getMemoryUsage())),
						).lessThan((memoryLimit / 100) * 10),
					); // 10% error at most
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
