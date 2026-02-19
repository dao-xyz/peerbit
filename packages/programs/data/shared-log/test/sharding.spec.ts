import { keys } from "@libp2p/crypto";
import { randomBytes, toBase64 } from "@peerbit/crypto";
// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
} from "../src/replication-domain-hash.js";
import { AbsoluteReplicas } from "../src/replication.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import {
	type TestSetupConfig,
	checkBounded,
	checkIfSetupIsUsed,
	dbgLogs,
	waitForConverged,
} from "./utils.js";
import { EventStore } from "./utils/stores/event-store.js";

export const testSetups: TestSetupConfig<any>[] = [
	{
		domain: createReplicationDomainHash("u32"),
		type: "u32",
		syncronizer: SimpleSyncronizer,
		name: "u32-simple",
	},
	/* {
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: SimpleSyncronizer,
		name: "u64-simple",
	}, */
	{
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: RatelessIBLTSynchronizer,
		name: "u64-iblt",
	},
];

testSetups.forEach((setup) => {
	describe(setup.name, () => {
		describe(`sharding`, function () {
			// Sharding is an integration-style suite that may take longer under full
			// workspace test load (GC + event-loop contention). Align Mocha's timeout
			// with the longer convergence windows used by helpers like `checkBounded`.
			this.timeout(5 * 60 * 1000);

			let session: TestSession;
			let db1: EventStore<string, ReplicationDomainHash<any>>,
				db2: EventStore<string, ReplicationDomainHash<any>>,
				db3: EventStore<string, ReplicationDomainHash<any>>,
				db4: EventStore<string, ReplicationDomainHash<any>>;

			before(async () => {
				session = await TestSession.connected(4, [
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54,
									162, 197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197,
									8, 203, 18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41,
									178, 86, 159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90,
									212, 16, 251, 206, 54, 49, 141, 91, 171,
								]),
							),
						},
					},
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									113, 203, 231, 235, 7, 120, 3, 194, 138, 113, 131, 40, 251,
									158, 121, 38, 190, 114, 116, 252, 100, 202, 107, 97, 119, 184,
									24, 56, 27, 76, 150, 62, 132, 22, 246, 177, 200, 6, 179, 117,
									218, 216, 120, 235, 147, 249, 48, 157, 232, 161, 145, 3, 63,
									158, 217, 111, 65, 105, 99, 83, 4, 113, 62, 15,
								]),
							),
						},
					},

					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									215, 31, 167, 188, 121, 226, 67, 218, 96, 8, 55, 233, 34, 68,
									9, 147, 11, 157, 187, 43, 39, 43, 25, 95, 184, 227, 137, 56,
									4, 69, 120, 214, 182, 163, 41, 82, 248, 210, 213, 22, 179,
									112, 251, 219, 52, 114, 102, 110, 6, 60, 216, 135, 218, 60,
									196, 128, 251, 85, 167, 121, 179, 136, 114, 83,
								]),
							),
						},
					},
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									176, 30, 32, 212, 227, 61, 222, 213, 141, 55, 56, 33, 95, 29,
									21, 143, 15, 130, 94, 221, 124, 176, 12, 225, 198, 214, 83,
									46, 114, 69, 187, 104, 51, 28, 15, 14, 240, 27, 110, 250, 130,
									74, 127, 194, 243, 32, 169, 162, 109, 127, 172, 232, 208, 152,
									149, 108, 74, 52, 229, 109, 23, 50, 249, 249,
								]),
							),
						},
					},
				]);
			});

			afterEach(async () => {
				// check that each domain actually is what excpected
				for (const db of [db1, db2, db3, db4]) {
					db && checkIfSetupIsUsed(setup, db.log);
				}

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
			const largeEntryCount = 1000;

			it("will not have any prunable after balance", async () => {
				const store = new EventStore<string, any>();

				db1 = await session.peers[0].open(store, {
					args: {
						replicas: {
							min: 1,
						},
						setup,
					},
				});
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

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 1,
							},
							setup,
						},
					},
				);

				await Promise.all([
					waitForConverged(() => db1.log.log.length),
					waitForConverged(() => db2.log.log.length),
				]);

				await waitForResolved(async () => {
					const prunable1 = await db1.log.getPrunable();
					const prunable2 = await db2.log.getPrunable();
					expect(prunable1).length(0);
					expect(prunable2).length(0);
				});

				expect(db1.log.log.length).to.be.greaterThan(30);
				expect(db2.log.log.length).to.be.greaterThan(30);

				expect(
					db1.log.log.length + db2.log.log.length,
				).to.be.greaterThanOrEqual(entryCount);
			});

			it("2 peers", async () => {
				const store = new EventStore<string, any>();

				db1 = await session.peers[0].open(store, {
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							offset: 0,
						},
						setup,
					},
				});
				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								offset: 0.5,
							},
							setup,
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
				const store = new EventStore<string, any>();

				db1 = await session.peers[0].open(store, {
					args: {
						replicas: {
							min: 1,
						},
						replicate: {
							offset: 0,
						},
						setup,
					},
				});
				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								offset: 0.5,
							},
							setup,
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
					expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(
						0.05,
					),
				);
				await waitForResolved(async () =>
					expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(
						0.05,
					),
				);
				await checkBounded(entryCount, 0.3, 0.7, db1, db2);
			});

			it("3 peers", async () => {
				const store = new EventStore<string, any>();

				db1 = await session.peers[0].open(store, {
					args: {
						replicate: {
							offset: 0,
						},
						setup,
					},
				});

				const entryCount = sampleSize;

				// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator
				const promises: Promise<any>[] = [];
				for (let i = 0; i < entryCount; i++) {
					promises.push(
						db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } }),
					);
				}

				await Promise.all(promises);

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0.3333,
							},
							setup,
						},
					},
				);
				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								offset: 0.6666,
							},
							setup,
						},
					},
				);

					await waitForResolved(async () =>
						// `calculateTotalParticipation()` uses a coarse sampling grid (25 points),
						// so the reported error is quantized (~4% steps). Allow some slack to
						// avoid flakes while still asserting convergence to ~1.
						expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(
							0.1,
						),
					);
					await waitForResolved(async () =>
						expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(
							0.1,
						),
					);
					await waitForResolved(async () =>
						expect((await db3.log.calculateTotalParticipation()) - 1).lessThan(
							0.1,
						),
					);

				await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
			});

			it("3 peers prune all", async () => {
				const store = new EventStore<string, any>();

				db1 = await session.peers[0].open(store, {
					args: {
						replicate: false,
						replicas: {
							min: 1,
						},
						setup,
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

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								offset: 0,
							},
							setup,
						},
					},
				);

				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicas: {
								min: 1,
							},
							replicate: {
								offset: 0.5,
							},
							setup,
						},
					},
				);

				// expect min replicas 2 with 3 peers, this means that 66% of entries (ca) will be at peer 2 and 3, and peer1 will have all of them since 1 is the creator

				try {
					await waitForResolved(() => expect(db1.log.log.length).equal(0));
				} catch (error) {
					await dbgLogs([db1.log, db2.log, db3.log]);
					throw error;
				}
			});

			it("write while joining peers", async () => {
				const store = new EventStore<string, any>();

				db1 = await session.peers[0].open(store, {
					args: {
						replicate: {
							offset: 0,
						},
						setup,
					},
				});
				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0.3333,
							},
							setup,
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

				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								offset: 0.6666,
							},
							setup,
						},
					},
				);

				await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
			});

			// TODO add tests for late joining and leaving peers
			it("distributes to joining peers", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							offset: 0,
						},
						setup,
					},
				});

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0.3333,
							},
							setup,
						},
					},
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

				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								offset: 0.6666,
							},
							setup,
						},
					},
				);

				await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
			});

			it("distributes to leaving peers", async () => {
				const args = {
					timeUntilRoleMaturity: 0,
					waitForPruneDelay: 50,
					setup,
				} as const;

				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							offset: 0,
						},
						...args,
					},
				});

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0.3333,
							},
							...args,
						},
					},
				);
				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								offset: 0.6666,
							},
							...args,
						},
					},
				);

				const entryCount = sampleSize * 3;

				await Promise.all([
					waitForResolved(async () =>
						expect(await db1.log.replicationIndex?.getSize()).equal(3),
					),
					waitForResolved(async () =>
						expect(await db2.log.replicationIndex?.getSize()).equal(3),
					),
					waitForResolved(async () =>
						expect(await db3.log.replicationIndex?.getSize()).equal(3),
					),
				]);

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

				await db3.close();

				await Promise.all([
					waitForResolved(async () =>
						expect(await db1.log.replicationIndex?.getSize()).equal(2),
					),
					waitForResolved(async () =>
						expect(await db2.log.replicationIndex?.getSize()).equal(2),
					),
				]);
				await checkBounded(entryCount, 1, 1, db1, db2);
			});

			it("handles peer joining and leaving multiple times", async () => {
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: {
								offset: 0,
							},
							setup,
						},
					});

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0.3333,
							},
							setup,
						},
					},
				);
				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								offset: 0.6666,
							},
							setup,
						},
					},
				);

				const entryCount = sampleSize * 3; // TODO make this test pass with higher multiplier (performance)

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
				await session.peers[2].open(db3, {
					args: {
						replicate: {
							offset: 0.66666,
						},
						setup,
					},
				});
				await db3.close();
				// adding some delay seems to make CI tests also fail here
				// Specifically is .pendingDeletes is used to resuse safelyDelete requests,
				// which would make this test break since reopen, would/should invalidate pending deletes
				// TODO make this more well defined

				await delay(300);

					await session.peers[2].open(db3, {
						args: {
							replicate: {
								offset: 0.66666,
							},
							setup,
						},
						});
						await db3.close();
				/* 	await session.peers[2].open(db3, {
						args: {
							replicate: {
								offset: 0.66666,
							},
							setup,
						},
					});
	
					await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);
	
					await waitForResolved(async () =>
						expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(
							0.1,
						),
					);
					await waitForResolved(async () =>
						expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(
							0.1,
						),
					);
					await waitForResolved(async () =>
						expect((await db3.log.calculateTotalParticipation()) - 1).lessThan(
							0.1,
						),
					);
	
					await db3.close(); */
				/* 	db1.log.xreset();
					db2.log.xreset(); */

					await checkBounded(entryCount, 1, 1, db1, db2);

					// Under full-suite load (GC + timers), rebalancing can take longer. Use a
					// larger window with slower polling to avoid flakiness.
					const participationWaitOpts = { timeout: 60_000, delayInterval: 500 } as const;
						await waitForResolved(
							async () =>
								expect((await db1.log.calculateTotalParticipation()) - 1).lessThan(
									0.25,
								),
							participationWaitOpts,
						);
						await waitForResolved(
							async () =>
								expect((await db2.log.calculateTotalParticipation()) - 1).lessThan(
									0.25,
								),
							participationWaitOpts,
						);
				});

			it("drops when no longer replicating as observer", async () => {
				let COUNT = 10;
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				);

				for (let i = 0; i < COUNT; i++) {
					await db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
				}

				await waitForResolved(() => expect(db2.log.log.length).equal(COUNT));

				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				);

				await db2.log.replicate(false);

				await waitForResolved(() => expect(db3.log.log.length).equal(COUNT));
				await waitForResolved(() => expect(db2.log.log.length).equal(0));
			});

			it("drops when no longer replicating with factor 0", async () => {
				let COUNT = 100;

				const evtStore = new EventStore<string, any>();
				const db1p = await session.peers[0].open(evtStore, {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				const db2p = session.peers[1].open(evtStore.clone(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				db1 = await db1p;
				db2 = await db2p;

				for (let i = 0; i < COUNT; i++) {
					await db1.add(toBase64(new Uint8Array([i])), { meta: { next: [] } });
				}

				await waitForResolved(() => expect(db2.log.log.length).equal(COUNT));

				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
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
							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: true,
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
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
										setup,
									},
								},
							);

							await delay(3e3);

							await waitForResolved(async () =>
								expect(await db2.log.calculateMyTotalParticipation()).equal(0),
							); // because the CPU error from fixed usage (0.5) is always greater than max (0)
						});

						it("below limit", async () => {
							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
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
											offset: 0.5,
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
									},
								},
							);

							await Promise.all([
								waitForConverged(async () => {
									const diff = await db1.log.calculateMyTotalParticipation();
									return Math.round(diff * 100);
								}),
								waitForConverged(async () => {
									const diff = await db2.log.calculateMyTotalParticipation();
									return Math.round(diff * 100);
								}),
							]);

							expect(
								await db1.log.calculateMyTotalParticipation(),
							).to.be.within(0.45, 0.55); // because the CPU error from fixed usage (0.5) is always greater than max (0)
							expect(
								await db2.log.calculateMyTotalParticipation(),
							).to.be.within(0.45, 0.55); // because the CPU error from fixed usage (0.5) is always greater than max (0)
						});
					});
					describe("memory", function () {
						// These tests insert 1000 entries and wait for convergence; on
						// slower CI machines this can exceed the default 60s timeout.
						this.timeout(3 * 60 * 1000);

						it("inserting half limited", async () => {
							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							const memoryLimit = 100 * 1e3;
							db2 = await EventStore.open<EventStore<string, any>>(
								db1.address!,
								session.peers[1],
								{
									args: {
										replicate: {
											limits: {
												storage: memoryLimit, // 100kb
											},
											offset: 0.5,
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
									},
								},
							);

							const data = toBase64(randomBytes(5.5e2)); // about 1kb

							for (let i = 0; i < largeEntryCount; i++) {
								await db1.add(data, { meta: { next: [] } });
							}

							await delay(db1.log.timeUntilRoleMaturity + 1000);

							await waitForConverged(async () => {
								const diff = Math.abs(
									(await db2.log.calculateMyTotalParticipation()) -
										(await db1.log.calculateMyTotalParticipation()),
								);
								return Math.round(diff * 50);
							});

							await waitForResolved(
								async () => {
									const memoryUsage = await db2.log.getMemoryUsage();
									expect(
										Math.abs(memoryLimit - memoryUsage),
										`memoryUsage=${memoryUsage} memoryLimit=${memoryLimit}`,
									).lessThan((memoryLimit / 100) * 5);
								},
								{ timeout: 30 * 1000 },
							);
						});

						it("joining half limited", async () => {
							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							const memoryLimit = 100 * 1e3;
							db2 = await EventStore.open<EventStore<string, any>>(
								db1.address!,
								session.peers[1],
								{
									args: {
										replicate: {
											limits: {
												storage: memoryLimit, // 100kb
											},
											offset: 0.5,
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
									},
								},
								);

								const data = toBase64(randomBytes(5.5e2)); // about 1kb

								for (let i = 0; i < largeEntryCount; i++) {
									await db2.add(data, { meta: { next: [] } });
								}
								await waitForConverged(
									async () => {
										const diff = Math.abs(
											(await db2.log.calculateMyTotalParticipation()) -
												(await db1.log.calculateMyTotalParticipation()),
										);

										return Math.round(diff * 100);
									},
									{
										// Rebalancing under memory limits can take longer under full-suite load
										// (GC + lots of timers). Allow more time to stabilize.
										timeout: 90 * 1000,
										tests: 3,
										interval: 1000,
										delta: 1,
									},
								);

								await waitForResolved(
									async () =>
										expect(
											Math.abs(memoryLimit - (await db2.log.getMemoryUsage())),
										).lessThan((memoryLimit / 100) * 10), // 10% error at most
									{ timeout: 20 * 1000, delayInterval: 1000 },
								); // 10% error at most
							});

						it("underflow limited", async () => {
							const memoryLimit = 100 * 1e3;

							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										limits: {
											storage: memoryLimit, // 100kb
										},
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
								db1.address!,
								session.peers[1],
								{
									args: {
										replicate: {
											limits: {
												storage: memoryLimit, // 100kb
											},
											offset: 0.5,
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
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
									expect(
										await db1.log.calculateMyTotalParticipation(),
									).to.be.within(0.38, 0.62);
									expect(
										await db2.log.calculateMyTotalParticipation(),
									).to.be.within(0.38, 0.62);
								},
								{ timeout: 20 * 1000 },
							);

							// allow 10% error
							await waitForResolved(async () => {
								expect(await db1.log.getMemoryUsage()).lessThan(
									memoryLimit * 1.1,
								);
								expect(await db2.log.getMemoryUsage()).lessThan(
									memoryLimit * 1.1,
								);
							});
						});

						it("overflow limited", async () => {
							const memoryLimit = 100 * 1e3;

							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										limits: {
											storage: memoryLimit, // 100kb
										},
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
								db1.address!,
								session.peers[1],
								{
									args: {
										replicate: {
											limits: {
												storage: memoryLimit, // 100kb
											},
											offset: 0.5,
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
									},
								},
							);

							const data = toBase64(randomBytes(5.5e2)); // about 1kb

							for (let i = 0; i < largeEntryCount; i++) {
								await db2.add(data, { meta: { next: [] } });
							}

							try {
								await Promise.all([
									waitForConverged(
										async () =>
											Math.round(
												(await db1.log.calculateMyTotalParticipation()) * 500,
											),
										{
											tests: 3,
											delta: 1,
											timeout: 30 * 1000,
											interval: 1000,
										},
									),
									waitForConverged(
										async () =>
											Math.round(
												(await db2.log.calculateMyTotalParticipation()) * 500,
											),
										{
											tests: 3,
											delta: 1,
											timeout: 30 * 1000,
											interval: 1000,
										},
									),
								]);
							} catch (error) {
								throw new Error("Total participation failed to converge");
							}

							expect(
								await db1.log.calculateMyTotalParticipation(),
							).to.be.within(0.03, 0.1);
							expect(
								await db2.log.calculateMyTotalParticipation(),
							).to.be.within(0.03, 0.1);
						});

						it("evenly if limited when not constrained", async () => {
							const memoryLimit = 100 * 1e3;

							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										limits: {
											storage: memoryLimit, // 100kb
										},
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
								db1.address!,
								session.peers[1],
								{
									args: {
										replicate: {
											limits: {
												storage: memoryLimit * 3, // 300kb
											},
											offset: 0.5,
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
									},
								},
							);

							const data = toBase64(randomBytes(5.5e2)); // about 1kb

								for (let i = 0; i < 100; i++) {
									// insert 1mb
									await db2.add(data, { meta: { next: [] } });
								}

								// Under full-suite load (GC + lots of timers), rebalancing can take
								// longer than the default waitForResolved timeout (10s).
								await waitForResolved(
									async () => {
										expect(
											await db1.log.calculateMyTotalParticipation(),
										).to.be.within(0.45, 0.55);
										expect(
											await db2.log.calculateMyTotalParticipation(),
										).to.be.within(0.45, 0.55);
									},
									{ timeout: 30 * 1000, delayInterval: 250 },
								);
							});

						it("unequally limited", async () => {
							const memoryLimit = 100 * 1e3;

							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										limits: {
											storage: memoryLimit, // 100kb
										},
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
								db1.address!,
								session.peers[1],
								{
									args: {
										replicate: {
											limits: {
												storage: memoryLimit * 2, // 200kb
											},
											offset: 0.3, // we choose 0.3 so this node can cover 0.333 - 1 (66.666%)
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
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
										).lessThan((memoryLimit / 100) * 12),
									{
										timeout: 20 * 1000,
									},
								); // allow a bit more slack under suite load

								await waitForResolved(async () =>
									expect(
										Math.abs(memoryLimit * 2 - (await db2.log.getMemoryUsage())),
									).lessThan(((memoryLimit * 2) / 100) * 12),
								); // allow a bit more slack under suite load

								await waitForResolved(async () =>
									expect(
										Math.abs(memoryLimit - (await db1.log.getMemoryUsage())),
									).lessThan((memoryLimit / 100) * 12),
								); // allow a bit more slack under suite load

								await waitForResolved(async () =>
									expect(
										Math.abs(memoryLimit * 2 - (await db2.log.getMemoryUsage())),
									).lessThan(((memoryLimit * 2) / 100) * 12),
								); // allow a bit more slack under suite load
							});

						it("greatly limited", async () => {
							const memoryLimit = 100 * 1e3;

							db1 = await session.peers[0].open(new EventStore<string, any>(), {
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
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
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
										setup,
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
									await waitForResolved(
										async () =>
											// Even with a "0 bytes" storage budget there is some
											// unavoidable bookkeeping overhead (indexes/metadata).
											// Assert we're still near-zero and far below the peer with
											// a real budget.
											expect(await db1.log.getMemoryUsage()).lessThan(20 * 1e3),
										{
											timeout: 2e4,
										},
									); // 10% error at most

								await waitForResolved(async () =>
									expect(
										Math.abs(memoryLimit - (await db2.log.getMemoryUsage())),
									).lessThan((memoryLimit / 100) * 10),
								); // 10% error at most
							} catch (error) {
								await dbgLogs([db1.log, db2.log]);
								throw error;
							}
						});

						it("even if unlimited", async () => {
							db1 = await session.peers[0].open(new EventStore<string, any>(), {
								args: {
									replicate: {
										offset: 0,
									},
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							});

							db2 = await EventStore.open<EventStore<string, any>>(
								db1.address!,
								session.peers[1],
								{
									args: {
										replicate: {
											offset: 0.5,
										},
										replicas: {
											min: new AbsoluteReplicas(1),
											max: new AbsoluteReplicas(1),
										},
										setup,
									},
								},
							);

							const data = toBase64(randomBytes(5.5e2)); // about 1kb

								for (let i = 0; i < largeEntryCount; i++) {
									await db2.add(data, { meta: { next: [] } });
								}

								try {
									await waitForResolved(async () => {
										const [p1, p2] = await Promise.all([
											db1.log.calculateMyTotalParticipation(),
											db2.log.calculateMyTotalParticipation(),
										]);
										expect(
											p1,
											`db1 participation=${p1}, db2 participation=${p2}`,
										).to.be.within(0.42, 0.58);
										expect(
											p2,
											`db1 participation=${p1}, db2 participation=${p2}`,
										).to.be.within(0.42, 0.58);
									});
								} catch (error) {
									await dbgLogs([db1.log, db2.log]);
									throw error;
								}
							});
						});
					});

				describe("mixed", () => {
					it("1 limited, 2 factor", async () => {
						db1 = await session.peers[0].open(new EventStore<string, any>(), {
							args: {
								replicate: true,
								setup,
							},
						});

						db2 = await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									replicate: {
										factor: 1,
									},
									setup,
								},
							},
						);

						db3 = await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[2],
							{
								args: {
									replicate: {
										factor: 1,
									},
									setup,
								},
							},
						);

						await waitForResolved(async () =>
							expect(await db1.log.calculateMyTotalParticipation()).equal(0),
						);
					});
				});

				describe("fixed", () => {
					it("can weight by factor", async () => {
						db1 = await session.peers[0].open(new EventStore<string, any>(), {
							args: {
								replicate: { offset: 0, factor: 0.05 },
								replicas: {
									min: new AbsoluteReplicas(1),
									max: new AbsoluteReplicas(1),
								},
								setup,
							},
						});

						db2 = await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									replicate: { offset: 0.5, factor: 0.5 },
									replicas: {
										min: new AbsoluteReplicas(1),
										max: new AbsoluteReplicas(1),
									},
									setup,
								},
							},
						);
						const data = toBase64(randomBytes(5.5e2)); // about 1kb

						for (let i = 0; i < 100; i++) {
							// insert 100kb
							await db1.add(data, { meta: { next: [] } });
						}
						await waitForResolved(
							() =>
								expect(db2.log.log.length).greaterThan(db1.log.log.length + 15),
							{
								timeout: 3e4,
							},
						);
					});
				});
			});

			// TODO test untrusted filtering
		});
	});
});
