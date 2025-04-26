import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import { type IndexedResults, Sort } from "@peerbit/indexer-interface";
import type { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import path from "path";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import {
	type ReplicationDomainHash,
	type ReplicationRangeIndexable,
	type SharedLog,
	createReplicationDomainTime,
} from "../src/index.js";
import { denormalizer } from "../src/integers.js";
import { ReplicationIntent, isMatured } from "../src/ranges.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { EventStore } from "./utils/stores/event-store.js";

const checkRoleIsDynamic = async (log: SharedLog<any, any>) => {
	const roles: any[] = [];
	log.events.addEventListener("replication:change", (change) => {
		if (change.detail.publicKey.equals(log.node.identity.publicKey)) {
			roles.push(change.detail);
		}
	});

	/// expect role to update a few times
	await waitForResolved(() => expect(roles.length).greaterThan(3));
};

const scaleToU64 = denormalizer("u64");

describe(`replicate`, () => {
	let session: TestSession;
	let db1: EventStore<string, ReplicationDomainHash<"u32">>,
		db2: EventStore<string, ReplicationDomainHash<"u32">>;

	before(async () => {
		session = await TestSession.disconnected(3, [
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
							168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
							159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53,
							142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72,
							148, 82, 66, 138, 199, 185,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
							157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120,
							122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251,
							100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6,
							174, 212, 159, 187, 2, 137, 47, 192,
						]),
					),
				},
			},
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
		]);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);
		await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
		await session.peers[2].services.blocks.waitFor(session.peers[1].peerId);
	});

	after(async () => {
		await session.stop();
	});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1?.closed === false) {
			await db1?.drop();
		}
		if (db2?.closed === false) {
			await db2?.drop();
		}
	});

	it("none", async () => {
		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: { factor: 1 } },
		});

		db2 = (await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { replicate: false },
			},
		))!;

		await db1.waitFor(session.peers[1].peerId);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.length === 2); // db2 can write ...
		expect(
			(await db1.log.log.toArray()).map((x) => x.payload.getValue().value),
		).to.have.members(["hello", "world"]);
		expect(db2.log.log.length).equal(1); // ... but will not receive entries
	});

	describe("observer", () => {
		it("can update", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());

			expect(
				(db1.log.node.services.pubsub as any)["subscriptions"].get(
					db1.log.rpc.topic,
				).counter,
			).equal(1);
			expect([...(await db1.log.getReplicators())]).to.deep.equal([
				db1.node.identity.publicKey.hashcode(),
			]);
			expect(await db1.log.isReplicating()).to.be.true;
			await db1.log.replicate(false);
			expect(await db1.log.isReplicating()).to.be.false;
			expect(
				(db1.log.node.services.pubsub as any)["subscriptions"].get(
					db1.log.rpc.topic,
				).counter,
			).equal(1);
		});

		it("observer", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: { replicate: false },
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);

			await db1.add("hello");
			await db2.add("world");

			await waitFor(() => db1.log.log.length === 2); // db2 can write ...
			expect(
				(await db1.log.log.toArray()).map((x) => x.payload.getValue().value),
			).to.have.members(["hello", "world"]);
			expect(db2.log.log.length).equal(1); // ... but will not receive entries
		});
	});

	describe("replicator", () => {
		it("fixed-object", async () => {
			db1 = await session.peers[0].open(new EventStore(), {
				args: {
					replicate: {
						offset: 0.7,
						factor: 0.5,
					},
				},
			});

			let ranges = await db1.log.getMyReplicationSegments();
			expect(ranges).to.have.length(1);
			expect(Number(ranges[0].toReplicationRange().offset)).to.closeTo(
				Number(scaleToU64(0.7)),
				Number(scaleToU64(0.000001)),
			);
			expect(Number(ranges[0].toReplicationRange().factor)).to.closeTo(
				Number(scaleToU64(0.5)),
				Number(scaleToU64(0.000001)),
			);
		});

		it("fixed-simple", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: 1,
				},
			});

			let ranges = await db1.log.getMyReplicationSegments();
			expect(ranges).to.have.length(1);
			expect(ranges[0].toReplicationRange().factor).to.eq(scaleToU64(1));
		});

		it("can unreplicate", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: 1,
				},
			});
			db2 = await session.peers[1].open(db1.clone(), {
				args: {
					replicate: 1,
				},
			});

			await waitForResolved(async () =>
				expect((await db1.log.getReplicators()).size).to.equal(2),
			);
			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(2),
			);

			await db1.log.replicate(false);
			expect(await db1.log.isReplicating()).to.be.false;
			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(1),
			);
			await waitForResolved(async () =>
				expect((await db2.log.getReplicators()).size).to.equal(1),
			);
		});

		it("adding segments", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						offset: 0,
						factor: 0.1,
					},
				},
			});
			db2 = await session.peers[1].open(db1.clone(), {
				args: {
					replicate: 1,
				},
			});

			await waitForResolved(async () =>
				expect(await db1.log.calculateTotalParticipation()).to.be.closeTo(
					1.1,
					0.1,
				),
			);
			await waitForResolved(async () =>
				expect(await db2.log.calculateTotalParticipation()).to.be.closeTo(
					1.1,
					0.1,
				),
			);

			await db1.log.replicate({ offset: 0.2, factor: 0.2 });

			await waitForResolved(async () =>
				expect(await db1.log.calculateTotalParticipation()).to.be.closeTo(
					1.3,
					0.1,
				),
			);
			await waitForResolved(async () =>
				expect(await db2.log.calculateTotalParticipation()).to.be.closeTo(
					1.3,
					0.1,
				),
			);
		});

		it("merge segments", async () => {
			let domain = createReplicationDomainTime({
				canMerge: () => true,
			});

			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						offset: 0.3,
						factor: 0.3,
					},
					domain,
				},
			});

			db2 = await session.peers[1].open(db1.clone(), {
				args: {
					replicate: 1,
					domain,
				},
			});

			await db1.log.replicate(
				{ offset: 0.15, factor: 0.3 },
				{ mergeSegments: true },
			);

			const segmentsAfterReplicate = await db1.log.getMyReplicationSegments();
			expect(segmentsAfterReplicate).to.have.length(1);
			expect(
				segmentsAfterReplicate.map((x) => x.widthNormalized)[0],
			).to.be.closeTo(0.45, 0.001);
		});

		it("dynamic by default", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());
			await checkRoleIsDynamic(db1.log);
		});

		it("update to dynamic role", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());
			await db1.log.replicate(false);
			await db1.log.replicate({ limits: {} });
			await checkRoleIsDynamic(db1.log);
		});

		it("waitForReplicator waits until maturity", async () => {
			const store = new EventStore<string, any>();

			const db1 = await session.peers[0].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
				},
			});
			db2.log.getDefaultMinRoleAge = () => Promise.resolve(3e3);
			const t0 = +new Date();
			await db2.log.waitForReplicator(db1.node.identity.publicKey);
			const t1 = +new Date();
			expect(t1 - t0).greaterThanOrEqual(
				(await db2.log.getDefaultMinRoleAge()) - 100,
			); // - 100 for handle timer inaccuracy
		});
		describe("getDefaultMinRoleAge", () => {
			it("if not replicating, min role age is 0", async () => {
				const store = new EventStore<string, any>();

				await session.peers[0].open(store.clone(), {
					args: {
						replicate: {
							factor: 1,
						},
					},
				});
				const db2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: false,
					},
				});
				await waitForResolved(async () =>
					expect((await db2.log.getReplicators()).size).to.equal(1),
				);
				expect(await db2.log.getDefaultMinRoleAge()).equal(0);
			});

			it("oldest is always mature", async () => {
				const store = new EventStore<string, any>();

				const timeUntilRoleMaturity = 500;
				const tsm = 1000;

				const db1 = await session.peers[0].open(store.clone(), {
					args: {
						replicate: {
							factor: 1,
						},
						timeUntilRoleMaturity,
					},
				});

				await delay(tsm);

				const db2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: {
							factor: 1,
						},
						timeUntilRoleMaturity,
					},
				});
				await waitForResolved(async () =>
					expect(await db1.log.replicationIndex?.getSize()).equal(2),
				);
				await waitForResolved(async () =>
					expect(await db2.log.replicationIndex?.getSize()).equal(2),
				);

				const db1MinRoleAge = await db1.log.getDefaultMinRoleAge();
				const db2MinRoleAge = await db2.log.getDefaultMinRoleAge();

				expect(db1MinRoleAge - db2MinRoleAge).lessThanOrEqual(1); // db1 sets the minRole age because it is the oldest. So both dbs get same minRole age limit (including some error margin)

				const now = +new Date();

				// Mature because if "first"
				let selfMatured = isMatured(
					(await db1.log.getMyReplicationSegments())[0],
					now,
					await db1.log.getDefaultMinRoleAge(),
				);
				expect(selfMatured).to.be.true;

				await waitForResolved(async () => {
					const minRoleAge = await db1.log.getDefaultMinRoleAge();
					expect(
						(await db1.log.replicationIndex.iterate().all())
							.map((x) => x.value)
							.filter((x) => isMatured(x, now, minRoleAge))
							.map((x) => x.hash),
					).to.deep.equal([db1.node.identity.publicKey.hashcode()]);
				});

				// assume other nodes except me are mature if they open before me
				selfMatured = isMatured(
					(await db2.log.getMyReplicationSegments())[0],
					now,
					await db2.log.getDefaultMinRoleAge(),
				);
				expect(selfMatured).to.be.false;

				const minRoleAge = await db2.log.getDefaultMinRoleAge();
				expect(
					(await db2.log.replicationIndex.iterate().all())
						.map((x) => x.value)
						.map((x) => isMatured(x, now, minRoleAge)),
				).to.have.members([false, true]);
			});

			// TODO more tests for behaviours of getDefaultMinRoleAge
		});

		describe("mode", () => {
			describe("strict", () => {
				it("on open", async () => {
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: {
								normalized: false,
								offset: 0,
								factor: 1,
							},
						},
					});
					db2 = await session.peers[1].open(db1.clone(), {
						args: {
							replicate: {
								normalized: false,
								factor: 2,
								offset: 5,
								strict: true,
							},
						},
					});

					await waitForResolved(async () =>
						expect(await db1.log.replicationIndex.count()).to.equal(2),
					);
					const segments = await db1.log.replicationIndex
						.iterate({ sort: [new Sort({ key: "start1" })] })
						.all();

					expect(segments[0].value.start1).to.eq(0n);
					expect(segments[0].value.mode).to.eq(ReplicationIntent.NonStrict);
					expect(segments[1].value.start1).to.eq(5n);
					expect(segments[1].value.mode).to.eq(ReplicationIntent.Strict);
				});

				it("will not rebalance on maturity of strict", async () => {
					let maturityTime = 1e3;

					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: false,
							timeUntilRoleMaturity: maturityTime,
						},
					});
					db2 = await session.peers[1].open(db1.clone(), {
						args: {
							replicate: false,
							timeUntilRoleMaturity: maturityTime,
						},
					});

					const maturedObservedFrom1 = pDefer<void>();

					db1.log.events.addEventListener("replicator:mature", (e) => {
						if (e.detail.publicKey.equals(db2.node.identity.publicKey)) {
							maturedObservedFrom1.resolve();
						}
					});

					const maturedObservedFrom2 = pDefer<void>();

					db2.log.events.addEventListener("replicator:mature", (e) => {
						if (e.detail.publicKey.equals(db2.node.identity.publicKey)) {
							maturedObservedFrom2.resolve();
						}
					});

					const onReplicationChange1 = sinon.spy(db1.log.onReplicationChange);
					db1.log.onReplicationChange = onReplicationChange1;
					const onReplicationChange2 = sinon.spy(db2.log.onReplicationChange);
					db2.log.onReplicationChange = onReplicationChange2;

					await db2.log.replicate({ factor: 1, strict: true });
					await maturedObservedFrom1.promise;
					await maturedObservedFrom2.promise;

					await delay(3e3);
					expect(
						onReplicationChange1.getCalls().map((x) => x.firstArg[0].matured),
					).to.deep.eq([undefined]);
					expect(
						onReplicationChange1
							.getCalls()
							.map((x) => x.firstArg[0].range.hash),
					).to.deep.eq([db2.node.identity.publicKey.hashcode()]);
					expect(
						onReplicationChange2.getCalls().map((x) => x.firstArg[0].matured),
					).to.deep.eq([undefined]);
					expect(
						onReplicationChange2
							.getCalls()
							.map((x) => x.firstArg[0].range.hash),
					).to.deep.eq([db2.node.identity.publicKey.hashcode()]);
				});

				it("will prune on replication fulfillment", async () => {
					let maturityTime = 1e3;
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: false,
							timeUntilRoleMaturity: maturityTime,
							replicas: {
								min: 1,
							},
						},
					});
					await db1.add("0");

					db2 = await session.peers[1].open(db1.clone(), {
						args: {
							replicate: false,
							timeUntilRoleMaturity: maturityTime,
							replicas: {
								min: 1,
							},
						},
					});

					await db2.log.replicate({ factor: 1, strict: true });
					await waitForResolved(() => expect(db1.log.log.length).to.eq(0));
					expect(db2.log.log.length).to.eq(1);
				});
			});
		});

		describe("entry", () => {
			it("entry", async () => {
				const store = new EventStore<string, any>();

				let domain = createReplicationDomainHash("u32");

				const db1 = await session.peers[0].open(store.clone(), {
					args: {
						replicate: false,
						domain,
					},
				});

				const db2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: false,
						domain,
					},
				});

				const checkReplication = async (
					db: EventStore<string, any>,
					entry: Entry<any>,
				) => {
					const offset = await db.log.domain.fromEntry(added.entry);

					const ranges = await db.log.replicationIndex.iterate().all();
					expect(ranges).to.have.length(1);

					const range = ranges[0].value.toReplicationRange();
					expect(range.offset).to.be.closeTo(offset, 0.0001);
					expect(range.factor).to.equal(1); // mininum unit of length
				};

				const checkUnreplication = async (db: EventStore<string, any>) => {
					const ranges = await db.log.replicationIndex.iterate().all();
					expect(ranges).to.have.length(0);
				};

				const added = await db1.add("data", { replicate: true });

				await checkReplication(db1, added.entry);
				await waitForResolved(async () => checkReplication(db2, added.entry));

				await db1.log.unreplicate(added.entry);

				await checkUnreplication(db1);
				await waitForResolved(async () => checkUnreplication(db2));
			});

			it("entry with range", async () => {
				const store = new EventStore<string, any>();

				let domain = createReplicationDomainHash("u32");

				let startFactor = 500000;
				let startOffset = 0;
				const db1 = await session.peers[0].open(store.clone(), {
					args: {
						replicate: {
							factor: startFactor,
							offset: startOffset,
							normalized: false,
						},
						domain,
					},
				});

				const db2 = await session.peers[1].open(store.clone(), {
					args: {
						replicate: false,
						domain,
					},
				});

				const checkReplication = async (
					db: EventStore<string, any>,
					entry: Entry<any>,
				) => {
					const offset = await db.log.domain.fromEntry(added.entry);
					const ranges = await db.log.replicationIndex
						.iterate({ sort: new Sort({ key: ["start1"] }) })
						.all();
					expect(ranges).to.have.length(2);

					const rangeStart = ranges[0].value.toReplicationRange();
					expect(rangeStart.offset).to.be.eq(startOffset);
					expect(rangeStart.factor).to.equal(startFactor);

					const rangeEntry = ranges[1].value.toReplicationRange();
					expect(rangeEntry.offset).to.be.closeTo(offset, 0.0001);
					expect(rangeEntry.factor).to.equal(1); // mininum unit of length
				};

				const checkUnreplication = async (db: EventStore<string, any>) => {
					const ranges = await db.log.replicationIndex
						.iterate({ sort: new Sort({ key: ["start1"] }) })
						.all();
					expect(ranges).to.have.length(1);

					const rangeStart = ranges[0].value.toReplicationRange();
					expect(rangeStart.offset).to.be.eq(startOffset);
					expect(rangeStart.factor).to.equal(startFactor);
				};

				const added = await db1.add("data", { replicate: true });

				await checkReplication(db1, added.entry);
				await waitForResolved(async () => checkReplication(db2, added.entry));

				await db1.log.unreplicate(added.entry);

				await checkUnreplication(db1);
				await waitForResolved(async () => checkUnreplication(db2));
			});
		});
		describe("persistance", () => {
			before(async () => {
				await session.stop();
			});
			beforeEach(async () => {
				let directory = path.join("./tmp", "role", uuid());

				session = await TestSession.disconnected(3, [
					{
						libp2p: {
							privateKey: privateKeyFromRaw(
								new Uint8Array([
									113, 203, 231, 235, 7, 120, 3, 194, 138, 113, 131, 40, 251,
									158, 121, 38, 190, 114, 116, 252, 100, 202, 107, 97, 119, 184,
									24, 56, 27, 76, 150, 62, 132, 22, 246, 177, 200, 6, 179, 117,
									218, 216, 120, 235, 147, 249, 48, 157, 232, 161, 145, 3, 63,
									158, 217, 111, 65, 105, 99, 83, 4, 113, 62, 15,
								]),
							),
						},
						directory: path.join(directory, "0"),
					},
					{
						libp2p: {
							privateKey: privateKeyFromRaw(
								new Uint8Array([
									27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54,
									162, 197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197,
									8, 203, 18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41,
									178, 86, 159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90,
									212, 16, 251, 206, 54, 49, 141, 91, 171,
								]),
							),
						},
						directory: path.join(directory, "1"),
					},

					{
						libp2p: {
							privateKey: privateKeyFromRaw(
								new Uint8Array([
									74, 93, 20, 232, 197, 192, 134, 244, 246, 38, 182, 15, 230,
									234, 217, 208, 198, 116, 172, 72, 115, 255, 45, 169, 152, 186,
									201, 53, 92, 19, 38, 58, 2, 71, 5, 140, 64, 40, 151, 4, 130,
									48, 131, 62, 123, 138, 241, 43, 59, 196, 181, 214, 205, 240,
									100, 152, 182, 122, 244, 49, 134, 190, 116, 106,
								]),
							),
						},
						directory: path.join(directory, "2"),
					},
				]);
				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
				]);
				await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
				await session.peers[2].services.blocks.waitFor(session.peers[1].peerId);
			});

			afterEach(async () => {
				await session.stop();
			});

			it("restart after adding", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							offset: 0.3,
							factor: 0.1,
						},
					},
				});

				await db1.log.replicate({ factor: 0.2, offset: 0.6 });

				const checkSegments = async (db: EventStore<string, any>) => {
					const segments = await db.log.replicationIndex
						.iterate({ sort: [new Sort({ key: "start1" })] })
						.all();
					expect(segments).to.have.length(2);
					expect(segments[0].value.toReplicationRange().factor).to.equal(
						scaleToU64(0.1),
					);
					expect(segments[0].value.toReplicationRange().offset).to.equal(
						scaleToU64(0.3),
					);

					expect(segments[1].value.toReplicationRange().factor).to.equal(
						scaleToU64(0.2),
					);
					expect(segments[1].value.toReplicationRange().offset).to.equal(
						scaleToU64(0.6),
					);
				};
				await checkSegments(db1);

				// true will not start replicating dynamically since we already have a replication segment persisted
				await db1.close();
				db1 = await session.peers[0].open(db1.clone(), {
					args: {
						replicate: true,
					},
				});

				await checkSegments(db1);

				// empty object treated the same way
				await db1.close();
				db1 = await session.peers[0].open(db1.clone(), {
					args: {
						replicate: {},
					},
				});

				// no options treated the same way
				await db1.close();
				db1 = await session.peers[0].open(db1.clone(), {
					args: {},
				});

				await checkSegments(db1);
			});

			it("restart another settings", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							offset: 0.3,
							factor: 0.1,
						},
					},
				});
				await db1.close();

				db1 = await session.peers[0].open(db1.clone(), {
					args: {
						replicate: {
							offset: 0.6,
							factor: 0.2,
						},
					},
				});

				const segments = await db1.log.replicationIndex.iterate().all();
				expect(segments).to.have.length(1);
			});

			describe("pruneOfflineReplicators", () => {
				it("will re-check replication segments on restart and prune offline", async () => {
					// make sure non-reachable peers are not included in the replication segments
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: {
								offset: 0.3,
								factor: 0.1,
							},
						},
					});

					db2 = await session.peers[1].open(db1.clone(), {
						args: {
							replicate: {
								offset: 0.6,
								factor: 0.2,
							},
						},
					});

					await waitForResolved(async () => {
						const segments = await db1.log.replicationIndex.iterate().all();
						expect(segments).to.have.length(2);
						expect(segments.map((x) => x.value.hash)).to.contain(
							db1.node.identity.publicKey.hashcode(),
						);
						expect(segments.map((x) => x.value.hash)).to.contain(
							db2.node.identity.publicKey.hashcode(),
						);
					});

					await db1.close();
					await db2.close();

					/* 				
					await waitForResolved(async () => expect((await db1.node.services.pubsub.getSubscribers(db1.log.rpc.topic))).to.have.length(1))
					 */
					db1 = db1.clone();
					let joinEvents = 0;
					db1.log.events.addEventListener("replicator:join", () => {
						joinEvents++;
					});

					db1 = await session.peers[0].open(db1, {
						args: {
							replicate: {
								offset: 0.6,
								factor: 0.2,
							},
							waitForReplicatorTimeout: 1000,
						},
					});

					await waitForResolved(async () => {
						const segments = await db1.log.replicationIndex.iterate().all();
						expect(segments).to.have.length(1);
						expect(segments[0].value.hash).to.equal(
							db1.node.identity.publicKey.hashcode(),
						);
					});

					expect(joinEvents).to.equal(0);
				});

				it("will re-check replication segments on restart and announce online", async () => {
					// make sure non-reachable peers are not included in the replication segments
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: {
								offset: 0.3,
								factor: 0.1,
							},
						},
					});

					db2 = await session.peers[1].open(db1.clone(), {
						args: {
							replicate: {
								offset: 0.6,
								factor: 0.2,
							},
						},
					});

					await waitForResolved(async () => {
						const segments = await db1.log.replicationIndex.iterate().all();
						expect(segments).to.have.length(2);
						expect(segments.map((x) => x.value.hash)).to.contain(
							db1.node.identity.publicKey.hashcode(),
						);
						expect(segments.map((x) => x.value.hash)).to.contain(
							db2.node.identity.publicKey.hashcode(),
						);
					});

					await db1.close();

					db1 = db1.clone();
					let joinEvents = 0;
					db1.log.events.addEventListener("replicator:join", () => {
						joinEvents++;
					});

					db1 = await session.peers[0].open(db1, {
						args: {
							replicate: {
								offset: 0.6,
								factor: 0.2,
							},
							waitForReplicatorTimeout: 5e3,
						},
					});

					await waitForResolved(() => expect(joinEvents).to.equal(1));
				});

				it("will not throw when closed", async () => {
					session = await TestSession.connected(1);
					const store = new EventStore();
					await session.peers[0].open(store, {
						args: {
							replicate: false,
						},
					});
					await session.peers[0].stop();
					await store.log.pruneOfflineReplicators();
				});
			});

			it("will not be blocked by replicator re-check on start", async () => {
				// make sure non-reachable peers are not included in the replication segments
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							offset: 0.3,
							factor: 0.1,
						},
					},
				});

				db2 = await session.peers[1].open(db1.clone(), {
					args: {
						replicate: {
							offset: 0.6,
							factor: 0.2,
						},
					},
				});

				await waitForResolved(async () => {
					const segments = await db1.log.replicationIndex.iterate().all();
					expect(segments).to.have.length(2);
					expect(segments.map((x) => x.value.hash)).to.contain(
						db1.node.identity.publicKey.hashcode(),
					);
					expect(segments.map((x) => x.value.hash)).to.contain(
						db2.node.identity.publicKey.hashcode(),
					);
				});

				await db1.close();
				await db2.close();

				let t0 = +new Date();
				db1 = await session.peers[0].open(db1.clone(), {
					args: {
						replicate: {
							offset: 0.6,
							factor: 0.2,
						},
						waitForReplicatorTimeout: 1e4, // long wait check
					},
				});

				expect(+new Date() - t0).to.be.lessThan(1e3); // not blocked by waitForReplicatorTimeout
			});

			it("segments updated while offline", async () => {
				// make sure non-reachable peers are not included in the replication segments
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							offset: 0.1,
							factor: 0.1,
						},
					},
				});

				db2 = await session.peers[1].open(db1.clone(), {
					args: {
						replicate: {
							offset: 0.2,
							factor: 0.2,
						},
					},
				});

				await waitForResolved(async () => {
					const segments = await db1.log.replicationIndex.iterate().all();
					expect(segments).to.have.length(2);
					expect(segments.map((x) => x.value.hash)).to.contain(
						db1.node.identity.publicKey.hashcode(),
					);
					expect(segments.map((x) => x.value.hash)).to.contain(
						db2.node.identity.publicKey.hashcode(),
					);
				});

				await db1.close();
				await db2.close();
				await delay(1000);

				db1 = await session.peers[0].open(db1.clone(), {
					args: {
						replicate: {
							offset: 0.3,
							factor: 0.2,
						},
					},
				});

				let joinEvents = 0;
				/* 	let leaveEvents = 0; */

				db1.log.events.addEventListener("replicator:join", () => {
					joinEvents++;
				});

				/* db1.log.events.addEventListener("replicator:leave", () => {
					leaveEvents++;
				}); */

				db2 = await session.peers[1].open(db2.clone(), {
					args: {
						replicate: {
							offset: 0.4,
							factor: 0.2,
						},
					},
				});

				await waitForResolved(async () => {
					const checkSegments = (
						segments: IndexedResults<ReplicationRangeIndexable<any>>,
					) => {
						expect(segments).to.have.length(2);

						expect(segments.map((x) => x.value.hash)).to.contain(
							db1.node.identity.publicKey.hashcode(),
						);
						expect(segments.map((x) => x.value.hash)).to.contain(
							db2.node.identity.publicKey.hashcode(),
						);
						expect(
							segments.map((x) => x.value.toReplicationRange().offset),
						).to.contain(scaleToU64(0.3));
						expect(
							segments.map((x) => x.value.toReplicationRange().offset),
						).to.contain(scaleToU64(0.4));
					};
					checkSegments(await db1.log.replicationIndex.iterate().all());
					checkSegments(await db2.log.replicationIndex.iterate().all());
				});

				await waitForResolved(() => expect(joinEvents).to.equal(1));
				/* expect(leaveEvents).to.equal(0); */ // TODO assert correctly (this assertion is flaky since leave events can happen due to that the goodbye from the pubsub layer is delayed)
			});

			it('replicate "resume"', async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							type: "resume",
							default: {
								offset: 0.3,
								factor: 0.1,
							},
						},
					},
				});

				await db1.log.replicate({ factor: 0.2, offset: 0.6 });
				await db1.close();
				db1 = await session.peers[0].open(db1.clone(), {
					args: {
						replicate: { type: "resume", default: { factor: 0.69 } },
					},
				});

				let segments = await db1.log.replicationIndex.iterate().all();
				expect(segments.map((x) => x.value.widthNormalized)).to.have.members([
					0.1, 0.2,
				]);

				await db1.close();
				db1 = await session.peers[0].open(db1.clone(), {
					args: {
						replicate: false,
					},
				});

				segments = await db1.log.replicationIndex.iterate().all();
				expect(segments).to.have.length(0);
			});
		});
	});
});
/* it("encrypted clock sync write 1 entry replicate false", async () => {
	await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
	const encryptionKey = await client1.keystore.createEd25519Key({
		id: "encryption key",
		group: topic,
	});
	db2 = await client2.open<EventStore<string, any>>(
		await EventStore.load<EventStore<string, any>>(
			client2.libp2p.services.blocks,
			db1.address!
		),
		{ replicate: false }
	);

	await db1.add("hello", {
		receiver: {
			next: encryptionKey.keypair.publicKey,
			meta: encryptionKey.keypair.publicKey,
			payload: encryptionKey.keypair.publicKey,
			signatures: encryptionKey.keypair.publicKey,
		},
	});


	// Now the db2 will request sync clocks even though it does not replicate any content
	await db2.add("world");

	await waitFor(() => db1.store.oplog.length === 2);
	expect(
		db1.store.oplog.toArray().map((x) => x.payload.getValue().value)
	).to.have.members(["hello", "world"]);
	expect(db2.store.oplog.length).equal(1);
}); */
