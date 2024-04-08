import { EventStore } from "./utils/stores/event-store.js";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { Ed25519Keypair, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { Replicator } from "../src/role.js";
import { deserialize } from "@dao-xyz/borsh";
import { slowDownSend } from "./utils.js";
import { ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { expect } from 'chai';

/**
 * TOOD make these test part of ranges.test.ts
 */
describe(`leaders`, function () {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

	const options = {
		args: {
			timeUntilRoleMaturity: 0,
			replicas: {
				min: 1,
				max: 10000
			}
		}
	};
	before(async () => {
		session = await TestSession.connected(3, [
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
			}
		]);
	});

	after(async () => {
		await session.stop();
	});

	beforeEach(async () => { });

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		if (db3) await db3.drop();
	});

	it("select leaders for one or two peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: { ...options.args, role: { type: "replicator", offset: 0, factor: 0.5 } }
		});
		const isLeaderAOneLeader = await db1.log.isLeader(123, 1);
		expect(isLeaderAOneLeader);
		const isLeaderATwoLeader = await db1.log.isLeader(123, 2);
		expect(isLeaderATwoLeader);

		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: { ...options.args, role: { type: "replicator", offset: 0.5, factor: 0.5 } }
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).to.have.length(2)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).to.have.length(2)
		);

		// leader rotation is kind of random, so we do a sequence of tests
		for (let i = 0; i < 3; i++) {

			let slot = (0.1 + i) % 1;

			// One leader
			const isLeaderAOneLeader = await db1.log.isLeader(slot, 1);
			const isLeaderBOneLeader = await db2.log.isLeader(slot, 1);
			expect([isLeaderAOneLeader, isLeaderBOneLeader]).to.have.members([
				false,
				true
			]);

			// Two leaders
			const isLeaderATwoLeaders = await db1.log.isLeader(slot, 2);
			const isLeaderBTwoLeaders = await db2.log.isLeader(slot, 2);


			expect([isLeaderATwoLeaders, isLeaderBTwoLeaders]).to.have.members([
				true,
				true
			]);
		}
	});

	it("leader are selected from 1 replicating peer", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		const store = await new EventStore<string>();
		db1 = await session.peers[0].open(store, {
			args: { role: "observer", ...options.args }
		});
		db2 = (await EventStore.open(
			db1.address!,
			session.peers[1],
			options
		)) as EventStore<string>;

		await delay(5000); // some delay so that if peers are to replicate, they would have had time to notify each other

		// One leader
		const slot = 0;

		// Two leaders, but only one will be leader since only one is replicating
		const isLeaderA = await db1.log.isLeader(slot, 2);
		const isLeaderB = await db2.log.isLeader(slot, 2);

		expect(!isLeaderA); // because replicate is false
		expect(isLeaderB);
	});

	it("leader are selected from 2 replicating peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		const store = await new EventStore<string>();
		db1 = await session.peers[0].open(store, {
			args: { role: "observer", ...options.args }
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: { ...options.args, role: { type: "replicator", factor: 0.5 } }
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: { ...options.args, role: { type: "replicator", factor: 0.5 } }
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).to.have.length(2)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).to.have.length(2)
		);

		// One leader
		const slot = 0;

		// Two leaders, but only one will be leader since only one is replicating
		const isLeaderA = await db1.log.isLeader(slot, 3);
		const isLeaderB = await db2.log.isLeader(slot, 3);
		const isLeaderC = await db3.log.isLeader(slot, 3);

		expect(!isLeaderA); // because replicate is false
		expect(isLeaderB);
		expect(isLeaderC);
	});

	it("select leaders for three peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: {
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: {
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).to.have.length(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).to.have.length(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).to.have.length(3)
		);

		let resolved = 0;
		for (let i = 0; i < 100; i++) {
			try {
				const slot = Math.random();
				const isLeaderAOneLeader = await db1.log.isLeader(slot, 1, {
					roleAge: 0
				});
				const isLeaderBOneLeader = await db2.log.isLeader(slot, 1, {
					roleAge: 0
				});
				const isLeaderCOneLeader = await db3.log.isLeader(slot, 1, {
					roleAge: 0
				});
				expect([
					isLeaderAOneLeader,
					isLeaderBOneLeader,
					isLeaderCOneLeader
				]).include.members([false, false, true]);

				// Two leaders
				const isLeaderATwoLeaders = await db1.log.isLeader(slot, 2, {
					roleAge: 0
				});
				const isLeaderBTwoLeaders = await db2.log.isLeader(slot, 2, {
					roleAge: 0
				});
				const isLeaderCTwoLeaders = await db3.log.isLeader(slot, 2, {
					roleAge: 0
				});
				expect([
					isLeaderATwoLeaders,
					isLeaderBTwoLeaders,
					isLeaderCTwoLeaders
				]).include.members([false, true, true]);

				// Three leders
				const isLeaderAThreeLeaders = await db1.log.isLeader(slot, 3, {
					roleAge: 0
				});
				const isLeaderBThreeLeaders = await db2.log.isLeader(slot, 3, {
					roleAge: 0
				});
				const isLeaderCThreeLeaders = await db3.log.isLeader(slot, 3, {
					roleAge: 0
				});
				expect([
					isLeaderAThreeLeaders,
					isLeaderBThreeLeaders,
					isLeaderCThreeLeaders
				]).include.members([true, true, true]);
				resolved += 1;
			} catch (error) { }
		}
		// since the distribution only in best scenarios distributes perfectly
		// we might have duplication, i.e. more than expected amount of leaders for a particular
		// slot
		expect(resolved).greaterThan(40);
	});

	it("evenly distributed", async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
		db2 = (await EventStore.open(
			db1.address!,
			session.peers[1],
			options
		)) as EventStore<string>;
		db3 = (await EventStore.open(
			db1.address!,
			session.peers[2],
			options
		)) as EventStore<string>;

		await waitForResolved(() =>
			expect(Math.abs((db1.log.role as Replicator).factor - 0.33)).lessThan(
				0.02
			)
		);
		await waitForResolved(() =>
			expect(Math.abs((db2.log.role as Replicator).factor - 0.33)).lessThan(
				0.02
			)
		);
		await waitForResolved(() =>
			expect(Math.abs((db3.log.role as Replicator).factor - 0.33)).lessThan(
				0.02
			)
		);

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).to.have.length(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).to.have.length(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).to.have.length(3)
		);

		let a = 0,
			b = 0,
			c = 0;
		const count = 10000;
		for (let i = 0; i < count; i++) {
			a += (await db1.log.isLeader(String(i), 2, { roleAge: 0 })) ? 1 : 0;
			b += (await db2.log.isLeader(String(i), 2, { roleAge: 0 })) ? 1 : 0;
			c += (await db3.log.isLeader(String(i), 2, { roleAge: 0 })) ? 1 : 0;
		}

		const from = count * 0.5;
		const to = count * 0.95;
		expect(a).greaterThan(from);
		expect(a).lessThan(to);
		expect(b).greaterThan(from);
		expect(b).lessThan(to);
		expect(c).greaterThan(from);
		expect(c).lessThan(to);
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
				expect(db1.log.getReplicatorsSorted()?.length).equal(2);
			});

			await waitForResolved(() => {
				expect(db2.log.getReplicatorsSorted()?.length).equal(2);
			});

			// expect either db1 to replicate more than 50% or db2 to replicate more than 50%
			// for these
			expect(db1.log.getReplicatorUnion(0)).to.deep.equal([
				session.peers[0].identity.publicKey.hashcode()
			]);

			expect(db2.log.getReplicatorUnion(0)).to.deep.equal([
				session.peers[1].identity.publicKey.hashcode()
			]);
		});

		it("will consider in flight", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store.clone(), {
				args: {
					role: {
						type: "replicator",
						factor: 0.5
					},
					replicas: {
						min: 2
					},
					timeUntilRoleMaturity: 1e4
				}
			});

			const abortController = new AbortController();
			await db1.add("hello!");

			slowDownSend(db1.log, ExchangeHeadsMessage, 1e5, abortController.signal);

			db2 = await session.peers[1].open(store, {
				args: {
					role: {
						type: "replicator",
						factor: 0.5
					},
					replicas: {
						min: 2
					},
					timeUntilRoleMaturity: 1e4
				}
			});

			await waitForResolved(() => {
				expect(db1.log.getReplicatorsSorted()?.length).equal(2);
			});

			await waitForResolved(() => {
				expect(db2.log.getReplicatorsSorted()?.length).equal(2);
			});

			await waitForResolved(() =>
				expect(
					db2.log["syncInFlight"].has(db1.node.identity.publicKey.hashcode())
				).to.be.true
			);

			// expect either db1 to replicate more than 50% or db2 to replicate more than 50%
			// for these
			expect(db2.log.getReplicatorUnion(0)).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode()
			]);

			abortController.abort("Start sending now");
			await waitForResolved(() => {
				expect(
					db2.log["syncInFlight"].has(db1.node.identity.publicKey.hashcode())
				).to.be.false
			})

			// no more inflight
			expect(db2.log.getReplicatorUnion(0)).to.deep.equal([
				session.peers[1].identity.publicKey.hashcode()
			]);
		});

		it("sets replicators groups correctly", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store, {
				args: {
					replicas: {
						min: 1
					},
					role: {
						type: "replicator",
						factor: 0.34
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
						},
						role: {
							type: "replicator",
							factor: 0.34
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
							factor: 0.34
						}
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).to.have.length(3)
			);
			for (let i = 1; i <= 3; i++) {
				db1.log.replicas.min = { getValue: () => i };

				// min replicas 3 only need to query 1 (every one have all the data)
				// min replicas 2 only need to query 2
				// min replicas 1 only need to query 3 (data could end up at any of the 3 nodes)
				expect(db1.log.getReplicatorUnion(0)).to.have.length(3 - i + 1);
			}
		});

		it("all non-mature, only me included", async () => {
			const store = new EventStore<string>();

			db1 = await session.peers[0].open(store, {
				args: {
					replicas: {
						min: 1
					},
					role: {
						type: "replicator",
						factor: 0.34
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
						},
						role: {
							type: "replicator",
							factor: 0.34
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
							factor: 0.34
						}
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).to.have.length(3)
			);

			for (let i = 3; i <= 3; i++) {
				db3.log.replicas.min = { getValue: () => i };

				// Should always include all nodes since no is mature
				expect(
					db3.log.getReplicatorUnion(Number.MAX_SAFE_INTEGER)
				).to.have.length(1);
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
						factor: 0.34
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
							factor: 0.34
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
							factor: 0.34
						}
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).to.have.length(3)
			);

			// TODO not sure if db2 results should be included here
			// db2 is not mature from db3 perspective (?). Might be (?)
			// this test is kind of pointless anyway since we got the range.test.ts that tests all the cases

			for (let i = 1; i < 3; i++) {
				db3.log.replicas.min = { getValue: () => i };

				// Should always include all nodes since no is mature
				expect(db3.log.getReplicatorUnion(MATURE_TIME)).to.have.length(3);
			}

			await delay(MATURE_TIME);

			for (let i = 1; i <= 3; i++) {
				db3.log.replicas.min = { getValue: () => i };

				// all is matured now
				expect(db3.log.getReplicatorUnion(MATURE_TIME)).to.have.length(3 - i + 1); // since I am replicating with factor 1 and is mature
			}
		});
	});

	describe("balance", () => {
		it("small fractions means little replication", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), {
				args: {
					role: { type: "replicator", factor: 0.05 } // cover 5%
				}
			});

			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						role: { type: "replicator", factor: 0.48 } // cover 48%  appx 10 times more)
					}
				}
			);

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).to.have.length(2)
			);

			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).to.have.length(2)
			);

			let a = 0,
				b = 0;
			const count = 10000;

			// expect db1 and db2 segments to not overlap for this test asserts to work out well
			expect(
				(db1.log.role as Replicator).segments[0].overlaps(
					(db2.log.role as Replicator).segments[0]
				)
			).to.be.false;
			expect(
				(db2.log.role as Replicator).segments[0].overlaps(
					(db1.log.role as Replicator).segments[0]
				)
			).to.be.false;

			for (let i = 0; i < count; i++) {
				a += (await db1.log.isLeader(String(i), 1, { roleAge: 0 })) ? 1 : 0;
				b += (await db2.log.isLeader(String(i), 1, { roleAge: 0 })) ? 1 : 0;
			}

			expect(a + b).equal(count);

			// TODO choose factors so this becomes predicatable. i.e. hardcode offsets we can maximize factors without overlap
			expect(a / count).greaterThan(0.04);
			expect(a / count).lessThan(0.06);
			expect(b / count).greaterThan(0.94);
			expect(b / count).lessThan(0.96);
		});
	});

	it("leader always defined", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
			args: {
				role: {
					...options.args,
					type: "replicator",
					factor: 0.3333
				}
			}
		});
		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: {
				...options.args,
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;
		db3 = (await EventStore.open(db1.address!, session.peers[2], {
			args: {
				...options.args,
				role: {
					type: "replicator",
					factor: 0.3333
				}
			}
		})) as EventStore<string>;

		await waitForResolved(() =>
			expect(db1.log.getReplicatorsSorted()).to.have.length(3)
		);
		await waitForResolved(() =>
			expect(db2.log.getReplicatorsSorted()).to.have.length(3)
		);
		await waitForResolved(() =>
			expect(db3.log.getReplicatorsSorted()).to.have.length(3)
		);

		for (let i = 0; i < 100; i++) {
			const leaders: Set<string | undefined> = new Set(
				await db1.log.findLeaders(String(i), 3, { roleAge: 0 })
			);
			expect(leaders.has(undefined)).to.be.false;
			expect(leaders.size).equal(3);
		}
	});

	describe("get replicators sorted", () => {
		const checkSorted = (values: { role: { offset: number } }[]) => {
			const sorted = [...values].sort((a, b) => a.role.offset - b.role.offset);
			expect(sorted).to.deep.equal(values);
		};
		it("can handle peers leaving and joining", async () => {
			db1 = await session.peers[0].open(new EventStore<string>(), options);
			db2 = (await EventStore.open(
				db1.address!,
				session.peers[1],
				options
			)) as EventStore<string>;

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).to.have.length(2)
			);

			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).to.have.length(2)
			);

			db3 = (await EventStore.open(
				db1.address!,
				session.peers[2],
				options
			)) as EventStore<string>;

			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).to.have.length(3)
			);

			await db2.close();

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).to.have.length(2)
			);

			await waitForResolved(() =>
				expect(
					db1.log
						.getReplicatorsSorted()
						?.toArray()
						?.map((x) => x.publicKey.hashcode())
				).to.have.members([
					getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
					getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
				])
			);

			await waitForResolved(() =>
				expect(
					db3.log
						.getReplicatorsSorted()
						?.toArray()
						?.map((x) => x.publicKey.hashcode())
				).to.have.members([
					getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
					getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
				])
			);

			expect(db2.log.getReplicatorsSorted()).equal(undefined);

			db2 = (await EventStore.open(
				db1.address!,
				session.peers[1],
				options
			)) as EventStore<string>;

			await waitForResolved(() =>
				expect(db1.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db2.log.getReplicatorsSorted()).to.have.length(3)
			);
			await waitForResolved(() =>
				expect(db3.log.getReplicatorsSorted()).to.have.length(3)
			);

			expect(
				db1.log
					.getReplicatorsSorted()
					?.toArray()
					?.map((x) => x.publicKey.hashcode())
			).to.have.members([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);
			expect(
				db2.log
					.getReplicatorsSorted()
					?.toArray()
					?.map((x) => x.publicKey.hashcode())
			).to.have.members([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);
			expect(
				db3.log
					.getReplicatorsSorted()
					?.toArray()
					?.map((x) => x.publicKey.hashcode())
			).to.have.members([
				getPublicKeyFromPeerId(session.peers[0].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[1].peerId).hashcode(),
				getPublicKeyFromPeerId(session.peers[2].peerId).hashcode()
			]);

			checkSorted(db1.log.getReplicatorsSorted()!.toArray());
			checkSorted(db2.log.getReplicatorsSorted()!.toArray());
			checkSorted(db3.log.getReplicatorsSorted()!.toArray());
		});
	});
});
