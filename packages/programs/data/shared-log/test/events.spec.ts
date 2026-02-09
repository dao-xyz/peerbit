import { randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import { EventStore } from "./utils/stores/index.js";

describe("events", () => {
	let session: TestSession;

	afterEach(async () => {
		await session.stop();
	});

	it("replicator:(join|leave)", async () => {
		// Joining now includes a targeted replication-info handshake which can race on
		// slower CI machines. Use waitForResolved instead of a fixed delay.
		session = await TestSession.connected(2);

		let db1JoinEvents: string[] = [];
		let db1LeaveEvents: string[] = [];

		const db1a = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});
		db1a.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		db1a.log.events.addEventListener("replicator:leave", (event) => {
			db1LeaveEvents.push(event.detail.publicKey.hashcode());
		});

		const db1b = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});

		const db2a = await session.peers[1].open(db1a.clone(), {
			args: { replicate: 0.6 },
		});

		const db2b = await session.peers[1].open(db1b.clone(), {
			args: { replicate: 0.4 },
		});
		await waitForResolved(
			() =>
				expect(db1JoinEvents).to.have.members([
					session.peers[1].identity.publicKey.hashcode(),
				]),
			{ timeout: 20_000 },
		);

		await db2a.close();
		await db2b.close();

		// try open another db and make sure it does not trigger join event to db1
		await waitForResolved(
			() =>
				expect(db1LeaveEvents).to.have.members([
					session.peers[1].identity.publicKey.hashcode(),
				]),
			{ timeout: 20_000 },
		);
		expect(db1JoinEvents).to.have.length(1); // no new join event
	});

	it("cleans prune response tracking on unsubscribe", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});

		const disconnectedPublicKey = session.peers[1].identity.publicKey;
		const disconnectedPeerHash = disconnectedPublicKey.hashcode();
		const entryHash = uuid();

		const responseMap = (db1.log as any)[
			"_requestIPruneResponseReplicatorSet"
		] as Map<string, Set<string>>;
		responseMap.set(entryHash, new Set([disconnectedPeerHash]));

		await db1.log.handleSubscriptionChange(
			disconnectedPublicKey,
			[db1.log.topic],
			false,
		);

		expect(responseMap.get(entryHash)).to.be.undefined;
	});

	it("replicate:join not emitted on update", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
			},
		});
		store1.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		await store2.log.replicate({ factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});

		expect(db1JoinEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]); // no new join events
	});

	it("replicator:mature not emitted more than once on update same same range id", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		let timeUntilRoleMaturity = 1e3;
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		store1.log.events.addEventListener("replicator:mature", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		let rangeId = randomBytes(32);
		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { id: rangeId, factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		// reset: true means we will re-initalize hence we expect a maturity event
		await store2.log.replicate({ id: rangeId, factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});
		expect(store.log.pendingMaturity.size).to.be.eq(0);

		await delay(timeUntilRoleMaturity * 2); // wait a little bit more
		expect(db1JoinEvents).to.have.members([
			session.peers[0].identity.publicKey.hashcode(),
			session.peers[1].identity.publicKey.hashcode(),
		]); // no new join events

		expect(store.log.pendingMaturity.size).to.eq(0);
	});

	it("replicator:mature emit twice on update reset", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		let timeUntilRoleMaturity = 1e3;
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		store1.log.events.addEventListener("replicator:mature", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		// reset: true means we will re-initalize hence we expect a maturity event
		await store2.log.replicate({ factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});
		expect(store.log.pendingMaturity.size).to.be.greaterThan(0);

		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		expect(store.log.pendingMaturity.size).to.eq(0);
	});

	describe("waitForReplicators", async () => {
		it("resolves immediately is offline and replicating and mature", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity: 0,
				},
			});
			let timeout = 1e4;
			let t0 = Date.now();
			await store1.log.waitForReplicators({
				timeout,
			});
			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThan(1e3); // "immediately"
		});

		it("times out after mature when is offline and replicating and unmature", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();

			let t0 = Date.now();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity: 6e3, // > 3e3
				},
			});

			let timeout = 1e4;

			await store1.log.waitForReplicators({
				timeout,
			});

			let t1 = Date.now();
			expect(t1 - t0).to.be.greaterThan(3e3); // should wait for maturity
		});

		it("resolves when replication starts after wait is pending", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 1e3,
				},
			});

			const waitPromise = store1.log.waitForReplicators({
				timeout: 10e3,
				waitForNewPeers: true,
			});

			await delay(100);
			await store1.log.replicate({ factor: 1 });

			await waitPromise;
		});

		it("resolves even if maturity timers are cleared", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const timeUntilRoleMaturity = 3e3;
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity,
				},
			});

			const hash = session.peers[0].identity.publicKey.hashcode();
			// @ts-ignore accessing internal state for test purposes
			const pending = store1.log.pendingMaturity.get(hash);
			expect(pending, "expected pending maturity timers").to.exist;
			if (pending) {
				for (const [_key, value] of pending) {
					clearTimeout(value.timeout);
				}
				pending.clear();
			}

			await store1.log.waitForReplicators({
				timeout: 10e3,
			});
		});

		it("times out after timeout if online", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: false,
				},
			});

			const store2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
				},
			});

			await store1.log.waitFor(store2.log.node.identity.publicKey);

			let timeout = 3e3;
			let t0 = Date.now();
			await expect(
				store1.log.waitForReplicators({
					timeout,
				}),
			).to.be.eventually.rejectedWith("Timeout");
			let t1 = Date.now();
			expect(t1 - t0).to.be.greaterThanOrEqual(timeout);
		});

		it("will wait for role age", async () => {
			session = await TestSession.connected(1);

			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
				},
			});

			let waitForRoleAge = 2e3;
			let t0 = Date.now();
			await store1.log.waitForReplicators({
				roleAge: waitForRoleAge,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});
			let t1 = Date.now();
			// Allow some timer jitter across environments/CI
			expect(t1 - t0).to.be.greaterThanOrEqual(waitForRoleAge - 250);
		});

		it("will wait for warmup when restarting", async () => {
			session = await TestSession.connected(1, {
				directory:
					"./tmp/shared-log/waitForReplicators/wait-for-warmup/" + uuid(),
			});

			const store = new EventStore();
			let store1 = await session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
				},
			});

			await delay(3e3);
			await store1.close();
			store1 = await session.peers[0].open(store1, {
				args: {
					replicate: {
						type: "resume",
						default: {
							factor: 0.5,
						},
					},
				},
			});

			let waitForRoleAge = 3e3;
			let t0 = Date.now();
			await store1.log.waitForReplicators({
				roleAge: waitForRoleAge,
				timeout: 1e4,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});
			let t1 = Date.now();
			// Allow some timer jitter across environments/CI
			expect(t1 - t0).to.be.greaterThanOrEqual(waitForRoleAge - 250);
			expect(t1 - t0).to.be.lessThan(waitForRoleAge + 3e3);
		});

		it("will wait joining replicator role age", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			await session.peers[1].open(store.clone(), {
				args: {
					replicate: { factor: 1 },
				},
			});

			await delay(3e3);

			const store2 = await session.peers[0].open(store, {
				args: {
					replicate: false,
				},
			});

			let waitForRoleAge = 3e3;
			let t0 = Date.now();
			await store2.log.waitForReplicators({
				roleAge: waitForRoleAge,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});

			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThanOrEqual(waitForRoleAge); // because store1
		});
	});
});
