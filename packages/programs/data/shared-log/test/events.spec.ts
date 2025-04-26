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
		await delay(2e3); // some time for all join events to emit
		expect(db1JoinEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]);

		await db2a.close();
		await db2b.close();

		// try open another db and make sure it does not trigger join event to db1
		await delay(2e3); // some time for all leave events to emit
		expect(db1LeaveEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]);
		expect(db1JoinEvents).to.have.length(1); // no new join event
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
		it("times out", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: false,
				},
			});
			let timeout = 1e3;
			let t0 = Date.now();
			await expect(
				store1.log.waitForReplicators({
					timeout,
				}),
			).to.be.eventually.rejectedWith("Timeout");
			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThan(timeout + 1e3); // + extra time
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
			});
			let t1 = Date.now();
			expect(t1 - t0).to.be.greaterThan(waitForRoleAge);
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
			});
			let t1 = Date.now();
			expect(t1 - t0).to.be.greaterThan(waitForRoleAge - 1);
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
			});

			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThanOrEqual(waitForRoleAge); // because store1
		});
	});
});
