// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import {
	RequestMaybeSync,
	RequestMaybeSyncCoordinate,
	SimpleSyncronizer,
} from "../src/sync/simple.js";
import { slowDownMessage } from "./utils.js";
import { EventStore } from "./utils/stores/index.js";

describe("lifecycle", () => {
	let session: TestSession;

	afterEach(async () => {
		await session.stop();
	});

	describe("close", () => {
		it("will close all indices", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);
			const stopEntryCoordinatesIndex = sinon.spy(
				db.log.entryCoordinatesIndex,
				"stop",
			);
			const stopReplicationIndex = sinon.spy(db.log.replicationIndex, "stop");
			const closeLog = sinon.spy(db.log, "close");
			await db.close();
			expect(stopEntryCoordinatesIndex.called).to.be.true;
			expect(stopReplicationIndex.called).to.be.true;
			expect(closeLog.called).to.be.true;
		});

		it("closing does not affect other instances", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const db2 = await session.peers[0].open(new EventStore());
			await db2.add("hello");

			await db.close();
			expect((await db2.iterator({ limit: -1 })).collect()).to.have.length(1);
		});
	});
	describe("drop", () => {
		it("will drop all data", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);
			await db.log.replicate({ factor: 0.3, offset: 0.3 });
			await db.log.replicate({ factor: 0.6, offset: 0.6 });
			await db.add("hello");
			await db.drop();

			const reopen = await session.peers[0].open(store);
			expect((await reopen.iterator({ limit: -1 })).collect()).to.have.length(
				0,
			);
			expect(await reopen.log.entryCoordinatesIndex.count()).to.equal(0);
			expect(await reopen.log.replicationIndex.count()).to.equal(1);
		});

		it("open cloned after dropped", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);
			await db.add("hello");
			await db.drop();

			const reopen = await session.peers[0].open(store.clone());
			expect((await reopen.iterator({ limit: -1 })).collect()).to.have.length(
				0,
			);
		});
	});

	describe("replicators", () => {
		it("uses existing subsription", async () => {
			session = await TestSession.connected(2);

			const store = new EventStore();
			const db1 = await session.peers[0].open(store);
			await session.peers[1].services.pubsub.requestSubscribers(db1.log.topic);
			await waitForResolved(async () =>
				expect(
					(await session.peers[1].services.pubsub.getSubscribers(
						db1.log.topic,
					))!.find((x) => x.equals(session.peers[0].identity.publicKey)),
				),
			);

			// Adding a delay is necessary so that old subscription messages are not flowing around
			// so that we are sure the we are "really" using existing subscriptions on start to build replicator set
			await delay(1000);

			const db2 = await session.peers[1].open(store.clone());
			await waitForResolved(async () =>
				expect([...(await db1.log.getReplicators())]).to.have.members(
					session.peers.map((x) => x.identity.publicKey.hashcode()),
				),
			);
			await waitForResolved(async () =>
				expect([...(await db2.log.getReplicators())]).to.have.members(
					session.peers.map((x) => x.identity.publicKey.hashcode()),
				),
			);
		});

		it("clears in flight info when leaving", async () => {
			const store = new EventStore<string, any>();

			session = await TestSession.connected(3);

			const db1 = await session.peers[0].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
					replicas: {
						min: 3,
					},
					setup: {
						syncronizer: SimpleSyncronizer,
						domain: createReplicationDomainHash("u32"),
						type: "u32",
						name: "u32-simple",
					},
				},
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
					replicas: {
						min: 3,
					},
					setup: {
						syncronizer: SimpleSyncronizer,
						domain: createReplicationDomainHash("u32"),
						type: "u32",
						name: "u32-simple",
					},
				},
			});

			const abortController = new AbortController();
			const { entry } = await db1.add("hello!");
			await waitForResolved(() => expect(db2.log.log.length).equal(1));

			slowDownMessage(
				db1.log,
				ExchangeHeadsMessage,
				1e4,
				abortController.signal,
			);
			slowDownMessage(
				db2.log,
				ExchangeHeadsMessage,
				1e4,
				abortController.signal,
			);
			slowDownMessage(db2.log, RequestMaybeSync, 2e3, abortController.signal); // make db2 a bit slower so the assertions below become deterministic (easily)
			slowDownMessage(
				db2.log,
				RequestMaybeSyncCoordinate,
				2e3,
				abortController.signal,
			); // make db2 a bit slower so the assertions below become deterministic (easily)

			const db3 = await session.peers[2].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					replicas: {
						min: 3,
					},
					setup: {
						syncronizer: SimpleSyncronizer,
						domain: createReplicationDomainHash("u32"),
						type: "u32",
						name: "u32-simple",
					},
				},
			});

			await waitForResolved(async () => {
				expect((await db3.log.getReplicators()).size).equal(3);
			});

			await waitForResolved(
				() =>
					expect(
						db3.log.syncronizer.syncInFlight.has(
							db1.node.identity.publicKey.hashcode(),
						),
					).to.be.true,
			);
			await waitForResolved(
				() =>
					expect(
						!!(db3.log.syncronizer as SimpleSyncronizer<any>)[
							"syncInFlightQueue"
						]
							.get(entry.hash)
							?.find((x) => x.equals(db2.node.identity.publicKey)),
					).to.be.true,
			);
			await waitForResolved(
				() =>
					expect(
						(db3.log.syncronizer as SimpleSyncronizer<any>)[
							"syncInFlightQueueInverted"
						].has(db2.node.identity.publicKey.hashcode()),
					).to.be.true,
			); // because db2 is slower
			await waitForResolved(
				() =>
					expect(
						(db3.log.syncronizer as SimpleSyncronizer<any>)[
							"syncInFlightQueueInverted"
						].has(db1.node.identity.publicKey.hashcode()),
					).to.be.false,
			);

			await db1.close();
			await db2.close();

			await waitForResolved(
				() =>
					expect(
						db3.log.syncronizer.syncInFlight.has(
							db1.node.identity.publicKey.hashcode(),
						),
					).to.be.false,
			);
			await waitForResolved(
				() =>
					expect(
						(db3.log.syncronizer as SimpleSyncronizer<any>)[
							"syncInFlightQueue"
						].has(entry.hash),
					).to.be.false,
			);
			await waitForResolved(
				() =>
					expect(
						(db3.log.syncronizer as SimpleSyncronizer<any>)[
							"syncInFlightQueueInverted"
						].has(db2.node.identity.publicKey.hashcode()),
					).to.be.false,
			);

			abortController.abort("Done");
		});
	});
});
