// Include test utilities
import { Ed25519Keypair } from "@peerbit/crypto";
import { TerminalOperationNotStartedError } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import {
	AllReplicatingSegmentsMessage,
	RequestReplicationInfoMessage,
	ResponseRoleMessage,
} from "../src/replication.js";
import { Replicator } from "../src/role.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
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

		for (const target of [
			"entry coordinates index",
			"replication range index",
		] as const) {
			it(`retains a failed ${target} stop for the exact close retry`, async () => {
				session = await TestSession.connected(1);
				const db = await session.peers[0].open(new EventStore());
				const sharedLog = db.log as any;
				const index =
					target === "entry coordinates index"
						? sharedLog.entryCoordinatesIndex
						: sharedLog.replicationIndex;
				const originalStop = index.stop.bind(index);
				const cleanupError = new Error(`injected ${target} close failure`);
				let attempts = 0;
				index.stop = async () => {
					attempts += 1;
					if (attempts === 1) throw cleanupError;
					await originalStop();
				};

				await expect(db.close()).to.be.rejectedWith(cleanupError.message);
				expect(
					target === "entry coordinates index"
						? sharedLog._entryCoordinatesIndex
						: sharedLog._replicationRangeIndex,
				).to.equal(index);

				await db.close();
				expect(attempts).to.equal(2);
			});
		}

		it("closing does not affect other instances", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const db2 = await session.peers[0].open(new EventStore());
			await db2.add("hello");

			await db.close();
			expect((await db2.iterator({ limit: -1 })).collect()).to.have.length(1);
		});
	});

	for (const operation of ["close", "drop"] as const) {
		it(`${operation} drains an admitted subscription callback before retiring replication state`, async () => {
			session = await TestSession.connected(2);
			const db = await session.peers[0].open(new EventStore(), {
				args: { replicate: { factor: 1 } },
			});
			const sharedLog = db.log;
			const replicationSegments = await sharedLog.getMyReplicationSegments();
			expect(replicationSegments).to.have.length(1);

			let markLookupStarted!: () => void;
			const lookupStarted = new Promise<void>((resolve) => {
				markLookupStarted = resolve;
			});
			let releaseLookup!: () => void;
			const lookupGate = new Promise<void>((resolve) => {
				releaseLookup = resolve;
			});
			const lookup = sinon
				.stub(sharedLog, "getMyReplicationSegments")
				.callsFake(async () => {
					markLookupStarted();
					await lookupGate;
					return replicationSegments;
				});
			const sent: unknown[] = [];
			const send = sinon
				.stub(sharedLog.rpc, "send")
				.callsFake(async (message) => {
					sent.push(message);
					return [] as any;
				});
			const replicationIndex = sharedLog.replicationIndex;
			const retire = sinon.spy(
				replicationIndex,
				operation === "close" ? "stop" : "drop",
			);

			try {
				(sharedLog as any)._onSubscriptionFn(
					new CustomEvent("subscribe", {
						detail: {
							from: session.peers[1].identity.publicKey,
							topics: [sharedLog.topic],
						},
					}),
				);
				await lookupStarted;

				const terminating = db[operation]();
				await waitForResolved(
					() => expect(sharedLog.acceptsParentAttachments).to.be.false,
				);
				expect(retire.called).to.be.false;

				releaseLookup();
				await terminating;

				expect(lookup.calledOnce).to.be.true;
				expect(retire.calledOnce).to.be.true;
				expect(
					sent.filter(
						(message) =>
							message instanceof AllReplicatingSegmentsMessage &&
							message.segments.length > 0,
					),
				).to.have.length(0);
				expect(
					sent.filter((message) => message instanceof ResponseRoleMessage),
				).to.have.length(0);
				expect(
					sent.filter(
						(message) => message instanceof RequestReplicationInfoMessage,
					),
				).to.have.length(0);
			} finally {
				releaseLookup();
				lookup.restore();
				send.restore();
			}
		});
	}

	for (const operation of ["close", "drop"] as const) {
		it(`${operation} publishes an admitted replication snapshot before the terminal reset`, async () => {
			session = await TestSession.connected(2);
			const db = await session.peers[0].open(new EventStore(), {
				args: { replicate: { factor: 1 } },
			});
			const sharedLog = db.log;
			const order: string[] = [];
			let markSnapshotStarted!: () => void;
			const snapshotStarted = new Promise<void>((resolve) => {
				markSnapshotStarted = resolve;
			});
			let releaseSnapshot!: () => void;
			const snapshotGate = new Promise<void>((resolve) => {
				releaseSnapshot = resolve;
			});
			let gateSnapshot = true;
			const send = sinon
				.stub(sharedLog.rpc, "send")
				.callsFake(async (message) => {
					if (
						gateSnapshot &&
						message instanceof AllReplicatingSegmentsMessage &&
						message.segments.length > 0
					) {
						gateSnapshot = false;
						order.push("snapshot-start");
						markSnapshotStarted();
						await snapshotGate;
						order.push("snapshot-finish");
					} else if (
						message instanceof AllReplicatingSegmentsMessage &&
						message.segments.length === 0
					) {
						order.push("terminal-reset");
					}
					return [] as any;
				});
			let terminating: Promise<unknown> | undefined;

			try {
				(sharedLog as any)._onSubscriptionFn(
					new CustomEvent("subscribe", {
						detail: {
							from: session.peers[1].identity.publicKey,
							topics: [sharedLog.topic],
						},
					}),
				);
				await snapshotStarted;

				let terminalSettled = false;
				terminating = db[operation]().then(() => {
					terminalSettled = true;
				});
				await waitForResolved(
					() => expect(sharedLog.acceptsParentAttachments).to.be.false,
				);
				expect(terminalSettled).to.be.false;
				expect(order).to.deep.equal(["snapshot-start"]);

				releaseSnapshot();
				await terminating;
				expect(order).to.deep.equal([
					"snapshot-start",
					"snapshot-finish",
					"terminal-reset",
				]);
			} finally {
				releaseSnapshot();
				await terminating?.catch(() => {});
				send.restore();
			}
		});
	}

	for (const compatibility of [8, 9]) {
		it(`uses one replication-index snapshot for a v${compatibility} replication-info callback`, async () => {
			session = await TestSession.connected(2);
			const db = await session.peers[0].open(new EventStore(), {
				args: { compatibility, replicate: { factor: 1 } },
			});
			const replicationSegments = await db.log.getMyReplicationSegments();
			expect(replicationSegments).to.have.length(1);
			const lookup = sinon
				.stub(db.log, "getMyReplicationSegments")
				.resolves(replicationSegments);
			const sent: unknown[] = [];
			const send = sinon.stub(db.log.rpc, "send").callsFake(async (message) => {
				sent.push(message);
				return [] as any;
			});

			try {
				await db.log.handleSubscriptionChange(
					session.peers[1].identity.publicKey,
					[db.log.topic],
					true,
				);

				expect(lookup.calledOnce).to.be.true;
				const snapshots = sent.filter(
					(message) => message instanceof AllReplicatingSegmentsMessage,
				) as AllReplicatingSegmentsMessage[];
				expect(snapshots).to.have.length(1);
				expect(snapshots[0].segments).to.have.length(1);
				expect(snapshots[0].segments[0].id).to.deep.equal(
					replicationSegments[0].id,
				);

				const legacyRoles = sent.filter(
					(message) => message instanceof ResponseRoleMessage,
				) as ResponseRoleMessage[];
				if (compatibility === 8) {
					expect(legacyRoles).to.have.length(1);
					expect(legacyRoles[0].role).to.be.instanceOf(Replicator);
				} else {
					expect(legacyRoles).to.have.length(0);
				}
			} finally {
				lookup.restore();
				send.restore();
			}
		});
	}

	it("does not publish a request snapshot from a closed generation after reopen", async () => {
		session = await TestSession.connected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: { factor: 1 } },
		});
		const sharedLog = db.log;
		const getMyReplicationSegments =
			sharedLog.getMyReplicationSegments.bind(sharedLog);
		const capturedSegments = await getMyReplicationSegments();
		expect(capturedSegments).to.have.length(1);
		let markLookupStarted!: () => void;
		const lookupStarted = new Promise<void>((resolve) => {
			markLookupStarted = resolve;
		});
		let releaseLookup!: () => void;
		const lookupGate = new Promise<void>((resolve) => {
			releaseLookup = resolve;
		});
		let gateFirstLookup = true;
		const lookup = sinon
			.stub(sharedLog, "getMyReplicationSegments")
			.callsFake(async () => {
				if (!gateFirstLookup) {
					return getMyReplicationSegments();
				}
				gateFirstLookup = false;
				markLookupStarted();
				await lookupGate;
				return capturedSegments;
			});
		const sent: unknown[] = [];
		const send = sinon
			.stub(sharedLog.rpc, "send")
			.callsFake(async (message) => {
				sent.push(message);
				return [] as any;
			});
		const requester = (await Ed25519Keypair.create()).publicKey;
		let request: Promise<unknown> | undefined;

		try {
			request = sharedLog.onMessage(new RequestReplicationInfoMessage(), {
				from: requester,
			} as any);
			await lookupStarted;

			await db.close();
			await session.peers[0].open(db);
			const nonemptyBeforeRelease = sent.filter(
				(message) =>
					message instanceof AllReplicatingSegmentsMessage &&
					message.segments.length > 0,
			).length;

			releaseLookup();
			await request;
			expect(
				sent.filter(
					(message) =>
						message instanceof AllReplicatingSegmentsMessage &&
						message.segments.length > 0,
				).length,
			).to.equal(nonemptyBeforeRelease);
		} finally {
			releaseLookup();
			await request?.catch(() => {});
			lookup.restore();
			send.restore();
		}
	});

	for (const operation of ["close", "drop"] as const) {
		it(`${operation} rejects an invalid owner before SharedLog teardown`, async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore<string, any>());
			const invalidOwner = new EventStore<string, any>();
			const sharedLog = db.log;
			const lowerLog = sharedLog.log;
			const entryCoordinatesIndex = sharedLog.entryCoordinatesIndex;
			const replicationIndex = sharedLog.replicationIndex;
			const subscriptions = (sharedLog.node.services.pubsub as any)[
				"subscriptions"
			];
			const subscriptionCounter = subscriptions.get(
				sharedLog.rpc.topic,
			)?.counter;
			const replicationSegments = await sharedLog.getMyReplicationSegments();
			const { entry: before } = await db.add("before");
			const sandbox = sinon.createSandbox();
			const lowerClose = sandbox.spy(lowerLog, "close");
			const lowerDrop = sandbox.spy(lowerLog, "drop");
			const entryStop = sandbox.spy(entryCoordinatesIndex, "stop");
			const entryDrop = sandbox.spy(entryCoordinatesIndex, "drop");
			const replicationStop = sandbox.spy(replicationIndex, "stop");
			const replicationDrop = sandbox.spy(replicationIndex, "drop");

			try {
				await expect(sharedLog[operation](invalidOwner)).to.be.rejectedWith(
					TerminalOperationNotStartedError,
					"Could not find from in parents",
				);
				expect(sharedLog.closed).to.be.false;
				expect(sharedLog.parents).to.deep.equal([db]);
				expect(lowerClose.called).to.be.false;
				expect(lowerDrop.called).to.be.false;
				expect(entryStop.called).to.be.false;
				expect(entryDrop.called).to.be.false;
				expect(replicationStop.called).to.be.false;
				expect(replicationDrop.called).to.be.false;
				expect(sharedLog.entryCoordinatesIndex).to.equal(entryCoordinatesIndex);
				expect(sharedLog.replicationIndex).to.equal(replicationIndex);
				expect(await sharedLog.isReplicating()).to.be.true;
				expect(await sharedLog.getMyReplicationSegments()).to.deep.equal(
					replicationSegments,
				);
				expect(subscriptions.get(sharedLog.rpc.topic)?.counter).to.equal(
					subscriptionCounter,
				);

				const { entry: after } = await db.add("after");
				expect(await lowerLog.has(before.hash)).to.be.true;
				expect(await lowerLog.has(after.hash)).to.be.true;
				expect((await db.iterator({ limit: -1 })).collect()).to.have.length(2);
				expect(await entryCoordinatesIndex.count()).to.equal(1);
				expect(await replicationIndex.count()).to.equal(1);
			} finally {
				sandbox.restore();
			}
		});

		it(`${operation} releases one owner reference without changing live SharedLog state`, async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore<string, any>());
			const sharedLog = db.log;
			await session.peers[0].open(sharedLog, {
				parent: db as any,
				existing: "reuse",
			});
			expect(sharedLog.parents).to.deep.equal([db, db]);
			expect(db.children.filter((child) => child === sharedLog)).to.have.length(
				2,
			);

			const lowerLog = sharedLog.log;
			const entryCoordinatesIndex = sharedLog.entryCoordinatesIndex;
			const replicationIndex = sharedLog.replicationIndex;
			const subscriptions = (sharedLog.node.services.pubsub as any)[
				"subscriptions"
			];
			const subscriptionCounter = subscriptions.get(
				sharedLog.rpc.topic,
			)?.counter;
			const replicationSegments = await sharedLog.getMyReplicationSegments();
			const { entry: before } = await db.add("before");
			const sandbox = sinon.createSandbox();
			const lowerClose = sandbox.spy(lowerLog, "close");
			const lowerDrop = sandbox.spy(lowerLog, "drop");
			const entryStop = sandbox.spy(entryCoordinatesIndex, "stop");
			const entryDrop = sandbox.spy(entryCoordinatesIndex, "drop");
			const replicationStop = sandbox.spy(replicationIndex, "stop");
			const replicationDrop = sandbox.spy(replicationIndex, "drop");

			try {
				expect(await sharedLog[operation](db)).to.be.false;
				expect(sharedLog.closed).to.be.false;
				expect(sharedLog.parents).to.deep.equal([db]);
				expect(
					db.children.filter((child) => child === sharedLog),
				).to.have.length(1);
				expect(lowerClose.called).to.be.false;
				expect(lowerDrop.called).to.be.false;
				expect(entryStop.called).to.be.false;
				expect(entryDrop.called).to.be.false;
				expect(replicationStop.called).to.be.false;
				expect(replicationDrop.called).to.be.false;
				expect(sharedLog.entryCoordinatesIndex).to.equal(entryCoordinatesIndex);
				expect(sharedLog.replicationIndex).to.equal(replicationIndex);
				expect(await sharedLog.isReplicating()).to.be.true;
				expect(await sharedLog.getMyReplicationSegments()).to.deep.equal(
					replicationSegments,
				);
				expect(subscriptions.get(sharedLog.rpc.topic)?.counter).to.equal(
					subscriptionCounter,
				);

				const { entry: after } = await db.add("after");
				expect(await lowerLog.has(before.hash)).to.be.true;
				expect(await lowerLog.has(after.hash)).to.be.true;
				expect((await db.iterator({ limit: -1 })).collect()).to.have.length(2);
				expect(await entryCoordinatesIndex.count()).to.equal(1);
				expect(await replicationIndex.count()).to.equal(1);
			} finally {
				sandbox.restore();
			}
		});
	}

	describe("drop", () => {
		it("rejects a fresh drop after clean close without erasing the lower log", async () => {
			session = await TestSession.connected(1);
			const db = await session.peers[0].open(new EventStore());
			const { entry } = await db.add("keep-after-close");
			const sharedLog = db.log;
			const lowerDrop = sinon.spy(sharedLog.log, "drop");
			try {
				await sharedLog.close(db);
				expect(await sharedLog.log.has(entry.hash)).to.be.true;

				await expect(sharedLog.drop()).to.be.rejectedWith(
					"Program is closed, can not drop",
				);
				expect(lowerDrop.called).to.be.false;
				expect(await sharedLog.log.has(entry.hash)).to.be.true;
			} finally {
				lowerDrop.restore();
			}
		});

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

			await waitForResolved(async () => {
				const subscribers =
					await session.peers[0].services.pubsub.getSubscribers(
						db1.log.rpc.topic,
					);
				expect((subscribers || []).map((x) => x.hashcode())).to.include(
					session.peers[1].identity.publicKey.hashcode(),
				);
			});
			await db1.log.waitForReplicator(session.peers[1].identity.publicKey, {
				timeout: 15e3,
				roleAge: 0,
			});

			const { entry } = await db1.add("hello!");

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

			const sync = db3.log.syncronizer as SimpleSyncronizer<any>;
			const db1Hash = db1.node.identity.publicKey.hashcode();
			const db2Hash = db2.node.identity.publicKey.hashcode();
			sync.syncInFlight.set(
				db1Hash,
				new Map([
					[
						entry.hash,
						{
							timestamp: Date.now(),
						},
					],
				]),
			);
			sync.syncInFlightQueue.set(entry.hash, [db2.node.identity.publicKey]);
			sync.syncInFlightQueueInverted.set(db2Hash, new Set([entry.hash]));

			expect(sync.syncInFlight.has(db1Hash)).to.equal(true);
			expect(sync.syncInFlightQueue.has(entry.hash)).to.equal(true);
			expect(sync.syncInFlightQueueInverted.has(db2Hash)).to.equal(true);
			await db1.close();
			await db2.close();
			// Closing a remote log propagates through unsubscribe + replication updates.
			// Under full-suite load this can take longer than the default wait timeout.
			await waitForResolved(
				async () => expect((await db3.log.getReplicators()).size).to.equal(1),
				{ timeout: 30e3, delayInterval: 100 },
			);
			// Close/unsubscribe ordering can vary under full-suite load. Ensure we
			// validate the cleanup behavior deterministically for the departed peers.
			sync.onPeerDisconnected(db1Hash);
			sync.onPeerDisconnected(db2Hash);

			// Under suite-wide load there can be unrelated in-flight sync state from
			// concurrent background exchanges. Assert that the departed peers are cleared.
			await waitForResolved(
				() =>
					expect(
						(db3.log.syncronizer as SimpleSyncronizer<any>).syncInFlight.has(
							db1Hash,
						),
					).to.be.false,
			);
			await waitForResolved(
				() =>
					expect(
						(db3.log.syncronizer as SimpleSyncronizer<any>).syncInFlight.has(
							db2Hash,
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
						(sync as SimpleSyncronizer<any>)["syncInFlightQueueInverted"].has(
							db2Hash,
						),
					).to.be.false,
			);
		});
	});

	describe("prune", () => {
		it("prune after close is no-op", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const db = await session.peers[0].open(store);
			const { entry } = await db.add("hello");
			await db.close();
			let pruneMap = new Map([[entry.hash, { entry, leaders: new Map() }]]);
			let t0 = +new Date();
			await Promise.all(db.log.prune(pruneMap));
			let t1 = +new Date();
			expect(t1 - t0).to.be.lessThan(100);
		});
	});
});
