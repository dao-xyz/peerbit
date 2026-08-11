import { TestSession } from "@peerbit/test-utils";
import { TimeoutError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	AbsoluteReplicas,
	RequestReplicationInfoMessage,
	encodeReplicas,
} from "../src/replication.js";
import { checkBounded } from "./utils.js";
import { EventStore } from "./utils/stores/index.js";

describe("waitForReplicator", () => {
	let session: TestSession;
	let db: EventStore<string, any>;
	let clock: sinon.SinonFakeTimers | undefined;

	const createFakeBoundedDb = (options: {
		id: string;
		length: number | (() => number);
		hash?: string;
	}) => {
		const entry = {
			hash: options.hash ?? "entry-1",
			meta: { data: encodeReplicas(new AbsoluteReplicas(1)), gid: "gid-1" },
		};

		const currentLength = () =>
			typeof options.length === "function" ? options.length() : options.length;

		return {
			log: {
				replicas: {
					min: new AbsoluteReplicas(1),
					max: new AbsoluteReplicas(1),
				},
				node: {
					identity: {
						publicKey: {
							hashcode: () => options.id,
						},
					},
				},
				syncronizer: {
					syncInFlight: new Set<string>(),
				},
				_gidPeersHistory: new Map(),
				getAllReplicationSegments: async () => [],
				getPrunable: async () => [],
				createCoordinates: async () => [],
				log: {
					get length() {
						return currentLength();
					},
					toArray: async () => [entry],
					blocks: {
						has: async () => true,
					},
					has: async () => true,
				},
			},
		};
	};

	afterEach(async () => {
		clock?.restore();
		clock = undefined;
		if (db && db.closed === false) {
			await db.drop();
		}
		await session?.stop();
	});

	it("respects configured request retry limits", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				compatibility: 9,
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
				waitForReplicatorRequestMaxAttempts: 2,
			},
		});

		const originalSend = db.log.rpc.send.bind(db.log.rpc);
		let requestCount = 0;
		db.log.rpc.send = async (message: any, options: any) => {
			if (message instanceof RequestReplicationInfoMessage) {
				requestCount++;
				return;
			}
			return originalSend(message, options);
		};

		// `open()` may schedule a best-effort replication info request to recently seen peers.
		// We only want to count the retries issued by `waitForReplicator()`.
		await delay(100);
		const baseline = requestCount;

		try {
			await db.log.waitForReplicator(session.peers[1].identity.publicKey, {
				timeout: 300,
				eager: true,
			});
			throw new Error("Expected waitForReplicator() to time out");
		} catch (error) {
			expect(error).to.be.instanceOf(TimeoutError);
		} finally {
			db.log.rpc.send = originalSend;
		}

		expect(requestCount - baseline).to.equal(2);
	});

	it("nudges parked V2 recovery without sending legacy requests by default", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
				waitForReplicatorRequestMaxAttempts: 2,
			},
		});
		await session.peers[1].open(db.clone(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const originalSend = log.rpc.send.bind(log.rpc);
		let legacyRequests = 0;
		log.rpc.send = async (message: any, options: any) => {
			if (message instanceof RequestReplicationInfoMessage) {
				legacyRequests++;
				return;
			}
			return originalSend(message, options);
		};
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		await waitForResolved(() =>
			expect(log._peerSessions.current(remoteHash)?.phase).to.equal("open"),
		);
		// Isolate the caller-owned wait loop from the session-lifetime recovery
		// ticker started at the end of the subscription callback. `markOpen`
		// precedes that scheduling, so wait for the exact-session job itself.
		await waitForResolved(() =>
			expect(
				log._replicationInfoRequestByPeer.get(remoteHash)?.peerSession,
			).to.equal(log._peerSessions.current(remoteHash)),
		);
		log.cancelReplicationInfoRequests(remoteHash);
		const scheduleRecovery = sinon
			.stub(log, "scheduleReplicationInfoV2Recovery")
			.callsFake(() => {});
		const resumed: Array<{
			properties: {
				peerHash: string;
				peerSession: object;
				receiveEpoch: object | null;
			};
			currentPeerSession: object | undefined;
			currentPhase: string | undefined;
			currentReceiveEpoch: object | null;
		}> = [];
		const resume = sinon
			.stub(log._v2Receive, "resumeParkedRequest")
			.callsFake((properties: unknown) => {
				resumed.push({
					properties: properties as (typeof resumed)[number]["properties"],
					currentPeerSession: log._peerSessions.current(remoteHash),
					currentPhase: log._peerSessions.current(remoteHash)?.phase,
					currentReceiveEpoch: log._peerSessions.receiveEpoch(remoteHash),
				});
				return false;
			});

		try {
			await expect(
				db.log.waitForReplicator(remote, {
					timeout: 300,
					eager: true,
				}),
			).to.be.rejectedWith(TimeoutError);
			expect(legacyRequests).to.equal(0);
			expect(resume.callCount).to.equal(2);
			for (const observation of resumed) {
				expect(observation.properties.peerHash).to.equal(remoteHash);
				expect(observation.properties.peerSession).to.equal(
					observation.currentPeerSession,
				);
				expect(observation.currentPhase).to.equal("open");
				expect(observation.properties.receiveEpoch).to.equal(
					observation.currentReceiveEpoch,
				);
			}
		} finally {
			resume.restore();
			scheduleRecovery.restore();
			log.rpc.send = originalSend;
		}
	});

	it("requests an authoritative subscriber snapshot when V2 has no peer session", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
				waitForReplicatorRequestMaxAttempts: 2,
			},
		});

		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		expect(log._peerSessions.current(remoteHash)).to.equal(null);

		const requestSubscribers = sinon
			.stub(session.peers[0].services.pubsub, "requestSubscribers")
			.resolves();
		const resume = sinon.stub(log._v2Receive, "resumeParkedRequest");

		try {
			await expect(
				log.waitForReplicator(remote, {
					timeout: 300,
					eager: true,
				}),
			).to.be.rejectedWith(TimeoutError);
			expect(requestSubscribers.callCount).to.equal(2);
			for (const call of requestSubscribers.getCalls()) {
				expect(call.args).to.deep.equal([log.topic, remote]);
			}
			expect(resume.notCalled).to.be.true;
		} finally {
			resume.restore();
			requestSubscribers.restore();
		}
	});

	it("requests a subscriber snapshot for a departing V2 peer session", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
				waitForReplicatorRequestMaxAttempts: 2,
			},
		});

		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const departing = log._peerSessions.rotate(remoteHash, "departing");
		expect(log._peerSessions.current(remoteHash)).to.equal(departing);

		const requestSubscribers = sinon
			.stub(session.peers[0].services.pubsub, "requestSubscribers")
			.resolves();
		const resume = sinon.stub(log._v2Receive, "resumeParkedRequest");

		try {
			await expect(
				log.waitForReplicator(remote, {
					timeout: 300,
					eager: true,
				}),
			).to.be.rejectedWith(TimeoutError);
			expect(requestSubscribers.callCount).to.equal(2);
			expect(requestSubscribers.alwaysCalledWithExactly(log.topic, remote)).to
				.be.true;
			expect(resume.notCalled).to.be.true;
		} finally {
			resume.restore();
			requestSubscribers.restore();
		}
	});

	it("coalesces an in-flight V2 subscriber snapshot across wait retries", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
				waitForReplicatorRequestMaxAttempts: 3,
			},
		});

		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		let releaseSnapshot!: () => void;
		const snapshot = new Promise<void>((resolve) => {
			releaseSnapshot = resolve;
		});
		const requestSubscribers = sinon
			.stub(session.peers[0].services.pubsub, "requestSubscribers")
			.returns(snapshot);

		try {
			await expect(
				log.waitForReplicator(remote, {
					timeout: 300,
					eager: true,
				}),
			).to.be.rejectedWith(TimeoutError);
			expect(requestSubscribers.calledOnceWithExactly(log.topic, remote)).to.be
				.true;
		} finally {
			releaseSnapshot();
			await snapshot;
			await delay(0);
			requestSubscribers.restore();
		}
	});

	it("does not request a subscriber snapshot while a V2 peer session is opening", async () => {
		session = await TestSession.connected(2);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
				waitForReplicatorRequestIntervalMs: 50,
				waitForReplicatorRequestMaxAttempts: 2,
			},
		});

		const log = db.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const opening = log._peerSessions.rotate(remoteHash, "opening");
		expect(log._peerSessions.current(remoteHash)).to.equal(opening);

		const requestSubscribers = sinon.stub(
			session.peers[0].services.pubsub,
			"requestSubscribers",
		);
		const resume = sinon.stub(log._v2Receive, "resumeParkedRequest");

		try {
			await expect(
				log.waitForReplicator(remote, {
					timeout: 300,
					eager: true,
				}),
			).to.be.rejectedWith(TimeoutError);
			expect(requestSubscribers.notCalled).to.be.true;
			expect(resume.notCalled).to.be.true;
		} finally {
			resume.restore();
			requestSubscribers.restore();
		}
	});

	it("rejects waitForReplicators when internal leader check throws", async () => {
		session = await TestSession.connected(1);
		db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				timeUntilRoleMaturity: 0,
			},
		});

		const originalFindLeaders = db.log.findLeaders.bind(db.log);
		(db.log as any).findLeaders = async () => {
			throw new Error("forced-findLeaders-error");
		};

		try {
			await expect(
				(db.log as any)._waitForReplicators(
					[0n],
					{
						hash: "bafkreif4wi7jfhqqlvgyj7a5z2fi6zt2fx5b5h3h3rfwjz2wco6n2w2k7u",
						meta: { next: [] },
					},
					[],
					{ timeout: 200 },
				),
			).to.be.rejectedWith("forced-findLeaders-error");
		} finally {
			(db.log as any).findLeaders = originalFindLeaders;
		}
	});

	it("covers checkBounded success with parallel waits", async () => {
		clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		const db1 = createFakeBoundedDb({ id: "db-1", length: 1, hash: "entry-1" });
		const db2 = createFakeBoundedDb({ id: "db-2", length: 1, hash: "entry-1" });

		const promise = checkBounded(1, 1, 1, db1 as any, db2 as any);
		await clock.tickAsync(1_000);
		await promise;
	});

	it("covers checkBounded convergence failure", async () => {
		clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		const db = createFakeBoundedDb({
			id: "db-converge",
			length: () => {
				throw new Error("forced-length-read-error");
			},
		});

		const promise = checkBounded(1, 1, 1, db as any);
		await clock.tickAsync(120_000);
		await expect(promise).to.be.rejectedWith("Log length did not converge");
	});

	it("covers checkBounded lower-bound failure reporting", async () => {
		clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		const db = createFakeBoundedDb({ id: "db-lower", length: 0 });

		const promise = checkBounded(1, 1, 1, db as any);
		await clock.tickAsync(120_000);
		await expect(promise).to.be.rejectedWith(
			"Log did not reach lower bound length of 1 got 0",
		);
	});

	it("covers checkBounded upper-bound failure reporting", async () => {
		clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		const db = createFakeBoundedDb({ id: "db-upper", length: 2 });

		const promise = checkBounded(1, 0, 1, db as any);
		await clock.tickAsync(120_000);
		await expect(promise).to.be.rejectedWith(
			"Log did not conform to upper bound length of 1 got 2",
		);
	});
});
