import { TestSession } from "@peerbit/test-utils";
import { TimeoutError, delay } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	AbsoluteReplicas,
	encodeReplicas,
	RequestReplicationInfoMessage,
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
