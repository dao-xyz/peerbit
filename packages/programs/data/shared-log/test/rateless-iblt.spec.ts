import { deserialize, serialize } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import { Ed25519Keypair } from "@peerbit/crypto";
import { EncoderWrapper } from "@peerbit/riblt";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	type ReplicationDomainHash,
	createReplicationDomainHash,
} from "../src/index.js";
import { TransportMessage } from "../src/message.js";
import {
	CodedSymbolBatch,
	MoreSymbols,
	RatelessIBLTSynchronizer,
	RequestAll,
	RequestMoreSymbols,
	StartSync,
} from "../src/sync/rateless-iblt.js";
import {
	RequestMaybeSync,
	RequestMaybeSyncCoordinate,
	ResponseMaybeSync,
} from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

const setup = {
	domain: createReplicationDomainHash("u64"),
	type: "u64" as const,
	syncronizer: RatelessIBLTSynchronizer,
	name: "u64-iblt",
	coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
};

describe("rateless-iblt-syncronizer", () => {
	let session: TestSession | undefined;
	let db1: EventStore<string, ReplicationDomainHash<"u64">>,
		db2: EventStore<string, ReplicationDomainHash<"u64">>;

	// Helper to capture messages from a log instance
	const collectMessages = async (
		log: EventStore<string, ReplicationDomainHash<"u64">>,
	) => {
		const messages: TransportMessage[] = [];
		const onMessage = log.log.rpc["_responseHandler"];
		log.log.rpc["_responseHandler"] = async (msg: any, context: any) => {
			messages.push(msg);
			return onMessage(msg, context);
		};
		return {
			get calls(): TransportMessage[] {
				return messages;
			},
		};
	};

	const countMessages = (
		messages: TransportMessage[],
		type: new (...args: any[]) => TransportMessage,
	) => {
		return messages.filter((x) => x instanceof type).length;
	};

	const createDispatchTestSynchronizer = (
		send: (
			message: TransportMessage,
			options?: { signal?: AbortSignal },
		) => Promise<void> | void,
		sync?: any,
	) =>
		new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			sync,
		});

	const createDispatchTestEntries = (boundaryIndex?: number, count = 400) => {
		const entries = new Map<string, any>();
		for (let i = 0; i < count; i++) {
			const hash = `hash-${i}`;
			entries.set(hash, {
				hash,
				hashNumber: BigInt(i + 1),
				assignedToRangeBoundary: i === boundaryIndex,
			});
		}
		return entries;
	};

	const stubTrackedRepairSession = (
		sync: RatelessIBLTSynchronizer<"u64">,
		target = "target",
	) => {
		const result = {
			target,
			requested: 1,
			resolved: 0,
			unresolved: ["hash-0"],
			attempts: 1,
			durationMs: 0,
			completed: false,
		};
		let settled = false;
		let resolveDone!: (results: (typeof result)[]) => void;
		const done = new Promise<(typeof result)[]>((resolve) => {
			resolveDone = resolve;
		});
		const settle = () => {
			if (settled) {
				return;
			}
			settled = true;
			resolveDone([result]);
		};
		const cancel = sinon.spy(settle);
		const start = sinon
			.stub(sync.simple, "startRepairSession")
			.returns({ id: "tracked-repair", done, cancel });
		return { cancel, settle, start };
	};

	it("roundtrips coded symbol batches through existing transport variants", () => {
		const symbols = [
			{ count: 1n, hash: 2n, symbol: 3n },
			{ count: 4n, hash: 5n, symbol: 6n },
		];
		const expectedFlat = [1n, 2n, 3n, 4n, 5n, 6n];

		const startSyncBytes = serialize(
			new StartSync({ from: 7n, to: 11n, symbols }),
		);
		const emptyStartSyncBytes = serialize(
			new StartSync({ from: 7n, to: 11n, symbols: [] }),
		);
		expect(startSyncBytes.length - emptyStartSyncBytes.length).to.equal(48);
		const startSync = deserialize(startSyncBytes, TransportMessage);
		expect(startSync).to.be.instanceOf(StartSync);
		expect((startSync as StartSync).symbols.length).to.equal(symbols.length);
		expect(Array.from((startSync as StartSync).symbols.toFlat())).to.deep.equal(
			expectedFlat,
		);

		const moreSymbolsBytes = serialize(
			new MoreSymbols({
				syncId: new Uint8Array(32).fill(1),
				lastSeqNo: 1n,
				symbols,
			}),
		);
		const emptyMoreSymbolsBytes = serialize(
			new MoreSymbols({
				syncId: new Uint8Array(32).fill(1),
				lastSeqNo: 1n,
				symbols: [],
			}),
		);
		expect(moreSymbolsBytes.length - emptyMoreSymbolsBytes.length).to.equal(48);
		const moreSymbols = deserialize(moreSymbolsBytes, TransportMessage);
		expect(moreSymbols).to.be.instanceOf(MoreSymbols);
		expect((moreSymbols as MoreSymbols).symbols.length).to.equal(
			symbols.length,
		);
		expect(
			Array.from((moreSymbols as MoreSymbols).symbols.toFlat()),
		).to.deep.equal(expectedFlat);
	});

	const setupLogs = async (
		syncedCount: number,
		unsyncedCount: number,
		oneSided = false,
	) => {
		session = await TestSession.disconnected(2);
		db1 = await session.peers[0].open(
			new EventStore<string, ReplicationDomainHash<"u64">>(),
			{
				args: {
					replicate: { factor: 1 },
					setup,
				},
			},
		);

		db2 = await session.peers[1].open(db1.clone(), {
			args: {
				replicate: { factor: 1 },
				setup,
			},
		});

		// Add synced entries (present on both logs)
		for (let i = 0; i < syncedCount; i++) {
			const out = await db1.add("test", { meta: { next: [] } });
			await db2.log.join([out.entry]);
		}

		// Add unsynced entries (present on one or both logs)
		for (let i = 0; i < unsyncedCount; i++) {
			await db1.add("test", { meta: { next: [] } });
			if (!oneSided) {
				await db2.add("test", { meta: { next: [] } });
			}
		}

		expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount);
		expect(db2.log.log.length).to.equal(
			syncedCount + (oneSided ? 0 : unsyncedCount),
		);
	};

	afterEach(async () => {
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	it("already synced", async function () {
		this.timeout(120_000);
		const syncedCount = 1000;
		await setupLogs(syncedCount, 0);

		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();

		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(syncedCount),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0);
	});

	it("all missing will skip iblt syncing", async () => {
		const syncedCount = 0;
		const unsyncedCount = 1000;
		const oneSided = true;

		await setupLogs(syncedCount, unsyncedCount, oneSided);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();
		await Promise.all([
			db1.log.waitForReplicator(session!.peers[1].identity.publicKey, {
				timeout: 15_000,
				roleAge: 0,
			}),
			db2.log.waitForReplicator(session!.peers[0].identity.publicKey, {
				timeout: 15_000,
				roleAge: 0,
			}),
		]);
		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(unsyncedCount),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(unsyncedCount),
		);

		await waitForResolved(() => {
			const totalMoreSymbols =
				countMessages(db1Messages.calls, MoreSymbols) +
				countMessages(db2Messages.calls, MoreSymbols);
			const totalRequestAll =
				countMessages(db1Messages.calls, RequestAll) +
				countMessages(db2Messages.calls, RequestAll);
			const totalStartSync =
				countMessages(db1Messages.calls, StartSync) +
				countMessages(db2Messages.calls, StartSync);
			// Direction can vary with scheduling, but behavior should remain:
			// no incremental IBLT symbol exchange and at least one fallback/full-sync trigger.
			expect(totalMoreSymbols).to.equal(0);
			expect(totalRequestAll).to.be.greaterThan(0);
			expect(totalStartSync).to.be.greaterThan(0);
		});
	});

	it("one missing", async () => {
		const syncedCount = 1000;
		const unsyncedCount = 1;

		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();

		await waitForResolved(() =>
			expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);
		await waitForResolved(() =>
			expect(db2.log.log.length).to.equal(syncedCount + unsyncedCount * 2),
		);

		expect(countMessages(db1Messages.calls, MoreSymbols)).to.equal(0);
		expect(countMessages(db2Messages.calls, MoreSymbols)).to.equal(0);
	});

	it("large missing sets dispatch with rateless IBLT", async () => {
		const sentMessages: TransportMessage[] = [];
		const profileEvents: { name: string }[] = [];
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: {
				send: async (message: TransportMessage) => {
					sentMessages.push(message);
				},
			} as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			sync: { profile: (event) => profileEvents.push(event) },
		});

		try {
			const entries = new Map<string, any>();
			for (let i = 0; i < 3000; i++) {
				const hash = `hash-${i}`;
				entries.set(hash, {
					hash,
					hashNumber: BigInt(i + 1),
					assignedToRangeBoundary: false,
				});
			}

			await sync.onMaybeMissingEntries({
				entries,
				targets: ["target"],
			});

			const startSyncMessages = sentMessages.filter(
				(message) => message instanceof StartSync,
			);
			expect(sentMessages).to.have.length(1);
			expect(startSyncMessages).to.have.length(1);
			expect(
				(startSyncMessages[0] as StartSync).symbols.length,
			).to.be.greaterThan(0);
			const profileNames = profileEvents.map((event) => event.name);
			expect(profileNames).to.include("rateless.prepareStartSyncEncoder");
			expect(profileNames).to.include("rateless.onMaybeMissingEntries");
		} finally {
			await sync.close();
		}
	});

	it("frees an outgoing encoder once when post-prepare profiling throws", async () => {
		const profileError = new Error("prepare profile failed");
		const free = sinon.spy(EncoderWrapper.prototype, "free");
		const sync = createDispatchTestSynchronizer(() => {}, {
			profile: (event: { name: string }) => {
				if (event.name === "rateless.prepareStartSyncEncoder") {
					throw profileError;
				}
			},
		});

		try {
			await expect(
				sync.onMaybeMissingEntries({
					entries: createDispatchTestEntries(),
					targets: ["target"],
				}),
			).to.be.rejectedWith(profileError.message);

			expect(free.calledOnce).to.equal(true);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			await sync.close();
			expect(free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
			free.restore();
		}
	});

	it("cleans a registered outgoing encoder once when dispatch profiling throws", async () => {
		const profileError = new Error("dispatch profile failed");
		const free = sinon.spy(EncoderWrapper.prototype, "free");
		const sync = createDispatchTestSynchronizer(() => {}, {
			profile: (event: { name: string }) => {
				if (event.name === "rateless.dispatchMode") {
					throw profileError;
				}
			},
		});

		try {
			await expect(
				sync.onMaybeMissingEntries({
					entries: createDispatchTestEntries(),
					targets: ["target"],
				}),
			).to.be.rejectedWith(profileError.message);

			expect(free.calledOnce).to.equal(true);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			expect((sync as any).outgoingSyncProcessByTarget.size).to.equal(0);
			await sync.close();
			expect(free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
			free.restore();
		}
	});

	it("frees an outgoing encoder once when initial symbol production throws", async () => {
		const producerError = new Error("initial symbol producer failed");
		const prepareAndProduce = sinon
			.stub(
				EncoderWrapper.prototype,
				"add_symbols_sorted_find_range_and_produce",
			)
			.value(undefined);
		const produce = sinon
			.stub(EncoderWrapper.prototype, "produce_next_coded_symbols")
			.throws(producerError);
		const free = sinon.spy(EncoderWrapper.prototype, "free");
		const sync = createDispatchTestSynchronizer(() => {});

		try {
			await expect(
				sync.onMaybeMissingEntries({
					entries: createDispatchTestEntries(),
					targets: ["target"],
				}),
			).to.be.rejectedWith(producerError.message);

			expect(produce.calledOnce).to.equal(true);
			expect(free.calledOnce).to.equal(true);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			await sync.close();
			expect(free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
			free.restore();
			produce.restore();
			prepareAndProduce.restore();
		}
	});

	it("does not start rateless sync when aborted at its readiness boundary", async () => {
		const sentMessages: TransportMessage[] = [];
		const profileEvents: any[] = [];
		const sync = createDispatchTestSynchronizer(
			(message) => {
				sentMessages.push(message);
			},
			{ profile: (event: any) => profileEvents.push(event) },
		);
		const controller = new AbortController();

		try {
			const dispatch = sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["target"],
				signal: controller.signal,
			});
			controller.abort();
			await dispatch;

			expect(
				sentMessages.some((message) => message instanceof StartSync),
			).to.equal(false);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			const topLevelEvents = profileEvents.filter(
				(event) => event.name === "rateless.onMaybeMissingEntries",
			);
			expect(topLevelEvents).to.have.length(1);
			expect(topLevelEvents[0].messages).to.equal(0);
			expect(topLevelEvents[0].symbols).to.equal(0);
			expect(topLevelEvents[0].details).to.include({
				mode: "rateless",
				phase: "riblt-ready",
				cancelled: true,
			});
		} finally {
			controller.abort();
			await sync.close();
		}
	});

	it("does not register or start rateless sync after its simple prelude is aborted", async () => {
		const sentMessages: TransportMessage[] = [];
		const profileEvents: any[] = [];
		let releaseSimpleSend!: () => void;
		const simpleSendReleased = new Promise<void>((resolve) => {
			releaseSimpleSend = resolve;
		});
		let markSimpleSendStarted!: () => void;
		const simpleSendStarted = new Promise<void>((resolve) => {
			markSimpleSendStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer(
			async (message) => {
				sentMessages.push(message);
				if (message instanceof RequestMaybeSync) {
					markSimpleSendStarted();
					await simpleSendReleased;
				}
			},
			{ profile: (event: any) => profileEvents.push(event) },
		);
		const controller = new AbortController();

		try {
			const dispatch = sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(0),
				targets: ["target"],
				signal: controller.signal,
			});
			await simpleSendStarted;
			controller.abort();
			releaseSimpleSend();
			await dispatch;

			expect(
				sentMessages.filter((message) => message instanceof RequestMaybeSync),
			).to.have.length(1);
			expect(
				sentMessages.some((message) => message instanceof StartSync),
			).to.equal(false);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			const topLevelEvents = profileEvents.filter(
				(event) => event.name === "rateless.onMaybeMissingEntries",
			);
			expect(topLevelEvents).to.have.length(1);
			expect(topLevelEvents[0].details).to.include({
				mode: "rateless",
				phase: "simple-prelude",
				cancelled: true,
			});
		} finally {
			releaseSimpleSend();
			await sync.close();
		}
	});

	it("does not let a signal-less blocked prelude cross close and reopen", async () => {
		const sends: {
			message: TransportMessage;
			options?: { signal?: AbortSignal };
		}[] = [];
		let releasePrelude!: () => void;
		const preludeReleased = new Promise<void>((resolve) => {
			releasePrelude = resolve;
		});
		let markPreludeStarted!: () => void;
		const preludeStarted = new Promise<void>((resolve) => {
			markPreludeStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer(async (message, options) => {
			sends.push({ message, options });
			if (message instanceof RequestMaybeSync) {
				markPreludeStarted();
				await preludeReleased;
			}
		});

		try {
			const dispatch = sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(0),
				targets: ["target"],
			});
			await preludeStarted;
			const preludeSignal = sends.find(
				(send) => send.message instanceof RequestMaybeSync,
			)?.options?.signal;
			expect(preludeSignal?.aborted).to.equal(false);

			await sync.close();
			expect(preludeSignal?.aborted).to.equal(true);
			await sync.open();
			releasePrelude();
			await dispatch;

			expect(sends.some((send) => send.message instanceof StartSync)).to.equal(
				false,
			);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
		} finally {
			releasePrelude();
			await sync.close();
		}
	});

	it("runs delayed full-set retries for a capped convergent repair", async () => {
		const clock = sinon.useFakeTimers();
		const sync = createDispatchTestSynchronizer(() => {}, {
			maxConvergentTrackedHashes: 1,
		});
		const tracked = stubTrackedRepairSession(sync);
		const dispatch = sinon.stub(sync, "onMaybeMissingEntries").resolves();

		try {
			const repair = sync.startRepairSession({
				entries: createDispatchTestEntries(undefined, 2),
				targets: ["target"],
				mode: "convergent",
				timeoutMs: 1_000,
				retryIntervalsMs: [100, 300],
			});

			await clock.tickAsync(99);
			expect(dispatch.called).to.equal(false);
			await clock.tickAsync(1);
			expect(dispatch.calledOnce).to.equal(true);
			const retrySignal = dispatch.firstCall.args[0].signal;
			expect(retrySignal).to.be.instanceOf(AbortSignal);
			expect(retrySignal?.aborted).to.equal(false);

			await clock.tickAsync(200);
			expect(dispatch.callCount).to.equal(2);
			tracked.settle();
			const results = await repair.done;
			expect(results[0]).to.include({
				requestedTotal: 2,
				truncated: true,
			});
			expect((sync as any).ratelessRepairSessions.size).to.equal(0);
		} finally {
			tracked.settle();
			dispatch.restore();
			tracked.start.restore();
			await sync.close();
			clock.restore();
		}
	});

	it("does not dispatch an old delayed repair after close and reopen", async () => {
		const clock = sinon.useFakeTimers();
		const sends: TransportMessage[] = [];
		const sync = createDispatchTestSynchronizer(
			(message) => {
				sends.push(message);
			},
			{ maxConvergentTrackedHashes: 1 },
		);
		const tracked = stubTrackedRepairSession(sync);
		const dispatch = sinon.stub(sync, "onMaybeMissingEntries").resolves();

		try {
			const repair = sync.startRepairSession({
				entries: createDispatchTestEntries(undefined, 2),
				targets: ["target"],
				mode: "convergent",
				timeoutMs: 10_000,
				retryIntervalsMs: [1_000],
			});

			await clock.tickAsync(999);
			expect(dispatch.called).to.equal(false);
			await sync.close();
			await sync.open();
			await repair.done;
			await clock.tickAsync(10_000);

			expect(tracked.cancel.calledOnce).to.equal(true);
			expect(dispatch.called).to.equal(false);
			expect(
				sends.some(
					(message) =>
						message instanceof StartSync || message instanceof RequestMaybeSync,
				),
			).to.equal(false);
			expect((sync as any).ratelessRepairSessions.size).to.equal(0);
		} finally {
			tracked.settle();
			dispatch.restore();
			tracked.start.restore();
			await sync.close();
			clock.restore();
		}
	});

	it("aborts an in-flight capped convergent retry when cancelled", async () => {
		const clock = sinon.useFakeTimers();
		const sync = createDispatchTestSynchronizer(() => {}, {
			maxConvergentTrackedHashes: 1,
		});
		const tracked = stubTrackedRepairSession(sync);
		let retrySignal: AbortSignal | undefined;
		let markRetryStarted!: () => void;
		const retryStarted = new Promise<void>((resolve) => {
			markRetryStarted = resolve;
		});
		const dispatch = sinon
			.stub(sync, "onMaybeMissingEntries")
			.callsFake(({ signal }: { signal?: AbortSignal }) => {
				retrySignal = signal;
				markRetryStarted();
				// Model a transport that ignores abort and never settles. The repair
				// lifecycle itself must still release session.done.
				return new Promise<void>(() => {});
			});

		try {
			const repair = sync.startRepairSession({
				entries: createDispatchTestEntries(undefined, 2),
				targets: ["target"],
				mode: "convergent",
				timeoutMs: 10_000,
				retryIntervalsMs: [0, 1_000],
			});
			await retryStarted;
			expect(retrySignal?.aborted).to.equal(false);

			repair.cancel();
			expect(retrySignal?.aborted).to.equal(true);
			await repair.done;
			await clock.tickAsync(10_000);

			expect(tracked.cancel.calledOnce).to.equal(true);
			expect(dispatch.calledOnce).to.equal(true);
			expect((sync as any).ratelessRepairSessions.size).to.equal(0);
		} finally {
			tracked.settle();
			dispatch.restore();
			tracked.start.restore();
			await sync.close();
			clock.restore();
		}
	});

	it("times out a never-settling capped convergent retry", async () => {
		const clock = sinon.useFakeTimers();
		const sync = createDispatchTestSynchronizer(() => {}, {
			maxConvergentTrackedHashes: 1,
		});
		const tracked = stubTrackedRepairSession(sync);
		let retrySignal: AbortSignal | undefined;
		let markRetryStarted!: () => void;
		const retryStarted = new Promise<void>((resolve) => {
			markRetryStarted = resolve;
		});
		const dispatch = sinon
			.stub(sync, "onMaybeMissingEntries")
			.callsFake(({ signal }: { signal?: AbortSignal }) => {
				retrySignal = signal;
				markRetryStarted();
				return new Promise<void>(() => {});
			});

		try {
			const repair = sync.startRepairSession({
				entries: createDispatchTestEntries(undefined, 2),
				targets: ["target"],
				mode: "convergent",
				timeoutMs: 500,
				retryIntervalsMs: [0],
			});
			await retryStarted;
			expect(retrySignal?.aborted).to.equal(false);

			await clock.tickAsync(500);
			const results = await repair.done;

			expect(retrySignal?.aborted).to.equal(true);
			expect(tracked.cancel.calledOnce).to.equal(true);
			expect(results[0].completed).to.equal(false);
			expect(dispatch.calledOnce).to.equal(true);
			expect((sync as any).ratelessRepairSessions.size).to.equal(0);
			expect(clock.countTimers()).to.equal(0);

			await clock.tickAsync(10_000);
			expect(dispatch.calledOnce).to.equal(true);
			expect(clock.countTimers()).to.equal(0);
		} finally {
			tracked.settle();
			dispatch.restore();
			tracked.start.restore();
			await sync.close();
			clock.restore();
		}
	});

	it("cleans registered rateless state when its dispatch signal is aborted", async () => {
		const sentMessages: TransportMessage[] = [];
		let startSyncSignal: AbortSignal | undefined;
		const sync = createDispatchTestSynchronizer((message, options) => {
			sentMessages.push(message);
			if (message instanceof StartSync) {
				startSyncSignal = options?.signal;
			}
		});
		const controller = new AbortController();
		const entries = createDispatchTestEntries();
		const expectMaybeSyncResponse = sinon.spy(
			sync.simple,
			"expectMaybeSyncResponse",
		);

		try {
			await sync.onMaybeMissingEntries({
				entries,
				targets: ["target"],
				signal: controller.signal,
			});

			expect(
				sentMessages.filter((message) => message instanceof StartSync),
			).to.have.length(1);
			expect(sync.outgoingSyncProcesses.size).to.equal(1);
			expect(startSyncSignal).not.to.equal(controller.signal);
			expect(startSyncSignal?.aborted).to.equal(false);
			expect(
				[...sync.outgoingSyncProcesses.values()][0].authorizedHashes,
			).to.deep.equal(new Set(entries.keys()));
			expect(expectMaybeSyncResponse.called).to.equal(false);

			controller.abort();

			expect(startSyncSignal?.aborted).to.equal(true);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
		} finally {
			controller.abort();
			expectMaybeSyncResponse.restore();
			await sync.close();
		}
	});

	it("keeps each target independent across RequestAll and RequestMoreSymbols", async () => {
		const sends: { message: TransportMessage; options?: any }[] = [];
		const sync = createDispatchTestSynchronizer((message, options) => {
			sends.push({ message, options });
		});
		const peer = (target: string) => ({ hashcode: () => target }) as any;
		const startFor = (target: string) =>
			sends.find(
				(send) =>
					send.message instanceof StartSync &&
					send.options?.mode?.to?.[0] === target,
			)?.message as StartSync | undefined;

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a", "peer-b"],
			});
			const startA = startFor("peer-a");
			const startB = startFor("peer-b");
			expect(startA).to.be.instanceOf(StartSync);
			expect(startB).to.be.instanceOf(StartSync);
			expect(startA!.syncId).not.to.deep.equal(startB!.syncId);
			expect(sync.outgoingSyncProcesses.size).to.equal(2);

			await sync.onMessage(new RequestAll({ syncId: startA!.syncId }), {
				from: peer("peer-a"),
			} as any);
			expect(sync.outgoingSyncProcesses.size).to.equal(1);
			expect(
				sends.filter(
					(send) =>
						send.message instanceof RequestMaybeSync &&
						send.options?.mode?.to?.[0] === "peer-a",
				),
			).to.have.length(1);

			await sync.onMessage(
				new RequestMoreSymbols({
					syncId: startB!.syncId,
					lastSeqNo: 0n,
				}),
				{ from: peer("peer-b") } as any,
			);
			expect(
				sends.filter(
					(send) =>
						send.message instanceof MoreSymbols &&
						send.options?.mode?.to?.[0] === "peer-b",
				),
			).to.have.length(1);

			await sync.onMessage(new RequestAll({ syncId: startB!.syncId }), {
				from: peer("peer-b"),
			} as any);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			expect(
				sends.filter(
					(send) =>
						send.message instanceof RequestMaybeSync &&
						send.options?.mode?.to?.[0] === "peer-b",
				),
			).to.have.length(1);
		} finally {
			await sync.close();
		}
	});

	it("disconnecting one target leaves the other rateless process live", async () => {
		const sends: { message: TransportMessage; options?: any }[] = [];
		const sync = createDispatchTestSynchronizer((message, options) => {
			sends.push({ message, options });
		});
		const peerB = { hashcode: () => "peer-b" } as any;

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a", "peer-b"],
			});
			const startB = sends.find(
				(send) =>
					send.message instanceof StartSync &&
					send.options?.mode?.to?.[0] === "peer-b",
			)?.message as StartSync;

			await sync.onPeerDisconnected("peer-a");
			expect(sync.outgoingSyncProcesses.size).to.equal(1);
			expect([...sync.outgoingSyncProcesses.values()][0].target).to.equal(
				"peer-b",
			);

			await sync.onMessage(
				new RequestMoreSymbols({
					syncId: startB.syncId,
					lastSeqNo: 0n,
				}),
				{ from: peerB } as any,
			);
			expect(
				sends.some(
					(send) =>
						send.message instanceof MoreSymbols &&
						send.options?.mode?.to?.[0] === "peer-b",
				),
			).to.equal(true);
		} finally {
			await sync.close();
		}
	});

	it("keeps rateless authorization direct above the Simple 10,000-hash cap", async () => {
		const sends: { message: TransportMessage; options?: any }[] = [];
		const sync = createDispatchTestSynchronizer((message, options) => {
			sends.push({ message, options });
		});
		const expectMaybeSyncResponse = sinon.spy(
			sync.simple,
			"expectMaybeSyncResponse",
		);
		const shipAuthorized = sinon
			.stub(sync.simple, "shipAuthorizedMaybeSyncResponse")
			.resolves({ messages: 0, fused: false, entries: 1 });
		const target = { hashcode: () => "peer-a" } as any;

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(undefined, 10_001),
				targets: ["peer-a"],
			});

			expect(
				sends.filter((send) => send.message instanceof StartSync),
			).to.have.length(1);
			expect(
				sends.filter((send) => send.message instanceof RequestMaybeSync),
			).to.have.length(0);
			expect(expectMaybeSyncResponse.called).to.equal(false);
			expect(sync.outgoingSyncProcesses.size).to.equal(1);

			await sync.onMessage(
				new ResponseMaybeSync({
					hashes: ["hash-10000", "not-advertised"],
				}),
				{ from: target } as any,
			);
			expect(shipAuthorized.calledOnce).to.equal(true);
			expect(shipAuthorized.firstCall.args[0].hashes).to.deep.equal([
				"hash-10000",
			]);
			expect(shipAuthorized.firstCall.args[0].from).to.equal(target);

			await sync.onMessage(new ResponseMaybeSync({ hashes: ["hash-9999"] }), {
				from: { hashcode: () => "foreign-peer" },
			} as any);
			expect(shipAuthorized.calledOnce).to.equal(true);
		} finally {
			shipAuthorized.restore();
			expectMaybeSyncResponse.restore();
			await sync.close();
		}
	});

	it("lets an accepted >10,000-hash response finish after its process timeout", async () => {
		const clock = sinon.useFakeTimers();
		let releaseShipment!: () => void;
		const shipmentReleased = new Promise<void>((resolve) => {
			releaseShipment = resolve;
		});
		let markShipmentStarted!: () => void;
		const shipmentStarted = new Promise<void>((resolve) => {
			markShipmentStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer(() => {});
		let leaseSignal: AbortSignal | undefined;
		const ship = sinon
			.stub(sync.simple, "shipAuthorizedMaybeSyncResponse")
			.callsFake(async ({ signal }: { signal: AbortSignal }) => {
				leaseSignal = signal;
				markShipmentStarted();
				await shipmentReleased;
				return { messages: 1, fused: false, entries: 1 };
			});

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(undefined, 10_001),
				targets: ["peer-a"],
			});
			const handling = sync.onMessage(
				new ResponseMaybeSync({ hashes: ["hash-10000"] }),
				{ from: { hashcode: () => "peer-a" } } as any,
			);
			await shipmentStarted;

			await clock.tickAsync(10_001);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			expect(leaseSignal?.aborted).to.equal(false);

			releaseShipment();
			expect(await handling).to.equal(true);
			expect(ship.calledOnce).to.equal(true);
		} finally {
			releaseShipment();
			await sync.close();
			ship.restore();
			clock.restore();
		}
	});

	it("does not abort an accepted response when a newer target process replaces it", async () => {
		let releaseShipment!: () => void;
		const shipmentReleased = new Promise<void>((resolve) => {
			releaseShipment = resolve;
		});
		let markShipmentStarted!: () => void;
		const shipmentStarted = new Promise<void>((resolve) => {
			markShipmentStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer(() => {});
		let leaseSignal: AbortSignal | undefined;
		const ship = sinon
			.stub(sync.simple, "shipAuthorizedMaybeSyncResponse")
			.callsFake(async ({ signal }: { signal: AbortSignal }) => {
				leaseSignal = signal;
				markShipmentStarted();
				await shipmentReleased;
				return { messages: 1, fused: false, entries: 1 };
			});

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a"],
			});
			const handling = sync.onMessage(
				new ResponseMaybeSync({ hashes: ["hash-399"] }),
				{ from: { hashcode: () => "peer-a" } } as any,
			);
			await shipmentStarted;

			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(undefined, 401),
				targets: ["peer-a"],
			});
			expect(sync.outgoingSyncProcesses.size).to.equal(1);
			expect(leaseSignal?.aborted).to.equal(false);

			releaseShipment();
			expect(await handling).to.equal(true);
		} finally {
			releaseShipment();
			await sync.close();
			ship.restore();
		}
	});

	it("aborts an accepted response lease on close", async () => {
		let releaseShipment!: () => void;
		const shipmentReleased = new Promise<void>((resolve) => {
			releaseShipment = resolve;
		});
		let markShipmentStarted!: () => void;
		const shipmentStarted = new Promise<void>((resolve) => {
			markShipmentStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer(() => {});
		let leaseSignal: AbortSignal | undefined;
		const ship = sinon
			.stub(sync.simple, "shipAuthorizedMaybeSyncResponse")
			.callsFake(async ({ signal }: { signal: AbortSignal }) => {
				leaseSignal = signal;
				markShipmentStarted();
				await shipmentReleased;
				return { messages: 0, fused: false, entries: 0 };
			});

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a"],
			});
			const handling = sync.onMessage(
				new ResponseMaybeSync({ hashes: ["hash-399"] }),
				{ from: { hashcode: () => "peer-a" } } as any,
			);
			await shipmentStarted;

			await sync.close();
			expect(leaseSignal?.aborted).to.equal(true);
			releaseShipment();
			expect(await handling).to.equal(true);
		} finally {
			releaseShipment();
			ship.restore();
			await sync.close();
		}
	});

	it("rolls back rateless response consumption after an ordinary ship failure", async () => {
		const sync = createDispatchTestSynchronizer(() => {});
		const ship = sinon.stub(sync.simple, "shipAuthorizedMaybeSyncResponse");
		ship.onFirstCall().rejects(new Error("ship failed"));
		ship.onSecondCall().resolves({ messages: 1, fused: false, entries: 1 });
		const from = { hashcode: () => "peer-a" } as any;

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a"],
			});
			const process = [...sync.outgoingSyncProcesses.values()][0];

			await expect(
				sync.onMessage(new ResponseMaybeSync({ hashes: ["hash-399"] }), {
					from,
				} as any),
			).to.be.rejectedWith("ship failed");
			expect(process.consumedResponseHashes.has("hash-399")).to.equal(false);

			expect(
				await sync.onMessage(new ResponseMaybeSync({ hashes: ["hash-399"] }), {
					from,
				} as any),
			).to.equal(true);
			expect(ship.callCount).to.equal(2);
		} finally {
			ship.restore();
			await sync.close();
		}
	});

	it("preleases a mixed Simple remainder and profiles direct rateless shipping", async () => {
		const clock = sinon.useFakeTimers();
		const profileEvents: any[] = [];
		let releaseRateless!: () => void;
		const ratelessReleased = new Promise<void>((resolve) => {
			releaseRateless = resolve;
		});
		let markRatelessStarted!: () => void;
		const ratelessStarted = new Promise<void>((resolve) => {
			markRatelessStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer(() => {}, {
			profile: (event: any) => profileEvents.push(event),
		});
		const ship = sinon
			.stub(sync.simple, "shipAuthorizedMaybeSyncResponse")
			.callsFake(async ({ hashes }: { hashes: string[] }) => {
				if (hashes[0] === "hash-399") {
					markRatelessStarted();
					await ratelessReleased;
					return { messages: 2, fused: true, entries: 1 };
				}
				return { messages: 1, fused: false, entries: hashes.length };
			});
		const consumeSimple = sinon.spy(
			sync.simple,
			"consumeAuthorizedMaybeSyncResponse",
		);
		const from = { hashcode: () => "peer-a" } as any;

		try {
			await sync.simple.onMaybeMissingHashes({
				hashes: ["simple-remainder"],
				targets: ["peer-a"],
			});
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a"],
			});

			const handling = sync.onMessage(
				new ResponseMaybeSync({
					hashes: ["hash-399", "simple-remainder"],
				}),
				{ from } as any,
			);
			await ratelessStarted;
			expect(consumeSimple.calledOnce).to.equal(true);
			expect(consumeSimple.firstCall.args[0]).to.deep.equal([
				"simple-remainder",
			]);

			await clock.tickAsync(30_001);
			releaseRateless();
			expect(await handling).to.equal(true);
			expect(ship.getCalls().map((call) => call.args[0].hashes)).to.deep.equal([
				["hash-399"],
				["simple-remainder"],
			]);

			const directProfile = profileEvents.find(
				(event) =>
					event.name === "simple.exchangeHeads" &&
					event.details?.source === "ratelessResponseMaybeSync",
			);
			expect(directProfile).to.include({
				entries: 1,
				messages: 2,
				targets: 1,
			});
			expect(directProfile.details.fused).to.equal(true);
		} finally {
			releaseRateless();
			consumeSimple.restore();
			ship.restore();
			await sync.close();
			clock.restore();
		}
	});

	it("ships and releases a preleased Simple remainder when direct profiling throws", async () => {
		const profileError = new Error("direct profile failed");
		const profileEvents: any[] = [];
		const sync = createDispatchTestSynchronizer(() => {}, {
			profile: (event: any) => {
				profileEvents.push(event);
				if (
					event.name === "simple.exchangeHeads" &&
					event.details?.source === "ratelessResponseMaybeSync"
				) {
					throw profileError;
				}
			},
		});
		const ship = sinon
			.stub(sync.simple, "shipAuthorizedMaybeSyncResponse")
			.resolves({ messages: 1, fused: false, entries: 1 });
		const from = { hashcode: () => "peer-a" } as any;

		try {
			await sync.simple.onMaybeMissingHashes({
				hashes: ["simple-remainder"],
				targets: ["peer-a"],
			});
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a"],
			});

			await expect(
				sync.onMessage(
					new ResponseMaybeSync({
						hashes: ["hash-399", "simple-remainder"],
					}),
					{ from } as any,
				),
			).to.be.rejectedWith(profileError.message);

			expect(ship.getCalls().map((call) => call.args[0].hashes)).to.deep.equal([
				["hash-399"],
				["simple-remainder"],
			]);
			expect(
				profileEvents.some(
					(event) =>
						event.name === "simple.exchangeHeads" &&
						event.details?.source === "responseMaybeSync",
				),
			).to.equal(true);
			expect((sync.simple as any).pendingMaybeSyncResponseCount).to.equal(0);
			expect((sync.simple as any).syncDispatchTargets.size).to.equal(0);
		} finally {
			ship.restore();
			await sync.close();
		}
	});

	it("does not let a foreign target drive an outgoing rateless process", async () => {
		const sends: TransportMessage[] = [];
		const sync = createDispatchTestSynchronizer((message) => {
			sends.push(message);
		});

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["peer-a"],
			});
			const startSync = sends.find(
				(message) => message instanceof StartSync,
			) as StartSync;
			const sendsBeforeForeignRequests = sends.length;
			const foreignContext = {
				from: { hashcode: () => "peer-b" },
			} as any;

			await sync.onMessage(
				new RequestMoreSymbols({
					syncId: startSync.syncId,
					lastSeqNo: 0n,
				}),
				foreignContext,
			);
			await sync.onMessage(
				new RequestAll({ syncId: startSync.syncId }),
				foreignContext,
			);

			expect(sends).to.have.length(sendsBeforeForeignRequests);
			expect(sync.outgoingSyncProcesses.size).to.equal(1);
		} finally {
			await sync.close();
		}
	});

	it("isolates the same incoming sync id by sender and frees only a disconnected sender", async () => {
		const peerA = (await Ed25519Keypair.create()).publicKey;
		const peerB = (await Ed25519Keypair.create()).publicKey;
		const peerC = (await Ed25519Keypair.create()).publicKey;
		const peerAHash = peerA.hashcode();
		const peerBHash = peerB.hashcode();
		const sends: { message: TransportMessage; options?: any }[] = [];
		const sync = createDispatchTestSynchronizer((message, options) => {
			sends.push({ message, options });
		});
		const addA = sinon.spy();
		const addB = sinon.spy();
		const freeA = sinon.spy();
		let peerBProcess: any;
		let peerBWasAbortedWhenFreed = false;
		const decoderA = {
			add_coded_symbol: addA,
			try_decode: () => {},
			decoded: () => false,
			get_remote_symbols: () => [],
			free: freeA,
		};
		const decoderB = {
			add_coded_symbol: addB,
			try_decode: () => {},
			decoded: () => false,
			get_remote_symbols: () => [],
			free: () => {
				peerBWasAbortedWhenFreed =
					peerBProcess?.controller.signal.aborted === true;
			},
		};
		const getDecoder = sinon
			.stub(sync as any, "getLocalDecoderForRange")
			.onFirstCall()
			.resolves(decoderA)
			.onSecondCall()
			.resolves(decoderB);
		const start = new StartSync({ from: 0n, to: 10n, symbols: [] });
		const more = new MoreSymbols({
			syncId: start.syncId,
			lastSeqNo: 0n,
			symbols: [{ count: 0n, hash: 0n, symbol: 0n } as any],
		});

		try {
			await sync.onMessage(start, { from: peerA } as any);
			await sync.onMessage(start, { from: peerB } as any);
			expect(sync.ingoingSyncProcesses.size).to.equal(2);
			expect(getDecoder.callCount).to.equal(2);
			peerBProcess = [...sync.ingoingSyncProcesses.values()].find(
				(process) => process.sender === peerBHash,
			);

			const sendsBeforeForeign = sends.length;
			await sync.onMessage(more, { from: peerC } as any);
			expect(sends).to.have.length(sendsBeforeForeign);
			expect(addA.called).to.equal(false);
			expect(addB.called).to.equal(false);

			await sync.onMessage(more, { from: peerA } as any);
			expect(addA.calledOnce).to.equal(true);
			expect(addB.called).to.equal(false);
			expect(
				sends.some(
					(send) =>
						send.message instanceof RequestMoreSymbols &&
						send.options?.mode?.to?.[0] === peerAHash,
				),
			).to.equal(true);

			sync.onPeerDisconnected(peerAHash);
			expect(freeA.calledOnce).to.equal(true);
			expect(sync.ingoingSyncProcesses.size).to.equal(1);
			expect([...sync.ingoingSyncProcesses.values()][0]).to.equal(peerBProcess);

			await sync.close();
			expect(peerBWasAbortedWhenFreed).to.equal(true);
		} finally {
			getDecoder.restore();
			await sync.close();
		}
	});

	it("frees a decoder initializer that resumes after close and reopen without sending", async () => {
		const peer = (await Ed25519Keypair.create()).publicKey;
		let releaseDecoder!: (decoder: any) => void;
		const decoderReleased = new Promise<any>((resolve) => {
			releaseDecoder = resolve;
		});
		let markDecoderStarted!: () => void;
		const decoderStarted = new Promise<void>((resolve) => {
			markDecoderStarted = resolve;
		});
		const sends: TransportMessage[] = [];
		const sync = createDispatchTestSynchronizer((message) => {
			sends.push(message);
		});
		const free = sinon.spy();
		const getDecoder = sinon
			.stub(sync as any, "getLocalDecoderForRange")
			.callsFake(async () => {
				markDecoderStarted();
				return decoderReleased;
			});
		const start = new StartSync({ from: 0n, to: 10n, symbols: [] });

		try {
			const handling = sync.onMessage(start, {
				from: peer,
			} as any);
			await decoderStarted;
			const oldProcess = [...sync.ingoingSyncProcesses.values()][0];
			expect(oldProcess.controller.signal.aborted).to.equal(false);

			await sync.close();
			expect(oldProcess.controller.signal.aborted).to.equal(true);
			expect(sync.ingoingSyncProcesses.size).to.equal(0);
			await sync.open();

			releaseDecoder({
				add_coded_symbol: () => {},
				try_decode: () => {},
				decoded: () => false,
				get_remote_symbols: () => [],
				free,
			});
			expect(await handling).to.equal(true);
			expect(free.calledOnce).to.equal(true);
			expect(sync.ingoingSyncProcesses.size).to.equal(0);
			expect(
				sends.some(
					(message) =>
						message instanceof RequestAll ||
						message instanceof RequestMoreSymbols,
				),
			).to.equal(false);
		} finally {
			releaseDecoder({
				free: () => {},
			});
			getDecoder.restore();
			await sync.close();
		}
	});

	for (const decoderFound of [true, false]) {
		it(`cleans the incoming ${decoderFound ? "decoder" : "no-decoder process"} when outer decoder profiling throws`, async () => {
			const peer = (await Ed25519Keypair.create()).publicKey;
			const profileError = new Error("incoming decoder profile failed");
			const send = sinon.spy();
			const decoderFree = sinon.spy();
			let trackedProcess: any;
			let processAbort: sinon.SinonSpy | undefined;
			const sync = createDispatchTestSynchronizer(send, {
				profile: (event: { name: string }) => {
					if (event.name !== "rateless.getLocalDecoderForRange") {
						return;
					}
					trackedProcess = [...sync.ingoingSyncProcesses.values()][0];
					processAbort = sinon.spy(trackedProcess.controller, "abort");
					throw profileError;
				},
			});
			const getDecoder = sinon
				.stub(sync as any, "getLocalDecoderForRange")
				.resolves(decoderFound ? ({ free: decoderFree } as any) : false);

			try {
				await expect(
					sync.onMessage(new StartSync({ from: 0n, to: 10n, symbols: [] }), {
						from: peer,
					} as any),
				).to.be.rejectedWith(profileError.message);

				expect(trackedProcess).not.to.equal(undefined);
				expect(processAbort?.calledOnce).to.equal(true);
				expect(trackedProcess.controller.signal.aborted).to.equal(true);
				expect(trackedProcess.timeout).to.equal(undefined);
				expect(trackedProcess.decoder).to.equal(undefined);
				expect(sync.ingoingSyncProcesses.size).to.equal(0);
				expect(send.called).to.equal(false);
				expect(decoderFree.callCount).to.equal(decoderFound ? 1 : 0);

				await sync.close();
				expect(processAbort?.calledOnce).to.equal(true);
				expect(decoderFree.callCount).to.equal(decoderFound ? 1 : 0);
			} finally {
				processAbort?.restore();
				getDecoder.restore();
				await sync.close();
			}
		});
	}

	it("consumes an asynchronous StartSync send rejection", async () => {
		let rejectStartSync!: (error: Error) => void;
		const startSyncSend = new Promise<void>((_resolve, reject) => {
			rejectStartSync = reject;
		});
		let startSyncSignal: AbortSignal | undefined;
		const sync = createDispatchTestSynchronizer((message, options) => {
			if (message instanceof StartSync) {
				startSyncSignal = options?.signal;
				return startSyncSend;
			}
		});
		const controller = new AbortController();

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["target"],
				signal: controller.signal,
			});
			expect(sync.outgoingSyncProcesses.size).to.equal(1);
			expect(startSyncSignal).not.to.equal(controller.signal);
			expect(startSyncSignal?.aborted).to.equal(false);

			rejectStartSync(new Error("transport failed"));
			await Promise.resolve();

			expect(sync.outgoingSyncProcesses.size).to.equal(0);
		} finally {
			controller.abort();
			rejectStartSync(new Error("cancelled"));
			await sync.close();
		}
	});

	it("aborts an in-flight StartSync send when closed", async () => {
		let startSyncSignal: AbortSignal | undefined;
		const sync = createDispatchTestSynchronizer((message, options) => {
			if (!(message instanceof StartSync)) {
				return;
			}
			startSyncSignal = options?.signal;
			return new Promise<void>((_resolve, reject) => {
				const rejectForAbort = () =>
					reject(startSyncSignal?.reason ?? new Error("transport aborted"));
				if (startSyncSignal?.aborted) {
					rejectForAbort();
				} else {
					startSyncSignal?.addEventListener("abort", rejectForAbort, {
						once: true,
					});
				}
			});
		});
		const unhandledRejections: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandledRejections.push(reason);
		};
		process.on("unhandledRejection", onUnhandledRejection);

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["target"],
			});
			expect(startSyncSignal?.aborted).to.equal(false);
			expect(sync.outgoingSyncProcesses.size).to.equal(1);

			await sync.close();
			await new Promise<void>((resolve) => setImmediate(resolve));

			expect(startSyncSignal?.aborted).to.equal(true);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			expect(unhandledRejections).to.deep.equal([]);
		} finally {
			process.removeListener("unhandledRejection", onUnhandledRejection);
			await sync.close();
		}
	});

	it("consumes an asynchronous RequestMoreSymbols follow-up rejection without profiling", async () => {
		const peer = (await Ed25519Keypair.create()).publicKey;
		let rejectFollowUp!: (error: Error) => void;
		const followUpSend = new Promise<void>((_resolve, reject) => {
			rejectFollowUp = reject;
		});
		let followUpSendCount = 0;
		const sync = createDispatchTestSynchronizer((message) => {
			if (message instanceof RequestMoreSymbols) {
				followUpSendCount += 1;
				if (followUpSendCount > 1) {
					return followUpSend;
				}
			}
		});
		const getDecoder = sinon
			.stub(sync as any, "getLocalDecoderForRange")
			.resolves({
				add_coded_symbol: () => {},
				try_decode: () => {},
				decoded: () => false,
				get_remote_symbols: () => [],
				free: () => {},
			});
		const unhandledRejections: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandledRejections.push(reason);
		};
		process.on("unhandledRejection", onUnhandledRejection);

		try {
			const start = new StartSync({ from: 0n, to: 10n, symbols: [] });
			await sync.onMessage(start, {
				from: peer,
			} as any);
			expect(followUpSendCount).to.equal(1);
			const handling = sync.onMessage(
				new MoreSymbols({
					syncId: start.syncId,
					lastSeqNo: 0n,
					symbols: CodedSymbolBatch.fromSymbols([]),
				}),
				{ from: peer } as any,
			);
			await waitForResolved(() => expect(followUpSendCount).to.equal(2));

			rejectFollowUp(new Error("transport failed"));
			expect(await handling).to.equal(true);
			await new Promise<void>((resolve) => setImmediate(resolve));

			expect(unhandledRejections).to.deep.equal([]);
		} finally {
			process.removeListener("unhandledRejection", onUnhandledRejection);
			rejectFollowUp(new Error("test cleanup"));
			getDecoder.restore();
			await sync.close();
		}
	});

	it("stops a captured RequestAll after its first chunk is aborted", async () => {
		const sends: {
			message: TransportMessage;
			options?: { signal?: AbortSignal };
		}[] = [];
		let releaseFirstChunk!: () => void;
		const firstChunkReleased = new Promise<void>((resolve) => {
			releaseFirstChunk = resolve;
		});
		let markFirstChunkStarted!: () => void;
		const firstChunkStarted = new Promise<void>((resolve) => {
			markFirstChunkStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer(async (message, options) => {
			sends.push({ message, options });
			if (
				message instanceof RequestMaybeSync &&
				sends.filter((send) => send.message instanceof RequestMaybeSync)
					.length === 1
			) {
				markFirstChunkStarted();
				await firstChunkReleased;
			}
		});
		const controller = new AbortController();

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(undefined, 3000),
				targets: ["target"],
				signal: controller.signal,
			});
			const startSync = sends.find((send) => send.message instanceof StartSync)
				?.message as StartSync;

			const requestAll = sync.onMessage(
				new RequestAll({ syncId: startSync.syncId }),
				{
					from: { hashcode: () => "target" },
				} as any,
			);
			await firstChunkStarted;
			controller.abort();
			releaseFirstChunk();
			await requestAll;

			const chunks = sends.filter(
				(send) => send.message instanceof RequestMaybeSync,
			);
			expect(chunks).to.have.length(1);
			expect(chunks[0].options?.signal).not.to.equal(controller.signal);
			expect(chunks[0].options?.signal?.aborted).to.equal(true);
			expect((chunks[0].message as RequestMaybeSync).hashes).to.have.length(
				1024,
			);
		} finally {
			controller.abort();
			releaseFirstChunk();
			await sync.close();
		}
	});

	it("does not send MoreSymbols after a captured process aborts", async () => {
		const sends: TransportMessage[] = [];
		const sync = createDispatchTestSynchronizer((message) => {
			sends.push(message);
		});
		const controller = new AbortController();

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["target"],
				signal: controller.signal,
			});
			const startSync = sends.find(
				(message) => message instanceof StartSync,
			) as StartSync;
			const process = [...sync.outgoingSyncProcesses.values()][0];
			let nextCalled = false;
			process.next = () => {
				nextCalled = true;
				controller.abort();
				return CodedSymbolBatch.fromSymbols([]);
			};

			await sync.onMessage(
				new RequestMoreSymbols({
					syncId: startSync.syncId,
					lastSeqNo: 0n,
				}),
				{
					from: { hashcode: () => "target" },
				} as any,
			);

			expect(nextCalled).to.equal(true);
			expect(sends.some((message) => message instanceof MoreSymbols)).to.equal(
				false,
			);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
		} finally {
			controller.abort();
			await sync.close();
		}
	});

	it("cancels an in-flight MoreSymbols send without an unhandled rejection", async () => {
		const sends: {
			message: TransportMessage;
			options?: { signal?: AbortSignal };
		}[] = [];
		let moreSymbolsSignal: AbortSignal | undefined;
		const sync = createDispatchTestSynchronizer((message, options) => {
			sends.push({ message, options });
			if (message instanceof MoreSymbols) {
				moreSymbolsSignal = options?.signal;
				return new Promise<void>((_resolve, reject) => {
					const rejectForAbort = () =>
						reject(moreSymbolsSignal?.reason ?? new Error("transport aborted"));
					if (moreSymbolsSignal?.aborted) {
						rejectForAbort();
					} else {
						moreSymbolsSignal?.addEventListener("abort", rejectForAbort, {
							once: true,
						});
					}
				});
			}
		});
		const controller = new AbortController();
		const unhandledRejections: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandledRejections.push(reason);
		};
		process.on("unhandledRejection", onUnhandledRejection);

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["target"],
				signal: controller.signal,
			});
			const startSync = sends.find((send) => send.message instanceof StartSync)
				?.message as StartSync;
			expect(startSync).to.be.instanceOf(StartSync);
			const outgoingProcess = [...sync.outgoingSyncProcesses.values()][0];
			expect(outgoingProcess).to.not.equal(undefined);
			outgoingProcess.next = () => CodedSymbolBatch.fromSymbols([]);
			const handling = sync.onMessage(
				new RequestMoreSymbols({
					syncId: startSync.syncId,
					lastSeqNo: 0n,
				}),
				{ from: { hashcode: () => "target" } } as any,
			);
			expect(moreSymbolsSignal).not.to.equal(controller.signal);
			expect(moreSymbolsSignal?.aborted).to.equal(false);

			controller.abort(new Error("repair lifecycle cancelled"));
			expect(await handling).to.equal(true);
			expect(moreSymbolsSignal?.aborted).to.equal(true);
			await new Promise<void>((resolve) => setImmediate(resolve));

			expect(
				sends.filter((send) => send.message instanceof MoreSymbols),
			).to.have.length(1);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			expect(unhandledRejections).to.deep.equal([]);
		} finally {
			controller.abort(new Error("test cleanup"));
			process.removeListener("unhandledRejection", onUnhandledRejection);
			await sync.close();
		}
	});

	it("aborts an in-flight MoreSymbols send before RequestAll fallback", async () => {
		const sends: {
			message: TransportMessage;
			options?: { signal?: AbortSignal };
		}[] = [];
		let moreSymbolsSignal: AbortSignal | undefined;
		let markMoreSymbolsStarted!: () => void;
		const moreSymbolsStarted = new Promise<void>((resolve) => {
			markMoreSymbolsStarted = resolve;
		});
		const sync = createDispatchTestSynchronizer((message, options) => {
			sends.push({ message, options });
			if (!(message instanceof MoreSymbols)) {
				return;
			}
			moreSymbolsSignal = options?.signal;
			markMoreSymbolsStarted();
			return new Promise<void>((_resolve, reject) => {
				const rejectForAbort = () =>
					reject(moreSymbolsSignal?.reason ?? new Error("transport aborted"));
				if (moreSymbolsSignal?.aborted) {
					rejectForAbort();
				} else {
					moreSymbolsSignal?.addEventListener("abort", rejectForAbort, {
						once: true,
					});
				}
			});
		});
		const from = { hashcode: () => "target" } as any;

		try {
			await sync.onMaybeMissingEntries({
				entries: createDispatchTestEntries(),
				targets: ["target"],
			});
			const startSync = sends.find((send) => send.message instanceof StartSync)
				?.message as StartSync;
			const handlingMore = sync.onMessage(
				new RequestMoreSymbols({
					syncId: startSync.syncId,
					lastSeqNo: 0n,
				}),
				{ from } as any,
			);
			await moreSymbolsStarted;
			expect(moreSymbolsSignal?.aborted).to.equal(false);

			const handlingAll = sync.onMessage(
				new RequestAll({ syncId: startSync.syncId }),
				{ from } as any,
			);
			expect(await handlingMore).to.equal(true);
			expect(await handlingAll).to.equal(true);

			expect(moreSymbolsSignal?.aborted).to.equal(true);
			expect(sync.outgoingSyncProcesses.size).to.equal(0);
			expect(
				sends.filter((send) => send.message instanceof RequestMaybeSync),
			).to.have.length(1);
		} finally {
			await sync.close();
		}
	});

	it("skips decoded remote symbols that are already present locally", async () => {
		const sentMessages: TransportMessage[] = [];
		let localPresenceChecks = 0;
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: {
				send: async (message: TransportMessage) => {
					sentMessages.push(message);
				},
			} as any,
			rangeIndex: {} as any,
			entryIndex: {
				count: async () => {
					localPresenceChecks += 1;
					return 1;
				},
			} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 1000, ttl: 1000 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});

		(sync as any).getLocalDecoderForRange = async () => ({
			add_coded_symbol: () => {},
			try_decode: () => {},
			decoded: () => true,
			get_remote_symbols: () => [42n],
			free: () => {},
		});

		try {
			await sync.onMessage(
				new StartSync({
					from: 0n,
					to: 100n,
					symbols: [{ count: 0n, hash: 0n, symbol: 0n } as any],
				}),
				{
					from: {
						hashcode: () => "peer-a",
						equals: () => false,
					},
				} as any,
			);

			await waitForResolved(() => expect(localPresenceChecks).to.equal(1));
			expect(
				sentMessages.some(
					(message) => message instanceof RequestMaybeSyncCoordinate,
				),
			).to.equal(false);
		} finally {
			await sync.close();
		}
	});

	it("many missing", async function () {
		this.timeout(120_000);
		const syncedCount = 3000;
		const unsyncedCount = 3000;

		await setupLogs(syncedCount, unsyncedCount);
		const db1Messages = await collectMessages(db1);
		const db2Messages = await collectMessages(db2);

		await session!.connect();

		const expectedCount = syncedCount + unsyncedCount * 2;
		await Promise.all([
			waitForResolved(
				() => expect(db1.log.log.length).to.equal(expectedCount),
				{ timeout: 60_000 },
			),
			waitForResolved(
				() => expect(db2.log.log.length).to.equal(expectedCount),
				{ timeout: 60_000 },
			),
		]);

		const totalRequestAll =
			countMessages(db1Messages.calls, RequestAll) +
			countMessages(db2Messages.calls, RequestAll);
		// Depending on exchange-head timing, the peers may converge before rateless
		// repair needs to send StartSync. The deterministic dispatch test above
		// covers large-set IBLT selection; this integration path must not fall back
		// to full RequestAll transfer.
		expect(totalRequestAll).to.equal(0);
	});
});
