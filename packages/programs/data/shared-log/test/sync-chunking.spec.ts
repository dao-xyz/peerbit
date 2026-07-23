import { Cache } from "@peerbit/cache";
import { Ed25519Keypair } from "@peerbit/crypto";
import { CONVERGENCE_MESSAGE_PRIORITY } from "@peerbit/stream-interface";
import { expect } from "chai";
import sinon from "sinon";
import { RawExchangeHeadsMessage } from "../src/exchange-heads.js";
import {
	RequestMaybeSync,
	RequestMaybeSyncCoordinateCapabilities,
	RequestMaybeSyncCoordinate,
	ResponseMaybeSync,
	SYNC_MESSAGE_PRIORITY,
	ResponseMaybeSyncCapabilities,
	SimpleSyncronizer,
} from "../src/sync/simple.js";

describe("sync-chunking", () => {
	let peerA: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];
	let peerB: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];

	before(async () => {
		[peerA, peerB] = await Promise.all([
			Ed25519Keypair.create().then((keypair) => keypair.publicKey),
			Ed25519Keypair.create().then((keypair) => keypair.publicKey),
		]);
	});

	const waitFor = async (condition: () => boolean) => {
		for (let i = 0; i < 1_000; i++) {
			if (condition()) {
				return;
			}
			await Promise.resolve();
		}
		throw new Error("condition was not reached");
	};

	it("uses the convergence transport priority for sync messages", () => {
		expect(SYNC_MESSAGE_PRIORITY).to.equal(CONVERGENCE_MESSAGE_PRIORITY);
	});

	it("chunks hash maybe-sync messages", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 2,
			},
		});

		try {
			const entries = new Map<string, any>();
			for (let i = 0; i < 5; i++) {
				entries.set(`h${i}`, { hash: `h${i}` });
			}

			await sync.onMaybeMissingEntries({
				entries: entries as any,
				targets: ["p"],
			});

			expect(send.callCount).to.equal(3);
			const sentHashes = send.getCalls().map((call) => {
				const message = call.args[0];
				expect(call.args[1].priority).to.equal(SYNC_MESSAGE_PRIORITY);
				expect(message).to.be.instanceOf(RequestMaybeSync);
				return (message as RequestMaybeSync).hashes;
			});
			expect(sentHashes.flat()).to.deep.equal(["h0", "h1", "h2", "h3", "h4"]);
			expect(sentHashes.map((x) => x.length)).to.deep.equal([2, 2, 1]);
		} finally {
			await sync.close();
		}
	});

	it("retains authorization for a 10,001-hash tail-only response", async () => {
		const clock = sinon.useFakeTimers();
		try {
			const send = sinon.stub().resolves();
			const sendRawExchangeHeads = sinon.stub().resolves(1);
			const sync = new SimpleSyncronizer<"u64">({
				rpc: { send } as any,
				entryIndex: {} as any,
				log: {} as any,
				coordinateToHash: new Cache<string>({ max: 10 }),
				sendRawExchangeHeads,
				sync: {
					maxSimpleHashesPerMessage: 10_000,
				},
			});
			const hashes = Array.from({ length: 10_001 }, (_, index) => `h${index}`);

			const dispatch = sync.onMaybeMissingHashes({
				hashes,
				targets: [peerA.hashcode()],
			});
			await waitFor(() => send.callCount === 1);
			expect(send.firstCall.args[0].hashes).to.have.length(10_000);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(10_000);

			await clock.tickAsync(30_001);
			await dispatch;
			expect(send.callCount).to.equal(2);
			expect(send.secondCall.args[0].hashes).to.deep.equal(["h10000"]);

			await sync.onMessage(
				new ResponseMaybeSyncCapabilities({ hashes: ["h10000"] }),
				{ from: peerA } as any,
			);
			expect(sendRawExchangeHeads.calledOnce).to.equal(true);
			expect(sendRawExchangeHeads.firstCall.args[0]).to.deep.equal(["h10000"]);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
		} finally {
			clock.restore();
		}
	});

	it("rotates a bounded response window across two 6,000-hash targets", async () => {
		const send = sinon.stub().resolves();
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
			sync: {
				maxSimpleHashesPerMessage: 6_000,
			},
		});
		const hashes = Array.from({ length: 6_000 }, (_, index) => `h${index}`);

		const dispatch = sync.onMaybeMissingHashes({
			hashes,
			targets: [peerA.hashcode(), peerB.hashcode()],
		});
		await waitFor(() => send.callCount === 2);
		expect(send.firstCall.args[1].mode.to).to.deep.equal([peerA.hashcode()]);
		expect(send.secondCall.args[1].mode.to).to.deep.equal([peerB.hashcode()]);
		expect(send.firstCall.args[0].hashes).to.have.length(5_000);
		expect(send.secondCall.args[0].hashes).to.have.length(5_000);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(10_000);

		const firstWindow = hashes.slice(0, 5_000);
		await Promise.all([
			sync.onMessage(
				new ResponseMaybeSyncCapabilities({ hashes: firstWindow }),
				{ from: peerA } as any,
			),
			sync.onMessage(
				new ResponseMaybeSyncCapabilities({ hashes: firstWindow }),
				{ from: peerB } as any,
			),
		]);
		await waitFor(() => send.callCount === 4);
		expect(send.thirdCall.args[1].mode.to).to.deep.equal([peerA.hashcode()]);
		expect(send.getCall(3).args[1].mode.to).to.deep.equal([peerB.hashcode()]);
		expect(send.thirdCall.args[0].hashes).to.deep.equal(hashes.slice(5_000));
		expect(send.getCall(3).args[0].hashes).to.deep.equal(hashes.slice(5_000));
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(2_000);

		const tail = hashes.slice(5_000);
		await Promise.all([
			sync.onMessage(new ResponseMaybeSyncCapabilities({ hashes: tail }), {
				from: peerA,
			} as any),
			sync.onMessage(new ResponseMaybeSyncCapabilities({ hashes: tail }), {
				from: peerB,
			} as any),
		]);
		await dispatch;

		expect(sendRawExchangeHeads.callCount).to.equal(4);
		expect(sendRawExchangeHeads.firstCall.args[1]).to.deep.equal([
			peerA.hashcode(),
		]);
		expect(sendRawExchangeHeads.secondCall.args[1]).to.deep.equal([
			peerB.hashcode(),
		]);
		expect(sendRawExchangeHeads.thirdCall.args[1]).to.deep.equal([
			peerA.hashcode(),
		]);
		expect(sendRawExchangeHeads.getCall(3).args[1]).to.deep.equal([
			peerB.hashcode(),
		]);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
	});

	it("fences a blocked signal-less dispatch across close and reopen", async () => {
		let releaseSend!: () => void;
		const sendReleased = new Promise<void>((resolve) => {
			releaseSend = resolve;
		});
		let markSendStarted!: () => void;
		const sendStarted = new Promise<void>((resolve) => {
			markSendStarted = resolve;
		});
		let dispatchSignal: AbortSignal | undefined;
		const send = sinon
			.stub()
			.callsFake(
				async (
					_message: RequestMaybeSync,
					options: { signal?: AbortSignal },
				) => {
					dispatchSignal = options.signal;
					markSendStarted();
					await sendReleased;
				},
			);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 2,
			},
		});

		const dispatch = sync.onMaybeMissingHashes({
			hashes: ["h0", "h1", "h2"],
			targets: [peerA.hashcode()],
		});
		await sendStarted;
		await sync.close();
		await sync.open();
		releaseSend();
		await dispatch;

		expect(dispatchSignal).not.to.equal(undefined);
		expect(dispatchSignal?.aborted).to.equal(true);
		expect(send.callCount).to.equal(1);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
		await sync.close();
	});

	it("aborts a live-caller fused response when the synchronizer closes", async () => {
		const send = sinon.stub().resolves();
		let markResponseStarted!: () => void;
		const responseStarted = new Promise<void>((resolve) => {
			markResponseStarted = resolve;
		});
		let responseSignal: AbortSignal | undefined;
		const sendRawExchangeHeads = sinon
			.stub()
			.callsFake(
				async (
					_hashes: string[],
					_targets: string[],
					options?: { signal?: AbortSignal },
				) => {
					responseSignal = options?.signal;
					markResponseStarted();
					await new Promise<void>((_resolve, reject) => {
						const rejectForAbort = () =>
							reject(responseSignal?.reason ?? new Error("aborted"));
						if (responseSignal?.aborted) {
							rejectForAbort();
						} else {
							responseSignal?.addEventListener("abort", rejectForAbort, {
								once: true,
							});
						}
					});
					return 1;
				},
			);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});
		const caller = new AbortController();

		await sync.onMaybeMissingHashes({
			hashes: ["hash"],
			targets: [peerA.hashcode()],
			signal: caller.signal,
		});
		const handling = sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["hash"] }),
			{ from: peerA } as any,
		);
		await responseStarted;
		await sync.close();

		expect(await handling).to.equal(true);
		expect(caller.signal.aborted).to.equal(false);
		expect(responseSignal).not.to.equal(caller.signal);
		expect(responseSignal?.aborted).to.equal(true);
	});

	it("invalidates a capacity waiter on disconnect until a fresh dispatch", async () => {
		const send = sinon.stub().resolves();
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
			sync: { maxSimpleHashesPerMessage: 10_000 },
		});
		const occupyingHashes = Array.from(
			{ length: 10_000 },
			(_, index) => `occupying-${index}`,
		);
		await sync.onMaybeMissingHashes({
			hashes: occupyingHashes,
			targets: [peerA.hashcode()],
		});

		const staleDispatch = sync.onMaybeMissingHashes({
			hashes: ["stale"],
			targets: [peerB.hashcode()],
		});
		await waitFor(
			() => (sync as any).pendingMaybeSyncResponseWaiters.size === 1,
		);
		sync.onPeerDisconnected(peerB);
		await staleDispatch;

		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: occupyingHashes }),
			{ from: peerA } as any,
		);
		expect(send.callCount).to.equal(1);

		await sync.onMaybeMissingHashes({
			hashes: ["fresh"],
			targets: [peerB.hashcode()],
		});
		expect(send.callCount).to.equal(2);
		expect(send.secondCall.args[0].hashes).to.deep.equal(["fresh"]);
		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["fresh"] }),
			{ from: peerB } as any,
		);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
	});

	it("aborts an in-flight target send on disconnect and fences later chunks", async () => {
		let blockSend = true;
		let markSendStarted!: () => void;
		const sendStarted = new Promise<void>((resolve) => {
			markSendStarted = resolve;
		});
		const send = sinon
			.stub()
			.callsFake(
				async (
					_message: RequestMaybeSync,
					options: { signal?: AbortSignal },
				) => {
					if (!blockSend) {
						return;
					}
					markSendStarted();
					await new Promise<void>((_resolve, reject) => {
						const rejectForAbort = () =>
							reject(options.signal?.reason ?? new Error("aborted"));
						if (options.signal?.aborted) {
							rejectForAbort();
						} else {
							options.signal?.addEventListener("abort", rejectForAbort, {
								once: true,
							});
						}
					});
				},
			);
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
			sync: { maxSimpleHashesPerMessage: 1 },
		});

		const staleDispatch = sync.onMaybeMissingHashes({
			hashes: ["stale-0", "stale-1"],
			targets: [peerB.hashcode()],
		});
		await sendStarted;
		const staleSignal = send.firstCall.args[1].signal as AbortSignal;
		sync.onPeerDisconnected(peerB);
		await staleDispatch;
		expect(staleSignal.aborted).to.equal(true);
		expect(send.callCount).to.equal(1);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);

		blockSend = false;
		await sync.onMaybeMissingHashes({
			hashes: ["fresh"],
			targets: [peerB.hashcode()],
		});
		expect(send.callCount).to.equal(2);
		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["fresh"] }),
			{ from: peerB } as any,
		);
	});

	it("releases first-chunk authorization after an ordinary send failure", async () => {
		const failure = new Error("transport failed");
		const send = sinon.stub();
		send.onFirstCall().rejects(failure);
		send.onSecondCall().resolves();
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
			sync: {
				maxSimpleHashesPerMessage: 2,
			},
		});

		let thrown: unknown;
		try {
			await sync.onMaybeMissingHashes({
				hashes: ["h0", "h1", "h2", "h3", "h4"],
				targets: [peerA.hashcode()],
			});
		} catch (error) {
			thrown = error;
		}
		expect(thrown).to.equal(failure);
		expect(send.callCount).to.equal(1);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);

		await sync.onMaybeMissingHashes({
			hashes: ["recovery"],
			targets: [peerA.hashcode()],
		});
		expect(send.callCount).to.equal(2);
		expect(send.secondCall.args[0].hashes).to.deep.equal(["recovery"]);
		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["recovery"] }),
			{ from: peerA } as any,
		);
		expect(sendRawExchangeHeads.calledOnce).to.equal(true);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
	});

	it("does not resend hashes that already retain matching authorization", async () => {
		const send = sinon.stub().resolves();
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});

		await sync.onMaybeMissingHashes({
			hashes: ["hash"],
			targets: [peerA.hashcode()],
		});
		await sync.onMaybeMissingHashes({
			hashes: ["hash"],
			targets: [peerA.hashcode()],
		});

		expect(send.callCount).to.equal(1);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(1);
		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["hash"] }),
			{ from: peerA } as any,
		);
	});

	it("advances a 10,001-hash window as soon as a response frees capacity", async () => {
		const send = sinon.stub().resolves();
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
			sync: { maxSimpleHashesPerMessage: 10_000 },
		});
		const hashes = Array.from({ length: 10_001 }, (_, index) => `h${index}`);

		const dispatch = sync.onMaybeMissingHashes({
			hashes,
			targets: [peerA.hashcode()],
		});
		await waitFor(() => send.callCount === 1);
		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["h0"] }),
			{ from: peerA } as any,
		);
		await dispatch;

		expect(send.callCount).to.equal(2);
		expect(send.secondCall.args[0].hashes).to.deep.equal(["h10000"]);
		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({
				hashes: [...hashes.slice(1, 10_000), "h10000"],
			}),
			{ from: peerA } as any,
		);
		expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
	});

	it("stops hash chunk dispatch after an in-flight send is aborted", async () => {
		const profileEvents: any[] = [];
		let releaseFirstSend!: () => void;
		const firstSendReleased = new Promise<void>((resolve) => {
			releaseFirstSend = resolve;
		});
		let markFirstSendStarted!: () => void;
		const firstSendStarted = new Promise<void>((resolve) => {
			markFirstSendStarted = resolve;
		});
		const send = sinon.stub().callsFake(async () => {
			markFirstSendStarted();
			await firstSendReleased;
		});
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 2,
				profile: (event) => profileEvents.push(event),
			},
		});
		const entries = new Map<string, any>();
		for (let i = 0; i < 5; i++) {
			entries.set(`h${i}`, { hash: `h${i}` });
		}
		const controller = new AbortController();

		const dispatch = sync.onMaybeMissingEntries({
			entries,
			targets: ["p"],
			signal: controller.signal,
		});
		await firstSendStarted;
		controller.abort();
		releaseFirstSend();
		await dispatch;

		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0]).to.be.instanceOf(RequestMaybeSync);
		expect(send.firstCall.args[0].hashes).to.deep.equal(["h0", "h1"]);
		expect(send.firstCall.args[1].signal).not.to.equal(controller.signal);
		expect(send.firstCall.args[1].signal.aborted).to.equal(true);
		const profile = profileEvents.find(
			(event) => event.name === "simple.onMaybeMissingEntries",
		);
		expect(profile.messages).to.equal(1);
		expect(profile.details?.cancelled).to.equal(true);
	});

	it("handles transport abort rejection during hash chunk dispatch", async () => {
		let markSendStarted!: () => void;
		const sendStarted = new Promise<void>((resolve) => {
			markSendStarted = resolve;
		});
		const send = sinon
			.stub()
			.callsFake(
				async (
					_message: RequestMaybeSync,
					options: { signal?: AbortSignal },
				) => {
					markSendStarted();
					await new Promise<void>((_resolve, reject) => {
						options.signal?.addEventListener(
							"abort",
							() => reject(options.signal?.reason),
							{ once: true },
						);
					});
				},
			);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 2,
			},
		});
		const entries = new Map<string, any>();
		for (let i = 0; i < 5; i++) {
			entries.set(`h${i}`, { hash: `h${i}` });
		}
		const controller = new AbortController();

		const dispatch = sync.onMaybeMissingEntries({
			entries,
			targets: ["p"],
			signal: controller.signal,
		});
		await sendStarted;
		controller.abort(new Error("cancelled"));
		await dispatch;

		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[1].signal).not.to.equal(controller.signal);
		expect(send.firstCall.args[1].signal.aborted).to.equal(true);
	});

	it("does not count an aborted TypeScript exchange-head send as sent", async () => {
		const controller = new AbortController();
		const failure = new Error("cancelled");
		const send = sinon
			.stub()
			.callsFake(
				async (_message: unknown, options: { signal?: AbortSignal }) => {
					controller.abort(failure);
					throw options.signal?.reason;
				},
			);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {
				get: async (hash: string) => ({
					hash,
					size: 1,
					meta: { gid: `gid-${hash}` },
				}),
				entryIndex: { getUniqueReferenceGids: () => [] },
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		const shipped = await (sync as any).shipExchangeHeads(
			["head-a"],
			peerA,
			false,
			controller.signal,
		);

		expect(send.calledOnce).to.equal(true);
		expect(shipped).to.deep.equal({ messages: 0, fused: false });
	});

	it("does not let a delayed simple response borrow a newer lifecycle signal", async () => {
		const send = sinon.stub().resolves();
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});
		const target = peerA.hashcode();
		const oldController = new AbortController();

		await sync.onMaybeMissingHashes({
			hashes: ["old-hash"],
			targets: [target],
			signal: oldController.signal,
		});
		oldController.abort(new Error("old ownership lifecycle closed"));
		expect(
			await sync.onMessage(
				new ResponseMaybeSyncCapabilities({ hashes: ["old-hash"] }),
				{ from: peerA } as any,
			),
		).to.equal(true);
		expect(sendRawExchangeHeads.called).to.equal(false);

		const currentController = new AbortController();
		await sync.onMaybeMissingHashes({
			hashes: ["current-hash"],
			targets: [target],
			signal: currentController.signal,
		});
		expect(
			await sync.onMessage(
				new ResponseMaybeSyncCapabilities({
					hashes: ["old-hash", "current-hash"],
				}),
				{ from: peerA } as any,
			),
		).to.equal(true);

		expect(sendRawExchangeHeads.calledOnce).to.equal(true);
		expect(sendRawExchangeHeads.firstCall.args[0]).to.deep.equal([
			"current-hash",
		]);
		const responseSignal = sendRawExchangeHeads.firstCall.args[2].signal;
		expect(responseSignal).not.to.equal(currentController.signal);
		expect(responseSignal.aborted).to.equal(false);
		currentController.abort();
	});

	it("cancels an in-flight simple response payload with its request signal", async () => {
		const send = sinon.stub().resolves();
		let responseStarted!: () => void;
		const responseStart = new Promise<void>((resolve) => {
			responseStarted = resolve;
		});
		let capturedSignal: AbortSignal | undefined;
		const sendRawExchangeHeads = sinon
			.stub()
			.callsFake(
				async (
					_hashes: string[],
					_targets: string[],
					options?: { signal?: AbortSignal },
				) => {
					capturedSignal = options?.signal;
					responseStarted();
					await new Promise<void>((_resolve, reject) => {
						const rejectForAbort = () =>
							reject(capturedSignal?.reason ?? new Error("aborted"));
						if (capturedSignal?.aborted) {
							rejectForAbort();
						} else {
							capturedSignal?.addEventListener("abort", rejectForAbort, {
								once: true,
							});
						}
					});
					return 1;
				},
			);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});
		const controller = new AbortController();

		await sync.onMaybeMissingHashes({
			hashes: ["hash"],
			targets: [peerA.hashcode()],
			signal: controller.signal,
		});
		const handling = sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["hash"] }),
			{ from: peerA } as any,
		);
		await responseStart;
		expect(capturedSignal).not.to.equal(controller.signal);

		controller.abort(new Error("ownership lifecycle closed"));
		expect(await handling).to.equal(true);
		expect(capturedSignal?.aborted).to.equal(true);
		expect(sendRawExchangeHeads.calledOnce).to.equal(true);
	});

	it("rejects a delayed simple response after its peer disconnects", async () => {
		const send = sinon.stub().resolves();
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});

		await sync.onMaybeMissingHashes({
			hashes: ["old-hash"],
			targets: [peerA.hashcode()],
		});
		sync.onPeerDisconnected(peerA);
		expect(
			await sync.onMessage(
				new ResponseMaybeSyncCapabilities({ hashes: ["old-hash"] }),
				{ from: peerA } as any,
			),
		).to.equal(true);

		expect(sendRawExchangeHeads.called).to.equal(false);
	});

	it("does not let a blocked old open loop resume after close and reopen", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		let releaseHas!: (value: boolean) => void;
		const blockedHas = new Promise<boolean>((resolve) => {
			releaseHas = resolve;
		});
		let markHasStarted!: () => void;
		const hasStarted = new Promise<void>((resolve) => {
			markHasStarted = resolve;
		});
		const has = sinon.stub().callsFake(() => {
			markHasStarted();
			return blockedHas;
		});
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		try {
			await sync.open();
			await waitFor(() => (sync as any).syncMoreInterval !== undefined);
			await sync.queueSync(["hash"], peerA, { skipCheck: true });
			sync.syncInFlight.get(peerA.hashcode())!.get("hash")!.timestamp = 0;
			send.resetHistory();

			await clock.tickAsync(3_000);
			await hasStarted;
			await sync.close();
			await sync.open();
			releaseHas(false);
			await Promise.resolve();
			await clock.tickAsync(3_000);

			expect(send.called).to.equal(false);
			expect(has.calledOnce).to.equal(true);
			expect(sync.pending).to.equal(0);
		} finally {
			releaseHas(false);
			await sync.close();
			clock.restore();
		}
	});

	it("observes a background request rejection without an unhandled rejection", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const send = sinon.stub();
		send.onFirstCall().resolves();
		send.onSecondCall().rejects(new Error("background transport failed"));
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const unhandledRejections: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandledRejections.push(reason);
		};
		process.on("unhandledRejection", onUnhandledRejection);

		try {
			await sync.open();
			await waitFor(() => (sync as any).syncMoreInterval !== undefined);
			await sync.queueSync(["hash"], peerA, { skipCheck: true });
			sync.syncInFlight.get(peerA.hashcode())!.get("hash")!.timestamp = 0;

			await clock.tickAsync(3_000);
			await new Promise<void>((resolve) => setImmediate(resolve));

			expect(send.callCount).to.equal(2);
			expect(unhandledRejections).to.deep.equal([]);
		} finally {
			process.removeListener("unhandledRejection", onUnhandledRejection);
			await sync.close();
			clock.restore();
		}
	});

	it("chunks coordinate maybe-sync requests", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: { count: async () => 0 } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleCoordinatesPerMessage: 2,
			},
		});

		await sync.queueSync(
			[1n, 2n, 3n, 4n, 5n],
			{
				hashcode: () => "peer-a",
				equals: () => false,
			} as any,
			{ skipCheck: true },
		);

		expect(send.callCount).to.equal(3);
		const sentCoordinates = send.getCalls().map((call) => {
			const message = call.args[0];
			expect(call.args[1].priority).to.equal(SYNC_MESSAGE_PRIORITY);
			expect(message).to.be.instanceOf(RequestMaybeSyncCoordinate);
			return (message as RequestMaybeSyncCoordinate).hashNumbers;
		});
		expect(sentCoordinates.flat()).to.deep.equal([1n, 2n, 3n, 4n, 5n]);
		expect(sentCoordinates.map((x) => x.length)).to.deep.equal([2, 2, 1]);
	});

	it("uses native resolver for coordinate queue preflight", async () => {
		const send = sinon.stub().resolves();
		const count = sinon.stub().throws(new Error("entry index should not be used"));
		const resolveHashesForSymbols = sinon
			.stub()
			.returns(new Map([[42n, ["head-a"]]]));
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { count } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashesForSymbols,
		});

		await sync.queueSync(
			[42n, 7n],
			{
				hashcode: () => "peer-a",
				equals: () => false,
			} as any,
		);

		expect(count.called).to.equal(false);
		expect(resolveHashesForSymbols.firstCall.args[0]).to.deep.equal([42n, 7n]);
		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0]).to.be.instanceOf(RequestMaybeSyncCoordinate);
		expect(send.firstCall.args[0].hashNumbers).to.deep.equal([7n]);
	});

	it("uses native coordinate symbol resolver before index lookup", async () => {
		const send = sinon.stub().resolves();
		const iterate = sinon.stub().throws(new Error("entry index should not be used"));
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: { iterate } as any,
			log: {
				get: async (hash: string) => ({
					hash,
					size: 1,
					meta: { gid: `gid-${hash}` },
				}),
				entryIndex: { getUniqueReferenceGids: () => [] },
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashesForSymbols: (symbols) => {
				expect(symbols).to.deep.equal([42n]);
				return new Map([[42n, ["head-a"]]]);
			},
		});

		await sync.onMessage(
			new RequestMaybeSyncCoordinate({ hashNumbers: [42n] }),
			{ from: peerA } as any,
		);

		expect(iterate.called).to.equal(false);
		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0].heads.map((x: any) => x.entry.hash)).to.deep.equal([
			"head-a",
		]);
	});

	it("uses native flat coordinate symbol resolver for response lookup", async () => {
		const send = sinon.stub().resolves();
		const iterate = sinon.stub().throws(new Error("entry index should not be used"));
		const resolveHashesForSymbols = sinon
			.stub()
			.throws(new Error("map resolver should not be used"));
		const resolveHashListForSymbols = sinon.stub().returns(["head-a"]);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { iterate } as any,
			log: {
				get: async (hash: string) => ({
					hash,
					size: 1,
					meta: { gid: `gid-${hash}` },
				}),
				entryIndex: { getUniqueReferenceGids: () => [] },
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashesForSymbols,
			resolveHashListForSymbols,
		});

		await sync.onMessage(
			new RequestMaybeSyncCoordinate({ hashNumbers: [42n] }),
			{ from: peerA } as any,
		);

		expect(iterate.called).to.equal(false);
		expect(resolveHashListForSymbols.calledOnce).to.equal(true);
		expect(resolveHashListForSymbols.firstCall.args[0]).to.deep.equal([42n]);
		expect(resolveHashesForSymbols.called).to.equal(false);
		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0].heads.map((x: any) => x.entry.hash)).to.deep.equal([
			"head-a",
		]);
	});

	it("splits mixed hash and coordinate maybe-sync batches by type", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: { count: async () => 0 } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 8,
				maxSimpleCoordinatesPerMessage: 8,
			},
		});

		await (sync as any).requestSync(["h1", 2n, "h2", 4n], ["peer-a"]);

		expect(send.callCount).to.equal(2);

		const sentHashMessages = send
			.getCalls()
			.filter((call) => {
				expect(call.args[1].priority).to.equal(SYNC_MESSAGE_PRIORITY);
				return true;
			})
			.map((call) => call.args[0])
			.filter((message) => message instanceof ResponseMaybeSync);
		expect(sentHashMessages).to.have.length(1);
		expect((sentHashMessages[0] as ResponseMaybeSync).hashes).to.deep.equal([
			"h1",
			"h2",
		]);

		const sentCoordinateMessages = send
			.getCalls()
			.map((call) => call.args[0])
			.filter((message) => message instanceof RequestMaybeSyncCoordinate);
		expect(sentCoordinateMessages).to.have.length(1);
		expect(
			(sentCoordinateMessages[0] as RequestMaybeSyncCoordinate).hashNumbers,
		).to.deep.equal([2n, 4n]);
	});

	it("advertises raw exchange-head support when enabled", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { count: async () => 0 } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				rawExchangeHeads: true,
				maxSimpleHashesPerMessage: 8,
				maxSimpleCoordinatesPerMessage: 8,
			},
		});

		await (sync as any).requestSync(["h1", 2n], ["peer-a"]);

		const messages = send.getCalls().map((call) => call.args[0]);
		const hashMessage = messages.find(
			(message) => message instanceof ResponseMaybeSyncCapabilities,
		) as ResponseMaybeSyncCapabilities | undefined;
		const coordinateMessage = messages.find(
			(message) => message instanceof RequestMaybeSyncCoordinateCapabilities,
		) as RequestMaybeSyncCoordinateCapabilities | undefined;
		expect(hashMessage?.hashes).to.deep.equal(["h1"]);
		expect(coordinateMessage?.hashNumbers).to.deep.equal([2n]);
	});

	it("responds with raw exchange heads only to capable requests", async () => {
		const send = sinon.stub().resolves();
		const get = sinon.stub().throws(new Error("full entry get should not be used"));
		const getMany = sinon.stub().resolves([new Uint8Array([1, 2, 3])]);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {
				get,
				blocks: { getMany },
				entryIndex: {
					getUniqueReferenceGidRowsFlatBatch: sinon.stub().returns([]),
				},
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		await sync.onMaybeMissingHashes({
			hashes: ["head-a"],
			targets: [peerA.hashcode()],
		});
		send.resetHistory();
		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["head-a"] }),
			{ from: peerA } as any,
		);

		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0]).to.be.instanceOf(RawExchangeHeadsMessage);
		expect(send.firstCall.args[0].heads.map((head: any) => head.hash)).to.deep.equal(
			["head-a"],
		);
		expect(get.called).to.equal(false);
		expect(getMany.calledOnceWithExactly(["head-a"])).to.equal(true);
	});
});
