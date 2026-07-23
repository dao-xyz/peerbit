import { Cache } from "@peerbit/cache";
import { Ed25519Keypair } from "@peerbit/crypto";
import { CONVERGENCE_MESSAGE_PRIORITY } from "@peerbit/stream-interface";
import { expect } from "chai";
import sinon from "sinon";
import { RawExchangeHeadsMessage } from "../src/exchange-heads.js";
import {
	MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER,
	MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER,
	MAX_PENDING_SIMPLE_SYNC_KEYS_GLOBAL,
	MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
	MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
	MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS,
	MAX_SIMPLE_COORDINATE_RESPONSE_HASHES,
	MAX_SIMPLE_SYNC_RETRY_KEYS_PER_TICK,
	PENDING_SIMPLE_SYNC_KEY_TTL_MS,
	RequestMaybeSync,
	RequestMaybeSyncCoordinate,
	RequestMaybeSyncCoordinateCapabilities,
	ResponseMaybeSync,
	ResponseMaybeSyncCapabilities,
	SYNC_MESSAGE_PRIORITY,
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

	it("expires staggered response authorizations without scanning batch sets", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const batches = (sync as any)
			.pendingMaybeSyncResponseBatches as Set<unknown>;
		const reservations: { release: () => void }[] = [];

		try {
			for (let index = 0; index < 64; index += 1) {
				const reservation = sync.expectMaybeSyncResponse({
					hashes: [`response-expiry-${index}`],
					targets: [peerA.hashcode()],
				});
				expect(reservation).to.not.equal(undefined);
				reservations.push(reservation!);
				await clock.tickAsync(1);
			}
			expect((sync as any).pendingMaybeSyncResponseExpiryHeap).to.have.length(
				64,
			);
			Object.defineProperty(batches, Symbol.iterator, {
				configurable: true,
				value: () => {
					throw new Error("response batch set must not be scanned");
				},
			});

			await clock.tickAsync(30_000);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
			expect((sync as any).pendingMaybeSyncResponseExpiryHeap).to.have.length(
				0,
			);
		} finally {
			delete (batches as any)[Symbol.iterator];
			for (const reservation of reservations) {
				reservation.release();
			}
			await sync.close();
			clock.restore();
		}
	});

	it("enforces response authorization TTL without waiting for its timer", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		let expired: ReturnType<typeof sync.expectMaybeSyncResponse> | undefined;
		let replaced: ReturnType<typeof sync.expectMaybeSyncResponse> | undefined;

		try {
			expired = sync.expectMaybeSyncResponse({
				hashes: ["expired-response"],
				targets: [peerA.hashcode()],
			});
			replaced = sync.expectMaybeSyncResponse({
				hashes: ["replace-response"],
				targets: [peerA.hashcode()],
			});
			expect(expired).to.not.equal(undefined);
			expect(replaced).to.not.equal(undefined);

			clock.setSystemTime(130_001);
			expect(expired!.retained()).to.equal(false);
			expect(
				sync.consumeAuthorizedMaybeSyncResponse(["expired-response"], peerA),
			).to.deep.equal([]);

			const fresh = sync.expectMaybeSyncResponse({
				hashes: ["replace-response"],
				targets: [peerA.hashcode()],
			});
			expect(replaced!.retained()).to.equal(false);
			expect(fresh).to.not.equal(undefined);
			expect(fresh!.retained()).to.equal(true);
			const leases = sync.consumeAuthorizedMaybeSyncResponse(
				["replace-response"],
				peerA,
			);
			expect(leases.map((lease) => lease.hashes)).to.deep.equal([
				["replace-response"],
			]);
			for (const lease of leases) {
				lease.release();
			}
			fresh!.release();
		} finally {
			expired?.release();
			replaced?.release();
			await sync.close();
			clock.restore();
		}
	});

	it("reclaims an expired full response quota before admitting an unrelated hash", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		let full: ReturnType<typeof sync.expectMaybeSyncResponse> | undefined;
		let fresh: ReturnType<typeof sync.expectMaybeSyncResponse> | undefined;

		try {
			full = sync.expectMaybeSyncResponse({
				hashes: Array.from(
					{ length: 10_000 },
					(_, index) => `expired-cap-${index}`,
				),
				targets: [peerA.hashcode()],
			});
			expect(full).to.not.equal(undefined);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(10_000);

			// Move wall time past the deadline without running the timer queue.
			clock.setSystemTime(130_001);
			fresh = sync.expectMaybeSyncResponse({
				hashes: ["fresh-unrelated"],
				targets: [peerA.hashcode()],
			});

			expect(fresh).to.not.equal(undefined);
			expect(fresh!.retained()).to.equal(true);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(1);
		} finally {
			fresh?.release();
			full?.release();
			await sync.close();
			clock.restore();
		}
	});

	it("bounds inspected hashes in an unauthorized maybe-sync response", async () => {
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const reservation = sync.expectMaybeSyncResponse({
			hashes: ["authorized-tail"],
			targets: [peerA.hashcode()],
		});
		let yielded = 0;
		const adversarial = {
			*[Symbol.iterator]() {
				for (let index = 0; index < 20_000; index += 1) {
					yielded += 1;
					yield `unauthorized-${index}`;
				}
				yielded += 1;
				yield "authorized-tail";
			},
		};

		try {
			expect(
				sync.consumeAuthorizedMaybeSyncResponse(adversarial, peerA),
			).to.deep.equal([]);
			expect(yielded).to.equal(MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER);
			expect(reservation!.retained()).to.equal(true);
		} finally {
			reservation?.release();
			await sync.close();
		}
	});

	it("retains bounded active response work across close and reopen", async () => {
		const releases: (() => void)[] = [];
		const sendRawExchangeHeads = sinon.stub().callsFake(
			() =>
				new Promise<number>((resolve) => {
					releases.push(() => resolve(1));
				}),
		);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});
		const hashes = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
			(_, index) => `active-response-${index}`,
		);
		const reservation = sync.expectMaybeSyncResponse({
			hashes,
			targets: [peerA.hashcode()],
		});
		const active: Promise<boolean>[] = [];

		try {
			for (
				let index = 0;
				index < MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER;
				index += 1
			) {
				active.push(
					sync.onMessage(
						new ResponseMaybeSyncCapabilities({ hashes: [hashes[index]!] }),
						{ from: peerA } as any,
					),
				);
			}
			await waitFor(
				() =>
					sendRawExchangeHeads.callCount ===
					MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER,
			);

			await sync.onMessage(
				new ResponseMaybeSyncCapabilities({
					hashes: [hashes[MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER]!],
				}),
				{ from: peerA } as any,
			);
			expect(sendRawExchangeHeads.callCount).to.equal(
				MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER,
			);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);

			await sync.close();
			await sync.open();
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(
				MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER,
			);
			expect((sync as any).activeMaybeSyncResponseCount).to.equal(
				MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER,
			);
			expect(
				sync.expectMaybeSyncResponse({
					hashes: Array.from(
						{
							length:
								MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER -
								MAX_ACTIVE_SIMPLE_SYNC_RESPONSES_PER_PEER +
								1,
						},
						(_, index) => `blocked-reopen-${index}`,
					),
					targets: [peerB.hashcode()],
				}),
			).to.equal(undefined);

			for (const release of releases) {
				release();
			}
			await Promise.all(active);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
			expect((sync as any).activeMaybeSyncResponseCount).to.equal(0);

			const reclaimed = sync.expectMaybeSyncResponse({
				hashes: Array.from(
					{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
					(_, index) => `reclaimed-response-${index}`,
				),
				targets: [peerB.hashcode()],
			});
			expect(reclaimed).to.not.equal(undefined);
			reclaimed!.release();
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
		} finally {
			for (const release of releases) {
				release();
			}
			await Promise.all(active);
			reservation?.release();
			await sync.close();
		}
	});

	it("disposes many-target response lifecycles without per-batch target scans", async () => {
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const reservation = sync.expectMaybeSyncResponse({
			hashes: ["many-target-response"],
			targets: Array.from({ length: 512 }, (_, index) => `target-${index}`),
		});
		const batch = [...(sync as any).pendingMaybeSyncResponseBatches][0] as any;
		const targets = batch.targetLifecycle.lifecycle.targets as Map<
			string,
			unknown
		>;
		const originalValues = targets.values.bind(targets);
		let valuesCalls = 0;
		Object.defineProperty(targets, "values", {
			configurable: true,
			value: () => {
				valuesCalls += 1;
				return originalValues();
			},
		});

		try {
			reservation!.release();
			expect(valuesCalls).to.be.lessThan(3);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
		} finally {
			delete (targets as any).values;
			await sync.close();
		}
	});

	it("wakes bounded response waiters as charged capacity is released", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const occupyingHashes = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
			(_, index) => `waiter-occupant-${index}`,
		);
		const occupying = sync.expectMaybeSyncResponse({
			hashes: occupyingHashes,
			targets: [peerA.hashcode()],
		});
		const blocked = Array.from({ length: 64 }, (_, index) =>
			sync.onMaybeMissingHashes({
				hashes: [`waiter-${index}`],
				targets: [`waiter-target-${index}`],
			}),
		);
		let reserve: ReturnType<typeof sinon.spy> | undefined;

		try {
			await waitFor(
				() => (sync as any).pendingMaybeSyncResponseWaiters.size === 64,
			);
			reserve = sinon.spy(sync as any, "tryReservePendingMaybeSyncResponse");
			const leases = sync.consumeAuthorizedMaybeSyncResponse(
				[occupyingHashes[0]!],
				peerA,
			);
			expect(leases).to.have.length(1);
			reserve.resetHistory();
			leases[0]!.release();

			await waitFor(() => send.calledOnce);
			expect(reserve.callCount).to.equal(1);
			expect((sync as any).pendingMaybeSyncResponseWaiters.size).to.equal(63);
		} finally {
			reserve?.restore();
			await sync.close();
			await Promise.all(blocked);
			occupying?.release();
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

	it("retains blocked request-send quota across close and reopen", async () => {
		let releaseFirstSend!: () => void;
		const firstSendReleased = new Promise<void>((resolve) => {
			releaseFirstSend = resolve;
		});
		const send = sinon.stub();
		send.onFirstCall().returns(firstSendReleased);
		send.onSecondCall().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: { maxSimpleHashesPerMessage: 10_000 },
		});
		const fullWindow = Array.from(
			{ length: 10_000 },
			(_, index) => `blocked-generation-${index}`,
		);

		try {
			const staleDispatch = sync.onMaybeMissingHashes({
				hashes: fullWindow,
				targets: [peerA.hashcode()],
			});
			await waitFor(() => send.callCount === 1);

			await sync.close();
			await sync.open();
			const freshDispatch = sync.onMaybeMissingHashes({
				hashes: ["fresh-generation"],
				targets: [peerA.hashcode()],
			});
			await Promise.resolve();

			expect(send.callCount).to.equal(1);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(10_000);

			releaseFirstSend();
			await Promise.all([staleDispatch, freshDispatch]);

			expect(send.callCount).to.equal(2);
			expect(send.secondCall.args[0].hashes).to.deep.equal([
				"fresh-generation",
			]);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(1);
		} finally {
			releaseFirstSend();
			await sync.close();
		}
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

	it("keeps unrelated hashes when another caller owns an overlapping authorization", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const callerA = new AbortController();
		const callerB = new AbortController();

		try {
			await sync.onMaybeMissingHashes({
				hashes: ["overlap"],
				targets: [peerA.hashcode()],
				signal: callerA.signal,
			});
			const secondDispatch = sync.onMaybeMissingHashes({
				hashes: ["overlap", "new-b", "new-c"],
				targets: [peerA.hashcode()],
				signal: callerB.signal,
			});
			await waitFor(() => send.callCount === 2);

			expect(send.callCount).to.equal(2);
			expect(send.firstCall.args[0].hashes).to.deep.equal(["overlap"]);
			expect(send.secondCall.args[0].hashes).to.deep.equal(["new-b", "new-c"]);
			const overlapLeases = sync.consumeAuthorizedMaybeSyncResponse(
				["overlap"],
				peerA,
			);
			for (const lease of overlapLeases) {
				lease.release({ fulfilled: true });
			}
			await secondDispatch;
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(2);
		} finally {
			await sync.close();
		}
	});

	it("retries a conflicting hash when its pending authorization is aborted", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const callerA = new AbortController();
		const callerB = new AbortController();

		try {
			await sync.onMaybeMissingHashes({
				hashes: ["contended"],
				targets: [peerA.hashcode()],
				signal: callerA.signal,
			});
			const follower = sync.onMaybeMissingHashes({
				hashes: ["contended"],
				targets: [peerA.hashcode()],
				signal: callerB.signal,
			});
			await Promise.resolve();
			expect(send.callCount).to.equal(1);

			callerA.abort(new Error("first caller stopped"));
			await follower;

			expect(send.callCount).to.equal(2);
			expect(send.secondCall.args[0]).to.be.instanceOf(RequestMaybeSync);
			expect(send.secondCall.args[0].hashes).to.deep.equal(["contended"]);
		} finally {
			await sync.close();
		}
	});

	it("retries a no-signal conflict when the first transport send fails", async () => {
		let rejectFirstSend!: (error: Error) => void;
		const firstSend = new Promise<void>((_resolve, reject) => {
			rejectFirstSend = reject;
		});
		const send = sinon.stub();
		send.onFirstCall().returns(firstSend);
		send.onSecondCall().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		try {
			const firstDispatch = sync
				.onMaybeMissingHashes({
					hashes: ["no-signal-conflict"],
					targets: [peerA.hashcode()],
				})
				.catch((error) => error);
			await waitFor(() => send.callCount === 1);
			const follower = sync.onMaybeMissingHashes({
				hashes: ["no-signal-conflict"],
				targets: [peerA.hashcode()],
			});
			await waitFor(
				() => (sync as any).pendingMaybeSyncResponseConflictWaiterCount === 1,
			);
			expect(send.callCount).to.equal(1);

			const failure = new Error("first transport failed");
			rejectFirstSend(failure);
			expect(await firstDispatch).to.equal(failure);
			await follower;

			expect(send.callCount).to.equal(2);
			expect(send.secondCall.args[0]).to.be.instanceOf(RequestMaybeSync);
			expect(send.secondCall.args[0].hashes).to.deep.equal([
				"no-signal-conflict",
			]);
		} finally {
			await sync.close();
		}
	});

	it("wakes a same-scope follower when the first transport send commits", async () => {
		let releaseFirstSend!: () => void;
		const firstSend = new Promise<void>((resolve) => {
			releaseFirstSend = resolve;
		});
		const send = sinon.stub();
		send.onFirstCall().returns(firstSend);
		send.onSecondCall().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		try {
			const firstDispatch = sync.onMaybeMissingHashes({
				hashes: ["committed-conflict"],
				targets: [peerA.hashcode()],
			});
			await waitFor(() => send.callCount === 1);
			const follower = sync.onMaybeMissingHashes({
				hashes: ["committed-conflict"],
				targets: [peerA.hashcode()],
			});
			await waitFor(
				() => (sync as any).pendingMaybeSyncResponseConflictWaiterCount === 1,
			);

			releaseFirstSend();
			await Promise.all([firstDispatch, follower]);

			expect(send.callCount).to.equal(1);
			expect(
				(sync as any).pendingMaybeSyncResponseConflictWaiterCount,
			).to.equal(0);
			expect(
				(sync as any).pendingMaybeSyncResponseWaiterAssociationCount,
			).to.equal(0);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(1);
		} finally {
			releaseFirstSend();
			await sync.close();
		}
	});

	it("retries a conflicting hash when its accepted response shipment aborts", async () => {
		const send = sinon.stub().resolves();
		let markShipmentStarted!: () => void;
		const shipmentStarted = new Promise<void>((resolve) => {
			markShipmentStarted = resolve;
		});
		const sendRawExchangeHeads = sinon
			.stub()
			.callsFake(
				async (
					_hashes: string[],
					_targets: string[],
					options?: { signal?: AbortSignal },
				) => {
					markShipmentStarted();
					await new Promise<void>((_resolve, reject) => {
						const rejectForAbort = () =>
							reject(options?.signal?.reason ?? new Error("aborted"));
						if (options?.signal?.aborted) {
							rejectForAbort();
						} else {
							options?.signal?.addEventListener("abort", rejectForAbort, {
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
		const callerA = new AbortController();
		const callerB = new AbortController();

		try {
			await sync.onMaybeMissingHashes({
				hashes: ["contended-active"],
				targets: [peerA.hashcode()],
				signal: callerA.signal,
			});
			const follower = sync.onMaybeMissingHashes({
				hashes: ["contended-active"],
				targets: [peerA.hashcode()],
				signal: callerB.signal,
			});
			const response = sync.onMessage(
				new ResponseMaybeSyncCapabilities({
					hashes: ["contended-active"],
				}),
				{ from: peerA } as any,
			);
			await shipmentStarted;
			expect(send.callCount).to.equal(1);

			callerA.abort(new Error("accepted shipment stopped"));
			await response.catch(() => undefined);
			await follower;

			expect(send.callCount).to.equal(2);
			expect(send.secondCall.args[0]).to.be.instanceOf(RequestMaybeSync);
			expect(send.secondCall.args[0].hashes).to.deep.equal([
				"contended-active",
			]);
		} finally {
			await sync.close();
		}
	});

	it("retries a same-scope conflict when an active response shipment fails", async () => {
		const send = sinon.stub().resolves();
		let rejectShipment!: (error: Error) => void;
		let markShipmentStarted!: () => void;
		const shipmentStarted = new Promise<void>((resolve) => {
			markShipmentStarted = resolve;
		});
		const shipment = new Promise<number>((_resolve, reject) => {
			rejectShipment = reject;
		});
		const sendRawExchangeHeads = sinon.stub().callsFake(async () => {
			markShipmentStarted();
			return shipment;
		});
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});

		try {
			await sync.onMaybeMissingHashes({
				hashes: ["active-no-signal"],
				targets: [peerA.hashcode()],
			});
			const response = sync.onMessage(
				new ResponseMaybeSyncCapabilities({
					hashes: ["active-no-signal"],
				}),
				{ from: peerA } as any,
			);
			await shipmentStarted;
			const follower = sync.onMaybeMissingHashes({
				hashes: ["active-no-signal"],
				targets: [peerA.hashcode()],
			});
			await waitFor(
				() => (sync as any).pendingMaybeSyncResponseConflictWaiterCount === 1,
			);

			const failure = new Error("payload shipment failed");
			rejectShipment(failure);
			expect(await response.catch((error) => error)).to.equal(failure);
			await follower;

			expect(send.callCount).to.equal(2);
			expect(send.secondCall.args[0].hashes).to.deep.equal([
				"active-no-signal",
			]);
		} finally {
			await sync.close();
		}
	});

	it("starts the response TTL only after a blocked request send commits", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		let releaseSend!: () => void;
		const blockedSend = new Promise<void>((resolve) => {
			releaseSend = resolve;
		});
		const send = sinon.stub().returns(blockedSend);
		const sendRawExchangeHeads = sinon.stub().resolves(1);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sendRawExchangeHeads,
		});

		try {
			const dispatch = sync.onMaybeMissingHashes({
				hashes: ["slow-request"],
				targets: [peerA.hashcode()],
			});
			await waitFor(() => send.calledOnce);

			// Move wall time past the normal response deadline without resolving
			// the transport send or running its timer queue.
			clock.setSystemTime(130_001);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(1);
			expect((sync as any).pendingMaybeSyncResponseExpiryHeap).to.have.length(
				0,
			);

			releaseSend();
			await dispatch;
			expect((sync as any).pendingMaybeSyncResponseExpiryHeap).to.have.length(
				1,
			);

			clock.setSystemTime(160_000);
			expect(
				await sync.onMessage(
					new ResponseMaybeSyncCapabilities({ hashes: ["slow-request"] }),
					{ from: peerA } as any,
				),
			).to.equal(true);
			expect(sendRawExchangeHeads.calledOnce).to.equal(true);
			expect((sync as any).pendingMaybeSyncResponseCount).to.equal(0);
		} finally {
			releaseSend();
			await sync.close();
			clock.restore();
		}
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

	it("retries only each selected peer's hashes without a cross product", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has: sinon.stub().resolves(false) } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		try {
			await sync.open();
			await waitFor(() => (sync as any).syncMoreInterval !== undefined);
			await sync.queueSync(["from-a"], peerA, { skipCheck: true });
			await sync.queueSync(["from-b"], peerB, { skipCheck: true });
			sync.syncInFlight.get(peerA.hashcode())!.get("from-a")!.timestamp = 0;
			sync.syncInFlight.get(peerB.hashcode())!.get("from-b")!.timestamp = 0;
			send.resetHistory();

			await clock.tickAsync(3_000);
			await waitFor(() => send.callCount === 2);

			const hashesByTarget = new Map<string, string[]>();
			for (const call of send.getCalls()) {
				const message = call.args[0] as ResponseMaybeSync;
				const targets = call.args[1].mode.to as string[];
				expect(targets).to.have.length(1);
				hashesByTarget.set(targets[0]!, message.hashes);
			}
			expect(hashesByTarget.get(peerA.hashcode())).to.deep.equal(["from-a"]);
			expect(hashesByTarget.get(peerB.hashcode())).to.deep.equal(["from-b"]);
			expect([
				...sync.syncInFlight.get(peerA.hashcode())!.keys(),
			]).to.deep.equal(["from-a"]);
			expect([
				...sync.syncInFlight.get(peerB.hashcode())!.keys(),
			]).to.deep.equal(["from-b"]);
		} finally {
			await sync.close();
			clock.restore();
		}
	});

	it("drops retry work captured before a peer disconnects and reconnects", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		let releaseSecond!: (present: boolean) => void;
		const blockedSecond = new Promise<boolean>((resolve) => {
			releaseSecond = resolve;
		});
		const has = sinon.stub();
		has.onFirstCall().resolves(false);
		has.onSecondCall().returns(blockedSecond);
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const hashes = ["stale-retry", "satisfied-retry"];

		try {
			await sync.open();
			await waitFor(() => (sync as any).syncMoreInterval !== undefined);
			await sync.queueSync(hashes, peerA, { skipCheck: true });
			await sync.queueSync(hashes, peerB, { skipCheck: true });
			for (const state of sync.syncInFlight.get(peerA.hashcode())!.values()) {
				state.timestamp = 0;
			}
			const oldEpoch = (sync as any).syncDispatchTargetEpochs.get(
				peerA.hashcode(),
			);
			send.resetHistory();

			await clock.tickAsync(3_000);
			await waitFor(() => has.callCount === 2);

			sync.onPeerDisconnected(peerA);
			await sync.queueSync(hashes, peerA, { skipCheck: true });
			expect(
				(sync as any).syncDispatchTargetEpochs.get(peerA.hashcode()),
			).to.not.equal(oldEpoch);
			send.resetHistory();

			releaseSecond(true);
			await new Promise<void>((resolve) => setImmediate(resolve));

			expect(send.callCount).to.equal(0);
			expect(sync.syncInFlightQueue.has("satisfied-retry")).to.equal(false);
		} finally {
			releaseSecond?.(true);
			await sync.close();
			clock.restore();
		}
	});

	it("bounds persistent retry index checks per timer tick", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const send = sinon.stub().resolves();
		const has = sinon.stub().resolves(false);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { has } as any,
			coordinateToHash: new Cache<string>({
				max: MAX_SIMPLE_SYNC_RETRY_KEYS_PER_TICK + 1,
			}),
		});
		const hashes = Array.from(
			{ length: MAX_SIMPLE_SYNC_RETRY_KEYS_PER_TICK + 1 },
			(_, index) => `retry-budget-${index}`,
		);

		try {
			await sync.open();
			await waitFor(() => (sync as any).syncMoreInterval !== undefined);
			await sync.queueSync(hashes, peerA, { skipCheck: true });
			for (const state of sync.syncInFlight.get(peerA.hashcode())!.values()) {
				state.timestamp = 0;
			}
			has.resetHistory();
			send.resetHistory();

			await clock.tickAsync(3_000);
			expect(has.callCount).to.equal(MAX_SIMPLE_SYNC_RETRY_KEYS_PER_TICK);

			await clock.tickAsync(3_000);
			expect(has.callCount).to.equal(hashes.length);
		} finally {
			await sync.close();
			clock.restore();
		}
	});

	it("caps incoming maybe-sync claims per peer before persistent lookup", async () => {
		const send = sinon.stub().resolves();
		const hasMany = sinon.stub().resolves([]);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({
				max: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER + 1,
			}),
		});
		const hashes = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER + 1 },
			(_, index) => `peer-cap-${index}`,
		);

		try {
			expect(
				await sync.onMessage(new RequestMaybeSync({ hashes }), {
					from: peerA,
				} as any),
			).to.equal(true);

			expect(hasMany.calledOnce).to.equal(true);
			expect(hasMany.firstCall.args[0]).to.have.length(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);
			expect(sync.pending).to.equal(MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER);
			expect(
				sync.syncInFlightQueueInverted.get(peerA.hashcode())?.size,
			).to.equal(MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER);
			expect((sync as any).pendingSyncClaimCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);
			expect(sync.syncInFlightQueue.has(hashes.at(-1)!)).to.equal(false);

			const refreshAliases = sinon.spy(
				sync as any,
				"refreshQueuedSyncCoordinateAliases",
			);
			await sync.queueSync([], peerA);
			await sync.queueSync(["over-cap"], peerA);
			expect(refreshAliases.called).to.equal(false);
			expect(hasMany.calledOnce).to.equal(true);
		} finally {
			await sync.close();
		}
	});

	it("refreshes late coordinate aliases with bounded per-message work", async () => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 1_000 });
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash,
		});
		const coordinates = Array.from({ length: 512 }, (_, index) =>
			BigInt(index),
		);
		const aliasPeer = { hashcode: () => "alias-peer" } as any;

		try {
			await sync.queueSync(coordinates, peerA, { skipCheck: true });
			coordinateToHash.add(0n, "late-coordinate-hash");
			const get = sinon.spy(coordinateToHash, "get");
			send.resetHistory();

			await sync.queueSync(["late-coordinate-hash"], aliasPeer, {
				skipCheck: true,
			});

			expect(get.callCount).to.be.lessThanOrEqual(132);
			expect(send.calledOnce).to.equal(true);
			expect(send.firstCall.args[0]).to.be.instanceOf(
				RequestMaybeSyncCoordinate,
			);
			expect(send.firstCall.args[0].hashNumbers).to.deep.equal([0n]);
			expect(
				sync.syncInFlightQueue.get(0n)?.map((peer) => peer.hashcode()),
			).to.deep.equal([peerA.hashcode(), aliasPeer.hashcode()]);
			expect(sync.syncInFlightQueue.has("late-coordinate-hash")).to.equal(
				false,
			);
		} finally {
			await sync.close();
		}
	});

	it("bounds stale reverse-alias inspection and resumes at a valid alias", async () => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 1_000 });
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash,
		});
		const coordinates = Array.from({ length: 129 }, (_, index) =>
			BigInt(index),
		);
		const alias = "bounded-reverse-alias";

		try {
			await sync.queueSync(coordinates, peerA, { skipCheck: true });
			coordinateToHash.add(coordinates.at(-1)!, alias);
			(sync as any).syncInFlightQueuedCoordinatesByHash.set(
				alias,
				new Set(coordinates),
			);
			for (const coordinate of coordinates) {
				(sync as any).syncInFlightQueuedHashByCoordinate.set(coordinate, alias);
			}
			const get = sinon.spy(coordinateToHash, "get");

			const deferred = (sync as any).getQueuedSyncKeyForAdmission(alias);
			expect(typeof deferred).to.equal("symbol");
			expect(get.callCount).to.equal(128);
			expect(sync.syncInFlightQueue.has(alias)).to.equal(false);

			get.resetHistory();
			expect((sync as any).getQueuedSyncKeyForAdmission(alias)).to.equal(
				coordinates.at(-1),
			);
			expect(get.callCount).to.be.lessThanOrEqual(2);
		} finally {
			await sync.close();
		}
	});

	it("reconciles a late coordinate alias discovered across bounded refreshes", async () => {
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 1_000 });
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash,
		});
		const coordinates = Array.from({ length: 512 }, (_, index) =>
			BigInt(index),
		);
		const lateCoordinate = coordinates[300]!;
		const lateHash = "late-bounded-alias";

		try {
			await sync.queueSync(coordinates, peerA, { skipCheck: true });
			coordinateToHash.add(lateCoordinate, lateHash);
			await sync.queueSync([lateHash], peerB, { skipCheck: true });
			expect(sync.syncInFlightQueue.has(lateHash)).to.equal(true);
			expect(sync.pending).to.equal(coordinates.length + 1);

			await sync.queueSync([lateHash], peerB, { skipCheck: true });
			await sync.queueSync([lateHash], peerB, { skipCheck: true });

			expect(sync.syncInFlightQueue.has(lateHash)).to.equal(false);
			expect(
				sync.syncInFlightQueue
					.get(lateCoordinate)
					?.map((peer) => peer.hashcode()),
			).to.deep.equal([peerA.hashcode(), peerB.hashcode()]);
			expect(sync.pending).to.equal(coordinates.length);
		} finally {
			await sync.close();
		}
	});

	it("requests a newly admitted shared-key source before unrelated lookup work settles", async () => {
		let releaseLookup!: (hashes: string[]) => void;
		const hasMany = sinon.stub().returns(
			new Promise<string[]>((resolve) => {
				releaseLookup = resolve;
			}),
		);
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		try {
			await sync.queueSync(["shared-key"], peerA, { skipCheck: true });
			send.resetHistory();

			const handling = sync.queueSync(["shared-key", "new-key"], peerB);
			await waitFor(() => hasMany.calledOnce && send.calledOnce);

			expect(send.firstCall.args[0]).to.be.instanceOf(ResponseMaybeSync);
			expect(send.firstCall.args[0].hashes).to.deep.equal(["shared-key"]);
			expect(send.firstCall.args[1].mode.to).to.deep.equal([peerB.hashcode()]);

			releaseLookup([]);
			await handling;
			expect(send.callCount).to.equal(2);
			expect(send.secondCall.args[0].hashes).to.deep.equal(["new-key"]);
		} finally {
			releaseLookup?.([]);
			await sync.close();
		}
	});

	it("filters locally satisfied keys before a delayed initial dispatch", async () => {
		let releaseSecond!: (count: number) => void;
		const secondCount = new Promise<number>((resolve) => {
			releaseSecond = resolve;
		});
		const count = sinon.stub();
		count.onFirstCall().resolves(0);
		count.onSecondCall().returns(secondCount);
		const send = sinon.stub().resolves();
		const coordinateToHash = new Cache<string>({ max: 10 });
		coordinateToHash.add(1n, "satisfied-before-dispatch");
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { count } as any,
			log: { has: sinon.stub().resolves(false) } as any,
			coordinateToHash,
		});

		try {
			const handling = sync.queueSync([1n, 2n], peerA);
			await waitFor(() => count.callCount === 2);
			sync.onEntryAddedHash("satisfied-before-dispatch");
			releaseSecond(0);
			await handling;

			expect(send.calledOnce).to.equal(true);
			expect(send.firstCall.args[0]).to.be.instanceOf(
				RequestMaybeSyncCoordinate,
			);
			expect(send.firstCall.args[0].hashNumbers).to.deep.equal([2n]);
			expect(sync.syncInFlightQueue.has(1n)).to.equal(false);
		} finally {
			releaseSecond?.(0);
			await sync.close();
		}
	});

	it("checks repeated shared-key claimants in constant time", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});
		const claimantHashes = Array.from({ length: 256 }, (_, index) =>
			sinon.spy(() => `shared-claimant-${index}`),
		);
		const claimants = claimantHashes.map((hashcode) => ({ hashcode }) as any);

		try {
			for (const claimant of claimants) {
				await sync.queueSync(["shared-key"], claimant, { skipCheck: true });
			}
			for (const hashcode of claimantHashes) {
				hashcode.resetHistory();
			}

			await sync.queueSync(["shared-key"], claimants.at(-1)!, {
				skipCheck: true,
			});

			expect(
				claimantHashes.slice(0, -1).every((hashcode) => hashcode.notCalled),
			).to.equal(true);
			expect(claimantHashes.at(-1)!.callCount).to.equal(2);

			for (const hashcode of claimantHashes) {
				hashcode.resetHistory();
			}
			sync.onPeerDisconnected(claimants[100]!);
			expect(claimantHashes[100]!.callCount).to.equal(1);
			expect(claimantHashes.at(-1)!.callCount).to.equal(1);
			expect(
				claimantHashes
					.filter((_, index) => index !== 100 && index !== 255)
					.every((hashcode) => hashcode.notCalled),
			).to.equal(true);
		} finally {
			await sync.close();
		}
	});

	it("caps global maybe-sync peer-key claims even when keys are shared", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({
				max: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			}),
		});
		const hashes = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
			(_, index) => `global-cap-${index}`,
		);
		const peerCount =
			MAX_PENDING_SIMPLE_SYNC_KEYS_GLOBAL /
			MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER;
		const peers = Array.from({ length: peerCount + 1 }, (_, index) => {
			return { hashcode: () => `claimant-${index}` } as any;
		});

		try {
			for (const peer of peers) {
				await sync.queueSync(hashes, peer, { skipCheck: true });
			}

			expect(sync.pending).to.equal(MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER);
			expect((sync as any).pendingSyncClaimCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_GLOBAL,
			);
			for (const peer of peers.slice(0, peerCount)) {
				expect(
					sync.syncInFlightQueueInverted.get(peer.hashcode())?.size,
				).to.equal(MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER);
			}
			expect(
				sync.syncInFlightQueueInverted.has(peers.at(-1)!.hashcode()),
			).to.equal(false);
			expect(sync.syncInFlightQueue.get(hashes[0]!)).to.have.length(peerCount);
		} finally {
			await sync.close();
		}
	});

	it("does not refresh repeated claims and expires all retry state for reuse", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const send = sinon.stub().resolves();
		const hasMany = sinon.stub().resolves([]);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		try {
			await sync.onMessage(new RequestMaybeSync({ hashes: ["claim"] }), {
				from: peerA,
			} as any);
			const firstDeadline = (sync as any).syncInFlightQueueExpiresAt.get(
				"claim",
			);
			expect(firstDeadline).to.equal(100_000 + PENDING_SIMPLE_SYNC_KEY_TTL_MS);
			expect(sync.syncInFlight.get(peerA.hashcode())?.has("claim")).to.equal(
				true,
			);

			await clock.tickAsync(PENDING_SIMPLE_SYNC_KEY_TTL_MS - 1);
			await sync.onMessage(
				new RequestMaybeSync({ hashes: ["claim", "claim"] }),
				{
					from: peerA,
				} as any,
			);
			expect(hasMany.calledOnce).to.equal(true);
			expect(send.calledOnce).to.equal(true);
			expect((sync as any).syncInFlightQueueExpiresAt.get("claim")).to.equal(
				firstDeadline,
			);
			expect((sync as any).pendingSyncClaimCount).to.equal(1);

			await clock.tickAsync(1);
			expect(sync.pending).to.equal(0);
			expect(sync.syncInFlightQueueInverted.size).to.equal(0);
			expect(sync.syncInFlight.size).to.equal(0);
			expect((sync as any).pendingSyncClaimCount).to.equal(0);

			await sync.onMessage(new RequestMaybeSync({ hashes: ["claim"] }), {
				from: peerA,
			} as any);
			expect(hasMany.callCount).to.equal(2);
			expect(send.callCount).to.equal(2);
			expect(sync.pending).to.equal(1);
			expect((sync as any).syncInFlightQueueExpiresAt.get("claim")).to.equal(
				firstDeadline + PENDING_SIMPLE_SYNC_KEY_TTL_MS,
			);
		} finally {
			await sync.close();
			clock.restore();
		}
	});

	it("reclaims an expired full claim quota before checking fresh admission", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({
				max: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			}),
		});

		try {
			await sync.queueSync(
				Array.from(
					{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
					(_, index) => `expired-claim-${index}`,
				),
				peerA,
				{ skipCheck: true },
			);
			expect(sync.pending).to.equal(MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER);

			// Advance Date without letting the scheduled expiry callback run.
			clock.setSystemTime(100_000 + PENDING_SIMPLE_SYNC_KEY_TTL_MS + 1);
			await sync.queueSync(["fresh-claim"], peerA, { skipCheck: true });

			expect(sync.pending).to.equal(1);
			expect(sync.syncInFlightQueue.has("fresh-claim")).to.equal(true);
			expect((sync as any).pendingSyncClaimCount).to.equal(1);
		} finally {
			await sync.close();
			clock.restore();
		}
	});

	it("expires staggered keys from the indexed deadline heap without map rescans", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 100 }),
		});
		const expiryMap = (sync as any).syncInFlightQueueExpiresAt as Map<
			string,
			number
		>;

		try {
			for (let index = 0; index < 64; index += 1) {
				await sync.queueSync([`staggered-${index}`], peerA, {
					skipCheck: true,
				});
				await clock.tickAsync(1);
			}
			expect((sync as any).pendingSyncExpiryHeap).to.have.length(64);
			Object.defineProperty(expiryMap, Symbol.iterator, {
				configurable: true,
				value: () => {
					throw new Error("expiry map must not be scanned");
				},
			});

			await clock.tickAsync(PENDING_SIMPLE_SYNC_KEY_TTL_MS);
			expect(sync.pending).to.equal(0);
			expect((sync as any).pendingSyncExpiryHeap).to.have.length(0);
		} finally {
			delete (expiryMap as any)[Symbol.iterator];
			await sync.close();
			clock.restore();
		}
	});

	it("clears a received hash through reverse indexes without scanning targets", async () => {
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 1_000 }),
		});
		const hashes = Array.from(
			{ length: 512 },
			(_, index) => `reverse-cleanup-${index}`,
		);

		try {
			await sync.queueSync(hashes, peerA, { skipCheck: true });
			Object.defineProperty(sync.syncInFlight, Symbol.iterator, {
				configurable: true,
				value: () => {
					throw new Error("target maps must not be scanned");
				},
			});

			sync.onEntryAddedHash(hashes[0]!);
			expect(sync.pending).to.equal(hashes.length - 1);
			expect(sync.syncInFlight.get(peerA.hashcode())?.has(hashes[0]!)).to.equal(
				false,
			);
		} finally {
			delete (sync.syncInFlight as any)[Symbol.iterator];
			await sync.close();
		}
	});

	it("expires lookup reservations and rejects their late results", async () => {
		const clock = sinon.useFakeTimers({
			now: 100_000,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		let resolveLookup!: (hashes: string[]) => void;
		const blockedLookup = new Promise<string[]>((resolve) => {
			resolveLookup = resolve;
		});
		const hasMany = sinon.stub();
		hasMany.onFirstCall().returns(blockedLookup);
		hasMany.onSecondCall().resolves([]);
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({
				max: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER + 1,
			}),
		});
		const blockedHashes = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
			(_, index) => `late-${index}`,
		);

		try {
			const handling = sync.onMessage(
				new RequestMaybeSync({ hashes: blockedHashes }),
				{ from: peerA } as any,
			);
			await waitFor(() => hasMany.calledOnce);
			expect((sync as any).pendingSyncAdmissionCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);

			await clock.tickAsync(PENDING_SIMPLE_SYNC_KEY_TTL_MS);
			// Expiry invalidates the late result but cannot release the quota slot
			// while the non-abortable lookup is still consuming resources.
			expect((sync as any).pendingSyncAdmissionCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);
			await sync.onMessage(new RequestMaybeSync({ hashes: ["fresh"] }), {
				from: peerA,
			} as any);
			expect(hasMany.calledOnce).to.equal(true);
			resolveLookup([]);
			await handling;
			expect((sync as any).pendingSyncAdmissionCount).to.equal(0);
			expect(sync.pending).to.equal(0);
			expect(send.called).to.equal(false);

			await sync.onMessage(new RequestMaybeSync({ hashes: ["fresh"] }), {
				from: peerA,
			} as any);
			expect(hasMany.callCount).to.equal(2);
			expect(sync.pending).to.equal(1);
			expect(send.calledOnce).to.equal(true);
		} finally {
			resolveLookup([]);
			await sync.close();
			clock.restore();
		}
	});

	it("retains locally settled admission quota until blocked lookup work settles", async () => {
		let resolveLookup!: (hashes: string[]) => void;
		const hasMany = sinon.stub();
		hasMany.onFirstCall().returns(
			new Promise<string[]>((resolve) => {
				resolveLookup = resolve;
			}),
		);
		hasMany.onSecondCall().resolves([]);
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({
				max: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER + 1,
			}),
		});
		const hashes = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
			(_, index) => `locally-settled-${index}`,
		);

		try {
			const handling = sync.onMessage(new RequestMaybeSync({ hashes }), {
				from: peerA,
			} as any);
			await waitFor(() => hasMany.calledOnce);

			sync.onEntryAddedHashes(hashes);
			expect((sync as any).pendingSyncAdmissionCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);
			expect(
				(sync as any).pendingSyncAdmissionReservationsByIdentity.size,
			).to.equal(0);

			await sync.onMessage(
				new RequestMaybeSync({ hashes: ["must-wait-for-old-lookup"] }),
				{ from: peerA } as any,
			);
			expect(hasMany.calledOnce).to.equal(true);

			resolveLookup(hashes);
			await handling;
			expect((sync as any).pendingSyncAdmissionCount).to.equal(0);
			expect((sync as any).pendingSyncAdmissionCountByPeer.size).to.equal(0);

			await sync.onMessage(
				new RequestMaybeSync({ hashes: ["after-old-lookup"] }),
				{ from: peerA } as any,
			);
			expect(hasMany.callCount).to.equal(2);
			expect(send.calledOnce).to.equal(true);
		} finally {
			resolveLookup?.(hashes);
			await sync.close();
		}
	});

	it("counts a promoted claimant separately until its blocked resolver settles", async () => {
		let resolveLookup!: (hashes: string[]) => void;
		const hasMany = sinon.stub().returns(
			new Promise<string[]>((resolve) => {
				resolveLookup = resolve;
			}),
		);
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({
				max: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			}),
		});
		const shared = "promoted-shared";
		const reserved = [
			shared,
			...Array.from(
				{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER - 2 },
				(_, index) => `promoted-reserved-${index}`,
			),
		];

		try {
			const blocked = sync.onMessage(
				new RequestMaybeSync({ hashes: reserved }),
				{ from: peerA } as any,
			);
			await waitFor(() => hasMany.calledOnce);
			await sync.queueSync([shared], peerB, { skipCheck: true });
			send.resetHistory();

			await sync.onMessage(new RequestMaybeSync({ hashes: [shared] }), {
				from: peerA,
			} as any);

			expect(
				sync.syncInFlightQueue.get(shared)?.map((peer) => peer.hashcode()),
			).to.deep.equal([peerB.hashcode(), peerA.hashcode()]);
			expect((sync as any).pendingSyncAdmissionCount).to.equal(reserved.length);
			expect((sync as any).pendingSyncClaimCount).to.equal(2);
			expect(
				(sync as any).pendingSyncAdmissionCountByPeer.get(peerA.hashcode()) +
					(sync.syncInFlightQueueInverted.get(peerA.hashcode())?.size ?? 0),
			).to.equal(MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER);
			expect(send.calledOnce).to.equal(true);
			expect(send.firstCall.args[1].mode.to).to.deep.equal([peerA.hashcode()]);

			sync.onEntryAddedHash(shared);
			expect((sync as any).pendingSyncClaimCount).to.equal(0);
			expect((sync as any).pendingSyncAdmissionCount).to.equal(reserved.length);

			resolveLookup(reserved);
			await blocked;
			expect((sync as any).pendingSyncAdmissionCount).to.equal(0);
			expect(sync.pending).to.equal(0);
		} finally {
			resolveLookup?.(reserved);
			await sync.close();
		}
	});

	it("bounds concurrent lookup reservations and reclaims them on disconnect", async () => {
		let resolveLookup!: (hashes: string[]) => void;
		const blockedLookup = new Promise<string[]>((resolve) => {
			resolveLookup = resolve;
		});
		const hasMany = sinon.stub();
		hasMany.onFirstCall().returns(blockedLookup);
		hasMany.onSecondCall().resolves([]);
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({
				max: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER * 2,
			}),
		});
		const first = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
			(_, index) => `blocked-${index}`,
		);
		const second = Array.from(
			{ length: MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER },
			(_, index) => `rejected-while-blocked-${index}`,
		);

		try {
			const blocked = sync.onMessage(new RequestMaybeSync({ hashes: first }), {
				from: peerA,
			} as any);
			await waitFor(() => hasMany.calledOnce);
			await sync.onMessage(new RequestMaybeSync({ hashes: first }), {
				from: peerA,
			} as any);
			await sync.onMessage(new RequestMaybeSync({ hashes: second }), {
				from: peerA,
			} as any);

			expect(hasMany.calledOnce).to.equal(true);
			expect(sync.pending).to.equal(0);
			expect((sync as any).pendingSyncAdmissionCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);

			sync.onPeerDisconnected(peerA);
			expect((sync as any).pendingSyncAdmissionCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_KEYS_PER_PEER,
			);
			await sync.onMessage(new RequestMaybeSync({ hashes: second }), {
				from: peerA,
			} as any);
			expect(hasMany.calledOnce).to.equal(true);
			resolveLookup([]);
			await blocked;
			expect((sync as any).pendingSyncAdmissionCount).to.equal(0);
			expect(sync.pending).to.equal(0);

			await sync.onMessage(new RequestMaybeSync({ hashes: ["reclaimed"] }), {
				from: peerA,
			} as any);
			expect(hasMany.callCount).to.equal(2);
			expect(sync.pending).to.equal(1);
		} finally {
			resolveLookup([]);
			await sync.close();
		}
	});

	it("bounds one-key resolver calls across disconnect until they settle", async () => {
		const releases: ((hashes: string[]) => void)[] = [];
		let blockLookups = true;
		const hasMany = sinon.stub().callsFake(() =>
			blockLookups
				? new Promise<string[]>((resolve) => {
						releases.push(resolve);
					})
				: Promise.resolve([]),
		);
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: { hasMany } as any,
			coordinateToHash: new Cache<string>({ max: 100 }),
		});

		try {
			const blocked = Array.from(
				{ length: MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER },
				(_, index) =>
					sync.onMessage(
						new RequestMaybeSync({ hashes: [`blocked-call-${index}`] }),
						{ from: peerA } as any,
					),
			);
			await waitFor(
				() => hasMany.callCount === MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);

			await sync.onMessage(
				new RequestMaybeSync({ hashes: ["over-call-cap"] }),
				{ from: peerA } as any,
			);
			expect(hasMany.callCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);

			sync.onPeerDisconnected(peerA);
			await sync.onMessage(
				new RequestMaybeSync({ hashes: ["over-call-cap-after-disconnect"] }),
				{ from: peerA } as any,
			);
			expect(hasMany.callCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);

			blockLookups = false;
			for (const release of releases) {
				release([]);
			}
			await Promise.all(blocked);
			expect((sync as any).pendingSyncAdmissionReservations.size).to.equal(0);

			await sync.onMessage(
				new RequestMaybeSync({ hashes: ["after-call-cap"] }),
				{ from: peerA } as any,
			);
			expect(hasMany.callCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER + 1,
			);
		} finally {
			blockLookups = false;
			for (const release of releases) {
				release([]);
			}
			await sync.close();
		}
	});

	it("rejects oversized coordinate requests before resolver work", async () => {
		const resolveHashListForSymbols = sinon.stub().returns([]);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashListForSymbols,
		});
		const ship = sinon.stub(sync as any, "shipExchangeHeads").resolves({
			messages: 0,
			fused: false,
		});

		try {
			await sync.onMessage(
				new RequestMaybeSyncCoordinate({
					hashNumbers: Array.from(
						{ length: MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS + 1 },
						(_, index) => BigInt(index),
					),
				}),
				{ from: peerA } as any,
			);

			expect(resolveHashListForSymbols.called).to.equal(false);
			expect(ship.called).to.equal(false);
		} finally {
			await sync.close();
		}
	});

	it("retains coordinate lookup permits across disconnect until work settles", async () => {
		const releases: ((hashes: string[]) => void)[] = [];
		const resolveHashListForSymbols = sinon.stub().callsFake(
			() =>
				new Promise<string[]>((resolve) => {
					releases.push(resolve);
				}),
		);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashListForSymbols,
		});
		const pending: Promise<boolean>[] = [];

		try {
			for (
				let index = 0;
				index < MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER;
				index += 1
			) {
				pending.push(
					sync.onMessage(
						new RequestMaybeSyncCoordinate({
							hashNumbers: [BigInt(index)],
						}),
						{ from: peerA } as any,
					),
				);
			}
			await waitFor(
				() =>
					resolveHashListForSymbols.callCount ===
					MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);

			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [100n] }),
				{ from: peerA } as any,
			);
			sync.onPeerDisconnected(peerA);
			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [101n] }),
				{ from: peerA } as any,
			);
			expect(resolveHashListForSymbols.callCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);
			expect((sync as any).pendingCoordinateLookupCount).to.equal(
				MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER,
			);

			releases.shift()!([]);
			await pending.shift();
			pending.push(
				sync.onMessage(
					new RequestMaybeSyncCoordinate({ hashNumbers: [102n] }),
					{ from: peerA } as any,
				),
			);
			await waitFor(
				() =>
					resolveHashListForSymbols.callCount ===
					MAX_PENDING_SIMPLE_SYNC_LOOKUPS_PER_PEER + 1,
			);
		} finally {
			for (const release of releases) {
				release([]);
			}
			await Promise.all(pending);
			await sync.close();
		}
	});

	it("caps coordinate response hashes before shipping", async () => {
		const hashes = Array.from(
			{ length: MAX_SIMPLE_COORDINATE_RESPONSE_HASHES + 1 },
			(_, index) => `head-${index}`,
		);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashListForSymbols: sinon.stub().returns(hashes),
		});
		const ship = sinon.stub(sync as any, "shipExchangeHeads").resolves({
			messages: 1,
			fused: false,
		});

		try {
			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [1n] }),
				{ from: peerA } as any,
			);

			expect(ship.calledOnce).to.equal(true);
			expect(ship.firstCall.args[0]).to.have.length(
				MAX_SIMPLE_COORDINATE_RESPONSE_HASHES,
			);
		} finally {
			await sync.close();
		}
	});

	it("bounds duplicate-yielding coordinate resolver work", async () => {
		let yielded = 0;
		const duplicateHashes = {
			*[Symbol.iterator]() {
				for (let index = 0; index < 20_000; index += 1) {
					yielded += 1;
					yield "duplicate-head";
				}
			},
		};
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashesForSymbols: sinon
				.stub()
				.returns(new Map([[1n, duplicateHashes]])),
		});
		const ship = sinon.stub(sync as any, "shipExchangeHeads").resolves({
			messages: 1,
			fused: false,
		});

		try {
			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [1n] }),
				{ from: peerA } as any,
			);

			expect(yielded).to.equal(MAX_SIMPLE_COORDINATE_RESPONSE_HASHES);
			expect(ship.calledOnce).to.equal(true);
			expect(ship.firstCall.args[0]).to.deep.equal(["duplicate-head"]);
			expect(sync.coordinateToHash.get(1n)).to.equal(undefined);
		} finally {
			await sync.close();
		}
	});

	it("does not cache a boundary-truncated coordinate as uniquely resolved", async () => {
		let boundaryYields = 0;
		const first = Array.from(
			{ length: MAX_SIMPLE_COORDINATE_RESPONSE_HASHES - 1 },
			(_, index) => `head-${index}`,
		);
		const boundary = {
			*[Symbol.iterator]() {
				boundaryYields += 1;
				yield "boundary-first";
				boundaryYields += 1;
				yield "boundary-second";
			},
		};
		const coordinateToHash = new Cache<string>({ max: 10 });
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash,
			resolveHashesForSymbols: sinon.stub().returns(
				new Map<bigint, Iterable<string>>([
					[1n, first],
					[2n, boundary],
				]),
			),
		});
		const ship = sinon.stub(sync as any, "shipExchangeHeads").resolves({
			messages: 1,
			fused: false,
		});

		try {
			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [1n, 2n] }),
				{ from: peerA } as any,
			);

			expect(boundaryYields).to.equal(1);
			expect(ship.firstCall.args[0]).to.have.length(
				MAX_SIMPLE_COORDINATE_RESPONSE_HASHES,
			);
			expect(coordinateToHash.get(2n)).to.equal(undefined);
		} finally {
			await sync.close();
		}
	});

	it("releases coordinate lookups before bounded response sends settle", async () => {
		const resolveHashListForSymbols = sinon.stub().returns(["head"]);
		const releases: ((result: { messages: number; fused: boolean }) => void)[] =
			[];
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashListForSymbols,
		});
		const ship = sinon.stub(sync as any, "shipExchangeHeads").callsFake(
			() =>
				new Promise<{ messages: number; fused: boolean }>((resolve) => {
					releases.push(resolve);
				}),
		);
		const pending: Promise<boolean>[] = [];

		try {
			for (
				let index = 0;
				index < MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER;
				index += 1
			) {
				pending.push(
					sync.onMessage(
						new RequestMaybeSyncCoordinate({
							hashNumbers: [BigInt(index)],
						}),
						{ from: peerA } as any,
					),
				);
			}
			await waitFor(
				() =>
					ship.callCount === MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER,
			);
			expect((sync as any).pendingCoordinateLookupCount).to.equal(0);
			expect((sync as any).pendingCoordinateResponseCount).to.equal(
				MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER,
			);

			await sync.close();
			await sync.open();
			await sync.onMessage(
				new RequestMaybeSyncCoordinate({ hashNumbers: [100n] }),
				{ from: peerA } as any,
			);
			expect(resolveHashListForSymbols.callCount).to.equal(
				MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER + 1,
			);
			expect(ship.callCount).to.equal(
				MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER,
			);

			releases.shift()!({ messages: 1, fused: false });
			await pending.shift();
			pending.push(
				sync.onMessage(
					new RequestMaybeSyncCoordinate({ hashNumbers: [101n] }),
					{ from: peerA } as any,
				),
			);
			await waitFor(
				() =>
					ship.callCount ===
					MAX_PENDING_SIMPLE_COORDINATE_RESPONSES_PER_PEER + 1,
			);
		} finally {
			for (const release of releases) {
				release({ messages: 1, fused: false });
			}
			await Promise.all(pending);
			await sync.close();
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

	it("clamps configured coordinate chunks to the receiver work limit", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleCoordinatesPerMessage:
					MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS * 2,
			},
		});
		const coordinates = Array.from(
			{ length: MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS + 1 },
			(_, index) => BigInt(index),
		);

		try {
			await sync.queueSync(coordinates, peerA, { skipCheck: true });

			expect(send.callCount).to.equal(2);
			expect(send.firstCall.args[0].hashNumbers).to.have.length(
				MAX_SIMPLE_COORDINATE_REQUEST_SYMBOLS,
			);
			expect(send.secondCall.args[0].hashNumbers).to.have.length(1);
		} finally {
			await sync.close();
		}
	});

	it("uses native resolver for coordinate queue preflight", async () => {
		const send = sinon.stub().resolves();
		const count = sinon
			.stub()
			.throws(new Error("entry index should not be used"));
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

		await sync.queueSync([42n, 7n], {
			hashcode: () => "peer-a",
			equals: () => false,
		} as any);

		expect(count.called).to.equal(false);
		expect(resolveHashesForSymbols.firstCall.args[0]).to.deep.equal([42n, 7n]);
		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0]).to.be.instanceOf(RequestMaybeSyncCoordinate);
		expect(send.firstCall.args[0].hashNumbers).to.deep.equal([7n]);
	});

	it("uses native coordinate symbol resolver before index lookup", async () => {
		const send = sinon.stub().resolves();
		const iterate = sinon
			.stub()
			.throws(new Error("entry index should not be used"));
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
		expect(
			send.firstCall.args[0].heads.map((x: any) => x.entry.hash),
		).to.deep.equal(["head-a"]);
	});

	it("uses native flat coordinate symbol resolver for response lookup", async () => {
		const send = sinon.stub().resolves();
		const iterate = sinon
			.stub()
			.throws(new Error("entry index should not be used"));
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
		expect(
			send.firstCall.args[0].heads.map((x: any) => x.entry.hash),
		).to.deep.equal(["head-a"]);
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

	it("normalizes fractional hash and coordinate chunk settings", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 0.5,
				maxSimpleCoordinatesPerMessage: 0.5,
			},
		});

		try {
			await (sync as any).requestSync(
				["fractional-hash-1", "fractional-hash-2", 1n, 2n],
				[peerA.hashcode()],
			);

			expect(send.callCount).to.equal(4);
			expect(
				send
					.getCalls()
					.filter((call) => call.args[0] instanceof ResponseMaybeSync)
					.map((call) => call.args[0].hashes),
			).to.deep.equal([["fractional-hash-1"], ["fractional-hash-2"]]);
			expect(
				send
					.getCalls()
					.filter((call) => call.args[0] instanceof RequestMaybeSyncCoordinate)
					.map((call) => call.args[0].hashNumbers),
			).to.deep.equal([[1n], [2n]]);
		} finally {
			await sync.close();
		}
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
		const get = sinon
			.stub()
			.throws(new Error("full entry get should not be used"));
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
		expect(
			send.firstCall.args[0].heads.map((head: any) => head.hash),
		).to.deep.equal(["head-a"]);
		expect(get.called).to.equal(false);
		expect(getMany.calledOnceWithExactly(["head-a"])).to.equal(true);
	});
});
