import { serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { type IndexKeyScanPage } from "@peerbit/indexer-interface";
import { type Entry, Log } from "@peerbit/log";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	EXCHANGE_HEADS_REPAIR_HINT,
	ExchangeHeadsMessage,
	SYNC_CAPABILITY_ENTRY_INVENTORY,
} from "../src/exchange-heads.js";
import { SharedLog } from "../src/index.js";
import { AbsoluteReplicas } from "../src/replication.js";
import { type EntryInventoryPass } from "../src/sync/entry-inventory-recovery.js";
import {
	ConfirmEntriesMessage,
	ENTRY_INVENTORY_PAGE_SIZE,
	RequestEntryInventoryV1,
} from "../src/sync/simple.js";

describe("entry inventory pass progress", () => {
	let blocks: AnyBlockStore[];
	let logs: Log<Uint8Array>[];
	let clock: sinon.SinonFakeTimers | undefined;
	let owner: AbortController;
	let pending: Promise<unknown>[];
	let releases: (() => void)[];
	let passes: EntryInventoryPass[];

	beforeEach(() => {
		blocks = [];
		logs = [];
		pending = [];
		releases = [];
		passes = [];
		owner = new AbortController();
	});

	afterEach(async () => {
		owner.abort();
		for (const release of releases) release();
		try {
			await clock?.runAllAsync();
			await Promise.allSettled(pending);
			await Promise.all(passes.map((pass) => pass.close()));
			if (clock) expect(clock.countTimers()).to.equal(0);
		} finally {
			clock?.restore();
			clock = undefined;
			sinon.restore();
			await Promise.all(logs.map((log) => log.close()));
			await Promise.all(blocks.map((store) => store.stop()));
		}
	});

	const openLog = async (canAppend?: (entry: Entry<Uint8Array>) => boolean) => {
		const store = new AnyBlockStore();
		blocks.push(store);
		await store.start();
		const log = new Log<Uint8Array>();
		logs.push(log);
		await log.open(store, await Ed25519Keypair.create(), { canAppend });
		return log;
	};

	const gate = () => {
		const value = pDefer<void>();
		releases.push(() => value.resolve());
		return value;
	};
	const advance = (pass: EntryInventoryPass) => {
		const result = pass.next();
		void result.catch(() => {});
		pending.push(result);
		return result;
	};
	const harness = (source: Log<Uint8Array>, keys: string[]) => {
		const peer = "inventory-target";
		const transportSession = 7n;
		const session = { phase: "open", isActive: () => true };
		const state = { session, receiveEpoch: {} };
		const replicaBytes = serialize(new AbsoluteReplicas(1));
		let offset = 0;
		const next = sinon.spy((): IndexKeyScanPage => {
			const page = keys.slice(offset, offset + ENTRY_INVENTORY_PAGE_SIZE);
			offset += page.length;
			return {
				status: offset === keys.length ? "complete" : "more",
				keys: page,
			};
		});
		const close = sinon.spy();
		const scan = sinon.spy(
			(options: { pageSize: number; signal: AbortSignal }) => {
				expect(options.pageSize).to.equal(ENTRY_INVENTORY_PAGE_SIZE);
				expect(options.signal.aborted).to.equal(false);
				return { next, close };
			},
		);
		const coordinate = sinon.stub().callsFake(async (hash: string) => ({
			hash,
			meta: { data: replicaBytes },
		}));
		const leaders = sinon
			.stub()
			.callsFake(
				async (
					_coordinate: unknown,
					_replicas: number,
					_options: unknown,
					_lifecycle: AbortController,
				) => new Map([[peer, true]]),
			);
		const shared = Object.create(SharedLog.prototype) as any;
		Object.defineProperties(shared, {
			node: { value: {} },
			closed: { value: false },
		});
		Object.assign(shared, {
			log: source,
			_closeController: owner,
			_instanceLifecycle: { _receiveOwnershipRevision: 0 },
			_peerSyncCapabilitySessions: new Map([[peer, transportSession]]),
			_peerSyncCapabilities: new Map([[peer, SYNC_CAPABILITY_ENTRY_INVENTORY]]),
			_peerSessions: {
				isCurrent: (key: string, captured: unknown) =>
					key === peer && captured === state.session,
				receiveEpoch: () => state.receiveEpoch,
				isReplicationInfoBlocked: () => false,
				isReceiveCleanupGateOpen: () => true,
			},
			_coordinates: {
				scanAuthoritativeCoordinateKeys: scan,
				getAuthoritativeCoordinateEntryForInventory: coordinate,
			},
			captureReplicationOwnershipLifecycle: () => owner,
			findLeadersFromEntry: leaders,
		});
		const present = (hashes: string[]) => [
			{
				response: new ConfirmEntriesMessage({ hashes }),
				from: { hashcode: () => peer },
				message: { header: { session: transportSession } },
			},
		];
		shared.rpc = {
			request: sinon
				.stub()
				.callsFake(async (request: RequestEntryInventoryV1, options: any) => {
					options.onPhysicalSettlement(Promise.resolve());
					return present(request.hashes);
				}),
			send: sinon.spy(async () => {}),
		};
		const createPass = () => {
			const pass = shared.recoverPeerEntryInventory(
				peer,
				session,
				owner.signal,
			) as EntryInventoryPass;
			passes.push(pass);
			return pass;
		};
		return {
			shared,
			peer,
			transportSession,
			session,
			state,
			next,
			close,
			scan,
			coordinate,
			leaders,
			present,
			createPass,
		};
	};

	for (const fault of ["lost response", "rejected entry"] as const) {
		it(`reaches a missing tail beyond sixteen pages despite an early ${fault}`, async () => {
			const source = await openLog();
			const early = (
				await source.append(new Uint8Array([1]), { meta: { next: [] } })
			).entry;
			const tail = (
				await source.append(new Uint8Array([2]), { meta: { next: [] } })
			).entry;
			const canAppend = sinon.spy(
				(entry: Entry<Uint8Array>) => entry.hash !== early.hash,
			);
			const receiver = await openLog(canAppend);
			const pageCount = 18;
			// Already-present inventory rows are synthetic: only the two missing
			// entries require payload reads. Their offers use real signed Log entries
			// and the receiver's ordinary Log.join/canAppend path below.
			const keys = Array.from(
				{ length: pageCount * ENTRY_INVENTORY_PAGE_SIZE },
				(_, index) => `present-${index}`,
			);
			keys[0] = early.hash;
			keys[keys.length - 1] = tail.hash;
			// Exercise the real pass, page, session fencing and shared egress limiter
			// without background membership/transport work or 2,304 local appends.
			const {
				shared,
				peer,
				transportSession,
				createPass,
				scan,
				close,
				next,
				leaders,
			} = harness(source, keys);
			const queryStarts: number[] = [];
			const queriedPages: string[] = [];
			const offered: string[] = [];
			let lost = false;
			shared.rpc = {
				request: sinon.spy(
					async (request: RequestEntryInventoryV1, options: any) => {
						expect(request).to.be.instanceOf(RequestEntryInventoryV1);
						expect(request.expectedReceiverSession).to.equal(transportSession);
						expect(request.hashes.length).to.be.at.most(
							ENTRY_INVENTORY_PAGE_SIZE,
						);
						expect(options.isCurrent()).to.equal(true);
						options.onPhysicalSettlement(Promise.resolve());
						queryStarts.push(Date.now());
						queriedPages.push(request.hashes[0]);
						if (
							fault === "lost response" &&
							!lost &&
							request.hashes.includes(early.hash)
						) {
							lost = true;
							return []; // RPC's bounded timeout returns no correlated response.
						}
						const present: string[] = [];
						for (const hash of request.hashes) {
							if (
								(hash !== early.hash && hash !== tail.hash) ||
								(await receiver.has(hash))
							)
								present.push(hash);
						}
						return [
							{
								response: new ConfirmEntriesMessage({ hashes: present }),
								from: { hashcode: () => peer },
								message: { header: { session: transportSession } },
							},
						];
					},
				),
				send: sinon.spy(
					async (message: ExchangeHeadsMessage<Uint8Array>, options: any) => {
						expect(message).to.be.instanceOf(ExchangeHeadsMessage);
						expect(message.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT).to.equal(
							EXCHANGE_HEADS_REPAIR_HINT,
						);
						expect(options.isCurrent()).to.equal(true);
						for (const head of message.heads) {
							expect(await head.entry.verifySignatures()).to.equal(true);
							offered.push(head.entry.hash);
						}
						await receiver.join(
							message.heads.map((head) => head.entry),
							{ signal: options.signal },
						);
					},
				),
			};
			const admission = sinon.spy(
				shared,
				"waitForPersistedReceiptEgressAdmission",
			);
			clock = sinon.useFakeTimers({
				now: Date.now(),
				toFake: ["Date", "setTimeout", "clearTimeout"],
			});
			const pass = createPass();
			const result = (async () => {
				try {
					while (true) {
						const result = await pass.next();
						if (result !== "more") return result;
					}
				} finally {
					await pass.close();
				}
			})();
			void result.catch(() => {});
			pending.push(result);
			await clock.runAllAsync();
			expect(await result).to.equal("retry"); // The early page never proved presence.
			expect(scan.calledOnce && close.calledOnce).to.equal(true);
			expect(next.callCount).to.equal(pageCount);
			expect(leaders.callCount).to.equal(keys.length);
			for (const call of leaders.getCalls()) {
				expect(call.args[2]).to.deep.equal({
					roleAge: 0,
					freshLeaderPlan: true,
				});
			}
			expect(new Set(queriedPages).size).to.equal(pageCount);
			expect(admission.callCount).to.equal(queryStarts.length);
			// The first sixteen credits are a burst; the next request must wait for
			// replenishment. Both inventory and durability use this same limiter.
			expect(queryStarts[16] - queryStarts[0]).to.be.at.least(125);
			expect(offered).to.deep.equal(
				fault === "rejected entry" ? [early.hash, tail.hash] : [tail.hash],
			);
			expect(await receiver.has(early.hash)).to.equal(false);
			expect(await receiver.has(tail.hash)).to.equal(true);
			const received = await receiver.get(tail.hash, { remote: false });
			expect(serialize(received!)).to.deep.equal(serialize(tail));
			expect(canAppend.calledWithMatch({ hash: tail.hash })).to.equal(true);
			if (fault === "rejected entry") {
				expect(canAppend.calledWithMatch({ hash: early.hash })).to.equal(true);
				expect(queryStarts.length).to.be.greaterThan(pageCount);
			} else {
				expect(lost).to.equal(true);
				expect(queryStarts).to.have.length(pageCount + 1);
			}
		});
	}

	for (const boundary of ["coordinate", "leaders"] as const) {
		it(`rejects replacement ${boundary === "coordinate" ? "sessions" : "receive epochs"} during an awaited ${boundary} read`, async () => {
			const source = await openLog();
			const h = harness(source, ["held-entry"]);
			const entered = gate();
			const resume = gate();
			const read = h[boundary];
			read.callsFake(async () => {
				entered.resolve();
				await resume.promise;
				return boundary === "coordinate"
					? {
							hash: "held-entry",
							meta: { data: serialize(new AbsoluteReplicas(1)) },
						}
					: new Map([[h.peer, true]]);
			});
			const result = advance(h.createPass());
			await entered.promise;
			if (boundary === "coordinate") h.state.session = { ...h.session };
			else h.state.receiveEpoch = {};
			resume.resolve();
			expect(await result).to.equal("retry");
			expect(h.shared.rpc.request.called).to.equal(false);
			expect(h.shared.rpc.send.called).to.equal(false);
			if (boundary === "coordinate") expect(h.leaders.called).to.equal(false);
		});
	}

	it("does not complete from a final reply after ownership or lower admission changed", async () => {
		for (const mutation of ["ownership", "lower"] as const) {
			const source = await openLog();
			const h = harness(source, ["held-entry"]);
			const entered = gate();
			const reply = gate();
			h.shared.rpc.request.callsFake(
				async (request: RequestEntryInventoryV1, options: any) => {
					options.onPhysicalSettlement(Promise.resolve());
					entered.resolve();
					await reply.promise;
					return h.present(request.hashes);
				},
			);
			const result = advance(h.createPass());
			await entered.promise;
			if (mutation === "ownership") {
				h.shared._instanceLifecycle._receiveOwnershipRevision++;
			} else {
				// Even a finished no-op admission must invalidate an older inventory.
				const lock = await source.entryIndex.acquireHashMutationLocks([
					"held-entry",
				]);
				source.entryIndex.releaseHashMutationLocks(lock);
			}
			reply.resolve();
			expect(await result, mutation).to.equal("retry");
			expect(h.shared.rpc.request.calledOnce).to.equal(true);
			expect(h.shared.rpc.send.called).to.equal(false);
		}
	});

	it("keeps a cancelled and closed page pending until its physical request settles", async () => {
		const source = await openLog();
		const h = harness(source, ["held-entry"]);
		const entered = gate();
		const physical = gate();
		const logicalAbort = gate();
		h.shared.rpc.request.callsFake(
			async (_request: RequestEntryInventoryV1, options: any) => {
				options.onPhysicalSettlement(physical.promise);
				options.signal.addEventListener("abort", () => logicalAbort.resolve(), {
					once: true,
				});
				entered.resolve();
				await logicalAbort.promise;
				return []; // Logical RPC cancellation precedes the raw publish settling.
			},
		);
		clock = sinon.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		const pass = h.createPass();
		let settled = false;
		const result = advance(pass).then((value) => {
			settled = true;
			return value;
		});
		void result.catch(() => {});
		await entered.promise;
		owner.abort();
		await pass.close();
		await logicalAbort.promise;
		await clock.tickAsync(0);
		expect(h.close.calledOnce).to.equal(true);
		expect(settled).to.equal(false);
		expect(h.shared.rpc.send.called).to.equal(false);
		physical.resolve();
		expect(await result).to.equal("retry");
		expect(settled).to.equal(true);
		expect(clock.countTimers()).to.equal(0);
	});

	it("omits an entry when the fresh awaited leader plan no longer includes the peer", async () => {
		const source = await openLog();
		const h = harness(source, ["held-entry"]);
		const entered = gate();
		const resume = gate();
		let eligible = true;
		h.leaders.callsFake(async () => {
			entered.resolve();
			await resume.promise;
			return eligible ? new Map([[h.peer, true]]) : new Map();
		});
		const result = advance(h.createPass());
		await entered.promise;
		eligible = false;
		resume.resolve();
		expect(await result).to.equal("complete");
		expect(h.leaders.firstCall.args[2]).to.deep.equal({
			roleAge: 0,
			freshLeaderPlan: true,
		});
		expect(h.shared.rpc.request.called).to.equal(false);
		expect(h.shared.rpc.send.called).to.equal(false);
	});

	type Harness = ReturnType<typeof harness>;
	const presentKeys = (count: number) =>
		Array.from({ length: count }, (_, index) => `present-${index}`);
	// The harness scan keeps one offset across scans. A restarted pass walks
	// again from the first key, so every scan here starts its own walk.
	const freshScan = (h: Harness, keys: string[]) => {
		h.shared._coordinates.scanAuthoritativeCoordinateKeys = (options: {
			pageSize: number;
			signal: AbortSignal;
		}) => {
			expect(options.pageSize).to.equal(ENTRY_INVENTORY_PAGE_SIZE);
			expect(options.signal.aborted).to.equal(false);
			let offset = 0;
			return {
				next: (): IndexKeyScanPage => {
					const page = keys.slice(offset, offset + ENTRY_INVENTORY_PAGE_SIZE);
					offset += page.length;
					return {
						status: offset === keys.length ? "complete" : "more",
						keys: page,
					};
				},
				close: () => {},
			};
		};
	};
	const drive = async (pass: EntryInventoryPass) => {
		try {
			while (true) {
				const result = await pass.next();
				if (result !== "more") return result;
			}
		} finally {
			await pass.close();
		}
	};
	// A finished no-op admission is a local write as far as a pass can tell.
	const admit = async (log: Log<Uint8Array>, key: string) => {
		const lock = await log.entryIndex.acquireHashMutationLocks([key]);
		log.entryIndex.releaseHashMutationLocks(lock);
	};
	// The peer holds every hash. When `cut` is true for a request (counted from
	// one), a local write lands before its reply returns.
	const answerAllPresent = (
		h: Harness,
		source: Log<Uint8Array>,
		cut: (request: number) => boolean,
	) => {
		const asked: string[][] = [];
		h.shared.rpc.request.callsFake(
			async (request: RequestEntryInventoryV1, options: any) => {
				options.onPhysicalSettlement(Promise.resolve());
				asked.push([...request.hashes]);
				if (cut(asked.length)) await admit(source, request.hashes[0]);
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: request.hashes }),
						from: { hashcode: () => h.peer },
						message: {
							header: {
								session: h.shared._peerSyncCapabilitySessions.get(h.peer),
							},
						},
					},
				];
			},
		);
		return asked;
	};
	const confirmations = (h: Harness) =>
		h.shared._entryInventoryConfirmed?.get(h.peer) as
			| { hashes: Set<string> }
			| undefined;
	const confirmedTotal = (h: Harness) => {
		const total = h.shared._entryInventoryConfirmedTotal;
		expect(total, "confirmed total").to.be.a("number");
		return total as number;
	};
	// The fields removeRepairFrontierTarget touches besides the inventory.
	const allowDeparture = (h: Harness) => {
		const cancelJoinWarmupTarget = sinon.spy();
		Object.assign(h.shared, {
			joinWarmup: {
				_warmupSessionsByTarget: new Map(),
				cancelJoinWarmupTarget,
			},
			_repairFrontierByMode: new Map(),
			_repairFrontierActiveTargetsByMode: new Map(),
			_repairFrontierBypassKnownPeersByMode: new Map(),
		});
		return cancelJoinWarmupTarget;
	};

	it("asks each confirmed hash once while local writes restart the pass", async () => {
		const source = await openLog();
		const keys = presentKeys(4 * ENTRY_INVENTORY_PAGE_SIZE + 1);
		const h = harness(source, keys);
		freshScan(h, keys);
		// A reply that crosses a local write is never acted on and its pass ends
		// in a retry. The first twenty requests all cross one.
		const asked = answerAllPresent(h, source, (request) => request <= 20);
		const results: string[] = [];
		while (results.length < 30 && results[results.length - 1] !== "complete") {
			results.push(await drive(h.createPass()));
		}
		expect(asked.flat().length, "requested hashes").to.equal(keys.length);
		expect(asked.flat(), "each key asked once, in order").to.deep.equal(keys);
		expect(asked.length, "requests").to.equal(5);
		expect(h.coordinate.callCount, "coordinate reads").to.equal(keys.length);
		expect(h.leaders.callCount, "leader plans").to.equal(keys.length);
		expect(results).to.deep.equal([
			"retry",
			"retry",
			"retry",
			"retry",
			"retry",
			"complete",
		]);
		expect(h.shared.rpc.send.called).to.equal(false);
		expect(confirmations(h), "record after completion").to.equal(undefined);
		expect(confirmedTotal(h)).to.equal(0);
	});

	it("forgets confirmations when the ownership snapshot changes", async () => {
		const source = await openLog();
		const keys = presentKeys(2 * ENTRY_INVENTORY_PAGE_SIZE);
		const h = harness(source, keys);
		freshScan(h, keys);
		const asked = answerAllPresent(h, source, (request) => request === 1);
		expect(await drive(h.createPass())).to.equal("retry");
		expect(
			confirmations(h)?.hashes.size,
			"confirmations after the cut pass",
		).to.equal(ENTRY_INVENTORY_PAGE_SIZE);
		h.shared._instanceLifecycle._receiveOwnershipRevision++;
		expect(await drive(h.createPass())).to.equal("complete");
		expect(asked.slice(1)).to.deep.equal([
			keys.slice(0, ENTRY_INVENTORY_PAGE_SIZE),
			keys.slice(ENTRY_INVENTORY_PAGE_SIZE),
		]);
		expect(confirmedTotal(h)).to.equal(0);
	});

	for (const scope of [
		"session",
		"transport session",
		"receive epoch",
		"entry index",
	] as const) {
		it(`forgets confirmations when the ${scope} changes`, async () => {
			const source = await openLog();
			const keys = presentKeys(2 * ENTRY_INVENTORY_PAGE_SIZE);
			const h = harness(source, keys);
			freshScan(h, keys);
			const asked = answerAllPresent(h, source, (request) => request === 1);
			expect(await drive(h.createPass())).to.equal("retry");
			expect(
				confirmations(h)?.hashes.size,
				"confirmations after the cut pass",
			).to.equal(ENTRY_INVENTORY_PAGE_SIZE);
			if (scope === "session") {
				h.state.session = { phase: "open", isActive: () => true };
			} else if (scope === "transport session") {
				h.shared._peerSyncCapabilitySessions.set(h.peer, 8n);
			} else if (scope === "receive epoch") {
				h.state.receiveEpoch = {};
			} else {
				h.shared.log = await openLog();
			}
			const pass = h.shared.recoverPeerEntryInventory(
				h.peer,
				h.state.session,
				owner.signal,
			) as EntryInventoryPass;
			passes.push(pass);
			expect(await drive(pass)).to.equal("complete");
			expect(asked.slice(1)).to.deep.equal([
				keys.slice(0, ENTRY_INVENTORY_PAGE_SIZE),
				keys.slice(ENTRY_INVENTORY_PAGE_SIZE),
			]);
			expect(confirmedTotal(h)).to.equal(0);
		});
	}

	// Only the first reply is faulty. It also crosses a local write, the one case
	// where a reply that is not acted on is still kept as presence.
	for (const fault of [
		"sent by another peer",
		"on another transport session",
		"with an unrequested hash",
		"with a duplicate hash",
		"with one hash too many",
	] as const) {
		it(`keeps no confirmations from a reply ${fault}`, async () => {
			const source = await openLog();
			const keys = presentKeys(ENTRY_INVENTORY_PAGE_SIZE);
			const h = harness(source, keys);
			freshScan(h, keys);
			const asked: string[][] = [];
			h.shared.rpc.request.callsFake(
				async (request: RequestEntryInventoryV1, options: any) => {
					options.onPhysicalSettlement(Promise.resolve());
					asked.push([...request.hashes]);
					const hashes = [...request.hashes];
					if (asked.length > 1) return h.present(hashes);
					await admit(source, hashes[0]);
					if (fault === "with an unrequested hash") hashes[0] = "unrequested";
					if (fault === "with a duplicate hash") hashes[1] = hashes[0];
					if (fault === "with one hash too many") hashes.push(hashes[0]);
					const [reply] = h.present(hashes);
					if (fault === "sent by another peer")
						reply.from = { hashcode: () => "another-peer" };
					if (fault === "on another transport session")
						reply.message.header.session = h.transportSession - 1n;
					return [reply];
				},
			);
			expect(await drive(h.createPass())).to.equal("retry");
			expect(
				confirmations(h)?.hashes.size ?? 0,
				"confirmations from the rejected reply",
			).to.equal(0);
			expect(h.shared._entryInventoryConfirmedTotal ?? 0).to.equal(0);
			expect(await drive(h.createPass())).to.equal("complete");
			expect(asked, "the second pass asks the whole page").to.deep.equal([
				keys,
				keys,
			]);
			expect(confirmedTotal(h)).to.equal(0);
		});
	}

	// A valid reply, but one of the pass's other fences fails while it is in
	// flight.
	for (const fence of [
		"replication info is blocked",
		"the cleanup gate closes",
		"the ownership snapshot changes",
		"its pass is aborted",
	] as const) {
		it(`keeps no confirmations from a reply that arrives after ${fence}`, async () => {
			const source = await openLog();
			const keys = presentKeys(ENTRY_INVENTORY_PAGE_SIZE);
			const h = harness(source, keys);
			freshScan(h, keys);
			const fences = { blocked: false, gateOpen: true };
			Object.assign(h.shared._peerSessions, {
				isReplicationInfoBlocked: () => fences.blocked,
				isReceiveCleanupGateOpen: () => fences.gateOpen,
			});
			const job = new AbortController();
			const asked: string[][] = [];
			h.shared.rpc.request.callsFake(
				async (request: RequestEntryInventoryV1, options: any) => {
					options.onPhysicalSettlement(Promise.resolve());
					asked.push([...request.hashes]);
					if (asked.length === 1) {
						if (fence === "replication info is blocked") fences.blocked = true;
						else if (fence === "the cleanup gate closes")
							fences.gateOpen = false;
						else if (fence === "the ownership snapshot changes")
							h.shared._instanceLifecycle._receiveOwnershipRevision++;
						else job.abort();
					}
					return h.present(request.hashes);
				},
			);
			const pass = h.shared.recoverPeerEntryInventory(
				h.peer,
				h.session,
				job.signal,
			) as EntryInventoryPass;
			passes.push(pass);
			expect(await drive(pass)).to.equal("retry");
			expect(
				confirmations(h)?.hashes.size ?? 0,
				"confirmations from the fenced reply",
			).to.equal(0);
			expect(h.shared._entryInventoryConfirmedTotal ?? 0).to.equal(0);
			fences.blocked = false;
			fences.gateOpen = true;
			expect(await drive(h.createPass())).to.equal("complete");
			expect(asked, "the second pass asks the whole page").to.deep.equal([
				keys,
				keys,
			]);
			expect(confirmedTotal(h)).to.equal(0);
		});
	}

	it("asks only the unconfirmed hash after a write cuts its page", async () => {
		const source = await openLog();
		const held = (
			await source.append(new Uint8Array([1]), { meta: { next: [] } })
		).entry;
		const receiver = await openLog();
		const keys = presentKeys(2 * ENTRY_INVENTORY_PAGE_SIZE);
		keys[5] = held.hash;
		const h = harness(source, keys);
		freshScan(h, keys);
		const asked: string[][] = [];
		h.shared.rpc.request.callsFake(
			async (request: RequestEntryInventoryV1, options: any) => {
				options.onPhysicalSettlement(Promise.resolve());
				asked.push([...request.hashes]);
				const present: string[] = [];
				for (const hash of request.hashes) {
					if (hash !== held.hash || (await receiver.has(hash)))
						present.push(hash);
				}
				return h.present(present);
			},
		);
		h.shared.rpc.send = sinon.spy(
			async (message: ExchangeHeadsMessage<Uint8Array>, options: any) => {
				await receiver.join(
					message.heads.map((head) => head.entry),
					{ signal: options.signal },
				);
				// A local write lands before the page asks again.
				await admit(source, held.hash);
			},
		);
		expect(await drive(h.createPass())).to.equal("retry");
		expect(asked.length).to.equal(1);
		expect(await drive(h.createPass())).to.equal("complete");
		expect(
			asked[1].length,
			"hashes in the first request of the second pass",
		).to.equal(1);
		expect(asked[1]).to.deep.equal([held.hash]);
		expect(h.shared.rpc.send.callCount).to.equal(1);
		expect(asked.flat().length, "requested hashes").to.equal(
			ENTRY_INVENTORY_PAGE_SIZE + 1 + ENTRY_INVENTORY_PAGE_SIZE,
		);
		expect(await receiver.has(held.hash)).to.equal(true);
	});

	it("caps the confirmations it keeps", async () => {
		const source = await openLog();
		const keys = presentKeys(4 * ENTRY_INVENTORY_PAGE_SIZE + 1);
		const h = harness(source, keys);
		freshScan(h, keys);
		h.shared.entryInventoryConfirmedLimit = 200;
		const asked = answerAllPresent(h, source, (request) => request <= 2);
		expect(await drive(h.createPass())).to.equal("retry");
		expect(confirmedTotal(h)).to.equal(ENTRY_INVENTORY_PAGE_SIZE);
		expect(await drive(h.createPass())).to.equal("retry");
		expect(confirmedTotal(h)).to.equal(200);
		expect(confirmations(h)?.hashes.size).to.equal(200);
		expect(await drive(h.createPass())).to.equal("complete");
		expect(confirmedTotal(h)).to.equal(0);
		expect(asked.map((hashes) => hashes.length)).to.deep.equal([
			128, 128, 56, 128, 128, 1,
		]);
		expect(asked[2]).to.deep.equal(keys.slice(200, 256));
		expect(asked.flat().length, "requested hashes").to.equal(569);
	});

	it("counts every peer's confirmations against one log-wide limit", async () => {
		const source = await openLog();
		const keys = presentKeys(4 * ENTRY_INVENTORY_PAGE_SIZE + 1);
		const h = harness(source, keys);
		freshScan(h, keys);
		h.shared.entryInventoryConfirmedLimit = 200;
		const other = {
			session: {},
			transportSession: 1n,
			receiveEpoch: {},
			revision: 0,
			lower: source.entryIndex,
			hashes: new Set(presentKeys(150).map((key) => `other-${key}`)),
		};
		h.shared._entryInventoryConfirmed = new Map([["other-peer", other]]);
		h.shared._entryInventoryConfirmedTotal = 150;
		const asked = answerAllPresent(h, source, (request) => request <= 2);
		expect(await drive(h.createPass())).to.equal("retry");
		expect(
			confirmations(h)?.hashes.size,
			"confirmations kept for this peer",
		).to.equal(50);
		expect(confirmedTotal(h)).to.equal(200);
		expect(await drive(h.createPass())).to.equal("retry");
		expect(
			confirmations(h)?.hashes.size,
			"confirmations kept for this peer",
		).to.equal(50);
		expect(await drive(h.createPass())).to.equal("complete");
		expect(confirmedTotal(h)).to.equal(150);
		expect(h.shared._entryInventoryConfirmed.get("other-peer")).to.equal(other);
		expect(asked.map((hashes) => hashes.length)).to.deep.equal([
			128, 78, 78, 128, 128, 128, 1,
		]);
	});

	it("keeps no confirmations for a backend without key scans", async () => {
		const source = await openLog();
		const h = harness(source, ["held-entry"]);
		h.shared._coordinates.scanAuthoritativeCoordinateKeys = () => undefined;
		expect(await drive(h.createPass())).to.equal("complete");
		expect(confirmations(h)).to.equal(undefined);
		expect(h.shared.rpc.request.called).to.equal(false);
	});

	it("drops confirmations when the peer leaves", async () => {
		const source = await openLog();
		const keys = presentKeys(2 * ENTRY_INVENTORY_PAGE_SIZE);
		const h = harness(source, keys);
		freshScan(h, keys);
		answerAllPresent(h, source, (request) => request === 1);
		expect(await drive(h.createPass())).to.equal("retry");
		expect(
			confirmations(h)?.hashes.size,
			"confirmations after the cut pass",
		).to.equal(ENTRY_INVENTORY_PAGE_SIZE);
		const cancelJoinWarmupTarget = allowDeparture(h);
		h.shared.removeRepairFrontierTarget(h.peer);
		expect(cancelJoinWarmupTarget.calledOnceWith(h.peer)).to.equal(true);
		expect(confirmations(h), "record after the peer left").to.equal(undefined);
		expect(confirmedTotal(h)).to.equal(0);
	});

	it("keeps no confirmations from a reply that arrives after the peer left", async () => {
		const source = await openLog();
		const keys = presentKeys(ENTRY_INVENTORY_PAGE_SIZE);
		const h = harness(source, keys);
		freshScan(h, keys);
		allowDeparture(h);
		const entered = gate();
		const reply = gate();
		h.shared.rpc.request.callsFake(
			async (request: RequestEntryInventoryV1, options: any) => {
				options.onPhysicalSettlement(Promise.resolve());
				entered.resolve();
				await reply.promise;
				return h.present(request.hashes);
			},
		);
		const result = advance(h.createPass());
		await entered.promise;
		h.shared.removeRepairFrontierTarget(h.peer);
		reply.resolve();
		// The harness has no scheduler to abort the pass, so only the identity of
		// the record keeps this reply out of the log-wide total.
		expect(await result).to.equal("complete");
		expect(confirmations(h), "record after the peer left").to.equal(undefined);
		expect(confirmedTotal(h)).to.equal(0);
	});
});
