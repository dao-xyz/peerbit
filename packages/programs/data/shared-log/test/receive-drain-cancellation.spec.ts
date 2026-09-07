import { Ed25519Keypair } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SyncReceiveAbortError } from "../src/sync/dispatch-lifecycle.js";
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
	SimpleSyncronizer,
} from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

const bound = async <T>(work: Promise<T>, label: string): Promise<T> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			work,
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error(`${label}: 3000ms`)), 3_000);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
};

const fixture = async (
	syncronizer: typeof SimpleSyncronizer | typeof RatelessIBLTSynchronizer,
	run: (f: any) => Promise<void>,
) => {
	const session = await TestSession.disconnected(1);
	const sandbox = sinon.createSandbox();
	const pending: Promise<unknown>[] = [];
	const releases: (() => void)[] = [];
	const errors: unknown[] = [];
	const expectedErrors = new Set<unknown>();
	try {
		const db = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: false,
				setup: {
					type: "u64",
					domain: createReplicationDomainHash("u64"),
					syncronizer,
					name: "receive-drain-cancellation",
				},
			},
		});
		const log = db.log as any;
		const sync = log.syncronizer;
		const simple = sync.simple ?? sync;
		const { entry } = await db.add("real-local-entry", { target: "none" });
		const peer = (await Ed25519Keypair.create()).publicKey;
		const otherPeer = (await Ed25519Keypair.create()).publicKey;
		const sends: { message: any; options: any }[] = [];
		let sendWork = async (_message: any, _options: any) => {};
		sandbox
			.stub(log.rpc, "send")
			.callsFake(async (message: any, options: any) => {
				sends.push({ message, options });
				await sendWork(message, options);
				return [];
			});
		const track = <T>(promise: Promise<T>) => {
			pending.push(promise);
			void promise.catch(() => {});
			return promise;
		};
		const gate = (type: any) => {
			const entered = pDefer<AbortSignal>();
			const aborted = pDefer<void>();
			const physical = pDefer<void>();
			let armed = true;
			releases.push(physical.resolve);
			sendWork = async (message, options) => {
				if (!(message instanceof type) || !armed) {
					return;
				}
				armed = false;
				const signal = options.signal as AbortSignal;
				expect(signal).to.be.instanceOf(AbortSignal);
				const onAbort = () => aborted.resolve();
				signal.addEventListener("abort", onAbort, { once: true });
				if (signal.aborted) onAbort();
				entered.resolve(signal);
				try {
					await Promise.race([aborted.promise, physical.promise]);
					await physical.promise;
					if (signal.aborted) throw signal.reason;
				} finally {
					signal.removeEventListener("abort", onAbort);
				}
			};
			return { entered, aborted, physical };
		};
		const authorize = () => {
			const reservation = simple.expectMaybeSyncResponse({
				hashes: [entry.hash],
				targets: [peer.hashcode()],
			});
			expect(reservation).to.exist;
			releases.push(() => reservation.release());
			return simple.pendingMaybeSyncResponses
				.get(peer.hashcode())
				.get(entry.hash);
		};
		const outgoing = async () => {
			const entries = new Map<string, any>();
			for (let i = 0; i < 400; i++) {
				const hash = i === 0 ? entry.hash : `hash-${i}`;
				entries.set(hash, {
					hash,
					hashNumber: BigInt(i + 1),
					assignedToRangeBoundary: false,
				});
			}
			await bound(
				sync.onMaybeMissingEntries({
					entries,
					targets: [peer.hashcode(), otherPeer.hashcode()],
				}),
				"outgoing processes",
			);
			const process = sync.outgoingSyncProcessByTarget.get(peer.hashcode());
			expect(process).to.exist;
			const start = sends.find(
				({ message, options }) =>
					message instanceof StartSync &&
					options.mode.to[0] === peer.hashcode(),
			)!.message;
			return { process, start };
		};
		await run({
			db,
			log,
			sync,
			simple,
			entry,
			peer,
			otherPeer,
			sends,
			sandbox,
			track,
			gate,
			authorize,
			outgoing,
			releases,
			expectedErrors,
			send: (callback: typeof sendWork) => {
				sendWork = callback;
			},
		});
	} catch (error) {
		errors.push(error);
	} finally {
		for (const release of releases) {
			try {
				release();
			} catch (error) {
				errors.push(error);
			}
		}
		for (const promise of pending) {
			try {
				await bound(promise, "pending receive cleanup");
			} catch (error) {
				if (!expectedErrors.has(error) && !errors.includes(error))
					errors.push(error);
			}
		}
		sandbox.restore();
		try {
			await bound(session.stop(), "session cleanup");
		} catch (error) {
			errors.push(error);
		}
	}
	if (errors.length === 1) throw errors[0];
	if (errors.length > 1)
		throw new AggregateError(errors, "receive cancellation test failures");
};

describe("receive admission sync cancellation", () => {
	it("joins an admitted fallback even when encoder retirement throws", async () => {
		await fixture(RatelessIBLTSynchronizer, async (f) => {
			const { process, start } = await f.outgoing();
			const retirementFailure = new Error(
				"encoder retirement reported failure",
			);
			const originalFree = process.encoder.free.bind(process.encoder);
			f.sandbox.stub(process.encoder, "free").callsFake(() => {
				// Actually release the fixture's native resource; still report the
				// cleanup failure. No successful native cleanup is inferred in code.
				originalFree();
				throw retirementFailure;
			});
			const gate = f.gate(RequestMaybeSync);
			const controller = new AbortController();
			f.expectedErrors.add(retirementFailure);
			let settled = false;
			const receive = f.track(
				f.sync
					.onMessage(
						new RequestAll({ syncId: start.syncId }),
						{ from: f.peer },
						{ signal: controller.signal },
					)
					.finally(() => {
						settled = true;
					}),
			);
			await bound(gate.entered.promise, "fallback before retirement failure");
			await Promise.resolve();
			await Promise.resolve();
			expect(settled).to.be.false;
			controller.abort(
				new SyncReceiveAbortError("failed retirement receive draining"),
			);
			await bound(gate.aborted.promise, "failed retirement fallback cancelled");
			expect(settled).to.be.false;
			gate.physical.resolve();
			const [result] = await bound(
				Promise.allSettled([receive]),
				"failed retirement physical drain",
			);
			expect(result.status).to.equal("rejected");
			expect((result as PromiseRejectedResult).reason).to.equal(
				retirementFailure,
			);
		});
	});
	it("cannot free a decoder before overlapping initialization returns", async () => {
		await fixture(RatelessIBLTSynchronizer, async (f) => {
			const entered = pDefer<void>(),
				lookup = pDefer<any>();
			f.releases.push(() => lookup.resolve(false));
			const free = f.sandbox.spy();
			f.sandbox.stub(f.sync, "getLocalDecoderForRange").callsFake(async () => {
				entered.resolve();
				return lookup.promise;
			});
			const start = new StartSync({ from: 0n, to: 10n, symbols: [] });
			let initialized = false;
			const first = f.track(
				f.log.onMessage(start, { from: f.peer }).then(() => {
					initialized = true;
				}),
			);
			await bound(entered.promise, "overlapping initialization");
			const second = f.track(
				f.log.onMessage(
					new MoreSymbols({ syncId: start.syncId, lastSeqNo: 0n, symbols: [] }),
					{ from: f.peer },
				),
			);
			const draining = f.track(
				f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
			);
			await bound(second, "faster overlapping receive");
			expect(initialized).to.be.false;
			expect(free.called).to.be.false;
			expect(f.sync.ingoingSyncProcesses.size).to.equal(0);
			expect(f.sync.incomingRatelessProcessAdmissions.size).to.equal(1);
			lookup.resolve({
				free,
				decoded: () => {
					throw new Error("late decoder used");
				},
			});
			await bound(Promise.all([first, draining]), "late initialization drain");
			expect(free.callCount).to.equal(1);
			expect(f.sync.incomingRatelessProcessAdmissions.size).to.equal(0);
		});
	});

	for (const variant of [
		"simple-response",
		"rateless-response",
		"incoming-fallback",
		"outgoing-fallback",
	] as const) {
		it(`does not mask a genuine ${variant} failure that races receive cancellation`, async () => {
			await fixture(
				variant === "simple-response"
					? SimpleSyncronizer
					: RatelessIBLTSynchronizer,
				async (f) => {
					const controller = new AbortController();
					const entered = pDefer<void>(),
						physical = pDefer<void>();
					f.releases.push(physical.resolve);
					const failure = new Error("genuine receive-owned send failure");
					f.expectedErrors.add(failure);
					let message: any;
					if (variant === "incoming-fallback")
						message = new StartSync({
							from: 0n,
							to: 10n,
							symbols: CodedSymbolBatch.fromFlat(new BigUint64Array(1_025 * 3)),
						});
					else if (variant === "outgoing-fallback") {
						const { start } = await f.outgoing();
						message = new RequestAll({ syncId: start.syncId });
					} else {
						if (variant === "simple-response") f.authorize();
						else await f.outgoing();
						message = new ResponseMaybeSync({ hashes: [f.entry.hash] });
					}
					f.send(async () => {
						entered.resolve();
						await physical.promise;
						throw failure;
					});
					const receive = f.track(
						f.sync.onMessage(
							message,
							{ from: f.peer },
							{ signal: controller.signal },
						),
					);
					await bound(entered.promise, "receive-owned send");
					controller.abort(new SyncReceiveAbortError("draining receive"));
					physical.resolve();
					const [result] = await bound(
						Promise.allSettled([receive]),
						"genuine failure settlement",
					);
					expect(result.status).to.equal("rejected");
					expect((result as PromiseRejectedResult).reason).to.equal(failure);
				},
			);
		});
	}
	for (const syncType of [SimpleSyncronizer, RatelessIBLTSynchronizer]) {
		for (const cancelRemoval of [false, true]) {
			it(`${syncType.name} cancels admitted response work before ${cancelRemoval ? "revoked" : "committed"} removal physically drains`, async () => {
				await fixture(syncType, async (f) => {
					const gate = f.gate(ExchangeHeadsMessage);
					const authorization = f.authorize();
					const disconnect = f.sandbox.spy(f.sync, "onPeerDisconnected");
					const receive = f.track(
						f.log.onMessage(new ResponseMaybeSync({ hashes: [f.entry.hash] }), {
							from: f.peer,
						}),
					);
					const signal = await bound<AbortSignal>(
						gate.entered.promise,
						"response send admission",
					);
					let shouldRemove = true,
						removalDone = false;
					const removing = f.track(
						f.log
							.removeReplicator(f.peer, { shouldRemove: () => shouldRemove })
							.then(() => {
								removalDone = true;
							}),
					);
					await bound(gate.aborted.promise, "receive cancellation");
					expect(signal.aborted).to.be.true;
					expect(authorization.batch.targetLifecycle.controller.signal.aborted)
						.to.be.false;
					expect(f.simple.activeMaybeSyncResponseCount).to.equal(1);
					expect(
						f.log._activeReceiveHandlersByPeer.get(f.peer.hashcode())
							.activeBuckets.size,
					).to.equal(1);
					expect(disconnect.called).to.be.false;
					expect(removalDone).to.be.false;
					if (cancelRemoval) shouldRemove = false;
					gate.physical.resolve();
					await bound(
						Promise.all([receive, removing]),
						"receive/removal settlement",
					);
					expect(authorization.settled).to.equal("released");
					expect(f.simple.activeMaybeSyncResponseCount).to.equal(0);
					expect(disconnect.callCount).to.equal(cancelRemoval ? 0 : 1);
					expect(f.log._peerSessions._receiveCleanupGateByPeer.size).to.equal(
						0,
					);
					const fresh = f.authorize();
					await bound(
						f.log.onMessage(new ResponseMaybeSync({ hashes: [f.entry.hash] }), {
							from: f.peer,
						}),
						"fresh same-peer receive",
					);
					expect(fresh.settled).to.equal("fulfilled");
				});
			});
		}

		it(`${syncType.name} does not cancel a queued removal revoked before admission`, async () => {
			await fixture(syncType, async (f) => {
				const blocker = pDefer<void>();
				f.releases.push(blocker.resolve);
				const laneEntered = pDefer<void>();
				const lane = f.track(
					f.log.withReplicationInfoApplyQueue(f.peer.hashcode(), async () => {
						laneEntered.resolve();
						await blocker.promise;
					}),
				);
				await bound(laneEntered.promise, "queue blocker");
				const gate = f.gate(ExchangeHeadsMessage);
				f.authorize();
				const receive = f.track(
					f.log.onMessage(new ResponseMaybeSync({ hashes: [f.entry.hash] }), {
						from: f.peer,
					}),
				);
				const signal = await bound<AbortSignal>(
					gate.entered.promise,
					"send admission",
				);
				let shouldRemove = true;
				const removing = f.track(
					f.log.removeReplicator(f.peer, { shouldRemove: () => shouldRemove }),
				);
				shouldRemove = false;
				blocker.resolve();
				await bound(Promise.all([lane, removing]), "revoked queued removal");
				expect(signal.aborted).to.be.false;
				gate.physical.resolve();
				await bound(receive, "uncancelled receive");
			});
		});

		it(`${syncType.name} cannot cancel a replacement admitted after its receive snapshot`, async () => {
			await fixture(syncType, async (f) => {
				const gate = f.gate(ExchangeHeadsMessage);
				f.authorize();
				const receive = f.track(
					f.log.onMessage(new ResponseMaybeSync({ hashes: [f.entry.hash] }), {
						from: f.peer,
					}),
				);
				await bound(gate.entered.promise, "old response send");
				const draining = f.track(
					f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
				);
				await bound(gate.aborted.promise, "old bucket cancelled");
				const freshSession = f.log._peerSessions.rotate(
					f.peer.hashcode(),
					"opening",
				);
				const lease = f.log.acquirePeerReceiveLease(
					f.peer.hashcode(),
					f.log._instanceLifecycle.membershipLifecycleController,
					freshSession,
				);
				expect(lease).to.exist;
				f.releases.push(lease);
				const freshBucket = f.log._activeReceiveHandlersByPeer.get(
					f.peer.hashcode(),
				).current;
				expect(lease.signal.aborted).to.be.false;
				gate.physical.resolve();
				await bound(Promise.all([receive, draining]), "old snapshot drain");
				expect(
					f.log._activeReceiveHandlersByPeer.get(f.peer.hashcode()).current,
				).to.equal(freshBucket);
				expect(freshBucket.active).to.equal(1);
				expect(lease.signal.aborted).to.be.false;
				lease();
				expect(f.log._activeReceiveHandlersByPeer.size).to.equal(0);
			});
		});
	}

	it("joins an eager Simple send after a cancelled fresh-key lookup returns", async () => {
		await fixture(SimpleSyncronizer, async (f) => {
			await f.simple.queueSync(["existing-missing"], f.otherPeer, {
				skipCheck: true,
			});
			const gate = f.gate(ResponseMaybeSync);
			const lookupEntered = pDefer<void>(),
				lookup = pDefer<string[]>();
			f.releases.push(() => lookup.resolve([]));
			f.sandbox.stub(f.log.log, "hasMany").callsFake(async () => {
				lookupEntered.resolve();
				return lookup.promise;
			});
			let received = false;
			const receive = f.track(
				f.log
					.onMessage(
						new RequestMaybeSync({
							hashes: ["existing-missing", "fresh-missing"],
						}),
						{ from: f.peer },
					)
					.then(() => {
						received = true;
					}),
			);
			await bound(
				Promise.all([gate.entered.promise, lookupEntered.promise]),
				"eager send and fresh lookup",
			);
			const draining = f.track(
				f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
			);
			await bound(gate.aborted.promise, "eager send cancelled");
			lookup.resolve([]);
			await Promise.resolve();
			await Promise.resolve();
			expect(received).to.be.false;
			expect(f.log._activeReceiveHandlersByPeer.size).to.equal(1);
			gate.physical.resolve();
			await bound(Promise.all([receive, draining]), "eager physical drain");
		});
	});

	it("does not admit a Simple coordinate send after its cancelled lookup", async () => {
		await fixture(SimpleSyncronizer, async (f) => {
			const entered = pDefer<void>(),
				lookup = pDefer<string[]>();
			f.releases.push(() => lookup.resolve([f.entry.hash]));
			f.simple.resolveHashListForSymbols = async () => {
				entered.resolve();
				return lookup.promise;
			};
			const receive = f.track(
				f.log.onMessage(new RequestMaybeSyncCoordinate({ hashNumbers: [1n] }), {
					from: f.peer,
				}),
			);
			await bound(entered.promise, "coordinate lookup");
			const draining = f.track(
				f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
			);
			expect(f.simple.pendingCoordinateLookupCount).to.equal(1);
			lookup.resolve([f.entry.hash]);
			await bound(Promise.all([receive, draining]), "coordinate lookup drain");
			expect(
				f.sends.filter(
					(send: any) => send.message instanceof ExchangeHeadsMessage,
				),
			).to.have.length(0);
			await bound(
				f.log.onMessage(new RequestMaybeSyncCoordinate({ hashNumbers: [1n] }), {
					from: f.peer,
				}),
				"fresh coordinate request",
			);
			expect(
				f.sends.filter(
					(send: any) => send.message instanceof ExchangeHeadsMessage,
				),
			).to.have.length(1);
		});
	});

	for (const variant of [
		"start",
		"more",
		"fallback",
		"existing-fallback",
	] as const) {
		it(`physically drains Rateless incoming ${variant} cancellation`, async () => {
			await fixture(RatelessIBLTSynchronizer, async (f) => {
				const free = f.sandbox.spy();
				f.sandbox.stub(f.sync, "getLocalDecoderForRange").resolves({
					add_coded_symbol: () => {},
					try_decode: () => {},
					decoded: () => false,
					get_remote_symbols: () => [],
					free,
				});
				const start = new StartSync({ from: 0n, to: 10n, symbols: [] });
				if (variant !== "start")
					await bound(
						f.log.onMessage(start, { from: f.peer }),
						"initialize incoming",
					);
				const gate = f.gate(
					variant.includes("fallback") ? RequestAll : RequestMoreSymbols,
				);
				if (variant === "existing-fallback") {
					const process = [...f.sync.ingoingSyncProcesses.values()][0] as any;
					f.track(process.fallbackToSimple());
					await bound(gate.entered.promise, "existing RequestAll send");
				}
				const message =
					variant === "start"
						? start
						: new MoreSymbols({
								syncId: start.syncId,
								lastSeqNo: 0n,
								symbols: variant.includes("fallback")
									? []
									: [{ count: 1n, hash: 2n, symbol: 3n }],
							});
				let received = false;
				const receive = f.track(
					f.log.onMessage(message, { from: f.peer }).then(() => {
						received = true;
					}),
				);
				await bound(gate.entered.promise, "incoming send");
				const draining = f.track(
					f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
				);
				await bound(gate.aborted.promise, "incoming cancellation");
				expect(received).to.be.false;
				expect(free.callCount).to.equal(0);
				expect(f.sync.incomingRatelessProcessAdmissions.size).to.equal(1);
				gate.physical.resolve();
				await bound(
					Promise.all([receive, draining]),
					"incoming physical drain",
				);
				expect(free.callCount).to.equal(1);
				expect(f.sync.incomingRatelessProcessAdmissions.size).to.equal(0);
			});
		});
	}

	for (const prestarted of [false, true]) {
		it(`cancels only the captured Rateless ${prestarted ? "already-started" : "new"} Simple fallback`, async () => {
			await fixture(RatelessIBLTSynchronizer, async (f) => {
				const { process, start } = await f.outgoing();
				const other = f.sync.outgoingSyncProcessByTarget.get(
					f.otherPeer.hashcode(),
				);
				const free = f.sandbox.spy(process.encoder, "free");
				const gate = f.gate(RequestMaybeSync);
				if (prestarted) {
					f.track(process.startSimpleFallback());
					await bound(gate.entered.promise, "prestarted Simple fallback");
				}
				let received = false;
				const receive = f.track(
					f.log
						.onMessage(new RequestAll({ syncId: start.syncId }), {
							from: f.peer,
						})
						.then(() => {
							received = true;
						}),
				);
				const signal = await bound<AbortSignal>(
					gate.entered.promise,
					"Simple fallback send",
				);
				expect(process.signal.aborted).to.be.true;
				expect(signal.aborted).to.be.false;
				const draining = f.track(
					f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
				);
				await bound(gate.aborted.promise, "memoized fallback cancellation");
				expect(received).to.be.false;
				expect(other.signal.aborted).to.be.false;
				gate.physical.resolve();
				await bound(
					Promise.all([receive, draining]),
					"fallback physical drain",
				);
				expect(free.callCount).to.equal(1);
				expect(other.signal.aborted).to.be.false;
			});
		});
	}

	it("rolls back cancelled Rateless authorization without retiring its live process", async () => {
		await fixture(RatelessIBLTSynchronizer, async (f) => {
			const { process } = await f.outgoing();
			const gate = f.gate(ExchangeHeadsMessage);
			const receive = f.track(
				f.log.onMessage(new ResponseMaybeSync({ hashes: [f.entry.hash] }), {
					from: f.peer,
				}),
			);
			await bound(gate.entered.promise, "rateless response");
			const draining = f.track(
				f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
			);
			await bound(gate.aborted.promise, "rateless response cancelled");
			expect(f.sync.activeRatelessResponseCount).to.equal(1);
			expect(process.signal.aborted).to.be.false;
			gate.physical.resolve();
			await bound(Promise.all([receive, draining]), "rateless response drain");
			expect(process.consumedResponseHashes.has(f.entry.hash)).to.be.false;
			await bound(
				f.log.onMessage(new ResponseMaybeSync({ hashes: [f.entry.hash] }), {
					from: f.peer,
				}),
				"fresh rateless response",
			);
			expect(process.consumedResponseHashes.has(f.entry.hash)).to.be.true;
		});
	});

	it("preserves a genuine incoming fallback send failure before process cleanup", async () => {
		await fixture(RatelessIBLTSynchronizer, async (f) => {
			const failure = new Error("ordinary RequestAll failure");
			f.expectedErrors.add(failure);
			f.send(async (message: any) => {
				if (message instanceof RequestAll) throw failure;
			});
			const oversized = CodedSymbolBatch.fromFlat(
				new BigUint64Array(1_025 * 3),
			);
			// SharedLog intentionally reports ordinary receive errors to its logger;
			// pin the synchronizer's actual error identity at its public boundary.
			const receive = f.track(
				f.sync.onMessage(
					new StartSync({ from: 0n, to: 10n, symbols: oversized }),
					{ from: f.peer },
					{ signal: new AbortController().signal },
				),
			);
			await expect(bound(receive, "failed RequestAll")).to.be.rejectedWith(
				failure,
			);
			expect(f.sync.incomingRatelessProcessAdmissions.size).to.equal(0);
		});
	});

	for (const primary of [new Error("ordinary lookup failure"), undefined]) {
		it(`joins Simple eager cleanup and preserves dual failure with ${primary === undefined ? "undefined" : "Error"} lookup rejection`, async () => {
			await fixture(SimpleSyncronizer, async (f) => {
				await f.simple.queueSync(["existing-missing"], f.otherPeer, {
					skipCheck: true,
				});
				const sendEntered = pDefer<void>(),
					physical = pDefer<void>();
				f.releases.push(physical.resolve);
				const secondary = new Error("ordinary eager send failure");
				f.send(async (message: any) => {
					if (message instanceof ResponseMaybeSync) {
						sendEntered.resolve();
						await physical.promise;
						throw secondary;
					}
				});
				f.sandbox.stub(f.log.log, "hasMany").callsFake(async () => {
					throw primary;
				});
				let settled = false;
				const receive = f.track(
					f.simple
						.onMessage(
							new RequestMaybeSync({
								hashes: ["existing-missing", "fresh-missing"],
							}),
							{ from: f.peer },
							{ signal: new AbortController().signal },
						)
						.finally(() => {
							settled = true;
						}),
				);
				await bound(sendEntered.promise, "eager failure send");
				await Promise.resolve();
				await Promise.resolve();
				expect(settled).to.be.false;
				physical.resolve();
				const result = await bound(
					Promise.allSettled([receive]),
					"dual failure settlement",
				);
				expect(result[0].status).to.equal("rejected");
				const error = (result[0] as PromiseRejectedResult).reason;
				f.expectedErrors.add(error);
				expect(error).to.be.instanceOf(AggregateError);
				expect(error.errors).to.deep.equal([primary, secondary]);
				expect(error.cause).to.equal(primary);
			});
		});
	}

	it("rolls back a cancelled coordinate shipment without erasing a successor's dedupe stamp", async () => {
		await fixture(SimpleSyncronizer, async (f) => {
			f.simple.resolveHashListForSymbols = async () => [f.entry.hash];
			const gate = f.gate(ExchangeHeadsMessage);
			const coordinate = () =>
				new RequestMaybeSyncCoordinate({ hashNumbers: [1n] });
			const receive = f.track(f.log.onMessage(coordinate(), { from: f.peer }));
			await bound(gate.entered.promise, "coordinate send");
			const draining = f.track(
				f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
			);
			await bound(gate.aborted.promise, "coordinate cancellation");
			const oldRow = f.simple.recentlySentExchangeHeads.get(f.peer.hashcode());
			oldRow.get(f.entry.hash).timestamp -= 10_000;
			await bound(
				f.log.onMessage(coordinate(), { from: f.peer }),
				"successor coordinate send",
			);
			const successorStamp = oldRow.get(f.entry.hash);
			gate.physical.resolve();
			await bound(Promise.all([receive, draining]), "old coordinate drain");
			expect(
				f.simple.recentlySentExchangeHeads
					.get(f.peer.hashcode())
					.get(f.entry.hash),
			).to.equal(successorStamp);
			await bound(
				f.log.onMessage(coordinate(), { from: f.peer }),
				"deduped successor request",
			);
			expect(
				f.sends.filter(
					(send: any) => send.message instanceof ExchangeHeadsMessage,
				),
			).to.have.length(2);
		});
	});

	it("drains Rateless RequestMoreSymbols without cancelling another target", async () => {
		await fixture(RatelessIBLTSynchronizer, async (f) => {
			const { process, start } = await f.outgoing();
			const other = f.sync.outgoingSyncProcessByTarget.get(
				f.otherPeer.hashcode(),
			);
			const free = f.sandbox.spy(process.encoder, "free");
			const gate = f.gate(MoreSymbols);
			const receive = f.track(
				f.log.onMessage(
					new RequestMoreSymbols({ syncId: start.syncId, lastSeqNo: 0n }),
					{ from: f.peer },
				),
			);
			await bound(gate.entered.promise, "MoreSymbols send");
			const draining = f.track(
				f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
			);
			await bound(gate.aborted.promise, "MoreSymbols cancelled");
			expect(free.called).to.be.false;
			expect(other.signal.aborted).to.be.false;
			gate.physical.resolve();
			await bound(Promise.all([receive, draining]), "MoreSymbols drain");
			expect(free.callCount).to.equal(1);
			expect(other.signal.aborted).to.be.false;
		});
	});

	it("retains Rateless initialization admission until a non-abortable lookup returns", async () => {
		await fixture(RatelessIBLTSynchronizer, async (f) => {
			const entered = pDefer<void>(),
				lookup = pDefer<any>();
			f.releases.push(() => lookup.resolve(false));
			const free = f.sandbox.spy();
			f.sandbox.stub(f.sync, "getLocalDecoderForRange").callsFake(async () => {
				entered.resolve();
				return lookup.promise;
			});
			const receive = f.track(
				f.log.onMessage(new StartSync({ from: 0n, to: 10n, symbols: [] }), {
					from: f.peer,
				}),
			);
			await bound(entered.promise, "decoder initialization");
			const draining = f.track(
				f.log.drainPeerReceiveHandlers(f.peer.hashcode()),
			);
			expect(f.sync.incomingRatelessProcessAdmissions.size).to.equal(1);
			expect(f.log._activeReceiveHandlersByPeer.size).to.equal(1);
			lookup.resolve({ free });
			await bound(
				Promise.all([receive, draining]),
				"decoder initialization drain",
			);
			expect(free.callCount).to.equal(1);
			expect(f.sync.incomingRatelessProcessAdmissions.size).to.equal(0);
		});
	});
});
