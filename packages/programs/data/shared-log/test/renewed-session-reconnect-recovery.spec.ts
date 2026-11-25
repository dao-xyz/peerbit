import { serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import { type Entry, Log } from "@peerbit/log";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer, { type DeferredPromise } from "p-defer";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import {
	AbsoluteReplicas,
	decodeReplicas,
	encodeReplicas,
} from "../src/replication.js";
import { JSON_ENCODING } from "./utils/stores/encoding.js";
import { EventStore } from "./utils/stores/index.js";

type EdgeRecord = {
	cycle: number;
	session: any;
	phase?: string;
	active: boolean;
	committed: boolean;
};

// A deadline on one awaited event. It never paces the test.
const within = async <T>(
	promise: Promise<T>,
	timeoutMs: number,
	message: string,
): Promise<T> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error(message)), timeoutMs);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
};

describe("receive admission renewed-session reconnect recovery", function () {
	// Settling and the drain of the real startup repair windows take up to 50
	// seconds; each of the three partitions then has five seconds to deliver.
	this.timeout(120_000);
	for (const nativeStorage of [false, true]) {
		it(
			"delivers partition-era entries through the renewed session's own repair in every edge ordering" +
				(nativeStorage ? " (native storage)" : ""),
			async () => {
				const peers: Peerbit[] = [];
				const authorBlocks = new AnyBlockStore();
				const authorLog = new Log<any>();
				const sandbox = sinon.createSandbox();
				const listeners = new AbortController();
				// Deferreds that product code may await; teardown resolves them.
				const releases: DeferredPromise<any>[] = [];
				const edges: EdgeRecord[][] = [[], []];
				// Small ids for sessions in failure diagnostics.
				const sessionIds = new WeakMap<object, number>();
				let nextSessionId = 0;
				const sessionId = (session: object | null | undefined) => {
					if (!session) return null;
					let id = sessionIds.get(session);
					if (id === undefined) sessionIds.set(session, (id = ++nextSessionId));
					return id;
				};
				let diagnostics: (() => object) | undefined;
				let partitioned = false;
				let failed = false;
				let testError: unknown;
				let phase = "create offline author and runtime peers";
				try {
					// C is a real independent signer, never a transport member. Each
					// cycle imports one of its entries through public SharedLog.join.
					await authorBlocks.start();
					const author = await Ed25519Keypair.create();
					await authorLog.open(authorBlocks, author, {
						encoding: JSON_ENCODING,
					});
					const offline: {
						entry: Entry<any>;
						bytes: Uint8Array;
						value: string;
					}[] = [];
					for (let cycle = 1; cycle <= 3; cycle++) {
						const value = "cycle-" + cycle + " offline-third-author head";
						const { entry } = await authorLog.append(
							{ op: "ADD", value },
							{
								meta: {
									next: [],
									data: encodeReplicas(new AbsoluteReplicas(2)),
								},
							},
						);
						offline.push({ entry, bytes: serialize(entry), value });
					}
					const authorHash = author.publicKey.hashcode();
					await authorLog.close();
					await authorBlocks.stop();
					for (let index = 0; index < 2; index++) {
						peers.push(
							await Peerbit.create({
								...(nativeStorage
									? createRustPeerbitOptions({ network: false })
									: {}),
								libp2p: {
									connectionGater: {
										denyDialPeer: () => partitioned,
										denyInboundConnection: () => partitioned,
									},
								},
							}),
						);
					}
					const hashes = peers.map((peer) =>
						peer.identity.publicKey.hashcode(),
					);
					expect(hashes).not.to.include(authorHash);
					const template = new EventStore<string, any>();
					const args = {
						replicate: 1,
						replicas: { min: 2 },
						timeUntilRoleMaturity: 0,
						...(nativeStorage
							? { nativeGraph: true, nativeBackbone: { optional: false } }
							: {}),
					};
					// Index 0 is A, the writer. Index 1 is B.
					const stores = await Promise.all(
						peers.map((peer) => peer.open(template.clone(), { args })),
					);
					const logs = stores.map((store) => store.log as any);
					if (nativeStorage)
						for (const log of logs) {
							expect(log._nativeBackbone, "required native backbone").to.exist;
							expect(log.log.entryIndex.properties.nativeGraph).to.exist;
							expect(
								log._coordinates.canUseNativeBackboneResidentCoordinateState(),
							).to.equal(true);
						}

					phase = "instrument both peers";
					let cycle = 0;
					const create = logs.map((log) =>
						sandbox.spy(log, "recoverPeerEntryInventory"),
					);
					const wakes = logs.map((log) =>
						sandbox.spy(log._entryInventoryRecovery, "wake"),
					);
					let edgeWaiter:
						| {
								cycle: number;
								seen: Set<number>;
								deferred: DeferredPromise<void>;
						  }
						| undefined;
					let unreachable: DeferredPromise<void>[] = [];
					const unreachableTimes: number[] = [];
					let committed: DeferredPromise<any>[] = [];
					for (const [index, peer] of peers.entries()) {
						const other = hashes[1 - index];
						// Registered after open, so it runs after SharedLog's own listener.
						(peer.services.pubsub as any).addEventListener(
							"peer:stream-ready",
							({ detail }: CustomEvent<PublicSignKey>) => {
								if (detail.hashcode() !== other) return;
								const session = logs[index]._peerSessions.current(other);
								edges[index].push({
									cycle,
									session,
									phase: session?.phase,
									active: session?.isActive() ?? false,
									committed: session
										? logs[index]._v2Receive._cutoverPeerSessions.has(session)
										: false,
								});
								if (edgeWaiter?.cycle === cycle) {
									edgeWaiter.seen.add(index);
									if (edgeWaiter.seen.size === 2) edgeWaiter.deferred.resolve();
								}
							},
							{ signal: listeners.signal },
						);
						peer.services.pubsub.addEventListener(
							"unsubscribe",
							({ detail }) => {
								if (
									detail.reason === "peer-unreachable" &&
									detail.topics.includes(logs[index].topic) &&
									detail.from.hashcode() === other
								) {
									unreachableTimes[index] ??= performance.now();
									unreachable[index]?.resolve();
								}
							},
							{ signal: listeners.signal },
						);
						const receive = logs[index]._v2Receive;
						const commit = receive.commit.bind(receive);
						sandbox.stub(receive, "commit").callsFake((admission: any) => {
							const result = commit(admission);
							if (
								result === true &&
								admission.kind === "full" &&
								admission.state.peerHash === other &&
								admission.state.peerSession ===
									logs[index]._peerSessions.current(other)
							)
								committed[index]?.resolve(admission.state.peerSession);
							return result;
						});
					}
					const offers = sandbox.spy(
						logs[0].syncronizer,
						"onMaybeMissingEntries",
					);
					const pushes = sandbox.spy(logs[0], "pushEntryHashes");
					const callsFor = (
						spy: sinon.SinonSpy,
						index: number,
						session: object,
					) =>
						spy
							.getCalls()
							.filter(
								(call) =>
									call.args[0] === hashes[1 - index] &&
									call.args[1] === session,
							).length;
					const passesFor = (index: number, session: object) =>
						callsFor(create[index], index, session);
					const wakesFor = (index: number, session: object) =>
						callsFor(wakes[index], index, session);
					diagnostics = () => ({
						edges: edges.map((records) =>
							records.map((record) => ({
								cycle: record.cycle,
								session: sessionId(record.session),
								phase: record.phase,
								active: record.active,
								committed: record.committed,
							})),
						),
						passes: create.map((spy) =>
							spy.getCalls().map((call) => sessionId(call.args[1])),
						),
						wakes: wakes.map((spy) =>
							spy.getCalls().map((call) => sessionId(call.args[1])),
						),
						current: [0, 1].map((index) =>
							sessionId(logs[index]._peerSessions.current(hashes[1 - index])),
						),
					});

					const settled = (index: number) => {
						const log = logs[index],
							other = hashes[1 - index];
						const session = log._peerSessions.current(other);
						const send = log._v2Send._sendStates.get(other);
						const receive = log._v2Receive._receiveStates.get(other);
						expect(
							session?.phase,
							"peer " + index + "'s opposite session",
						).to.equal("open");
						expect(send?.established).to.equal(true);
						expect(send.suspended).to.equal(false);
						for (const key of [
							"worker",
							"pending",
							"retryTimer",
							"applicationConfirmationRequest",
						])
							expect(send[key], key).to.equal(undefined);
						expect(receive?.phase).to.equal("active");
						for (const key of [
							"requestTimer",
							"requestInFlight",
							"reservedAdmission",
						])
							expect(receive[key], key).to.equal(undefined);
						expect(receive.receiveEpoch).to.equal(
							log._peerSessions.receiveEpoch(other),
						);
					};
					phase = "connect and settle both full replicas";
					await peers[1].dial(peers[0].getMultiaddrs());
					await waitForResolved(
						() => {
							for (let index = 0; index < 2; index++) {
								settled(index);
								expect(
									logs[index]._joinAuthoritativeRepairTimersByDelay.has(30_000),
								).to.equal(true);
							}
						},
						{ timeout: 10_000 },
					);
					phase = "drain real initial membership repair windows";
					await waitForResolved(
						() => {
							for (const log of logs) {
								expect(log.replicationChangeDebounceFn.size()).to.equal(0);
								expect(log.pendingMaturity.size).to.equal(0);
								expect(log._joinAuthoritativeRepairTimersByDelay.size).to.equal(
									0,
								);
								expect(log._repairSweepRunning).to.equal(false);
								expect(log._repairSweepPendingModes.size).to.equal(0);
								expect(log._appendBackfillPendingByTarget.size).to.equal(0);
								expect(log._entryInventoryRecovery.active.size).to.equal(0);
								expect(log._entryInventoryRecovery.pending.size).to.equal(0);
							}
						},
						{ timeout: 40_000 },
					);

					const exactLocalEntry = async (
						index: number,
						entry: Entry<any>,
						value: string,
					) => {
						const log = stores[index].log.log;
						const local = await log.get(entry.hash, { remote: false });
						expect(local, `${value} must be held locally by peer ${index}`).to
							.exist;
						expect((await local!.getPayloadValue()).value).to.equal(value);
						expect(await local!.verifySignatures()).to.equal(true);
						expect(local!.meta.next).to.deep.equal([]);
						const shallow = await log.entryIndex.getShallow(entry.hash);
						expect(shallow?.value.head).to.equal(true);
						const coordinate = await logs[
							index
						]._coordinates.getAuthoritativeCoordinateEntryForInventory(
							entry.hash,
						);
						expect(coordinate?.hash).to.equal(entry.hash);
						expect(coordinate!.coordinates.length).to.be.greaterThan(0);
					};
					const exactEntry = async (
						index: number,
						{ entry, bytes, value }: (typeof offline)[number],
					) => {
						const store = stores[index];
						const local = await store.log.log.get(entry.hash, {
							remote: false,
						});
						expect(
							local,
							"missing " + value + " at " + (index === 0 ? "A" : "B"),
						).to.exist;
						expect(serialize(local!)).to.deep.equal(bytes);
						expect((await local!.getPayloadValue()).value).to.equal(value);
						expect(local!.meta.next).to.deep.equal([]);
						expect(await local!.verifySignatures()).to.equal(true);
						expect(
							(await local!.getPublicKeys()).map((key) => key.hashcode()),
						).to.deep.equal([authorHash]);
						expect(
							(await store.log.log.entryIndex.getShallow(entry.hash))?.value
								.head,
						).to.equal(true);
						const coordinates = logs[index]._coordinates;
						const coordinate =
							await coordinates.getAuthoritativeCoordinateEntryForInventory(
								entry.hash,
							);
						expect(coordinate?.hash).to.equal(entry.hash);
						expect(coordinate.coordinates.length).to.be.greaterThan(0);
						if (nativeStorage)
							expect(
								coordinates._residentEntryCoordinatesByHash.has(entry.hash),
							).to.equal(true);
					};

					let previous: any[] = [];
					const renewed: any[] = [];
					let unreachableAt = 0;
					let reconnectAt = 0;
					const partition = async () => {
						unreachable = [pDefer<void>(), pDefer<void>()];
						unreachableTimes.length = 0;
						committed = [pDefer<any>(), pDefer<any>()];
						const deadline = performance.now() + 5_000;
						partitioned = true;
						await Promise.all([
							peers[0].hangUp(peers[1].identity.publicKey),
							peers[1].hangUp(peers[0].identity.publicKey),
						]);
						await within(
							Promise.all(unreachable.map((deferred) => deferred.promise)),
							Math.max(1, deadline - performance.now()),
							"both peers observe the other as unreachable",
						);
						await waitForResolved(
							() => {
								for (const peer of peers)
									expect(peer.libp2p.getConnections()).to.have.length(0);
							},
							{ timeout: Math.max(1, deadline - performance.now()) },
						);
						unreachableAt = Math.min(...unreachableTimes);
						previous = [0, 1].map((index) =>
							logs[index]._peerSessions.current(hashes[1 - index]),
						);
					};
					const write = async (n: number) => {
						const value = "cycle-" + n + " local";
						const { entry: local } = await stores[0].add(value, {
							meta: { next: [] },
						});
						expect(
							decodeReplicas(local).getValue(logs[0]),
							"the local append inherits the store's replicas",
						).to.equal(2);
						await stores[0].log.join([offline[n - 1].entry], {
							verifySignatures: true,
						});
						await exactLocalEntry(0, local, value);
						await exactEntry(0, offline[n - 1]);
						expect(await stores[1].log.log.has(local.hash)).to.equal(false);
						expect(
							await stores[1].log.log.has(offline[n - 1].entry.hash),
						).to.equal(false);
						return { entry: local, value };
					};
					const heal = async () => {
						await waitForResolved(
							() => {
								// libp2p joins an existing queued dial before rechecking its
								// gater. Let dials denied during the partition retire, then
								// unblock in this same turn so the one explicit dial cannot
								// inherit an earlier denial.
								for (const [index, peer] of peers.entries())
									expect(
										peer.libp2p
											.getDialQueue()
											.filter((dial) =>
												dial.peerId?.equals(peers[1 - index].libp2p.peerId),
											),
									).to.have.length(0);
								partitioned = false;
								reconnectAt = performance.now();
							},
							{ timeout: 2_000 },
						);
						await peers[1].dial(peers[0].getMultiaddrs());
						expect(
							reconnectAt - unreachableAt,
							"the reconnect starts within nine seconds of the departure",
						).to.be.lessThan(9_000);
					};
					const renew = async () => {
						const sessions = await within(
							Promise.all(committed.map((deferred) => deferred.promise)),
							10_000,
							"both renewed sessions commit their Full",
						);
						for (let index = 0; index < 2; index++) {
							renewed[index] = sessions[index];
							expect(
								sessions[index] !== previous[index],
								"side " + index + " commits a renewed session",
							).to.equal(true);
							expect(sessions[index].hasPredecessor).to.equal(true);
						}
					};
					const delivered = async (
						n: number,
						local: Awaited<ReturnType<typeof write>>,
					) => {
						await waitForResolved(
							async () => {
								await exactLocalEntry(1, local.entry, local.value);
								await exactEntry(1, offline[n - 1]);
								expect(performance.now() - reconnectAt).to.be.lessThan(5_000);
							},
							{
								timeout: Math.max(1, 5_000 - (performance.now() - reconnectAt)),
							},
						);
					};
					// Hold each side's Subscribe processing until both sides saw their
					// new stream become writable, so both edges precede both opens.
					const holdSubscriptionsUntilBothEdges = (n: number) => {
						const bothEdges = pDefer<void>();
						releases.push(bothEdges);
						edgeWaiter = { cycle: n, seen: new Set(), deferred: bothEdges };
						const holds = logs.map((log) => {
							const original = log._onSubscription.bind(log);
							return sandbox
								.stub(log, "_onSubscription")
								.callsFake(async (event: any) => {
									await bothEdges.promise;
									return original(event);
								});
						});
						return {
							bothEdges: bothEdges.promise,
							restore: () => {
								for (const hold of holds) hold.restore();
							},
						};
					};
					const assertUnwokenCycle = (n: number) => {
						for (let index = 0; index < 2; index++) {
							const records = edges[index].filter(
								(record) => record.cycle === n,
							);
							expect(records, "side " + index + " edges").not.to.be.empty;
							for (const record of records) {
								expect(
									record.phase,
									"side " + index + " edge phase",
								).not.to.equal("open");
								expect(
									record.committed,
									"side " + index + " edge committed",
								).to.equal(false);
							}
							expect(
								wakesFor(index, renewed[index]),
								"side " + index + " wakes",
							).to.equal(0);
							expect(
								passesFor(index, renewed[index]),
								"side " + index + " passes",
							).to.equal(0);
						}
					};

					// Both edges arrive before either side opens its renewed session.
					cycle = 1;
					phase = "cycle 1: partition";
					await partition();
					phase = "cycle 1: write while partitioned";
					const a1 = await write(1);
					phase = "cycle 1: heal with Subscribe processing held";
					const hold1 = holdSubscriptionsUntilBothEdges(1);
					await heal();
					await within(
						hold1.bothEdges,
						5_000,
						"forcing device: an edge was not observed",
					);
					hold1.restore();
					phase = "cycle 1: renewed sessions commit their Full";
					await renew();
					phase = "cycle 1: deliver within five seconds of the reconnect";
					await delivered(1, a1);
					phase = "cycle 1: no edge saw an open session and no side walked";
					assertUnwokenCycle(1);

					// The same ordering with no drain, after a join-authoritative sweep
					// for B ran while B was away. The sweep names B, which makes B a
					// full-replica repair candidate, so A must send this cycle's
					// entries into the dead transport, and delivery after the return
					// must not wait on those sends.
					cycle = 2;
					const timersPending =
						logs[0]._joinAuthoritativeRepairTimersByDelay.size;
					let repairForBRunning: boolean | undefined;
					let sentDuringAbsence: string | undefined;
					const cycle2 = (text: string) =>
						`cycle 2 (timersPending=${timersPending}, repairForBRunning=${repairForBRunning ?? "unrecorded"}, sentDuringAbsence=${sentDuringAbsence ?? "unrecorded"}): ${text}`;
					phase = cycle2("partition");
					await partition();
					phase = cycle2("write while partitioned");
					const a2 = await write(2);
					// Nothing carried this cycle's entries before it wrote them, and B
					// stays away until the heal below, so every send of them until
					// then went into the dead transport.
					const cycle2Hashes = new Set([a2.entry.hash, offline[1].entry.hash]);
					const carriesCycle2Entry = (entryHashes: Iterable<string>) => {
						for (const hash of entryHashes)
							if (cycle2Hashes.has(hash)) return true;
						return false;
					};
					const sendsToB = () => {
						const offered = offers.getCalls().some((call) => {
							const { entries, targets } = call.args[0] as {
								entries: ReadonlyMap<string, unknown>;
								targets: string[];
							};
							return (
								targets.includes(hashes[1]) &&
								carriesCycle2Entry(entries.keys())
							);
						});
						const pushed = pushes
							.getCalls()
							.some(
								(call) =>
									call.args[0] === hashes[1] &&
									carriesCycle2Entry(call.args[1] as string[]),
							);
						return offered
							? pushed
								? "offered and pushed"
								: "offered"
							: pushed
								? "pushed"
								: "nothing";
					};
					phase = cycle2(
						"run a delayed join-authoritative sweep while B is away",
					);
					// One of cycle 1's delayed sweeps can fire while B is away and
					// start the repair for B first. This sweep then adds this cycle's
					// entries to that repair, whose next round pushes them.
					repairForBRunning =
						logs[0]._repairFrontierActiveTargetsByMode
							.get("join-authoritative")
							?.has(hashes[1]) === true;
					logs[0].scheduleRepairSweep({
						mode: "join-authoritative",
						peers: new Set([hashes[1]]),
					});
					await waitForResolved(
						() => {
							expect(logs[0]._repairSweepRunning).to.equal(false);
							expect(logs[0]._repairSweepPendingModes.size).to.equal(0);
						},
						{ timeout: 5_000 },
					);
					phase = cycle2("A sent this cycle's entries into the dead transport");
					await waitForResolved(
						() => {
							sentDuringAbsence = sendsToB();
							expect(
								sentDuringAbsence,
								"the sweep names B, a full-replica repair candidate, so A offers or pushes a2 or the imported cycle-2 head into the dead transport",
							).not.to.equal("nothing");
						},
						{ timeout: 5_000 },
					);
					phase = cycle2("heal with Subscribe processing held");
					const hold2 = holdSubscriptionsUntilBothEdges(2);
					await heal();
					await within(
						hold2.bothEdges,
						5_000,
						"forcing device: an edge was not observed",
					);
					hold2.restore();
					phase = cycle2("renewed sessions commit their Full");
					await renew();
					phase = cycle2("deliver within five seconds of the reconnect");
					await delivered(2, a2);
					phase = cycle2("no edge saw an open session and no side walked");
					assertUnwokenCycle(2);

					// A's edge arrives after A opened its renewed session and before
					// that session's Full committed.
					cycle = 3;
					phase = "cycle 3: partition";
					await partition();
					phase = "cycle 3: write while partitioned";
					const a3 = await write(3);
					phase = "cycle 3: heal with A's edge held until A opens";
					const pubsubA = peers[0].services.pubsub as any;
					const dispatch = pubsubA.dispatchEvent.bind(pubsubA);
					let captured: PublicSignKey | undefined;
					const capture = sandbox
						.stub(pubsubA, "dispatchEvent")
						.callsFake((event: any) => {
							if (
								event.type === "peer:stream-ready" &&
								event.detail.hashcode() === hashes[1] &&
								captured === undefined &&
								logs[0]._peerSessions.current(hashes[1])?.phase !== "open"
							) {
								captured = event.detail;
								return true;
							}
							return dispatch(event);
						});
					const markOpen = logs[0]._peerSessions.markOpen.bind(
						logs[0]._peerSessions,
					);
					const aOpened = pDefer<object>();
					const open = sandbox
						.stub(logs[0]._peerSessions, "markOpen")
						.callsFake((peerHash: any, expected: any) => {
							markOpen(peerHash, expected);
							if (
								peerHash === hashes[1] &&
								logs[0]._peerSessions.current(hashes[1]) === expected &&
								expected.phase === "open"
							)
								aOpened.resolve(expected);
						});
					await heal();
					const opened = await within(
						aOpened.promise,
						5_000,
						"A opens its renewed session",
					);
					if (captured)
						dispatch(
							new CustomEvent("peer:stream-ready", { detail: captured }),
						);
					phase = "cycle 3: renewed sessions commit their Full";
					await renew();
					expect(
						renewed[0] === opened,
						"A's committed renewed session is the one it opened",
					).to.equal(true);
					phase = "cycle 3: deliver within five seconds of the reconnect";
					await delivered(3, a3);
					phase =
						"cycle 3: A's edge saw its open session before its Full and no side walked";
					const records3 = edges.map((records) =>
						records.filter((record) => record.cycle === 3),
					);
					expect(
						records3[0].some(
							(record) =>
								record.session === opened &&
								record.phase === "open" &&
								record.active === true &&
								record.committed === false,
						),
						"A records an edge under its open, uncommitted session",
					).to.equal(true);
					for (const [index, records] of records3.entries())
						for (const record of records)
							expect(
								record.committed,
								"side " + index + " edge committed",
							).to.equal(false);
					for (let index = 0; index < 2; index++) {
						expect(
							wakesFor(index, renewed[index]),
							"side " + index + " wakes",
						).to.equal(0);
						expect(
							passesFor(index, renewed[index]),
							"side " + index + " passes",
						).to.equal(0);
					}

					phase = "cycle 3: an edge after the Full wakes the settled session";
					capture.restore();
					open.restore();
					const passesBefore = create[0].callCount;
					pubsubA.dispatchEvent(
						new CustomEvent("peer:stream-ready", {
							detail: peers[1].identity.publicKey,
						}),
					);
					expect(edges[0][edges[0].length - 1].committed).to.equal(true);
					expect(wakesFor(0, opened)).to.equal(1);
					expect(passesFor(0, opened)).to.be.at.least(1);
					// The scheduler creates the pass synchronously from the wake.
					const first = create[0].getCall(passesBefore);
					expect(first?.args[0]).to.equal(hashes[1]);
					expect(
						first?.args[1] === opened,
						"the first new pass serves A's renewed session",
					).to.equal(true);
				} catch (error) {
					if (error instanceof Error)
						error.message = phase + ": " + error.message;
					testError = error;
					failed = true;
					try {
						console.error(
							"Renewed-session reconnect failure:",
							JSON.stringify({ phase, nativeStorage, ...diagnostics?.() }),
						);
					} catch {
						// Diagnostics never replace the failure.
					}
				} finally {
					for (const deferred of releases) deferred.resolve();
					listeners.abort();
					partitioned = false;
					try {
						const stopped = await Promise.allSettled(
							peers.map((peer) => peer.stop()),
						);
						const drained = await Promise.allSettled([
							authorLog.close(),
							authorBlocks.stop(),
						]);
						const cleanupErrors = [...stopped, ...drained].flatMap((result) =>
							result.status === "rejected" ? [result.reason] : [],
						);
						if (cleanupErrors.length) {
							testError = new AggregateError(
								failed ? [testError, ...cleanupErrors] : cleanupErrors,
								"Renewed-session fixture cleanup failed",
								{ cause: testError },
							);
							failed = true;
						}
					} finally {
						sandbox.restore();
					}
				}
				if (failed) throw testError;
			},
		);
	}
});
