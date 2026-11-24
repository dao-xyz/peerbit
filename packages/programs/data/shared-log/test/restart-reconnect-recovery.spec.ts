import { serialize } from "@dao-xyz/borsh";
import { keys } from "@libp2p/crypto";
import { AnyBlockStore } from "@peerbit/blocks";
import {
	Ed25519Keypair,
	type PublicSignKey,
	randomBytes,
} from "@peerbit/crypto";
import { type Entry, Log } from "@peerbit/log";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer, { type DeferredPromise } from "p-defer";
import path from "path";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { AbsoluteReplicas, encodeReplicas } from "../src/replication.js";
import { JSON_ENCODING } from "./utils/stores/encoding.js";
import { EventStore } from "./utils/stores/index.js";

type EdgeRecord = {
	session: any;
	phase?: string;
	active: boolean;
	committed: boolean;
};

type OfflineHead = {
	entry: Entry<any>;
	bytes: Uint8Array;
	value: string;
	authorHash: string;
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

// Entry hashes a shared-log message names: pushed heads (decoded or raw),
// offered or requested hashes, and hash numbers of a coordinate request.
const namesEntries = (message: any) =>
	(Array.isArray(message?.heads) && message.heads.length > 0) ||
	(Array.isArray(message?.hashes) && message.hashes.length > 0) ||
	(Array.isArray(message?.hashNumbers) && message.hashNumbers.length > 0);
const namedHashes = (message: any): string[] => {
	const hashes: string[] = [];
	for (const head of Array.isArray(message?.heads) ? message.heads : []) {
		const hash = head?.entry?.hash ?? head?.hash;
		if (typeof hash === "string") hashes.push(hash);
	}
	for (const hash of Array.isArray(message?.hashes) ? message.hashes : [])
		if (typeof hash === "string") hashes.push(hash);
	return hashes;
};

// A head signed by an independent author that never joins the network. The
// writer imports it through public SharedLog.join.
const createOfflineHead = async (value: string): Promise<OfflineHead> => {
	const blocks = new AnyBlockStore();
	const log = new Log<any>();
	await blocks.start();
	try {
		const author = await Ed25519Keypair.create();
		await log.open(blocks, author, { encoding: JSON_ENCODING });
		const { entry } = await log.append(
			{ op: "ADD", value },
			{
				meta: {
					next: [],
					data: encodeReplicas(new AbsoluteReplicas(2)),
				},
			},
		);
		await log.close();
		return {
			entry,
			bytes: serialize(entry),
			value,
			authorHash: author.publicKey.hashcode(),
		};
	} finally {
		await blocks.stop();
	}
};

const expectSettled = (log: any, other: string) => {
	const session = log._peerSessions.current(other);
	const send = log._v2Send._sendStates.get(other);
	const receive = log._v2Receive._receiveStates.get(other);
	expect(session?.phase).to.equal("open");
	expect(send?.established).to.equal(true);
	expect(send.suspended).to.equal(false);
	expect(receive?.phase).to.equal("active");
	for (const key of ["requestTimer", "requestInFlight", "reservedAdmission"])
		expect(receive[key], key).to.equal(undefined);
};

const expectRepairIdle = (log: any) => {
	expect(log.replicationChangeDebounceFn.size()).to.equal(0);
	expect(log.pendingMaturity.size).to.equal(0);
	expect(log._joinAuthoritativeRepairTimersByDelay.size).to.equal(0);
	expect(log._repairSweepRunning).to.equal(false);
	expect(log._repairSweepPendingModes.size).to.equal(0);
	expect(log._appendBackfillPendingByTarget.size).to.equal(0);
	expect(log._entryInventoryRecovery.active.size).to.equal(0);
	expect(log._entryInventoryRecovery.pending.size).to.equal(0);
};

const expectHeldLocal = async (
	store: EventStore<string, any>,
	entry: Entry<any>,
	value: string,
	where: string,
) => {
	const log = store.log.log;
	const local = await log.get(entry.hash, { remote: false });
	expect(local, `${value} must be held at ${where}`).to.exist;
	expect((await local!.getPayloadValue()).value).to.equal(value);
	expect(await local!.verifySignatures()).to.equal(true);
	expect(local!.meta.next).to.deep.equal([]);
	expect((await log.entryIndex.getShallow(entry.hash))?.value.head).to.equal(
		true,
	);
	const coordinate = await (
		store.log as any
	)._coordinates.getAuthoritativeCoordinateEntryForInventory(entry.hash);
	expect(coordinate?.hash).to.equal(entry.hash);
	expect(coordinate!.coordinates.length).to.be.greaterThan(0);
};

const expectHeldOffline = async (
	store: EventStore<string, any>,
	head: OfflineHead,
	where: string,
) => {
	await expectHeldLocal(store, head.entry, head.value, where);
	const local = await store.log.log.get(head.entry.hash, { remote: false });
	expect(serialize(local!)).to.deep.equal(head.bytes);
	expect(
		(await local!.getPublicKeys()).map((key) => key.hashcode()),
	).to.deep.equal([head.authorHash]);
};

describe("receive admission restart reconnect recovery", function () {
	// The second test drains the real startup repair windows, which takes up to
	// 40 seconds; each delivery then has five seconds.
	this.timeout(90_000);

	it("delivers what a peer missed before this node restarted on persisted state, once the peer dials back", async () => {
		const sandbox = sinon.createSandbox();
		const listeners = new AbortController();
		// Deferreds that product code may await; teardown resolves them.
		const releases: DeferredPromise<any>[] = [];
		const directories: string[] = [];
		const peers: Peerbit[] = [];
		let diagnostics: (() => object) | undefined;
		let partitioned = false;
		let blocked = false;
		let failed = false;
		let testError: unknown;
		let phase = "create the offline author's head";
		const connectionGater = () => ({
			denyDialPeer: () => partitioned,
			denyInboundConnection: () => partitioned,
		});
		try {
			const offline = await createOfflineHead(
				"restart offline-third-author head",
			);

			phase = "create directory-backed peers";
			for (const label of ["writer", "peer"])
				directories.push(
					await fs.mkdtemp(
						path.join(os.tmpdir(), `shared-log-restart-recovery-${label}-`),
					),
				);
			for (const directory of directories)
				peers.push(
					await Peerbit.create({
						directory,
						libp2p: { connectionGater: connectionGater() },
					}),
				);
			const hashes = peers.map((peer) => peer.identity.publicKey.hashcode());
			expect(hashes).not.to.include(offline.authorHash);
			const storeId = randomBytes(32);
			// Both peers resume their persisted range on open.
			const args = {
				replicate: { type: "resume" as const, default: 1 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			};
			const open = (peer: Peerbit) =>
				peer.open(new EventStore<string, any>({ id: storeId }), { args });
			// Index 0 is A, the writer that restarts. Index 1 is B.
			const stores = [await open(peers[0]), await open(peers[1])];
			const logB = stores[1].log as any;

			phase = "connect and settle";
			await peers[1].dial(peers[0].getMultiaddrs());
			await waitForResolved(
				() => {
					expectSettled(stores[0].log as any, hashes[1]);
					expectSettled(logB, hashes[0]);
				},
				{ timeout: 15_000 },
			);

			// B receives nothing that names an entry while blocked: a delivery cut
			// short by a crash right after the write.
			phase = "write while B's receipt is cut";
			const cut = new Set<string>();
			const cutWaiters = new Map<string, DeferredPromise<void>>();
			const onMessageB = logB.onMessage.bind(logB);
			sandbox
				.stub(logB, "onMessage")
				.callsFake(async (message: any, context: any) => {
					if (blocked && namesEntries(message)) {
						for (const hash of namedHashes(message)) {
							cut.add(hash);
							cutWaiters.get(hash)?.resolve();
						}
						return;
					}
					return onMessageB(message, context);
				});
			const cutFor = (hash: string) => {
				if (cut.has(hash)) return Promise.resolve();
				const deferred = pDefer<void>();
				releases.push(deferred);
				cutWaiters.set(hash, deferred);
				return deferred.promise;
			};
			blocked = true;
			const localValue = "restart local head";
			const { entry: local } = await stores[0].add(localValue, {
				meta: { next: [] },
			});
			await stores[0].log.join([offline.entry], { verifySignatures: true });
			await within(
				Promise.all([cutFor(local.hash), cutFor(offline.entry.hash)]),
				5_000,
				"A's delivery of both entries reaches B's cut",
			);
			await expectHeldLocal(stores[0], local, localValue, "A");
			await expectHeldOffline(stores[0], offline, "A");
			for (const hash of [local.hash, offline.entry.hash])
				expect(await stores[1].log.log.has(hash)).to.equal(false);

			phase = "A stops; B stays up";
			// Denied until A's program is open again, so A's restarted transport
			// cannot reach B early.
			partitioned = true;
			await peers[0].stop();
			blocked = false;

			phase = "A restarts on its directory";
			peers[0] = await Peerbit.create({
				directory: directories[0],
				libp2p: { connectionGater: connectionGater() },
			});
			expect(peers[0].identity.publicKey.hashcode()).to.equal(hashes[0]);
			stores[0] = await open(peers[0]);
			const logA = stores[0].log as any;
			expect(
				await logA.replicationIndex.count({ query: { hash: hashes[1] } }),
				"B's rows survive A's restart",
			).to.be.greaterThan(0);
			await expectHeldLocal(stores[0], local, localValue, "the restarted A");
			await expectHeldOffline(stores[0], offline, "the restarted A");

			phase = "instrument the restarted A";
			const passes = sandbox.spy(logA, "recoverPeerEntryInventory");
			const wakes = sandbox.spy(logA._entryInventoryRecovery, "wake");
			const sweeps = sandbox.spy(logA, "scheduleRepairSweep");
			const rangeChanges: string[] = [];
			const debouncer = logA.replicationChangeDebounceFn;
			const addChange = debouncer.add.bind(debouncer);
			sandbox.stub(debouncer, "add").callsFake((change: any) => {
				if (change?.range?.hash === hashes[1]) rangeChanges.push(change.type);
				return addChange(change);
			});
			const committed = pDefer<any>();
			releases.push(committed);
			const receive = logA._v2Receive;
			const commit = receive.commit.bind(receive);
			sandbox.stub(receive, "commit").callsFake((admission: any) => {
				const result = commit(admission);
				if (
					result === true &&
					admission.kind === "full" &&
					admission.state.peerHash === hashes[1] &&
					admission.state.peerSession === logA._peerSessions.current(hashes[1])
				)
					committed.resolve(admission.state.peerSession);
				return result;
			});
			// Registered after open, so it runs after SharedLog's own listener.
			const edges: EdgeRecord[] = [];
			peers[0].services.pubsub.addEventListener(
				"peer:stream-ready",
				({ detail }: CustomEvent<PublicSignKey>) => {
					if (detail.hashcode() !== hashes[1]) return;
					const session = logA._peerSessions.current(hashes[1]);
					edges.push({
						session,
						phase: session?.phase,
						active: session?.isActive() ?? false,
						committed: session
							? logA._v2Receive._cutoverPeerSessions.has(session)
							: false,
					});
				},
				{ signal: listeners.signal },
			);
			// A's edge for B comes after A opened its session and before that
			// session's Full commits: the ordering in which the pass used to run.
			const pubsubA = peers[0].services.pubsub as any;
			const dispatch = pubsubA.dispatchEvent.bind(pubsubA);
			let captured: PublicSignKey | undefined;
			let redispatched = false;
			sandbox.stub(pubsubA, "dispatchEvent").callsFake((event: any) => {
				if (
					event.type === "peer:stream-ready" &&
					event.detail.hashcode() === hashes[1] &&
					captured === undefined &&
					!redispatched &&
					logA._peerSessions.current(hashes[1])?.phase !== "open"
				) {
					captured = event.detail;
					return true;
				}
				return dispatch(event);
			});
			const opened = pDefer<any>();
			releases.push(opened);
			const markOpen = logA._peerSessions.markOpen.bind(logA._peerSessions);
			sandbox
				.stub(logA._peerSessions, "markOpen")
				.callsFake((peerHash: any, expected: any) => {
					const result = markOpen(peerHash, expected);
					if (
						peerHash === hashes[1] &&
						logA._peerSessions.current(hashes[1]) === expected &&
						expected.phase === "open"
					) {
						opened.resolve(expected);
						if (captured && !redispatched) {
							redispatched = true;
							const detail = captured;
							queueMicrotask(() =>
								dispatch(new CustomEvent("peer:stream-ready", { detail })),
							);
						}
					}
					return result;
				});
			diagnostics = () => ({
				edges: edges.map((record) => ({
					phase: record.phase,
					active: record.active,
					committed: record.committed,
				})),
				captured: captured !== undefined,
				redispatched,
				rangeChanges,
				passes: passes.callCount,
				wakes: wakes.callCount,
				joinAuthoritativeSweepsForB: sweeps
					.getCalls()
					.filter(
						(call) =>
							call.args[0]?.mode === "join-authoritative" &&
							call.args[0]?.peers?.has?.(hashes[1]),
					).length,
				cut: [...cut].filter(
					(hash) => hash === local.hash || hash === offline.entry.hash,
				).length,
			});

			phase = "B dials back once A's program is open";
			let reconnectAt = 0;
			await waitForResolved(
				() => {
					// libp2p joins an existing queued dial before rechecking its gater.
					// Let dials denied while A was away retire, then unblock in this
					// same turn so the one explicit dial cannot inherit a denial.
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
				{ timeout: 5_000 },
			);
			await peers[1].dial(peers[0].getMultiaddrs());
			const session = await within(
				opened.promise,
				5_000,
				"A opens its first session for B",
			);
			expect(session.hasPredecessor, "the restart's first session").to.equal(
				false,
			);
			expect(
				(await within(committed.promise, 10_000, "B's first Full commits")) ===
					session,
				"B's Full commits on the session A opened",
			).to.equal(true);
			expect(rangeChanges, "B's Full matches its surviving rows").to.deep.equal(
				[],
			);

			phase = "deliver within five seconds of the reconnect";
			await waitForResolved(
				async () => {
					await expectHeldLocal(stores[1], local, localValue, "B");
					await expectHeldOffline(stores[1], offline, "B");
					expect(performance.now() - reconnectAt).to.be.lessThan(5_000);
				},
				{
					timeout: Math.max(1, 5_000 - (performance.now() - reconnectAt)),
				},
			);

			phase =
				"A's edge saw its open session before the Full and no pass ran for it";
			expect(
				edges.some(
					(record) =>
						record.session === session &&
						record.phase === "open" &&
						record.active === true &&
						record.committed === false,
				),
				"A records an edge under its open, uncommitted session",
			).to.equal(true);
			const forSession = (spy: sinon.SinonSpy) =>
				spy
					.getCalls()
					.filter(
						(call) => call.args[0] === hashes[1] && call.args[1] === session,
					).length;
			expect(forSession(wakes), "wakes").to.equal(0);
			expect(forSession(passes), "passes").to.equal(0);
		} catch (error) {
			if (error instanceof Error) error.message = phase + ": " + error.message;
			testError = error;
			failed = true;
			try {
				console.error(
					"Restart reconnect failure:",
					JSON.stringify({ phase, ...diagnostics?.() }),
				);
			} catch {
				// Diagnostics never replace the failure.
			}
		} finally {
			for (const deferred of releases) deferred.resolve();
			listeners.abort();
			partitioned = false;
			blocked = false;
			try {
				const stopped = await Promise.allSettled(
					peers.map((peer) => peer.stop()),
				);
				const removed = await Promise.allSettled(
					directories.map((directory) =>
						fs.rm(directory, { recursive: true, force: true }),
					),
				);
				const cleanupErrors = [...stopped, ...removed].flatMap((result) =>
					result.status === "rejected" ? [result.reason] : [],
				);
				if (cleanupErrors.length) {
					testError = new AggregateError(
						failed ? [testError, ...cleanupErrors] : cleanupErrors,
						"Restart reconnect fixture cleanup failed",
						{ cause: testError },
					);
					failed = true;
				}
			} finally {
				sandbox.restore();
			}
		}
		if (failed) throw testError;
	});

	it("delivers an entry still marked known by a peer that came back with an identical range this node never saw leave", async () => {
		const sandbox = sinon.createSandbox();
		const listeners = new AbortController();
		const releases: DeferredPromise<any>[] = [];
		const peers: Peerbit[] = [];
		let diagnostics: (() => object) | undefined;
		let partitioned = false;
		let failed = false;
		let testError: unknown;
		let phase = "create peers";
		const connectionGater = () => ({
			denyDialPeer: () => partitioned,
			denyInboundConnection: () => partitioned,
		});
		try {
			// B comes back with the same identity and the same range id.
			const keyB = await keys.generateKeyPair("Ed25519");
			const rangeId = new Uint8Array(32).fill(7);
			peers.push(
				await Peerbit.create({
					libp2p: { connectionGater: connectionGater() },
				}),
			);
			peers.push(
				await Peerbit.create({
					libp2p: { privateKey: keyB, connectionGater: connectionGater() },
				}),
			);
			const hashes = peers.map((peer) => peer.identity.publicKey.hashcode());
			const template = new EventStore<string, any>();
			const argsA = {
				replicate: 1,
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			};
			const argsB = {
				replicate: { id: rangeId, factor: 1, offset: 0 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			};
			// Index 0 is A, the writer. Index 1 is B.
			const stores = [
				await peers[0].open(template.clone(), { args: argsA }),
				await peers[1].open(template.clone(), { args: argsB }),
			];
			const logA = stores[0].log as any;

			phase = "connect and settle";
			await peers[1].dial(peers[0].getMultiaddrs());
			await waitForResolved(
				() => {
					expectSettled(logA, hashes[1]);
					expectSettled(stores[1].log as any, hashes[0]);
				},
				{ timeout: 15_000 },
			);
			phase = "drain real initial membership repair windows";
			await waitForResolved(
				() => {
					expectRepairIdle(logA);
					expectRepairIdle(stores[1].log as any);
				},
				{ timeout: 40_000 },
			);

			// A real 500 ms timer flushes Y's append backfill, so Y is still
			// pending there when B comes back within that window. Hold the flush
			// so that window spans B's absence.
			phase = "A writes Y, which B receives and confirms";
			let holdBackfill = true;
			let heldController: AbortController | undefined;
			const flushBackfill = logA.flushAppendBackfill.bind(logA);
			sandbox
				.stub(logA, "flushAppendBackfill")
				.callsFake((...args: unknown[]) => {
					const [controller] = args as [AbortController];
					if (!holdBackfill) return flushBackfill(controller);
					heldController = controller;
				});
			const yValue = "delivered before the restart";
			const { entry: y } = await stores[0].add(yValue, {
				meta: { next: [] },
			});
			await waitForResolved(
				async () => {
					await expectHeldLocal(stores[1], y, yValue, "B");
					expect(logA.isEntryKnownByPeer(y.hash, hashes[1])).to.equal(true);
				},
				{ timeout: 10_000 },
			);
			const stale = logA._peerSessions.current(hashes[1]);

			// The narrowest stand-in for a departure this node never observed:
			// A's subscription callback ignores B's unsubscribe.
			phase = "B leaves while A misses the departure";
			const onUnsubscription = logA._onUnsubscription.bind(logA);
			sandbox
				.stub(logA, "_onUnsubscription")
				.callsFake((event: any) =>
					event?.detail?.from?.hashcode?.() === hashes[1]
						? Promise.resolve()
						: onUnsubscription(event),
				);
			const unreachable = [pDefer<void>(), pDefer<void>()];
			releases.push(...unreachable);
			for (const [index, peer] of peers.entries())
				peer.services.pubsub.addEventListener(
					"unsubscribe",
					({ detail }) => {
						if (
							detail.reason === "peer-unreachable" &&
							detail.topics.includes(logA.topic) &&
							detail.from.hashcode() === hashes[1 - index]
						)
							unreachable[index].resolve();
					},
					{ signal: listeners.signal },
				);
			partitioned = true;
			await Promise.all([
				peers[0].hangUp(peers[1].identity.publicKey),
				peers[1].hangUp(peers[0].identity.publicKey),
			]);
			await within(
				Promise.all(unreachable.map((deferred) => deferred.promise)),
				5_000,
				"both peers observe the other as unreachable",
			);
			await peers[1].stop();

			phase = "A writes X while B is away";
			const xValue = "written while B was away";
			const { entry: x } = await stores[0].add(xValue, {
				meta: { next: [] },
			});

			phase = "B restarts with the same key, range and empty storage";
			peers[1] = await Peerbit.create({
				libp2p: { privateKey: keyB, connectionGater: connectionGater() },
			});
			expect(peers[1].identity.publicKey.hashcode()).to.equal(hashes[1]);
			stores[1] = await peers[1].open(template.clone(), { args: argsB });
			for (const hash of [y.hash, x.hash])
				expect(await stores[1].log.log.has(hash)).to.equal(false);
			expect(
				logA._peerSessions.current(hashes[1]) === stale,
				"A still serves B's previous session",
			).to.equal(true);
			expect(
				await logA.replicationIndex.count({ query: { hash: hashes[1] } }),
				"A still holds B's rows",
			).to.be.greaterThan(0);
			expect(
				logA.isEntryKnownByPeer(y.hash, hashes[1]),
				"A still marks Y known by B",
			).to.equal(true);
			expect(
				logA._appendBackfillPendingByTarget.get(hashes[1])?.has(y.hash),
				"Y's append backfill for B is still pending",
			).to.equal(true);

			phase = "instrument A";
			const passes = sandbox.spy(logA, "recoverPeerEntryInventory");
			const wakes = sandbox.spy(logA._entryInventoryRecovery, "wake");
			const rangeChanges: string[] = [];
			const debouncer = logA.replicationChangeDebounceFn;
			const addChange = debouncer.add.bind(debouncer);
			sandbox.stub(debouncer, "add").callsFake((change: any) => {
				if (change?.range?.hash === hashes[1]) rangeChanges.push(change.type);
				return addChange(change);
			});
			const committed = pDefer<any>();
			releases.push(committed);
			const receive = logA._v2Receive;
			const commit = receive.commit.bind(receive);
			sandbox.stub(receive, "commit").callsFake((admission: any) => {
				const result = commit(admission);
				if (
					result === true &&
					admission.kind === "full" &&
					admission.state.peerHash === hashes[1] &&
					admission.state.peerSession !== stale &&
					admission.state.peerSession === logA._peerSessions.current(hashes[1])
				)
					committed.resolve(admission.state.peerSession);
				return result;
			});
			// Once the renewed session's repair holds Y in its frontier, release
			// the held backfill: its dispatch does not bypass the known mark.
			const frontierHoldsY = pDefer<void>();
			releases.push(frontierHoldsY);
			const queueFrontier = logA.queueRepairFrontierEntries.bind(logA);
			sandbox
				.stub(logA, "queueRepairFrontierEntries")
				.callsFake((...args: unknown[]) => {
					const [mode, target, entries] = args as [
						string,
						string,
						ReadonlyMap<string, unknown>,
					];
					const queued = queueFrontier(...args);
					if (
						holdBackfill &&
						queued &&
						mode === "join-authoritative" &&
						target === hashes[1] &&
						entries.has(y.hash) &&
						logA._peerSessions.current(hashes[1]) !== stale &&
						logA._repairFrontierBypassKnownPeersByMode.get(mode)?.has(target)
					) {
						holdBackfill = false;
						frontierHoldsY.resolve();
						const release =
							heldController ??
							logA._instanceLifecycle.ownershipLifecycleController;
						queueMicrotask(() => flushBackfill(release));
					}
					return queued;
				});
			const edges: EdgeRecord[] = [];
			peers[0].services.pubsub.addEventListener(
				"peer:stream-ready",
				({ detail }: CustomEvent<PublicSignKey>) => {
					if (detail.hashcode() !== hashes[1]) return;
					const session = logA._peerSessions.current(hashes[1]);
					edges.push({
						session,
						phase: session?.phase,
						active: session?.isActive() ?? false,
						committed: session
							? logA._v2Receive._cutoverPeerSessions.has(session)
							: false,
					});
				},
				{ signal: listeners.signal },
			);
			// A's edge for B comes after A opened its renewed session and before
			// that session's Full commits, never under the previous session.
			const pubsubA = peers[0].services.pubsub as any;
			const dispatch = pubsubA.dispatchEvent.bind(pubsubA);
			let captured: PublicSignKey | undefined;
			let redispatched = false;
			sandbox.stub(pubsubA, "dispatchEvent").callsFake((event: any) => {
				const current = logA._peerSessions.current(hashes[1]);
				if (
					event.type === "peer:stream-ready" &&
					event.detail.hashcode() === hashes[1] &&
					captured === undefined &&
					!redispatched &&
					(current === stale || current?.phase !== "open")
				) {
					captured = event.detail;
					return true;
				}
				return dispatch(event);
			});
			const opened = pDefer<any>();
			releases.push(opened);
			const markOpen = logA._peerSessions.markOpen.bind(logA._peerSessions);
			sandbox
				.stub(logA._peerSessions, "markOpen")
				.callsFake((peerHash: any, expected: any) => {
					const result = markOpen(peerHash, expected);
					if (
						peerHash === hashes[1] &&
						expected !== stale &&
						logA._peerSessions.current(hashes[1]) === expected &&
						expected.phase === "open"
					) {
						opened.resolve(expected);
						if (captured && !redispatched) {
							redispatched = true;
							const detail = captured;
							queueMicrotask(() =>
								dispatch(new CustomEvent("peer:stream-ready", { detail })),
							);
						}
					}
					return result;
				});
			const frontier = () =>
				Object.fromEntries(
					[...logA._repairFrontierByMode].flatMap(
						([mode, targets]: [string, Map<string, Map<string, unknown>>]) => {
							const pending = targets.get(hashes[1]);
							return pending
								? [
										[
											mode,
											{
												y: pending.has(y.hash),
												x: pending.has(x.hash),
												bypass:
													logA._repairFrontierBypassKnownPeersByMode
														.get(mode)
														?.has(hashes[1]) === true,
											},
										],
									]
								: [];
						},
					),
				);
			diagnostics = () => ({
				edges: edges.map((record) => ({
					stale: record.session === stale,
					phase: record.phase,
					active: record.active,
					committed: record.committed,
				})),
				captured: captured !== undefined,
				redispatched,
				backfillReleased: !holdBackfill,
				rangeChanges,
				passes: passes.callCount,
				wakes: wakes.callCount,
				frontier: frontier(),
				yKnown: logA.isEntryKnownByPeer(y.hash, hashes[1]),
			});

			phase = "B dials back";
			let reconnectAt = 0;
			await waitForResolved(
				() => {
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
				{ timeout: 5_000 },
			);
			await peers[1].dial(peers[0].getMultiaddrs());
			const session = await within(
				opened.promise,
				5_000,
				"A opens a renewed session for B",
			);
			expect(session.hasPredecessor).to.equal(true);
			expect(
				(await within(
					committed.promise,
					10_000,
					"B's renewed Full commits",
				)) === session,
				"B's Full commits on the session A opened",
			).to.equal(true);
			expect(rangeChanges, "B's renewed Full changes nothing").to.deep.equal(
				[],
			);
			await within(
				frontierHoldsY.promise,
				5_000,
				"forcing device: the renewed session's repair never queued Y",
			);

			phase = "deliver within five seconds of the reconnect";
			await waitForResolved(
				async () => {
					await expectHeldLocal(stores[1], y, yValue, "the restarted B");
					await expectHeldLocal(stores[1], x, xValue, "the restarted B");
					expect(performance.now() - reconnectAt).to.be.lessThan(5_000);
				},
				{
					timeout: Math.max(1, 5_000 - (performance.now() - reconnectAt)),
				},
			);

			phase =
				"A's edge saw its renewed session before the Full and no pass ran";
			expect(
				edges.some(
					(record) =>
						record.session === session &&
						record.phase === "open" &&
						record.active === true &&
						record.committed === false,
				),
				"A records an edge under its open, uncommitted renewed session",
			).to.equal(true);
			expect(
				edges.some((record) => record.session === stale),
				"no edge under B's previous session",
			).to.equal(false);
			expect(wakes.callCount, "wakes").to.equal(0);
			expect(passes.callCount, "passes").to.equal(0);
		} catch (error) {
			if (error instanceof Error) error.message = phase + ": " + error.message;
			testError = error;
			failed = true;
			try {
				console.error(
					"Unobserved restart reconnect failure:",
					JSON.stringify({ phase, ...diagnostics?.() }),
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
				const cleanupErrors = stopped.flatMap((result) =>
					result.status === "rejected" ? [result.reason] : [],
				);
				if (cleanupErrors.length) {
					testError = new AggregateError(
						failed ? [testError, ...cleanupErrors] : cleanupErrors,
						"Unobserved restart fixture cleanup failed",
						{ cause: testError },
					);
					failed = true;
				}
			} finally {
				sandbox.restore();
			}
		}
		if (failed) throw testError;
	});
});
