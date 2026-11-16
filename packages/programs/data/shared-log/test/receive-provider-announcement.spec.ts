import { toId } from "@peerbit/indexer-interface";
import { fanoutWire } from "@peerbit/pubsub";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import {
	RawEntryWithRefs,
	RawExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { EventStore } from "./utils/stores/index.js";

describe("receive admission provider announcement boundary", () => {
	it("admits a default prepared receive through a ready tracker without dialing a stopped bootstrap", async () => {
		const peers: Peerbit[] = [];
		const dialStarted = pDefer<void>();
		const dialPending = pDefer<never>();
		void dialPending.promise.catch(() => {});
		let receiving: Promise<void> | undefined;
		let restoreDial: (() => void) | undefined;
		let preparedJoinStarted = false;
		let preparedJoinFinished = false;
		try {
			peers.push(
				await Peerbit.create(),
				await Peerbit.create(),
				await Peerbit.create(),
			);
			const [sourcePeer, targetPeer, trackerPeer] = peers;
			const template = new EventStore<string, any>();
			const source = await sourcePeer.open(template.clone(), {
				args: { replicate: false },
			});
			const target = await targetPeer.open(template.clone(), {
				args: {
					replicate: 1,
					timeUntilRoleMaturity: 0,
					sync: {
						profile: (event) => {
							if (event.name === "log.joinPreparedFacts.prepare") {
								preparedJoinStarted = true;
							}
							if (event.name === "sharedLog.receive.lowerLogJoin") {
								preparedJoinFinished =
									event.details?.joinedPreparedFacts === true;
							}
						},
					},
				},
			});
			expect((target.log as any)._nativeBackbone).to.equal(undefined);
			const { entry } = await source.add("ready-provider-admission", {
				meta: { next: [] },
			});
			const bytes = await source.log.log.blocks.get(entry.hash, {
				remote: false,
			});
			expect(bytes).to.exist;
			const stoppedAddress = sourcePeer.libp2p.getMultiaddrs()[0]!;
			await sourcePeer.stop();
			await targetPeer.dial(trackerPeer);
			const fanout = targetPeer.services.fanout;
			const trackerAddress = trackerPeer.libp2p.getMultiaddrs()[0]!;
			await waitForResolved(() => {
				expect(
					(fanout as any).connectedPeerHashForBootstrap(trackerAddress),
				).to.equal(trackerPeer.services.fanout.publicKeyHash);
			});
			fanout.setBootstraps([stoppedAddress, trackerAddress]);
			const manager = (fanout as any).components.connectionManager;
			const originalDial = manager.openConnection;
			manager.openConnection = (address: any, options: any) => {
				if (address.toString() === stoppedAddress.toString()) {
					dialStarted.resolve();
					return dialPending.promise;
				}
				return originalDial.call(manager, address, options);
			};
			restoreDial = () => {
				manager.openConnection = originalDial;
			};
			receiving = target.log.onMessage(
				new RawExchangeHeadsMessage({
					heads: [
						new RawEntryWithRefs({
							hash: entry.hash,
							bytes: bytes!,
							gidRefrences: [],
						}),
					],
				}),
				{ from: sourcePeer.identity.publicKey } as any,
			);
			// No timer race: the dial is released only in cleanup. Local admission
			// must finish while the unavailable tracker remains blocked.
			expect(
				await Promise.race([
					receiving.then(() => "admitted"),
					dialStarted.promise.then(() => "dialed"),
				]),
			).to.equal("admitted");
			expect(preparedJoinStarted).to.equal(true);
			expect(preparedJoinFinished).to.equal(true);
			expect(
				(await target.log.log.entryIndex.getShallow(entry.hash))?.value.head,
			).to.equal(true);
			expect(await target.log.entryCoordinatesIndex.get(toId(entry.hash))).to
				.exist;
			await waitForResolved(async () => {
				expect(
					await trackerPeer.services.fanout.queryProviders(`cid:${entry.hash}`),
				).to.include(fanout.publicKeyHash);
			});
		} finally {
			dialPending.reject(new Error("test dial cleanup"));
			restoreDial?.();
			try {
				await receiving;
			} finally {
				await Promise.all(peers.map((peer) => peer.stop()));
			}
		}
	});
});

describe("append provider announcement boundary", () => {
	it("resolves an append while its provider bootstrap dial never answers", async () => {
		let peer: Peerbit | undefined;
		const dialStarted = pDefer<void>();
		const pendingDials = new Set<(error: Error) => void>();
		const dialSignals: Array<AbortSignal | undefined> = [];
		let restoreDial: (() => void) | undefined;
		let appending: Promise<unknown> | undefined;
		try {
			peer = await Peerbit.create();
			const store = await peer.open(new EventStore<string, any>());
			const fanout = peer.services.fanout;
			const manager = (fanout as any).components.connectionManager;
			const originalDial = manager.openConnection;
			// The only bootstrap never answers: its dial ends only when aborted.
			manager.openConnection = (
				_address: unknown,
				options: { signal?: AbortSignal } = {},
			) => {
				dialSignals.push(options.signal);
				dialStarted.resolve();
				return new Promise<never>((_resolve, reject) => {
					const finish = (error: Error) => {
						options.signal?.removeEventListener("abort", onAbort);
						pendingDials.delete(finish);
						reject(error);
					};
					const onAbort = () => finish(options.signal!.reason);
					pendingDials.add(finish);
					if (options.signal?.aborted) onAbort();
					else
						options.signal?.addEventListener("abort", onAbort, { once: true });
				});
			};
			restoreDial = () => {
				manager.openConnection = originalDial;
			};
			fanout.setBootstraps([peer.libp2p.getMultiaddrs()[0]!]);

			// Record when each block write of the log settles.
			const remoteBlocks = (store.log as any).remoteBlocks;
			const writes: Array<{ settled: boolean }> = [];
			for (const method of ["put", "putKnown", "putMany", "putKnownMany"]) {
				const write = sinon.stub(remoteBlocks, method);
				const original = write.wrappedMethod as (...args: unknown[]) => unknown;
				write.callsFake(function (this: unknown, ...args: unknown[]) {
					const result = original.apply(this, args);
					const state = { settled: false };
					writes.push(state);
					const settle = () => {
						state.settled = true;
					};
					void Promise.resolve(result).then(settle, settle);
					return result;
				});
			}
			const announce = sinon.spy(fanout, "announceProviders");

			appending = store.add("unanswered-provider", { meta: { next: [] } });
			await dialStarted.promise;
			// The write still had microtasks to run; the dial is pending until its
			// budget or an abort ends it.
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(writes).to.have.length(1);
			expect(
				writes[0]!.settled,
				"the log's block write resolved while its announcement dials",
			).to.equal(true);

			const { entry } = (await appending) as { entry: { hash: string } };
			expect(await store.log.log.has(entry.hash)).to.equal(true);
			expect(announce.called).to.equal(true);
			const signal = announce.firstCall.args[1]?.signal;
			expect(signal, "the announcement carries the log's close signal").to
				.exist;

			// Closing aborts the announcement instead of waiting for its dial.
			await peer.stop();
			peer = undefined;
			expect(signal!.aborted).to.equal(true);
			expect(dialSignals.length).to.be.greaterThan(0);
			expect(dialSignals.every((dialSignal) => dialSignal?.aborted)).to.equal(
				true,
			);
			expect(pendingDials.size).to.equal(0);
		} finally {
			for (const reject of pendingDials) reject(new Error("test dial cleanup"));
			restoreDial?.();
			sinon.restore();
			try {
				await appending;
			} finally {
				await peer?.stop();
			}
		}
	});

	it("closes while an append's provider announcement waits in its control send", async () => {
		const peers: Peerbit[] = [];
		const releaseSends = pDefer<void>();
		let appending: Promise<unknown> | undefined;
		try {
			peers.push(await Peerbit.create(), await Peerbit.create());
			const [tracker, writer] = peers;
			await writer.bootstrap(tracker.getMultiaddrs());
			// The tracker is then a ready bootstrap peer, so the append's
			// announcement goes straight to its control send rather than through
			// a bounded bootstrap dial that may give up on a loaded machine.
			await writer.services.fanout.waitFor(
				tracker.services.fanout.publicKeyHash,
				{ target: "neighbor" },
			);
			const store = await writer.open(new EventStore<string, any>(), {
				args: { replicate: false },
			});
			const remoteBlocks = (store.log as any).remoteBlocks;
			// Let announcements of blocks stored while opening settle, so the
			// only announcement below is the append's.
			await writer.services.blocks.waitForStoredNotifications();
			await remoteBlocks.waitForStoredNotifications();

			const fanout = writer.services.fanout;
			const sendStarted = pDefer<AbortSignal | undefined>();
			const publish = sinon.stub(fanout, "publishMessageMaybe");
			publish.callsFake(function (this: unknown, ...args) {
				const [, message, , , signal] = args;
				if (
					(message as { data?: Uint8Array }).data?.[0] !==
					fanoutWire.MSG_PROVIDER_ANNOUNCE
				) {
					return publish.wrappedMethod.apply(this, args);
				}
				sendStarted.resolve(signal);
				// Like a write that waits for queue capacity: it ends only when its
				// signal aborts (or when the test cleans up).
				return new Promise<boolean>((resolve) => {
					signal?.addEventListener("abort", () => resolve(false), {
						once: true,
					});
					void releaseSends.promise.then(() => resolve(false));
				});
			});

			appending = store.add("held-control-send", { meta: { next: [] } });
			const signal = await sendStarted.promise;
			expect(signal, "the control send carries the log's close signal").to
				.exist;
			expect(signal!.aborted).to.equal(false);
			const { entry } = (await appending) as { entry: { hash: string } };
			expect(await store.log.log.has(entry.hash)).to.equal(true);

			// Closing aborts the held send instead of waiting for it.
			await store.close();
			expect(signal!.aborted).to.equal(true);
			expect(remoteBlocks.getStoredNotificationTelemetry()).to.deep.include({
				pending: 0,
				running: 0,
			});
		} finally {
			releaseSends.resolve();
			sinon.restore();
			try {
				await appending;
			} finally {
				await Promise.all(peers.map((peer) => peer.stop()));
			}
		}
	});
});
