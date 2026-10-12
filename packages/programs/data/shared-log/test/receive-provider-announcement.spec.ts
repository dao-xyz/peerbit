import { toId } from "@peerbit/indexer-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
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
