import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { EventStore } from "./utils/stores/index.js";

describe("receive admission three-peer held entry recovery", function () {
	this.timeout(60_000);

	for (const nativeStorage of [false, true]) {
		it(`recovers C's entry in B | A+C then C | A+B (${nativeStorage ? "native" : "default"} storage)`, async () => {
			const peers: Peerbit[] = [];
			const isolated = new Set<number>();
			const listeners = new AbortController();
			const transitions: object[] = [];
			const deniedDials: object[] = [];
			let phase = "open three full replicas";
			let failure: unknown;
			let failed = false;
			try {
				for (let index = 0; index < 3; index++) {
					peers.push(
						await Peerbit.create({
							...(nativeStorage
								? createRustPeerbitOptions({ network: false })
								: {}),
							libp2p: {
								connectionGater: {
									denyDialPeer: (peerId) => {
										const denied =
											isolated.has(index) ||
											peers.some(
												(peer, other) =>
													isolated.has(other) &&
													peer.libp2p.peerId.equals(peerId),
											);
										if (denied) {
											deniedDials.push({
												phase,
												index,
												remote: peers.findIndex((peer) =>
													peer.libp2p.peerId.equals(peerId),
												),
												isolated: [...isolated],
											});
											if (deniedDials.length > 12) deniedDials.shift();
										}
										return denied;
									},
									denyInboundConnection: () => isolated.has(index),
								},
							},
						}),
					);
				}
				const [a, b, c] = peers;
				const hashes = peers.map((peer) => peer.identity.publicKey.hashcode());
				const template = new EventStore<string, any>();
				const stores = await Promise.all(
					peers.map((peer) =>
						peer.open(template.clone(), {
							args: {
								replicate: 1,
								replicas: { min: 3 },
								timeUntilRoleMaturity: 0,
								...(nativeStorage
									? { nativeGraph: true, nativeBackbone: { optional: false } }
									: {}),
							},
						}),
					),
				);
				const logs = stores.map((store) => store.log as any);
				// Passive diagnostics only: no close, notice, liveness or recovery path
				// is stubbed. Membership-driven repair is legitimate in this topology
				// test; the separate settled-session fixture excludes that alternative.
				for (const [index, store] of stores.entries()) {
					for (const name of ["replicator:join", "replicator:leave"] as const)
						store.log.events.addEventListener(
							name,
							(event) => {
								if (transitions.length < 24)
									transitions.push({
										phase,
										index,
										name,
										remote: hashes.indexOf(event.detail.publicKey.hashcode()),
									});
							},
							{ signal: listeners.signal },
						);
				}
				await a.dial(b.getMultiaddrs());
				await a.dial(c.getMultiaddrs());
				await b.dial(c.getMultiaddrs());
				await Promise.all(
					stores.flatMap((store, index) =>
						peers.flatMap((peer, other) =>
							index === other
								? []
								: [
										store.log.waitForReplicator(peer.identity.publicKey, {
											roleAge: 0,
											timeout: 10_000,
										}),
									],
						),
					),
				);
				for (const store of stores)
					expect([...(await store.log.getReplicators())].sort()).to.deep.equal(
						[...hashes].sort(),
					);
				await waitForResolved(
					() => {
						for (const [index, log] of logs.entries()) {
							for (let other = 0; other < 3; other++) {
								if (index === other) continue;
								expect(
									log._peerSessions.current(hashes[other])?.phase,
								).to.equal("open");
								expect(
									log._v2Send._sendStates.get(hashes[other])?.established,
								).to.equal(true);
								expect(
									log._v2Receive._receiveStates.get(hashes[other])?.phase,
								).to.equal("active");
							}
							expect(
								log._joinAuthoritativeRepairTimersByDelay.has(30_000),
							).to.equal(true);
							if (nativeStorage) {
								expect(log._nativeBackbone).to.exist;
								expect(
									log._coordinates.canUseNativeBackboneResidentCoordinateState(),
								).to.equal(true);
							}
						}
					},
					{ timeout: 10_000 },
				);
				phase = "drain actual startup membership repair windows";
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
				const disconnect = (left: number, right: number) =>
					Promise.all([
						peers[left].hangUp(peers[right].identity.publicKey),
						peers[right].hangUp(peers[left].identity.publicKey),
					]);
				const pairStreams = [a, b].flatMap((peer, index) =>
					[
						peer.services.pubsub,
						peer.services.fanout,
						peer.services.blocks,
					].map((service) => ({
						service,
						remote: hashes[1 - index],
						old: service.peers.get(hashes[1 - index]),
					})),
				);
				phase = "partition B from A+C";
				isolated.add(1);
				await Promise.all([disconnect(0, 1), disconnect(1, 2)]);
				await waitForResolved(() =>
					expect(b.libp2p.getConnections()).to.have.length(0),
				);
				const value =
					"C's signed independent head, relayed by A after partition";
				const { entry } = await stores[2].add(value, { meta: { next: [] } });
				const exactLocalEntry = async (index: number) => {
					const log = stores[index].log.log;
					const local = await log.get(entry.hash, { remote: false });
					expect(local, `entry must be held locally by peer ${index}`).to.exist;
					expect((await local!.getPayloadValue()).value).to.equal(value);
					expect(await local!.verifySignatures()).to.equal(true);
					expect(local!.meta.next).to.deep.equal([]);
					expect(
						(await log.entryIndex.getShallow(entry.hash))?.value.head,
					).to.equal(true);
					const coordinate = await logs[
						index
					]._coordinates.getAuthoritativeCoordinateEntryForInventory(
						entry.hash,
					);
					expect(coordinate?.hash).to.equal(entry.hash);
					expect(coordinate!.coordinates.length).to.be.greaterThan(0);
				};
				await waitForResolved(() => exactLocalEntry(0), { timeout: 2_000 });
				expect(await stores[1].log.log.has(entry.hash)).to.equal(false);
				phase = "partition C before reconnecting A+B";
				isolated.add(2);
				await disconnect(0, 2);
				await waitForResolved(() =>
					expect(c.libp2p.getConnections()).to.have.length(0),
				);
				let writableAt: number | undefined;
				const observeWritable = () => {
					if (
						writableAt === undefined &&
						pairStreams.every(({ service, remote, old }) => {
							const current = service.peers.get(remote);
							return current && current !== old && current.isWritable;
						})
					)
						writableAt = performance.now();
				};
				for (const { service } of pairStreams)
					(service as EventTarget).addEventListener(
						"stream:outbound",
						observeWritable,
						{
							signal: listeners.signal,
						},
					);
				phase = "drain denied A-B dials before healing the partition";
				let reconnectAt = 0;
				await waitForResolved(
					() => {
						// libp2p joins an existing queued dial before rechecking its gater.
						// Let attempts denied during the partition retire naturally, then
						// unblock in this same turn so the one explicit dial cannot inherit
						// an earlier denial. Nothing cancels, clears or retries a dial.
						for (const [index, peer] of [a, b].entries())
							expect(
								peer.libp2p
									.getDialQueue()
									.filter((dial) =>
										dial.peerId?.equals(peers[1 - index].libp2p.peerId),
									),
							).to.have.length(0);
						reconnectAt = performance.now();
						isolated.delete(1);
					},
					{ timeout: 2_000 },
				);
				phase = "redial A+B while C remains isolated";
				await a.dial(b.getMultiaddrs());
				await waitForResolved(() => expect(writableAt).to.be.a("number"), {
					timeout: Math.max(1, 5_000 - (performance.now() - reconnectAt)),
				});
				phase =
					"exact local B delivery within five seconds of reconnecting A+B";
				await waitForResolved(
					async () => {
						await exactLocalEntry(1);
						expect(performance.now() - reconnectAt).to.be.lessThan(5_000);
					},
					{ timeout: Math.max(1, 5_000 - (performance.now() - reconnectAt)) },
				);
				await exactLocalEntry(0);
				expect(c.libp2p.getConnections()).to.have.length(0);
				for (const peer of [a, b])
					expect(peer.libp2p.getConnections(c.libp2p.peerId)).to.have.length(0);
			} catch (error) {
				failed = true;
				failure = error;
				console.error(
					"Three-peer partition failure:",
					JSON.stringify({
						phase,
						nativeStorage,
						transitions,
						deniedDials,
					}),
				);
			} finally {
				listeners.abort();
				const stopped = await Promise.allSettled(
					peers.map((peer) => peer.stop()),
				);
				const errors = stopped.flatMap((result) =>
					result.status === "rejected" ? [result.reason] : [],
				);
				if (errors.length) {
					failure = new AggregateError(
						failed ? [failure, ...errors] : errors,
						"Three-peer cleanup failed",
					);
					failed = true;
				}
			}
			if (failed) throw failure;
		});
	}
});
