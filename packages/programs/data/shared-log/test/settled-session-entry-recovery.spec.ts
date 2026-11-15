import { serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Log } from "@peerbit/log";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import {
	ExchangeHeadsMessage,
	RawExchangeHeadsMessage,
	getExchangeHeadHash,
} from "../src/exchange-heads.js";
import {
	AbsoluteReplicas,
	FullReplicationInfoV2Message,
	encodeReplicas,
} from "../src/replication.js";
import { JSON_ENCODING } from "./utils/stores/encoding.js";
import { EventStore } from "./utils/stores/index.js";

describe("receive admission settled-session offline-author entry recovery", function () {
	// Drain the real 30-second membership repair window. Actual recovery still
	// has five seconds from writable replacement, within the unchanged 60s test.
	this.timeout(60_000);
	for (const nativeStorage of [false, true]) {
		for (const syntheticRepair of [false, true]) {
			it(
				(syntheticRepair
					? "positive control: exact offline-third-author backfill crosses the replacement transport"
					: "reoffers an imported offline-third-author head without a new topic session") +
					(nativeStorage ? " (native storage)" : ""),
				async () => {
					const peers: Peerbit[] = [];
					const authorBlocks = new AnyBlockStore();
					const authorLog = new Log<any>();
					const sandbox = sinon.createSandbox();
					const retirement = pDefer<void>();
					const listeners = new AbortController();
					const tasks: Promise<unknown>[] = [];
					const failures: unknown[] = [];
					let partitioned = false;
					let failed = false;
					let testError: unknown;
					let phase = "create offline author and runtime peers";
					try {
						// C is a real independent signer, never a transport member. Close its
						// normal Log before importing its entry through public SharedLog.join.
						await authorBlocks.start();
						const author = await Ed25519Keypair.create();
						await authorLog.open(authorBlocks, author, {
							encoding: JSON_ENCODING,
						});
						const value =
							"offline-third-author head held across settled reconnect";
						const { entry } = await authorLog.append(
							{ op: "ADD", value },
							{
								meta: {
									next: [],
									data: encodeReplicas(new AbsoluteReplicas(2)),
								},
							},
						);
						const signedBytes = serialize(entry);
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
						const [holderPeer, missingPeer] = peers;
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
						const stores = await Promise.all(
							peers.map((peer) => peer.open(template.clone(), { args })),
						);
						const [holder, missing] = stores;
						const logs = stores.map((store) => store.log as any);
						if (nativeStorage)
							for (const log of logs) {
								expect(log._nativeBackbone, "required native backbone").to
									.exist;
								expect(log.log.entryIndex.properties.nativeGraph).to.exist;
								expect(
									log._coordinates.canUseNativeBackboneResidentCoordinateState(),
								).to.equal(true);
							}
						let measuring = false;
						const activeChanges = [0, 0];
						const armed = new Set<number>();
						const membership: string[] = [];
						const fullMessages: string[] = [];
						const scheduledRepairs: number[] = [];
						const queued: { target: string; hash: string }[] = [];
						const offered: { target: string; hashes: string[] }[] = [];
						const received: { from: string; hashes: string[] }[] = [];
						const headHashes = (message: unknown) =>
							message instanceof RawExchangeHeadsMessage
								? message.heads.map((head) => head.hash)
								: message instanceof ExchangeHeadsMessage
									? message.heads.map(getExchangeHeadHash)
									: [];
						const queue = logs[0].queueAppendBackfill.bind(logs[0]);
						sandbox
							.stub(logs[0], "queueAppendBackfill")
							.callsFake((...parameters) => {
								if (measuring)
									queued.push({
										target: parameters[0] as string,
										hash: (parameters[1] as any).hash,
									});
								return queue(...parameters);
							});
						const receive = missing.log.onMessage.bind(missing.log);
						sandbox
							.stub(missing.log, "onMessage")
							.callsFake((message, context) => {
								const hashes = headHashes(message);
								if (measuring && context.from && hashes.length)
									received.push({ from: context.from.hashcode(), hashes });
								return receive(message, context);
							});
						for (const [index, log] of logs.entries()) {
							peers[index].services.pubsub.addEventListener(
								"unsubscribe",
								({ detail }) => {
									if (measuring && detail.topics.includes(log.topic))
										membership.push(index + ":unsubscribe:" + detail.reason);
								},
								{ signal: listeners.signal },
							);
							for (const name of [
								"replication:change",
								"replicator:join",
								"replicator:leave",
							] as const)
								stores[index].log.events.addEventListener(
									name,
									() => {
										if (measuring) membership.push(index + ":" + name);
									},
									{ signal: listeners.signal },
								);
							const change = log.onReplicationChange.bind(log);
							sandbox
								.stub(log, "onReplicationChange")
								.callsFake((...parameters) => {
									activeChanges[index]++;
									if (measuring) membership.push(index + ":range-pass");
									const task = change(...parameters) as Promise<void>;
									tasks.push(
										task.then(
											() => {
												activeChanges[index]--;
											},
											(error) => {
												activeChanges[index]--;
												failures.push(error);
											},
										),
									);
									return task;
								});
							const schedule = log.scheduleJoinAuthoritativeRepair.bind(log);
							sandbox
								.stub(log, "scheduleJoinAuthoritativeRepair")
								.callsFake((...parameters) => {
									if (measuring) scheduledRepairs.push(index);
									const result = schedule(...parameters);
									if (log._joinAuthoritativeRepairTimersByDelay.has(30_000))
										armed.add(index);
									return result;
								});
							const send = log.rpc.send.bind(log.rpc);
							sandbox
								.stub(log.rpc, "send")
								.callsFake((message: any, options: any) => {
									if (
										measuring &&
										message instanceof FullReplicationInfoV2Message
									)
										fullMessages.push(index + ":" + message.sequence);
									const hashes = headHashes(message);
									if (measuring && index === 0 && hashes.length)
										for (const target of options?.mode?.to ?? [])
											offered.push({ target, hashes });
									return send(message, options);
								});
						}
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
							const receiveEpoch = log._peerSessions.receiveEpoch(other);
							expect(receive.receiveEpoch).to.equal(receiveEpoch);
							return {
								session,
								send,
								receive,
								binding: {
									receiveEpoch,
									appliedRevision: send.appliedRevision,
									nextSequence: send.nextSequence,
									receiveVersion: receive.version,
									lastSequence: receive.lastSequence,
								},
							};
						};
						const noRepair = (index: number) => {
							const log = logs[index];
							expect(activeChanges[index]).to.equal(0);
							expect(log.pendingMaturity.size).to.equal(0);
							expect(log.replicationChangeDebounceFn.size()).to.equal(0);
							for (const map of [
								log._joinAuthoritativeRepairTimersByDelay,
								log._joinAuthoritativeRepairPeersByDelay,
								log._repairSweepPendingModes,
								log._appendBackfillPendingByTarget,
								log._entryInventoryRecovery.active,
								log._entryInventoryRecovery.pending,
								log.joinWarmup._joinWarmupScheduledRetriesByTarget,
							])
								expect(map.size).to.equal(0);
							expect(log._repairSweepRunning).to.equal(false);
							expect(log._entryInventoryRecovery.timer).to.equal(undefined);
							for (const frontier of [
								log._repairFrontierByMode,
								log._repairFrontierActiveTargetsByMode,
							])
								for (const targets of frontier.values())
									expect((targets as Map<unknown, unknown>).size).to.equal(0);
						};
						phase = "connect and settle both full replicas";
						await holderPeer.dial(missingPeer.getMultiaddrs());
						await waitForResolved(
							() => {
								for (let index = 0; index < 2; index++) {
									expect(armed.has(index)).to.equal(true);
									settled(index);
								}
							},
							{ timeout: 10_000 },
						);
						phase = "drain real initial membership repair windows";
						await waitForResolved(
							() => {
								noRepair(0);
								noRepair(1);
								expect(failures).to.deep.equal([]);
							},
							{ timeout: 40_000 },
						);
						if (syntheticRepair)
							// Only the explicit control disables the candidate's recovery wake.
							for (const log of logs)
								sandbox.stub(log._entryInventoryRecovery, "wake");
						const captured = [settled(0), settled(1)];
						const repairLifecycle =
							logs[0]._instanceLifecycle.ownershipLifecycleController;
						const sameSettledPair = () => {
							for (let index = 0; index < 2; index++) {
								const current = settled(index),
									previous = captured[index];
								for (const key of ["session", "send", "receive"] as const)
									expect(current[key]).to.equal(previous[key]);
								expect(current.binding).to.deep.equal(previous.binding);
							}
						};
						const oldStreams = peers.flatMap((peer, index) =>
							[
								peer.services.pubsub,
								peer.services.fanout,
								peer.services.blocks,
							].map((service) => ({
								service,
								remote: hashes[1 - index],
								old: service.peers.get(hashes[1 - index])!,
							})),
						);
						const closed = new Set<object>();
						for (const { old } of oldStreams) {
							expect(old?.isWritable).to.equal(true);
							const close = old.close.bind(old);
							// Actual resources close; only their retirement completion is held.
							sandbox.stub(old, "close").callsFake(() => {
								const task = (async () => {
									await close();
									closed.add(old);
									await retirement.promise;
								})();
								tasks.push(task);
								void task.catch(() => {});
								return task;
							});
						}
						measuring = true;
						phase =
							"physically partition both peers with old retirement pending";
						partitioned = true;
						await Promise.all([
							holderPeer.hangUp(missingPeer.identity.publicKey),
							missingPeer.hangUp(holderPeer.identity.publicKey),
						]);
						await waitForResolved(
							() => {
								for (const peer of peers)
									expect(peer.libp2p.getConnections()).to.have.length(0);
								expect(closed.size).to.equal(oldStreams.length);
								sameSettledPair();
							},
							{ timeout: 2_000 },
						);
						phase = "import the genuine offline author's entry only into A";
						await holder.log.join([entry], { verifySignatures: true });
						const exactEntry = async (store: EventStore<string, any>) => {
							const local = await store.log.log.get(entry.hash, {
								remote: false,
							});
							expect(
								local,
								"missing offline-author entry at " +
									(store === holder ? "A" : "B"),
							).to.exist;
							expect(serialize(local!)).to.deep.equal(signedBytes);
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
							const coordinates = (store.log as any)._coordinates;
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
						const noAlternative = () => {
							expect(membership).to.deep.equal([]);
							expect(fullMessages).to.deep.equal([]);
							expect(scheduledRepairs).to.deep.equal([]);
							expect(failures).to.deep.equal([]);
							sameSettledPair();
							for (const [index, peer] of peers.entries()) {
								expect(peer.services.pubsub.peers.has(authorHash)).to.equal(
									false,
								);
								expect(logs[index]._peerSessions.current(authorHash)).to.equal(
									null,
								);
							}
						};
						await exactEntry(holder);
						expect(await missing.log.log.has(entry.hash)).to.equal(false);
						noRepair(0);
						noRepair(1);
						noAlternative();
						expect(queued, "no pre-existing admission backfill").to.deep.equal(
							[],
						);
						let writableAt: number | undefined;
						const observeWritable = () => {
							if (
								writableAt !== undefined ||
								!oldStreams.every(({ service, remote, old }) => {
									const current = service.peers.get(remote);
									return current && current !== old && current.isWritable;
								})
							)
								return;
							writableAt = performance.now();
							retirement.resolve();
						};
						for (const { service } of oldStreams)
							(service as EventTarget).addEventListener(
								"stream:outbound",
								observeWritable,
								{ signal: listeners.signal },
							);
						phase = "replace writable streams before old retirement";
						partitioned = false;
						await holderPeer.dial(missingPeer.getMultiaddrs());
						await waitForResolved(() => expect(writableAt).to.be.a("number"), {
							timeout: 5_000,
						});
						if (syntheticRepair) {
							const coordinate =
								await logs[0]._coordinates.getAuthoritativeCoordinateEntryForInventory(
									entry.hash,
								);
							expect(coordinate?.hash).to.equal(entry.hash);
							expect(
								logs[0]._instanceLifecycle.ownershipLifecycleController,
							).to.equal(repairLifecycle);
							expect(repairLifecycle.signal.aborted).to.equal(false);
							// Explicit control only: one proven held coordinate via existing
							// backfill, not a production wake or fabricated admission.
							logs[0].queueAppendBackfill(
								hashes[1],
								coordinate,
								repairLifecycle,
							);
						}
						phase =
							"recover within five seconds of actual writable replacement";
						let deliveryError: unknown;
						try {
							await waitForResolved(
								async () => {
									await exactEntry(missing);
									expect(performance.now() - writableAt!).to.be.lessThan(5_000);
								},
								{
									timeout: Math.max(
										1,
										5_000 - (performance.now() - writableAt!),
									),
								},
							);
						} catch (error) {
							deliveryError = error;
						}
						// Validate strict causal controls even on baseline red.
						await exactEntry(holder);
						noAlternative();
						if (deliveryError) throw deliveryError;
						if (syntheticRepair)
							expect(queued).to.deep.equal([
								{ target: hashes[1], hash: entry.hash },
							]);
						expect(
							offered.some(
								(offer) =>
									offer.target === hashes[1] &&
									offer.hashes.includes(entry.hash),
							),
						).to.equal(true);
						expect(
							received.some(
								(message) =>
									message.from === hashes[0] &&
									message.hashes.includes(entry.hash),
							),
						).to.equal(true);
					} catch (error) {
						if (error instanceof Error)
							error.message = phase + ": " + error.message;
						testError = error;
						failed = true;
					} finally {
						retirement.resolve();
						listeners.abort();
						partitioned = false;
						try {
							const stopped = await Promise.allSettled(
								peers.map((peer) => peer.stop()),
							);
							const drained = await Promise.allSettled([
								...tasks,
								authorLog.close(),
								authorBlocks.stop(),
							]);
							const cleanupErrors = [...stopped, ...drained].flatMap(
								(result) =>
									result.status === "rejected" ? [result.reason] : [],
							);
							if (cleanupErrors.length) {
								testError = new AggregateError(
									failed ? [testError, ...cleanupErrors] : cleanupErrors,
									"Settled-session fixture cleanup failed",
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
	}
});
