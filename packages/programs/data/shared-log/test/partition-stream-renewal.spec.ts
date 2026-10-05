import { deserialize } from "@dao-xyz/borsh";
import { DecryptedThing, toBase64 } from "@peerbit/crypto";
import { PubSubData } from "@peerbit/pubsub-interface";
import { RPCMessage, RequestV0 } from "@peerbit/rpc";
import { AcknowledgeDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { SyncCapabilitiesMessage } from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import { FullReplicationInfoV2Message } from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("receive admission replication-info V2 retained-session stream renewal", function () {
	this.timeout(30_000);
	for (const { droppedType, backoff } of [
		FullReplicationInfoV2Message,
		SyncCapabilitiesMessage,
	].flatMap((droppedType) =>
		[false, true].map((backoff) => ({ droppedType, backoff })),
	)) {
		it(`resumes ${backoff ? "armed backoff for" : "a lost"} ${droppedType.name} when writable replacements precede old retirement`, async () => {
			const peers: Peerbit[] = [];
			const sandbox = sinon.createSandbox();
			const retirement = pDefer<void>();
			const writable = pDefer<void>();
			const listeners: Array<() => void> = [];
			let readinessTimer: ReturnType<typeof setTimeout> | undefined;
			let partitioned = false;
			let phase = "open default peers";
			try {
				for (let i = 0; i < 2; i++) {
					peers.push(
						await Peerbit.create({
							libp2p: {
								connectionGater: {
									denyDialPeer: () => partitioned,
									denyInboundConnection: () => partitioned,
								},
							},
						}),
					);
				}
				const [sender, receiver] = peers;
				const template = new EventStore<string, any>();
				const args = {
					replicate: 1,
					replicas: { min: 2 },
					timeUntilRoleMaturity: 0,
				};
				const source = await sender.open(template.clone(), { args });
				const target = await receiver.open(template.clone(), { args });
				const logs = [source.log, target.log] as any[];
				const pubsubs = peers.map((peer) => peer.services.pubsub as any);
				const hashes = peers.map((peer) => peer.identity.publicKey.hashcode());
				let signal: AbortSignal | undefined;
				let failedAttempts = 0;
				const fullAttempts: FullReplicationInfoV2Message[] = [];
				const send = logs[0].rpc.send.bind(logs[0].rpc);
				sandbox
					.stub(logs[0].rpc, "send")
					.callsFake((message: any, options: any) => {
						if (message instanceof droppedType) {
							signal ??= options?.signal;
						}
						if (message instanceof FullReplicationInfoV2Message) {
							fullAttempts.push(message);
						}
						if (
							backoff &&
							message instanceof droppedType &&
							failedAttempts < 4
						) {
							failedAttempts++;
							return Promise.reject(new Error("injected failure before write"));
						}
						return send(message, options);
					});
				let droppedId: string | undefined;
				let drop = !backoff;
				const pendingBackoff = () => {
					if (droppedType === FullReplicationInfoV2Message) {
						const state = logs[0]._v2Send._sendStates.get(hashes[1]);
						expect(state?.retryAttempts).to.be.at.least(4);
						expect(state.retryTimer).to.exist;
						expect(state.worker).to.equal(undefined);
						expect(state.established).to.equal(false);
					} else {
						const state =
							logs[0]._v2Receive._localCapabilityAdvertisementsByPeer.get(
								hashes[1],
							);
						expect(state?.attempts).to.be.at.least(4);
						expect(state.timer).to.exist;
						expect(state.inFlight).to.equal(undefined);
						expect(state.ready).to.equal(false);
						expect(state.acknowledgedReady).to.equal(undefined);
					}
				};
				const onData = pubsubs[1]._onDataMessage.bind(pubsubs[1]);
				sandbox
					.stub(pubsubs[1], "_onDataMessage")
					.callsFake((...parameters: any[]) => {
						const [from, , , envelope] = parameters;
						let selected = false;
						if (
							drop &&
							from.equals(sender.identity.publicKey) &&
							envelope.data
						) {
							try {
								const data = PubSubData.from(envelope.data);
								const rpc =
									data.topics.includes(logs[0].topic) &&
									deserialize(data.data, RPCMessage);
								selected =
									rpc instanceof RequestV0 &&
									rpc.request instanceof DecryptedThing &&
									rpc.request.getValue(TransportMessage) instanceof
										droppedType &&
									envelope.header.mode instanceof AcknowledgeDelivery;
							} catch {
								/* Other control frames do not contain this RPC envelope. */
							}
						}
						if (selected) {
							droppedId ??= toBase64(envelope.id);
							return Promise.resolve(); // Drop before ACK and application delivery.
						}
						return onData(...parameters);
					});
				phase = backoff
					? "initial dial and four failed sends"
					: "initial dial and pending ACK";
				await sender.dial(receiver.getMultiaddrs());
				await waitForResolved(
					() => {
						if (backoff) {
							expect(failedAttempts).to.equal(4);
							pendingBackoff();
						} else {
							expect(droppedId).to.exist;
							expect(pubsubs[0]._ackCallbacks.has(droppedId)).to.equal(true);
						}
						expect(signal?.aborted).to.equal(false);
					},
					{ timeout: backoff ? 10_000 : 5_000 },
				);
				const sessions = logs.map((log, i) =>
					log._peerSessions.current(hashes[1 - i]),
				);
				const sendState = logs[0]._v2Send._sendStates.get(hashes[1]);
				const lastFailedFull = fullAttempts.at(-1);
				const beforeRecoveryAttempts = fullAttempts.length;
				for (const session of sessions) expect(session?.phase).to.equal("open");
				const oldStreams = peers.flatMap((peer, i) =>
					[
						peer.services.pubsub,
						peer.services.fanout,
						peer.services.blocks,
					].map((service) => ({
						service,
						remote: hashes[1 - i],
						old: service.peers.get(hashes[1 - i])!,
					})),
				);
				const closed = new Set<object>();
				for (const { old } of oldStreams) {
					expect(old?.isWritable).to.equal(true);
					const close = old.close.bind(old);
					// Hold the return, not resource close. This forces the already-supported
					// replacement overlap before _removePeer's exact-object ownership check.
					sandbox.stub(old, "close").callsFake(async () => {
						await close();
						closed.add(old);
						await retirement.promise;
					});
				}
				const unsubscribe: unknown[] = [];
				for (const [i, pubsub] of pubsubs.entries()) {
					const listener = ({ detail }: any) => {
						if (
							detail.from.hashcode() === hashes[1 - i] &&
							detail.topics.includes(logs[i].topic)
						)
							unsubscribe.push(detail);
					};
					pubsub.addEventListener("unsubscribe", listener);
					listeners.push(() =>
						pubsub.removeEventListener("unsubscribe", listener),
					);
				}
				let writableAt: number | undefined;
				const observeWritable = () => {
					if (
						writableAt === undefined &&
						oldStreams.every(({ service, remote, old }) => {
							const current = service.peers.get(remote);
							return current && current !== old && current.isWritable;
						})
					) {
						writableAt = performance.now();
						retirement.resolve(); // Release immediately at actual replacement readiness.
						writable.resolve();
					}
				};
				for (const { service } of oldStreams) {
					const events = service as EventTarget;
					events.addEventListener("stream:outbound", observeWritable);
					listeners.push(() =>
						events.removeEventListener("stream:outbound", observeWritable),
					);
				}
				phase = "disconnect with old retirement pending";
				partitioned = true;
				await Promise.all([
					sender.hangUp(receiver.identity.publicKey),
					receiver.hangUp(sender.identity.publicKey),
				]);
				await waitForResolved(
					() => {
						for (const peer of peers)
							expect(peer.libp2p.getConnections()).to.have.length(0);
						expect(closed.size).to.equal(oldStreams.length);
						expect(unsubscribe).to.have.length(0);
						for (const [i, log] of logs.entries())
							expect(log._peerSessions.current(hashes[1 - i])).to.equal(
								sessions[i],
							);
						if (backoff) pendingBackoff();
						else expect(pubsubs[0]._ackCallbacks.has(droppedId)).to.equal(true);
						expect(signal!.aborted).to.equal(false);
					},
					{ timeout: 2_000 },
				);
				phase = "heal dial";
				drop = false;
				partitioned = false;
				await waitForResolved(
					() => {
						for (const peer of peers)
							expect(peer.libp2p.getDialQueue()).to.have.length(0);
					},
					{ timeout: 5_000 },
				);
				await sender.dial(receiver.getMultiaddrs());
				phase = "await replacement outbound readiness";
				await Promise.race([
					writable.promise,
					new Promise<never>((_resolve, reject) => {
						readinessTimer = setTimeout(
							() =>
								reject(
									new Error("replacement outbound readiness did not arrive"),
								),
							5_000,
						);
					}),
				]);
				clearTimeout(readinessTimer);
				expect(writableAt, "replacement outbound readiness event").to.be.a(
					"number",
				);
				phase = "recover Full on the retained topic session";
				await waitForResolved(
					() => {
						expect(unsubscribe).to.have.length(0);
						for (const [i, log] of logs.entries()) {
							expect(log._peerSessions.current(hashes[1 - i])).to.equal(
								sessions[i],
							);
							expect(
								log._v2Receive._receiveStates.get(hashes[1 - i])?.phase,
							).to.equal("active");
							expect(
								log._v2Send._sendStates.get(hashes[1 - i])?.established,
								JSON.stringify(
									log._v2Send.diagnosePeer({ peerHash: hashes[1 - i] }),
								),
							).to.equal(true);
							expect(log.uniqueReplicators.has(hashes[1 - i])).to.equal(true);
						}
						expect(pubsubs[0]._ackCallbacks.has(droppedId)).to.equal(false);
						expect(performance.now() - writableAt!).to.be.lessThan(2_000);
					},
					{ timeout: Math.max(1, 2_000 - (performance.now() - writableAt!)) },
				);
				if (droppedType === FullReplicationInfoV2Message) {
					expect(logs[0]._v2Send._sendStates.get(hashes[1])).to.equal(
						sendState,
					);
					expect(fullAttempts.length).to.be.greaterThan(beforeRecoveryAttempts);
					expect(
						fullAttempts.at(-1)!.sequence > lastFailedFull!.sequence,
					).to.equal(true);
					expect(fullAttempts.at(-1)!.senderEpoch).to.deep.equal(
						lastFailedFull!.senderEpoch,
					);
					expect(fullAttempts.at(-1)!.receiverChallenge).to.deep.equal(
						lastFailedFull!.receiverChallenge,
					);
				}
				phase = "replicate after retained-session recovery";
				const { entry } = await source.add("after writable replacement");
				await waitForResolved(
					async () => {
						expect(await target.log.log.has(entry.hash)).to.equal(true);
					},
					{ timeout: 5_000 },
				);
			} catch (error) {
				if (error instanceof Error)
					error.message = `${phase}: ${error.message}`;
				throw error;
			} finally {
				clearTimeout(readinessTimer);
				retirement.resolve();
				partitioned = false;
				for (const remove of listeners) remove();
				sandbox.restore();
				await Promise.all(peers.map((peer) => peer.stop()));
			}
		});
	}
});
