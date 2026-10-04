import { deserialize } from "@dao-xyz/borsh";
import { DecryptedThing, toBase64 } from "@peerbit/crypto";
import { PubSubData } from "@peerbit/pubsub-interface";
import { RPCMessage, RequestV0 } from "@peerbit/rpc";
import { AcknowledgeDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { SyncCapabilitiesMessage } from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";
import { FullReplicationInfoV2Message } from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("receive admission replication-info V2 partition recovery", function () {
	this.timeout(30_000);

	for (const MessageType of [
		FullReplicationInfoV2Message,
		SyncCapabilitiesMessage,
	]) {
		it(`retires a pending ${MessageType.name} ACK on real disconnect and recovers after redial`, async () => {
			const peers: Peerbit[] = [];
			const sandbox = sinon.createSandbox();
			let partitioned = false;
			let phase = "create peers and open programs";
			try {
				// Deliberately bypass TestSession's environment-selected transport.
				// Only the partition gate is customized; these are default real peers.
				for (let index = 0; index < 2; index++) {
					peers.push(
						await Peerbit.create({
							libp2p: {
								connectionGater: {
									denyDialPeer: async () => partitioned,
									denyInboundConnection: async () => partitioned,
								},
							},
						}),
					);
				}
				const [sender, receiver] = peers;
				const template = new EventStore<string, any>();
				const args = {
					replicas: { min: 2 },
					replicate: 1,
					timeUntilRoleMaturity: 0,
				};
				const source = await sender.open(template.clone(), { args });
				const target = await receiver.open(template.clone(), { args });
				const sourceLog = source.log as any;
				const targetLog = target.log as any;
				const sourcePubsub = sender.services.pubsub as any;
				const targetPubsub = receiver.services.pubsub as any;
				const senderHash = sender.identity.publicKey.hashcode();
				const receiverHash = receiver.identity.publicKey.hashcode();
				expect(sender.nativeNetwork).to.equal(undefined);
				expect(receiver.nativeNetwork).to.equal(undefined);
				expect(sourcePubsub.routes).to.equal(sender.services.fanout.routes);
				expect(targetPubsub.routes).to.equal(receiver.services.fanout.routes);

				let firstSendSignal: AbortSignal | undefined;
				const send = sourceLog.rpc.send.bind(sourceLog.rpc);
				sandbox
					.stub(sourceLog.rpc, "send")
					.callsFake((message: any, options: any) => {
						if (message instanceof MessageType && !firstSendSignal) {
							firstSendSignal = options?.signal;
						}
						return send(message, options);
					});
				let dropped: { id: string; mode: unknown } | undefined;
				let drop = true;
				const onDataMessage = targetPubsub._onDataMessage.bind(targetPubsub);
				sandbox
					.stub(targetPubsub, "_onDataMessage")
					.callsFake((...parameters: any[]) => {
						const [from, , , envelope] = parameters;
						if (
							drop &&
							from.equals(sender.identity.publicKey) &&
							envelope.data
						) {
							// Loss injection before ACK and application delivery. Decoding
							// identifies only the selected frame; it is not an auth bypass.
							let selected = false;
							try {
								const data = PubSubData.from(envelope.data);
								if (data.topics.includes(sourceLog.topic)) {
									const rpc = deserialize(data.data, RPCMessage);
									selected =
										rpc instanceof RequestV0 &&
										rpc.request instanceof DecryptedThing &&
										rpc.request.getValue(TransportMessage) instanceof
											MessageType;
								}
							} catch {
								// Other pubsub control frames do not carry this RPC envelope.
							}
							if (selected) {
								dropped ??= {
									id: toBase64(envelope.id),
									mode: envelope.header.mode,
								};
								return Promise.resolve();
							}
						}
						return onDataMessage(...parameters);
					});
				const unsubscribed = [false, false];
				for (const [index, peer] of peers.entries()) {
					const remoteHash = index === 0 ? receiverHash : senderHash;
					peer.services.pubsub.addEventListener("unsubscribe", ({ detail }) => {
						if (
							detail.from.hashcode() === remoteHash &&
							detail.topics.includes(sourceLog.topic) &&
							detail.reason === "peer-unreachable"
						) {
							unsubscribed[index] = true;
						}
					});
				}
				phase = "initial dial";
				await sender.dial(receiver.getMultiaddrs());
				phase = "observe pending ACK";
				await waitForResolved(
					() => {
						expect(dropped).to.exist;
						expect(dropped!.mode).to.be.instanceOf(AcknowledgeDelivery);
						expect(sourcePubsub._ackCallbacks.has(dropped!.id)).to.equal(true);
						expect(firstSendSignal?.aborted).to.equal(false);
						expect(
							sourcePubsub
								.getSubscribers(sourceLog.topic)
								?.map((key: any) => key.hashcode()),
						).to.include(receiverHash);
						expect(
							targetPubsub
								.getSubscribers(targetLog.topic)
								?.map((key: any) => key.hashcode()),
						).to.include(senderHash);
					},
					{ timeout: 5_000 },
				);
				const oldSession = sourceLog._peerSessions.current(receiverHash);
				expect(oldSession?.phase).to.equal("open");
				phase = "disconnect";
				unsubscribed.fill(false);
				partitioned = true;
				await Promise.all([
					sender.hangUp(receiver.identity.publicKey),
					receiver.hangUp(sender.identity.publicKey),
				]);
				phase = "observe unsubscribe and ACK cancellation";
				await waitForResolved(
					() => {
						expect(
							sender.libp2p.getConnections(receiver.libp2p.peerId),
						).to.have.length(0);
						expect(
							receiver.libp2p.getConnections(sender.libp2p.peerId),
						).to.have.length(0);
						expect(unsubscribed).to.deep.equal([true, true]);
						expect(sourceLog._peerSessions.current(receiverHash)).not.to.equal(
							oldSession,
						);
						expect(firstSendSignal!.aborted).to.equal(true);
						expect(sourcePubsub._ackCallbacks.has(dropped!.id)).to.equal(false);
					},
					{ timeout: 2_000 },
				);

				// No synthetic subscription or coordinator reset: recovery must come
				// from the actual renewed protocol streams and signed subscription.
				drop = false;
				partitioned = false;
				phase = "drain partition-era dial jobs";
				// libp2p coalesces dials by peer, even when an existing gated job
				// has rejected but has not left the queue yet. Drain that lifetime
				// before the single heal dial; never retry a rejected heal attempt.
				await waitForResolved(
					() => {
						for (const [peer, remote] of [
							[sender, receiver],
							[receiver, sender],
						]) {
							expect(
								peer.libp2p
									.getDialQueue()
									.filter((dial) => dial.peerId?.equals(remote.libp2p.peerId)),
							).to.have.length(0);
						}
					},
					{ timeout: 5_000 },
				);
				phase = "heal dial";
				await sender.dial(receiver.getMultiaddrs());
				phase = "renewed handshake";
				await waitForResolved(
					async () => {
						for (const [log, remoteHash] of [
							[sourceLog, receiverHash],
							[targetLog, senderHash],
						] as const) {
							expect(
								log._v2Receive._receiveStates.get(remoteHash)?.phase,
							).to.equal("active");
							expect(
								log._v2Send._sendStates.get(remoteHash)?.established,
							).to.equal(true);
							expect([...(await log.getReplicators())]).to.have.members([
								senderHash,
								receiverHash,
							]);
						}
					},
					{ timeout: 5_000 },
				);
				phase = "replicate after heal";
				const { entry } = await source.add("after actual partition and redial");
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
				partitioned = false;
				sandbox.restore();
				await Promise.all(peers.map((peer) => peer.stop()));
			}
		});
	}
});
