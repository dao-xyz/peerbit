import type { PeerId } from "@libp2p/interface";
import { tcp } from "@libp2p/tcp";
import { type PublicSignKey, toBase64 } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	type ACK,
	AcknowledgeDelivery,
	type DataMessage,
} from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	DirectStream,
	type DirectStreamComponents,
	waitForNeighbour,
} from "../src/index.js";

class AlternateRouteStream extends DirectStream {
	constructor(components: DirectStreamComponents, seekTimeout?: number) {
		super(components, ["/alternate-route-disconnect/1.0.0"], {
			canRelayMessage: true,
			connectionManager: false,
			seekTimeout,
		});
	}
}

describe("stream alternate-route disconnect", function () {
	this.timeout(30_000);
	it("keeps a signed ACK-learned relay path after the direct edge closes", async () => {
		let blocked = false;
		const endpoints = new Set<string>();
		const session = await TestSession.disconnected<{
			directstream: AlternateRouteStream;
		}>(3, {
			addresses: { listen: ["/ip4/127.0.0.1/tcp/0"] },
			transports: [tcp()],
			connectionManager: { reconnectRetries: 0 },
			services: {
				directstream: (components) => {
					// TestSession does not forward connectionGater options. These are the
					// real gates used by dialing/upgrading; only the A-B edge is denied.
					const deny = (remote: PeerId) =>
						blocked &&
						endpoints.has(components.peerId.toString()) &&
						endpoints.has(remote.toString());
					Object.assign(components.connectionGater, {
						denyDialPeer: deny,
						denyInboundEncryptedConnection: deny,
						denyOutboundEncryptedConnection: deny,
					});
					return new AlternateRouteStream(components);
				},
			},
		});
		const [aNode, bNode, relayNode] = session.peers;
		const a = aNode!.services.directstream;
		const b = bNode!.services.directstream;
		const relay = relayNode!.services.directstream;
		endpoints.add(aNode!.peerId.toString());
		endpoints.add(bNode!.peerId.toString());
		const received: DataMessage[] = [];
		const loss: string[] = [];
		const receivedAcks: Array<{ via: string; ack: ACK }> = [];
		const onData = ({ detail }: CustomEvent<DataMessage>) => {
			received.push(detail);
		};
		const onLost = ({ detail }: CustomEvent<PublicSignKey>) => {
			loss.push(detail.hashcode());
		};
		const onAck = a.onAck.bind(a);
		const ackObserver = sinon.stub(a, "onAck").callsFake(async (...args) => {
			const result = await onAck(...args);
			if (result !== false)
				receivedAcks.push({ via: args[0].hashcode(), ack: args[3] });
			return result;
		});
		b.addEventListener("data", onData);
		a.addEventListener("peer:unreachable", onLost);
		try {
			await session.connect();
			await Promise.all([
				waitForNeighbour(a, b),
				waitForNeighbour(a, relay),
				waitForNeighbour(b, relay),
			]);
			const warmup = await a.createMessage(new Uint8Array([1]), {
				mode: new AcknowledgeDelivery({ to: [b.publicKeyHash], redundancy: 2 }),
			});
			await a.publishMessage(a.publicKey, warmup);
			await b.publish(new Uint8Array([2]), {
				mode: new AcknowledgeDelivery({ to: [a.publicKeyHash], redundancy: 2 }),
			});
			await waitForResolved(
				() => {
					for (const [subject, target] of [
						[a, b],
						[b, a],
					] as const) {
						expect(
							subject
								.getRouteHints(target.publicKeyHash)
								.map((hint) => hint.nextHop),
						).to.have.members([target.publicKeyHash, relay.publicKeyHash]);
					}
					expect(
						receivedAcks
							.filter(
								({ ack }) =>
									toBase64(ack.messageIdToAcknowledge) === toBase64(warmup.id),
							)
							.map(({ via }) => via),
					).to.have.members([b.publicKeyHash, relay.publicKeyHash]);
					for (const peer of session.peers)
						expect(peer.getDialQueue()).to.have.length(0);
				},
				{ timeout: 5_000 },
			);
			for (const { ack } of receivedAcks.filter(
				({ ack }) =>
					toBase64(ack.messageIdToAcknowledge) === toBase64(warmup.id),
			)) {
				expect(await ack.verify(true)).to.equal(true);
				expect(
					ack.header.signatures!.publicKeys[0]!.equals(b.publicKey),
				).to.equal(true);
			}
			const targetSession = a.routes.findNeighbor(
				a.publicKeyHash,
				b.publicKeyHash,
			)!.remoteSession;
			expect(targetSession).to.equal(Number(b.session));
			expect(a.routes.getSession(b.publicKeyHash)).to.equal(-1);
			loss.length = 0;
			blocked = true;
			await Promise.all([
				aNode!.hangUp(bNode!.peerId),
				bNode!.hangUp(aNode!.peerId),
			]);
			await waitForResolved(
				() => {
					expect(aNode!.getConnections(bNode!.peerId)).to.have.length(0);
					expect(bNode!.getConnections(aNode!.peerId)).to.have.length(0);
					expect(a.peers.has(b.publicKeyHash)).to.equal(false);
					expect(b.peers.has(a.publicKeyHash)).to.equal(false);
					expect(
						a.getRouteHints(b.publicKeyHash).map((hint) => hint.nextHop),
					).to.deep.equal([relay.publicKeyHash]);
				},
				{ timeout: 5_000 },
			);
			// Assert preservation before any new payload could rediscover a lost route.
			expect(a.routes.getSession(b.publicKeyHash)).to.equal(targetSession);
			expect(a.routes.isReachable(a.publicKeyHash, b.publicKeyHash)).to.equal(
				true,
			);
			expect(loss).not.to.include(b.publicKeyHash);
			const payload = new Uint8Array([7, 31, 255, 0]);
			const message = await a.createMessage(payload, {
				mode: new AcknowledgeDelivery({ to: [b.publicKeyHash], redundancy: 1 }),
			});
			await a.publishMessage(a.publicKey, message);
			await waitForResolved(
				() => {
					expect(
						received.filter(
							(value) => toBase64(value.id) === toBase64(message.id),
						),
					).to.have.length(1);
					expect(
						receivedAcks
							.filter(
								({ ack }) =>
									toBase64(ack.messageIdToAcknowledge) === toBase64(message.id),
							)
							.map(({ via }) => via),
					).to.deep.equal([relay.publicKeyHash]);
				},
				{ timeout: 5_000 },
			);
			const delivered = received.find(
				(value) => toBase64(value.id) === toBase64(message.id),
			)!;
			expect(new Uint8Array(delivered.data!)).to.deep.equal(payload);
			expect(await delivered.verify(true)).to.equal(true);
			expect(
				delivered.header.signatures!.publicKeys[0]!.equals(a.publicKey),
			).to.equal(true);
			const receipt = receivedAcks.find(
				({ ack }) =>
					toBase64(ack.messageIdToAcknowledge) === toBase64(message.id),
			)!.ack;
			expect(await receipt.verify(true)).to.equal(true);
			expect(
				receipt.header.signatures!.publicKeys[0]!.equals(b.publicKey),
			).to.equal(true);
			expect(aNode!.getConnections(bNode!.peerId)).to.have.length(0);
			expect(bNode!.getConnections(aNode!.peerId)).to.have.length(0);
			expect(aNode!.getConnections(relayNode!.peerId)).not.to.have.length(0);
			expect(bNode!.getConnections(relayNode!.peerId)).not.to.have.length(0);
			expect(loss).not.to.include(b.publicKeyHash);
		} finally {
			ackObserver.restore();
			b.removeEventListener("data", onData);
			a.removeEventListener("peer:unreachable", onLost);
			await session.stop();
		}
	});

	it("tells dependents once a retained relay path fails its probe after the peer leaves", async () => {
		const seekTimeout = 1_000;
		const blocked = new Set<string>();
		const pair = (x: PeerId, y: PeerId) =>
			[x.toString(), y.toString()].sort().join("|");
		const session = await TestSession.disconnected<{
			directstream: AlternateRouteStream;
		}>(3, {
			addresses: { listen: ["/ip4/127.0.0.1/tcp/0"] },
			transports: [tcp()],
			connectionManager: { reconnectRetries: 0 },
			services: {
				directstream: (components) => {
					const deny = (remote: PeerId) =>
						blocked.has(pair(components.peerId, remote));
					Object.assign(components.connectionGater, {
						denyDialPeer: deny,
						denyInboundEncryptedConnection: deny,
						denyOutboundEncryptedConnection: deny,
					});
					return new AlternateRouteStream(components, seekTimeout);
				},
			},
		});
		const [aNode, bNode, relayNode] = session.peers;
		const a = aNode!.services.directstream;
		const b = bNode!.services.directstream;
		const relay = relayNode!.services.directstream;
		const lost: string[] = [];
		const onLost = ({ detail }: CustomEvent<PublicSignKey>) => {
			lost.push(detail.hashcode());
		};
		const hops = (from: DirectStream, to: DirectStream) =>
			from.getRouteHints(to.publicKeyHash).map((hint) => hint.nextHop);
		const send = (from: DirectStream, to: DirectStream, redundancy: number) =>
			from.publish(new Uint8Array([redundancy]), {
				mode: new AcknowledgeDelivery({ to: [to.publicKeyHash], redundancy }),
			});
		a.addEventListener("peer:unreachable", onLost);
		try {
			await session.connect();
			await Promise.all([
				waitForNeighbour(a, b),
				waitForNeighbour(a, relay),
				waitForNeighbour(b, relay),
			]);
			// Redundant ACKs teach a "b via relay" and the relay "b via a".
			await send(a, b, 2);
			await send(relay, b, 2);
			await waitForResolved(
				() => {
					expect(hops(a, b)).to.have.members([
						b.publicKeyHash,
						relay.publicKeyHash,
					]);
					expect(hops(relay, b)).to.have.members([
						b.publicKeyHash,
						a.publicKeyHash,
					]);
				},
				{ timeout: 5_000 },
			);
			blocked.add(pair(aNode!.peerId, bNode!.peerId));
			await Promise.all([
				aNode!.hangUp(bNode!.peerId),
				bNode!.hangUp(aNode!.peerId),
			]);
			await waitForResolved(
				() => {
					expect(a.peers.has(b.publicKeyHash)).to.equal(false);
					expect(hops(a, b)).to.deep.equal([relay.publicKeyHash]);
				},
				{ timeout: 5_000 },
			);
			// a now reaches b only through the relay, which still keeps "b via a".
			await send(a, b, 1);
			expect(hops(relay, b)).to.have.members([
				b.publicKeyHash,
				a.publicKeyHash,
			]);
			expect(lost).not.to.include(b.publicKeyHash);

			await bNode!.stop();
			// The relay's retained path leads back to a, so its probe fails. Without
			// the departure hint, a would keep b reachable through the relay.
			await waitForResolved(
				() =>
					expect(
						a.routes.isReachable(a.publicKeyHash, b.publicKeyHash),
					).to.equal(false),
				{ timeout: 5 * seekTimeout },
			);
			await waitForResolved(() => expect(lost).to.include(b.publicKeyHash), {
				timeout: 5 * seekTimeout,
			});
		} finally {
			a.removeEventListener("peer:unreachable", onLost);
			await session.stop();
		}
	});
});
