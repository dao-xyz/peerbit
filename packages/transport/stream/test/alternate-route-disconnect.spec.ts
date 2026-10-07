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
	constructor(components: DirectStreamComponents) {
		super(components, ["/alternate-route-disconnect/1.0.0"], {
			canRelayMessage: true,
			connectionManager: false,
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
});
