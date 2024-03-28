import { waitFor, delay, waitForResolved, TimeoutError } from "@peerbit/time";
import crypto from "crypto";
import {
	waitForPeers as waitForPeerStreams,
	DirectStream,
	DirectStreamComponents,
	ConnectionManagerArguments
} from "..";
import { Cache } from "@peerbit/cache";
import {
	ACK,
	AcknowledgeDelivery,
	AnyWhere,
	DataMessage,
	Goodbye,
	Message,
	MessageHeader,
	SeekDelivery,
	SilentDelivery,
	getMsgId
} from "@peerbit/stream-interface";
import { Ed25519Keypair, PublicSignKey, randomBytes } from "@peerbit/crypto";
import { PeerId } from "@libp2p/interface";
import { Multiaddr } from "@multiformats/multiaddr";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { jest } from "@jest/globals";
import { Libp2pOptions } from "libp2p";
import { DeliveryError } from "@peerbit/stream-interface";
import { yamux } from "@chainsafe/libp2p-yamux";
import { YamuxStream } from "@chainsafe/libp2p-yamux/stream";
import { equals } from "uint8arrays";

type TestSessionStream = TestSession<{ directstream: DirectStream }>;

const connectLine = async (session: TestSessionStream) => {
	await session.connectLine();
	for (let i = 0; i < session.peers.length - 1; i++) {
		await waitForPeerStreams(
			session.peers[i].services.directstream,
			session.peers[i + 1].services.directstream
		);
	}
	for (let i = 1; i < session.peers.length - 1; i++) {
		expect([
			...session.peers[i].services.directstream.peers.keys()
		]).toContainValues([
			session.peers[i - 1].services.directstream.publicKeyHash,
			session.peers[i + 1].services.directstream.publicKeyHash
		]);
	}
	expect([...session.peers[0].services.directstream.peers.keys()]).toEqual([
		session.peers[1].services.directstream.publicKeyHash
	]);
	expect([
		...session.peers[
			session.peers.length - 1
		].services.directstream.peers.keys()
	]).toEqual([
		session.peers[session.peers.length - 2].services.directstream.publicKeyHash
	]);
};
const collectDataWrites = (client: DirectStream) => {
	const writes: Map<string, DataMessage[]> = new Map();
	for (const [name, peer] of client.peers) {
		writes.set(name, []);
		const writeFn = peer.write.bind(peer);
		peer.write = (data, priority) => {
			const bytes = data instanceof Uint8Array ? data : data.subarray();
			const message = deserialize(bytes, Message);
			if (message instanceof DataMessage) {
				writes.get(name)?.push(message);
			}
			return writeFn(data, priority);
		};
	}
	return writes;
};

const getWritesCount = (writes: Map<string, DataMessage[]>) => {
	let sum = 0;
	for (const [k, v] of writes) {
		sum += v.length;
	}
	return sum;
};

const createMetrics = (stream: DirectStream) => {
	const s: {
		stream: TestDirectStream;
		messages: Message[];
		received: DataMessage[];
		ack: ACK[];
		goodbye: Goodbye[];
		reachable: PublicSignKey[];
		unrechable: PublicSignKey[];
		session: PublicSignKey[];
		processed: Map<string, number>;
	} = {
		reachable: [],
		unrechable: [],
		messages: [],
		received: [],
		session: [],
		ack: [],
		goodbye: [],
		processed: new Map(),
		stream
	};

	s.stream.addEventListener("message", (msg) => {
		s.messages.push(msg.detail);
	});

	s.stream.addEventListener("data", (msg) => {
		s.received.push(msg.detail);
	});

	s.stream.addEventListener("peer:reachable", (msg) => {
		s.reachable.push(msg.detail);
	});

	s.stream.addEventListener("peer:unreachable", (msg) => {
		s.unrechable.push(msg.detail);
	});

	s.stream.addEventListener("peer:session", (msg) => {
		s.session.push(msg.detail);
	});

	let processMessage = s.stream.processMessage.bind(s.stream);
	s.stream.processMessage = async (k, v, msg) => {
		const msgId = await getMsgId(
			msg instanceof Uint8Array ? msg : msg.subarray()
		);
		let prev = s.processed.get(msgId);
		s.processed.set(msgId, (prev ?? 0) + 1);
		return processMessage(k, v, msg);
	};

	const ackFn = s.stream.onAck.bind(s.stream);
	s.stream.onAck = (a, b, c, d) => {
		s.ack.push(d);
		return ackFn(a, b, c, d);
	};

	const goodByeFn = s.stream.onGoodBye.bind(s.stream);
	s.stream.onGoodBye = (a, b, c, d) => {
		s.goodbye.push(d);
		return goodByeFn(a, b, c, d);
	};

	return s;
};
const resetMetrics = (streams: ReturnType<typeof createMetrics>[]) => {
	streams.map((x) => {
		x.messages = [];
		(x.processed = new Map()), (x.reachable = []);
		x.received = [];
		x.unrechable = [];
	});
};
const collectMetrics = (session: TestSession<any>) => {
	let streams: ReturnType<typeof createMetrics>[] = [];
	for (const peer of session.peers) {
		streams.push(createMetrics(peer.services.directstream));
	}
	return streams;
};

class TestDirectStream extends DirectStream {
	constructor(
		components: DirectStreamComponents,
		options: {
			id?: string;
			connectionManager?: ConnectionManagerArguments;
			seekTimeout?: number;
			routeSeekInterval?: number;
			routeMaxRetentionPeriod?: number;
		} = {}
	) {
		super(components, [options.id || "/test/0.0.0"], {
			canRelayMessage: true,
			connectionManager: options.connectionManager,
			seekTimeout: options.seekTimeout,
			routeSeekInterval: options.routeSeekInterval,
			routeMaxRetentionPeriod: options.routeMaxRetentionPeriod,
			...options
		});
	}
}
const connected = async (
	n: number,
	options?:
		| Libp2pOptions<{ directstream: TestDirectStream }>
		| Libp2pOptions<{ directstream: TestDirectStream }>[]
) => {
	let session: TestSessionStream = await TestSession.connected(
		n,
		options || {
			services: {
				directstream: (components) => new TestDirectStream(components, options)
			}
		}
	);
	return session;
};

const disconnected = async (
	n: number,
	options?:
		| Libp2pOptions<{ directstream: TestDirectStream }>
		| Libp2pOptions<{ directstream: TestDirectStream }>[]
) => {
	let session: TestSessionStream = await TestSession.disconnected(
		n,
		options || {
			services: {
				directstream: (components) => new TestDirectStream(components, options)
			}
		}
	);
	return session;
};

const stream = (s: TestSessionStream, i: number): TestDirectStream =>
	service(s, i, "directstream") as TestDirectStream;
const service = (s: TestSessionStream, i: number, service: string) =>
	s.peers[i].services[service];
const waitForPeers = (s: TestSessionStream) =>
	waitForPeerStreams(...s.peers.map((x) => x.services.directstream));

describe("streams", function () {
	describe("mode", () => {
		const data = new Uint8Array([1, 2, 3]);

		describe("all", () => {
			let session: TestSessionStream;
			let streams: ReturnType<typeof createMetrics>[];

			beforeAll(async () => {});

			beforeEach(async () => {
				// 0 and 2 not connected
				session = await disconnected(4, {
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: false
							})
					}
				});

				/* 
				┌─┐
				│0│
				└┬┘
				┌▽┐
				│1│
				└┬┘
				┌▽┐
				│2│
				└┬┘
				┌▽┐
				│3│
				└─┘
				*/

				streams = collectMetrics(session);
				await session.connect([
					// behaviour seems to be more predictable if we connect after start (TODO improve startup to use existing connections in a better way)
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[2], session.peers[3]]
				]);

				await waitForPeerStreams(streams[0].stream, streams[1].stream);
				await waitForPeerStreams(streams[1].stream, streams[2].stream);
				await waitForPeerStreams(streams[2].stream, streams[3].stream);
			});

			afterEach(async () => {
				await session.stop();
			});
			it("1->all", async () => {
				await streams[0].stream.publish(data, { mode: new AnyWhere() });
				await waitFor(() => streams[1].received.length === 1);
				expect(new Uint8Array(streams[1].received[0].data!)).toEqual(data);
				await waitFor(() => streams[2].received.length === 1);
				expect(new Uint8Array(streams[2].received[0].data!)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(1);
				expect(streams[2].received).toHaveLength(1);
				expect(streams[3].received).toHaveLength(1);
			});
		});
		describe("auto", () => {
			let session: TestSessionStream;
			let streams: ReturnType<typeof createMetrics>[];

			beforeAll(async () => {});

			beforeEach(async () => {
				// 0 and 2 not connected
				session = await disconnected(4, {
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: false,
								routeSeekInterval: Number.MAX_SAFE_INTEGER //  disable auto seek so we can control routing changes manually
							})
					}
				});

				/* 
				┌─┐
				│0│
				└┬┘
				┌▽┐
				│1│
				└┬┘
				┌▽┐
				│2│
				└┬┘
				┌▽┐
				│3│
				└─┘
				*/

				streams = collectMetrics(session);

				await connectLine(session);
			});

			afterEach(async () => {
				await session.stop();
			});

			it("1->unknown", async () => {
				let t0 = +new Date();
				await streams[0].stream.publish(data);
				await waitFor(() => streams[1].received.length === 1);
				expect(new Uint8Array(streams[1].received[0].data!)).toEqual(data);
				await waitFor(() => streams[2].received.length === 1);
				expect(new Uint8Array(streams[2].received[0].data!)).toEqual(data);

				for (const [i, stream] of streams.entries()) {
					if (i < 2) {
						// because i = 2 is the last node and that node has no-where else to look
						expect(stream.stream.pending).toBeTrue(); // beacuse seeking with explitictly defined end (will timeout eventuallyl)
					}
				}

				// expect routes to have be defined
				await waitForResolved(() =>
					expect(streams[0].stream.routes.count()).toEqual(3)
				);

				let t1 = +new Date();
				expect(t1 - t0).toBeLessThan(4000); // routes are discovered in a limited time (will not wait for any timeouts)

				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(1);
				expect(streams[2].received).toHaveLength(1);
				expect(streams[3].received).toHaveLength(1);
			});

			it("1->2", async () => {
				await streams[0].stream.publish(data, {
					to: [streams[1].stream.components.peerId]
				});

				await waitFor(() => streams[1].received.length === 1);

				for (const stream of streams) {
					expect(stream.stream.pending).toBeFalse(); // since receiver is known and SilentDeliery by default if providing to: [...]
				}

				let receivedMessage = streams[1].received[0];
				expect(new Uint8Array(receivedMessage.data!)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(1);
				expect(streams[2].received).toHaveLength(0);

				// Never seen a message twice
				expect(
					[...streams[0].processed.values()].find((x) => x > 1)
				).toBeUndefined();
				expect(
					[...streams[1].processed.values()].find((x) => x > 1)
				).toBeUndefined();
				expect(
					[...streams[2].processed.values()].find((x) => x > 1)
				).toBeUndefined();
			});

			it("1->3", async () => {
				await streams[0].stream.publish(data, {
					to: [streams[2].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);
				expect(new Uint8Array(streams[2].received[0].data!)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[2].received).toHaveLength(1);
				expect(streams[1].received).toHaveLength(0);
			});

			it("1->3 10mb data", async () => {
				const bigData = crypto.randomBytes(1e7);
				await streams[0].stream.publish(bigData, {
					to: [streams[2].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);

				expect(new Uint8Array(streams[2].received[0].data!)).toHaveLength(
					bigData.length
				);
				expect(streams[2].received).toHaveLength(1);
				expect(streams[1].received).toHaveLength(0);
			});

			it("1->3 still works even if routing is missing", async () => {
				streams[0].stream.routes.clear();
				streams[1].stream.routes.clear();
				await streams[0].stream.publish(data, {
					to: [streams[2].stream.components.peerId]
				});
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);
				expect(new Uint8Array(streams[2].received[0].data!)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[2].received).toHaveLength(1);
				expect(streams[1].received).toHaveLength(0);
			});

			it("will favor shortest path", async () => {
				/* 
				┌───┐
				│0  │
				└┬─┬┘
				 │┌▽┐
				 ││1│
				 │└┬┘
				┌▽─▽┐
				│2  │
				└┬──┘
				┌▽┐  
				│3│  
				└─┘   
				*/

				await session.connect([[session.peers[0], session.peers[2]]]);
				await waitForPeerStreams(streams[0].stream, streams[2].stream);

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(
						streams[1].messages.filter((x) => x instanceof DataMessage)
					).toHaveLength(2)
				); // seeking will yield 2 DataMessages to node 1

				streams[1].messages = [];

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(streams[3].received).toHaveLength(1)
				);
				expect(
					streams[1].messages.filter((x) => x instanceof DataMessage)
				).toHaveLength(0);
			});

			it("the shortest path will always exist", async () => {
				/* 
				┌───┐
				│0  │
				└┬─┬┘
				 │┌▽┐
				 ││1│
				 │└┬┘
				┌▽─▽┐
				│2  │
				└┬──┘
				┌▽┐  
				│3│  
				└─┘   
				*/

				await session.connect([[session.peers[0], session.peers[2]]]);
				await waitForPeerStreams(streams[0].stream, streams[2].stream);

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					mode: new SeekDelivery({
						redundancy: 2,
						to: [streams[3].stream.components.peerId]
					})
				});

				await waitForResolved(() =>
					expect(
						streams[3].messages.filter((x) => x instanceof DataMessage)
					).toHaveLength(2)
				); // seeking will yield 3 DataMessages to node 3, since redundancy: 2 with seek (?)

				resetMetrics(streams);

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(streams[3].received).toHaveLength(1)
				);

				// Path goes through the fastest route
				expect(
					streams[1].messages.filter((x) => x instanceof DataMessage)
				).toHaveLength(0);

				expect(
					streams[3].received.filter((x) => x instanceof DataMessage)
				).toHaveLength(1);

				// Artificially make the path through node 2 the slowst
				const write02 = streams[0].stream.peers
					.get(streams[2].stream.publicKeyHash)!
					.write.bind(
						streams[0].stream.peers.get(streams[2].stream.publicKeyHash)
					);

				streams[0].stream.peers.get(streams[2].stream.publicKeyHash)!.write = (
					data,
					priority
				) => {
					delay(3000, { signal: streams[0].stream.closeController.signal })
						.then(() => write02(data, priority))
						.catch(() => {});
				};

				// Reseek again and check that path 0 -> 1 -> 2 -> 3 is "fastest"
				resetMetrics(streams);

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					mode: new SeekDelivery({
						redundancy: 2,
						to: [streams[3].stream.components.peerId]
					})
				});

				await waitForResolved(() =>
					expect(
						streams[3].messages.filter((x) => x instanceof DataMessage)
					).toHaveLength(2)
				); // seeking will yield 3 DataMessages to node 3, since redundancy: 2 with seek (?)

				resetMetrics(streams);

				// Make sure messages can still be delivered
				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(streams[3].received).toHaveLength(1)
				);

				// Path goes through the fastest route
				expect(
					streams[1].messages.filter((x) => x instanceof DataMessage)
				).toHaveLength(1);

				expect(
					streams[3].received.filter((x) => x instanceof DataMessage)
				).toHaveLength(1);
			});

			it("will eventually figure out shortest path", async () => {
				/* 
				┌───┐
				│ 0 │
				└┬─┬┘
				 │┌▽┐
				 ││1│
				 │└┬┘
				┌▽─▽┐
				│2  │
				└┬──┘
				┌▽┐  
				│3│  
				└─┘   
				*/

				await session.connect([[session.peers[0], session.peers[2]]]);
				await waitForPeerStreams(streams[0].stream, streams[2].stream);

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});

				// since redundancy is set to 2 by default we wil receive 2 acks
				await waitForResolved(() => expect(streams[0].ack).toHaveLength(2));
				await delay(2000);
				await waitForResolved(() => expect(streams[0].ack).toHaveLength(2));

				streams[1].messages = [];
				streams[3].received = [];

				expect(
					streams[0].stream.routes
						.findNeighbor(
							streams[0].stream.publicKeyHash,
							streams[3].stream.publicKeyHash
						)
						?.list?.map((x) => x.hash)
				).toEqual([
					streams[2].stream.publicKeyHash,
					streams[1].stream.publicKeyHash
				]); // "2" is fastest route

				await waitForResolved(() =>
					expect(
						streams[2].stream.routes
							.findNeighbor(
								streams[0].stream.publicKeyHash,
								streams[3].stream.publicKeyHash
							)
							?.list?.map((x) => x.hash)
					).toEqual([streams[3].stream.publicKeyHash])
				);

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(streams[3].received).toHaveLength(1)
				);

				expect(streams[1].messages).toHaveLength(0); // Because shortest route is 0 -> 2 -> 3
				expect(streams[1].stream.routes.count()).toEqual(2);
			});

			it("will not unecessarely seek", async () => {
				streams[0].stream.routeSeekInterval = 1000;
				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[1].stream.components.peerId]
				});
				await delay(1000);
				for (let i = 0; i < 10; i++) {
					await streams[0].stream.publish(crypto.randomBytes(1e2), {
						to: [streams[1].stream.components.peerId]
					});
				}
				await waitForResolved(() =>
					expect(streams[1].received).toHaveLength(11)
				);
				await waitForResolved(() =>
					expect(
						streams[1].received.filter(
							(x) => x.header.mode instanceof SeekDelivery
						).length
					).toBeLessThanOrEqual(2)
				);
			});
		});

		describe("seek", () => {
			let session: TestSessionStream;

			beforeAll(async () => {});

			afterEach(async () => {
				await session.stop();
			});
			it("will resolve immediately of no neighbours", async () => {
				session = await disconnected(1);
				await expect(
					session.peers[0].services.directstream.publish(
						new Uint8Array([1, 2, 3]),
						{
							mode: new SeekDelivery({
								redundancy: 1,
								to: [(await Ed25519Keypair.create()).publicKey]
							})
						}
					)
				).rejects.toThrow(DeliveryError);
				expect(session.peers[0].services.directstream.pending).toBeFalse();
			});
		});
	});

	describe("fanout", () => {
		describe("relay", () => {
			let session: TestSessionStream;
			let streams: ReturnType<typeof createMetrics>[];

			beforeAll(async () => {});

			beforeEach(async () => {
				session = await connected(3, {
					services: {
						directstream: (c) =>
							new TestDirectStream(c, { connectionManager: false })
					}
				});

				streams = collectMetrics(session);

				await waitForPeerStreams(streams[0].stream, streams[1].stream);
			});

			afterEach(async () => {
				await session.stop();
			});

			it("will not publish to 'from' when explicitly providing to", async () => {
				const msg = new DataMessage({
					data: new Uint8Array([0]),
					header: new MessageHeader({
						session: +new Date(),
						mode: new SeekDelivery({ redundancy: 1 })
					})
				});
				streams[2].stream.canRelayMessage = false; // so that 2 does not relay to 0

				await streams[1].stream.publishMessage(
					session.peers[0].services.directstream.publicKey,
					await msg.sign(streams[1].stream.sign),
					[
						//streams[1].stream.peers.get(streams[0].stream.publicKeyHash)!,
						streams[1].stream.peers.get(streams[2].stream.publicKeyHash)!
					]
				);
				const msgId = await getMsgId(msg.bytes());
				await waitForResolved(() =>
					expect(streams[2].processed.get(msgId)).toEqual(1)
				);

				await delay(1000); // wait for more messages eventually propagate
				expect(streams[0].processed.get(msgId)).toBeUndefined();
				expect(streams[1].processed.get(msgId)).toBeUndefined();
			});

			/**
			 * If tests below fails, dead-locks can apphear in unpredictable ways
			 */
			it("to in message will not send back", async () => {
				const msg = new DataMessage({
					data: new Uint8Array([0]),
					header: new MessageHeader({
						session: +new Date(),
						mode: new SeekDelivery({
							to: [
								streams[0].stream.publicKeyHash,
								streams[2].stream.publicKeyHash
							],
							redundancy: 1
						})
					})
				});
				streams[2].stream.canRelayMessage = false; // so that 2 does not relay to 0

				await msg.sign(streams[1].stream.sign);
				await streams[1].stream.publishMessage(
					session.peers[0].services.directstream.publicKey,
					msg,
					undefined,
					true
				);
				await delay(1000);
				const msgId = await getMsgId(msg.bytes());
				expect(streams[0].processed.get(msgId)).toBeUndefined();
				expect(streams[1].processed.get(msgId)).toBeUndefined();
				expect(streams[2].processed.get(msgId)).toEqual(1);
			});

			it("rejects when to peers is from", async () => {
				const msg = new DataMessage({
					data: new Uint8Array([0]),
					header: new MessageHeader({
						session: +new Date(),
						mode: new SilentDelivery({
							to: [streams[0].stream.publicKeyHash],
							redundancy: 1
						})
					})
				});
				await msg.sign(streams[1].stream.sign);
				await expect(
					streams[1].stream.publishMessage(
						session.peers[0].services.directstream.publicKey,
						msg,
						[streams[1].stream.peers.get(streams[0].stream.publicKeyHash)!]
					)
				).rejects.toThrowError("Message did not have any valid receivers");
			});

			it("rejects when only to is from", async () => {
				const msg = new DataMessage({
					data: new Uint8Array([0]),
					header: new MessageHeader({
						session: +new Date(),
						mode: new SilentDelivery({
							to: [streams[0].stream.publicKeyHash],
							redundancy: 1
						})
					})
				});
				await msg.sign(streams[1].stream.sign);
				await streams[1].stream.publishMessage(
					session.peers[0].services.directstream.publicKey,
					msg
				);
				const msgId = await getMsgId(msg.bytes());
				await delay(1000);
				expect(streams[0].processed.get(msgId)).toBeUndefined();
				expect(streams[1].processed.get(msgId)).toBeUndefined();
				expect(streams[2].processed.get(msgId)).toBeUndefined();
			});

			it("will send through peer", async () => {
				await session.peers[0].hangUp(session.peers[1].peerId);

				// send a message with to=[2]
				// make sure message is received
				const msg = new DataMessage({
					data: new Uint8Array([0]),
					header: new MessageHeader({
						session: +new Date(),
						mode: new SeekDelivery({
							to: [streams[2].stream.publicKeyHash],
							redundancy: 1
						})
					})
				});
				await msg.sign(streams[1].stream.sign);
				await streams[0].stream.publishMessage(
					session.peers[0].services.directstream.publicKey,
					msg
				);
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);
			});

			it("will only send to neighbour if available", async () => {
				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					mode: new SeekDelivery({
						redundancy: 2,
						to: [streams[2].stream.components.peerId]
					})
				});

				await waitForResolved(() => {
					const fanout = streams[0].stream.routes.getFanout(
						streams[0].stream.publicKey.hashcode(),
						[streams[2].stream.publicKey.hashcode()],
						100 // max redundancy
					);
					expect(fanout!.size).toEqual(1); // only the neigbour
					expect(
						[
							...fanout!.get(streams[2].stream.publicKey.hashcode())!.values()
						]?.map((x) => x.to)
					).toEqual([streams[2].stream.publicKey.hashcode()]);
				});
			});

			// TODO do we want this feat? will this leave to uneceessary messages?
			/* it("always relays if target is neighbour", async () => {
				await session.peers[0].hangUp(session.peers[2].peerId);
				streams.map(x => x.stream.routes.clear());

				// 0 -> 1 -> 2 still works
				streams[0].stream.routes.add(
					streams[0].stream.publicKeyHash,
					streams[1].stream.publicKeyHash,
					streams[2].stream.publicKeyHash,
					0,
					+new Date(),
					+new Date()
				);

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[2].stream.components.peerId]
				});
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);

				// ...yet make sure the data has not travelled this path
				expect(
					streams[1].messages.filter((x) => x instanceof DataMessage)
				).toHaveLength(0);
			}); */
		});

		describe("routes", () => {
			describe("redundancy", () => {
				describe("direct routes", () => {
					let session: TestSessionStream;
					let streams: ReturnType<typeof createMetrics>[];

					beforeAll(async () => {});

					beforeEach(async () => {
						session = await disconnected(3);
						streams = collectMetrics(session);
					});

					afterEach(async () => {
						await session.stop();
					});

					it("only uses direct routes on new connections", async () => {
						await session.connect([[session.peers[0], session.peers[1]]]);
						await session.connect([[session.peers[1], session.peers[2]]]);
						await waitForResolved(() =>
							expect(session.peers[1].services.directstream.peers.size).toEqual(
								2
							)
						);
						await session.peers[0].services.directstream.publish(
							new Uint8Array([0]),
							{
								mode: new SilentDelivery({
									to: [session.peers[2].peerId],
									redundancy: 1
								})
							}
						);
						await waitForResolved(() =>
							waitForResolved(() => expect(streams[2].received).toHaveLength(1))
						);
						expect(
							streams[1].messages.filter((x) => x instanceof DataMessage)
						).toHaveLength(1);
						await session.connect([[session.peers[0], session.peers[2]]]);
						await waitForResolved(() =>
							expect(session.peers[0].services.directstream.peers.size).toEqual(
								2
							)
						);
						await session.peers[0].services.directstream.publish(
							new Uint8Array([0]),
							{
								mode: new SilentDelivery({
									to: [session.peers[2].peerId],
									redundancy: 1
								})
							}
						);
						await waitForResolved(() =>
							waitForResolved(() => expect(streams[2].received).toHaveLength(2))
						);
						expect(
							streams[1].messages.filter((x) => x instanceof DataMessage)
						).toHaveLength(1); // because there is a direct route to 2 from 0 so no point more message should arrive here
					});
				});

				describe("1->3", () => {
					let session: TestSessionStream;
					let streams: ReturnType<typeof createMetrics>[];

					beforeAll(async () => {});

					beforeEach(async () => {
						session = await connected(4);
						for (const peer of session.peers) {
							await waitForResolved(() =>
								expect(peer.services.directstream.peers.size).toEqual(
									session.peers.length - 1
								)
							);
						}
						streams = collectMetrics(session);

						for (const peer of session.peers) {
							await waitForResolved(() =>
								expect(peer.services.directstream.peers.size).toEqual(
									session.peers.length - 1
								)
							);
						}
					});

					afterEach(async () => {
						await session.stop();
					});

					it("sends to neighbours", async () => {
						await waitForResolved(() =>
							expect(
								session.peers[0].services.directstream.routes.countAll()
							).toEqual(3)
						);

						await session.peers[0].services.directstream.publish(
							new Uint8Array([0]),
							{
								mode: new SilentDelivery({
									to: session.peers.map(
										(x) => x.services.directstream.publicKeyHash
									),
									redundancy: 1
								})
							}
						);
						await Promise.all(
							streams
								.slice(1)
								.map((x) =>
									waitForResolved(() => expect(x.received).toHaveLength(1))
								)
						);
					});
				});

				describe("1->2", () => {
					let session: TestSessionStream;
					let streams: ReturnType<typeof createMetrics>[];
					const data = new Uint8Array([1, 2, 3]);

					beforeAll(async () => {});

					beforeEach(async () => {
						session = await connected(3, {
							services: {
								directstream: (c) =>
									new TestDirectStream(c, { connectionManager: false })
							}
						});
						streams = collectMetrics(session);
						for (const peer of session.peers) {
							await waitForResolved(() =>
								expect(peer.services.directstream.peers.size).toEqual(
									session.peers.length - 1
								)
							);
						}
					});

					afterEach(async () => {
						await session.stop();
					});

					it("messages are only sent once to each peer", async () => {
						streams.forEach((stream) => {
							const processFn = stream.stream.processMessage.bind(
								stream.stream
							);
							stream.stream.processMessage = async (a, b, c) => {
								await delay(200);
								return processFn(a, b, c);
							};
						});

						let totalWrites = 10;
						expect(streams[0].ack).toHaveLength(0);

						//  push one message to ensure paths are found
						await streams[0].stream.publish(data, {
							mode: new SeekDelivery({
								redundancy: 2,
								to: [
									streams[1].stream.publicKeyHash,
									streams[2].stream.publicKeyHash
								]
							})
						});

						// message delivered to 1 from 0 and relayed through 2. (2 ACKS)
						// message delivered to 2 from 0 and relayed through 1. (2 ACKS)
						// 2 + 2 = 4
						expect(
							streams[0].stream.routes.isReachable(
								streams[0].stream.publicKeyHash,
								streams[1].stream.publicKeyHash
							)
						).toBeTrue();
						expect(
							streams[0].stream.routes.isReachable(
								streams[0].stream.publicKeyHash,
								streams[2].stream.publicKeyHash
							)
						).toBeTrue();

						await waitForResolved(() =>
							expect(streams[0].stream.routes.countAll()).toEqual(4)
						);
						await waitForResolved(() =>
							expect(streams[1].stream.routes.countAll()).toEqual(3)
						);
						await waitForResolved(() =>
							expect(streams[2].stream.routes.countAll()).toEqual(3)
						);

						streams[0].stream.routeSeekInterval = Number.MAX_VALUE; // disable seek so that we can check that the right amount of messages are sent below
						streams[1].received = [];
						streams[2].received = [];
						const allWrites = streams.map((x) => collectDataWrites(x.stream));

						// expect the data to be sent smartly
						for (let i = 0; i < totalWrites; i++) {
							await streams[0].stream.publish(data, {
								to: [
									streams[1].stream.publicKeyHash,
									streams[2].stream.publicKeyHash
								]
							});
						}

						await waitForResolved(() =>
							expect(streams[1].received).toHaveLength(totalWrites)
						);
						await waitForResolved(() =>
							expect(streams[2].received).toHaveLength(totalWrites)
						);

						await delay(2000);

						// Check number of writes for each node
						expect(getWritesCount(allWrites[0])).toEqual(totalWrites * 2); // write to "1" or "2"
						expect(getWritesCount(allWrites[1])).toEqual(0); // "1" should never has to push any data
						expect(getWritesCount(allWrites[2])).toEqual(0); // "2" should never has to push any data
					});
				});

				describe("1->2->2", () => {
					/** 
					┌─────┐ 
					│0    │ 
					└┬───┬┘ 
					┌▽─┐┌▽─┐
					│2 ││1 │
					└┬┬┘└┬┬┘
					││  ││ 
					││  ││ 
					││  └│┐
					└│──┐││
					┌│──│┘│
					││  │┌┘
					┌▽▽┐┌▽▽┐
					│3 ││4 │ // 3 and 4 are connected also
					└──┘└──┘
					*/

					let session: TestSessionStream;
					let streams: ReturnType<typeof createMetrics>[];
					const data = new Uint8Array([1, 2, 3]);

					beforeAll(async () => {});

					beforeEach(async () => {
						session = await disconnected(5, {
							services: {
								directstream: (c) =>
									new TestDirectStream(c, { connectionManager: false })
							}
						});
						streams = collectMetrics(session);

						await session.connect([
							// behaviour seems to be more predictable if we connect after start (TODO improve startup to use existing connections in a better way)
							[session.peers[0], session.peers[1]],
							[session.peers[0], session.peers[2]],

							[session.peers[1], session.peers[3]],
							[session.peers[1], session.peers[4]],

							[session.peers[2], session.peers[3]],
							[session.peers[2], session.peers[4]],

							[session.peers[3], session.peers[4]]
						]);

						await waitForPeerStreams(streams[0].stream, streams[1].stream);
						await waitForPeerStreams(streams[0].stream, streams[2].stream);
						await waitForPeerStreams(streams[1].stream, streams[3].stream);
						await waitForPeerStreams(streams[1].stream, streams[4].stream);
						await waitForPeerStreams(streams[2].stream, streams[3].stream);
						await waitForPeerStreams(streams[2].stream, streams[4].stream);
						await waitForPeerStreams(streams[3].stream, streams[4].stream);
					});

					afterEach(async () => {
						await session.stop();
					});

					it("messages are only sent once to each peer", async () => {
						streams.forEach((stream) => {
							const processFn = stream.stream.processMessage.bind(
								stream.stream
							);
							stream.stream.processMessage = async (a, b, c) => {
								await delay(200);
								return processFn(a, b, c);
							};
						});

						await streams[0].stream.publish(data, {
							mode: new SeekDelivery({
								to: [
									streams[3].stream.publicKeyHash,
									streams[4].stream.publicKeyHash
								],
								redundancy: 2
							})
						});

						expect(
							streams[0].stream.routes.isReachable(
								streams[0].stream.publicKeyHash,
								streams[3].stream.publicKeyHash
							)
						).toBeTrue();
						expect(
							streams[0].stream.routes.isReachable(
								streams[0].stream.publicKeyHash,
								streams[4].stream.publicKeyHash
							)
						).toBeTrue();

						expect(
							streams[0].stream.routes
								.findNeighbor(
									streams[0].stream.publicKeyHash,
									streams[3].stream.publicKeyHash
								)
								?.list.map((x) => x.hash)
						).toHaveLength(2);

						expect(
							streams[0].stream.routes.findNeighbor(
								streams[0].stream.publicKeyHash,
								streams[4].stream.publicKeyHash
							)?.list
						).toHaveLength(2);

						await waitForResolved(() =>
							expect(streams[0].stream.routes.countAll()).toEqual(6)
						);
						await waitForResolved(() =>
							expect(
								streams[1].stream.routes.countAll()
							).toBeGreaterThanOrEqual(5)
						);
						await waitForResolved(() =>
							expect(
								streams[2].stream.routes.countAll()
							).toBeGreaterThanOrEqual(5)
						);
						await waitForResolved(() =>
							expect(
								streams[3].stream.routes.countAll()
							).toBeGreaterThanOrEqual(3)
						);

						await waitForResolved(() =>
							expect(
								streams[4].stream.routes.countAll()
							).toBeGreaterThanOrEqual(3)
						);

						let totalWrites = 1;

						const allWrites = streams.map((x) => collectDataWrites(x.stream));

						streams[3].received = [];
						streams[4].received = [];
						/* 	streams[3].messages = [];
							streams[4].messages = []; */
						streams[3].processed.clear();
						streams[4].processed.clear();

						streams.forEach(
							(x) => (x.stream.routeSeekInterval = Number.MAX_SAFE_INTEGER)
						);

						for (let i = 0; i < totalWrites; i++) {
							streams[0].stream.publish(data, {
								mode: new SilentDelivery({
									redundancy: 1,
									to: [
										streams[3].stream.publicKeyHash,
										streams[4].stream.publicKeyHash
									]
								})
							});
						}

						await waitForResolved(() =>
							expect(streams[3].received).toHaveLength(totalWrites)
						);
						await waitForResolved(() =>
							expect(streams[4].received).toHaveLength(totalWrites)
						);

						const id1 = await getMsgId(serialize(streams[3].received[0]));

						await delay(3000); // Wait some exstra time if additional messages are propagating through

						expect(streams[3].processed.get(id1)).toEqual(1); // 1 delivery even though there are multiple path leading to this node
						expect(streams[4].processed.get(id1)).toEqual(1); // 1 delivery even though there are multiple path leading to this node

						// Check number of writes for each node
						expect(getWritesCount(allWrites[0])).toEqual(totalWrites); // write to "1" or "2"
						expect(
							getWritesCount(allWrites[1]) + getWritesCount(allWrites[2])
						).toEqual(totalWrites * 2); // write to "3" and "4"
						expect(getWritesCount(allWrites[3])).toEqual(0); // "3" should never has to push any data
						expect(getWritesCount(allWrites[4])).toEqual(0); // "4" should never has to push any data
					});

					it("can send with higher redundancy", async () => {
						await streams[0].stream.publish(data, {
							mode: new SeekDelivery({
								redundancy: 2,
								to: [
									streams[3].stream.publicKeyHash,
									streams[4].stream.publicKeyHash
								]
							})
						});

						const neighbourTo3 = streams[0].stream.routes.findNeighbor(
							streams[0].stream.publicKeyHash,
							streams[3].stream.publicKeyHash
						)!.list[0];

						expect(
							streams[0].stream.routes.isReachable(
								streams[0].stream.publicKeyHash,
								streams[3].stream.publicKeyHash
							)
						).toBeTrue();
						expect(
							streams[0].stream.routes.isReachable(
								streams[0].stream.publicKeyHash,
								streams[4].stream.publicKeyHash
							)
						).toBeTrue();

						streams.find(
							(x) => x.stream.publicKeyHash === neighbourTo3.hash
						)!.stream.processMessage = async (a, b, c) => {
							// dont do anything
						};

						await streams[0].stream.publish(data, {
							mode: new AcknowledgeDelivery({
								redundancy: 2,
								to: [
									streams[3].stream.publicKeyHash,
									streams[4].stream.publicKeyHash
								]
							}) // send at least 2 routes
						});
					});
				});
			});

			describe("invalidation", () => {
				let session: TestSessionStream;

				beforeAll(async () => {});

				afterEach(async () => {
					await session.stop();
				});

				it("always keeps a route to direct connections", async () => {
					session = await connected(2, {
						services: {
							directstream: (c) =>
								new TestDirectStream(c, {
									connectionManager: false,
									seekTimeout: 3e3
								})
						}
					});
					await waitForPeerStreams(
						session.peers[0].services.directstream,
						session.peers[1].services.directstream
					);

					// make it so that one node is responsive
					session.peers[1].services.directstream.publishMessage =
						(() => {}) as any;

					// now route should persist even if peer can't reach
					await expect(
						session.peers[0].services.directstream.publish(new Uint8Array(0), {
							mode: new SeekDelivery({
								redundancy: 1,
								to: [session.peers[1].peerId]
							})
						})
					).rejects.toThrow();
					expect(
						session.peers[0].services.directstream.routes.isReachable(
							session.peers[0].services.directstream.publicKeyHash,
							session.peers[1].services.directstream.publicKeyHash,
							0
						)
					).toBeTrue();
				});

				it("keeps old routes until timeout", async () => {
					const routeMaxRetentionPeriod = 2e3;
					session = await disconnected(4, {
						services: {
							directstream: (c) =>
								new TestDirectStream(c, {
									connectionManager: false,
									routeMaxRetentionPeriod
								})
						}
					});
					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
						[session.peers[2], session.peers[3]]
					]);

					await waitForPeerStreams(
						session.peers[0].services.directstream,
						session.peers[1].services.directstream
					);
					await waitForPeerStreams(
						session.peers[1].services.directstream,
						session.peers[2].services.directstream
					);
					await waitForPeerStreams(
						session.peers[2].services.directstream,
						session.peers[3].services.directstream
					);

					await session.peers[0].services.directstream.publish(
						new Uint8Array([123]),
						{ mode: new SeekDelivery({ redundancy: 2 }) }
					);

					await session.connect([[session.peers[0], session.peers[2]]]);

					await waitForPeerStreams(
						session.peers[0].services.directstream,
						session.peers[2].services.directstream
					);

					await session.peers[0].services.directstream.publish(
						new Uint8Array([123]),
						{ mode: new SeekDelivery({ redundancy: 2 }) }
					);

					await waitForResolved(async () => {
						const routes23From01 =
							session.peers[2].services.directstream.routes.routes
								.get(session.peers[1].services.directstream.publicKeyHash)
								?.get(session.peers[3].services.directstream.publicKeyHash);
						expect(routes23From01?.list.map((x) => x.hash)).toEqual([
							session.peers[3].services.directstream.publicKeyHash,
							session.peers[3].services.directstream.publicKeyHash
						]);
					});
					const fanout23From01 =
						session.peers[2].services.directstream.routes.getFanout(
							session.peers[1].services.directstream.publicKeyHash,
							[session.peers[3].services.directstream.publicKeyHash],
							2
						);
					const routes231 = fanout23From01?.get(
						session.peers[3].services.directstream.publicKeyHash
					);
					expect([...routes231!.values()]?.map((x) => x.to)).toEqual([
						session.peers[3].services.directstream.publicKeyHash
					]);

					await delay(routeMaxRetentionPeriod + 1000);
					const routes23From01 =
						session.peers[2].services.directstream.routes.routes
							.get(session.peers[1].services.directstream.publicKeyHash)
							?.get(session.peers[3].services.directstream.publicKeyHash);
					expect(routes23From01?.list.map((x) => x.hash)).toEqual([
						session.peers[3].services.directstream.publicKeyHash
					]);
				});

				it("updating existing route will reset expiry time, neighbour", async () => {
					let routeMaxRetentionPeriod = 2000;
					session = await connected(1, {
						services: {
							directstream: (c) =>
								new TestDirectStream(c, {
									connectionManager: false,
									routeMaxRetentionPeriod
								})
						}
					});

					const neighbour = (await Ed25519Keypair.create()).publicKey;

					// Update route twice to force cleanup of old route
					session.peers[0].services.directstream.routes.add(
						session.peers[0].services.directstream.publicKey.hashcode(),
						neighbour.hashcode(),
						neighbour.hashcode(),
						0,
						+new Date(),
						-1
					);
					session.peers[0].services.directstream.routes.add(
						session.peers[0].services.directstream.publicKey.hashcode(),
						neighbour.hashcode(),
						neighbour.hashcode(),
						0,
						+new Date(),
						-1
					);
					expect(
						session.peers[0].services.directstream.routes
							.getFanout(
								session.peers[0].services.directstream.publicKey.hashcode(),
								[neighbour.hashcode()],
								1
							)
							?.get(neighbour.hashcode())!.size
					).toEqual(1);
					await delay(routeMaxRetentionPeriod);
					expect(
						session.peers[0].services.directstream.routes
							.getFanout(
								session.peers[0].services.directstream.publicKey.hashcode(),
								[neighbour.hashcode()],
								1
							)
							?.get(neighbour.hashcode())!.size
					).toEqual(1);
				});

				it("updating existing route will reset expiry time, hop", async () => {
					let routeMaxRetentionPeriod = 2000;
					session = await connected(1, {
						services: {
							directstream: (c) =>
								new TestDirectStream(c, {
									connectionManager: false,
									routeMaxRetentionPeriod
								})
						}
					});
					const neighbour = (await Ed25519Keypair.create()).publicKey;
					const remote = (await Ed25519Keypair.create()).publicKey;

					// First route
					const now = +new Date();
					session.peers[0].services.directstream.routes.add(
						session.peers[0].services.directstream.publicKey.hashcode(),
						neighbour.hashcode(),
						remote.hashcode(),
						0,
						now - 100,
						-1
					);

					// New route is longer
					session.peers[0].services.directstream.routes.add(
						session.peers[0].services.directstream.publicKey.hashcode(),
						neighbour.hashcode(),
						remote.hashcode(),
						1,
						now,
						-1
					);

					expect(
						session.peers[0].services.directstream.routes.routes
							.get(session.peers[0].services.directstream.publicKey.hashcode())
							?.get(remote.hashcode())
							?.list.map((x) => x.hash)
					).toEqual([neighbour.hashcode(), neighbour.hashcode()]);
					await delay(routeMaxRetentionPeriod + 1000);
					expect(
						session.peers[0].services.directstream.routes.routes
							.get(session.peers[0].services.directstream.publicKey.hashcode())
							?.get(remote.hashcode())
							?.list.map((x) => x.hash)
					).toEqual([neighbour.hashcode()]);
				});
			});
		});

		describe("bandwidth", () => {
			let session: TestSessionStream;
			let streams: ReturnType<typeof createMetrics>[];

			beforeEach(async () => {
				session = await connected(3, {
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: {
									dialer: false,
									minConnections: 1,
									pruner: { interval: 1000 }
								}
							})
					}
				});
				streams = collectMetrics(session);

				await waitForPeerStreams(streams[0].stream, streams[1].stream);
				await waitForPeerStreams(streams[0].stream, streams[2].stream);
				await waitForPeerStreams(streams[1].stream, streams[2].stream);
			});

			afterEach(async () => {
				await session.stop();
			});

			it("tracks stream usage over time", async () => {
				expect(
					streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
						?.usedBandwidth
				).toEqual(0);
				expect(
					streams[0].stream.peers.get(streams[2].stream.publicKey.hashcode())
						?.usedBandwidth
				).toEqual(0);

				await streams[0].stream.publish(new Uint8Array(100), {
					to: [streams[1].stream.publicKey, streams[2].stream.publicKey]
				});

				expect(
					streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
						?.usedBandwidth
				).toBeGreaterThan(30);
				expect(
					streams[0].stream.peers.get(streams[2].stream.publicKey.hashcode())
						?.usedBandwidth
				).toBeGreaterThan(30);
				await waitForResolved(() =>
					expect(
						streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
							?.usedBandwidth
					).toBeLessThan(30)
				);
				await waitForResolved(() =>
					expect(
						streams[0].stream.peers.get(streams[2].stream.publicKey.hashcode())
							?.usedBandwidth
					).toBeLessThan(30)
				);
			});

			it("bandwidth limits pruning", async () => {
				expect(streams[0].stream.peers.size).toEqual(2);
				streams[0].stream.connectionManagerOptions.pruner!.bandwidth = 1;
				await streams[0].stream.publish(new Uint8Array(100), {
					to: [streams[1].stream.publicKey, streams[2].stream.publicKey]
				});
				await waitForResolved(() =>
					expect(streams[0].stream.peers.size).toEqual(1)
				);
				expect(
					streams[1].received.filter((x) => x.data?.length || 0 > 0)
				).toHaveLength(1);
				expect(
					streams[2].received.filter((x) => x.data?.length || 0 > 0)
				).toHaveLength(1);

				await streams[0].stream.publish(new Uint8Array(101), {
					to: [streams[1].stream.publicKey, streams[2].stream.publicKey]
				});

				// messages can still deliver
				await waitForResolved(() => {
					expect(
						streams[1].received.filter((x) => x.data?.length || 0 > 0)
					).toHaveLength(2);
					expect(
						streams[2].received.filter((x) => x.data?.length || 0 > 0)
					).toHaveLength(2);
				});
			});

			it("max queued buffer pruning", async () => {
				streams[0].stream.connectionManagerOptions.pruner!.maxBuffer = 1;
				await streams[0].stream.maybePruneConnections();
				expect(streams[0].stream.peers.size).toEqual(2);

				[...streams[0].stream.peers.values()].forEach((x) => {
					x.outboundStream! = { ...x.outboundStream, readableLength: 2 } as any;
				});
				await streams[0].stream.maybePruneConnections();
				await waitForResolved(() =>
					expect(streams[0].stream.peers.size).toEqual(1)
				);
			});

			//  maybe connect directly if pruned

			it("rejects incomming connections that are pruned", async () => {
				streams[0].stream.connectionManagerOptions.pruner!.bandwidth = 1;
				await streams[0].stream.publish(new Uint8Array(100), {
					to: [streams[2].stream.publicKey]
				});
				await waitForResolved(() =>
					expect(streams[0].stream.peers.size).toEqual(1)
				);
				await waitForResolved(() =>
					expect(streams[1].stream.peers.size).toEqual(1)
				);

				expect(streams[0].stream["prunedConnectionsCache"]!.size).toEqual(1);
				expect(
					streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
				).toBeUndefined(); // beacuse stream[1] has received less data from stream[0] (least important)

				await session.peers[1].dial(session.peers[0].getMultiaddrs());

				await delay(3000);

				// expect a connection to not be established
				expect(
					streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
				).toBeUndefined(); // beacuse stream[1] has received less data from stream[0] (least important)
				streams[0].stream["prunedConnectionsCache"]?.clear();
				session.peers[0].services.directstream.connectionManagerOptions.pruner =
					undefined;
				session.peers[1].services.directstream.connectionManagerOptions.pruner =
					undefined;
				await session.peers[1].dial(session.peers[0].getMultiaddrs());

				await delay(3000);
				await waitForResolved(() =>
					expect(
						streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
					).toBeDefined()
				);
			});

			// if incomed incommection and pruned, timeout (?)
			it("will not dial that are pruned", async () => {
				// enable the autodialer (TODO do this on the setup step instead)
				streams[0].stream.connectionManagerOptions.dialer = { retryDelay: 1e4 };
				streams[0].stream["recentDials"] = new Cache({
					ttl: 1e4,
					max: 1e3
				});

				streams[0].stream.connectionManagerOptions.pruner!.bandwidth = 1;
				await streams[0].stream.publish(new Uint8Array(100), {
					to: [streams[2].stream.publicKey]
				});
				await waitForResolved(() =>
					expect(streams[0].stream.peers.size).toEqual(1)
				);
				await waitForResolved(() =>
					expect(streams[1].stream.peers.size).toEqual(1)
				);

				expect(streams[0].stream["prunedConnectionsCache"]!.size).toEqual(1);
				expect(
					streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
				).toBeUndefined();

				streams[0].stream.connectionManagerOptions.pruner!.bandwidth =
					Number.MAX_SAFE_INTEGER;

				await streams[0].stream.publish(new Uint8Array(100), {
					mode: new SeekDelivery({
						redundancy: 1,
						to: [streams[1].stream.publicKey]
					})
				});
				await delay(2000);

				// will not dial
				expect(
					streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
				).toBeUndefined();

				// clear the map that filter the dial
				streams[0].stream["prunedConnectionsCache"]?.clear();
				await streams[0].stream.publish(new Uint8Array(100), {
					mode: new SeekDelivery({
						redundancy: 2,
						to: [streams[1].stream.publicKey.hashcode()]
					})
				});

				await waitForResolved(() =>
					expect(
						streams[0].stream.peers.get(streams[1].stream.publicKey.hashcode())
					).toBeDefined()
				);
			});
		});
	});

	describe("concurrency", () => {
		let session: TestSessionStream;
		let streams: ReturnType<typeof createMetrics>[];
		let timer: ReturnType<typeof setTimeout>;

		beforeAll(async () => {});

		beforeEach(async () => {
			session = await connected(3, {
				services: {
					directstream: (c) =>
						new TestDirectStream(c, { connectionManager: false })
				}
			});
			streams = collectMetrics(session);

			await waitForPeerStreams(streams[0].stream, streams[2].stream);
			await waitForPeerStreams(streams[0].stream, streams[1].stream);
			await waitForPeerStreams(streams[1].stream, streams[2].stream);
		});

		afterEach(async () => {
			timer && clearTimeout(timer);
			await session.stop();
		});

		it("can concurrently seek and wait for ack", async () => {
			await streams[0].stream.publish(crypto.randomBytes(1e2), {
				to: [streams[2].stream.components.peerId]
			});
			const p = streams[0].stream.publish(crypto.randomBytes(1e2), {
				mode: new AcknowledgeDelivery({
					redundancy: 1,
					to: [streams[2].stream.components.peerId]
				})
			});
			streams[0].stream.publish(crypto.randomBytes(1e2), {
				mode: new SeekDelivery({
					redundancy: 1,
					to: [streams[2].stream.components.peerId]
				})
			});

			streams[0].stream.publish(crypto.randomBytes(1e2), {
				mode: new SeekDelivery({
					redundancy: 1,
					to: [streams[2].stream.components.peerId]
				})
			});
			streams[2].stream.publish(crypto.randomBytes(1e2), {
				mode: new SeekDelivery({
					redundancy: 1,
					to: [streams[0].stream.components.peerId]
				})
			});

			await p;
		});
	});

	describe("limits", () => {
		let session: TestSessionStream;

		beforeAll(async () => {});

		beforeEach(async () => {
			session = await connected(2);
			await waitForPeerStreams(
				session.peers[0].services.directstream,
				session.peers[1].services.directstream
			);
		});

		afterEach(async () => {
			await session.stop();
		});
		it("max message size", async () => {
			await expect(
				session.peers[0].services.directstream.publish(
					new Uint8Array(1e7 + 1001),
					{
						mode: new SeekDelivery({
							to: [session.peers[1].services.directstream.publicKeyHash],
							redundancy: 1
						})
					}
				)
			).rejects.toThrow(/^Message too large/);
		});
	});
});

// TODO test that messages are not sent backward, triangles etc

describe("join/leave", () => {
	let session: TestSessionStream;
	let streams: ReturnType<typeof createMetrics>[];
	const data = new Uint8Array([1, 2, 3]);
	let autoDialRetryDelay = 5 * 1000;

	describe("auto dialer", () => {
		beforeEach(async () => {
			session = await disconnected(
				4,
				new Array(4).fill(0).map((_x, i) => {
					return {
						services: {
							directstream: (c) =>
								new TestDirectStream(c, {
									connectionManager: {
										pruner: undefined,
										dialer: i === 0 ? { retryDelay: autoDialRetryDelay } : false // allow client 0 to auto dial
									}
								})
						}
					};
				})
			); // Second arg is due to https://github.com/transport/js-libp2p/issues/1690

			streams = collectMetrics(session);

			for (const [i, peer] of session.peers.entries()) {
				if (i === 0) {
					expect(
						!!peer.services.directstream.connectionManagerOptions.dialer
					).toBeTrue();
				} else {
					expect(
						!!peer.services.directstream.connectionManagerOptions.dialer
					).toBeFalse();
				}
			}

			await connectLine(session);
		});

		afterEach(async () => {
			await session.stop();
		});

		it("directly if possible", async () => {
			const dialFn = jest.fn(
				streams[0].stream.components.connectionManager.openConnection
			);
			streams[0].stream.components.connectionManager.openConnection = dialFn;

			streams[3].received = [];
			expect(streams[0].stream.peers.size).toEqual(1);

			await streams[0].stream.publish(data, {
				mode: new SeekDelivery({
					to: [streams[3].stream.components.peerId],
					redundancy: 1
				})
			});

			await waitFor(() => streams[0].ack.length === 1);

			// Dialing will yield a new connection
			await waitForResolved(() =>
				expect(streams[0].stream.peers.size).toEqual(2)
			);

			expect(dialFn).toHaveBeenCalledOnce();

			// Republishing will not result in an additional dial
			await streams[0].stream.publish(data, {
				to: [streams[3].stream.components.peerId]
			});
			await waitFor(() => streams[3].received.length === 2);
			expect(dialFn).toHaveBeenCalledOnce();
			expect(streams[0].stream.peers.size).toEqual(2);
			expect(
				streams[0].stream.peers.has(streams[3].stream.publicKeyHash)
			).toBeTrue();
			expect(
				streams[0].stream.peers.has(streams[1].stream.publicKeyHash)
			).toBeTrue();
		});

		it("intermediate routes are eventually updated", async () => {
			expect(streams[0].stream.peers.size).toEqual(1);
			expect(streams[1].stream.peers.size).toEqual(2);
			expect(streams[2].stream.peers.size).toEqual(2);
			expect(streams[3].stream.peers.size).toEqual(1);

			await streams[0].stream.publish(data, {
				mode: new SeekDelivery({
					to: [streams[3].stream.peerId],
					redundancy: 1
				})
			});

			await waitForResolved(() =>
				expect(
					streams[0].stream.peers.has(streams[3].stream.publicKeyHash)
				).toBeTrue()
			);

			await waitForResolved(() =>
				expect(
					streams[3].stream.peers.has(streams[0].stream.publicKeyHash)
				).toBeTrue()
			);

			expect(streams[0].stream.peers.size).toEqual(2);
			expect(streams[1].stream.peers.size).toEqual(2);
			expect(streams[2].stream.peers.size).toEqual(2);
			expect(streams[3].stream.peers.size).toEqual(2);

			await streams[0].stream.publish(data, {
				mode: new SilentDelivery({
					redundancy: 1,
					to: [streams[3].stream.peerId]
				})
			});

			await waitForResolved(() =>
				expect(streams[0].stream.pending).toBeFalse()
			);
			await waitForResolved(() => expect(streams[3].received).toHaveLength(2));

			streams[3].received = [];
			streams[3].messages = [];

			await streams[0].stream.publish(data, {
				mode: new SilentDelivery({
					redundancy: 1,
					to: [streams[3].stream.peerId]
				})
			});

			// Expect no unecessary messages
			await waitForResolved(() =>
				expect(
					streams[3].messages.filter((x) => x instanceof DataMessage)
				).toHaveLength(1)
			);
			await delay(1000);
			expect(
				streams[3].messages.filter((x) => x instanceof DataMessage)
			).toHaveLength(1);
		});

		it("can leave and join quickly", async () => {
			await streams[0].stream.publish(data, {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});
			await waitForResolved(
				() => expect(streams[0].stream.routes.count()).toEqual(2) // neighbour + streams[3]
			);

			// miss on messages

			let missedOne = false;

			let msg: any[] = [];
			let unreachable: PublicSignKey[] = [];

			streams[0].stream.addEventListener("peer:unreachable", (e) => {
				unreachable.push(e.detail);
			});

			// simulate beeing offline for 1 messages
			const onDataMessage = streams[3].stream.onDataMessage.bind(
				streams[3].stream
			);
			streams[3].stream.onDataMessage = async (
				publicKey,
				peerStream,
				message,
				seenBefore
			) => {
				msg.push(message);
				if (!missedOne) {
					missedOne = true;
					return true;
				}
				return onDataMessage(publicKey, peerStream, message, seenBefore);
			};

			const publishToMissing = streams[0].stream.publish(data, {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});
			await delay(1000);
			await streams[0].stream.publish(data, {
				// Since this the next message

				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});
			await expect(publishToMissing).rejects.toThrow(DeliveryError);
			expect(missedOne).toBeTrue();
			expect(unreachable).toHaveLength(0); // because the next message reached the node before the first message timed out
		});

		it("retry dial after a while", async () => {
			let dials: (PeerId | Multiaddr | Multiaddr[])[] = [];
			streams[0].stream.components.connectionManager.openConnection = (
				a,
				b
			) => {
				dials.push(a);
				throw new Error("Mock Error");
			};

			streams[3].received = [];
			expect(streams[0].stream.peers.size).toEqual(1);

			await streams[0].stream.publish(data, {
				mode: new SeekDelivery({
					to: [streams[3].stream.components.peerId],
					redundancy: 1
				})
			});

			await waitForResolved(() => expect(streams[0].ack).toHaveLength(1));

			// Dialing will yield a new connection
			await waitFor(() => streams[0].stream.peers.size === 1);
			let expectedDialsCount = 1; // 1 dial directly
			expect(dials).toHaveLength(expectedDialsCount);

			// Republishing will not result in an additional dial
			await streams[0].stream.publish(data, {
				mode: new SeekDelivery({
					to: [streams[3].stream.components.peerId],
					redundancy: 1
				})
			});

			await waitForResolved(() => expect(streams[0].ack).toHaveLength(2));

			let t1 = +new Date();
			expect(dials).toHaveLength(expectedDialsCount); // No change, because TTL > autoDialRetryTimeout
			await waitFor(() => +new Date() - t1 > autoDialRetryDelay);

			// Try again, now expect another dial call, since the retry interval has been reached
			await streams[0].stream.publish(data, {
				mode: new SeekDelivery({
					to: [streams[3].stream.components.peerId],
					redundancy: 1
				})
			});
			await waitForResolved(() => expect(streams[0].ack).toHaveLength(3));

			expect(dials).toHaveLength(2);
		});

		/* TODO test that autodialler tries multiple addresses 
		
		it("through relay if fails", async () => {
			const dialFn =
				streams[0].stream.components.connectionManager.openConnection.bind(
					streams[0].stream.components.connectionManager
				);
		
			let directlyDialded = false;
			const filteredDial = (address: PeerId | Multiaddr | Multiaddr[]) => {
				if (
					isPeerId(address) &&
					address.toString() === streams[3].stream.peerIdStr
				) {
					throw new Error("Mock fail"); // don't allow connect directly
				}
		
				let addresses: Multiaddr[] = Array.isArray(address)
					? address
					: [address as Multiaddr];
				for (const a of addresses) {
					if (
						!a.protoNames().includes("p2p-circuit") &&
						a.toString().includes(streams[3].stream.peerIdStr)
					) {
						throw new Error("Mock fail"); // don't allow connect directly
					}
				}
				addresses = addresses.map((x) =>
					x.protoNames().includes("p2p-circuit")
						? multiaddr(x.toString().replace("/webrtc/", "/"))
						: x
				); // TODO use webrtc in node
		
				directlyDialded = true;
				return dialFn(addresses);
			};
		
			streams[0].stream.components.connectionManager.openConnection =
				filteredDial;
			expect(streams[0].stream.peers.size).toEqual(1);
			await streams[0].stream.publish(data, {
				to: [streams[3].stream.components.peerId],
				mode: new SeekDelivery({redundancy: 1})
			});
			await waitFor(() => streams[3].received.length === 1);
			await waitForResolved(() => expect(directlyDialded).toBeTrue());
		}); */
	});

	describe("re-route", () => {
		beforeEach(async () => {
			session = await disconnected(4, [
				{
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215
						]),
						Ed25519Keypair
					).toPeerId(),
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: false,
								seekTimeout: 5000
							})
					}
				},
				{
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15
						]),
						Ed25519Keypair
					).toPeerId(),
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: false,
								seekTimeout: 5000
							})
					}
				},
				{
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242
						]),
						Ed25519Keypair
					).toPeerId(),
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: false,
								seekTimeout: 5000
							})
					}
				},
				{
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 89, 189, 223, 17, 89, 221, 173, 81, 113, 69, 226, 180, 190,
							119, 201, 16, 59, 208, 95, 19, 142, 231, 71, 166, 43, 90, 10, 250,
							109, 68, 89, 118, 0, 27, 51, 234, 79, 160, 31, 81, 189, 54, 105,
							205, 202, 34, 30, 101, 16, 64, 52, 113, 222, 160, 31, 73, 148,
							161, 240, 201, 36, 71, 121, 134, 83
						]),
						Ed25519Keypair
					).toPeerId(),
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: false,
								seekTimeout: 5000
							})
					}
				}
			]);

			streams = [];
			for (const peer of session.peers) {
				streams.push(createMetrics(peer.services.directstream));
			}
		});

		afterEach(async () => {
			await session.stop();
		});

		it("re-route new connection", async () => {
			// line topology
			await connectLine(session);

			await streams[0].stream.publish(new Uint8Array(0), {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});
			expect(
				streams[0].stream.routes
					.findNeighbor(
						streams[0].stream.publicKeyHash,
						streams[3].stream.publicKeyHash
					)
					?.list?.map((x) => x.hash)
			).toEqual([streams[1].stream.publicKeyHash]);

			/* 					
			┌───┐ 
			│0  │ 
			└┬─┬┘ 
			│┌▽┐ 
			││1│ 
			│└┬┘ 
			│┌▽─┐
			││2 │
			│└┬─┘
			┌▽─▽┐ 
			│3  │ 
			└───┘ 
			*/

			await session.connect([[session.peers[0], session.peers[3]]]);
			await waitForPeerStreams(streams[0].stream, streams[3].stream);

			await streams[0].stream.publish(new Uint8Array(0), {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});

			await waitForResolved(() => {
				expect(
					streams[0].stream.routes
						.findNeighbor(
							streams[0].stream.publicKeyHash,
							streams[3].stream.publicKeyHash
						)
						?.list?.map((x) => {
							return { hash: x.hash, distance: x.distance };
						})
				).toEqual([
					{ distance: -1, hash: streams[3].stream.publicKeyHash },
					{ distance: 0, hash: streams[1].stream.publicKeyHash },
					{ distance: 1, hash: streams[1].stream.publicKeyHash }
				]);
			});
		});

		it("neighbour drop", async () => {
			await connectLine(session);

			await streams[0].stream.publish(new Uint8Array(0), {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});
			expect(
				streams[0].stream.routes
					.findNeighbor(
						streams[0].stream.publicKeyHash,
						streams[3].stream.publicKeyHash
					)
					?.list?.map((x) => x.hash)
			).toEqual([streams[1].stream.publicKeyHash]);

			await session.peers[1].stop();

			await waitForResolved(() =>
				expect(
					streams[0].unrechable.map((x) => x.hashcode())
				).toContainAllValues([
					streams[1].stream.publicKeyHash,
					streams[3].stream.publicKeyHash
				])
			);
		});

		it("neighbour drop but maybe reachable", async () => {
			// V shape
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForPeerStreams(streams[0].stream, streams[1].stream);
			await session.connect([[session.peers[0], session.peers[2]]]);
			await waitForPeerStreams(streams[0].stream, streams[2].stream);

			await streams[0].stream.publish(new Uint8Array(0), {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[1].stream.publicKeyHash]
				})
			});
			expect(
				streams[0].stream.routes
					.findNeighbor(
						streams[0].stream.publicKeyHash,
						streams[1].stream.publicKeyHash
					)
					?.list?.map((x) => x.hash)
			).toEqual([streams[1].stream.publicKeyHash]);

			const seekTimeout = 2e3;
			streams[0].stream.seekTimeout = seekTimeout;
			await session.peers[1].stop();

			// will immediately become unreachable
			await waitForResolved(() =>
				expect(
					streams[0].unrechable.map((x) => x.hashcode())
				).toContainAllValues([streams[1].stream.publicKeyHash])
			);
		});

		it("re-seeks on connection drop", async () => {
			/* 					
			┌───┐ 
			│0  │ 
			└┬─┬┘ 
			│┌▽┐ 
			││1│ 
			│└┬┘ 
			│┌▽─┐
			││2 │
			│└┬─┘
			┌▽─▽┐ 
			│3  │ 
			└───┘ 
			*/

			await connectLine(session);
			await session.connect([[session.peers[0], session.peers[3]]]);
			await waitForPeerStreams(streams[0].stream, streams[3].stream);

			await streams[0].stream.publish(new Uint8Array([123]), {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});
			expect(
				streams[0].stream.routes
					.findNeighbor(
						streams[0].stream.publicKeyHash,
						streams[3].stream.publicKeyHash
					)
					?.list?.map((x) => x.hash)
			).toContainValues([
				streams[1].stream.publicKeyHash,
				streams[3].stream.publicKeyHash
			]);

			expect(streams[3].received).toHaveLength(1);

			streams[0].unrechable = [];
			streams[0].reachable = [];

			await session.peers[0].hangUp(session.peers[3].peerId);

			await waitForResolved(() => {
				expect(
					streams[0].stream.routes
						.findNeighbor(
							streams[0].stream.publicKeyHash,
							streams[3].stream.publicKeyHash
						)
						?.list?.map((x) => x.hash)
				).toEqual([streams[1].stream.publicKeyHash]);
			});

			// will emit unreachable and reachable events (again)
			expect(streams[0].unrechable.map((x) => x.hashcode())).toContainAllValues(
				[streams[3].stream.publicKeyHash]
			);
			expect(streams[0].reachable.map((x) => x.hashcode())).toContainAllValues([
				streams[3].stream.publicKeyHash
			]);

			// the route should now be the long route
			await streams[0].stream.publish(new Uint8Array([234]), {
				mode: new SilentDelivery({
					redundancy: 1,
					to: [streams[3].stream.publicKeyHash]
				})
			});

			await waitForResolved(() => expect(streams[3].received).toHaveLength(2));
			expect(streams[3].received[1].header.mode).toBeInstanceOf(SilentDelivery);
		});

		it("distant drop", async () => {
			// line topology
			await connectLine(session);

			expect(
				streams[0].stream.routes
					.findNeighbor(
						streams[0].stream.publicKeyHash,
						streams[3].stream.publicKeyHash
					)
					?.list?.map((x) => x.hash)
			).toBeUndefined();
			await streams[0].stream.publish(new Uint8Array(0), {
				mode: new SeekDelivery({
					redundancy: 2,
					to: [streams[3].stream.publicKeyHash]
				})
			});
			expect(
				streams[0].stream.routes
					.findNeighbor(
						streams[0].stream.publicKeyHash,
						streams[3].stream.publicKeyHash
					)
					?.list?.map((x) => x.hash)
			).toEqual([streams[1].stream.publicKeyHash]);

			await waitForResolved(() =>
				expect(streams[0].reachable.map((x) => x.hashcode())).toEqual([
					streams[1].stream.publicKeyHash,
					streams[3].stream.publicKeyHash
				])
			);
			await waitForResolved(() =>
				expect(streams[1].reachable.map((x) => x.hashcode())).toEqual([
					streams[0].stream.publicKeyHash,
					streams[2].stream.publicKeyHash
				])
			);

			await session.peers[3].stop();

			await waitForResolved(
				() =>
					expect(streams[0].unrechable.map((x) => x.hashcode())).toEqual([
						streams[3].stream.publicKeyHash
					]),
				{ timeout: 20 * 1000 }
			);
			expect(streams[1].unrechable.map((x) => x.hashcode())).toEqual([]); // because node 3 was never "reachable" directly from 2, just as a relay

			expect(
				streams[0].stream.routes.isReachable(
					streams[0].stream.publicKeyHash,
					streams[3].stream.publicKeyHash
				)
			).toEqual(false);

			expect(
				streams[0].stream.routes.findNeighbor(
					streams[0].stream.publicKeyHash,
					streams[3].stream.publicKeyHash
				)
			).toBeUndefined();
		});
	});

	describe("invalidation", () => {
		let extraSession: TestSessionStream;
		beforeEach(async () => {});
		afterEach(async () => {
			await session?.stop();
			await extraSession?.stop();
		});

		it("will not get blocked for slow writes", async () => {
			session = await connected(3);

			for (let i = 0; i < session.peers.length; i++) {
				await waitForResolved(() =>
					expect(session.peers[i].services.directstream.routes.count()).toEqual(
						2
					)
				);
			}
			let slowPeer = [1, 2];
			let fastPeer = [2, 1];
			let seekDelivery = [true, false];

			for (let i = 0; i < slowPeer.length; i++) {
				// reset routes
				await session.peers[0].services.directstream.publish(
					new Uint8Array([1, 2, 3]),
					{
						mode: new SeekDelivery({
							redundancy: 1,
							to: [
								session.peers[0].services.directstream.publicKeyHash,
								session.peers[1].services.directstream.publicKeyHash
							]
						}) // undefined ?
					}
				);

				const slow = session.peers[0].services.directstream.peers.get(
					session.peers[slowPeer[i]].services.directstream.publicKeyHash
				)!;
				const fast = session.peers[0].services.directstream.peers.get(
					session.peers[fastPeer[i]].services.directstream.publicKeyHash
				)!;

				expect(slow).toBeDefined();
				const waitForWriteDefaultFn = slow.waitForWrite.bind(slow);

				let abortController = new AbortController();
				slow.waitForWrite = async (bytes) => {
					try {
						await delay(3000, { signal: abortController.signal });
					} catch (error) {
						return;
					}
					return waitForWriteDefaultFn(bytes);
				};

				const t0 = +new Date();
				let t1: number | undefined = undefined;

				let listener = () => {
					t1 = +new Date();
				};
				session.peers[fastPeer[i]].services.directstream.addEventListener(
					"data",
					listener
				);

				const p = session.peers[0].services.directstream.publish(
					new Uint8Array([1, 2, 3]),
					{
						mode: seekDelivery[i]
							? new SeekDelivery({
									redundancy: 1,
									to: [slow.publicKey, fast.publicKey]
								})
							: new SilentDelivery({
									redundancy: 1,
									to: [slow.publicKey, fast.publicKey]
								}) // undefined ?
					}
				);

				await waitForResolved(() => expect(t1).toBeDefined());
				expect(t1! - t0).toBeLessThan(3000);

				// reset
				abortController.abort();
				slow.waitForWrite = waitForWriteDefaultFn;
				session.peers[fastPeer[i]].services.directstream.removeEventListener(
					"data",
					listener
				);
				await p;
			}
		});
		it("reset route info on rejoiing peers", async () => {
			session = await disconnected(4);
			let streams = collectMetrics(session);

			await connectLine(session);

			await session.peers[0].services.directstream.publish(
				new Uint8Array([0]),
				{
					mode: new SeekDelivery({
						redundancy: 1,
						to: [session.peers[3].services.directstream.publicKeyHash]
					})
				}
			);

			await waitForResolved(() => expect(streams[2].ack).toHaveLength(1));
			await waitForResolved(() => expect(streams[3].received).toHaveLength(1));
			await waitForResolved(() =>
				expect(
					session.peers[2].services.directstream.routes.countAll()
				).toEqual(3)
			);
			expect(streams[0].reachable.map((x) => x.hashcode())).toEqual([
				streams[1].stream.publicKeyHash,
				streams[3].stream.publicKeyHash
			]);

			expect(streams[0].session.map((x) => x.hashcode())).toEqual([
				streams[1].stream.publicKeyHash,
				//	streams[2].stream.publicKeyHash, peer 2 will never emit any messages
				streams[3].stream.publicKeyHash
			]);

			await session.peers[3].stop();

			await waitForResolved(() => expect(streams[0].goodbye).toHaveLength(1));
			await session.peers[3].start();
			await session.connect([[session.peers[3], session.peers[2]]]);
			await waitForResolved(() =>
				expect(session.peers[2].services.directstream.peers.size).toEqual(2)
			);
			streams[0].reachable = [];

			await session.peers[0].services.directstream.publish(
				new Uint8Array([0]),
				{
					mode: new SilentDelivery({
						redundancy: 1,
						to: [session.peers[3].services.directstream.publicKeyHash]
					})
				}
			);

			await waitForResolved(() => expect(streams[3].received).toHaveLength(2));
			await waitForResolved(() =>
				expect(streams[0].session.map((x) => x.hashcode())).toEqual([
					streams[1].stream.publicKeyHash,
					streams[3].stream.publicKeyHash,
					streams[2].stream.publicKeyHash, // sent us a goodbye message for node 3
					streams[3].stream.publicKeyHash // the new session (the restart)
				])
			);
			expect(streams[0].unrechable.length).toEqual(0);
			expect(streams[0].reachable.length).toEqual(0);
		});
	});

	describe("prioritization", () => {
		afterEach(async () => {
			await session?.stop();
		});

		// https://www.youtube.com/watch?v=kdv_4RHAatQ
		it("seeking in fast lane", async () => {
			session = await disconnected(3, {
				// we use yamux since mplex does not support backpressure. This means that mplex would consume all outbound messages even though mplex can't handle them :(
				streamMuxers: [yamux()],
				services: {
					directstream: (c) =>
						new TestDirectStream(c, {
							connectionManager: { dialer: false, pruner: false }
						})
				}
			});
			await connectLine(session);
			await session.peers[0].services.directstream.publish(new Uint8Array(0), {
				to: [session.peers[2].peerId]
			});
			const stream = session.peers[1].services.directstream.peers.get(
				session.peers[2].services.directstream.publicKeyHash
			)!.rawOutboundStream as YamuxStream;
			const sendFn = stream.sendData.bind(stream);
			const abortContoller = new AbortController();

			/**
			 * introduce lag in the relay
			 */
			let lag = 300;
			stream.sendData = async (data) => {
				await delay(lag, { signal: abortContoller.signal });
				return sendFn(data);
			};

			const metric = createMetrics(session.peers[2].services.directstream);

			let t0 = +new Date();

			/**
			 * Send a bunch of data (potentially clog the really)
			 */
			let dataMessageCount = 100;
			for (let i = 0; i < dataMessageCount; i++) {
				await session.peers[0].services.directstream.publish(randomBytes(1e2), {
					mode: new SilentDelivery({
						to: [session.peers[2].peerId],
						redundancy: 1
					})
				});
			}

			const seekMessageData = randomBytes(1e3);

			// Send a seek that we really need to get through in time
			await session.peers[0].services.directstream.publish(seekMessageData, {
				mode: new SeekDelivery({ to: [session.peers[2].peerId], redundancy: 1 })
			});

			let t1 = +new Date();
			expect(t1 - t0).toBeLessThan(5000); // it would have taken lag * dataMessageCount time if we to wait for seek ack if we didn't prioritize it
			expect(
				metric.received.findIndex(
					(x) => x.data && equals(x.data, seekMessageData)
				)
			).toBeLessThan(dataMessageCount / 10); // if no prioritization this would be at index dataMessageCount
			abortContoller.abort(new Error("Done"));
		});
	});
});

describe("start/stop", () => {
	let session: TestSessionStream;

	afterEach(async () => {
		await session.stop();
	});

	it("can restart", async () => {
		session = await connected(2, {
			transports: [tcp(), webSockets({ filter: filters.all })],
			services: {
				directstream: (c) => new TestDirectStream(c)
			}
		}); // use 2 transports as this might cause issues if code is not handling multiple connections correctly
		await waitForPeerStreams(stream(session, 0), stream(session, 1));

		/* await waitFor(() => stream(session, 1).helloMap.size == 1); */
		await stream(session, 0).stop();
		/* await waitFor(() => stream(session, 1).helloMap.size === 0); */

		await stream(session, 1).stop();
		expect(stream(session, 0).peers.size).toEqual(0);
		await delay(3000);
		await stream(session, 0).start();
		/* expect(stream(session, 0).helloMap.size).toEqual(0); */
		await stream(session, 1).start();

		await waitFor(() => stream(session, 0).peers.size === 1);
		/* 	await waitFor(() => stream(session, 0).helloMap.size === 1);
			await waitFor(() => stream(session, 1).helloMap.size === 1); */
		await waitForPeerStreams(stream(session, 0), stream(session, 1));
	});
	it("can connect after start", async () => {
		session = await disconnected(2, {
			transports: [tcp(), webSockets({ filter: filters.all })],
			services: {
				directstream: (c) => new TestDirectStream(c)
			}
		});

		await session.connect();
		await waitForPeerStreams(stream(session, 0), stream(session, 1));
	});

	it("can connect before start", async () => {
		session = await connected(2, {
			transports: [tcp(), webSockets({ filter: filters.all })],
			services: {
				directstream: (c) => new TestDirectStream(c)
			}
		});
		await delay(3000);

		await stream(session, 0).start();
		await stream(session, 1).start();

		await waitForPeerStreams(stream(session, 0), stream(session, 1));
	});

	it("can connect with delay", async () => {
		session = await connected(2, {
			transports: [tcp(), webSockets({ filter: filters.all })],
			services: {
				directstream: (c) => new TestDirectStream(c)
			}
		});
		await waitForPeerStreams(stream(session, 0), stream(session, 1));
		await session.peers[0].services.directstream.stop();
		await session.peers[1].services.directstream.stop();
		await waitFor(
			() => session.peers[0].services.directstream.peers.size === 0
		);
		await waitFor(
			() => session.peers[1].services.directstream.peers.size === 0
		);
		await session.peers[1].services.directstream.start();
		await delay(3000);
		await session.peers[0].services.directstream.start();
		await waitForPeerStreams(stream(session, 0), stream(session, 1));
	});

	it("one peer can restart line", async () => {
		session = await disconnected(2, {
			transports: [tcp() /* , webSockets({ filter: filters.all }) */],
			services: {
				directstream: (c) => new TestDirectStream(c)
			}
		});
		await connectLine(session);
		await delay(2000);
		await session.peers[0].services.directstream.publish(new Uint8Array(0), {
			mode: new SeekDelivery({
				redundancy: 2,
				to: [session.peers[1].services.directstream.publicKeyHash]
			})
		});
		/* 	session.peers[0].services.directstream.peers.get(session.peers[1].services.directstream.publicKeyHash)?.write(new Uint8Array(0))
			session.peers[1].services.directstream.peers.get(session.peers[0].services.directstream.publicKeyHash)?.write(new Uint8Array(0))
	 */
		await session.peers[1].stop();

		await session.peers[1].start();

		await session.connect([[session.peers[1], session.peers[0]]]);
		await waitForPeerStreams(stream(session, 0), stream(session, 1));
		await waitForPeerStreams(stream(session, 1), stream(session, 0));

		/* 	await delay(13000)
			await waitForPeerStreams(stream(session, 0), stream(session, 1)); */
	});

	it("wait for only waits for reachable", async () => {
		session = await disconnected(3, {
			transports: [tcp()],
			services: {
				directstream: (c) =>
					new TestDirectStream(c, {
						connectionManager: { dialer: false, pruner: false }
					})
			}
		});

		await session.connect([
			// behaviour seems to be more predictable if we connect after start (TODO improve startup to use existing connections in a better way)
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]]
		]);
		await waitForPeerStreams(stream(session, 0), stream(session, 1));
		await waitForPeerStreams(stream(session, 1), stream(session, 2));

		expect(
			session.peers[0].services.directstream.routes.isReachable(
				session.peers[0].services.directstream.publicKey.hashcode(),
				session.peers[2].services.directstream.publicKey.hashcode()
			)
		).toBeFalse();
		await session.peers[0].services.directstream.publish(new Uint8Array([0]), {
			mode: new SeekDelivery({ redundancy: 1 })
		});
		await session.peers[0].services.directstream.waitFor(
			session.peers[2].peerId
		);
		await expect(
			session.peers[0].services.directstream.waitFor(session.peers[2].peerId, {
				neighbour: true,
				timeout: 1000
			})
		).rejects.toThrow();

		await expect(
			session.peers[0].services.directstream.waitFor(session.peers[1].peerId, {
				neighbour: true,
				timeout: 1000
			})
		);
	});

	it("start and stop", async () => {
		session = await disconnected(2);
		await session.connect([[session.peers[0], session.peers[1]]]);
		await delay(1000); /// TODO remove when https://github.com/ChainSafe/js-libp2p-yamux/issues/72 fixed
		await session.peers[0].stop();
		await session.peers[0].start();
	});
});

describe("multistream", () => {
	let session: TestSessionStream;
	beforeEach(async () => {
		session = await TestSession.connected(2, {
			transports: [tcp(), webSockets({ filter: filters.all })],
			services: {
				directstream: (c) => new TestDirectStream(c),
				directstream2: (c) =>
					new TestDirectStream(c, { id: "another-protocol" })
			}
		}); // use 2 transports as this might cause issues if code is not handling multiple connections correctly
	});

	afterEach(async () => {
		await session.stop();
	});

	it("can setup multiple streams at once", async () => {
		await waitFor(() => !!stream(session, 0).peers.size);
		await waitFor(() => !!stream(session, 1).peers.size);
		await waitFor(() => !!service(session, 0, "directstream2").peers.size);
		await waitFor(() => !!service(session, 1, "directstream2").peers.size);
	});
});
