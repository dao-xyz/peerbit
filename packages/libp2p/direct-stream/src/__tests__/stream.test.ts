import { LSession } from "@dao-xyz/libp2p-test-utils";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import crypto from "crypto";
import { waitForPeers, DirectStream, ConnectionManagerOptions } from "..";
import { Libp2p } from "libp2p";
import { DataMessage, Message } from "../messages";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { PeerId, isPeerId } from "@libp2p/interface-peer-id";
import { Multiaddr } from "@multiformats/multiaddr";
import { multiaddr } from "@multiformats/multiaddr";
import { tcp } from "@libp2p/tcp";

class TestStreamImpl extends DirectStream {
	constructor(
		libp2p: Libp2p,
		options: {
			id?: string;
			pingInterval?: number;
			connectionManager?: ConnectionManagerOptions;
		} = {}
	) {
		super(libp2p, [options.id || "test/0.0.0"], {
			canRelayMessage: true,
			emitSelf: true,
			connectionManager: options.connectionManager || {
				autoDial: false,
			},
			...options,
		});
	}
}

describe("streams", function () {
	describe("ping", () => {
		let session: LSession, streams: TestStreamImpl[];

		afterEach(async () => {
			streams && (await Promise.all(streams.map((s) => s.stop())));
			await session?.stop();
		});

		it("2-ping", async () => {
			// 0 and 2 not connected
			session = await LSession.connected(2);

			streams = session.peers.map((x) => new TestStreamImpl(x));
			await Promise.all(streams.map((x) => x.start()));

			await waitForPeers(...streams);

			// Pings can be aborted, by the interval pinging, so we just need to check that eventually we get results
			await streams[0].ping(streams[0].peers.get(streams[1].publicKeyHash)!);
			await waitFor(
				() =>
					streams[0].peers.get(streams[1].publicKeyHash)?.pingLatency! < 1000
			);
		});

		it("4-ping", async () => {
			// 0 and 2 not connected
			session = await LSession.connected(4, { transports: [tcp()] }); // TODO github CI fails we do both websocket and tcp here (some CPU limit?)

			streams = session.peers.map((x) => new TestStreamImpl(x));
			await Promise.all(streams.map((x) => x.start()));

			await waitForPeers(...streams);

			// Pings can be aborted, by the interval pinging, so we just need to check that eventually we get results
			await streams[0].ping(streams[0].peers.get(streams[1].publicKeyHash)!);
			await waitFor(
				() =>
					streams[0].peers.get(streams[1].publicKeyHash)?.pingLatency! < 1000
			);
		});

		it("ping interval", async () => {
			// 0 and 2 not connected
			session = await LSession.connected(2);

			const pingInterval = 1000;
			streams = session.peers.map(
				(x) => new TestStreamImpl(x, { pingInterval })
			);
			await Promise.all(streams.map((x) => x.start()));

			await waitForPeers(...streams);

			let counter = 0;
			const pingFn = streams[0].onPing.bind(streams[0]);
			streams[0].onPing = (a, b, c) => {
				counter += 1;
				return pingFn(a, b, c);
			};
			await waitFor(() => counter > 5);
		});
	});

	describe("publish", () => {
		let session: LSession;
		let peers: {
			stream: TestStreamImpl;
			messages: Message[];
			recieved: DataMessage[];
			reachable: PublicSignKey[];
			unrechable: PublicSignKey[];
		}[];
		const data = new Uint8Array([1, 2, 3]);

		beforeAll(async () => {});

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await LSession.disconnected(4);

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

			peers = [];
			for (const peer of session.peers) {
				const stream = new TestStreamImpl(peer);
				const client: {
					stream: TestStreamImpl;
					messages: Message[];
					recieved: DataMessage[];
					reachable: PublicSignKey[];
					unrechable: PublicSignKey[];
				} = {
					messages: [],
					recieved: [],
					reachable: [],
					unrechable: [],
					stream,
				};
				peers.push(client);
				stream.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				stream.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				stream.addEventListener("peer:reachable", (msg) => {
					client.reachable.push(msg.detail);
				});
				stream.addEventListener("peer:unreachable", (msg) => {
					client.unrechable.push(msg.detail);
				});
				await stream.start();
			}
			await session.connect([
				// behaviour seems to be more predictable if we connect after start (TODO improve startup to use existing connections in a better way)
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[2], session.peers[3]],
			]);

			await waitForPeers(peers[0].stream, peers[1].stream);
			await waitForPeers(peers[1].stream, peers[2].stream);
			await waitForPeers(peers[2].stream, peers[3].stream);
		});

		afterEach(async () => {
			await Promise.all(peers.map((peer) => peer.stream.stop()));
			await session.stop();
		});

		afterAll(async () => {});

		it("many", async () => {
			let iterations = 300;

			for (let i = 0; i < iterations; i++) {
				const small = crypto.randomBytes(1e3); // 1kb
				peers[0].stream.publish(small);
			}
			await waitFor(() => peers[2].recieved.length === iterations, {
				delayInterval: 300,
				timeout: 30 * 1000,
			});
		});

		it("1->unknown", async () => {
			await peers[0].stream.publish(data);
			await waitFor(() => peers[1].recieved.length === 1);
			expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(1);
			expect(peers[2].recieved).toHaveLength(1);
		});

		it("1->2", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[1].stream.libp2p.peerId],
			});
			await waitFor(() => peers[1].recieved.length === 1);
			expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(1);
			expect(peers[2].recieved).toHaveLength(0);
		});

		it("1->3", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[2].stream.libp2p.peerId],
			});
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(peers[2].recieved).toHaveLength(1);
			expect(peers[1].recieved).toHaveLength(0);
		});

		it("1->3 10mb data", async () => {
			const bigData = crypto.randomBytes(1e7);
			await peers[0].stream.publish(bigData, {
				to: [peers[2].stream.libp2p.peerId],
			});
			await waitFor(() => peers[2].recieved.length === 1, {
				delayInterval: 10,
				timeout: 10 * 1000,
			});
			expect(new Uint8Array(peers[2].recieved[0].data)).toHaveLength(
				bigData.length
			);
			expect(peers[2].recieved).toHaveLength(1);
			expect(peers[1].recieved).toHaveLength(0);
		});
		it("publishes on direct stream, even path is longer", async () => {
			await session.connect([[session.peers[0], session.peers[2]]]);
			await waitForPeers(peers[0].stream, peers[2].stream);

			// make path 1->3 longest, to make sure we send over it directly anyways because it is a direct path
			peers[0].stream.routes.graph.setEdgeAttribute(
				peers[0].stream.routes.getLink(
					peers[0].stream.publicKeyHash,
					peers[2].stream.publicKeyHash
				),
				"weight",
				1e5
			);
			await peers[0].stream.publish(crypto.randomBytes(1e2), {
				to: [peers[2].stream.libp2p.peerId],
			});
			peers[1].messages = [];
			await waitFor(() => peers[2].recieved.length === 1, {
				delayInterval: 10,
				timeout: 10 * 1000,
			});
			expect(
				peers[1].messages.filter((x) => x instanceof DataMessage)
			).toHaveLength(0);
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

			await waitForPeers(peers[0].stream, peers[2].stream);
			const defaultEdgeWeightFnPeer0 =
				peers[0].stream.routes.graph.getEdgeAttribute.bind(
					peers[0].stream.routes.graph
				);

			// make path long
			peers[0].stream.routes.graph.getEdgeAttribute = (
				edge: unknown,
				name: any
			) => {
				if (edge === link) {
					return 1e5;
				}
				return defaultEdgeWeightFnPeer0(edge, name);
			};

			await peers[0].stream.publish(crypto.randomBytes(1e2), {
				to: [peers[3].stream.libp2p.peerId],
			});

			peers[1].messages = [];

			await waitFor(
				() =>
					peers[1].messages.filter((x) => x instanceof DataMessage).length === 1
			); // will send through peer [1] since path [0] -> [2] -> [3] directly is currently longer
			await waitFor(() => peers[3].recieved.length === 1);

			peers[1].messages = [];

			// Make [0] -> [2] path short
			let link = peers[0].stream.routes.getLink(
				peers[0].stream.publicKeyHash,
				peers[2].stream.publicKeyHash
			);
			peers[0].stream.routes.graph.getEdgeAttribute = (
				edge: unknown,
				name: any
			) => {
				if (edge === link) {
					return 0;
				}
				return defaultEdgeWeightFnPeer0(edge, name);
			};

			expect(
				peers[0].stream.routes.getPath(
					peers[0].stream.publicKeyHash,
					peers[2].stream.publicKeyHash
				).length
			).toEqual(2);
			await peers[0].stream.publish(crypto.randomBytes(1e2), {
				to: [peers[3].stream.libp2p.peerId],
			});
			await waitFor(() => peers[3].recieved.length === 1);
			const messages = peers[1].messages.filter(
				(x) => x instanceof DataMessage
			);
			expect(messages).toHaveLength(0); // no new messages for peer 1, because sending 0 -> 2 -> 3 directly is now faster
			expect(peers[1].recieved).toHaveLength(0);
		});
	});

	// TODO test that messages are not sent backward, triangles etc

	describe("join/leave", () => {
		let session: LSession;
		let peers: {
			stream: TestStreamImpl;
			messages: Message[];
			recieved: DataMessage[];
			reachable: PublicSignKey[];
			unrechable: PublicSignKey[];
		}[];
		const data = new Uint8Array([1, 2, 3]);
		let autoDialRetryDelay = 5 * 1000;

		describe("direct connections", () => {
			beforeEach(async () => {
				session = await LSession.disconnected(4, [
					{ browser: true },
					{},
					{},
					{ browser: true },
				]); // Second arg is due to https://github.com/libp2p/js-libp2p/issues/1690
				peers = [];
				for (const [i, peer] of session.peers.entries()) {
					const stream = new TestStreamImpl(peer, {
						connectionManager: {
							autoDial: i === 0,
							retryDelay: autoDialRetryDelay,
						},
					});

					if (i === 0) {
						expect(stream["connectionManagerOptions"].autoDial).toBeTrue();
					}

					const client: {
						stream: TestStreamImpl;
						messages: Message[];
						recieved: DataMessage[];
						reachable: PublicSignKey[];
						unrechable: PublicSignKey[];
					} = {
						messages: [],
						recieved: [],
						reachable: [],
						unrechable: [],
						stream,
					};
					peers.push(client);
					stream.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					stream.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});
					stream.addEventListener("peer:reachable", (msg) => {
						client.reachable.push(msg.detail);
					});
					stream.addEventListener("peer:unreachable", (msg) => {
						client.unrechable.push(msg.detail);
					});
					await stream.start();
				}

				// slowly connect to that the route maps are deterministic
				await session.connect([[session.peers[0], session.peers[1]]]);
				await waitFor(() => peers[0].stream.routes.linksCount === 1);
				await waitFor(() => peers[1].stream.routes.linksCount === 1);
				await session.connect([[session.peers[1], session.peers[2]]]);
				await waitFor(() => peers[0].stream.routes.linksCount === 2);
				await waitFor(() => peers[1].stream.routes.linksCount === 2);
				await session.connect([[session.peers[2], session.peers[3]]]);
				await waitFor(() => peers[0].stream.routes.linksCount === 3);
				await waitFor(() => peers[1].stream.routes.linksCount === 3);
				await waitFor(() => peers[2].stream.routes.linksCount === 3);
				await waitForPeers(peers[0].stream, peers[1].stream);
				await waitForPeers(peers[1].stream, peers[2].stream);
				await waitForPeers(peers[2].stream, peers[3].stream);

				for (const peer of peers) {
					expect(peer.reachable.map((x) => x.hashcode())).toContainAllValues(
						peers
							.map((x) => x.stream.publicKeyHash)
							.filter((x) => x !== peer.stream.publicKeyHash)
					); // peer has recevied reachable event from everone
				}
			});

			afterEach(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
			});

			it("directly if possible", async () => {
				let dials = 0;
				const dialFn = peers[0].stream.libp2p.dial.bind(peers[0].stream.libp2p);
				peers[0].stream.libp2p.dial = (a, b) => {
					dials += 1;
					return dialFn(a, b);
				};

				peers[3].recieved = [];
				expect(peers[0].stream.peers.size).toEqual(1);

				await peers[0].stream.publish(data, {
					to: [peers[3].stream.libp2p.peerId],
				});

				await waitFor(() => peers[3].recieved.length === 1);
				expect(
					peers[3].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				// Dialing will yield a new connection
				await waitFor(() => peers[0].stream.peers.size === 2);
				expect(dials).toEqual(1);

				// Republishing will not result in an additional dial
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.libp2p.peerId],
				});
				await waitFor(() => peers[3].recieved.length === 2);
				expect(dials).toEqual(1);
				expect(peers[0].stream.peers.size).toEqual(2);
				expect(
					peers[0].stream.peers.has(peers[3].stream.publicKeyHash)
				).toBeTrue();
				expect(
					peers[0].stream.peers.has(peers[1].stream.publicKeyHash)
				).toBeTrue();
			});

			it("retry dial after a while", async () => {
				let dials: (PeerId | Multiaddr | Multiaddr[])[] = [];
				peers[0].stream.libp2p.dial = (a, b) => {
					dials.push(a);
					throw new Error("Mock Error");
				};

				peers[3].recieved = [];
				expect(peers[0].stream.peers.size).toEqual(1);

				await peers[0].stream.publish(data, {
					to: [peers[3].stream.libp2p.peerId],
				});

				await waitFor(() => peers[3].recieved.length === 1);
				expect(
					peers[3].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				// Dialing will yield a new connection
				await waitFor(() => peers[0].stream.peers.size === 1);
				let expectedDialsCount =
					1 + peers[2].stream.libp2p.getMultiaddrs().length; // 1 dial directly, X dials through neighbour as relay
				expect(dials).toHaveLength(expectedDialsCount);

				// Republishing will not result in an additional dial
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.libp2p.peerId],
				});
				let t1 = +new Date();
				expect(dials).toHaveLength(expectedDialsCount); // No change, because TTL > autoDialRetryTimeout

				await waitFor(() => peers[3].recieved.length === 2);
				await waitFor(() => +new Date() - t1 > autoDialRetryDelay);

				// Try again, now expect another dial call, since the retry interval has been reached
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.libp2p.peerId],
				});
				expect(dials).toHaveLength(expectedDialsCount * 2); // 1 dial directly, X dials through neighbour as relay
			});

			it("through relay if fails", async () => {
				const dialFn = peers[0].stream.libp2p.dial.bind(peers[0].stream.libp2p);
				const filteredDial = (address: PeerId | Multiaddr | Multiaddr[]) => {
					if (
						isPeerId(address) &&
						address.toString() === peers[3].stream.peerIdStr
					) {
						throw new Error("Mock fail"); // don't allow connect directly
					}

					let addresses: Multiaddr[] = Array.isArray(address)
						? address
						: [address as Multiaddr];
					for (const a of addresses) {
						if (
							!a.protoNames().includes("p2p-circuit") &&
							a.toString().includes(peers[3].stream.peerIdStr)
						) {
							throw new Error("Mock fail"); // don't allow connect directly
						}
					}
					addresses = addresses.map((x) =>
						x.protoCodes().includes(281)
							? multiaddr(x.toString().replace("/webrtc/", "/"))
							: x
					); // TODO we can't seem to dial webrtc addresses directly in a Node env (?)
					return dialFn(addresses);
				};

				peers[0].stream.libp2p.dial = filteredDial;
				expect(peers[0].stream.peers.size).toEqual(1);
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.libp2p.peerId],
				});
				await waitFor(() => peers[3].recieved.length === 1);
				await waitFor(() => peers[0].stream.peers.size === 3); // 1 originally + 1 the relay + 1 the forwarded connecction to the peer
			});
		});

		describe("4", () => {
			beforeEach(async () => {
				session = await LSession.disconnected(4);

				/* 
				┌─┐
				│3│
				└┬┘
				┌▽┐
				│0│
				└┬┘
				┌▽┐
				│1│
				└┬┘
				┌▽┐
				│2│
				└─┘
				
				 */

				peers = [];
				for (const peer of session.peers) {
					const stream = new TestStreamImpl(peer, {
						connectionManager: { autoDial: false },
					});
					const client: {
						stream: TestStreamImpl;
						messages: Message[];
						recieved: DataMessage[];
						reachable: PublicSignKey[];
						unrechable: PublicSignKey[];
					} = {
						messages: [],
						recieved: [],
						reachable: [],
						unrechable: [],
						stream,
					};
					peers.push(client);
					stream.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					stream.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});
					stream.addEventListener("peer:reachable", (msg) => {
						client.reachable.push(msg.detail);
					});
					stream.addEventListener("peer:unreachable", (msg) => {
						client.unrechable.push(msg.detail);
					});
					await stream.start();
				}

				// slowly connect to that the route maps are deterministic
				try {
					await session.connect([[session.peers[0], session.peers[1]]]);
					await waitFor(() => peers[0].stream.routes.linksCount === 1);
					await waitFor(() => peers[1].stream.routes.linksCount === 1);
					await session.connect([[session.peers[1], session.peers[2]]]);
					await waitFor(() => peers[0].stream.routes.linksCount === 2);
					await waitFor(() => peers[1].stream.routes.linksCount === 2);
					await waitFor(() => peers[2].stream.routes.linksCount === 2);
					await session.connect([[session.peers[0], session.peers[3]]]);
					await waitFor(() => peers[0].stream.routes.linksCount === 3);
					await waitFor(() => peers[1].stream.routes.linksCount === 3);
					await waitFor(() => peers[2].stream.routes.linksCount === 3);
					await waitFor(() => peers[3].stream.routes.linksCount === 3);
					await waitForPeers(peers[0].stream, peers[1].stream);
					await waitForPeers(peers[1].stream, peers[2].stream);
					await waitForPeers(peers[0].stream, peers[3].stream);
				} catch (error) {
					console.log(
						[peers.map((x) => x.stream.peerIdStr)],
						[...peers[0].stream.multiaddrsMap.values()],
						peers[0].stream.routes.linksCount,
						peers[1].stream.routes.linksCount,
						peers[2].stream.routes.linksCount,
						peers[3].stream.routes.linksCount
					);
					console.log([...peers[0].stream.multiaddrsMap.values()]);
					throw error;
				}
				for (const peer of peers) {
					await waitFor(() => peer.reachable.length === 3);
					expect(peer.reachable.map((x) => x.hashcode())).toContainAllValues(
						peers
							.map((x) => x.stream.publicKeyHash)
							.filter((x) => x !== peer.stream.publicKeyHash)
					); // peer has recevied reachable event from everone
				}

				for (const peer of peers) {
					expect(peer.unrechable).toHaveLength(0); // No unreachable events before stopping
				}
			});

			afterEach(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
			});

			it("will emit unreachable events on shutdown", async () => {
				/** Shut down slowly and check that all unreachable events are fired */
				let reachableBeforeStop = peers[2].reachable.length;
				await peers[0].stream.stop();
				const hasAll = (arr: PublicSignKey[], cmp: PublicSignKey[]) => {
					let a = new Set(arr.map((x) => x.hashcode()));
					let b = new Set(cmp.map((x) => x.hashcode()));
					if (
						a.size === b.size &&
						a.size === arr.length &&
						arr.length === cmp.length
					) {
						for (const key of cmp) {
							if (!arr.find((x) => x.equals(key))) {
								return false;
							}
						}
						return true;
					}
					return false;
				};

				expect(reachableBeforeStop).toEqual(peers[1].reachable.length);
				expect(reachableBeforeStop).toEqual(peers[2].reachable.length);
				expect(reachableBeforeStop).toEqual(peers[0].reachable.length);

				expect(peers[0].unrechable).toHaveLength(0);
				await waitFor(() =>
					hasAll(peers[1].unrechable, [
						peers[0].stream.publicKey,
						peers[3].stream.publicKey,
					])
				);
				await peers[1].stream.stop();
				await waitFor(() =>
					hasAll(peers[2].unrechable, [
						peers[0].stream.publicKey,
						peers[1].stream.publicKey,
						peers[3].stream.publicKey,
					])
				);

				await peers[2].stream.stop();
				await waitFor(() =>
					hasAll(peers[3].unrechable, [
						peers[0].stream.publicKey,
						peers[1].stream.publicKey,
						peers[2].stream.publicKey,
					])
				);
				await peers[3].stream.stop();
			});

			it("will publish on routes", async () => {
				peers[2].recieved = [];
				peers[3].recieved = [];

				await peers[0].stream.publish(data, {
					to: [peers[2].stream.libp2p.peerId],
				});
				await waitFor(() => peers[2].recieved.length === 1);
				expect(
					peers[2].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				await delay(1000); // some delay to allow all messages to progagate
				expect(peers[3].recieved).toHaveLength(0);
				expect(
					peers[3].messages.find((x) => x instanceof DataMessage)
				).toBeUndefined();
			});

			it("re-route new connection", async () => {
				/* 					
				┌───┐ 
				│3  │ 
				└┬─┬┘ 
				│┌▽┐ 
				││0│ 
				│└┬┘ 
				│┌▽─┐
				││1 │
				│└┬─┘
				┌▽─▽┐ 
				│2  │ 
				└───┘ 
				 */

				expect(
					peers[3].stream.routes.getPath(
						peers[3].stream.publicKeyHash,
						peers[2].stream.publicKeyHash
					)
				).toHaveLength(4);
				await session.connect([[session.peers[2], session.peers[3]]]);
				await waitFor(
					() =>
						peers[3].stream.routes.getPath(
							peers[3].stream.publicKeyHash,
							peers[2].stream.publicKeyHash
						).length === 2
				);
			});

			it("handle on drop no routes", async () => {
				expect(
					peers[3].stream.routes.getPath(
						peers[3].stream.publicKeyHash,
						peers[2].stream.publicKeyHash
					)
				).toHaveLength(4);
				expect(peers[1].stream.earlyGoodbyes.size).toEqual(2);
				expect(peers[3].stream.earlyGoodbyes.size).toEqual(1);

				await peers[0].stream.stop();
				await waitFor(() => peers[3].stream.routes.linksCount === 0); // because 1, 2 are now disconnected
				await delay(1000); // make sure nothing get readded
				expect(peers[3].stream.routes.linksCount).toEqual(0);
				expect(
					peers[3].stream.routes.getPath(
						peers[3].stream.publicKeyHash,
						peers[2].stream.publicKeyHash
					)
				).toHaveLength(0);
				expect(peers[3].stream.earlyGoodbyes.size).toEqual(0);
			});
		});

		describe("6", () => {
			/* 
			┌─┐
			│0│
			└△┘
			┌▽┐
			│1│
			└△┘
			┌▽┐
			│2│
			└─┘

			< 2 connects with 3 >

			┌─┐
			│3│
			└△┘
			┌▽┐
			│4│
			└△┘
			┌▽┐
			│5│
			└─┘ 
			*/

			beforeEach(async () => {
				session = await LSession.disconnected(6);
				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[3], session.peers[4]],
					[session.peers[4], session.peers[5]],
				]);

				peers = [];
				for (const [i, peer] of session.peers.entries()) {
					const stream = new TestStreamImpl(peer, {
						connectionManager: { autoDial: false },
					});
					const client: {
						stream: TestStreamImpl;
						messages: Message[];
						recieved: DataMessage[];
						reachable: PublicSignKey[];
						unrechable: PublicSignKey[];
					} = {
						messages: [],
						recieved: [],
						reachable: [],
						unrechable: [],
						stream,
					};
					peers.push(client);
					stream.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					stream.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});
					await stream.start();
				}

				for (const peer of peers.values()) {
					await waitFor(() => peer.stream.routes.linksCount === 2);
				}

				for (let i = 0; i < 2; i++) {
					await waitForPeers(peers[i].stream, peers[i + 1].stream);
				}
				for (let i = 3; i < 5; i++) {
					await waitForPeers(peers[i].stream, peers[i + 1].stream);
				}
			});

			afterAll(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			it("will replay on connect", async () => {
				for (let i = 3; i < 5; i++) {
					await waitForPeers(peers[i].stream, peers[i + 1].stream);
				}
				expect(peers[2].stream.helloMap.size).toEqual(2); // these hellos will be forwarded on connect
				expect(peers[3].stream.helloMap.size).toEqual(2); // these hellos will be forwarded on connect
				await session.connect([[session.peers[2], session.peers[3]]]);

				for (const peer of peers) {
					await waitFor(() => peer.stream.routes.linksCount === 5); // everyone knows everone
				}
			});
		});
	});

	describe("start/stop", () => {
		let session: LSession, stream1: TestStreamImpl, stream2: TestStreamImpl;

		beforeEach(async () => {
			session = await LSession.connected(2);
		});

		afterEach(async () => {
			await stream1?.stop();
			await stream2?.stop();
			await session.stop();
		});

		it("can restart", async () => {
			await session.connect();
			stream1 = new TestStreamImpl(session.peers[0], {
				connectionManager: { autoDial: false },
			});
			stream2 = new TestStreamImpl(session.peers[1], {
				connectionManager: { autoDial: false },
			});
			await stream1.start();
			await stream2.start();
			await waitFor(() => stream2.helloMap.size == 1);
			await stream1.stop();
			await waitFor(() => stream2.helloMap.size === 0);

			await stream2.stop();
			expect(stream1.peers.size).toEqual(0);
			await stream1.start();
			expect(stream1.helloMap.size).toEqual(0);

			await stream2.start();
			await waitFor(() => stream1.peers.size === 1);
			await waitFor(() => stream1.helloMap.size === 1);
			await waitFor(() => stream2.helloMap.size === 1);
			await waitForPeers(stream1, stream2);
		});
		it("can connect after start", async () => {
			stream1 = new TestStreamImpl(session.peers[0]);
			stream2 = new TestStreamImpl(session.peers[1]);

			await stream1.start();
			await stream2.start();

			await session.connect();
			await waitForPeers(stream1, stream2);
		});

		it("can connect before start", async () => {
			stream1 = new TestStreamImpl(session.peers[0]);
			stream2 = new TestStreamImpl(session.peers[1]);
			await session.connect();
			await delay(3000);

			await stream1.start();
			await stream2.start();
			await waitForPeers(stream1, stream2);
		});

		it("can connect with delay", async () => {
			stream1 = new TestStreamImpl(session.peers[0]);
			stream2 = new TestStreamImpl(session.peers[1]);
			stream2.start();
			await delay(3000);
			stream1.start();

			await waitForPeers(stream1, stream2);
		});
	});

	describe("multistream", () => {
		let session: LSession, stream1: TestStreamImpl, stream2: TestStreamImpl;
		let stream1b: TestStreamImpl, stream2b: TestStreamImpl;

		beforeEach(async () => {
			session = await LSession.connected(2);
		});

		afterEach(async () => {
			await stream1?.stop();
			await stream2?.stop();
			await stream1b?.stop();
			await stream2b?.stop();
			await session.stop();
		});

		it("can setup multiple streams at once", async () => {
			stream1 = new TestStreamImpl(session.peers[0]);
			stream2 = new TestStreamImpl(session.peers[1]);
			stream1b = new TestStreamImpl(session.peers[0], { id: "alt" });
			stream2b = new TestStreamImpl(session.peers[1], { id: "alt" });
			stream1.start();
			stream2.start();
			stream1b.start();
			stream2b.start();
			await waitFor(() => !!stream1.peers.size);
			await waitFor(() => !!stream2.peers.size);
			await waitFor(() => !!stream1b.peers.size);
			await waitFor(() => !!stream2b.peers.size);
		});
	});
});
