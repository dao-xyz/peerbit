import { LSession } from "@dao-xyz/libp2p-test-utils";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import crypto from "crypto";
import { waitForPeers, DirectStream } from "..";
import { Libp2p } from "libp2p";
import { DataMessage, Message } from "../messages";

class TestStreamImpl extends DirectStream {
	constructor(libp2p: Libp2p) {
		super(libp2p, ["test/0.0.0"], {
			canRelayMessage: true,
			emitSelf: true,
		});
	}
}

describe("streams", function () {
	describe("to", () => {
		let session: LSession,
			stream1: TestStreamImpl,
			stream2: TestStreamImpl,
			stream3: TestStreamImpl;
		let recievedMessages1: DataMessage[];
		let recievedMessages2: DataMessage[];
		let recievedMessages3: DataMessage[];
		const data = new Uint8Array([1, 2, 3]);

		beforeAll(async () => { });

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await LSession.disconnected(3);

			/* 
			┌─┐
			│1│
			└┬┘
			┌▽┐
			│2│
			└┬┘
			┌▽┐
			│3│
			└─┘
			*/

			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			stream1 = new TestStreamImpl(session.peers[0]);
			stream2 = new TestStreamImpl(session.peers[1]);
			stream3 = new TestStreamImpl(session.peers[2]);
			recievedMessages1 = [];
			recievedMessages2 = [];
			recievedMessages3 = [];

			stream1.addEventListener("data", (msg) => {
				recievedMessages1.push(msg.detail);
			});
			stream2.addEventListener("data", (msg) => {
				recievedMessages2.push(msg.detail);
			});
			stream3.addEventListener("data", (msg) => {
				recievedMessages3.push(msg.detail);
			});

			await stream1.start();

			await stream2.start();
			//	await delay(5000);

			await stream3.start();
			await waitForPeers(stream1, stream2);
			await waitForPeers(stream2, stream3);
		});

		afterEach(async () => {
			await stream1?.stop();
			await stream2?.stop();
			await stream3?.stop();
			await session.stop();
		});

		afterAll(async () => { });

		it("many", async () => {
			let done = 0;
			let iterations = 300;
			const listener = () => {
				done += 1;
			};
			stream3.addEventListener("data", listener);

			for (let i = 0; i < iterations; i++) {
				const small = crypto.randomBytes(1e3); // 1kb
				stream1.publish(small);
			}
			try {
				await waitFor(() => done === iterations, {
					delayInterval: 300,
					timeout: 30 * 1000,
				});
			} catch (error) {
				console.log("failed but messages done:", done);
				await delay(1000);
				console.log("failed but messages done:", done);
				throw error;
			}
			stream3.removeEventListener("data", listener);
		});

		it("1->unknown", async () => {
			await stream1.publish(data);
			await waitFor(() => recievedMessages2.length === 1);
			expect(new Uint8Array(recievedMessages2[0].data)).toEqual(data);
			await waitFor(() => recievedMessages3.length === 1);
			expect(new Uint8Array(recievedMessages3[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(recievedMessages2).toHaveLength(1);
			expect(recievedMessages3).toHaveLength(1);
		});

		it("1->2", async () => {
			await stream1.publish(data, { to: [stream2.libp2p.peerId] });
			await waitFor(() => recievedMessages2.length === 1);
			expect(new Uint8Array(recievedMessages2[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(recievedMessages2).toHaveLength(1);
			expect(recievedMessages3).toHaveLength(0);
		});

		it("1->3", async () => {
			await stream1.publish(data, { to: [stream3.libp2p.peerId] });
			await waitFor(() => recievedMessages3.length === 1);
			expect(new Uint8Array(recievedMessages3[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(recievedMessages3).toHaveLength(1);
			expect(recievedMessages2).toHaveLength(0);
		});

		it("1->3 10mb data", async () => {
			const bigData = crypto.randomBytes(1e7);
			await stream1.publish(bigData, { to: [stream3.libp2p.peerId] });
			await waitFor(() => recievedMessages3.length === 1, {
				delayInterval: 10,
				timeout: 50 * 1000,
			});
			expect(new Uint8Array(recievedMessages3[0].data)).toHaveLength(
				bigData.length
			);
			expect(recievedMessages3).toHaveLength(1);
			expect(recievedMessages2).toHaveLength(0);
		});
	});

	describe("routing", () => {
		let session: LSession;
		let peers: {
			stream: TestStreamImpl;
			messages: Message[];
			recieved: DataMessage[];
		}[];

		const data = new Uint8Array([1, 2, 3]);

		beforeAll(async () => { });
		beforeEach(async () => {
			session = await LSession.disconnected(4);

			/* 
			┌────┐
			│0   │
			└┬──┬┘
			┌▽┐┌▽┐
			│1││3│
			└┬┘└─┘
			┌▽┐   
			│2│   
			└─┘   
			*/

			peers = [];
			for (const peer of session.peers) {
				const stream = new TestStreamImpl(peer);
				const client: {
					stream: TestStreamImpl;
					messages: Message[];
					recieved: DataMessage[];
				} = {
					messages: [],
					recieved: [],
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

			// slowly connect to that the route maps are deterministic
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
		});

		afterEach(async () => {
			await Promise.all(peers.map((peer) => peer.stream.stop()));
			await session.stop();
		});
		afterAll(async () => { });

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

	describe("join/leave", () => {
		let session: LSession;
		let peers: {
			stream: TestStreamImpl;
			messages: Message[];
			recieved: DataMessage[];
		}[];

		const data = new Uint8Array([1, 2, 3]);

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
					const stream = new TestStreamImpl(peer);
					const client: {
						stream: TestStreamImpl;
						messages: Message[];
						recieved: DataMessage[];
					} = {
						messages: [],
						recieved: [],
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

				// slowly connect to that the route maps are deterministic
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
			});

			afterEach(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
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
					const stream = new TestStreamImpl(peer);
					const client: {
						stream: TestStreamImpl;
						messages: Message[];
						recieved: DataMessage[];
					} = {
						messages: [],
						recieved: [],
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
				await session.stop();
			});
			it("will replay on connect", async () => {
				for (let i = 3; i < 5; i++) {
					await waitForPeers(peers[i].stream, peers[i + 1].stream);
				}
				expect(peers[2].stream.hellosToReplay.size).toEqual(2); // these hellos will be forwarded on connect
				expect(peers[3].stream.hellosToReplay.size).toEqual(2); // these hellos will be forwarded on connect
				await session.connect([[session.peers[2], session.peers[3]]]);

				for (const peer of peers) {
					await waitFor(() => peer.stream.routes.linksCount === 5); // everyone knows everone
				}
			});
		});
	});

	describe("lifecycle", () => {
		let session: LSession, stream1: TestStreamImpl, stream2: TestStreamImpl;

		beforeAll(async () => {
			session = await LSession.connected(2);
		});
		beforeEach(async () => {
			stream1 = new TestStreamImpl(session.peers[0]);
			stream2 = new TestStreamImpl(session.peers[1]);
			await stream1.start();
			await stream2.start();
			await waitForPeers(stream1, stream2);
		});

		afterEach(async () => {
			await stream1?.stop();
			await stream2?.stop();
		});

		afterAll(async () => {
			await session.stop();
		});
		it("can restart", async () => {
			await stream1.stop();
			await stream2.stop();
			await delay(1000); // Some delay seems to be necessary TODO fix
			await stream1.start();
			await stream2.start();
			await waitForPeers(stream1, stream2);
		});
	});
});
