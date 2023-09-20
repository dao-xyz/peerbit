import { PeerbitProxyHost } from "../host";
import { TestSession } from "@peerbit/test-utils";
import { Ed25519Keypair } from "@peerbit/crypto";
import { delay, waitForResolved } from "@peerbit/time";
import { GetSubscribers } from "@peerbit/pubsub-interface";
import { deserialize } from "@dao-xyz/borsh";
import { EventEmitter } from "@libp2p/interface/events";
import { DataMessage } from "@peerbit/stream-interface";
import { PeerbitProxyClient } from "../client.js";
import { EventEmitterNode } from "./utils.js";

describe("index", () => {
	let session: TestSession;
	let hostWithClients: [
		PeerbitProxyHost,
		PeerbitProxyClient,
		PeerbitProxyClient
	][];
	let client1: PeerbitProxyClient;
	let client1b: PeerbitProxyClient;
	let host1: PeerbitProxyHost;
	let client2: PeerbitProxyClient;
	let client2b: PeerbitProxyClient;
	let host2: PeerbitProxyHost;

	beforeEach(async () => {
		hostWithClients = [];
		session = await TestSession.disconnected(2);

		const events = new EventEmitter();
		for (let i = 0; i < 2; i++) {
			const host = new PeerbitProxyHost(
				session.peers[i],
				new EventEmitterNode(events).start()
			);
			await host.init();
			const client1 = new PeerbitProxyClient(
				new EventEmitterNode(events).start()
			);
			client1.messages.connect({ to: { id: host.messages.id, parent: true } });
			await client1.connect();
			const client2 = new PeerbitProxyClient(
				new EventEmitterNode(events).start()
			);
			client2.messages.connect({ to: { id: host.messages.id, parent: true } });
			await client2.connect();
			hostWithClients.push([host, client1, client2]);
		}

		client1 = hostWithClients[0][1];
		client1b = hostWithClients[0][2];
		host1 = hostWithClients[0][0];
		client2 = hostWithClients[1][1];
		client2b = hostWithClients[1][2];
		host2 = hostWithClients[1][0];
	});

	afterEach(async () => {
		await session.stop();
	});

	it("peerId", async () => {
		expect(client1.peerId.equals(host1.peerId)).toBeTrue();
	});

	it("getMultiaddrs", async () => {
		expect(client1.getMultiaddrs().map((x) => x.toString())).toEqual(
			host1.getMultiaddrs().map((x) => x.toString())
		);
	});

	it("dial", async () => {
		await client1.dial(session.peers[1].getMultiaddrs());
		expect(session.peers[1]);
		expect(client1.getMultiaddrs().map((x) => x.toString())).toEqual(
			host1.getMultiaddrs().map((x) => x.toString())
		);
	});

	describe("memory", () => {
		let key = "key";
		let data = new Uint8Array([1, 2, 3]);

		it("open", async () => {
			await client1.memory.close();
			expect(await client1.memory.status()).toEqual("closed");
			expect(await host1.memory.status()).toEqual("closed");
			await client1.memory.open();
			expect(await client1.memory.status()).toEqual("open");
			expect(await host1.memory.status()).toEqual("open");
		});

		it("put", async () => {
			expect(await client1.memory.get(key)).toEqual(undefined);
			expect(await host1.memory.get(key)).toEqual(undefined);
			await client1.memory.put(key, data);
			expect(new Uint8Array((await client1.memory.get(key))!)).toEqual(data);
			expect(new Uint8Array((await host1.memory.get(key))!)).toEqual(data);
		});

		it("del", async () => {
			await client1.memory.put(key, data);

			expect(new Uint8Array((await client1.memory.get(key))!)).toEqual(data);
			expect(new Uint8Array((await host1.memory.get(key))!)).toEqual(data);

			await client1.memory.del(key);

			expect(await client1.memory.get(key)).toEqual(undefined);
			expect(await host1.memory.get(key)).toEqual(undefined);
		});
		it("iterator", async () => {
			const keys = [key, key + "-2"];
			const datas = [data, new Uint8Array([1])];
			await client1.memory.put(key, data);
			await Promise.all(
				keys.map((key, ix) => client1.memory.put(key, datas[ix]))
			);
			await client1.memory.idle?.();

			let c = 0;
			for await (const iter of client1.memory.iterator()) {
				expect(iter[0]).toEqual(keys[c]);
				expect(new Uint8Array(iter[1])).toEqual(datas[c]);

				c++;
			}
			expect(c).toEqual(2);
			expect(host1["_memoryIterator"].size).toEqual(0);
		});

		it("iterator early break no leak", async () => {
			const keys = [key, key + "-2"];
			const datas = [data, new Uint8Array([1])];
			await client1.memory.put(key, data);
			await Promise.all(
				keys.map((key, ix) => client1.memory.put(key, datas[ix]))
			);
			await client1.memory.idle?.();

			let c = 0;
			for await (const iter of client1.memory.iterator()) {
				break;
			}
			expect(host1["_memoryIterator"].size).toEqual(0);
		});

		it("clear", async () => {
			await client1.memory.put(key, data);
			await client1.memory.clear();

			expect(await client1.memory.get(key)).toEqual(undefined);
			expect(await host1.memory.get(key)).toEqual(undefined);
		});

		it("close", async () => {
			await client1.memory.open();
			expect(await client1.memory.status()).toEqual("open");
			expect(await host1.memory.status()).toEqual("open");
			await client1.memory.close();
			expect(await client1.memory.status()).toEqual("closed");
			expect(await host1.memory.status()).toEqual("closed");
		});

		it("sublevel", async () => {
			const sublevel = await client1.memory.sublevel("sublevel");
			await waitForResolved(async () =>
				expect(await sublevel.status()).toEqual("open")
			);
			await sublevel.put(key, data);
			expect(new Uint8Array((await sublevel.get(key))!)).toEqual(data);
			await client1.memory.clear();
			expect(await sublevel.get(key)).toBeUndefined();
		});
	});

	describe("keychain", () => {
		it("import", async () => {
			const keypair = await Ed25519Keypair.create();
			const id = new Uint8Array([1, 2, 3]);
			await client1.keychain.import(keypair, id);
			expect(
				(await client1.keychain.exportById(id, "ed25519"))?.equals(keypair)
			).toBeTrue();
			expect(
				(await host1.keychain.exportById(id, "ed25519"))?.equals(keypair)
			).toBeTrue();
			expect(
				(await client1.keychain.exportByKey(keypair.publicKey))?.equals(keypair)
			).toBeTrue();
			expect(
				(await host1.keychain.exportByKey(keypair.publicKey))?.equals(keypair)
			).toBeTrue();
		});
	});

	describe("blocks", () => {
		let data = new Uint8Array([1, 2, 3]);

		it("put/rm", async () => {
			const cid = await client1.services.blocks.put(data);
			expect(new Uint8Array((await client1.services.blocks.get(cid))!)).toEqual(
				data
			);
			expect(await host1.services.blocks.get(cid)).toEqual(data);

			expect(await client1.services.blocks.has(cid)).toEqual(true);
			expect(await host1.services.blocks.has(cid)).toEqual(true);

			await client1.services.blocks.rm(cid);

			let t0 = +new Date();
			expect(await client1.services.blocks.get(cid, { timeout: 1000 })).toEqual(
				undefined
			);
			expect(await host1.services.blocks.get(cid, { timeout: 1000 })).toEqual(
				undefined
			);
			expect(+new Date() - t0).toBeLessThan(3000);

			expect(await client1.services.blocks.has(cid)).toEqual(false);
			expect(await host1.services.blocks.has(cid)).toEqual(false);
		});

		it("waitFor", async () => {
			const waitForFn = host1.services.blocks.waitFor.bind(
				host1.services.blocks
			);
			let invoked = false;
			host1.services.blocks.waitFor = (p) => {
				invoked = true;
				return waitForFn(p);
			};
			await client1.dial(session.peers[1].getMultiaddrs());
			await client1.services.blocks.waitFor(session.peers[1].peerId);
		});
	});

	describe("pubsub", () => {
		let data = new Uint8Array([1, 2, 3]);

		beforeEach(async () => {
			await client1.dial(client2.getMultiaddrs());
			await client1.services.pubsub.waitFor(client2.peerId);
		});

		it("subscribe/unsubscribe", async () => {
			await client2.services.pubsub.subscribe("topic");
			let msg1 = false;
			let msg1b = false;
			let msg2 = false;
			let msg2b = false;

			await client1.services.pubsub.addEventListener("data", () => {
				msg1 = true;
			});

			await client1b.services.pubsub.addEventListener("data", () => {
				msg1b = true;
			});

			await client2.services.pubsub.addEventListener("data", () => {
				msg2 = true;
			});

			await client2b.services.pubsub.addEventListener("data", () => {
				msg2b = true;
			});

			await client1.services.pubsub.requestSubscribers("topic");
			await waitForResolved(async () =>
				expect(
					(await client1.services.pubsub.getSubscribers("topic"))!.size
				).toEqual(1)
			);
			await client1.services.pubsub.publish(data, { topics: ["topic"] });
			await waitForResolved(() => expect(msg2).toBeTrue());
			expect(msg2b).toBeFalse();
			await client2b.services.pubsub.subscribe("topic");
			await client1.services.pubsub.publish(data, { topics: ["topic"] });
			await waitForResolved(() => expect(msg2b).toBeTrue());

			expect(msg1).toBeFalse();
			expect(msg1b).toBeFalse();

			await client2.services.pubsub.unsubscribe("topic");

			msg2 = false;
			await client1.services.pubsub.publish(data, { topics: ["topic"] });
			await delay(3000);
			expect(msg2).toBeFalse();
		});

		it("getSubscribers", async () => {
			await client1.services.pubsub.waitFor(client2.peerId);
			await client2.services.pubsub.subscribe("topic");
			let msg = false;
			await client2.services.pubsub.addEventListener("data", () => {
				msg = true;
			});
			await client1.services.pubsub.requestSubscribers("topic");
			await waitForResolved(async () =>
				expect(
					(await client1.services.pubsub.getSubscribers("topic"))?.get(
						client2.identity.publicKey.hashcode()
					)
				).toBeDefined()
			);
		});

		it("requestSubsribers", async () => {
			let receivedMessages: (GetSubscribers | undefined)[] = [];
			await client2.services.pubsub.addEventListener("message", (message) => {
				if (message.detail instanceof DataMessage) {
					receivedMessages.push(
						deserialize(message.detail.data, GetSubscribers)
					);
				}
			});
			await client1.services.pubsub.requestSubscribers(
				"topic",
				client2.identity.publicKey
			);

			await waitForResolved(() => expect(receivedMessages).toHaveLength(1));
			expect(receivedMessages[0]).toBeInstanceOf(GetSubscribers);
		});

		it("emitSelf", () => {
			expect(host1.services.pubsub.emitSelf).toBeFalse();
			expect(client1.services.pubsub.emitSelf).toBeFalse();
		});
	});
});
