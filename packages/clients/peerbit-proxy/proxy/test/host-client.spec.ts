import { deserialize } from "@dao-xyz/borsh";
import { TypedEventEmitter } from "@libp2p/interface";
import { Ed25519Keypair, X25519Keypair } from "@peerbit/crypto";
import { GetSubscribers } from "@peerbit/pubsub-interface";
import { DataMessage } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { PeerbitProxyClient } from "../src/client.js";
import { PeerbitProxyHost } from "../src/host.js";
import { EventEmitterNode } from "./utils.js";

describe("index", () => {
	let session: TestSession;
	let hostWithClients: [
		PeerbitProxyHost,
		PeerbitProxyClient,
		PeerbitProxyClient,
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

		const events = new TypedEventEmitter();
		for (let i = 0; i < 2; i++) {
			const host = new PeerbitProxyHost(
				session.peers[i],
				new EventEmitterNode(events).start(),
			);
			await host.init();
			const client1 = new PeerbitProxyClient(
				new EventEmitterNode(events).start(),
			);
			client1.messages.connect({ to: { id: host.messages.id, parent: true } });
			await client1.connect();
			const client2 = new PeerbitProxyClient(
				new EventEmitterNode(events).start(),
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

		expect(host1).to.exist;
		expect(host2).to.exist;
	});

	afterEach(async () => {
		await session.stop();
	});

	it("peerId", async () => {
		expect(client1.peerId.equals(host1.peerId)).to.be.true;
	});

	it("getMultiaddrs", async () => {
		expect(client1.getMultiaddrs().map((x) => x.toString())).to.deep.equal(
			host1.getMultiaddrs().map((x) => x.toString()),
		);
	});

	it("dial", async () => {
		await client1.dial(session.peers[1].getMultiaddrs());
		expect(client1.getMultiaddrs().map((x) => x.toString())).to.deep.equal(
			host1.getMultiaddrs().map((x) => x.toString()),
		);
	});

	describe("memory", () => {
		let key = "key";
		let data = new Uint8Array([1, 2, 3]);

		it("open", async () => {
			await client1.storage.close();
			expect(await client1.storage.status()).equal("closed");
			expect(await host1.storage.status()).equal("closed");
			await client1.storage.open();
			expect(await client1.storage.status()).equal("open");
			expect(await host1.storage.status()).equal("open");
		});

		it("put", async () => {
			expect(await client1.storage.get(key)).equal(undefined);
			expect(await host1.storage.get(key)).equal(undefined);
			await client1.storage.put(key, data);
			expect(new Uint8Array((await client1.storage.get(key))!)).to.deep.equal(
				data,
			);
			expect(new Uint8Array((await host1.storage.get(key))!)).to.deep.equal(
				data,
			);
		});

		it("del", async () => {
			await client1.storage.put(key, data);

			expect(new Uint8Array((await client1.storage.get(key))!)).to.deep.equal(
				data,
			);
			expect(new Uint8Array((await host1.storage.get(key))!)).to.deep.equal(
				data,
			);

			await client1.storage.del(key);

			expect(await client1.storage.get(key)).equal(undefined);
			expect(await host1.storage.get(key)).equal(undefined);
		});
		it("iterator", async () => {
			const keys = [key, key + "-2"];
			const datas = [data, new Uint8Array([1])];
			await client1.storage.put(key, data);
			await Promise.all(
				keys.map((key, ix) => client1.storage.put(key, datas[ix])),
			);

			let c = 0;
			for await (const iter of client1.storage.iterator()) {
				expect(iter[0]).equal(keys[c]);
				expect(new Uint8Array(iter[1])).to.deep.equal(datas[c]);

				c++;
			}
			expect(c).equal(2);
			expect(host1["_memoryIterator"].size).equal(0);
		});

		it("iterator early break no leak", async () => {
			const keys = [key, key + "-2"];
			const datas = [data, new Uint8Array([1])];
			await client1.storage.put(key, data);
			await Promise.all(
				keys.map((key, ix) => client1.storage.put(key, datas[ix])),
			);

			// eslint-disable-next-line no-unreachable-loop
			for await (const _iter of client1.storage.iterator()) {
				break;
			}
			expect(host1["_memoryIterator"].size).equal(0);
		});

		it("clear", async () => {
			await client1.storage.put(key, data);
			await client1.storage.clear();

			expect(await client1.storage.get(key)).equal(undefined);
			expect(await host1.storage.get(key)).equal(undefined);
		});

		it("size", async () => {
			const size1 = await client1.storage.size();
			expect(size1).equal(0);
			await client1.storage.put("key", data);
			const size2 = await client1.storage.size();
			expect(size2).greaterThan(0);
		});

		it("close", async () => {
			await client1.storage.open();
			expect(await client1.storage.status()).equal("open");
			expect(await host1.storage.status()).equal("open");
			await client1.storage.close();
			expect(await client1.storage.status()).equal("closed");
			expect(await host1.storage.status()).equal("closed");
		});

		it("sublevel", async () => {
			const sublevel = await client1.storage.sublevel("sublevel");
			await waitForResolved(async () =>
				expect(await sublevel.status()).equal("open"),
			);
			await sublevel.put(key, data);
			expect(new Uint8Array((await sublevel.get(key))!)).to.deep.equal(data);
			await client1.storage.clear();
			expect(await sublevel.get(key)).equal(undefined);
		});

		it("persisted", async () => {
			expect(await client1.storage.persisted()).equal(false);

			// TODO try case where it is persisted
		});
	});

	describe("keychain", () => {
		it("ed25519", async () => {
			const keypair = await Ed25519Keypair.create();
			const id = new Uint8Array([1, 2, 3]);
			await client1.services.keychain.import({ keypair, id });
			expect(
				(
					await client1.services.keychain.exportById(id, Ed25519Keypair)
				)?.equals(keypair),
			).to.be.true;
			expect(
				(await host1.services.keychain.exportById(id, Ed25519Keypair))?.equals(
					keypair,
				),
			).to.be.true;
			expect(
				(
					await client1.services.keychain.exportByKey(keypair.publicKey)
				)?.equals(keypair),
			).to.be.true;
			expect(
				(await host1.services.keychain.exportByKey(keypair.publicKey))?.equals(
					keypair,
				),
			).to.be.true;
		});

		it("x25519", async () => {
			const keypair = await Ed25519Keypair.create();
			const id = new Uint8Array([1, 2, 3]);
			await client1.services.keychain.import({ keypair, id });
			const xkeypair = await X25519Keypair.from(keypair);
			expect(
				(
					await client1.services.keychain.exportByKey(xkeypair.publicKey)
				)?.equals(xkeypair),
			).to.be.true;

			expect(
				(await host1.services.keychain.exportByKey(xkeypair.publicKey))?.equals(
					xkeypair,
				),
			).to.be.true;
		});
	});

	describe("blocks", () => {
		let data = new Uint8Array([1, 2, 3]);

		it("put/rm", async () => {
			const cid = await client1.services.blocks.put(data);
			expect(
				new Uint8Array((await client1.services.blocks.get(cid))!),
			).to.deep.equal(data);
			expect(await host1.services.blocks.get(cid)).to.deep.equal(data);

			expect(await client1.services.blocks.has(cid)).equal(true);
			expect(await host1.services.blocks.has(cid)).equal(true);

			await client1.services.blocks.rm(cid);

			let t0 = +new Date();
			expect(
				await client1.services.blocks.get(cid, { remote: { timeout: 1000 } }),
			).equal(undefined);
			expect(
				await host1.services.blocks.get(cid, { remote: { timeout: 1000 } }),
			).equal(undefined);
			expect(+new Date() - t0).lessThan(3000);

			expect(await client1.services.blocks.has(cid)).equal(false);
			expect(await host1.services.blocks.has(cid)).equal(false);
		});

		it("waitFor", async () => {
			const waitForFn = host1.services.blocks.waitFor.bind(
				host1.services.blocks,
			);
			let invoked = false;
			host1.services.blocks.waitFor = (p) => {
				invoked = true;
				return waitForFn(p);
			};
			await client1.dial(session.peers[1].getMultiaddrs());
			await client1.services.blocks.waitFor(session.peers[1].peerId);
			expect(invoked).to.be.true;
		});

		it("size", async () => {
			const size1 = await client1.services.blocks.size();
			expect(size1).equal(0);
			await client1.services.blocks.put(data);
			const size2 = await client1.services.blocks.size();
			expect(size2).greaterThan(0);
		});

		it("persisted", async () => {
			expect(await client1.services.blocks.persisted()).equal(false);

			// TODO try case where it is persisted
		});
	});

	describe("pubsub", () => {
		let data = new Uint8Array([1, 2, 3]);

		beforeEach(async () => {
			await client1.dial(client2.getMultiaddrs());
			await client1.services.pubsub.waitFor(client2.peerId);
		});

		describe("publish", () => {
			it("multiple hosts", async () => {
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
						(await client1.services.pubsub.getSubscribers("topic"))!.length,
					).equal(1),
				);
				await client1.services.pubsub.publish(data, { topics: ["topic"] });
				await waitForResolved(() => expect(msg2).to.be.true);
				expect(msg2b).to.be.false;
				await client2b.services.pubsub.subscribe("topic");
				await client1.services.pubsub.publish(data, { topics: ["topic"] });
				await waitForResolved(() => expect(msg2b).to.be.true);

				expect(msg1).to.be.false;
				expect(msg1b).to.be.false;

				await client2.services.pubsub.unsubscribe("topic");

				msg2 = false;
				await client1.services.pubsub.publish(data, { topics: ["topic"] });
				await delay(3000);
				expect(msg2).to.be.false;
			});

			it("same host", async () => {
				let msg1data = false;
				let msg1publish = false;
				let msg2data = false;
				let msg2publish = false;

				await client1.services.pubsub.addEventListener("data", () => {
					msg1data = true;
				});

				await client1.services.pubsub.addEventListener("publish", () => {
					msg1publish = true;
				});

				await client1b.services.pubsub.addEventListener("data", () => {
					msg2data = true;
				});

				await client1b.services.pubsub.addEventListener("publish", () => {
					msg2publish = true;
				});

				await client1.services.pubsub.subscribe("topic");
				await client1b.services.pubsub.subscribe("topic");
				await client1.services.pubsub.publish(data, { topics: ["topic"] });

				expect(msg1data).to.be.false;
				expect(msg1publish).to.be.true;
				await waitForResolved(() => expect(msg2data).to.be.true);

				expect(msg2publish).to.be.true; // TODO expected?
			});

			it("publish will not emit to data", async () => {
				let data = false;

				host1.services.pubsub.addEventListener("data", (evt) => {
					data = true;
				});

				await host1.services.pubsub.subscribe("topic");
				await host1.services.pubsub.publish(new Uint8Array([123]), {
					topics: ["topic"],
				});

				expect(data).to.exist;
			});
		});

		it("getSubscribers", async () => {
			await client1.services.pubsub.waitFor(client2.peerId);
			await client2.services.pubsub.subscribe("topic");
			await client1.services.pubsub.requestSubscribers("topic");
			await waitForResolved(
				async () =>
					expect(
						(await client1.services.pubsub.getSubscribers("topic"))?.find((x) =>
							x.equals(client2.identity.publicKey),
						),
					).to.exist,
			);
		});

		it("requestSubsribers", async () => {
			let receivedMessages: (GetSubscribers | undefined)[] = [];
			await client2.services.pubsub.addEventListener("message", (message) => {
				if (message.detail instanceof DataMessage && message.detail.data) {
					receivedMessages.push(
						deserialize(message.detail.data, GetSubscribers),
					);
				}
			});
			await client1.services.pubsub.requestSubscribers(
				"topic",
				client2.identity.publicKey,
			);

			await waitForResolved(() => expect(receivedMessages).to.have.length(1));
			expect(receivedMessages[0]).to.be.instanceOf(GetSubscribers);
		});

		it("getPublicKey", async () => {
			await client1.services.pubsub.waitFor(client2.peerId);
			expect(
				(await client1.services.pubsub.getPublicKey(
					client2.identity.publicKey.hashcode(),
				))!.equals(client2.identity.publicKey),
			);
		});
	});
});
