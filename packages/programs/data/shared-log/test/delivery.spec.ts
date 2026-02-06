import { ACK, AcknowledgeDelivery, DataMessage } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { ExchangeHeadsMessage } from "../src/exchange-heads.js";
import { NoPeersError } from "../src/index.js";
import { EventStore } from "./utils/stores/index.js";

describe("append delivery options", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("awaits transport acks when delivery is set for target=replicators", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicas: { min: 2 },
				replicate: { offset: 0, factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: { min: 2 },
					replicate: { offset: 0, factor: 1 },
					timeUntilRoleMaturity: 0,
				},
			},
		);

		await db1.log.waitForReplicators({
			coverageThreshold: 1,
			roleAge: 0,
			timeout: 15e3,
		});

		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(async () => {
			const subscribers = await session.peers[0].services.pubsub.getSubscribers(
				db1.log.rpc.topic,
			);
			expect((subscribers || []).map((x) => x.hashcode())).to.include(
				remoteHash,
			);
		});

		const gate = pDefer<void>();
		const ackAttempted = pDefer<void>();
		const expectedAckedMessageIds = new Set<string>();
		const toB64 = (id: Uint8Array) => Buffer.from(id).toString("base64");

		const remotePubsub: any = session.peers[1].services.pubsub;
		const originalPublishMessage =
			remotePubsub.publishMessage.bind(remotePubsub);
		remotePubsub.publishMessage = async (...args: any[]) => {
			const message = args[1];
			if (message instanceof ACK) {
				// Only gate the ACKs that correspond to the message(s) produced by this test's `db1.add(...)`.
				// There can be unrelated ACK traffic during session setup in the full test suite.
				if (expectedAckedMessageIds.has(toB64(message.messageIdToAcknowledge))) {
					ackAttempted.resolve();
					await gate.promise;
				}
			}
			return originalPublishMessage(...args);
		};

		const localPubsub: any = session.peers[0].services.pubsub;
		const originalLocalPublishMessage =
			localPubsub.publishMessage.bind(localPubsub);
		localPubsub.publishMessage = async (...args: any[]) => {
			const message = args[1];
			if (
				message instanceof DataMessage &&
				message.header.mode instanceof AcknowledgeDelivery &&
				message.header.mode.to?.includes(remoteHash)
			) {
				expectedAckedMessageIds.add(toB64(message.id));
			}
			return originalLocalPublishMessage(...args);
		};

		let resolved = false;
		const promise = db1
			.add("hello", {
				target: "replicators",
				delivery: true,
			})
			.then((result) => {
				resolved = true;
				return result;
			});

		await ackAttempted.promise;
		expect(resolved).to.equal(false);

		gate.resolve();
		await promise;
		expect(resolved).to.equal(true);
	});

	it("throws when requireRecipients is true and there are no remotes", async () => {
		session = await TestSession.disconnected(1);

		const db1 = await session.peers[0].open(new EventStore<string, any>());

		await expect(
			db1.add("hello", {
				target: "replicators",
				delivery: { requireRecipients: true },
			}),
		).to.be.rejectedWith(NoPeersError);
	});

	it("throws when delivery options are used with target=all", async () => {
		session = await TestSession.connected(2);

		const root = (session.peers[0].services as any).fanout.publicKeyHash as string;
		const fanout = {
			root,
			channel: {
				msgRate: 10,
				msgSize: 256,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: true,
			},
			join: { timeoutMs: 10_000 },
		};

		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { fanout },
		});
		await EventStore.open<EventStore<string, any>>(db1.address!, session.peers[1], {
			args: { fanout },
		});

		await expect(
			(db1.add as any)("bad-delivery-all", {
				target: "all",
				delivery: true,
			}),
		).to.be.rejectedWith("delivery options are not supported with target=\"all\"");
	});

	it("throws on target=all when fanout channel is not configured", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore<string, any>());
		await EventStore.open<EventStore<string, any>>(db1.address!, session.peers[1]);

		await expect(
			db1.add("missing-fanout", { target: "all" }),
		).to.be.rejectedWith("No fanout channel configured");
	});

	it("uses fanout data plane for target=all when configured", async () => {
		session = await TestSession.connected(2);

		const root = (session.peers[0].services as any).fanout.publicKeyHash as string;
		const fanout = {
			root,
			channel: {
				msgRate: 10,
				msgSize: 256,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: true,
			},
			join: { timeoutMs: 10_000 },
		};

		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { fanout },
		});
		const db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { fanout },
			},
		);

		let exchangeHeadsRpcSends = 0;
		const rpcAny: any = db1.log.rpc;
		const originalSend = rpcAny.send.bind(rpcAny);
		rpcAny.send = async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				exchangeHeadsRpcSends++;
			}
			return originalSend(...args);
		};

		await db1.add("fanout-delivery", { target: "all" });

		await waitForResolved(async () => {
			const values = (await db2.log.log.toArray()).map(
				(entry) => entry.payload.getValue().value,
			);
			expect(values).to.include("fanout-delivery");
		});

		expect(exchangeHeadsRpcSends).to.equal(0);
	});

	it("resolves fanout root via topic-root-control-plane when root is omitted", async () => {
		session = await TestSession.connected(2);

		const writerRoot = (session.peers[0].services as any).fanout.publicKeyHash as string;
		for (const peer of session.peers) {
			(peer.services.pubsub as any)?.topicRootControlPlane?.setTopicRootCandidates?.([
				writerRoot,
			]);
		}

		const fanout = {
			channel: {
				msgRate: 10,
				msgSize: 256,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: true,
			},
			join: { timeoutMs: 10_000 },
		};

		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { fanout },
		});
		const db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { fanout },
			},
		);

		let exchangeHeadsRpcSends = 0;
		const rpcAny: any = db1.log.rpc;
		const originalSend = rpcAny.send.bind(rpcAny);
		rpcAny.send = async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				exchangeHeadsRpcSends++;
			}
			return originalSend(...args);
		};

		await db1.add("fanout-root-auto", { target: "all" });

		await waitForResolved(async () => {
			const values = (await db2.log.log.toArray()).map(
				(entry) => entry.payload.getValue().value,
			);
			expect(values).to.include("fanout-root-auto");
		});

		expect(exchangeHeadsRpcSends).to.equal(0);
	});

	it("does not fall back to rpc on target=all when a fanout member drops", async () => {
		session = await TestSession.connected(3);

		const root = (session.peers[0].services as any).fanout.publicKeyHash as string;
		const fanout = {
			root,
			channel: {
				msgRate: 10,
				msgSize: 256,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: true,
			},
			join: { timeoutMs: 10_000 },
		};

		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { fanout },
		});
		const db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { fanout },
			},
		);
		await EventStore.open<EventStore<string, any>>(db1.address!, session.peers[2], {
			args: { fanout },
		});

		const fanoutChannel: any = (db1.log as any)._fanoutChannel;
		await waitForResolved(() => {
			const peers = new Set(fanoutChannel.getPeerHashes());
			expect(peers.has(session.peers[1].identity.publicKey.hashcode())).to.equal(
				true,
			);
		});

		let exchangeHeadsRpcSends = 0;
		const rpcAny: any = db1.log.rpc;
		const originalSend = rpcAny.send.bind(rpcAny);
		rpcAny.send = async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				exchangeHeadsRpcSends++;
			}
			return originalSend(...args);
		};

		await session.peers[2].stop();
		await db1.add("fanout-churn", { target: "all" });

		await waitForResolved(async () => {
			const values = (await db2.log.log.toArray()).map(
				(entry) => entry.payload.getValue().value,
			);
			expect(values).to.include("fanout-churn");
		});

		expect(exchangeHeadsRpcSends).to.equal(0);
	});

	it("does not fall back to rpc when fanout publish fails", async () => {
		session = await TestSession.connected(2);

		const root = (session.peers[0].services as any).fanout.publicKeyHash as string;
		const fanout = {
			root,
			channel: {
				msgRate: 10,
				msgSize: 256,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: true,
			},
			join: { timeoutMs: 10_000 },
		};

		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { fanout },
		});
		await EventStore.open<EventStore<string, any>>(db1.address!, session.peers[1], {
			args: { fanout },
		});

		let exchangeHeadsRpcSends = 0;
		const rpcAny: any = db1.log.rpc;
		const originalSend = rpcAny.send.bind(rpcAny);
		rpcAny.send = async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				exchangeHeadsRpcSends++;
			}
			return originalSend(...args);
		};

		const fanoutChannel: any = (db1.log as any)._fanoutChannel;
		expect(fanoutChannel).to.exist;
		fanoutChannel.publish = async () => {
			throw new Error("fanout publish failed");
		};

		await expect(
			db1.add("fanout-fail", { target: "all" }),
		).to.be.rejectedWith("fanout publish failed");

		expect(exchangeHeadsRpcSends).to.equal(0);
	});

	it("settles towards the current replicators, not gid peer history", async () => {
		session = await TestSession.connected(3);

		const store = new EventStore<string, any>();

		const writer = await session.peers[0].open(store, {
			args: {
				replicas: { min: 1 },
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});

		const replicator1 = await session.peers[1].open(writer.clone(), {
			args: {
				replicas: { min: 1 },
				replicate: { offset: 0, factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const replicator2 = await session.peers[2].open(writer.clone(), {
			args: {
				replicas: { min: 1 },
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});

		await writer.log.waitForReplicators({
			coverageThreshold: 1,
			roleAge: 0,
			timeout: 15e3,
		});

		const getSingleLeader = async (entry: any): Promise<string> => {
			const leaders = await writer.log.findLeadersFromEntry(entry, 1, {
				roleAge: 0,
			});
			expect(leaders.size).to.equal(1);
			return [...leaders.keys()][0];
		};

		const initial = await writer.log.append({ op: "ADD", value: `seed` }, {
			target: "replicators",
		});
		const firstLeader = await getSingleLeader(initial.entry);

		const capturedModes: any[] = [];
		let capture = false;
		const rpc: any = writer.log.rpc;
		const originalSend = rpc.send.bind(rpc);
		rpc.send = async (...args: any[]) => {
			const message = args[0];
			const options = args[1];
			if (capture && message instanceof ExchangeHeadsMessage) {
				capturedModes.push(options?.mode);
			}
			return originalSend(...args);
		};

		// Flip the current replicator from peer 1 -> peer 2.
		const replicator2Hash = session.peers[2].identity.publicKey.hashcode();
		await replicator2.log.replicate({ offset: 0, factor: 1 }, { reset: true });
		await replicator1.log.unreplicate();

		await waitForResolved(async () => {
			const currentLeader = await getSingleLeader(initial.entry);
			expect(currentLeader).to.equal(replicator2Hash);
		});

		capturedModes.length = 0;
		capture = true;
		const res = await writer.log.append({ op: "ADD", value: `value` }, {
			target: "replicators",
			delivery: { settle: { min: 1 }, timeout: 15e3 },
		});
		capture = false;

		const leader = await getSingleLeader(res.entry);
		expect(leader).to.equal(replicator2Hash);

		const ackModes = capturedModes.filter(
			(mode) => mode instanceof AcknowledgeDelivery,
		) as AcknowledgeDelivery[];

		expect(ackModes).to.have.length(1);
		expect(ackModes[0].to).to.deep.equal([leader]);
		expect(firstLeader).to.not.equal(leader);
	});
});
