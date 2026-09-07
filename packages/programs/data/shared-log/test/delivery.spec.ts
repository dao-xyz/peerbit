import type {
	FanoutChannel,
	FanoutTree,
	FanoutTreeDataEvent,
} from "@peerbit/pubsub";
import {
	ACK,
	AcknowledgeDelivery,
	DataMessage,
	FOREGROUND_READ_MESSAGE_PRIORITY,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import {
	EXCHANGE_HEADS_REPAIR_HINT,
	ExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import {
	NoPeersError,
	type SyncProfileEvent,
	createReplicationDomainHash,
} from "../src/index.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

// The ack gates below resolve only if the LOCAL publish interceptor recorded the
// foreground message id before the REMOTE interceptor sees its ACK. When the
// native (rust-core) data plane plans and sends the delivery without passing
// through the patched JS `publishMessage`, nothing is recorded, the gate never
// matches, and a bare `await ackAttempted.promise` hangs until mocha's 60s
// timeout with no clue why -- which is exactly how this suite failed
// intermittently in the Native CI job (~13% of runs) while reporting nothing
// but "Timeout of 60000ms exceeded".
//
// Bounding the wait does not remove the race; it turns a silent hang into a
// failure that names the state needed to diagnose it. `gate` is released on the
// failure path so the in-flight append cannot wedge teardown.
const awaitAckGate = async (
	ackAttempted: { promise: Promise<void> },
	gate: { resolve: () => void },
	describeState: () => string,
	timeoutMs = 20e3,
) => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		await Promise.race([
			ackAttempted.promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() =>
						reject(
							new Error(
								`ack gate never fired within ${timeoutMs}ms — ${describeState()}`,
							),
						),
					timeoutMs,
				);
			}),
		]);
	} catch (error) {
		gate.resolve();
		throw error;
	} finally {
		if (timer) clearTimeout(timer);
	}
};

type DeliveryPeerServices = TestSession["peers"][number]["services"] & {
	fanout: FanoutTree;
};

const getDeliveryServices = (
	peer: TestSession["peers"][number],
): DeliveryPeerServices => peer.services as DeliveryPeerServices;

const hasNonRepairExchangeHeadsForEntry = (
	messages: ExchangeHeadsMessage<any>[],
	hash: string,
) =>
	messages.some(
		(message) =>
			(message.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT) === 0 &&
			message.heads.some(({ entry }) => entry.hash === hash),
	);

// Fanout routing does not depend on ownership or adaptive replication. Keep
// received entries explicitly so those background processes cannot supply the
// delivery that these tests intend to observe.
const fanoutOnlyArgs = {
	replicate: false as const,
	keep: () => true,
	setup: {
		type: "u32" as const,
		domain: createReplicationDomainHash("u32"),
		syncronizer: SimpleSyncronizer,
		name: "fanout-routing",
	},
	sync: { rawExchangeHeads: false },
};

describe("append delivery options", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("carries the top-level client upload limit into an opened SharedLog fanout channel", async () => {
		const peer = await Peerbit.create({
			pubsubUploadLimitBps: 20_000_000,
		});
		try {
			const fanout = peer.services.fanout;
			const root = fanout.publicKeyHash;
			const store = await peer.open(new EventStore<string, any>(), {
				args: { fanout: { root } },
			});
			const channel = (store.log as any)._fanoutChannel;

			expect(
				fanout.getChannelStats(channel.topic, channel.root)?.uploadLimitBps,
			).to.equal(20_000_000);
		} finally {
			await peer.stop();
		}
	});

	it("profiles fanout open with aggregate diagnostics and no peer identifiers", async () => {
		const peer = await Peerbit.create();
		try {
			const profileEvents: SyncProfileEvent[] = [];
			const root = peer.services.fanout.publicKeyHash;
			await peer.open(new EventStore<string, any>(), {
				args: {
					fanout: { root },
					sync: {
						profile: (event) => {
							profileEvents.push(event);
							if (event.name === "sharedLog.open.fanout") {
								throw new Error("diagnostic sink failure");
							}
						},
					},
				},
			});

			const event = profileEvents.find(
				(candidate) => candidate.name === "sharedLog.open.fanout",
			);
			expect(event).to.exist;
			expect(event?.component).to.equal("shared-log");
			expect(event?.durationMs).to.be.a("number");
			expect(event?.details).to.include({
				configured: true,
				mode: "root",
				outcome: "opened",
				joinReqSent: 0,
				bootstrapDialAttempts: 0,
				candidateDialAttempts: 0,
			});
			expect(event).not.to.have.property("peer");
			expect(
				Object.keys(event?.details ?? {}).some((key) =>
					/peer|topic|address/i.test(key),
				),
			).to.equal(false);
		} finally {
			await peer.stop();
		}
	});

	it("merges defined fanout channel overrides without mutation or implicit opt-in", async () => {
		session = await TestSession.connected(1);
		const peer = session.peers[0] as any;
		const defaults = {
			fanout: {
				channel: {
					uploadLimitBps: 20_000_000,
					msgRate: 30,
					repair: true,
				},
			},
		};
		const defaultsSnapshot = structuredClone(defaults);
		peer.sharedLogNativeDefaults = defaults;
		const fanout = getDeliveryServices(session.peers[0]).fanout;
		const root = fanout.publicKeyHash;

		const inherited = await peer.open(new EventStore<string, any>(), {
			args: { fanout: { root } },
		});
		const inheritedChannel = (inherited.log as any)._fanoutChannel;
		expect(
			fanout.getChannelStats(inheritedChannel.topic, inheritedChannel.root)
				?.uploadLimitBps,
		).to.equal(20_000_000);

		const numbered = await peer.open(new EventStore<string, any>(), {
			args: {
				fanout: { root, channel: { uploadLimitBps: 7_000_000 } },
			},
		});
		const numberedChannel = (numbered.log as any)._fanoutChannel;
		expect(
			fanout.getChannelStats(numberedChannel.topic, numberedChannel.root)
				?.uploadLimitBps,
		).to.equal(7_000_000);

		const zeroAndFalseArgs = {
			fanout: {
				root,
				channel: {
					uploadLimitBps: 0,
					msgRate: undefined,
					repair: false,
				},
			},
		};
		const zeroAndFalseSnapshot = structuredClone(zeroAndFalseArgs);
		const zeroAndFalse = await peer.open(new EventStore<string, any>(), {
			args: zeroAndFalseArgs,
		});
		const zeroAndFalseChannel = (zeroAndFalse.log as any)._fanoutChannel;
		const zeroAndFalseProperties = (zeroAndFalse.log as any)._logProperties
			.fanout.channel;
		expect(
			fanout.getChannelStats(
				zeroAndFalseChannel.topic,
				zeroAndFalseChannel.root,
			)?.uploadLimitBps,
		).to.equal(0);
		expect(zeroAndFalseProperties.msgRate).to.equal(30);
		expect(zeroAndFalseProperties.repair).to.equal(false);

		const undefinedOverrideArgs = {
			fanout: { root, channel: { uploadLimitBps: undefined } },
		};
		const undefinedOverride = await peer.open(new EventStore<string, any>(), {
			args: undefinedOverrideArgs,
		});
		const undefinedOverrideChannel = (undefinedOverride.log as any)
			._fanoutChannel;
		expect(
			fanout.getChannelStats(
				undefinedOverrideChannel.topic,
				undefinedOverrideChannel.root,
			)?.uploadLimitBps,
		).to.equal(20_000_000);
		expect(
			Object.prototype.hasOwnProperty.call(
				undefinedOverrideArgs.fanout.channel,
				"uploadLimitBps",
			),
		).to.equal(true);
		expect(undefinedOverrideArgs.fanout.channel.uploadLimitBps).to.equal(
			undefined,
		);

		const noFanout = await peer.open(new EventStore<string, any>());
		expect((noFanout.log as any)._fanoutChannel).to.equal(undefined);
		expect((noFanout.log as any)._logProperties.fanout).to.equal(undefined);

		expect(defaults).to.deep.equal(defaultsSnapshot);
		expect(zeroAndFalseArgs).to.deep.equal(zeroAndFalseSnapshot);
		expect((zeroAndFalse.log as any)._logProperties.fanout).not.to.equal(
			zeroAndFalseArgs.fanout,
		);
		expect(zeroAndFalseProperties).not.to.equal(
			zeroAndFalseArgs.fanout.channel,
		);
	});

	it("waits for remote delivery ownership when local coverage is already complete", async () => {
		session = await TestSession.connected(2);
		const args = {
			replicas: { min: 2 },
			replicate: { offset: 0, factor: 1 },
			timeUntilRoleMaturity: 0,
		};
		const db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args,
		});
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const gate = pDefer<void>();
		const entered = pDefer<void>();
		const originalReceive = log.handleReplicationInfoV2Announcement.bind(log);
		log.handleReplicationInfoV2Announcement = async (...parameters: any[]) => {
			if (parameters[1].from.hashcode() === remoteHash) {
				entered.resolve();
				await gate.promise;
			}
			return originalReceive(...parameters);
		};
		try {
			await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{ args },
			);
			await entered.promise;
			// These two fences intentionally reproduce the old delivery fixture:
			// the local full replica supplies coverage; subscription precedes apply.
			await db1.log.waitForReplicators({
				coverageThreshold: 1,
				roleAge: 0,
				timeout: 15e3,
			});
			const subscribers = await session.peers[0].services.pubsub.getSubscribers(
				db1.log.rpc.topic,
			);
			expect(subscribers?.map((peer) => peer.hashcode())).to.include(
				remoteHash,
			);
			expect((await db1.log.getReplicators()).has(remoteHash)).to.be.false;
			await expect(
				db1.add("not-yet-a-delivery-recipient", {
					target: "replicators",
					delivery: { reliability: "best-effort", requireRecipients: true },
				}),
			).to.be.rejectedWith(NoPeersError);

			const ready = db1.log.waitForReplicator(remoteKey, {
				roleAge: 0,
				timeout: 15e3,
			});
			gate.resolve();
			await ready;
			expect((await db1.log.getReplicators()).has(remoteHash)).to.be.true;
			await db1.add("delivery-recipient-admitted", {
				target: "replicators",
				delivery: { reliability: "best-effort", requireRecipients: true },
			});
		} finally {
			gate.resolve();
			log.handleReplicationInfoV2Announcement = originalReceive;
		}
	});

	it("awaits transport acks and applies an explicit priority for target=replicators", async () => {
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
		await db1.log.waitForReplicator(session.peers[1].identity.publicKey, {
			roleAge: 0,
			timeout: 15e3,
		});

		const gate = pDefer<void>();
		const ackAttempted = pDefer<void>();
		const deliveryPriorityByMessageId = new Map<string, number | undefined>();
		let foregroundMessageId: string | undefined;
		let foregroundAckedMessageId: string | undefined;
		const toB64 = (id: Uint8Array) => Buffer.from(id).toString("base64");

		const remotePubsub: any = session.peers[1].services.pubsub;
		const originalPublishMessage =
			remotePubsub.publishMessage.bind(remotePubsub);
		remotePubsub.publishMessage = async (...args: any[]) => {
			const message = args[1];
			if (message instanceof ACK) {
				const acknowledgedId = toB64(message.messageIdToAcknowledge);
				// Native convergence can emit unrelated acknowledged traffic during
				// this append. Gate only the exact foreground delivery under test.
				if (
					deliveryPriorityByMessageId.get(acknowledgedId) ===
					FOREGROUND_READ_MESSAGE_PRIORITY
				) {
					foregroundAckedMessageId = acknowledgedId;
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
				const messageId = toB64(message.id);
				deliveryPriorityByMessageId.set(messageId, message.header.priority);
				if (message.header.priority === FOREGROUND_READ_MESSAGE_PRIORITY) {
					foregroundMessageId = messageId;
				}
			}
			return originalLocalPublishMessage(...args);
		};

		let resolved = false;
		const promise = db1
			.add("hello", {
				target: "replicators",
				delivery: { priority: FOREGROUND_READ_MESSAGE_PRIORITY },
			})
			.then((result) => {
				resolved = true;
				return result;
			});

		await awaitAckGate(
			ackAttempted,
			gate,
			() =>
				`foregroundMessageId=${foregroundMessageId ?? "(never published via the JS path)"}, ` +
				`recorded=${JSON.stringify([...deliveryPriorityByMessageId])}`,
		);
		expect(resolved).to.equal(false);
		expect(foregroundAckedMessageId).to.equal(foregroundMessageId);
		expect(deliveryPriorityByMessageId.get(foregroundAckedMessageId!)).to.equal(
			FOREGROUND_READ_MESSAGE_PRIORITY,
		);

		gate.resolve();
		await promise;
		expect(resolved).to.equal(true);
	});

	it("uses best-effort delivery when reliability is set to best-effort", async () => {
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
		await db1.log.waitForReplicator(session.peers[1].identity.publicKey, {
			roleAge: 0,
			timeout: 15e3,
		});

		const capturedModes: any[] = [];
		const rpcAny: any = db1.log.rpc;
		const originalSend = rpcAny.send.bind(rpcAny);
		rpcAny.send = async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				capturedModes.push(args[1]?.mode);
			}
			return originalSend(...args);
		};

		await db1.add("hello-best-effort", {
			target: "replicators",
			delivery: {
				reliability: "best-effort",
				requireRecipients: true,
				timeout: 15e3,
			},
		});

		expect(capturedModes.length).to.be.greaterThan(0);
		expect(
			capturedModes.every((mode) => mode instanceof SilentDelivery),
		).to.equal(true);
	});

	it("awaits fanout unicast acks when delivery is set for target=replicators and fanout is configured", async () => {
		session = await TestSession.connected(2);

		const remoteHash = session.peers[1].identity.publicKey.hashcode();

		const root = getDeliveryServices(session.peers[0]).fanout.publicKeyHash;
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
			args: {
				fanout,
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
					fanout,
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

		await waitForResolved(async () => {
			const ch: any = (db1.log as any)._fanoutChannel;
			expect(ch, "expected shared-log to open a fanout channel").to.exist;
			const peers = ch.getPeerHashes({ includeSelf: true });
			expect(
				peers,
				"expected fanout overlay to include remote peer",
			).to.include(remoteHash);
		});

		// Ensure the remote peer is known as a replicator before we append; otherwise the
		// writer may compute a leader set that only includes itself and skip directed delivery.
		await db1.log.waitForReplicator(session.peers[1].identity.publicKey, {
			timeout: 15e3,
			roleAge: 0,
		});

		// Force fanout-token ACK routing for this test so we exercise the fanout unicast
		// ACK path (instead of a directstream-hinted ACK shortcut in 2-peer setups).
		const localPubsub: any = session.peers[0].services.pubsub;
		const originalGetUnifiedRouteHints =
			localPubsub.getUnifiedRouteHints?.bind(localPubsub);
		if (originalGetUnifiedRouteHints) {
			localPubsub.getUnifiedRouteHints = async (
				topic: string,
				targetHash: string,
			) => {
				const hints = await Promise.resolve(
					originalGetUnifiedRouteHints(topic, targetHash),
				);
				return (hints ?? []).filter(
					(hint: any) => hint?.kind === "fanout-token",
				);
			};
		}

		const gate = pDefer<void>();
		const ackAttempted = pDefer<void>();

		const remoteFanout = getDeliveryServices(session.peers[1]).fanout;
		const originalPublishMessage =
			remoteFanout.publishMessage.bind(remoteFanout);
		remoteFanout.publishMessage = async (
			...args: Parameters<typeof remoteFanout.publishMessage>
		) => {
			const message = args[1];
			if (message instanceof DataMessage) {
				const raw: any = message.data;
				const bytes: Uint8Array | undefined =
					raw instanceof Uint8Array ? raw : raw?.subarray?.();
				// FanoutTree internal: MSG_UNICAST_ACK = 17
				if (bytes?.[0] === 17) {
					ackAttempted.resolve();
					await gate.promise;
				}
			}
			return originalPublishMessage(...args);
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

		await awaitAckGate(
			ackAttempted,
			gate,
			() => "no acknowledged delivery reached the remote publish interceptor",
		);
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

		const root = getDeliveryServices(session.peers[0]).fanout.publicKeyHash;
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
		await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { fanout },
			},
		);

		await expect(
			(db1.add as any)("bad-delivery-all", {
				target: "all",
				delivery: true,
			}),
		).to.be.rejectedWith(
			'delivery options are not supported with target="all"',
		);
	});

	it("throws on target=all when fanout channel is not configured", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore<string, any>());
		await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
		);

		await expect(
			db1.add("missing-fanout", { target: "all" }),
		).to.be.rejectedWith("No fanout channel configured");
	});

	it("uses fanout data plane for target=all when configured", async () => {
		session = await TestSession.connected(2);

		const root = getDeliveryServices(session.peers[0]).fanout.publicKeyHash;
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
			args: { ...fanoutOnlyArgs, fanout },
		});
		const db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { ...fanoutOnlyArgs, fanout },
			},
		);

		const exchangeHeadsRpcMessages: ExchangeHeadsMessage<any>[] = [];
		const rpcAny: any = db1.log.rpc;
		const originalSend = rpcAny.send.bind(rpcAny);
		rpcAny.send = async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				exchangeHeadsRpcMessages.push(args[0]);
			}
			return originalSend(...args);
		};

		const { entry } = await db1.add("fanout-delivery", { target: "all" });

		await waitForResolved(async () => {
			const values = (await db2.log.log.toArray()).map(
				(entry) => entry.payload.getValue().value,
			);
			expect(values).to.include("fanout-delivery");
		});

		expect(
			hasNonRepairExchangeHeadsForEntry(exchangeHeadsRpcMessages, entry.hash),
		).to.equal(false);
	});

	it("resolves fanout root via topic-root-control-plane when root is omitted", async () => {
		session = await TestSession.connected(2);

		const writerRoot = getDeliveryServices(session.peers[0]).fanout
			.publicKeyHash;
		// Configure a deterministic root for this log topic without mutating the
		// pubsub shard-root candidate set (which can otherwise break RPC delivery).
		const store = new EventStore<string, any>();
		const topic = store.log.topic;
		for (const peer of session.peers) {
			const plane = getDeliveryServices(peer).fanout.topicRootControlPlane;
			expect(plane, "expected fanout to expose topicRootControlPlane").to.exist;
			plane.setTopicRoot(topic, writerRoot);
			expect(await plane.resolveTopicRoot(topic)).to.equal(writerRoot);
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

		const db1 = await session.peers[0].open(store, {
			args: { ...fanoutOnlyArgs, fanout },
		});
		const db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { ...fanoutOnlyArgs, fanout },
			},
		);

		await waitForResolved(async () => {
			const ch: any = (db1.log as any)._fanoutChannel;
			expect(ch, "expected shared-log to open a fanout channel").to.exist;
			const peers = ch.getPeerHashes({ includeSelf: true });
			expect(
				peers,
				"expected fanout overlay to include remote peer",
			).to.include(session.peers[1].identity.publicKey.hashcode());
		});

		const exchangeHeadsRpcMessages: ExchangeHeadsMessage<any>[] = [];
		const rpcAny: any = db1.log.rpc;
		const originalSend = rpcAny.send.bind(rpcAny);
		rpcAny.send = async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				exchangeHeadsRpcMessages.push(args[0]);
			}
			return originalSend(...args);
		};

		const { entry } = await db1.add("fanout-root-auto", { target: "all" });

		await waitForResolved(async () => {
			const values = (await db2.log.log.toArray()).map(
				(entry) => entry.payload.getValue().value,
			);
			expect(values).to.include("fanout-root-auto");
		});

		expect(
			hasNonRepairExchangeHeadsForEntry(exchangeHeadsRpcMessages, entry.hash),
		).to.equal(false);
	});

	for (const publishFails of [false, true]) {
		it(
			publishFails
				? "does not fall back to rpc when fanout publish fails during background sync"
				: "does not fall back to rpc on target=all when a fanout member drops during background sync",
			async () => {
				session = await TestSession.connected(3);
				const fanout = {
					root: getDeliveryServices(session.peers[0]).fanout.publicKeyHash,
					channel: {
						msgRate: 10,
						msgSize: 256,
						uploadLimitBps: 1_000_000,
						maxChildren: 8,
						repair: true,
					},
					join: { timeoutMs: 10_000 },
				};
				const args = { ...fanoutOnlyArgs, fanout };
				// Both remote members are leaves, so their membership is visible at
				// the root and stopping one cannot change the survivor's parent.
				const leafArgs = {
					...args,
					fanout: {
						...fanout,
						channel: { ...fanout.channel, uploadLimitBps: 0, maxChildren: 0 },
					},
				};
				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args,
				});
				const db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{ args: leafArgs },
				);
				await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{ args: leafArgs },
				);
				const fanoutChannel: FanoutChannel = (db1.log as any)._fanoutChannel;
				const receiverChannel: FanoutChannel = (db2.log as any)._fanoutChannel;
				await waitForResolved(() => {
					expect(fanoutChannel.getPeerHashes()).to.include.members(
						session.peers
							.slice(1)
							.map((peer) => peer.identity.publicKey.hashcode()),
					);
				});
				await session.peers[2].stop();

				const exchangeHeadsRpcMessages: ExchangeHeadsMessage<any>[] = [];
				const rpcAny: any = db1.log.rpc;
				const originalSend = rpcAny.send.bind(rpcAny);
				rpcAny.send = async (...sendArgs: any[]) => {
					if (sendArgs[0] instanceof ExchangeHeadsMessage) {
						exchangeHeadsRpcMessages.push(sendArgs[0]);
					}
					return originalSend(...sendArgs);
				};
				const logAny: any = db1.log;
				const originalReplicatorDelivery = logAny._appendDeliverToReplicators;
				let replicatorDeliveries = 0;
				logAny._appendDeliverToReplicators = async () => {
					replicatorDeliveries++;
					throw new Error("target=all entered replicator delivery");
				};
				const releasePublish = pDefer<void>();
				const originalPublish = fanoutChannel.publish.bind(fanoutChannel);
				const publishError = new Error("fanout publish failed");
				let publishedPayload: Uint8Array | undefined;
				fanoutChannel.publish = async (payload) => {
					publishedPayload = payload;
					await releasePublish.promise;
					if (publishFails) throw publishError;
					return originalPublish(payload);
				};
				const receivedPayloads: Uint8Array[] = [];
				const onFanoutData = (event: Event) => {
					receivedPayloads.push(
						(event as CustomEvent<FanoutTreeDataEvent>).detail.payload,
					);
				};
				receiverChannel.addEventListener("data", onFanoutData);
				const append = db1.add("fanout-churn", { target: "all" });
				// Observe an early rejection while gated assertions and the real
				// synchronization round trip are in progress.
				const appendOutcome = append.then(
					(result) => ({ result, error: undefined }),
					(error: unknown) => ({ result: undefined, error }),
				);
				try {
					await waitForResolved(() => {
						expect(publishedPayload, "append must reach fanout publish").to
							.exist;
					});
					const [entry] = await db1.log.log.toArray();
					expect(entry).to.exist;
					expect(exchangeHeadsRpcMessages).to.have.length(0);
					expect(db1.log.syncronizer).to.be.instanceOf(SimpleSyncronizer);
					// Fanout is paused, so this request/response exchange is the only
					// producer of the deliberately overlapping same-entry RPC message.
					await (
						db1.log.syncronizer as SimpleSyncronizer<any>
					).onMaybeMissingHashes({
						hashes: [entry!.hash],
						targets: [session.peers[1].identity.publicKey.hashcode()],
					});
					await waitForResolved(async () => {
						expect(await db2.log.log.toArray()).to.have.length(1);
						expect(exchangeHeadsRpcMessages).to.have.length(1);
					});
					// Prove why the old global predicate is not an append-routing oracle.
					expect(
						hasNonRepairExchangeHeadsForEntry(
							exchangeHeadsRpcMessages,
							entry!.hash,
						),
					).to.equal(true);
					const expectedSyncMessages = Object.freeze([
						...exchangeHeadsRpcMessages,
					]);
					expect(receivedPayloads).not.to.deep.include(publishedPayload);
					releasePublish.resolve();
					const outcome = await appendOutcome;
					if (publishFails) {
						expect(outcome.error).to.equal(publishError);
						expect(receivedPayloads).not.to.deep.include(publishedPayload);
					} else {
						expect(outcome.error).to.equal(undefined);
						expect(outcome.result?.entry.hash).to.equal(entry!.hash);
						// The background RPC already populated the log; observe the
						// actual fanout payload independently to prove fanout delivery.
						await waitForResolved(
							() => {
								expect(receivedPayloads).to.deep.include(publishedPayload);
							},
							{ timeout: 30_000 },
						);
					}
					expect(replicatorDeliveries).to.equal(0);
					// Exact identity and count catch any additional RPC fallback, even
					// one sending the same entry or bypassing replicator delivery.
					expect(exchangeHeadsRpcMessages).to.have.length(
						expectedSyncMessages.length,
					);
					expect(exchangeHeadsRpcMessages[0]).to.equal(expectedSyncMessages[0]);
				} finally {
					releasePublish.resolve();
					await appendOutcome;
					fanoutChannel.publish = originalPublish;
					logAny._appendDeliverToReplicators = originalReplicatorDelivery;
					rpcAny.send = originalSend;
					receiverChannel.removeEventListener("data", onFanoutData);
				}
			},
		);
	}

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

		const initial = await writer.log.append(
			{ op: "ADD", value: `seed` },
			{
				target: "replicators",
			},
		);
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
		const res = await writer.log.append(
			{ op: "ADD", value: `value` },
			{
				target: "replicators",
				delivery: { reliability: "ack", minAcks: 1, timeout: 15e3 },
			},
		);
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
