import { createStore } from "@peerbit/any-store";
import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer from "p-defer";
import path from "path";
import sinon from "sinon";
import {
	SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import {
	FullReplicationInfoV2Message,
	RequestReplicationInfoV2Message,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("receive admission replication-info V2 one-sided topic recovery", function () {
	this.timeout(20_000);
	let session: TestSession | undefined;
	let directory: string | undefined;

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
		if (directory) await fs.rm(directory, { recursive: true, force: true });
		directory = undefined;
	});

	const openPair = async (durableReceiver = false) => {
		if (durableReceiver) {
			directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-one-sided-recovery-"),
			);
			session = await TestSession.connected(2, [
				{},
				{
					directory,
					storage: {
						storeFactory: (directory?: string) => createStore(directory),
					},
					indexer: (directory?: string) => createSQLiteIndices(directory),
				},
			]);
		} else {
			session = await TestSession.connected(2);
		}
		const sender = await session.peers[0].open(new EventStore(), {
			args: { replicas: { min: 2 }, replicate: 1, timeUntilRoleMaturity: 0 },
		});
		const receiver = await EventStore.open<EventStore<string, any>>(
			sender.address!,
			session.peers[1],
			{
				args: { replicas: { min: 2 }, replicate: 1, timeUntilRoleMaturity: 0 },
			},
		);
		const senderLog = sender.log as any;
		const receiverLog = receiver.log as any;
		const senderHash = sender.node.identity.publicKey.hashcode();
		const receiverHash = receiver.node.identity.publicKey.hashcode();
		await waitForResolved(
			() => {
				for (const [log, hash] of [
					[senderLog, receiverHash],
					[receiverLog, senderHash],
				] as const) {
					expect(log._v2Receive._receiveStates.get(hash)?.phase).to.equal(
						"active",
					);
					expect(log._v2Send._sendStates.get(hash)?.established).to.equal(true);
					expect(log.uniqueReplicators.has(hash)).to.equal(true);
				}
			},
			{ timeout: 10_000 },
		);
		const target = {
			peerHash: receiverHash,
			peerSession: senderLog._peerSessions.current(receiverHash),
			receiverTransportSession:
				senderLog._peerSyncCapabilitySessions.get(receiverHash),
		};
		return {
			sender,
			receiver,
			senderLog,
			receiverLog,
			senderHash,
			receiverHash,
			target,
		};
	};

	const rotateReceiver = async (pair: Awaited<ReturnType<typeof openPair>>) => {
		const event = {
			detail: {
				from: pair.sender.node.identity.publicKey,
				topics: [pair.receiverLog.topic],
			},
		};
		await pair.receiverLog._onUnsubscription(event);
		expect(
			pair.receiverLog._peerSyncCapabilitySessions.has(pair.senderHash),
		).to.equal(false);
		expect(pair.receiverLog.uniqueReplicators.has(pair.senderHash)).to.equal(
			false,
		);
		await pair.receiverLog._onSubscription(event);
	};

	for (const departure of [
		{ name: "normal close", missDeparture: false, missReset: false },
		{ name: "only Unsubscribe lost", missDeparture: true, missReset: false },
		{
			name: "both negative announcements lost",
			missDeparture: true,
			missReset: true,
		},
	]) {
		it(`recovers a fresh receiver after ${departure.name} on the same transport`, async () => {
			const pair = await openPair();
			const membershipEvents: string[] = [];
			for (const event of ["replicator:join", "replicator:leave"] as const) {
				pair.sender.log.events.addEventListener(event, ({ detail }) => {
					if (detail.publicKey.hashcode() === pair.receiverHash) {
						membershipEvents.push(event);
					}
				});
			}
			expect([...(await pair.sender.log.getReplicators())]).to.have.members([
				pair.senderHash,
				pair.receiverHash,
			]);
			const remoteRangeIds = async () =>
				(
					await pair.sender.log.replicationIndex
						.iterate({ query: { hash: pair.receiverHash } })
						.all()
				)
					.map(({ value }) => value.idString)
					.sort();
			const previousRangeIds = await remoteRangeIds();
			expect(previousRangeIds).not.to.be.empty;
			let closing = true;
			// When the reopen starts. Pubsub stamps both copies of a departure
			// (direct and shard) in one synchronous step, before it sends either,
			// and the wait below needs one copy to arrive before the reopen
			// starts. So no copy of the closed program's departure is newer.
			let reopenStartedAt: bigint | undefined;
			let droppedDepartures = 0;
			let droppedResets = 0;
			if (departure.missDeparture) {
				const pubsub = pair.sender.node.services.pubsub as any;
				const original = pubsub.processUnsubscribeMessage.bind(pubsub);
				sinon
					.stub(pubsub, "processUnsubscribeMessage")
					.callsFake((message: any, unsubscribe: any, from: any) => {
						if (
							(reopenStartedAt === undefined ||
								message.header.timestamp <= reopenStartedAt) &&
							from.hashcode() === pair.receiverHash &&
							unsubscribe.topics.includes(pair.senderLog.topic)
						) {
							// Both signed direct and shard departures reach this verified
							// seam. They are separate messages, so one can still be in
							// flight after the wait below has counted the other and the
							// reopen has started. Drop every copy stamped before the
							// reopen, not only those seen while `closing`, and let any
							// later Unsubscribe reach the sender. Leave Subscribe,
							// capability and Full traffic untouched.
							droppedDepartures++;
							return;
						}
						return original(message, unsubscribe, from);
					});
			}
			if (departure.missReset) {
				const onMessage = pair.sender.log.onMessage.bind(pair.sender.log);
				sinon
					.stub(pair.sender.log, "onMessage")
					.callsFake(async (message, context) => {
						// This log's authenticated receive seam: discard only the closing
						// peer's empty Full, never positive Fulls or recovery traffic.
						// A close sends one such reset, and the wait below counts it
						// before `closing` ends, so this gate cannot pass a late copy.
						if (
							closing &&
							context.from?.hashcode() === pair.receiverHash &&
							message instanceof FullReplicationInfoV2Message &&
							message.segments.length === 0
						) {
							droppedResets++;
							return;
						}
						return onMessage(message, context);
					});
			}
			const receiverPeer = session!.peers[1];
			const transportSession = (receiverPeer.services.pubsub as any).session;
			const previousSenderSession = pair.senderLog._peerSessions.current(
				pair.receiverHash,
			);
			expect(previousSenderSession?.phase).to.equal("open");
			expect(await pair.receiver.close()).to.equal(true);
			await waitForResolved(
				async () => {
					expect(droppedDepartures > 0).to.equal(departure.missDeparture);
					expect(droppedResets > 0).to.equal(departure.missReset);
					expect([...(await pair.sender.log.getReplicators())]).to.have.members(
						departure.missReset
							? [pair.senderHash, pair.receiverHash]
							: [pair.senderHash],
					);
					expect(await remoteRangeIds()).to.deep.equal(
						departure.missReset ? previousRangeIds : [],
					);
					expect(membershipEvents).to.deep.equal(
						departure.missReset ? [] : ["replicator:leave"],
					);
				},
				{ timeout: 10_000 },
			);
			closing = false;
			reopenStartedAt = BigInt(Date.now());
			// A fresh program/topic instance, not a new transport generation or
			// process restart. The events above describe committed membership.
			const reopened = await EventStore.open<EventStore<string, any>>(
				pair.sender.address!,
				receiverPeer,
				{
					args: {
						replicas: { min: 2 },
						replicate: 1,
						timeUntilRoleMaturity: 0,
					},
				},
			);
			expect(reopened).not.to.equal(pair.receiver);
			expect((receiverPeer.services.pubsub as any).session).to.equal(
				transportSession,
			);
			const reopenedLog = reopened.log as any;
			await waitForResolved(
				async () => {
					const currentSenderSession = pair.senderLog._peerSessions.current(
						pair.receiverHash,
					);
					if (departure.missDeparture) {
						expect(droppedDepartures).to.be.greaterThan(0);
						expect(currentSenderSession).to.equal(previousSenderSession);
					} else {
						expect(currentSenderSession).not.to.equal(previousSenderSession);
					}
					expect(
						reopenedLog._peerSyncCapabilitySessions.get(pair.senderHash),
						"fresh receiver needs the sender's authenticated capability binding",
					).to.equal(BigInt((pair.sender.node.services.pubsub as any).session));
					for (const [log, hash] of [
						[pair.senderLog, pair.receiverHash],
						[reopenedLog, pair.senderHash],
					] as const) {
						expect(log._v2Receive._receiveStates.get(hash)?.phase).to.equal(
							"active",
						);
						expect(log._v2Send._sendStates.get(hash)?.established).to.equal(
							true,
						);
						expect(log.uniqueReplicators.has(hash)).to.equal(true);
						expect([...(await log.getReplicators())]).to.have.members([
							pair.senderHash,
							pair.receiverHash,
						]);
					}
				},
				{ timeout: 5_000 },
			);
			const { entry } = await reopened.add("after program reopen");
			await waitForResolved(
				async () => {
					expect(await pair.sender.log.log.has(entry.hash)).to.equal(true);
				},
				{ timeout: 10_000 },
			);
			expect(membershipEvents).to.deep.equal(
				departure.missReset ? [] : ["replicator:leave", "replicator:join"],
			);
		});
	}

	it("re-advertises when the peer dropped the opening advert while departing", async () => {
		const pair = await openPair();
		const receiverPeer = session!.peers[1];
		const senderKey = pair.sender.node.identity.publicKey;
		const topic = pair.senderLog.topic;
		expect(await pair.receiver.close()).to.equal(true);
		await waitForResolved(
			() =>
				expect(
					pair.senderLog._peerSessions.current(pair.receiverHash)?.phase,
				).to.equal("departing"),
			{ timeout: 5_000 },
		);

		let droppedAdverts = 0;
		let unanswerableRequests = 0;
		const advertDropped = pDefer<void>();
		const onMessage = pair.sender.log.onMessage.bind(pair.sender.log);
		sinon
			.stub(pair.sender.log, "onMessage")
			.callsFake(async (message: any, context: any) => {
				const fromReceiver = context.from?.hashcode() === pair.receiverHash;
				const departingAdvert =
					fromReceiver &&
					message instanceof SyncCapabilitiesMessage &&
					pair.senderLog._peerSessions.current(pair.receiverHash)?.phase ===
						"departing";
				if (
					fromReceiver &&
					message instanceof RequestReplicationInfoV2Message &&
					!pair.senderLog._peerSyncCapabilitySessions.has(pair.receiverHash)
				) {
					unanswerableRequests++;
				}
				const result = await onMessage(message, context);
				if (departingAdvert) {
					droppedAdverts++;
					advertDropped.resolve();
				}
				return result;
			});
		// The sender's Subscribe reaches the reopened receiver before the receiver
		// announces its own (in the wild a late shard Subscribe does this). The
		// receiver opens its session and advertises; the sender's transport ACKs
		// that advert, but its departing session drops it. Only then does the
		// receiver's Subscribe open the sender's new session, which solicits the
		// capability again with the rearm bit.
		const pubsub = receiverPeer.services.pubsub as any;
		const announce = pubsub._subscribe.bind(pubsub);
		sinon.stub(pubsub, "_subscribe").callsFake(async (...args: any[]) => {
			if (args[0].some(({ key }: { key: string }) => key === topic)) {
				await pubsub.requestSubscribers(topic, senderKey);
				await advertDropped.promise;
			}
			return announce(...args);
		});

		const reopened = await EventStore.open<EventStore<string, any>>(
			pair.sender.address!,
			receiverPeer,
			{
				args: {
					replicas: { min: 2 },
					replicate: 1,
					timeUntilRoleMaturity: 0,
				},
			},
		);
		const reopenedLog = reopened.log as any;
		expect(droppedAdverts).to.equal(1);
		await waitForResolved(
			() => {
				for (const [log, hash] of [
					[pair.senderLog, pair.receiverHash],
					[reopenedLog, pair.senderHash],
				] as const) {
					expect(log._peerSessions.current(hash)?.phase).to.equal("open");
					expect(log._v2Receive._receiveStates.get(hash)?.phase).to.equal(
						"active",
					);
					expect(log._v2Send._sendStates.get(hash)?.established).to.equal(true);
				}
			},
			{ timeout: 10_000 },
		);
		// The solicitation itself restored the sender's grant: the receiver did
		// not spend its request retries on a sender that could not answer them.
		expect(unanswerableRequests).to.equal(0);
	});

	it("restores membership after receiver-only rotation without a waiter", async () => {
		const pair = await openPair();
		const oldReceiverSession = pair.receiverLog._peerSessions.current(
			pair.senderHash,
		);
		await rotateReceiver(pair);
		expect(
			pair.receiverLog._peerSessions.current(pair.senderHash),
		).not.to.equal(oldReceiverSession);
		await waitForResolved(
			() => {
				expect(
					pair.receiverLog._peerSyncCapabilitySessions.get(pair.senderHash),
				).to.equal(BigInt((pair.sender.node.services.pubsub as any).session));
				expect(
					pair.receiverLog._v2Receive._receiveStates.get(pair.senderHash)
						?.phase,
				).to.equal("active");
				expect(
					pair.receiverLog.uniqueReplicators.has(pair.senderHash),
				).to.equal(true);
			},
			{ timeout: 5_000 },
		);
		expect(pair.senderLog._peerSessions.current(pair.receiverHash)).to.equal(
			pair.target.peerSession,
		);
		expect(
			pair.senderLog._peerSyncCapabilitySessions.get(pair.receiverHash),
		).to.equal(pair.target.receiverTransportSession);
	});

	it("fences a previous Applied confirmation before replacement Full can finish", async () => {
		const pair = await openPair();
		const acceptApplied = sinon.spy(pair.senderLog._v2Send, "acceptApplied");
		await pair.senderLog._v2Send.confirmLatestForPeer(pair.target, {
			timeout: 5_000,
		});
		expect(
			pair.senderLog._v2Send.isLatestConfirmedForPeer(pair.target),
		).to.equal(true);
		const oldApplied = acceptApplied.lastCall.args;
		const capabilityTimestamp =
			pair.senderLog._peerSyncCapabilityTimestamps.get(pair.receiverHash);
		const fullGate = pDefer<void>();
		const originalSend = pair.senderLog.rpc.send.bind(pair.senderLog.rpc);
		const send = sinon
			.stub(pair.senderLog.rpc, "send")
			.callsFake(async (...args: any[]) => {
				if (args[0] instanceof FullReplicationInfoV2Message)
					await fullGate.promise;
				return originalSend(...args);
			});
		try {
			await rotateReceiver(pair);
			await waitForResolved(
				() => {
					expect(
						pair.senderLog._peerSyncCapabilityTimestamps.get(
							pair.receiverHash,
						) > capabilityTimestamp,
					).to.equal(true);
				},
				{ timeout: 5_000 },
			);
			expect(
				pair.senderLog._v2Send.isLatestConfirmedForPeer(pair.target),
			).to.equal(false);
			expect(pair.senderLog._v2Send.acceptApplied(...oldApplied)).to.equal(
				false,
			);
			expect(
				pair.senderLog._v2Send.isLatestConfirmedForPeer(pair.target),
			).to.equal(false);
			fullGate.resolve();
			await waitForResolved(
				() => {
					expect(
						pair.receiverLog._v2Receive._receiveStates.get(pair.senderHash)
							?.phase,
					).to.equal("active");
				},
				{ timeout: 5_000 },
			);
			await pair.senderLog._v2Send.confirmLatestForPeer(pair.target, {
				timeout: 5_000,
			});
			expect(
				pair.senderLog._v2Send.isLatestConfirmedForPeer(pair.target),
			).to.equal(true);
		} finally {
			fullGate.resolve();
			send.restore();
		}
	});

	it("reciprocates a second receiver rotation before either replacement Full commits", async () => {
		const pair = await openPair();
		const fullGate = pDefer<void>();
		const sends = [pair.senderLog, pair.receiverLog].map((log) => {
			const original = log.rpc.send.bind(log.rpc);
			return sinon.stub(log.rpc, "send").callsFake(async (...args: any[]) => {
				if (args[0] instanceof FullReplicationInfoV2Message)
					await fullGate.promise;
				return original(...args);
			});
		});
		try {
			await rotateReceiver(pair);
			await waitForResolved(
				() =>
					expect(
						pair.receiverLog._peerSyncCapabilitySessions.has(pair.senderHash),
					).to.equal(true),
				{ timeout: 5_000 },
			);
			expect(
				pair.senderLog._v2Receive._receiveStates.get(pair.receiverHash).phase,
			).to.equal("resync");
			const firstSession = pair.receiverLog._peerSessions.current(
				pair.senderHash,
			);
			await rotateReceiver(pair);
			expect(
				pair.receiverLog._peerSessions.current(pair.senderHash),
			).not.to.equal(firstSession);
			await waitForResolved(
				() =>
					expect(
						pair.receiverLog._peerSyncCapabilitySessions.has(pair.senderHash),
					).to.equal(true),
				{ timeout: 5_000 },
			);
			fullGate.resolve();
			await waitForResolved(
				() => {
					expect(
						pair.receiverLog._v2Receive._receiveStates.get(pair.senderHash)
							?.phase,
					).to.equal("active");
					expect(
						pair.senderLog._v2Receive._receiveStates.get(pair.receiverHash)
							?.phase,
					).to.equal("active");
					expect(
						pair.receiverLog.uniqueReplicators.has(pair.senderHash),
					).to.equal(true);
				},
				{ timeout: 5_000 },
			);
		} finally {
			fullGate.resolve();
			for (const send of sends) send.restore();
		}
	});

	it("obtains a durable receipt after a previously confirmed receiver rotates alone", async () => {
		const pair = await openPair(true);
		const { entry } = await pair.sender.add("one-sided durable delivery", {
			target: "none",
		});
		await pair.senderLog.waitForPersistedReceiptPeerReadiness(
			pair.receiver.node.identity.publicKey,
			{
				entries: [entry],
				replicas: 2,
				timeout: 5_000,
			},
		);
		expect(
			pair.senderLog._v2Send.isLatestConfirmedForPeer(pair.target),
		).to.equal(true);
		await rotateReceiver(pair);
		await pair.senderLog.deliverPersistedEntries([entry], {
			target: "replicators",
			delivery: { reliability: "persisted", minAcks: 1, timeout: 5_000 },
		});
		expect(await pair.receiver.log.log.has(entry.hash)).to.equal(true);
		expect(pair.senderLog._peerSessions.current(pair.receiverHash)).to.equal(
			pair.target.peerSession,
		);
	});

	it("does not echo healthy capability refreshes or reapply an exact recovery hint", async () => {
		const pair = await openPair();
		await pair.senderLog._v2Send.confirmLatestForPeer(pair.target, {
			timeout: 5_000,
		});
		const oldSendState = pair.senderLog._v2Send._sendStates.get(
			pair.receiverHash,
		);
		const oldReceiveState = pair.senderLog._v2Receive._receiveStates.get(
			pair.receiverHash,
		);
		const oldChallenge = oldReceiveState.receiverRequestChallenge.slice();
		const senderRefresh = sinon.spy(
			pair.senderLog,
			"advertiseReplicationInfoV2ReceiveCapability",
		);
		const senderReceive = sinon.spy(pair.senderLog, "onMessage");
		for (let i = 0; i < 2; i++) {
			const previous = pair.senderLog._peerSyncCapabilityTimestamps.get(
				pair.receiverHash,
			);
			await pair.receiverLog.advertiseReplicationInfoV2ReceiveCapability({
				target: pair.sender.node.identity.publicKey,
				peerSession: pair.receiverLog._peerSessions.current(pair.senderHash),
				receiveEpoch: pair.receiverLog._peerSessions.receiveEpoch(
					pair.senderHash,
				),
				signal:
					pair.receiverLog._instanceLifecycle.membershipLifecycleController
						.signal,
			});
			await waitForResolved(
				() =>
					expect(
						pair.senderLog._peerSyncCapabilityTimestamps.get(
							pair.receiverHash,
						) > previous,
					).to.equal(true),
				{ timeout: 5_000 },
			);
		}
		expect(senderRefresh.callCount).to.equal(0);
		expect(pair.senderLog._v2Send._sendStates.get(pair.receiverHash)).to.equal(
			oldSendState,
		);
		expect(
			pair.senderLog._v2Send.isLatestConfirmedForPeer(pair.target),
		).to.equal(true);
		expect([...oldReceiveState.receiverRequestChallenge]).to.deep.equal([
			...oldChallenge,
		]);

		await rotateReceiver(pair);
		await waitForResolved(
			() => {
				expect(
					pair.receiverLog._v2Receive._receiveStates.get(pair.senderHash)
						?.phase,
				).to.equal("active");
				expect(
					pair.senderLog._v2Receive._receiveStates.get(pair.receiverHash)
						?.phase,
				).to.equal("active");
			},
			{ timeout: 5_000 },
		);
		const hint = senderReceive
			.getCalls()
			.find(
				(call) =>
					call.args[0] instanceof SyncCapabilitiesMessage &&
					(call.args[0].capabilities &
						SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM) !==
						0,
			);
		expect(hint).to.exist;
		const currentReceive = pair.senderLog._v2Receive._receiveStates.get(
			pair.receiverHash,
		);
		const currentSend = pair.senderLog._v2Send._sendStates.get(
			pair.receiverHash,
		);
		const version = currentReceive.version;
		const replyCount = senderRefresh.callCount;
		await pair.senderLog.onMessage(...hint!.args);
		expect(currentReceive.version).to.equal(version);
		expect(currentReceive.phase).to.equal("active");
		expect(pair.senderLog._v2Send._sendStates.get(pair.receiverHash)).to.equal(
			currentSend,
		);
		expect(senderRefresh.callCount).to.equal(replyCount);
	});
});
