import { createStore } from "@peerbit/any-store";
import { calculateRawCid } from "@peerbit/blocks-interface";
import {
	Ed25519Keypair,
	Ed25519PublicKey,
	X25519Keypair,
	X25519PublicKey,
	X25519SecretKey,
	randomBytes,
} from "@peerbit/crypto";
import { toId } from "@peerbit/indexer-interface";
import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import {
	EntryType,
	LamportClock,
	ShallowEntry,
	ShallowMeta,
	Timestamp,
} from "@peerbit/log";
import { ClosedError } from "@peerbit/program";
import { AcknowledgeDelivery, SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { AbortError, TimeoutError, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import pDefer from "p-defer";
import path from "path";
import sinon from "sinon";
import { PersistedDeliveryError } from "../src/errors.js";
import {
	EXCHANGE_HEADS_REPAIR_HINT,
	ExchangeHeadsMessage,
	SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM,
	SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import {
	FullReplicationInfoV2Message,
	ReplicationInfoV2AppliedMessage,
	RequestReplicationInfoV2AppliedMessage,
} from "../src/replication.js";
import {
	ConfirmEntriesMessage,
	RequestPersistedEntriesV1,
} from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

describe("append delivery options — persisted receipts", function () {
	this.timeout(60_000);
	const planningRecords = (log: any, entries: any[]) =>
		entries.map((entry) => log.snapshotPersistedDeliveryPlanningEntry(entry));
	const allowPersistedReceiptFreshness = (log: any) =>
		sinon.stub(log._v2Send, "confirmLatestForPeer").resolves();

	let session: TestSession | undefined;
	let directory: string | undefined;

	const crashSafeDirectoryOptions = (directory: string) => ({
		directory,
		storage: {
			storeFactory: (storeDirectory?: string) => createStore(storeDirectory),
		},
		indexer: (indexDirectory?: string) => createSQLiteIndices(indexDirectory),
	});

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	const openPair = async (
		durable: boolean,
		receiverCanAppend?: () => boolean,
	) => {
		if (durable) {
			directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-persisted-delivery-"),
			);
			session = await TestSession.connected(2, [
				crashSafeDirectoryOptions(path.join(directory, "writer")),
				crashSafeDirectoryOptions(path.join(directory, "receiver")),
			]);
		} else {
			session = await TestSession.connected(2);
		}

		const writer = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicas: { min: 2 },
				replicate: { offset: 0, factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const receiver = await EventStore.open<EventStore<string, any>>(
			writer.address!,
			session.peers[1],
			{
				args: {
					replicas: { min: 2 },
					replicate: { offset: 0, factor: 1 },
					timeUntilRoleMaturity: 0,
					canAppend: receiverCanAppend,
				},
			},
		);

		await writer.log.waitForReplicators({
			coverageThreshold: 1,
			roleAge: 0,
			timeout: 15_000,
		});
		await writer.log.waitForReplicator(receiver.node.identity.publicKey, {
			roleAge: 0,
			timeout: 15_000,
		});
		return { writer, receiver };
	};

	const waitForPersistedCapability = async (
		writer: EventStore<string, any>,
		receiver: EventStore<string, any>,
	) => {
		const receiverHash = receiver.node.identity.publicKey.hashcode();
		await waitForResolved(
			() => {
				const writerLog = writer.log as any;
				const receiverLog = receiver.log as any;
				const capabilities = writerLog._peerSyncCapabilities.get(receiverHash);
				expect(
					capabilities & SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
				).to.equal(SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS);
				for (const [hostLog, remote] of [
					[writerLog, receiver],
					[receiverLog, writer],
				] as const) {
					const peerHash = remote.node.identity.publicKey.hashcode();
					const peerSession = hostLog._peerSessions.current(peerHash);
					const senderTransportSession =
						hostLog._peerSyncCapabilitySessions.get(peerHash);
					expect(peerSession).to.exist;
					expect(senderTransportSession).to.not.equal(undefined);
					expect(
						hostLog._v2Receive.isCurrentActive({
							peerHash,
							peerSession,
							receiveEpoch: hostLog._peerSessions.receiveEpoch(peerHash),
							senderTransportSession,
						}),
					).to.equal(true);
				}
			},
			{ timeout: 15_000 },
		);
	};

	it("exposes capability stages and wakes when the current session becomes capable", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const capabilities = log._peerSyncCapabilities.get(receiverHash);
		const transportSession = log._peerSyncCapabilitySessions.get(receiverHash);
		const capabilityTimestamp =
			log._peerSyncCapabilityTimestamps.get(receiverHash);
		expect(capabilities).to.be.a("number");
		expect(transportSession).to.be.a("bigint");
		expect(capabilityTimestamp).to.be.a("bigint");

		const active = sinon.stub(log._v2Receive, "isCurrentActive").returns(true);
		const confirmed = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.returns(true);
		const readinessEvents: string[] = [];
		const readinessDetailsFrozen: boolean[] = [];
		const onReadiness = (event: any) => {
			readinessEvents.push(event.detail.peerHash);
			readinessDetailsFrozen.push(Object.isFrozen(event.detail));
		};
		log.events.addEventListener("persisted-receipt:readiness", onReadiness);
		try {
			const ready = await log.getPersistedReceiptPeerReadiness(receiverKey);
			expect(ready.status).to.equal("ready");
			expect(Object.isFrozen(ready)).to.equal(true);

			log._peerSyncCapabilities.set(
				receiverHash,
				capabilities & ~SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
			);
			const oldPeer = await log.getPersistedReceiptPeerReadiness(receiverKey);
			expect(oldPeer).to.deep.include({
				status: "unsupported",
				reason: "persisted-receipts-unsupported",
			});
			expect(Object.isFrozen(oldPeer)).to.equal(true);

			log._peerSyncCapabilities.set(
				receiverHash,
				(capabilities | SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS) &
					~SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM,
			);
			expect(
				await log.getPersistedReceiptPeerReadiness(receiverKey),
			).to.deep.include({
				status: "unsupported",
				reason: "replication-confirmation-unsupported",
			});

			log._peerSyncCapabilities.set(receiverHash, capabilities);
			log._peerSyncCapabilitySessions.delete(receiverHash);
			log._peerSyncCapabilityTimestamps.delete(receiverHash);
			expect(
				await log.getPersistedReceiptPeerReadiness(receiverKey),
			).to.deep.include({
				status: "pending",
				reason: "capability-pending",
			});

			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				timeout: 5_000,
			});
			expect(
				log.observePeerSyncCapabilities({
					peerHash: receiverHash,
					capabilities,
					transportSession,
					timestamp: capabilityTimestamp + 1n,
				}),
			).to.equal(true);
			const restored = await wait;
			expect(restored.status).to.equal("ready");
			expect(Object.isFrozen(restored)).to.equal(true);
			expect(readinessEvents).to.include(receiverHash);
			expect(readinessDetailsFrozen.every(Boolean)).to.equal(true);
		} finally {
			log.events.removeEventListener(
				"persisted-receipt:readiness",
				onReadiness,
			);
			confirmed.restore();
			active.restore();
		}
	});

	it("keeps readiness snapshots compatible while exposing bounded opt-in diagnostics", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const unknownKey = (await Ed25519Keypair.create()).publicKey;
		const hashcode = sinon.spy(unknownKey, "hashcode");

		const defaultSnapshot =
			await writer.log.getPersistedReceiptPeerReadiness(unknownKey);
		expect(hashcode.calledOnce).to.equal(true);
		expect(defaultSnapshot).to.deep.equal({
			status: "pending",
			reason: "no-current-session",
		});
		expect("diagnostic" in defaultSnapshot).to.equal(false);

		const snapshot = await writer.log.getPersistedReceiptPeerReadiness(
			unknownKey,
			{ diagnostics: true },
		);
		expect(snapshot).to.deep.include({
			status: "pending",
			reason: "no-current-session",
		});
		expect(snapshot.diagnostic).to.deep.include({ version: 1, log: "open" });
		expect(snapshot.diagnostic?.session).to.deep.include({
			state: "absent",
			established: false,
			suspended: true,
		});
		expect(snapshot.diagnostic?.sender).to.deep.include({
			state: "absent",
			reason: "state-absent",
			confirmationWaiters: 0,
		});
		expect(snapshot.diagnostic?.receiver).to.deep.include({
			state: "absent",
			reason: "state-absent",
		});
		expect(Object.isFrozen(snapshot)).to.equal(true);
		expect(Object.isFrozen(snapshot.diagnostic)).to.equal(true);
		expect(Object.isFrozen(snapshot.diagnostic?.session)).to.equal(true);
		expect(Object.isFrozen(snapshot.diagnostic?.sender)).to.equal(true);
		expect(Object.isFrozen(snapshot.diagnostic?.receiver)).to.equal(true);
		expect(JSON.stringify(snapshot.diagnostic).length).to.be.lessThanOrEqual(
			2_048,
		);

		let timeoutError: Error | undefined;
		try {
			await writer.log.waitForPersistedReceiptPeerReadiness(unknownKey, {
				timeout: 5,
			});
		} catch (error) {
			timeoutError = error as Error;
		}
		expect(timeoutError).to.be.instanceOf(TimeoutError);
		expect(timeoutError?.message).to.include(
			"diagnostic: session=absent/none/suspended",
		);
		expect(timeoutError?.message.length).to.be.lessThanOrEqual(1_024);
	});

	it("diagnoses current unconfirmed state and a same-key replacement without exposing identities", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const peerSession = log._peerSessions.current(receiverHash);
		const capabilitySession = log._peerSyncCapabilitySessions.get(receiverHash);
		expect(peerSession).to.exist;
		expect(capabilitySession).to.be.a("bigint");
		await log._v2Send.drain();
		const sendState = log._v2Send._sendStates.get(receiverHash);
		expect(sendState).to.exist;
		const previousAppliedRevision = sendState.appliedRevision;
		sendState.appliedRevision = undefined;
		const nudgeConfirmations = sinon.stub(log._v2Send, "nudgeConfirmations");
		const confirmationController = new AbortController();
		const confirmation = log._v2Send.confirmLatestForPeer(
			{
				peerHash: receiverHash,
				peerSession,
				receiverTransportSession: capabilitySession,
			},
			{ timeout: 5_000, signal: confirmationController.signal },
		);
		try {
			const current = await log.getPersistedReceiptPeerReadiness(receiverKey, {
				diagnostics: true,
			});
			expect(current).to.deep.include({
				status: "pending",
				reason: "replication-confirmation-pending",
			});
			expect(current.diagnostic.session).to.deep.include({
				state: "current",
				phase: "open",
				established: true,
			});
			expect(current.diagnostic.sender).to.deep.include({
				state: "current",
				reason: "latest-unconfirmed",
				confirmationWaiters: 1,
				currentRevision: log._v2Send._revision.toString(),
			});
			expect(current.diagnostic.sender.oldestConfirmationAgeMs).to.be.within(
				0,
				5_000,
			);
			expect(current.diagnostic.receiver).to.deep.include({
				state: "current",
				reason: "active",
				phase: "active",
			});

			const replacementSession = {
				phase: "open",
				openingBarrierActive: false,
				isActive: () => true,
			};
			const replacementReceiveEpoch = {};
			const originalCurrent = log._peerSessions.current.bind(log._peerSessions);
			const originalReceiveEpoch = log._peerSessions.receiveEpoch.bind(
				log._peerSessions,
			);
			const currentSession = sinon
				.stub(log._peerSessions, "current")
				.callsFake((...args: unknown[]) => {
					const hash = args[0] as string;
					return hash === receiverHash
						? replacementSession
						: originalCurrent(hash);
				});
			const currentReceiveEpoch = sinon
				.stub(log._peerSessions, "receiveEpoch")
				.callsFake((...args: unknown[]) => {
					const hash = args[0] as string;
					return hash === receiverHash
						? replacementReceiveEpoch
						: originalReceiveEpoch(hash);
				});
			const active = sinon
				.stub(log._v2Receive, "isCurrentActive")
				.returns(true);
			try {
				const replacement = await log.getPersistedReceiptPeerReadiness(
					receiverKey,
					{
						diagnostics: true,
					},
				);
				expect(replacement.diagnostic.sender).to.deep.include({
					state: "stale",
					reason: "peer-session-mismatch",
				});
				expect(replacement.diagnostic.receiver).to.deep.include({
					state: "stale",
					reason: "peer-session-mismatch",
				});
				expect(JSON.stringify(replacement.diagnostic)).not.to.include(
					capabilitySession.toString(),
				);
				expect(
					JSON.stringify(replacement.diagnostic).length,
				).to.be.lessThanOrEqual(2_048);
			} finally {
				active.restore();
				currentReceiveEpoch.restore();
				currentSession.restore();
			}
		} finally {
			confirmationController.abort(new Error("diagnostic confirmation done"));
			await expect(confirmation).to.be.rejectedWith(
				"diagnostic confirmation done",
			);
			nudgeConfirmations.restore();
			sendState.appliedRevision = previousAppliedRevision;
		}
	});

	const assertReadinessRebind = async (
		wake: "event" | "recovery tick" | "replicator loss",
	) => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		log.waitForReplicatorRequestIntervalMs = 50;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const oldSession = log._peerSessions.current(receiverHash);
		const oldReceiveEpoch = log._peerSessions.receiveEpoch(receiverHash);
		const oldTransportSession =
			log._peerSyncCapabilitySessions.get(receiverHash);
		const oldCapabilityTimestamp =
			log._peerSyncCapabilityTimestamps.get(receiverHash);
		const capabilities = log._peerSyncCapabilities.get(receiverHash);
		expect(oldSession).to.exist;
		expect(oldTransportSession).to.be.a("bigint");
		expect(oldCapabilityTimestamp).to.be.a("bigint");

		const newSession = { phase: "open", isActive: () => true };
		const newReceiveEpoch = {};
		const newTransportSession = oldTransportSession + 1n;
		let currentSession = oldSession;
		let currentReceiveEpoch = oldReceiveEpoch;
		const originalCurrent = log._peerSessions.current.bind(log._peerSessions);
		const originalReceiveEpoch = log._peerSessions.receiveEpoch.bind(
			log._peerSessions,
		);
		const current = sinon
			.stub(log._peerSessions, "current")
			.callsFake((...args: unknown[]) => {
				const peerHash = args[0] as string;
				return peerHash === receiverHash
					? currentSession
					: originalCurrent(peerHash);
			});
		const receiveEpoch = sinon
			.stub(log._peerSessions, "receiveEpoch")
			.callsFake((...args: unknown[]) => {
				const peerHash = args[0] as string;
				return peerHash === receiverHash
					? currentReceiveEpoch
					: originalReceiveEpoch(peerHash);
			});
		const advanceEpoch = sinon
			.stub(log._peerSessions, "advanceReceiveEpoch")
			.returns(newReceiveEpoch);
		const active = sinon
			.stub(log._v2Receive, "isCurrentActive")
			.callsFake(
				(properties: any) =>
					properties.peerSession === currentSession &&
					properties.receiveEpoch === currentReceiveEpoch,
			);
		const advanceRecovery = sinon.stub(log._v2Receive, "advanceRecovery");
		let replacementRearmed = false;
		const hasCurrentSendState = sinon
			.stub(log._v2Send, "hasCurrentStateForPeer")
			.callsFake(
				(target: any) =>
					target.peerSession === oldSession &&
					target.receiverTransportSession === oldTransportSession,
			);
		const rearmRemoteFull = sinon
			.stub(log._v2Receive, "reAdvertiseLocalCapabilityForRemoteFull")
			.callsFake((properties: any) => {
				if (
					properties.peerSession === newSession &&
					properties.receiveEpoch === newReceiveEpoch &&
					!properties.signal.aborted
				) {
					replacementRearmed = true;
					return true;
				}
				return false;
			});
		const latest = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.callsFake(
				(target: any) =>
					target.peerSession === newSession &&
					target.receiverTransportSession === newTransportSession &&
					replacementRearmed,
			);
		const oldConfirmationEntered = pDefer<AbortSignal>();
		const confirm = sinon
			.stub(log._v2Send, "confirmLatestForPeer")
			.callsFake(async (...args: unknown[]) => {
				const target = args[0] as any;
				const options = args[1] as { signal: AbortSignal };
				if (target.peerSession === oldSession) {
					const { signal } = options;
					oldConfirmationEntered.resolve(signal);
					await new Promise<void>((resolve, reject) => {
						const onAbort = () => reject(signal.reason ?? new AbortError());
						if (signal.aborted) {
							onAbort();
							return;
						}
						signal.addEventListener("abort", onAbort, { once: true });
					});
					return;
				}
				if (target.peerSession === newSession) return;
				throw new Error("unexpected receipt generation");
			});
		try {
			const oldSnapshot =
				await log.getPersistedReceiptPeerReadiness(receiverKey);
			expect(oldSnapshot).to.deep.include({
				status: "pending",
				reason: "replication-confirmation-pending",
			});

			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				timeout: 5_000,
			});
			const oldConfirmationSignal = await oldConfirmationEntered.promise;
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(1);
			expect(
				log.observePeerSyncCapabilities({
					peerHash: receiverHash,
					capabilities,
					transportSession: oldTransportSession,
					timestamp: oldCapabilityTimestamp,
				}),
			).to.equal(true);
			expect(oldConfirmationSignal.aborted).to.equal(false);
			const unrelatedKey = (await Ed25519Keypair.create()).publicKey;
			log.events.dispatchEvent(
				new CustomEvent("replication:change", {
					detail: { publicKey: unrelatedKey },
				}),
			);
			expect(oldConfirmationSignal.aborted).to.equal(false);

			if (wake === "replicator loss") {
				log.uniqueReplicators.delete(receiverHash);
				await waitForResolved(
					() => expect(oldConfirmationSignal.aborted).to.equal(true),
					{ timeout: 500 },
				);
				const lost = await log.getPersistedReceiptPeerReadiness(receiverKey);
				expect(lost).to.deep.include({
					status: "pending",
					reason: "not-replicating",
				});
				log.uniqueReplicators.add(receiverHash);
			}
			currentSession = newSession;
			currentReceiveEpoch = newReceiveEpoch;
			log._peerSyncCapabilitySessions.set(receiverHash, newTransportSession);
			if (wake === "event") {
				log.advanceReplicationInfoRecoveryEpoch(receiverHash);
				expect(oldConfirmationSignal.aborted).to.equal(true);
			} else {
				// Recovery must inspect the authoritative generation even if its
				// transition event was coalesced or missed during confirmation.
				await waitForResolved(
					() => expect(oldConfirmationSignal.aborted).to.equal(true),
					{ timeout: 500 },
				);
			}

			const replacement = await wait;
			expect(replacement.status).to.equal("ready");
			expect(replacement.generation).not.to.equal(oldSnapshot.generation);
			expect(confirm.firstCall.args[0].peerSession).to.equal(oldSession);
			expect(
				latest
					.getCalls()
					.some(
						(call) =>
							call.args[0].peerSession === newSession &&
							call.args[0].receiverTransportSession === newTransportSession,
					),
			).to.equal(true);
			expect(
				rearmRemoteFull
					.getCalls()
					.some(
						(call) =>
							call.args[0].peerSession === newSession &&
							call.args[0].receiveEpoch === newReceiveEpoch,
					),
			).to.equal(true);
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
		} finally {
			confirm.restore();
			latest.restore();
			rearmRemoteFull.restore();
			hasCurrentSendState.restore();
			advanceRecovery.restore();
			active.restore();
			advanceEpoch.restore();
			receiveEpoch.restore();
			current.restore();
			log._peerSyncCapabilitySessions.set(receiverHash, oldTransportSession);
		}
	};
	for (const wake of ["event", "recovery tick", "replicator loss"] as const) {
		it(`rebinds a readiness waiter to a same-key replacement generation through ${wake}`, () =>
			assertReadinessRebind(wake));
	}

	it("follows a real same-key replacement while a quiet log waits", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const oldSession = log._peerSessions.current(receiverHash);
		const oldCapabilitySession =
			log._peerSyncCapabilitySessions.get(receiverHash);
		expect(oldSession).to.exist;
		expect(oldCapabilitySession).to.be.a("bigint");

		const originalLatest = log._v2Send.isLatestConfirmedForPeer.bind(
			log._v2Send,
		);
		const latest = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.callsFake((target: any) =>
				target.peerSession === oldSession ? false : originalLatest(target),
			);
		const originalConfirm = log._v2Send.confirmLatestForPeer.bind(log._v2Send);
		const oldConfirmationEntered = pDefer<AbortSignal>();
		const confirm = sinon
			.stub(log._v2Send, "confirmLatestForPeer")
			.callsFake(async (target: any, options: any) => {
				if (target.peerSession !== oldSession) {
					return originalConfirm(target, options);
				}
				const signal = options.signal as AbortSignal;
				oldConfirmationEntered.resolve(signal);
				await new Promise<void>((resolve, reject) => {
					const onAbort = () => reject(signal.reason ?? new AbortError());
					if (signal.aborted) {
						onAbort();
						return;
					}
					signal.addEventListener("abort", onAbort, { once: true });
				});
			});
		try {
			const oldSnapshot =
				await writer.log.getPersistedReceiptPeerReadiness(receiverKey);
			expect(oldSnapshot).to.deep.include({
				status: "pending",
				reason: "replication-confirmation-pending",
			});
			const wait = writer.log.waitForPersistedReceiptPeerReadiness(
				receiverKey,
				{ timeout: 30_000 },
			);
			const oldConfirmationSignal = await oldConfirmationEntered.promise;

			await session!.peers[1].stop();
			await waitForResolved(
				() => expect(oldConfirmationSignal.aborted).to.equal(true),
				{ timeout: 5_000 },
			);
			await session!.peers[1].start();
			const replacement = await EventStore.open<EventStore<string, any>>(
				writer.address!,
				session!.peers[1],
				{
					args: {
						replicas: { min: 2 },
						replicate: { offset: 0, factor: 1 },
						timeUntilRoleMaturity: 0,
					},
				},
			);
			expect(replacement.node.identity.publicKey.equals(receiverKey)).to.equal(
				true,
			);
			await Promise.all([
				writer.waitFor(session!.peers[1].peerId),
				replacement.waitFor(session!.peers[0].peerId),
				writer.log.waitForReplicator(receiverKey, {
					eager: true,
					timeout: 15_000,
				}),
			]);
			await waitForPersistedCapability(writer, replacement);

			const ready = await wait;
			const replacementSession = log._peerSessions.current(receiverHash);
			const replacementCapabilitySession =
				log._peerSyncCapabilitySessions.get(receiverHash);
			expect(ready.status).to.equal("ready");
			expect(ready.generation).not.to.equal(oldSnapshot.generation);
			expect(replacementSession).to.exist.and.not.equal(oldSession);
			expect(replacementCapabilitySession).to.be.a("bigint");
			expect(replacementCapabilitySession).not.to.equal(oldCapabilitySession);
			expect(
				confirm
					.getCalls()
					.some((call) => call.args[0].peerSession === replacementSession),
			).to.equal(true);
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
		} finally {
			confirm.restore();
			latest.restore();
		}
	});

	it("keeps bounded recovery running behind one pending confirmation", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		log.waitForReplicatorRequestIntervalMs = 50;
		const latest = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.returns(false);
		const hasCurrent = sinon
			.stub(log._v2Send, "hasCurrentStateForPeer")
			.returns(false);
		const rearm = sinon
			.stub(log._v2Receive, "reAdvertiseLocalCapabilityForRemoteFull")
			.returns(true);
		const active = sinon.stub(log._v2Receive, "isCurrentActive").returns(true);
		let confirmationSignal: AbortSignal | undefined;
		const confirmationTimeouts: number[] = [];
		const confirm = sinon
			.stub(log._v2Send, "confirmLatestForPeer")
			.callsFake(async (...args: unknown[]) => {
				const options = args[1] as {
					timeout: number;
					signal: AbortSignal;
				};
				confirmationSignal = options.signal;
				confirmationTimeouts.push(options.timeout);
				await new Promise<void>((resolve, reject) => {
					const onAbort = () =>
						reject(options.signal.reason ?? new AbortError());
					if (options.signal.aborted) {
						onAbort();
						return;
					}
					options.signal.addEventListener("abort", onAbort, { once: true });
				});
			});
		const nudge = sinon.spy(log, "nudgePersistedReceiptPeerReadiness");
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			shouldClearNativeTimers: true,
		});
		try {
			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				timeout: 180,
			});
			const rejected = expect(wait).to.be.rejectedWith(
				TimeoutError,
				"pending/replication-confirmation-pending",
			);
			await clock.tickAsync(0);
			expect(confirm.calledOnce).to.be.true;
			const initialNudges = nudge.callCount;
			const initialRearms = rearm.callCount;

			// A reserved inbound Full temporarily closes receive admission without
			// changing the peer/session or role. Keep the outbound query alive.
			active.returns(false);
			await clock.tickAsync(50);
			expect(confirm.calledOnce).to.be.true;
			expect(confirmationSignal?.aborted).to.be.false;
			active.returns(true);

			await clock.tickAsync(60);
			expect(confirm.calledOnce).to.be.true;
			expect(nudge.callCount).to.be.at.least(initialNudges + 2);
			expect(rearm.callCount).to.be.at.least(initialRearms + 1);
			expect(rearm.getCalls().every((call) => !call.args[0].signal.aborted)).to
				.be.true;
			expect(confirmationTimeouts).to.have.length(1);
			expect(confirmationTimeouts[0]).to.be.within(1, 180);

			await clock.tickAsync(70);
			await rejected;
			expect(confirmationSignal?.aborted).to.be.true;
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
			const settledNudges = nudge.callCount;
			const settledRearms = rearm.callCount;
			await clock.tickAsync(500);
			expect(nudge.callCount).to.equal(settledNudges);
			expect(rearm.callCount).to.equal(settledRearms);
		} finally {
			clock.restore();
			nudge.restore();
			active.restore();
			confirm.restore();
			rearm.restore();
			hasCurrent.restore();
			latest.restore();
		}
	});

	it("rearms a current sender only after its confirmation watchdog expires", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const peerSession = log._peerSessions.current(receiverHash);
		const receiveEpoch = log._peerSessions.receiveEpoch(receiverHash);
		const capabilitySession = log._peerSyncCapabilitySessions.get(receiverHash);
		expect(peerSession).to.exist;
		expect(capabilitySession).to.be.a("bigint");
		expect(
			log._v2Receive.isCurrentActive({
				peerHash: receiverHash,
				peerSession,
				receiveEpoch,
				senderTransportSession: capabilitySession,
			}),
		).to.equal(true);
		log.waitForReplicatorRequestIntervalMs = 50;

		let confirmed = false;
		const confirmation = pDefer<void>();
		const hasCurrent = sinon
			.stub(log._v2Send, "hasCurrentStateForPeer")
			.returns(true);
		const latest = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.callsFake(() => confirmed);
		const confirm = sinon
			.stub(log._v2Send, "confirmLatestForPeer")
			.callsFake(async () => confirmation.promise);
		const rearm = sinon
			.stub(log._v2Receive, "reAdvertiseLocalCapabilityForRemoteFull")
			.callsFake((properties: any) => {
				expect(properties).to.include({
					peerHash: receiverHash,
					peerSession,
					receiveEpoch,
				});
				expect(properties.signal.aborted).to.equal(false);
				confirmed = true;
				confirmation.resolve();
				return true;
			});
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			shouldClearNativeTimers: true,
		});
		try {
			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				timeout: 1_000,
			});
			await clock.tickAsync(0);
			expect(confirm.calledOnce).to.equal(true);
			expect(rearm.notCalled).to.equal(true);

			await clock.tickAsync(49);
			expect(rearm.notCalled).to.equal(true);
			await clock.tickAsync(1);
			const ready = await wait;
			expect(ready.status).to.equal("ready");
			expect(rearm.calledOnce).to.equal(true);
			expect(confirm.calledOnce).to.equal(true);
			expect(hasCurrent.called).to.equal(true);
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
		} finally {
			clock.restore();
			rearm.restore();
			confirm.restore();
			latest.restore();
			hasCurrent.restore();
		}
	});

	it("does not diagnose internal pending checks or postpone current-sender rearm", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		log.waitForReplicatorRequestIntervalMs = 50;

		let confirmed = false;
		const hasCurrent = sinon
			.stub(log._v2Send, "hasCurrentStateForPeer")
			.returns(true);
		const latest = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.callsFake(() => confirmed);
		const confirm = sinon
			.stub(log._v2Send, "confirmLatestForPeer")
			.callsFake(async (...args: unknown[]) => {
				if (confirmed) return;
				const signal = (args[1] as { signal: AbortSignal }).signal;
				await new Promise<void>((resolve, reject) => {
					const onAbort = () => reject(signal.reason ?? new AbortError());
					if (signal.aborted) {
						onAbort();
						return;
					}
					signal.addEventListener("abort", onAbort, { once: true });
				});
			});
		const rearmTimes: number[] = [];
		const rearm = sinon
			.stub(log._v2Receive, "reAdvertiseLocalCapabilityForRemoteFull")
			.callsFake(() => {
				rearmTimes.push(Date.now());
				confirmed = true;
				return true;
			});
		const diagnoseSender = sinon.spy(log._v2Send, "diagnosePeer");
		const diagnoseReceiver = sinon.spy(log._v2Receive, "diagnosePeer");
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			shouldClearNativeTimers: true,
		});
		const startedAt = Date.now();
		try {
			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				timeout: 1_000,
				diagnostics: true,
			});
			await clock.tickAsync(0);
			expect(confirm.calledOnce).to.equal(true);
			expect(diagnoseSender.notCalled).to.equal(true);
			expect(diagnoseReceiver.notCalled).to.equal(true);

			// Every event aborts the current confirmation and moves the recovery
			// timer. The exact-generation watchdog itself must keep its original
			// deadline, so the fifth event drives the mature rearm at 50 ms.
			for (let index = 0; index < 5; index++) {
				await clock.tickAsync(10);
				log.events.dispatchEvent(
					new CustomEvent("persisted-receipt:readiness", {
						detail: { peerHash: receiverHash },
					}),
				);
				await clock.tickAsync(0);
				if (index < 4) {
					expect(diagnoseSender.notCalled).to.equal(true);
					expect(diagnoseReceiver.notCalled).to.equal(true);
				}
			}

			const ready = await wait;
			expect(ready.status).to.equal("ready");
			expect(ready.diagnostic).to.exist;
			expect(diagnoseSender.calledOnce).to.equal(true);
			expect(diagnoseReceiver.calledOnce).to.equal(true);
			expect(confirm.callCount).to.be.greaterThan(1);
			expect(rearmTimes).to.deep.equal([startedAt + 50]);
			expect(hasCurrent.called).to.equal(true);
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
		} finally {
			clock.restore();
			diagnoseReceiver.restore();
			diagnoseSender.restore();
			rearm.restore();
			confirm.restore();
			latest.restore();
			hasCurrent.restore();
		}
	});

	it("rebuilds a current but unconfirmed sender through the exact V2 wire path", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const { entry } = await writer.add("quiet-log-stale-confirmation", {
			target: "none",
		});
		const writerLog = writer.log as any;
		const receiverLog = receiver.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const peerSession = writerLog._peerSessions.current(receiverHash);
		const capabilitySession =
			writerLog._peerSyncCapabilitySessions.get(receiverHash);
		const initialState = writerLog._v2Send._sendStates.get(receiverHash);
		expect(peerSession).to.exist;
		expect(capabilitySession).to.be.a("bigint");
		expect(initialState).to.exist;
		expect(initialState.peerSession).to.equal(peerSession);
		expect(
			writerLog._v2Send.hasCurrentStateForPeer({
				peerHash: receiverHash,
				peerSession,
				receiverTransportSession: capabilitySession,
			}),
		).to.equal(true);
		writerLog.waitForReplicatorRequestIntervalMs = 50;

		// Keep the exact current sender state but remove its observed application
		// frontier. Its normal confirmation worker remains live; only responses for
		// this old binding are withheld so the watchdog must perform the wire rearm.
		initialState.appliedRevision = undefined;
		const originalAcceptApplied = writerLog._v2Send.acceptApplied.bind(
			writerLog._v2Send,
		);
		let withheldOldApplied = 0;
		const acceptApplied = sinon
			.stub(writerLog._v2Send, "acceptApplied")
			.callsFake((message: any, properties: any) => {
				if (writerLog._v2Send._sendStates.get(receiverHash) === initialState) {
					withheldOldApplied++;
					return false;
				}
				return originalAcceptApplied(message, properties);
			});
		const writerSend = sinon.spy(writerLog.rpc, "send");
		const receiverSend = sinon.spy(receiverLog.rpc, "send");
		const advanceRecovery = sinon.spy(
			receiverLog._v2Receive,
			"advanceRecovery",
		);
		const acceptRequest = sinon.spy(writerLog._v2Send, "acceptRequest");
		try {
			expect(
				await writer.log.getPersistedReceiptPeerReadiness(receiverKey, {
					entries: [entry],
					replicas: 2,
				}),
			).to.deep.include({
				status: "pending",
				reason: "replication-confirmation-pending",
			});

			const ready = await writer.log.waitForPersistedReceiptPeerReadiness(
				receiverKey,
				{ entries: [entry], replicas: 2, timeout: 15_000 },
			);
			const replacementState = writerLog._v2Send._sendStates.get(receiverHash);
			expect(ready.status).to.equal("ready");
			expect(withheldOldApplied).to.be.greaterThan(0);
			expect(advanceRecovery.called).to.equal(true);
			expect(acceptRequest.called).to.equal(true);
			expect(replacementState).to.exist.and.not.equal(initialState);
			expect(initialState.controller.signal.aborted).to.equal(true);
			expect(
				writerSend.getCalls().some((call) => {
					const message = call.args[0];
					return (
						message instanceof SyncCapabilitiesMessage &&
						(message.capabilities &
							SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM) !==
							0
					);
				}),
			).to.equal(true);
			expect(
				writerSend
					.getCalls()
					.some((call) => call.args[0] instanceof FullReplicationInfoV2Message),
			).to.equal(true);
			expect(
				writerSend
					.getCalls()
					.some(
						(call) =>
							call.args[0] instanceof RequestReplicationInfoV2AppliedMessage,
					),
			).to.equal(true);
			expect(
				receiverSend
					.getCalls()
					.some(
						(call) => call.args[0] instanceof ReplicationInfoV2AppliedMessage,
					),
			).to.equal(true);

			await writer.log.deliverPersistedEntries([entry], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 15_000,
				},
			});
			expect(await receiver.log.log.has(entry.hash)).to.equal(true);
		} finally {
			acceptRequest.restore();
			advanceRecovery.restore();
			receiverSend.restore();
			writerSend.restore();
			acceptApplied.restore();
		}
	});

	it("recovers a failed opening barrier through a fresh subscriber snapshot", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = writer.log as any;
		const remote = (await Ed25519Keypair.create()).publicKey;
		const remoteHash = remote.hashcode();
		const firstTransportSession = 41n;
		const replacementTransportSession = 42n;
		const subscribeEvent = (transportSession: bigint) =>
			({
				detail: {
					from: remote,
					topics: [log.topic],
					session: transportSession,
				},
			}) as any;
		const drain = sinon.stub(log, "drainPeerReceiveHandlers");
		drain.onFirstCall().rejects(new Error("forced opening-barrier failure"));
		drain.onSecondCall().resolves();
		const send = sinon.stub(log.rpc, "send").resolves([] as any);

		await expect(
			log._onSubscription(subscribeEvent(firstTransportSession)),
		).to.be.rejectedWith("forced opening-barrier failure");
		const failedSession = log._peerSessions.current(remoteHash);
		expect(failedSession?.phase).to.equal("opening");
		expect(failedSession?.openingBarrierActive).to.equal(false);
		expect(log._peerSessions.isReplicationInfoBlocked(remoteHash)).to.equal(
			true,
		);

		log.uniqueReplicators.add(remoteHash);
		const active = sinon.stub(log._v2Receive, "isCurrentActive").returns(true);
		const confirmed = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.returns(true);
		let replacementSession: any;
		const requestSubscribers = sinon
			.stub(log.node.services.pubsub, "requestSubscribers")
			.callsFake(async () => {
				await log._onSubscription(subscribeEvent(replacementTransportSession));
				replacementSession = log._peerSessions.current(remoteHash);
				log._peerSyncCapabilities.set(
					remoteHash,
					SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS |
						SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM,
				);
				log._peerSyncCapabilitySessions.set(
					remoteHash,
					replacementTransportSession,
				);
				log._peerSyncCapabilityTimestamps.set(remoteHash, 1n);
				log.dispatchPersistedReceiptReadinessChange(remoteHash);
			});
		try {
			const ready = await log.waitForPersistedReceiptPeerReadiness(remote, {
				timeout: 2_000,
			});
			expect(ready.status).to.equal("ready");
			expect(requestSubscribers.calledOnceWith(log.topic, remote)).to.equal(
				true,
			);
			expect(replacementSession).not.to.equal(failedSession);
			expect(replacementSession?.phase).to.equal("open");
			expect(log._peerSessions.isReplicationInfoBlocked(remoteHash)).to.equal(
				false,
			);
		} finally {
			requestSubscribers.restore();
			confirmed.restore();
			active.restore();
			send.restore();
			drain.restore();
		}
	});

	it("rechecks a fresh per-entry leader plan after a coalesced role change", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const { entry } = await writer.add("readiness-leader-replacement", {
			target: "none",
		});
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const firstPlanEntered = pDefer<void>();
		const releaseFirstPlan = pDefer<void>();
		let planningCalls = 0;
		const leaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				planningCalls++;
				if (planningCalls === 1) {
					firstPlanEntered.resolve();
					await releaseFirstPlan.promise;
					return new Map([[receiverHash, {}]]);
				}
				return new Map([[receiverHash, {}]]);
			});
		const confirmed = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.returns(true);
		try {
			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				entries: [entry],
				replicas: 1,
				timeout: 5_000,
			});
			await firstPlanEntered.promise;
			log.events.dispatchEvent(
				new CustomEvent("replication:change", {
					detail: { publicKey: receiverKey },
				}),
			);
			releaseFirstPlan.resolve();

			const ready = await wait;
			expect(ready.status).to.equal("ready");
			expect(leaders.callCount).to.be.greaterThanOrEqual(2);
			expect(
				leaders
					.getCalls()
					.every((call) => call.args[2]?.freshLeaderPlan === true),
			).to.equal(true);
		} finally {
			confirmed.restore();
			leaders.restore();
		}
	});

	it("skips stale confirmation work after a wake during inspection", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const { entry } = await writer.add("readiness-confirmation-recheck", {
			target: "none",
		});
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const firstPlanEntered = pDefer<void>();
		const releaseFirstPlan = pDefer<void>();
		let planningCalls = 0;
		const leaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				planningCalls++;
				if (planningCalls === 1) {
					firstPlanEntered.resolve();
					await releaseFirstPlan.promise;
				}
				return new Map([[receiverHash, {}]]);
			});
		const confirmed = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.callsFake(() => planningCalls >= 2);
		const confirm = sinon.stub(log._v2Send, "confirmLatestForPeer").resolves();
		try {
			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				entries: [entry],
				replicas: 1,
				timeout: 5_000,
			});
			await firstPlanEntered.promise;
			log.dispatchPersistedReceiptReadinessChange(receiverHash);
			releaseFirstPlan.resolve();

			const ready = await wait;
			expect(ready.status).to.equal("ready");
			expect(leaders.callCount).to.equal(2);
			expect(confirm.called).to.equal(false);
		} finally {
			releaseFirstPlan.resolve();
			confirm.restore();
			confirmed.restore();
			leaders.restore();
		}
	});

	it("stops an in-flight readiness inspection after caller cancellation", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const { entry } = await writer.add("cancel-readiness-planner", {
			target: "none",
		});
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const plannerEntered = pDefer<void>();
		const releasePlanner = pDefer<void>();
		const leaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				plannerEntered.resolve();
				await releasePlanner.promise;
				return new Map([[receiverHash, {}]]);
			});
		const nudge = sinon.stub(log, "nudgePersistedReceiptPeerReadiness");
		const controller = new AbortController();
		try {
			const wait = log.waitForPersistedReceiptPeerReadiness(receiverKey, {
				entries: [entry, entry],
				replicas: 1,
				signal: controller.signal,
				timeout: 5_000,
			});
			await plannerEntered.promise;
			controller.abort(new Error("cancel in-flight readiness"));
			await expect(wait).to.be.rejectedWith("cancel in-flight readiness");
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);

			releasePlanner.resolve();
			await new Promise((resolve) => setTimeout(resolve, 0));
			expect(leaders.callCount).to.equal(1);
			expect(nudge.called).to.equal(false);
		} finally {
			releasePlanner.resolve();
			nudge.restore();
			leaders.restore();
		}
	});

	it("does not publish readiness after the deadline elapses inside planning", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const { entry } = await writer.add("deadline-readiness-planner", {
			target: "none",
		});
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const baseNow = Date.now();
		let now = baseNow;
		const dateNow = sinon.stub(Date, "now").callsFake(() => now);
		const leaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				now = baseNow + 10;
				return new Map([[receiverHash, {}]]);
			});
		const confirmed = sinon
			.stub(log._v2Send, "isLatestConfirmedForPeer")
			.returns(true);
		try {
			await expect(
				log.waitForPersistedReceiptPeerReadiness(receiverKey, {
					entries: [entry],
					replicas: 1,
					timeout: 5,
				}),
			).to.be.rejectedWith(
				TimeoutError,
				"Timeout waiting for persisted-receipt readiness",
			);
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
		} finally {
			confirmed.restore();
			leaders.restore();
			dateNow.restore();
		}
	});

	it("validates replica bounds before consulting an absent peer", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const unknownKey = (await Ed25519Keypair.create()).publicKey;
		const candidate = sinon.spy(log, "persistedReceiptReadinessCandidate");
		const nudge = sinon.stub(log, "nudgePersistedReceiptPeerReadiness");
		try {
			await expect(
				log.getPersistedReceiptPeerReadiness(unknownKey, { replicas: 101 }),
			).to.be.rejectedWith("Higher replication degree than 100");
			expect(candidate.called).to.equal(false);

			await expect(
				log.waitForPersistedReceiptPeerReadiness(unknownKey, {
					replicas: 101,
					timeout: 1_000,
				}),
			).to.be.rejectedWith("Higher replication degree than 100");
			expect(candidate.called).to.equal(false);
			expect(nudge.called).to.equal(false);
			expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
		} finally {
			nudge.restore();
			candidate.restore();
		}
	});

	it("makes a quiet one-entry log receipt-ready before persisted delivery", async () => {
		const { writer, receiver } = await openPair(true);
		const receiverKey = receiver.node.identity.publicKey;
		await writer.log.waitForPersistedReceiptPeerReadiness(receiverKey, {
			timeout: 15_000,
		});
		const { entry } = await writer.add("quiet-log-entry", { target: "none" });
		const ready = await writer.log.waitForPersistedReceiptPeerReadiness(
			receiverKey,
			{ entries: [entry], replicas: 2, timeout: 15_000 },
		);
		expect(ready.status).to.equal("ready");

		await writer.log.deliverPersistedEntries([entry], {
			target: "replicators",
			delivery: {
				reliability: "persisted",
				minAcks: 1,
				timeout: 15_000,
			},
		});
		expect(await receiver.log.log.has(entry.hash)).to.equal(true);
	});

	it("ACKs steady capability grants but sends transient rearm hints silently", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const log = writer.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const peerSession = log._peerSessions.current(receiverHash);
		const receiveEpoch = log._peerSessions.receiveEpoch(receiverHash);
		expect(peerSession).to.exist;

		const send = sinon.stub(log.rpc, "send").resolves();
		const signal = new AbortController().signal;
		try {
			await log.advertiseReplicationInfoV2ReceiveCapability({
				target: receiverKey,
				peerSession,
				receiveEpoch,
				signal,
			});
			await log.advertiseReplicationInfoV2ReceiveCapability({
				target: receiverKey,
				peerSession,
				receiveEpoch,
				signal,
				requestRemoteFullRearm: true,
			});

			expect(send.callCount).to.equal(2);
			expect(send.firstCall.args[1].mode).to.be.instanceOf(AcknowledgeDelivery);
			expect(send.secondCall.args[1].mode).to.be.instanceOf(SilentDelivery);
			expect(
				(send.firstCall.args[0] as SyncCapabilitiesMessage).capabilities &
					SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM,
			).to.equal(0);
			expect(
				(send.secondCall.args[0] as SyncCapabilitiesMessage).capabilities &
					SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM,
			).to.equal(SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM);
		} finally {
			send.restore();
		}
	});

	it("repairs missing outbound state after preflight before persisted delivery", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		await writer.log.waitForPersistedReceiptPeerReadiness(receiverKey, {
			timeout: 5_000,
		});
		const { entry } = await writer.add("sender-disappeared-after-preflight", {
			target: "none",
		});
		const log = writer.log as any;
		await log._v2Send.drain();
		log._v2Send.clearPeer(receiverHash);
		expect(log.persistedReceiptPeerSession(receiverHash)).to.exist;
		expect(log._v2Send._sendStates.has(receiverHash)).to.be.false;

		await writer.log.deliverPersistedEntries([entry], {
			target: "replicators",
			delivery: { reliability: "persisted", minAcks: 1, timeout: 5_000 },
		});
		expect(await receiver.log.log.has(entry.hash)).to.be.true;
		expect(log._v2Send._sendStates.has(receiverHash)).to.be.true;
	});

	it("rebuilds missing outbound confirmation state from an active quiet peer", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const { entry } = await writer.add("quiet-log-missing-sender-state", {
			target: "none",
		});
		const writerLog = writer.log as any;
		const receiverLog = receiver.log as any;
		const receiverKey = receiver.node.identity.publicKey;
		const receiverHash = receiverKey.hashcode();
		const writerHash = writer.node.identity.publicKey.hashcode();
		const peerSession = writerLog._peerSessions.current(receiverHash);
		const capabilitySession =
			writerLog._peerSyncCapabilitySessions.get(receiverHash);
		const writerReceiveState =
			writerLog._v2Receive._receiveStates.get(receiverHash);
		const receiverReceiveState =
			receiverLog._v2Receive._receiveStates.get(writerHash);
		expect(peerSession).to.exist;
		expect(capabilitySession).to.be.a("bigint");
		expect(writerReceiveState?.phase).to.equal("active");
		expect(receiverReceiveState?.phase).to.equal("active");
		const initialReceiveEpoch =
			writerLog._peerSessions.receiveEpoch(receiverHash);
		writerLog.advanceReplicationInfoRecoveryEpoch(receiverHash);
		await waitForResolved(
			async () => {
				const receiveState =
					writerLog._v2Receive._receiveStates.get(receiverHash);
				expect(receiveState).to.equal(writerReceiveState);
				expect(receiveState?.phase).to.equal("active");
			},
			{ timeout: 15_000 },
		);
		expect(writerLog._peerSessions.receiveEpoch(receiverHash)).not.to.equal(
			initialReceiveEpoch,
		);

		await writerLog._v2Send.drain();
		writerLog._v2Send.clearPeer(receiverHash, peerSession);
		expect(
			writerLog._v2Send.hasCurrentStateForPeer({
				peerHash: receiverHash,
				peerSession,
				receiverTransportSession: capabilitySession,
			}),
		).to.equal(false);
		expect(
			await writer.log.getPersistedReceiptPeerReadiness(receiverKey, {
				entries: [entry],
				replicas: 2,
			}),
		).to.deep.include({
			status: "pending",
			reason: "replication-confirmation-pending",
		});

		const writerSend = sinon.spy(writerLog.rpc, "send");
		const acceptRequest = sinon.spy(writerLog._v2Send, "acceptRequest");
		try {
			const ready = await writer.log.waitForPersistedReceiptPeerReadiness(
				receiverKey,
				{ entries: [entry], replicas: 2, timeout: 15_000 },
			);
			expect(ready.status).to.equal("ready");
			expect(
				writerSend.getCalls().some((call) => {
					const message = call.args[0];
					return (
						message instanceof SyncCapabilitiesMessage &&
						(message.capabilities &
							SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM) !==
							0
					);
				}),
			).to.equal(true);
			expect(acceptRequest.called).to.equal(true);
			expect(
				receiverLog._peerSyncCapabilities.get(writerHash) &
					SYNC_CAPABILITY_REPLICATION_INFO_V2_REARM,
			).to.equal(0);
			expect(
				writerLog._v2Send.hasCurrentStateForPeer({
					peerHash: receiverHash,
					peerSession,
					receiverTransportSession: capabilitySession,
				}),
			).to.equal(true);
			expect(
				writerLog._v2Send.isLatestConfirmedForPeer({
					peerHash: receiverHash,
					peerSession,
					receiverTransportSession: capabilitySession,
				}),
			).to.equal(true);
			expect(writerLog._v2Receive._receiveStates.get(receiverHash)).to.equal(
				writerReceiveState,
			);
			expect(receiverLog._v2Receive._receiveStates.get(writerHash)).to.equal(
				receiverReceiveState,
			);
			expect(writerReceiveState.phase).to.equal("active");
			expect(receiverReceiveState.phase).to.equal("active");
			expect(writerLog._persistedReceiptReadinessWaiters.size).to.equal(0);

			await writer.log.deliverPersistedEntries([entry], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 15_000,
				},
			});
			expect(await receiver.log.log.has(entry.hash)).to.equal(true);
		} finally {
			acceptRequest.restore();
			writerSend.restore();
		}
	});

	it("cleans readiness waiters on cancellation, timeout, and close", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const unknownKey = (await Ed25519Keypair.create()).publicKey;

		const controller = new AbortController();
		const cancelled = log.waitForPersistedReceiptPeerReadiness(unknownKey, {
			signal: controller.signal,
			timeout: 5_000,
		});
		expect(log._persistedReceiptReadinessWaiters.size).to.equal(1);
		controller.abort(new Error("cancel receipt readiness"));
		await expect(cancelled).to.be.rejectedWith("cancel receipt readiness");
		expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);

		await expect(
			log.waitForPersistedReceiptPeerReadiness(unknownKey, { timeout: 5 }),
		).to.be.rejectedWith(
			TimeoutError,
			"Timeout waiting for persisted-receipt readiness",
		);
		expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);

		const closed = log.waitForPersistedReceiptPeerReadiness(unknownKey, {
			timeout: 5_000,
		});
		expect(log._persistedReceiptReadinessWaiters.size).to.equal(1);
		const rejectedOnClose = expect(closed).to.be.rejectedWith(ClosedError);
		await writer.close();
		await rejectedOnClose;
		expect(log._persistedReceiptReadinessWaiters.size).to.equal(0);
	});

	it("rejects a non-positive minAcks before committing", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());

		await expect(
			writer.add("invalid-quorum", {
				target: "replicators",
				delivery: { reliability: "persisted", minAcks: 0 },
			}),
		).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		expect(writer.log.log.length).to.equal(0);
	});

	it("rejects an unknown persisted target before committing", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());

		await expect(
			writer.add("invalid-target", {
				target: "future-target",
				delivery: { reliability: "persisted", minAcks: 1 },
			} as any),
		).to.be.rejectedWith(
			'persisted delivery requires target="replicators" (or an omitted target)',
		);
		expect(writer.log.log.length).to.equal(0);
	});

	it("rejects persisted delivery on trusted local commit seams", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const options = {
			target: "replicators",
			delivery: { reliability: "persisted", minAcks: 1 },
		};
		const expected =
			"trusted local append paths require delivery=false; call deliverPersistedEntries after the local commit";

		await expect(
			log.appendLocallyValidated({ op: "ADD", value: "validated" }, options),
		).to.be.rejectedWith(expected);
		await expect(
			log.appendLocallyPrepared({ op: "ADD", value: "prepared" }, options),
		).to.be.rejectedWith(expected);
		await expect(
			log.appendLocallyPreparedManyIndependent(
				[{ op: "ADD", value: "batch" }],
				options,
			),
		).to.be.rejectedWith(expected);
		expect(() =>
			log.appendLocallyPreparedPayloadCommitOnly(new Uint8Array([1]), options),
		).to.throw(expected);
		expect(writer.log.log.length).to.equal(0);
	});

	it("rejects a single oversized receipt hash before encoding it", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const encode = sinon.spy(TextEncoder.prototype, "encode");
		try {
			expect(() => {
				(writer.log as any).validatePersistedReceiptRequestShape(
					new RequestPersistedEntriesV1({
						expectedReceiverSession: 1n,
						hashes: ["x".repeat(128 * 1_024 + 1)],
					}),
				);
			}).to.throw("Invalid persisted receipt hash batch");
			expect(encode.called).to.equal(false);
		} finally {
			encode.restore();
		}
	});

	it("rate limits persisted receipt work per peer transport session", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const now = 1_000_000;

		for (let request = 0; request < 16; request++) {
			expect(log.admitPersistedReceiptIngress("peer-a", 1n, 512, now)).to.equal(
				true,
			);
		}
		expect(log.admitPersistedReceiptIngress("peer-a", 1n, 512, now)).to.equal(
			false,
		);
		expect(
			log.admitPersistedReceiptIngress("peer-a", 1n, 512, now - 1_000),
		).to.equal(false);
		expect(log.admitPersistedReceiptIngress("peer-a", 1n, 512, now)).to.equal(
			false,
		);
		expect(
			log.admitPersistedReceiptIngress("peer-a", 1n, 512, now + 125),
		).to.equal(true);
		expect(
			log.admitPersistedReceiptIngress("peer-a", 2n, 512, now + 125),
		).to.equal(true);
	});

	it("charges malformed receipt hashes against the ingress budget before parsing", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const now = 1_500_000;
		const sessionCurrent = sinon
			.stub(log, "isPersistedReceiptRequestSessionCurrent")
			.returns(true);
		const clock = sinon.stub(Date, "now").returns(now);
		const request = new RequestPersistedEntriesV1({
			expectedReceiverSession: 1n,
			hashes: ["not-a-cid"],
		});
		const context = { message: { header: { session: 1n } } };
		const lane = { fromHash: "malformed-peer" };

		try {
			for (let attempt = 0; attempt < 16; attempt++) {
				expect(
					await log.handleRequestPersistedEntriesV1(request, context, lane),
				).to.equal(undefined);
			}
			expect(
				log.admitPersistedReceiptIngress("malformed-peer", 1n, 1, now),
			).to.equal(false);
		} finally {
			clock.restore();
			sessionCurrent.restore();
		}
	});

	it("charges invalid receipt shapes before rejecting them", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const now = 1_750_000;
		const sessionCurrent = sinon
			.stub(log, "isPersistedReceiptRequestSessionCurrent")
			.returns(true);
		const clock = sinon.stub(Date, "now").returns(now);
		const oversized = new RequestPersistedEntriesV1({
			expectedReceiverSession: 1n,
			hashes: Array.from({ length: 1_025 }, () => "not-a-cid"),
		});
		const context = { message: { header: { session: 1n } } };
		const lane = { fromHash: "invalid-shape-peer" };

		try {
			for (let attempt = 0; attempt < 8; attempt++) {
				expect(
					await log.handleRequestPersistedEntriesV1(oversized, context, lane),
				).to.equal(undefined);
			}
			expect(
				log.admitPersistedReceiptIngress("invalid-shape-peer", 1n, 1, now),
			).to.equal(false);
		} finally {
			clock.restore();
			sessionCurrent.restore();
		}
	});

	it("shares the persisted receipt ingress budget across peers on one node", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const now = 2_000_000;

		for (let peer = 0; peer < 4; peer++) {
			for (let request = 0; request < 8; request++) {
				expect(
					log.admitPersistedReceiptIngress(
						`peer-${peer}`,
						BigInt(peer + 1),
						512,
						now,
					),
				).to.equal(true);
			}
		}
		expect(
			log.admitPersistedReceiptIngress("peer-extra", 9n, 512, now),
		).to.equal(false);
		expect(
			log.admitPersistedReceiptIngress("peer-extra", 9n, 512, now + 125),
		).to.equal(true);
	});

	it("shares one node ingress budget across separate SharedLog programs", async () => {
		session = await TestSession.disconnected(1);
		const first = await session.peers[0].open(new EventStore<string, any>());
		const second = await session.peers[0].open(new EventStore<string, any>());
		const firstLog = first.log as any;
		const secondLog = second.log as any;
		const now = 2_500_000;

		for (let request = 0; request < 16; request++) {
			expect(
				firstLog.admitPersistedReceiptIngress("peer-a", 1n, 512, now),
			).to.equal(true);
			expect(
				secondLog.admitPersistedReceiptIngress("peer-b", 1n, 512, now),
			).to.equal(true);
		}
		expect(
			secondLog.admitPersistedReceiptIngress("peer-extra", 1n, 1, now),
		).to.equal(false);
	});

	it("paces receipt egress across programs and sizes the omitted deadline", async () => {
		session = await TestSession.disconnected(1);
		const first = await session.peers[0].open(new EventStore<string, any>());
		const second = await session.peers[0].open(new EventStore<string, any>());
		const firstLog = first.log as any;
		const secondLog = second.log as any;
		const now = 3_000_000;

		for (let request = 0; request < 8; request++) {
			expect(
				firstLog.reservePersistedReceiptEgress("durable-peer", 1n, 512, now),
			).to.equal(0);
			expect(
				secondLog.reservePersistedReceiptEgress("durable-peer", 1n, 512, now),
			).to.equal(0);
		}
		expect(
			secondLog.reservePersistedReceiptEgress("durable-peer", 1n, 512, now),
		).to.equal(125);
		expect(
			secondLog.reservePersistedReceiptEgress(
				"durable-peer",
				1n,
				512,
				now + 125,
			),
		).to.equal(0);
		expect(
			secondLog.reservePersistedReceiptEgress("durable-peer", 2n, 512, now),
		).to.equal(0);

		const delivery = { reliability: "persisted", minAcks: 1 };
		expect(firstLog.persistedDeliveryTimeoutMs(delivery, 6_200)).to.equal(
			60_000,
		);
		expect(firstLog.persistedDeliveryTimeoutMs(delivery, 8_193)).to.equal(
			76_125,
		);
		expect(firstLog.persistedDeliveryTimeoutMs(delivery, 100_000)).to.equal(
			814_500,
		);
		expect(firstLog.persistedDeliveryTimeoutMs(delivery, 1_000_000)).to.equal(
			8_066_250,
		);
		expect(
			firstLog.persistedDeliveryTimeoutMs({ ...delivery, timeout: 7 }, 100_000),
		).to.equal(7);

		const clock = sinon.stub(Date, "now").returns(now);
		const abortController = new AbortController();
		const cancellation = new Error("cancel receipt pacing");
		try {
			for (let request = 0; request < 16; request++) {
				expect(
					firstLog.reservePersistedReceiptEgress("abort-peer", 1n, 512, now),
				).to.equal(0);
			}
			setTimeout(() => abortController.abort(cancellation), 5);
			await expect(
				firstLog.waitForPersistedReceiptEgressAdmission(
					"abort-peer",
					1n,
					512,
					abortController.signal,
				),
			).to.be.rejectedWith("cancel receipt pacing");
		} finally {
			clock.restore();
		}
	});

	it("rejects non-integral persisted quorum and invalid deadlines before committing", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const attempt = (delivery: Record<string, unknown>) =>
			writer.add("invalid-options", {
				target: "replicators",
				delivery: {
					reliability: "persisted" as const,
					...delivery,
				} as any,
			});

		await expect(attempt({ minAcks: 1.5 })).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		await expect(
			attempt({ minAcks: Number.MAX_SAFE_INTEGER + 1 }),
		).to.be.rejectedWith(
			'persisted delivery requires a positive explicit "minAcks"',
		);
		await expect(
			attempt({ minAcks: 1, timeout: 2_147_483_648 }),
		).to.be.rejectedWith(
			"persisted delivery timeout must be a positive number no greater than 2147483647",
		);
		const cancellation = new Error("cancelled before append");
		const abortController = new AbortController();
		abortController.abort(cancellation);
		let abortedFailure: unknown;
		try {
			await attempt({ minAcks: 1, signal: abortController.signal });
		} catch (error) {
			abortedFailure = error;
		}
		expect(abortedFailure).equal(cancellation);
		expect(writer.log.log.length).to.equal(0);
	});

	it("snapshots persisted options before asynchronous append work", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const process = sinon.stub(log, "processLocalAppend").resolves();
		const settle = sinon.stub(log, "settlePersistedDelivery").resolves();
		const originalAbort = new AbortController();
		const replacementAbort = new AbortController();
		const delivery: any = {
			reliability: "persisted",
			minAcks: 2,
			requireRecipients: true,
			priority: 7,
			timeout: 5_000,
			signal: originalAbort.signal,
		};
		const options: any = {
			target: "replicators",
			replicate: false,
			replicas: 2,
			delivery,
		};

		try {
			const pending = writer.add("snapshot-options", options);
			delivery.reliability = "ack";
			delivery.minAcks = 0;
			delivery.requireRecipients = false;
			delivery.priority = -1;
			delivery.timeout = 1;
			delivery.signal = replacementAbort.signal;
			options.target = "none";
			options.replicate = true;
			options.replicas = 1;
			await pending;

			expect(process.callCount).to.equal(1);
			expect(process.firstCall.args[2]).to.include({
				target: "replicators",
				replicate: false,
				replicas: 2,
			});
			expect(process.firstCall.args[3].minReplicasValue).to.equal(2);
			expect(settle.callCount).to.equal(1);
			const captured = settle.firstCall.args[2];
			expect(captured).to.include({
				reliability: "persisted",
				minAcks: 2,
				requireRecipients: true,
				priority: 7,
				timeout: 5_000,
			});
			expect(captured.signal).to.equal(originalAbort.signal);
			originalAbort.abort();
			expect(captured.signal.aborted).to.equal(true);
		} finally {
			settle.restore();
			process.restore();
		}
	});

	it("reads hostile persisted option getters exactly once", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const process = sinon.stub(log, "processLocalAppend").resolves();
		const settle = sinon.stub(log, "settlePersistedDelivery").resolves();
		const deliveryReads = new Map<PropertyKey, number>();
		const optionReads = new Map<PropertyKey, number>();
		const deliveryTarget = {
			reliability: "persisted",
			minAcks: 1,
			requireRecipients: true,
			priority: 3,
			timeout: 5_000,
			signal: new AbortController().signal,
		};
		const delivery = new Proxy(deliveryTarget, {
			get(target, property, receiver) {
				const reads = (deliveryReads.get(property) ?? 0) + 1;
				deliveryReads.set(property, reads);
				if (reads > 1) throw new Error(`delivery.${String(property)} reread`);
				return Reflect.get(target, property, receiver);
			},
		});
		const optionTarget = {
			target: "replicators" as const,
			replicate: false,
			replicas: 1,
			delivery,
		};
		Object.defineProperty(optionTarget, "unrelated", {
			enumerable: true,
			get() {
				throw new Error("unrelated option getter was evaluated");
			},
		});
		const options = new Proxy(optionTarget, {
			get(target, property, receiver) {
				const reads = (optionReads.get(property) ?? 0) + 1;
				optionReads.set(property, reads);
				if (reads > 1) throw new Error(`options.${String(property)} reread`);
				return Reflect.get(target, property, receiver);
			},
		});

		try {
			await writer.add("proxy-options", options as any);
			expect(optionReads.get("delivery")).to.equal(1);
			for (const property of Reflect.ownKeys(deliveryTarget)) {
				expect(deliveryReads.get(property), String(property)).to.equal(1);
			}
			expect(settle.firstCall.args[2].minAcks).to.equal(1);
		} finally {
			settle.restore();
			process.restore();
		}
	});

	it("deep-snapshots finite nested append option containers", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const canTrim = () => true;
		const cacheId = () => "trim-cache";
		const trim: any = {
			type: "length",
			to: 7,
			from: 9,
			filter: { canTrim, cacheId },
		};
		const keypair = await X25519Keypair.create();
		const metaRecipient = (await Ed25519Keypair.create()).publicKey;
		const payloadRecipient = await X25519PublicKey.create();
		const signatureRecipient = await X25519PublicKey.create();
		const keypairPublicBytes = Uint8Array.from(keypair.publicKey.publicKey);
		const keypairSecretBytes = Uint8Array.from(keypair.secretKey.secretKey);
		const metaRecipientBytes = Uint8Array.from(metaRecipient.publicKey);
		const payloadRecipientBytes = Uint8Array.from(payloadRecipient.publicKey);
		const signatureRecipientBytes = Uint8Array.from(
			signatureRecipient.publicKey,
		);
		const signatureRecipients = { signer: [signatureRecipient] };
		const receiver: any = {
			meta: [metaRecipient],
			payload: [payloadRecipient],
			signatures: signatureRecipients,
		};
		const next = new ShallowEntry({
			hash: "original-parent",
			payloadSize: 17,
			head: false,
			meta: new ShallowMeta({
				gid: "original-gid",
				next: ["grandparent"],
				type: EntryType.APPEND,
				data: new Uint8Array([4, 5]),
				clock: new LamportClock({
					id: new Uint8Array([1, 2]),
					timestamp: new Timestamp({ wallTime: 11n, logical: 3 }),
				}),
			}),
		});
		const options: any = {
			delivery: false,
			trim,
			encryption: { keypair, receiver },
			meta: { next: [next] },
		};

		const captured = (writer.log as any).snapshotDocumentAppendOptions(options);
		trim.type = "time";
		trim.to = 1;
		trim.from = 1;
		trim.filter.canTrim = () => false;
		receiver.meta.push(await X25519PublicKey.create());
		receiver.payload[0] = await X25519PublicKey.create();
		signatureRecipients.signer.push(await X25519PublicKey.create());
		keypair.publicKey.publicKey.fill(99);
		keypair.secretKey.secretKey.fill(99);
		metaRecipient.publicKey.fill(99);
		payloadRecipient.publicKey.fill(99);
		signatureRecipient.publicKey.fill(99);
		next.hash = "mutated-parent";
		next.payloadSize = 1;
		next.head = true;
		next.meta.gid = "mutated-gid";
		next.meta.next[0] = "mutated-grandparent";
		next.meta.data![0] = 99;
		next.meta.clock.id[0] = 99;
		next.meta.clock.timestamp.wallTime = 99n;
		next.meta.clock.timestamp.logical = 99;

		expect(captured.trim).to.deep.include({
			type: "length",
			to: 7,
			from: 9,
		});
		expect(captured.trim.filter.canTrim).equal(canTrim);
		expect(captured.trim.filter.cacheId).equal(cacheId);
		expect(Object.isFrozen(captured.trim)).equal(true);
		expect(Object.isFrozen(captured.trim.filter)).equal(true);
		expect(captured.encryption.keypair).to.not.equal(keypair);
		expect(captured.encryption.keypair.publicKey.publicKey).to.deep.equal(
			keypairPublicBytes,
		);
		expect(captured.encryption.keypair.secretKey.secretKey).to.deep.equal(
			keypairSecretBytes,
		);
		expect(captured.encryption.receiver.meta[0]).to.not.equal(metaRecipient);
		expect(captured.encryption.receiver.meta[0].publicKey).to.deep.equal(
			metaRecipientBytes,
		);
		expect(captured.encryption.receiver.payload[0]).to.not.equal(
			payloadRecipient,
		);
		expect(captured.encryption.receiver.payload[0].publicKey).to.deep.equal(
			payloadRecipientBytes,
		);
		expect(captured.encryption.receiver.signatures.signer[0]).to.not.equal(
			signatureRecipient,
		);
		expect(
			captured.encryption.receiver.signatures.signer[0].publicKey,
		).to.deep.equal(signatureRecipientBytes);
		expect(captured.encryption.receiver.meta).to.have.length(1);
		expect(captured.encryption.receiver.payload).to.have.length(1);
		expect(captured.encryption.receiver.signatures.signer).to.have.length(1);
		expect(Object.isFrozen(captured.encryption)).equal(true);
		expect(Object.isFrozen(captured.encryption.receiver)).equal(true);
		expect(Object.isFrozen(captured.encryption.receiver.meta)).equal(true);
		expect(
			Object.isFrozen(captured.encryption.receiver.signatures.signer),
		).equal(true);
		const capturedNext = captured.meta.next[0] as ShallowEntry;
		expect(capturedNext).to.not.equal(next);
		expect(capturedNext.hash).equal("original-parent");
		expect(capturedNext.payloadSize).equal(17);
		expect(capturedNext.head).equal(false);
		expect(capturedNext.meta.gid).equal("original-gid");
		expect(capturedNext.meta.next).to.deep.equal(["grandparent"]);
		expect(capturedNext.meta.data).to.deep.equal(new Uint8Array([4, 5]));
		expect(capturedNext.meta.clock.id).to.deep.equal(new Uint8Array([1, 2]));
		expect(capturedNext.meta.clock.timestamp.wallTime).equal(11n);
		expect(capturedNext.meta.clock.timestamp.logical).equal(3);
		expect(Object.isFrozen(capturedNext)).equal(true);
		expect(Object.isFrozen(capturedNext.meta)).equal(true);
	});

	it("normalizes split-module crypto shapes in the lower Log", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const sender = await X25519Keypair.create();
		const xRecipient = await X25519PublicKey.create();
		const edRecipient = (await Ed25519Keypair.create()).publicKey;
		class ForeignXPublicKey {
			constructor(readonly publicKey: Uint8Array) {}
			equals() {
				return false;
			}
			hashcode() {
				return "foreign-x";
			}
			toString() {
				return "foreign-x";
			}
		}
		class ForeignEdPublicKey extends ForeignXPublicKey {
			toPeerId() {
				throw new Error("classification must not invoke provider methods");
			}
		}
		let signaturePublicKeyReads = 0;
		const signatureRecipients: Record<string, unknown> = {
			signer: [new ForeignXPublicKey(xRecipient.publicKey)],
		};
		Object.defineProperty(signatureRecipients, "publicKey", {
			enumerable: true,
			get() {
				signaturePublicKeyReads++;
				if (signaturePublicKeyReads > 1) {
					throw new Error("signature recipient map key reread");
				}
				return [new ForeignXPublicKey(xRecipient.publicKey)];
			},
		});
		const encryption: any = {
			keypair: {
				publicKey: new ForeignXPublicKey(sender.publicKey.publicKey),
				secretKey: { secretKey: sender.secretKey.secretKey },
			},
			receiver: {
				meta: [new ForeignEdPublicKey(edRecipient.publicKey)],
				payload: new ForeignXPublicKey(xRecipient.publicKey),
				signatures: signatureRecipients,
			},
		};
		const lowerLog = writer.log.log as any;
		const sharedLog = writer.log as any;
		const snapshot = sinon.spy(
			lowerLog,
			"snapshotAppendEncryptionForTrustedCaller",
		);
		const process = sinon.stub(sharedLog, "processLocalAppend").resolves();
		const settle = sinon.stub(sharedLog, "settlePersistedDelivery").resolves();

		try {
			await writer.add("split-module-encryption", {
				target: "replicators",
				delivery: { reliability: "persisted", minAcks: 1 },
				encryption,
			});
			const captured = snapshot.firstCall.returnValue;
			expect(captured.keypair).to.be.instanceOf(X25519Keypair);
			expect(captured.keypair.publicKey).to.be.instanceOf(X25519PublicKey);
			expect(captured.keypair.secretKey).to.be.instanceOf(X25519SecretKey);
			expect(captured.receiver.meta[0]).to.be.instanceOf(Ed25519PublicKey);
			expect(captured.receiver.payload).to.be.instanceOf(X25519PublicKey);
			expect(captured.receiver.signatures.signer[0]).to.be.instanceOf(
				X25519PublicKey,
			);
			expect(captured.receiver.signatures.publicKey[0]).to.be.instanceOf(
				X25519PublicKey,
			);
			expect(signaturePublicKeyReads).to.equal(1);
			expect(process.callCount).to.equal(1);
			expect(settle.callCount).to.equal(1);
		} finally {
			settle.restore();
			process.restore();
			snapshot.restore();
		}
	});

	it("reads explicit shallow-parent getters once", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const reads = new Map<string, number>();
		const once = <T extends object>(name: string, value: T): T =>
			new Proxy(value, {
				get(target, property, receiver) {
					const key = `${name}.${String(property)}`;
					const count = (reads.get(key) ?? 0) + 1;
					reads.set(key, count);
					if (count > 1) throw new Error(`${key} reread`);
					return Reflect.get(target, property, receiver);
				},
			});
		const timestamp = once(
			"timestamp",
			new Timestamp({ wallTime: 17n, logical: 2 }),
		);
		const clock = once(
			"clock",
			new LamportClock({ id: new Uint8Array([7]), timestamp }),
		);
		const meta = once(
			"meta",
			new ShallowMeta({
				gid: "getter-gid",
				next: ["getter-grandparent"],
				type: EntryType.APPEND,
				data: new Uint8Array([8]),
				clock,
			}),
		);
		const next = once(
			"next",
			new ShallowEntry({
				hash: "getter-parent",
				payloadSize: 23,
				head: false,
				meta,
			}),
		);

		const captured = (writer.log as any).snapshotDocumentAppendOptions({
			delivery: false,
			meta: { next: [next] },
		});
		expect(captured.meta.next[0].hash).to.equal("getter-parent");
		for (const [property, count] of reads) {
			expect(count, property).to.equal(1);
		}
	});

	it("captures a hollow full parent hash once through an actual append", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const seed = (
			await writer.add("hollow-parent-seed", {
				target: "none",
				meta: { next: [] },
			})
		).entry;
		const canonicalHash = seed.hash;
		let hashReads = 0;
		const hollowParent = {
			get hash() {
				hashReads++;
				if (hashReads > 1) throw new Error("hollow parent hash reread");
				return canonicalHash;
			},
			meta: seed.meta,
			payloadSize: seed.toShallow(true).payloadSize,
			head: true,
			size: seed.size,
			createdLocally: seed.createdLocally,
			init() {
				return this;
			},
			getNext() {
				return this.meta.next;
			},
			getClock() {
				return this.meta.clock;
			},
			getStorageBytes(): Uint8Array {
				throw new Error("hollow parent has no JS storage bytes");
			},
			verifySignatures() {
				return true;
			},
		};
		const sharedLog = writer.log as any;
		const process = sinon.stub(sharedLog, "processLocalAppend").resolves();
		const settle = sinon.stub(sharedLog, "settlePersistedDelivery").resolves();

		try {
			const child = await writer.add("hollow-parent-child", {
				target: "replicators",
				delivery: { reliability: "persisted", minAcks: 1 },
				meta: { next: [hollowParent as any] },
			});

			expect(hashReads).to.equal(1);
			expect(child.entry.meta.next).to.deep.equal([canonicalHash]);
		} finally {
			process.restore();
			settle.restore();
		}
	});

	it("keeps a detached full explicit parent available for missing-parent join", async () => {
		session = await TestSession.disconnected(2);
		const source = await session.peers[0].open(new EventStore<string, any>());
		const target = await session.peers[1].open(new EventStore<string, any>());
		const parent = (
			await source.add("detached-full-parent", {
				target: "none",
				meta: { next: [] },
			})
		).entry;
		const parentHash = parent.hash;
		const parentGid = parent.meta.gid;
		const lowerLog = target.log.log as any;
		const sharedLog = target.log as any;
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => (entered = resolve));
		const releasePromise = new Promise<void>((resolve) => (release = resolve));
		const originalGetNexts = lowerLog.getNextsForAppend.bind(lowerLog);
		const nextGate = sinon
			.stub(lowerLog, "getNextsForAppend")
			.callsFake(async (...args: unknown[]) => {
				entered();
				await releasePromise;
				return originalGetNexts(...args);
			});
		const process = sinon.stub(sharedLog, "processLocalAppend").resolves();
		const settle = sinon.stub(sharedLog, "settlePersistedDelivery").resolves();

		try {
			const pending = target.add("detached-full-child", {
				target: "replicators",
				delivery: { reliability: "persisted", minAcks: 1 },
				meta: { next: [parent] },
			});
			await enteredPromise;
			parent.hash = "caller-mutated-parent";
			parent.meta.gid = "caller-mutated-gid";
			parent.meta.next.push("caller-mutated-next");
			parent.meta.clock.timestamp.wallTime = 999n;
			release();
			const child = await pending;

			expect(child.entry.meta.next).to.deep.equal([parentHash]);
			expect(child.entry.meta.gid).equal(parentGid);
			expect(await target.log.log.has(parentHash)).equal(true);
			expect(target.log.log.length).equal(2);
			expect(process.callCount).equal(1);
			expect(settle.callCount).equal(1);
		} finally {
			release();
			nextGate.restore();
			process.restore();
			settle.restore();
		}
	});

	for (const nativeGraph of [false, true]) {
		it(`binds ${nativeGraph ? "native" : "generic"} callback-mutated entries to the committed CID`, async () => {
			session = await TestSession.disconnected(1);
			const writer = await session.peers[0].open(
				new EventStore<string, any>(),
				{ args: { nativeGraph } },
			);
			const log = writer.log as any;
			const process = sinon.stub(log, "processLocalAppend").resolves();
			const settle = sinon.stub(log, "settlePersistedDelivery").resolves();
			let committed:
				| { hash: string; gid: string; next: string[]; wallTime: bigint }
				| undefined;

			try {
				await writer.add(`callback-decoy-${nativeGraph}`, {
					target: "replicators",
					delivery: {
						reliability: "persisted",
						minAcks: 1,
						timeout: 5_000,
					},
					onChange: (change) => {
						const entry = change.added[0]!.entry;
						committed = {
							hash: entry.hash,
							gid: entry.meta.gid,
							next: [...entry.meta.next],
							wallTime: entry.meta.clock.timestamp.wallTime,
						};
						entry.hash = "decoy-cid";
						entry.meta.gid = "decoy-gid";
						entry.meta.next.push("decoy-parent");
						entry.meta.clock.timestamp.wallTime = 0n;
					},
				});

				expect(committed).to.exist;
				const record = settle.firstCall.args[0][0];
				expect(record.canonicalHash).to.equal(committed!.hash);
				const shallow = record.createDefaultPlanningSource();
				expect(shallow.hash).to.equal(committed!.hash);
				expect(shallow.meta.gid).to.equal(committed!.gid);
				expect(shallow.meta.next).to.deep.equal(committed!.next);
				expect(shallow.meta.clock.timestamp.wallTime).to.equal(
					committed!.wallTime,
				);
				const full = record.createFullPlanningSource();
				expect(full.hash).to.equal(committed!.hash);
				expect(full.meta.gid).to.equal(committed!.gid);
			} finally {
				settle.restore();
				process.restore();
			}
		});
	}

	it("does no committed-entry snapshot work for non-persisted appends", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const capture = sinon.spy(log, "capturePersistedLocalAppendCommit");
		try {
			await writer.add("ordinary-ack", {
				target: "none",
				delivery: { reliability: "ack", minAcks: 1 },
			});
			expect(capture.callCount).to.equal(0);
		} finally {
			capture.restore();
		}
	});

	it("classifies a callback failure after the local commit as retry-unsafe", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const callbackFailure = new Error("change callback failed");
		let failure: unknown;

		try {
			await writer.add("callback-failure", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 250,
				},
				onChange: () => {
					throw callbackFailure;
				},
			});
		} catch (error) {
			failure = error;
		}

		expect(failure).to.be.instanceOf(PersistedDeliveryError);
		expect((failure as PersistedDeliveryError).cause).to.equal(callbackFailure);
		expect((failure as PersistedDeliveryError).committedHashes).to.have.length(
			1,
		);
		expect(writer.log.log.length).to.equal(1);
	});

	it("classifies a lower post-write failure before onChange with exact hashes", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const postWriteFailure = new Error("injected lower post-write failure");
		const trim = sinon
			.stub(writer.log.log as any, "trimIfConfigured")
			.rejects(postWriteFailure);
		let failure: unknown;

		try {
			await writer.add("lower-post-write-failure", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 250,
				},
			});
		} catch (error) {
			failure = error;
		}

		expect(trim.callCount).to.equal(1);
		expect(failure).to.be.instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).to.equal(postWriteFailure);
		expect(persistedFailure.committedHashes).to.have.length(1);
		expect(
			await writer.log.log.has(persistedFailure.committedHashes[0]!),
		).to.equal(true);
	});

	it("keeps a generic index failure before row admission retry-safe", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const preCommitFailure = new Error("injected pre-admission failure");
		const entryIndex = (writer.log.log as any).entryIndex;
		const nativeGraph = entryIndex.properties.nativeGraph;
		entryIndex.properties.nativeGraph = undefined;
		const indexPut = sinon
			.stub(entryIndex.properties.index, "put")
			.rejects(preCommitFailure);
		let failure: unknown;

		try {
			await writer.add("pre-admission-failure", {
				target: "replicators",
				durability: "strict",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 250,
				},
			});
		} catch (error) {
			failure = error;
		} finally {
			indexPut.restore();
			entryIndex.properties.nativeGraph = nativeGraph;
		}

		expect(failure).to.equal(preCommitFailure);
		expect(failure).not.to.be.instanceOf(PersistedDeliveryError);
		expect(indexPut.callCount).to.equal(1);
	});

	it("classifies a generic failure after row admission with the exact hash", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const postCommitFailure = new Error("injected post-admission failure");
		const entryIndex = (writer.log.log as any).entryIndex;
		const nativeGraph = entryIndex.properties.nativeGraph;
		entryIndex.properties.nativeGraph = undefined;
		const notifyShadowedGids = sinon
			.stub(entryIndex, "notifyShadowedGids")
			.rejects(postCommitFailure);
		let failure: unknown;

		try {
			await writer.add("post-admission-failure", {
				target: "replicators",
				durability: "strict",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 250,
				},
			});
		} catch (error) {
			failure = error;
		} finally {
			notifyShadowedGids.restore();
			entryIndex.properties.nativeGraph = nativeGraph;
		}

		expect(failure).to.be.instanceOf(PersistedDeliveryError);
		const persistedFailure = failure as PersistedDeliveryError;
		expect(persistedFailure.cause).to.equal(postCommitFailure);
		expect(persistedFailure.retrySafe).to.equal(false);
		expect(persistedFailure.committedHashes).to.have.length(1);
		expect(
			await entryIndex.properties.index.get(
				toId(persistedFailure.committedHashes[0]!),
			),
		).not.to.equal(undefined);
		expect(entryIndex.insertionPromises.size).to.equal(0);
	});

	it("returns a persisted receipt from a default directory-backed peer", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);

		const log = writer.log as any;
		const originalConfirm = log._v2Send.confirmLatestForPeer.bind(log._v2Send);
		let freshnessConfirmed = false;
		const confirmation = sinon
			.stub(log._v2Send, "confirmLatestForPeer")
			.callsFake(async (...args: any[]) => {
				await originalConfirm(...args);
				freshnessConfirmed = true;
			});
		const originalPush = log.pushEntryHashes.bind(log);
		const transferAttempts: Array<{
			hashes: string[];
			freshnessConfirmed: boolean;
		}> = [];
		const push = sinon
			.stub(log, "pushEntryHashes")
			.callsFake(async (...args: any[]) => {
				transferAttempts.push({
					hashes: [...args[1]],
					freshnessConfirmed,
				});
				return originalPush(...args);
			});
		const optimisticDelivery = sinon.spy(log, "_appendDeliverToReplicators");
		const backfill = sinon.spy(log, "queueAppendBackfill");
		const request = sinon.spy(writer.log.rpc, "request");
		try {
			const { entry } = await writer.add("durable", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 15_000,
				},
			});

			expect(await receiver.log.log.has(entry.hash)).to.equal(true);
			expect(optimisticDelivery.notCalled).to.equal(true);
			expect(confirmation.called).to.equal(true);
			const entryTransfers = transferAttempts.filter(({ hashes }) =>
				hashes.includes(entry.hash),
			);
			expect(entryTransfers).not.to.be.empty;
			expect(
				entryTransfers.every(({ freshnessConfirmed }) => freshnessConfirmed),
			).to.equal(true);
			expect(
				backfill
					.getCalls()
					.some(
						(call) =>
							call.args[0] === receiver.node.identity.publicKey.hashcode() &&
							call.args[1].hash === entry.hash,
					),
			).to.equal(true);
			expect(
				request
					.getCalls()
					.some((call) => call.args[0] instanceof RequestPersistedEntriesV1),
			).to.equal(true);
		} finally {
			request.restore();
			backfill.restore();
			optimisticDelivery.restore();
			push.restore();
			confirmation.restore();
		}
	});

	it("requires an exact active V2 generation for receipt candidates and ingress", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const writerLog = writer.log as any;
		const receiverLog = receiver.log as any;
		const writerHash = writer.node.identity.publicKey.hashcode();
		const receiverHash = receiver.node.identity.publicKey.hashcode();

		expect(writerLog.persistedReceiptPeerSession(receiverHash)).to.exist;
		const capabilities = writerLog._peerSyncCapabilities.get(receiverHash);
		writerLog._peerSyncCapabilities.set(
			receiverHash,
			capabilities & ~SYNC_CAPABILITY_REPLICATION_INFO_V2_CONFIRM,
		);
		expect(writerLog.persistedReceiptPeerSession(receiverHash)).to.equal(
			undefined,
		);
		writerLog._peerSyncCapabilities.set(receiverHash, capabilities);
		expect(writerLog.persistedReceiptPeerSession(receiverHash)).to.exist;
		const candidateActive = sinon
			.stub(writerLog._v2Receive, "isCurrentActive")
			.returns(false);
		expect(writerLog.persistedReceiptPeerSession(receiverHash)).to.equal(
			undefined,
		);
		expect(candidateActive.calledOnce).to.equal(true);
		candidateActive.restore();

		const writerSession = receiverLog._peerSessions.current(writerHash);
		const senderTransportSession = writerLog.ownTransportSession();
		const missingHash = (await calculateRawCid(randomBytes(32))).cid;
		const request = new RequestPersistedEntriesV1({
			expectedReceiverSession: receiverLog.ownTransportSession(),
			hashes: [missingHash],
		});
		const context = {
			from: writer.node.identity.publicKey,
			message: { header: { session: senderTransportSession } },
		};
		const lane = {
			fromHash: writerHash,
			session: writerSession,
			receiveEpoch: receiverLog._peerSessions.receiveEpoch(writerHash),
			ownershipLifecycleController:
				receiverLog.captureReplicationOwnershipLifecycle(),
		};
		expect(
			receiverLog.isPersistedReceiptRequestSessionCurrent(
				request,
				context,
				lane,
			),
		).to.equal(true);
		const ingressActive = sinon
			.stub(receiverLog._v2Receive, "isCurrentActive")
			.returns(false);
		expect(
			receiverLog.isPersistedReceiptRequestSessionCurrent(
				request,
				context,
				lane,
			),
		).to.equal(false);
		expect(ingressActive.calledOnce).to.equal(true);
		const ingressAdmission = sinon.spy(
			receiverLog,
			"admitPersistedReceiptIngress",
		);
		expect(
			await receiverLog.handleRequestPersistedEntriesV1(request, context, lane),
		).to.equal(undefined);
		expect(ingressAdmission.notCalled).to.equal(true);
		expect(ingressActive.callCount).to.equal(2);
	});

	it("backfills every freshly planned leader after a smaller persisted quorum", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const selfHash = writer.node.identity.publicKey.hashcode();
		const initiallyPlannedLeaders = ["planned-leader-a", "stale-leader-b"];
		const freshlyPlannedLeaders = ["planned-leader-a", "fresh-leader-c"];
		const fullReplicaExtra = "full-replica-d";
		const crossGidExtra = "cross-gid-leader-e";
		const initialLeaders = new Map([
			[selfHash, { intersecting: true }],
			...initiallyPlannedLeaders.map(
				(peer) => [peer, { intersecting: true }] as const,
			),
		]);
		const freshLeaders = new Map([
			[selfHash, { intersecting: true }],
			...freshlyPlannedLeaders.map(
				(peer) => [peer, { intersecting: true }] as const,
			),
		]);
		const nativePlan = sinon
			.stub(log, "planNativeAppendFacts")
			.resolves(undefined);
		const plan = sinon.stub(log, "planEntryLeaders");
		plan.resolves({
			coordinates: [1],
			leaders: initialLeaders,
			isLeader: true,
		});
		const extras = sinon
			.stub(log, "collectDeferredAppendBackfillExtras")
			.callsFake(async () => ({
				assignmentExtraLeaders: new Map([
					[fullReplicaExtra, { intersecting: true }],
				]),
				deliveryExtraTargets: new Set([crossGidExtra]),
				extrasOwnershipRevision:
					log._instanceLifecycle._receiveOwnershipRevision,
			}));
		const settle = sinon
			.stub(log, "settlePersistedDelivery")
			.callsFake(async (...args: any[]) => {
				args[6]?.(
					[freshLeaders],
					log._instanceLifecycle._receiveOwnershipRevision,
				);
			});
		const optimisticDelivery = sinon.spy(log, "_appendDeliverToReplicators");
		const repairEntry = sinon.spy(log, "createEntryReplicatedForRepair");
		const backfill = sinon.stub(log, "queueAppendBackfill");
		backfill.onFirstCall().throws(new Error("one backfill target failed"));
		const lifecycle = log.captureReplicationOwnershipLifecycle();

		try {
			const { entry } = await writer.add("preserve-replication-degree", {
				target: "replicators",
				replicas: 3,
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 1_000,
				},
			});

			expect(optimisticDelivery.notCalled).to.equal(true);
			expect(settle.calledOnce).to.equal(true);
			expect(settle.firstCall.args[5]).to.equal(true);
			expect(plan.calledOnce).to.equal(true);
			expect(extras.calledOnce).to.equal(true);
			expect(backfill.callCount).to.equal(4);
			expect(backfill.getCalls().map((call) => call.args[0])).to.have.members([
				...freshlyPlannedLeaders,
				fullReplicaExtra,
				crossGidExtra,
			]);
			expect(
				backfill
					.getCalls()
					.every(
						(call) =>
							call.args[1].hash === entry.hash && call.args[2] === lifecycle,
					),
			).to.equal(true);
			expect(
				backfill.getCalls().some((call) => call.args[0] === "stale-leader-b"),
			).to.equal(false);
			const repairLeaders = repairEntry.lastCall.args[0].leaders as Map<
				string,
				unknown
			>;
			expect([...repairLeaders.keys()]).to.have.members([
				selfHash,
				...freshlyPlannedLeaders,
				fullReplicaExtra,
			]);
			expect(repairLeaders.has(crossGidExtra)).to.equal(false);
			expect(repairLeaders.has("stale-leader-b")).to.equal(false);
		} finally {
			backfill.restore();
			repairEntry.restore();
			optimisticDelivery.restore();
			settle.restore();
			extras.restore();
			plan.restore();
			nativePlan.restore();
		}
	});

	it("keeps full replicas and cross-GID owners in deferred dissemination", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("deferred-dissemination", {
			target: "none",
		});
		const log = writer.log as any;
		const selfHash = writer.node.identity.publicKey.hashcode();
		const lifecycle = log.captureReplicationOwnershipLifecycle();
		const fullReplicas = sinon
			.stub(log, "getNativeFullReplicaDeliveryCandidates")
			.resolves(new Set([selfHash, "full-replica"]));
		const references = sinon
			.stub(log, "_mergeLeadersFromGidReferences")
			.callsFake(async (...args: any[]) => {
				(args[2] as Map<string, { intersecting: boolean }>).set(
					"cross-gid-leader",
					{ intersecting: true },
				);
			});

		const extras = await log.collectDeferredAppendBackfillExtras(
			entry,
			2,
			new Map([[selfHash, { intersecting: true }]]),
			undefined,
			lifecycle,
		);

		expect([...extras.assignmentExtraLeaders.keys()]).to.deep.equal([
			"full-replica",
		]);
		expect([...extras.deliveryExtraTargets]).to.deep.equal([
			"cross-gid-leader",
		]);
		expect(extras.extrasOwnershipRevision).to.equal(
			log._instanceLifecycle._receiveOwnershipRevision,
		);
		expect(fullReplicas.calledOnceWith(2, selfHash)).to.equal(true);
		expect(references.called).to.equal(true);
		expect(references.lastCall.args[4]).to.deep.equal({
			freshLeaderPlan: true,
		});
	});

	it("does not let best-effort backfill mask the persisted-delivery failure", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const selfHash = writer.node.identity.publicKey.hashcode();
		const primaryFailure = new Error("receipt settlement failed");
		const settle = sinon
			.stub(log, "settlePersistedDelivery")
			.callsFake(async (...args: any[]) => {
				args[6]?.(
					[
						new Map([
							[selfHash, { intersecting: true }],
							["planned-repair-target", { intersecting: true }],
						]),
					],
					log._instanceLifecycle._receiveOwnershipRevision,
				);
				throw primaryFailure;
			});
		const backfill = sinon
			.stub(log, "queuePersistedAppendBackfill")
			.throws(new Error("secondary best-effort failure"));
		let failure: unknown;

		try {
			await writer.add("preserve-primary-failure", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 1_000,
				},
			});
		} catch (error) {
			failure = error;
		}

		expect(failure).to.be.instanceOf(PersistedDeliveryError);
		expect((failure as PersistedDeliveryError).cause).to.equal(primaryFailure);
		expect(settle.calledOnce).to.equal(true);
		expect(backfill.calledOnce).to.equal(true);
	});

	it("rejects append backfill captured by stale lifecycle or ownership state", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const { entry } = await writer.add("backfill-generation", {
			target: "none",
		});
		const staleLifecycle = log.captureReplicationOwnershipLifecycle();

		log.startRepairLifecycle();
		log.queueAppendBackfill(
			"stale-target",
			{ hash: "stale-entry" },
			staleLifecycle,
		);

		expect(log._appendBackfillPendingByTarget.size).to.equal(0);

		const currentLifecycle = log.captureReplicationOwnershipLifecycle();
		const staleOwnershipRevision =
			log._instanceLifecycle._receiveOwnershipRevision;
		log._instanceLifecycle._receiveOwnershipRevision++;
		const currentOwnershipRevision =
			log._instanceLifecycle._receiveOwnershipRevision;
		const backfill = sinon.stub(log, "queueAppendBackfill");
		log.queuePersistedAppendBackfill(
			{
				entry,
				coordinates: [1],
				assignmentExtraLeaders: new Map([
					["stale-full-replica", { intersecting: true }],
				]),
				deliveryExtraTargets: new Set(["stale-cross-gid"]),
				extrasOwnershipRevision: staleOwnershipRevision,
			},
			new Map([
				[writer.node.identity.publicKey.hashcode(), { intersecting: true }],
				["fresh-base-leader", { intersecting: true }],
			]),
			1,
			currentLifecycle,
			currentOwnershipRevision,
		);

		expect(backfill.calledOnceWith("fresh-base-leader")).to.equal(true);
		expect(backfill.neverCalledWith("stale-full-replica")).to.equal(true);
		expect(backfill.neverCalledWith("stale-cross-gid")).to.equal(true);
	});

	it("receipts a durable native receiver from its authoritative resident coordinates", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-persisted-delivery-native-"),
		);
		const writerDirectory = path.join(directory, "writer");
		const receiverDirectory = path.join(directory, "receiver");
		session = await TestSession.connected(2, [
			crashSafeDirectoryOptions(writerDirectory),
			crashSafeDirectoryOptions(receiverDirectory),
		]);
		const writerArgs = {
			replicas: { min: 2 },
			replicate: { offset: 0, factor: 1 },
			timeUntilRoleMaturity: 0,
			sync: { rawExchangeHeads: true },
		};
		const receiverArgs = {
			...writerArgs,
			nativeGraph: true,
			nativeBackbone: { optional: false },
		};
		const writer = await session.peers[0].open(new EventStore<string, any>(), {
			args: writerArgs,
		});
		const receiver = await EventStore.open<EventStore<string, any>>(
			writer.address!,
			session.peers[1],
			{ args: receiverArgs },
		);
		const receiverClone = receiver.clone();
		await writer.log.waitForReplicators({
			coverageThreshold: 1,
			roleAge: 0,
			timeout: 15_000,
		});
		await writer.log.waitForReplicator(receiver.node.identity.publicKey, {
			roleAge: 0,
			timeout: 15_000,
		});
		await waitForPersistedCapability(writer, receiver);

		const receiverLog = receiver.log as any;
		expect(receiverLog._nativeBackbone).to.exist;
		expect(
			receiverLog._coordinates.canUseBackboneOnlyCoordinatePersistence(),
		).to.equal(true);

		const { entry } = await writer.add("native-durable", {
			target: "replicators",
			delivery: {
				reliability: "persisted",
				minAcks: 1,
				timeout: 15_000,
			},
		});

		// The native receive fast path intentionally avoids the duplicate generic
		// coordinate write. A positive receipt must therefore be based on the
		// resident/native coordinate, not an accidentally populated generic row.
		expect(
			await receiverLog.entryCoordinatesIndex.get(toId(entry.hash)),
		).to.equal(undefined);
		expect(receiverLog._residentEntryCoordinatesByHash.get(entry.hash)).to
			.exist;
		expect(
			await receiverLog._coordinates.getAuthoritativeCoordinateEntryForReceipt(
				entry.hash,
			),
		).to.exist;
		expect(await receiverLog.remoteBlocks.localStore.has(entry.hash)).to.equal(
			true,
		);
		expect(
			await receiverLog.log.entryIndex.properties.index.get(toId(entry.hash)),
		).to.exist;

		// Native absence is authoritative too: never revive a removed coordinate
		// from a stale generic-index row when deciding whether to receipt it.
		const residentCoordinate = receiverLog._residentEntryCoordinatesByHash.get(
			entry.hash,
		);
		const authoritativeCoordinate =
			await receiverLog._coordinates.getAuthoritativeCoordinateEntryForReceipt(
				entry.hash,
			);
		const genericGet = sinon
			.stub(receiverLog.entryCoordinatesIndex, "get")
			.resolves({ value: authoritativeCoordinate });
		receiverLog._residentEntryCoordinatesByHash.delete(entry.hash);
		try {
			expect(
				await receiverLog._coordinates.getAuthoritativeCoordinateEntryForReceipt(
					entry.hash,
				),
			).to.equal(undefined);
			expect(genericGet.callCount).to.equal(0);
		} finally {
			receiverLog._residentEntryCoordinatesByHash.set(
				entry.hash,
				residentCoordinate,
			);
			genericGet.restore();
		}

		// Reopen the same on-disk native receiver without the writer. This is a
		// fresh-process-shaped hydration check; dedicated SIGKILL tests cover the
		// physical block/index/native-journal primitives separately.
		await session.stop();
		session = undefined;
		session = await TestSession.disconnected(1, [
			crashSafeDirectoryOptions(receiverDirectory),
		]);
		const reopened = await session.peers[0].open(receiverClone, {
			args: { ...receiverArgs, replicate: false },
		});
		const reopenedLog = reopened.log as any;
		expect(await reopenedLog.log.has(entry.hash)).to.equal(true);
		expect(
			await reopenedLog.entryCoordinatesIndex.get(toId(entry.hash)),
		).to.equal(undefined);
		expect(reopenedLog._residentEntryCoordinatesByHash.get(entry.hash)).to
			.exist;
	});

	it("does not count a receipt signed by a peer other than the requested leader", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("forged-receipt", { target: "none" });
		const log = writer.log as any;
		const leader = "durable-leader";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer: leader },
		};
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[leader, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const request = sinon.stub(log.rpc, "request").callsFake(async () => [
			{
				response: new ConfirmEntriesMessage({ hashes: [entry.hash] }),
				from: { hashcode: () => "forged-peer" },
				message: { header: { session: 1n } },
			},
		]);

		try {
			await expect(
				log.settlePersistedDelivery(planningRecords(log, [entry]), 1, {
					reliability: "persisted",
					minAcks: 1,
					timeout: 150,
				}),
			).to.be.rejectedWith(
				"Timed out waiting for 1 persisted remote replicas.",
			);
			expect(request.callCount).to.be.greaterThan(0);
			expect(request.firstCall.args[1].mode.to).to.deep.equal([leader]);
		} finally {
			request.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("waits for exact receiver freshness before the first transfer", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("receiver-freshness", {
			target: "none",
		});
		const log = writer.log as any;
		const peer = "fresh-receiver";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer },
		};
		const events: string[] = [];
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[peer, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const retry = sinon.stub(log, "waitPersistedReceiptRetry").resolves();
		const confirmation = sinon
			.stub(log._v2Send, "confirmLatestForPeer")
			.onFirstCall()
			.callsFake(async () => {
				events.push("freshness:blocked");
				throw new Error("receiver has not applied the role state");
			});
		confirmation.onSecondCall().callsFake(async () => {
			events.push("freshness:applied");
		});
		const waitForAdmission = sinon
			.stub(log, "waitForPersistedTransferAdmission")
			.resolves(true);
		const push = sinon
			.stub(log, "pushEntryHashes")
			.callsFake(async (...args: unknown[]) => {
				const hashes = args[1] as string[];
				const options = args[2] as any;
				events.push("transfer");
				options.onChunkAttempted(hashes);
				await options.onChunkSent(hashes);
			});
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				events.push("receipt");
				const message = args[0] as RequestPersistedEntriesV1;
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => peer },
						message: { header: { session: 1n } },
					},
				];
			});

		try {
			await log.deliverPersistedEntries([entry], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 1_000,
				},
			});
			expect(events).to.deep.equal([
				"freshness:blocked",
				"freshness:applied",
				"transfer",
				"receipt",
			]);
			expect(confirmation.callCount).to.equal(2);
			expect(push.calledOnce).to.be.true;
			expect(request.calledOnce).to.be.true;
		} finally {
			request.restore();
			push.restore();
			waitForAdmission.restore();
			confirmation.restore();
			retry.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("rejects a receipt request retargeted to the wrong receiver session", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const { entry } = await writer.add("wrong-receiver-session", {
			target: "replicators",
		});
		await waitForResolved(
			async () => expect(await receiver.log.log.has(entry.hash)).to.equal(true),
			{ timeout: 15_000 },
		);

		const receiverHash = receiver.node.identity.publicKey.hashcode();
		const current = (writer.log as any).persistedReceiptPeerSession(
			receiverHash,
		);
		expect(current).to.exist;
		const responses = await writer.log.rpc.request(
			new RequestPersistedEntriesV1({
				expectedReceiverSession: current.capabilitySession + 1n,
				hashes: [entry.hash],
			}),
			{
				mode: new SilentDelivery({ to: [receiverHash], redundancy: 1 }),
				amount: 1,
				timeout: 250,
			},
		);
		expect(responses).to.deep.equal([]);
	});

	it("reissues an idempotent receipt after the first durable response is lost", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const rpc = writer.log.rpc as any;
		const originalRequest = rpc.request.bind(rpc);
		let droppedResponseHashes: string[] | undefined;
		let receiptRequests = 0;
		const request = sinon
			.stub(rpc, "request")
			.callsFake(async (...args: any[]) => {
				const responses = await originalRequest(...args);
				if (args[0] instanceof RequestPersistedEntriesV1) {
					receiptRequests++;
					const confirmedHashes = responses.flatMap(
						(response: any) => response.response.hashes,
					);
					if (!droppedResponseHashes && confirmedHashes.length > 0) {
						droppedResponseHashes = confirmedHashes;
						throw new Error("dropped persisted receipt response");
					}
				}
				return responses;
			});

		try {
			const { entry } = await writer.add("retry-after-durable-response", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 15_000,
				},
			});
			expect(droppedResponseHashes).to.deep.equal([entry.hash]);
			expect(receiptRequests).to.be.greaterThanOrEqual(2);
			expect(await receiver.log.log.has(entry.hash)).to.equal(true);
		} finally {
			request.restore();
		}
	});

	it("requests a receipt repair after a dropped initial entry", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const sharedLog = writer.log as any;
		let droppedInitialEntry = false;
		const originalPushEntryHashes = sharedLog.pushEntryHashes.bind(sharedLog);
		const attemptedReceiptRepair = new Set<string>();
		const presentAfterReceiptRepair = new Set<string>();
		const pushEntryHashes = sinon
			.stub(sharedLog, "pushEntryHashes")
			.callsFake(async (...args: any[]) => {
				const hashes = [...args[1]] as string[];
				if (args[2]?.repairHint !== true && !droppedInitialEntry) {
					droppedInitialEntry = true;
					// Model a transport attempt whose entry never arrives. Settlement must
					// probe it, observe the empty receipt, and schedule the repair round.
					args[2]?.onChunkAttempted?.(hashes);
					return;
				}
				const receiptRepairHashes =
					args[2]?.repairHint === true && args[2]?.operationQueue ? hashes : [];
				for (const hash of receiptRepairHashes) {
					// Background convergence can fill the entry after the missing
					// receipt response but before this repair dispatch. The invariant
					// is that settlement attempts the repair and the receiver has the
					// entry after it, not that absence remains observable at dispatch.
					attemptedReceiptRepair.add(hash);
				}
				const result = await originalPushEntryHashes(...args);
				for (const hash of receiptRepairHashes) {
					if (await receiver.log.log.has(hash)) {
						presentAfterReceiptRepair.add(hash);
					}
				}
				return result;
			});

		try {
			const { entry } = await writer.add("repair-before-receipt", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 15_000,
				},
			});

			expect(droppedInitialEntry).to.equal(true);
			expect(attemptedReceiptRepair.has(entry.hash)).to.equal(true);
			expect(presentAfterReceiptRepair.has(entry.hash)).to.equal(true);
			expect(await receiver.log.log.has(entry.hash)).to.equal(true);
		} finally {
			pushEntryHashes.restore();
		}
	});

	it("lets a durable receipt rescue a lost post-admission confirmation", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const result = await writer.addMany(
			Array.from({ length: 257 }, (_, index) => `confirm-loss-${index}`),
			{ target: "none" },
		);
		const log = writer.log as any;
		const peer = "durable-peer";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer },
		};
		const firstChunk = result.entries.slice(0, 256).map((entry) => entry.hash);
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[peer, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const waitForAdmission = sinon.stub(
			log,
			"waitForPersistedTransferAdmission",
		);
		waitForAdmission.resolves(true);
		waitForAdmission.onFirstCall().resolves(false);
		const send = sinon.stub(log.rpc, "send").resolves();
		let requestCalls = 0;
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				requestCalls++;
				const message = args[0] as RequestPersistedEntriesV1;
				return [
					{
						response: new ConfirmEntriesMessage({
							hashes: requestCalls === 1 ? firstChunk : message.hashes,
						}),
						from: { hashcode: () => peer },
						message: { header: { session: 1n } },
					},
				];
			});

		try {
			await log.deliverPersistedEntries(result.entries, {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 5_000,
				},
			});

			const transfers = send
				.getCalls()
				.map((call) => call.args[0])
				.filter(
					(message): message is ExchangeHeadsMessage<any> =>
						message instanceof ExchangeHeadsMessage,
				);
			const initialHashes = transfers
				.filter(
					(message) => (message.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT) === 0,
				)
				.flatMap((message) =>
					message.heads.map((head: any) => head.entry.hash),
				);
			const repairHashes = transfers
				.filter(
					(message) => (message.reserved[0] & EXCHANGE_HEADS_REPAIR_HINT) !== 0,
				)
				.flatMap((message) =>
					message.heads.map((head: any) => head.entry.hash),
				);
			expect(initialHashes).to.deep.equal(firstChunk);
			expect(repairHashes).to.deep.equal([result.entries[256]!.hash]);
			expect(
				waitForAdmission.getCalls().map((call) => call.args[1]),
			).to.deep.equal([firstChunk, [result.entries[256]!.hash]]);
			expect(
				request.getCalls().map((call) => call.args[0].hashes),
			).to.deep.equal([firstChunk, [result.entries[256]!.hash]]);
		} finally {
			request.restore();
			send.restore();
			waitForAdmission.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("reaches a healthy ninth leader past eight unconfirmed leaders", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("healthy-quorum", { target: "none" });
		const log = writer.log as any;
		const healthy = "healthy-leader";
		const rejecting = Array.from(
			{ length: 8 },
			(_, index) => `rejecting-leader-${index}`,
		);
		const peers = [...rejecting, healthy];
		const sessions = new Map(
			peers.map((peer, index) => [
				peer,
				{
					capabilitySession: BigInt(index + 1),
					peerSession: { peer },
				},
			]),
		);
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map(peers.map((peer) => [peer, {}])));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake((...args: unknown[]) => sessions.get(args[0] as string));
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const waitForAdmission = sinon
			.stub(log, "waitForPersistedTransferAdmission")
			.callsFake((...args: unknown[]) => {
				if (args[0] === healthy) return Promise.resolve(true);
				const signal = args[3] as AbortSignal;
				return new Promise<boolean>((resolve) => {
					const timeout = setTimeout(() => resolve(false), 1_500);
					const aborted = () => {
						clearTimeout(timeout);
						resolve(false);
					};
					if (signal.aborted) aborted();
					else signal.addEventListener("abort", aborted, { once: true });
				});
			});
		const send = sinon.stub(log.rpc, "send").callsFake((...args: unknown[]) => {
			const options = args[1] as any;
			const peer = options.mode.to[0] as string;
			if (peer === healthy) return Promise.resolve();
			const signal = options.signal as AbortSignal;
			return new Promise<void>((_resolve, reject) => {
				const aborted = () =>
					reject(signal.reason ?? new Error("chunk send aborted"));
				if (signal.aborted) aborted();
				else signal.addEventListener("abort", aborted, { once: true });
			});
		});
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				const peer = (args[1] as any).mode.to[0] as string;
				if (peer !== healthy) return [];
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => healthy },
						message: {
							header: {
								session: sessions.get(healthy)!.capabilitySession,
							},
						},
					},
				];
			});

		try {
			const started = Date.now();
			await log.deliverPersistedEntries([entry], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 2_000,
				},
			});
			expect(Date.now() - started).to.be.lessThan(1_800);
			expect(
				request.getCalls().some((call) => call.args[1].mode.to[0] === healthy),
			).to.equal(true);
			expect(
				send
					.getCalls()
					.filter((call) => call.args[0] instanceof ExchangeHeadsMessage)
					.every((call) => call.args[1].mode instanceof SilentDelivery),
			).to.equal(true);
		} finally {
			request.restore();
			send.restore();
			waitForAdmission.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("waits for both block and coordinate durability barriers", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const receiverLog = receiver.log as any;
		const receiptStorage = receiverLog.resolvePersistedReceiptStorage();
		const resolveReceiptStorage = sinon
			.stub(receiverLog, "resolvePersistedReceiptStorage")
			.returns(receiptStorage);
		const blockDurability = receiptStorage.block;
		const coordinateDurability = receiptStorage.coordinate;
		expect(blockDurability).not.equal(coordinateDurability);

		let releaseBlock!: () => void;
		let releaseCoordinate!: () => void;
		const blockGate = new Promise<void>((resolve) => {
			releaseBlock = resolve;
		});
		const coordinateGate = new Promise<void>((resolve) => {
			releaseCoordinate = resolve;
		});
		const blockBarrier = sinon
			.stub(blockDurability, "barrier")
			.callsFake(() => blockGate);
		const coordinateBarrier = sinon
			.stub(coordinateDurability, "barrier")
			.callsFake(() => coordinateGate);
		let settled = false;
		try {
			const write = writer
				.add("barrier-gated", {
					target: "replicators",
					delivery: {
						reliability: "persisted",
						minAcks: 1,
						timeout: 15_000,
					},
				})
				.then((result) => {
					settled = true;
					return result;
				});
			await waitForResolved(() => {
				expect(blockBarrier.calledOnce).to.equal(true);
				expect(coordinateBarrier.calledOnce).to.equal(true);
			});
			expect(settled).to.equal(false);
			releaseBlock();
			await new Promise((resolve) => setTimeout(resolve, 10));
			expect(settled).to.equal(false);
			releaseCoordinate();
			await write;
			expect(settled).to.equal(true);
		} finally {
			releaseBlock();
			releaseCoordinate();
			blockBarrier.restore();
			coordinateBarrier.restore();
			resolveReceiptStorage.restore();
		}
	});

	it("does not force durability barriers for absent receipt candidates", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);
		const receiverLog = receiver.log as any;
		const storage = receiverLog.resolvePersistedReceiptStorage();
		const barrierSpies = [...new Set(Object.values(storage))].map((store) =>
			sinon.spy(store as { barrier(): unknown }, "barrier"),
		);
		const lowerFlush = sinon.spy(
			receiverLog.log.entryIndex,
			"flushPendingWrites",
		);
		const coordinateFlush = sinon.spy(
			receiverLog._coordinates,
			"flushNativeBackboneCoordinateJournal",
		);
		const leaderPlan = sinon.spy(receiverLog, "findLeadersFromEntry");
		const missingHash = (await calculateRawCid(randomBytes(32))).cid;
		const receiverHash = receiver.node.identity.publicKey.hashcode();

		try {
			const responses = await (writer.log.rpc as any).request(
				new RequestPersistedEntriesV1({
					expectedReceiverSession: receiverLog.ownTransportSession(),
					hashes: [missingHash],
				}),
				{
					mode: new SilentDelivery({ to: [receiverHash], redundancy: 1 }),
					amount: 1,
					timeout: 5_000,
				},
			);
			expect(responses).to.have.length(1);
			expect(responses[0]!.response).to.be.instanceOf(ConfirmEntriesMessage);
			expect(responses[0]!.response.hashes).to.deep.equal([]);
			expect(lowerFlush.callCount).to.equal(0);
			expect(coordinateFlush.callCount).to.equal(0);
			expect(barrierSpies.every((spy) => spy.callCount === 0)).to.equal(true);
			expect(leaderPlan.callCount).to.equal(0);
		} finally {
			leaderPlan.restore();
			coordinateFlush.restore();
			lowerFlush.restore();
			for (const spy of barrierSpies) spy.restore();
		}
	});

	it("lets a memory non-replicator retire after a directory-backed receiver acknowledges", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-persisted-delivery-receiver-"),
		);
		session = await TestSession.connected(2, [
			{},
			crashSafeDirectoryOptions(path.join(directory, "receiver")),
		]);
		const writer = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicas: { min: 1 },
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const receiver = await EventStore.open<EventStore<string, any>>(
			writer.address!,
			session.peers[1],
			{
				args: {
					replicas: { min: 1 },
					replicate: { offset: 0, factor: 1 },
					timeUntilRoleMaturity: 0,
				},
			},
		);
		await writer.log.waitForReplicator(receiver.node.identity.publicKey, {
			roleAge: 0,
			timeout: 15_000,
		});
		await waitForPersistedCapability(writer, receiver);

		const { entry } = await writer.add("retire-writer", {
			target: "replicators",
			delivery: {
				reliability: "persisted",
				minAcks: 1,
				timeout: 15_000,
			},
		});
		const leaders = await writer.log.findLeadersFromEntry(entry, 1, {
			roleAge: 0,
		});
		expect(leaders.has(writer.node.identity.publicKey.hashcode())).to.equal(
			false,
		);
		expect(leaders.has(receiver.node.identity.publicKey.hashcode())).to.equal(
			true,
		);
		expect(await receiver.log.log.has(entry.hash)).to.equal(true);
	});

	it("times out after committing when no remote can issue receipts", async () => {
		const { writer, receiver } = await openPair(false);
		const receiverHash = receiver.node.identity.publicKey.hashcode();
		expect(
			((writer.log as any)._peerSyncCapabilities.get(receiverHash) ?? 0) &
				SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
		).to.equal(0);

		await expect(
			writer.add("non-capable", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 250,
				},
			}),
		).to.be.rejectedWith("Timed out waiting for 1 persisted remote replicas.");
		expect(writer.log.log.length).to.equal(1);
	});

	it("does not count legacy confirms as persisted receipts", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);

		const rpc = writer.log.rpc as any;
		const originalRequest = rpc.request.bind(rpc);
		const abortController = new AbortController();
		const cancellation = new Error("legacy confirm was ignored");
		let legacyConfirmInjected = false;
		let persistedRequestCount = 0;
		const request = sinon
			.stub(rpc, "request")
			.callsFake(async (...args: any[]) => {
				if (args[0] instanceof RequestPersistedEntriesV1) {
					persistedRequestCount++;
					if (!legacyConfirmInjected) {
						legacyConfirmInjected = true;
						await writer.log.onMessage(
							new ConfirmEntriesMessage({ hashes: args[0].hashes }),
							{ from: receiver.node.identity.publicKey } as any,
						);
					} else {
						abortController.abort(cancellation);
					}
					return [];
				}
				return originalRequest(...args);
			});

		try {
			await expect(
				writer.add("legacy-confirm", {
					target: "replicators",
					delivery: {
						reliability: "persisted",
						minAcks: 1,
						signal: abortController.signal,
					},
				}),
			).to.be.rejectedWith(cancellation.message);
			expect(
				request
					.getCalls()
					.some((call) => call.args[0] instanceof RequestPersistedEntriesV1),
			).to.equal(true);
			expect(legacyConfirmInjected).to.equal(true);
			expect(persistedRequestCount).to.be.greaterThan(1);
			expect(writer.log.log.length).to.equal(1);
		} finally {
			request.restore();
		}
	});

	it("does not count a stale persisted response session", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);

		const rpc = writer.log.rpc as any;
		const originalRequest = rpc.request.bind(rpc);
		const abortController = new AbortController();
		const cancellation = new Error("stale persisted response was ignored");
		let staleResponseInjected = false;
		let persistedRequestCount = 0;
		const request = sinon
			.stub(rpc, "request")
			.callsFake(async (...args: any[]) => {
				if (
					args[0] instanceof RequestPersistedEntriesV1 &&
					staleResponseInjected
				) {
					persistedRequestCount++;
					abortController.abort(cancellation);
					return [];
				}
				const responses = await originalRequest(...args);
				if (args[0] instanceof RequestPersistedEntriesV1) {
					persistedRequestCount++;
					for (const response of responses) {
						const wasCurrentSession =
							response.message.header.session ===
							args[0].expectedReceiverSession;
						response.message.header.session += 1n;
						if (
							wasCurrentSession &&
							response.response instanceof ConfirmEntriesMessage &&
							response.response.hashes.some((hash: string) =>
								args[0].hashes.includes(hash),
							)
						) {
							staleResponseInjected = true;
						}
					}
				}
				return responses;
			});

		try {
			await expect(
				writer.add("stale-session", {
					target: "replicators",
					delivery: {
						reliability: "persisted",
						minAcks: 1,
						signal: abortController.signal,
					},
				}),
			).to.be.rejectedWith(cancellation.message);
			expect(
				request
					.getCalls()
					.some((call) => call.args[0] instanceof RequestPersistedEntriesV1),
			).to.equal(true);
			expect(staleResponseInjected).to.equal(true);
			expect(persistedRequestCount).to.be.greaterThan(1);
			expect(writer.log.log.length).to.equal(1);
		} finally {
			request.restore();
		}
	});

	it("settles behind eight stale candidates without waiting for an extra peer", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("coordinator", { target: "none" });
		const log = writer.log as any;
		const peers = Array.from({ length: 10 }, (_, index) => `peer-${index}`);
		const sessions = new Map(
			peers.map((peer, index) => [
				peer,
				{
					capabilitySession: BigInt(index + 1),
					peerSession: { peer },
				},
			]),
		);
		const leaderPlan = new Map(peers.map((peer) => [peer, {}]));
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(leaderPlan);
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake((...args: unknown[]) => sessions.get(args[0] as string));
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				const options = args[1] as any;
				const peer = options.mode.to[0] as string;
				const index = peers.indexOf(peer);
				if (index < 8) {
					await new Promise((resolve) => setTimeout(resolve, 10));
					return [];
				}
				if (index === 9) {
					return new Promise((_resolve, reject) => {
						const rejectOnAbort = () =>
							reject(options.signal.reason ?? new Error("aborted"));
						if (options.signal.aborted) {
							rejectOnAbort();
							return;
						}
						options.signal.addEventListener("abort", rejectOnAbort, {
							once: true,
						});
					});
				}
				const receiptSession = sessions.get(peer)!;
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => peer },
						message: {
							header: { session: receiptSession.capabilitySession },
						},
					},
				];
			});

		try {
			const started = Date.now();
			await log.settlePersistedDelivery(planningRecords(log, [entry]), 1, {
				reliability: "persisted",
				minAcks: 1,
				timeout: 1_000,
			});
			expect(Date.now() - started).to.be.lessThan(500);
			expect(
				request.getCalls().some((call) => call.args[1].mode.to[0] === "peer-8"),
			).to.equal(true);
		} finally {
			request.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("requires two distinct receipt signers for minAcks two", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("two-signers", { target: "none" });
		const log = writer.log as any;
		const peers = ["first-durable-peer", "second-durable-peer"];
		const sessions = new Map(
			peers.map((peer, index) => [
				peer,
				{
					capabilitySession: BigInt(index + 1),
					peerSession: { peer },
				},
			]),
		);
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map(peers.map((peer) => [peer, {}])));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake((...args: unknown[]) => sessions.get(args[0] as string));
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		let releaseSecond!: () => void;
		const secondReceiptGate = new Promise<void>((resolve) => {
			releaseSecond = resolve;
		});
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				const options = args[1] as any;
				const peer = options.mode.to[0] as string;
				if (peer === peers[1]) {
					await secondReceiptGate;
				}
				const receiptSession = sessions.get(peer)!;
				const receipt = {
					response: new ConfirmEntriesMessage({ hashes: message.hashes }),
					from: { hashcode: () => peer },
					message: {
						header: { session: receiptSession.capabilitySession },
					},
				};
				return peer === peers[0] ? [receipt, receipt] : [receipt];
			});

		let settled = false;
		try {
			const settlement = log
				.settlePersistedDelivery(planningRecords(log, [entry]), 2, {
					reliability: "persisted",
					minAcks: 2,
					timeout: 1_000,
				})
				.then(() => {
					settled = true;
				});
			await waitForResolved(() => expect(request.callCount).to.equal(2));
			await new Promise((resolve) => setTimeout(resolve, 10));
			expect(settled).to.equal(false);
			releaseSecond();
			await settlement;
			expect(settled).to.equal(true);
		} finally {
			releaseSecond();
			request.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("requires an independent quorum for every hash in a batch", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const first = await writer.add("first", { target: "none" });
		const second = await writer.add("second", { target: "none" });
		const log = writer.log as any;
		const peerByHash = new Map([
			[first.entry.hash, "first-peer"],
			[second.entry.hash, "second-peer"],
		]);
		const sessions = new Map(
			[...peerByHash.values()].map((peer, index) => [
				peer,
				{
					capabilitySession: BigInt(index + 1),
					peerSession: { peer },
				},
			]),
		);
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake((...args: unknown[]) => {
				const entry = args[0] as { hash: string };
				return Promise.resolve(
					new Map([[peerByHash.get(entry.hash) as string, {}]]),
				);
			});
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake((...args: unknown[]) => sessions.get(args[0] as string));
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		let releaseSecond!: () => void;
		const secondReceiptGate = new Promise<void>((resolve) => {
			releaseSecond = resolve;
		});
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				const peer = (args[1] as any).mode.to[0] as string;
				if (peer === "second-peer") {
					await secondReceiptGate;
				}
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => peer },
						message: {
							header: { session: sessions.get(peer)!.capabilitySession },
						},
					},
				];
			});

		let settled = false;
		try {
			const settlement = log
				.settlePersistedDelivery(
					planningRecords(log, [first.entry, second.entry]),
					1,
					{
						reliability: "persisted",
						minAcks: 1,
						timeout: 1_000,
					},
				)
				.then(() => {
					settled = true;
				});
			await waitForResolved(() => expect(request.callCount).to.equal(2));
			await new Promise((resolve) => setTimeout(resolve, 10));
			expect(settled).to.equal(false);
			releaseSecond();
			await settlement;
			expect(settled).to.equal(true);
			expect(
				request
					.getCalls()
					.map((call) => [call.args[1].mode.to[0], call.args[0].hashes]),
			).to.have.deep.members([
				["first-peer", [first.entry.hash]],
				["second-peer", [second.entry.hash]],
			]);
		} finally {
			releaseSecond();
			request.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("retries a failed receipt request in a fresh round", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("retry", { target: "none" });
		const log = writer.log as any;
		const peer = "durable-peer";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer },
		};
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[peer, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const push = sinon
			.stub(log, "pushEntryHashes")
			.callsFake(async (...args: unknown[]) => {
				const hashes = args[1] as string[];
				const options = args[2] as {
					onChunkAttempted?: (hashes: string[]) => void;
				};
				options.onChunkAttempted?.(hashes);
			});
		const request = sinon
			.stub(log.rpc, "request")
			.onFirstCall()
			.rejects(new Error("dropped request"));
		request.onSecondCall().callsFake(async (...args: unknown[]) => {
			const message = args[0] as RequestPersistedEntriesV1;
			return [
				{
					response: new ConfirmEntriesMessage({ hashes: message.hashes }),
					from: { hashcode: () => peer },
					message: { header: { session: 1n } },
				},
			];
		});

		try {
			await log.deliverPersistedEntries([entry], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 10_000,
				},
			});
			expect(request.callCount).to.equal(2);
			expect(request.firstCall.args[0]).not.to.equal(
				request.secondCall.args[0],
			);
			expect(request.secondCall.args[1].timeout).to.be.greaterThan(
				request.firstCall.args[1].timeout,
			);
			const targetPushes = push
				.getCalls()
				.filter(
					(call) =>
						call.args[0] === peer &&
						call.args[1]?.length === 1 &&
						call.args[1][0] === entry.hash,
				);
			expect(targetPushes.length).to.equal(1);
			expect(targetPushes[0]!.args[2].repairHint).to.equal(false);
		} finally {
			request.restore();
			push.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("retries only hashes still missing a same-session receipt", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const first = await writer.add("carry-first", { target: "none" });
		const second = await writer.add("carry-second", { target: "none" });
		const log = writer.log as any;
		const peer = "durable-peer";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer },
		};
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[peer, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const waitForAdmission = sinon
			.stub(log, "waitForPersistedTransferAdmission")
			.resolves(true);
		const send = sinon.stub(log.rpc, "send").resolves();
		let requestCalls = 0;
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				requestCalls++;
				const message = args[0] as RequestPersistedEntriesV1;
				return [
					{
						response: new ConfirmEntriesMessage({
							hashes: requestCalls === 1 ? [first.entry.hash] : message.hashes,
						}),
						from: { hashcode: () => peer },
						message: { header: { session: 1n } },
					},
				];
			});

		try {
			await log.deliverPersistedEntries([first.entry, second.entry], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 5_000,
				},
			});
			expect(
				send
					.getCalls()
					.filter((call) => call.args[0] instanceof ExchangeHeadsMessage)
					.map((call) =>
						call.args[0].heads.map((head: any) => head.entry.hash),
					),
			).to.deep.equal([
				[first.entry.hash, second.entry.hash],
				[second.entry.hash],
			]);
			expect(
				request.getCalls().map((call) => call.args[0].hashes),
			).to.deep.equal([
				[first.entry.hash, second.entry.hash],
				[second.entry.hash],
			]);
		} finally {
			request.restore();
			send.restore();
			waitForAdmission.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("binds a receipt to the ownership revision captured by its round", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("ownership-race", { target: "none" });
		const log = writer.log as any;
		const peer = "same-returning-leader";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer },
		};
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[peer, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		allowPersistedReceiptFreshness(log);
		const waitForAdmission = sinon
			.stub(log, "waitForPersistedTransferAdmission")
			.resolves(true);
		const send = sinon.stub(log.rpc, "send").resolves();
		let releaseSecond!: () => void;
		const secondReceiptGate = new Promise<void>((resolve) => {
			releaseSecond = resolve;
		});
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				const receipt = {
					response: new ConfirmEntriesMessage({ hashes: message.hashes }),
					from: { hashcode: () => peer },
					message: { header: { session: 1n } },
				};
				if (request.callCount === 1) {
					log.invalidateLeaderSelectionContextCache();
					return [receipt];
				}
				await secondReceiptGate;
				return [receipt];
			});

		let settled = false;
		try {
			const settlement = log
				.settlePersistedDelivery(planningRecords(log, [entry]), 1, {
					reliability: "persisted",
					minAcks: 1,
					timeout: 1_000,
				})
				.then(() => {
					settled = true;
				});
			await waitForResolved(() => expect(request.callCount).to.equal(2));
			await new Promise((resolve) => setTimeout(resolve, 10));
			expect(settled).to.equal(false);
			releaseSecond();
			await settlement;
			expect(settled).to.equal(true);
			expect(request.callCount).to.equal(2);
		} finally {
			releaseSecond();
			request.restore();
			send.restore();
			waitForAdmission.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("purges carried receipts when the peer session changes", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const first = await writer.add("session-first", { target: "none" });
		const second = await writer.add("session-second", { target: "none" });
		const log = writer.log as any;
		const peer = "returning-peer";
		let receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer, generation: 1 },
		};
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[peer, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake(() => receiptSession);
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const push = sinon.stub(log, "pushEntryHashes").resolves();
		let requestCalls = 0;
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				requestCalls++;
				const message = args[0] as RequestPersistedEntriesV1;
				const capturedSession = receiptSession.capabilitySession;
				if (requestCalls === 1) {
					setTimeout(() => {
						receiptSession = {
							capabilitySession: 2n,
							peerSession: { peer, generation: 2 },
						};
					}, 0);
				}
				return [
					{
						response: new ConfirmEntriesMessage({
							hashes: requestCalls === 1 ? [first.entry.hash] : message.hashes,
						}),
						from: { hashcode: () => peer },
						message: { header: { session: capturedSession } },
					},
				];
			});

		try {
			await log.settlePersistedDelivery(
				planningRecords(log, [first.entry, second.entry]),
				1,
				{
					reliability: "persisted",
					minAcks: 1,
					timeout: 5_000,
				},
			);
			expect(
				request.getCalls().map((call) => ({
					hashes: call.args[0].hashes,
					session: call.args[0].expectedReceiverSession,
				})),
			).to.deep.equal([
				{
					hashes: [first.entry.hash, second.entry.hash],
					session: 1n,
				},
				{
					hashes: [first.entry.hash, second.entry.hash],
					session: 2n,
				},
			]);
			expect(push.callCount).to.equal(0);
		} finally {
			request.restore();
			push.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("does not receipt an entry rejected by receiver canAppend", async () => {
		const { writer, receiver } = await openPair(true, () => false);
		await waitForPersistedCapability(writer, receiver);

		await expect(
			writer.add("rejected", {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 500,
				},
			}),
		).to.be.rejectedWith("Timed out waiting for 1 persisted remote replicas.");
		const [entry] = await writer.log.log.toArray();
		expect(entry).to.exist;
		expect(await receiver.log.log.has(entry.hash)).to.equal(false);
	});

	it("groups each hash only to its planned persisted owner", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const first = await writer.add("first-owner", { target: "none" });
		const second = await writer.add("second-owner", { target: "none" });
		const log = writer.log as any;
		const ownerByHash = new Map([
			[first.entry.hash, "first-peer"],
			[second.entry.hash, "second-peer"],
		]);
		const sessions = new Map(
			[...ownerByHash.values()].map((peer, index) => [
				peer,
				{
					capabilitySession: BigInt(index + 1),
					peerSession: { peer },
				},
			]),
		);
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake((...args: unknown[]) => {
				const entry = args[0] as { hash: string };
				return Promise.resolve(
					new Map([[ownerByHash.get(entry.hash) as string, {}]]),
				);
			});
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake((...args: unknown[]) => sessions.get(args[0] as string));
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const waitForAdmission = sinon
			.stub(log, "waitForPersistedTransferAdmission")
			.resolves(true);
		const send = sinon.stub(log.rpc, "send").resolves();
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				const peer = (args[1] as any).mode.to[0] as string;
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => peer },
						message: {
							header: { session: sessions.get(peer)!.capabilitySession },
						},
					},
				];
			});

		try {
			await log.deliverPersistedEntries([first.entry, second.entry], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 5_000,
				},
			});
			expect(
				send
					.getCalls()
					.filter((call) => call.args[0] instanceof ExchangeHeadsMessage)
					.map((call) => [
						call.args[1].mode.to[0],
						call.args[0].heads.map((head: any) => head.entry.hash),
					]),
			).to.have.deep.members([
				["first-peer", [first.entry.hash]],
				["second-peer", [second.entry.hash]],
			]);
			expect(
				request
					.getCalls()
					.map((call) => [call.args[1].mode.to[0], call.args[0].hashes]),
			).to.have.deep.members([
				["first-peer", [first.entry.hash]],
				["second-peer", [second.entry.hash]],
			]);
		} finally {
			request.restore();
			send.restore();
			waitForAdmission.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("hashes self once and validates aligned final leaders for a persisted batch", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const result = await writer.addMany(
			["two-plan-delivery-first", "two-plan-delivery-second"],
			{ target: "none" },
		);
		const entries = result.entries;
		const log = writer.log as any;
		const peersByHash = new Map([
			[entries[0]!.hash, "first-durable-peer"],
			[entries[1]!.hash, "second-durable-peer"],
		]);
		const sessions = new Map(
			[...peersByHash.values()].map((peer, index) => [
				peer,
				{
					capabilitySession: BigInt(index + 1),
					peerSession: { peer },
				},
			]),
		);
		const events: string[] = [];
		const planLeaders = sinon
			.stub(log, "planPersistedDeliveryLeaders")
			.callsFake(async (...args: unknown[]) => {
				const plannedEntries = args[0] as Array<{ canonicalHash: string }>;
				events.push("plan");
				return plannedEntries.map(
					(entry) =>
						new Map([
							[peersByHash.get(entry.canonicalHash)!, { intersecting: true }],
						]),
				);
			});
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake((...args: unknown[]) => sessions.get(args[0] as string));
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const waitForAdmission = sinon
			.stub(log, "waitForPersistedTransferAdmission")
			.resolves(true);
		const send = sinon.stub(log.rpc, "send").resolves();
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				const peer = (args[1] as any).mode.to[0] as string;
				events.push(`receipt:${peer}`);
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => peer },
						message: {
							header: { session: sessions.get(peer)!.capabilitySession },
						},
					},
				];
			});
		const selfHashcode = sinon.spy(writer.node.identity.publicKey, "hashcode");

		try {
			selfHashcode.resetHistory();
			await log.deliverPersistedEntries(entries, {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 5_000,
				},
			});
			expect(planLeaders.callCount).equal(2);
			expect(selfHashcode.callCount).equal(1);
			expect(events[0]).equal("plan");
			expect(events.at(-1)).equal("plan");
			expect(events.slice(1, -1)).to.have.members([
				"receipt:first-durable-peer",
				"receipt:second-durable-peer",
			]);
		} finally {
			selfHashcode.restore();
			request.restore();
			send.restore();
			waitForAdmission.restore();
			stable.restore();
			peerSession.restore();
			planLeaders.restore();
		}
	});

	it("uses native persisted leader samples and falls back on an incomplete batch", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const result = await writer.addMany(["native-plan-a", "native-plan-b"], {
			target: "none",
		});
		const entries = result.entries;
		const log = writer.log as any;
		const originalRangePlanner = log._nativeRangePlanner;
		const originalBackbone = log._nativeBackbone;
		const nativeLeaders = [
			new Map([["native-a", { intersecting: true }]]),
			new Map([["native-b", { intersecting: true }]]),
		];
		const fallbackLeaders = [
			new Map([["fallback-a", { intersecting: true }]]),
			new Map([["fallback-b", { intersecting: true }]]),
		];
		const nativePlan = sinon.stub().returns(nativeLeaders);
		log._nativeRangePlanner = undefined;
		log._nativeBackbone = { planLeaderSamplesForGidsBatch: nativePlan };
		const context = sinon.spy(log, "createLeaderSelectionContext");
		const fallback = sinon.stub(log, "planEntryLeaderBatch").resolves(
			fallbackLeaders.map((leaders) => ({
				coordinates: [],
				leaders,
				isLeader: false,
			})),
		);

		try {
			const lifecycle = log.captureReplicationOwnershipLifecycle();
			const planned = await log.planPersistedDeliveryLeaders(
				planningRecords(log, entries),
				1,
				lifecycle,
			);
			expect(planned).to.deep.equal(nativeLeaders);
			expect(nativePlan.callCount).to.equal(1);
			expect(fallback.callCount).to.equal(0);
			expect(context.firstCall.args[0]).to.include({
				freshLeaderPlan: true,
				persist: false,
			});
			expect(nativePlan.firstCall.args[0]).to.deep.equal(
				entries.map((entry) => ({ gid: entry.meta.gid, replicas: 1 })),
			);

			nativePlan.returns([nativeLeaders[0]]);
			const fallbackPlanned = await log.planPersistedDeliveryLeaders(
				planningRecords(log, entries),
				1,
				lifecycle,
			);
			expect(fallbackPlanned).to.deep.equal(fallbackLeaders);
			expect(nativePlan.callCount).to.equal(2);
			expect(fallback.callCount).to.equal(1);
		} finally {
			fallback.restore();
			context.restore();
			log._nativeRangePlanner = originalRangePlanner;
			log._nativeBackbone = originalBackbone;
		}
	});

	it("bypasses the leader-selection context cache for a fresh receipt plan", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const staleContext = {
			roleAge: 123,
			selfHash: "stale-self",
			selfReplicating: false,
			peerFilter: new Set(["stale-peer"]),
			peerFilterArray: ["stale-peer"],
		};
		log._leaderSelectionContextCache = {
			expiresAt: Date.now() + 1_000,
			context: staleContext,
		};

		const cached = await log.createLeaderSelectionContext();
		expect(cached.selfHash).to.equal("stale-self");
		const fresh = await log.createLeaderSelectionContext({
			freshLeaderPlan: true,
		});
		expect(fresh.selfHash).to.equal(writer.node.identity.publicKey.hashcode());
		expect(fresh.selfHash).to.not.equal(cached.selfHash);
	});

	it("reuses one routing full-replica map for persisted delivery", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const result = await writer.addMany(["full-replica-a", "full-replica-b"], {
			target: "none",
		});
		const entries = result.entries;
		const log = writer.log as any;
		const originalRangePlanner = log._nativeRangePlanner;
		const originalBackbone = log._nativeBackbone;
		const fullReplicaLeaders = new Map([
			["full-replica-peer", { intersecting: true }],
		]);
		const sampledLeaders = entries.map(
			(_, index) => new Map([[`sampled-${index}`, { intersecting: true }]]),
		);
		const fullReplicaPlan = sinon.stub().returns(fullReplicaLeaders);
		const nativePlan = sinon.stub().returns(sampledLeaders);
		log._nativeRangePlanner = {
			getRoutingFullReplicaLeaders: fullReplicaPlan,
			planLeaderSamplesForGidsBatch: nativePlan,
		};
		log._nativeBackbone = undefined;
		const fallback = sinon.stub(log, "planEntryLeaderBatch").resolves([]);

		try {
			const lifecycle = log.captureReplicationOwnershipLifecycle();
			const planned = await log.planPersistedDeliveryLeaders(
				planningRecords(log, entries),
				1,
				lifecycle,
			);
			expect(planned).to.have.length(entries.length);
			expect(planned[0]).to.equal(fullReplicaLeaders);
			expect(planned[1]).to.equal(fullReplicaLeaders);
			expect(fullReplicaPlan.callCount).to.equal(1);
			expect(fullReplicaPlan.firstCall.args[0]).to.equal(1);
			expect(fullReplicaPlan.firstCall.args[1]).to.include({
				expandPeerFilter: true,
				fullReplicaFallback: true,
				includeStrictFullReplica: true,
			});
			expect(nativePlan.callCount).to.equal(0);
			expect(fallback.callCount).to.equal(0);

			fullReplicaPlan.returns(undefined);
			const sampled = await log.planPersistedDeliveryLeaders(
				planningRecords(log, entries),
				1,
				lifecycle,
			);
			expect(sampled).to.deep.equal(sampledLeaders);
			expect(nativePlan.callCount).to.equal(1);
			expect(fallback.callCount).to.equal(0);
		} finally {
			fallback.restore();
			log._nativeRangePlanner = originalRangePlanner;
			log._nativeBackbone = originalBackbone;
		}
	});

	it("materializes full entries for a custom persisted leader hook", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("custom-leader-entry", {
			target: "none",
		});
		const log = writer.log as any;
		const customLeaderHook = sinon.stub(log, "findLeadersFromEntry");
		let materializationCount = 0;

		try {
			const records = log.createPersistedDeliveryPlanningRecords(
				[log.createPreparedLocalAppendCommit(entry)],
				() => {
					materializationCount++;
					return [entry];
				},
			);
			expect(materializationCount).equal(1);
			expect(records).to.have.length(1);
			const planned = records[0].createFullPlanningSource();
			expect(planned).to.not.equal(entry);
			expect(planned.hash).to.equal(entry.hash);
		} finally {
			customLeaderHook.restore();
		}
	});

	it("gives a mutating custom planner a fresh canonical entry on every replan", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("custom-planner-replan", {
			target: "none",
		});
		const log = writer.log as any;
		const record = log.snapshotPersistedDeliveryPlanningEntry(entry);
		const observed: Array<{ source: object; hash: string; gid: string }> = [];
		const planner = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async (source: any) => {
				observed.push({
					source,
					hash: source.hash,
					gid: source.meta.gid,
				});
				source.hash = "planner-decoy";
				source.meta.gid = "planner-decoy-gid";
				return new Map([["leader", { intersecting: true }]]);
			});

		try {
			const lifecycle = log.captureReplicationOwnershipLifecycle();
			await log.planPersistedDeliveryLeaders([record], 1, lifecycle);
			await log.planPersistedDeliveryLeaders([record], 1, lifecycle);
			expect(observed).to.have.length(2);
			expect(observed[0]!.source).to.not.equal(observed[1]!.source);
			expect(observed.map(({ hash }) => hash)).to.deep.equal([
				entry.hash,
				entry.hash,
			]);
			expect(observed.map(({ gid }) => gid)).to.deep.equal([
				entry.meta.gid,
				entry.meta.gid,
			]);
		} finally {
			planner.restore();
		}
	});

	it("includes post-commit entry preparation in the persisted deadline", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const { entry } = await writer.add("deadline-materialization", {
			target: "none",
		});
		const log = writer.log as any;
		const settle = sinon.stub(log, "settlePersistedDelivery").resolves();
		let failure: unknown;

		try {
			await log.deliverPersistedPlanningEntries(
				() => {
					const materializationEndsAt = Date.now() + 10;
					while (Date.now() < materializationEndsAt) {
						// Deliberately synchronous: the numeric deadline must still catch it
						// before the timeout callback gets an event-loop turn.
					}
					return [entry];
				},
				[entry.hash],
				{
					target: "replicators",
					delivery: {
						reliability: "persisted",
						minAcks: 1,
						timeout: 1,
					},
				},
			);
		} catch (error) {
			failure = error;
		}

		expect(failure).to.be.instanceOf(PersistedDeliveryError);
		expect((failure as PersistedDeliveryError).cause).to.have.property(
			"message",
			"Timed out waiting for 1 persisted remote replicas.",
		);
		expect(settle.callCount).to.equal(0);
	});

	it("chunks a 513-entry persisted batch into 512 plus one per peer", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const result = await writer.addMany(
			Array.from({ length: 513 }, (_, index) => `entry-${index}`),
			{ target: "none" },
		);
		const log = writer.log as any;
		const peer = "durable-peer";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer },
		};
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map([[peer, {}]]));
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const waitForAdmission = sinon
			.stub(log, "waitForPersistedTransferAdmission")
			.resolves(true);
		const send = sinon.stub(log.rpc, "send").resolves();
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0] as RequestPersistedEntriesV1;
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => peer },
						message: { header: { session: 1n } },
					},
				];
			});

		try {
			await log.deliverPersistedEntries(result.entries, {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 5_000,
				},
			});
			const transfers = send
				.getCalls()
				.filter((call) => call.args[0] instanceof ExchangeHeadsMessage);
			expect(transfers.length).to.be.at.least(3);
			expect(
				transfers.every((call) => call.args[0].heads.length <= 256),
			).to.equal(true);
			expect(
				transfers.flatMap((call) =>
					call.args[0].heads.map((head: any) => head.entry.hash),
				),
			).to.deep.equal(result.entries.map((entry) => entry.hash));
			const receiptRequests = request
				.getCalls()
				.map((call) => call.args[0])
				.filter(
					(message): message is RequestPersistedEntriesV1 =>
						message instanceof RequestPersistedEntriesV1,
				);
			expect(
				receiptRequests.map((message) => message.hashes.length),
			).to.deep.equal([512, 1]);
			expect(
				receiptRequests.flatMap((message) => message.hashes),
			).to.deep.equal(result.entries.map((entry) => entry.hash));
		} finally {
			request.restore();
			send.restore();
			waitForAdmission.restore();
			stable.restore();
			peerSession.restore();
			findLeaders.restore();
		}
	});

	it("paces an 8193-hash receipt batch without a redundant repair", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());
		const log = writer.log as any;
		const peer = "durable-peer";
		const receiptSession = {
			capabilitySession: 1n,
			peerSession: { peer },
		};
		const entries = Array.from({ length: 8_193 }, (_, index) => ({
			canonicalHash: `synthetic-${index}`,
			createDefaultPlanningSource: () => {
				throw new Error("leader planner is stubbed in this test");
			},
		}));
		const leaders = new Map([[peer, {}]]);
		const plan = sinon
			.stub(log, "planPersistedDeliveryLeaders")
			.callsFake(async (...args: unknown[]) =>
				new Array((args[0] as unknown[]).length).fill(leaders),
			);
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.returns(receiptSession);
		allowPersistedReceiptFreshness(log);
		const stable = sinon
			.stub(log, "isReceiveOwnershipSnapshotStable")
			.returns(true);
		const push = sinon.spy(log, "pushEntryHashes");
		const requestTimes: number[] = [];
		const request = sinon
			.stub(log.rpc, "request")
			.callsFake(async (...args: unknown[]) => {
				requestTimes.push(Date.now());
				const message = args[0] as RequestPersistedEntriesV1;
				return [
					{
						response: new ConfirmEntriesMessage({ hashes: message.hashes }),
						from: { hashcode: () => peer },
						message: { header: { session: 1n } },
					},
				];
			});

		try {
			await log.settlePersistedDelivery(entries, 1, {
				reliability: "persisted",
				minAcks: 1,
			});
			expect(request.callCount).to.equal(17);
			expect(
				request.getCalls().map((call) => call.args[0].hashes.length),
			).to.deep.equal([...new Array(16).fill(512), 1]);
			expect(requestTimes[16]! - requestTimes[0]!).to.be.at.least(100);
			expect(push.callCount).to.equal(0);
		} finally {
			request.restore();
			push.restore();
			stable.restore();
			peerSession.restore();
			plan.restore();
		}
	});

	it("rejects chained appendMany persisted delivery before committing", async () => {
		session = await TestSession.disconnected(1);
		const writer = await session.peers[0].open(new EventStore<string, any>());

		await expect(
			writer.addMany(["a", "b", "c"], {
				target: "replicators",
				delivery: {
					reliability: "persisted",
					minAcks: 1,
					timeout: 15_000,
				},
			}),
		).to.be.rejectedWith(
			"persisted delivery is not supported for chained appendMany; use independent document puts",
		);
		expect(writer.log.log.length).to.equal(0);
	});
});
