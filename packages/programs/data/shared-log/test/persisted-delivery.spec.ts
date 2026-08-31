import { createStore } from "@peerbit/any-store";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { randomBytes } from "@peerbit/crypto";
import { toId } from "@peerbit/indexer-interface";
import { create as createSQLiteIndices } from "@peerbit/indexer-sqlite3";
import { SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import sinon from "sinon";
import { PersistedDeliveryError } from "../src/errors.js";
import {
	EXCHANGE_HEADS_REPAIR_HINT,
	ExchangeHeadsMessage,
	SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
} from "../src/exchange-heads.js";
import {
	ConfirmEntriesMessage,
	RequestPersistedEntriesV1,
} from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/index.js";

describe("append delivery options — persisted receipts", function () {
	this.timeout(60_000);

	let session: TestSession | undefined;
	let directory: string | undefined;

	const crashSafeDirectoryOptions = (directory: string) => ({
		directory,
		storage: {
			storeFactory: (storeDirectory?: string) => createStore(storeDirectory),
		},
		indexer: (indexDirectory?: string) =>
			createSQLiteIndices(indexDirectory),
	});

	afterEach(async () => {
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
				const capabilities = (writer.log as any)._peerSyncCapabilities.get(
					receiverHash,
				);
				expect(
					capabilities & SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS,
				).to.equal(SYNC_CAPABILITY_PERSISTED_ENTRY_RECEIPTS);
			},
			{ timeout: 15_000 },
		);
	};

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
			expect(
				request
					.getCalls()
					.some((call) => call.args[0] instanceof RequestPersistedEntriesV1),
			).to.equal(true);
		} finally {
			request.restore();
		}
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
				log.settlePersistedDelivery([entry], 1, {
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
		const rpc = sharedLog.rpc as any;
		const originalSend = rpc.send.bind(rpc);
		let droppedInitialEntry = false;
		const send = sinon.stub(rpc, "send").callsFake(async (...args: any[]) => {
			if (args[0] instanceof ExchangeHeadsMessage) {
				for (const head of args[0].heads) {
					const operation = await head.entry.getPayloadValue();
					if (operation.value === "repair-before-receipt") {
						if (!droppedInitialEntry) {
							droppedInitialEntry = true;
							return;
						}
					}
				}
			}
			return originalSend(...args);
		});
		const originalPushEntryHashes = sharedLog.pushEntryHashes.bind(sharedLog);
		const attemptedReceiptRepair = new Set<string>();
		const presentAfterReceiptRepair = new Set<string>();
		const pushEntryHashes = sinon
			.stub(sharedLog, "pushEntryHashes")
			.callsFake(async (...args: any[]) => {
				const receiptRepairHashes =
					args[2]?.repairHint === true && args[2]?.operationQueue
						? ([...args[1]] as string[])
						: [];
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
			send.restore();
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

	it("does not count legacy confirms or a stale persisted response session", async () => {
		const { writer, receiver } = await openPair(true);
		await waitForPersistedCapability(writer, receiver);

		const rpc = writer.log.rpc as any;
		const originalRequest = rpc.request.bind(rpc);
		let legacyConfirmInjected = false;
		const request = sinon
			.stub(rpc, "request")
			.callsFake(async (...args: any[]) => {
				const responses = await originalRequest(...args);
				if (args[0] instanceof RequestPersistedEntriesV1) {
					if (!legacyConfirmInjected) {
						legacyConfirmInjected = true;
						await writer.log.onMessage(
							new ConfirmEntriesMessage({ hashes: args[0].hashes }),
							{ from: receiver.node.identity.publicKey } as any,
						);
					}
					for (const response of responses) {
						response.message.header.session += 1n;
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
						timeout: 350,
					},
				}),
			).to.be.rejectedWith(
				"Timed out waiting for 1 persisted remote replicas.",
			);
			expect(
				request
					.getCalls()
					.some((call) => call.args[0] instanceof RequestPersistedEntriesV1),
			).to.equal(true);
			expect(legacyConfirmInjected).to.equal(true);
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
			await log.settlePersistedDelivery([entry], 1, {
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
				.settlePersistedDelivery([entry], 2, {
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
				.settlePersistedDelivery([first.entry, second.entry], 1, {
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
				.settlePersistedDelivery([entry], 1, {
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
			await log.settlePersistedDelivery([first.entry, second.entry], 1, {
				reliability: "persisted",
				minAcks: 1,
				timeout: 5_000,
			});
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
				const plannedEntries = args[0] as Array<{ hash: string }>;
				events.push("plan");
				return plannedEntries.map(
					(entry) =>
						new Map([[peersByHash.get(entry.hash)!, { intersecting: true }]]),
				);
			});
		const peerSession = sinon
			.stub(log, "persistedReceiptPeerSession")
			.callsFake((...args: unknown[]) => sessions.get(args[0] as string));
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
				entries,
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
				entries,
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
				entries,
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
				entries,
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
			const planningEntries = log.createPersistedDeliveryPlanningEntries(
				[
					{
						hash: entry.hash,
						coordinateFields: { hash: entry.hash },
					},
				],
				() => {
					materializationCount++;
					return [entry];
				},
			);
			expect(materializationCount).equal(1);
			expect(planningEntries).to.deep.equal([entry]);
		} finally {
			customLeaderHook.restore();
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
			hash: `synthetic-${index}`,
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
