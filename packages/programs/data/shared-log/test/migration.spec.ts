import { keys } from "@libp2p/crypto";
import { SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { SyncCapabilitiesMessage } from "../src/exchange-heads.js";
import {
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	FullReplicationInfoV2Message,
	RequestReplicationInfoMessage,
	RequestReplicationInfoV2Message,
	ResponseRoleMessage,
	StoppedReplicating,
	StoppedReplicationInfoV2Message,
} from "../src/replication.js";
import { Replicator } from "../src/role.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe(`migration-8-9`, function () {
	let session: TestSession;
	let db1: EventStore<string, any>, db2: EventStore<string, any>;
	let fakeOldLegacyRequests = 0;
	// Legacy traffic other than RequestReplicationInfoMessage crossing the
	// fake-old boundary in either direction: ResponseRole, All, Added, Stopped.
	// The default-mode test asserts this stays 0, so a re-enabled legacy
	// announcement path cannot hide behind the fixture's silent swallowing.
	let fakeOldLegacyAnnouncements = 0;
	let fakeOldV2Messages = 0;

	const isReplicationInfoV2Message = (message: unknown) =>
		message instanceof SyncCapabilitiesMessage ||
		message instanceof RequestReplicationInfoV2Message ||
		message instanceof FullReplicationInfoV2Message ||
		message instanceof AddedReplicationInfoV2Message ||
		message instanceof StoppedReplicationInfoV2Message;

	const isLegacyAnnouncementMessage = (message: unknown) =>
		message instanceof ResponseRoleMessage ||
		message instanceof AllReplicatingSegmentsMessage ||
		message instanceof AddedReplicationSegmentMessage ||
		message instanceof StoppedReplicating;

	const setup = async (compatibility?: number, order: boolean = false) => {
		fakeOldLegacyRequests = 0;
		fakeOldLegacyAnnouncements = 0;
		fakeOldV2Messages = 0;
		session = await TestSession.disconnected(2, [
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
							168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
							159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53,
							142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72,
							148, 82, 66, 138, 199, 185,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
							157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120,
							122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251,
							100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6,
							174, 212, 159, 187, 2, 137, 47, 192,
						]),
					),
				},
			},
		]);

		const db = new EventStore<string, any>();

		const createV8 = () => {
			const db1 = db.clone();
			const onMessageDefault = db1.log.onMessage.bind(db1.log);
			return session.peers[0].open(db1, {
				args: {
					replicate: {
						factor: 1,
					},
					// The fixture must not need the compatibility option for default
					// opens: only explicit pre-v10 opens pass the key at all.
					...(compatibility !== undefined ? { compatibility } : {}),
					onMessage: async (msg, context) => {
						if (isReplicationInfoV2Message(msg)) {
							fakeOldV2Messages++;
							return;
						}
						if (isLegacyAnnouncementMessage(msg)) {
							fakeOldLegacyAnnouncements++;
						}
						if (
							msg instanceof AddedReplicationSegmentMessage ||
							msg instanceof StoppedReplicating
						) {
							return; // this message type did not exist before
						}
						if (msg instanceof AllReplicatingSegmentsMessage) {
							return; // this message type did not exist before
						}
						if (msg instanceof RequestReplicationInfoMessage) {
							fakeOldLegacyRequests++;
							// TODO we never respond to this message, nor in older version do we need to send it
							// we are keeping this here to mimic the old behaviour
							await db1.log.rpc.send(
								new ResponseRoleMessage({
									role: new Replicator({ factor: 1, offset: 0 }),
								}),
								{
									mode: new SilentDelivery({
										to: [context.from!],
										redundancy: 1,
									}),
								},
							);
							return;
						}
						return onMessageDefault(msg, context);
					},
				},
			});
		};

		const createV9 = () => {
			return session.peers[1].open(db.clone(), {
				args: {
					replicate: {
						factor: 1,
					},
					...(compatibility !== undefined ? { compatibility } : {}),
				},
			});
		};

		if (order) {
			db1 = await createV8();
			db2 = await createV9();
		} else {
			db2 = await createV9();
			db1 = await createV8();
		}

		// Install both directions of the fake-old protocol boundary before the
		// peers connect. This prevents the current implementation hidden behind
		// the fixture from making the compatibility assertions pass through V2.
		const originalFakeOldSend = db1.log.rpc.send.bind(db1.log.rpc);
		(db1.log.rpc as any).send = async (message: unknown, options: unknown) => {
			if (isReplicationInfoV2Message(message)) {
				fakeOldV2Messages++;
				return;
			}
			if (isLegacyAnnouncementMessage(message)) {
				fakeOldLegacyAnnouncements++;
			}
			if (
				message instanceof AddedReplicationSegmentMessage ||
				message instanceof AllReplicatingSegmentsMessage ||
				message instanceof StoppedReplicating
			) {
				return;
			}
			return originalFakeOldSend(message as any, options as any);
		};

		await session.connect([[session.peers[0], session.peers[1]]]);

		await db1.waitFor(session.peers[1].peerId);
		await db2.waitFor(session.peers[0].peerId);
	};

	afterEach(async () => {
		if (db1 && db1.closed === false) {
			await db1.drop();
		}
		if (db2 && db2.closed === false) {
			await db2.drop();
		}

		await session?.stop();
	});

	it("8-9, replicates database of 1 entry", async () => {
		await setup(8);

		const value = "hello";

		await db1.add(value);
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
		expect(fakeOldLegacyRequests).to.be.greaterThan(0);
		expect(fakeOldV2Messages).to.be.greaterThan(0);
	});

	it("9-8, replicates database of 1 entry", async () => {
		await setup(8, true);

		const value = "hello";

		await db2.add(value);
		await waitForResolved(() => expect(db1.log.log.length).equal(1));
		expect(fakeOldLegacyRequests).to.be.greaterThan(0);
		expect(fakeOldV2Messages).to.be.greaterThan(0);
	});

	it("does not fall back to legacy when compatibility is omitted", async () => {
		await setup(undefined);
		const value = "hello";
		await db1.add(value);
		await expect(
			waitForResolved(() => expect(db2.log.log.length).equal(1), {
				timeout: 3000,
			}).catch(() => {
				throw new Error("timeout");
			}),
		).to.be.rejectedWith("timeout");
		// No legacy fallback of ANY class may cross the boundary in default
		// mode: not the request, and none of ResponseRole/All/Added/Stopped in
		// either direction (previously swallowed silently by the fixture).
		expect(fakeOldLegacyRequests).to.equal(0);
		expect(fakeOldLegacyAnnouncements).to.equal(0);
		expect(fakeOldV2Messages).to.be.greaterThan(0);
	});

	it("v8 uses simple sync u32", async () => {
		await setup(8);
		expect(db1.log.syncronizer).to.be.instanceOf(SimpleSyncronizer);
		expect(db1.log.domain.resolution).to.equal("u32");
	});

	it("v9 uses simple sync u32", async () => {
		await setup(9);
		expect(db1.log.syncronizer).to.be.instanceOf(SimpleSyncronizer);
		expect(db1.log.domain.resolution).to.equal("u32");
	});

	it("v0 stays on simple sync u32", async () => {
		await setup(0);
		expect(db1.log.syncronizer).to.be.instanceOf(SimpleSyncronizer);
		expect(db1.log.domain.resolution).to.equal("u32");
	});

	it("default open uses iblt u64", async () => {
		await setup();
		expect(db1.log.syncronizer).to.be.instanceOf(RatelessIBLTSynchronizer);
		expect(db1.log.domain.resolution).to.equal("u64");
	});
});
