import { keys } from "@libp2p/crypto";
import { SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	RequestReplicationInfoMessage,
	ResponseRoleMessage,
} from "../src/replication.js";
import { Replicator } from "../src/role.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe(`migration-8-9`, function () {
	let session: TestSession;
	let db1: EventStore<string, any>, db2: EventStore<string, any>;

	const setup = async (compatibility?: number, order: boolean = false) => {
		session = await TestSession.connected(2, [
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
					compatibility,
					onMessage: async (msg, context) => {
						if (msg instanceof AddedReplicationSegmentMessage) {
							return; // this message type did not exist before
						}
						if (msg instanceof AllReplicatingSegmentsMessage) {
							return; // this message type did not exist before
						}
						if (msg instanceof RequestReplicationInfoMessage) {
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
					compatibility,
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

		await session.stop();
	});

	it("8-9, replicates database of 1 entry", async () => {
		await setup(8);

		const value = "hello";

		await db1.add(value);
		await waitForResolved(() => expect(db2.log.log.length).equal(1));
	});

	it("9-8, replicates database of 1 entry", async () => {
		await setup(8, true);

		const value = "hello";

		await db2.add(value);
		await waitForResolved(() => expect(db1.log.log.length).equal(1));
	});

	it("can turn off old behaviour", async () => {
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

	it("v10+ uses iblt u64", async () => {
		await setup(undefined);
		expect(db1.log.syncronizer).to.be.instanceOf(RatelessIBLTSynchronizer);
		expect(db1.log.domain.resolution).to.equal("u64");
	});
});
