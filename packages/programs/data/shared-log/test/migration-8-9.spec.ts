import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	RequestReplicationInfoMessage,
	ResponseReplicationInfoMessage,
	ResponseRoleMessage,
	StartedReplicating,
} from "../src/replication.js";
import { Replicator } from "../src/role.js";
import { EventStore } from "./utils/stores/event-store.js";

describe(`migration-8-9`, function () {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>;

	const setup = async (compatibility?: number, order: boolean = false) => {
		session = await TestSession.connected(2, [
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
		]);

		const db = new EventStore<string>();

		const createV8 = () => {
			const db1 = db.clone();
			const onMessageDefault = db1.log._onMessage.bind(db1.log);
			return session.peers[0].open(db1, {
				args: {
					replicate: {
						factor: 1,
					},
					compatibility,
					onMessage: async (msg, context) => {
						if (msg instanceof StartedReplicating) {
							return; // this message type did not exist before
						}
						if (msg instanceof ResponseReplicationInfoMessage) {
							return; // this message type did not exist before
						}
						if (msg instanceof RequestReplicationInfoMessage) {
							// TODO we never respond to this message, nor in older version do we need to send it
							// we are keeping this here to mimic the old behaviour
							await db.log.rpc.send(
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
});
