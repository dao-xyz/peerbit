import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import type { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { ReplicationDomainTime } from "../src/replication-domain-time.js";
import { EventStore } from "./utils/stores/event-store.js";

/* import { CountRequest } from "@peerbit/indexer-interface"; */

/**
 * TOOD make these test part of ranges.test.ts
 */

const toEntry = (gid: string | number) => {
	return { meta: { gid: String(gid) } } as Entry<any>;
};

describe(`leaders`, function () {
	let session: TestSession;
	let db1: EventStore<string, typeof ReplicationDomainTime>,
		db2: EventStore<string, typeof ReplicationDomainTime>,
		db3: EventStore<string, typeof ReplicationDomainTime>;

	const options = {
		args: {
			timeUntilRoleMaturity: 0,
			replicas: {
				min: 1,
				max: 10000,
			},
		},
	};
	before(async () => {
		session = await TestSession.connected(3, [
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
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
		]);
	});

	after(async () => {
		await session.stop();
	});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1 && db1.closed === false) await db1.drop();
		if (db2 && db2.closed === false) await db2.drop();
		if (db3 && db3.closed === false) await db3.drop();
	});

	it("select leaders for one or two peers", async () => {
		// TODO fix test timeout, isLeader is too slow as we need to wait for peers
		// perhaps do an event based get peers using the pubsub peers api

		db1 = await session.peers[0].open(
			new EventStore<string, typeof ReplicationDomainTime>(),
			{
				args: { ...options.args, replicate: { offset: 0, factor: 0.5 } },
			},
		);
		const isLeaderAOneLeader = await db1.log.isLeader(toEntry(123), 1);
		expect(isLeaderAOneLeader);
		const isLeaderATwoLeader = await db1.log.isLeader(toEntry(123), 2);
		expect(isLeaderATwoLeader);

		db2 = (await EventStore.open(db1.address!, session.peers[1], {
			args: { ...options.args, replicate: { offset: 0.5, factor: 0.5 } },
		})) as EventStore<string, typeof ReplicationDomainTime>;
	});
});
