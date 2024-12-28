// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/event-store.js";

/**
 * Tests that are relavent for browser environments
 */

describe(`network`, () => {
	let session: TestSession;
	let db1: EventStore<string, any>, db2: EventStore<string, any>;

	after(async () => {});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		await session.stop();
	});

	it("can replicate entries through relay", async () => {
		session = await TestSession.disconnected(3);

		// peer 3 is relay, and dont connect 1 with 2 directly
		session.peers[0].dial(session.peers[2].getMultiaddrs()[0]);
		session.peers[1].dial(session.peers[2].getMultiaddrs()[0]);

		await session.peers[0].services.blocks.waitFor(session.peers[2].peerId);
		await session.peers[1].services.blocks.waitFor(session.peers[2].peerId);

		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db2 = await await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: {
						factor: 1,
					},
				},
			},
		);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.length === 2);
		expect(
			(await db1.log.log.toArray()).map((x) => x.payload.getValue().value),
		).to.have.members(["hello", "world"]);
		await waitForResolved(() => expect(db2.log.log.length).equal(2));
	});
});
