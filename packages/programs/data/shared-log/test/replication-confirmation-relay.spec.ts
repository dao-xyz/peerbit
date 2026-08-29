import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import type { TransportMessage } from "../src/message.js";
import { isReplicationInfoV2Message } from "../src/replication.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("replication application confirmation relay recovery", () => {
	let session: TestSession | undefined;
	let source: EventStore<string, any> | undefined;
	let target: EventStore<string, any> | undefined;
	let confirmation: Promise<void> | undefined;
	let releaseBlockedAnnouncements: (() => void) | undefined;

	afterEach(async () => {
		releaseBlockedAnnouncements?.();
		sinon.restore();
		if (source?.closed === false) await source.drop();
		if (target?.closed === false) await target.drop();
		await confirmation?.catch(() => {});
		await session?.stop();
	});

	it("confirms after a delayed relay path and one dropped V2 announcement", async function () {
		this.timeout(60_000);
		session = await TestSession.disconnected(3);
		const store = new EventStore<string, any>();
		source = await session.peers[0].open(store.clone(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		target = await session.peers[2].open(store.clone(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});

		const sourceLog = source.log as any;
		const originalSend = sourceLog.rpc.send.bind(sourceLog.rpc);
		const firstAnnouncementDropped = pDefer<void>();
		const forwardSubsequentAnnouncements = pDefer<void>();
		releaseBlockedAnnouncements = () =>
			forwardSubsequentAnnouncements.resolve();
		let droppedAnnouncements = 0;
		sinon.stub(sourceLog.rpc, "send").callsFake(async (message, options) => {
			if (isReplicationInfoV2Message(message as TransportMessage)) {
				if (droppedAnnouncements === 0) {
					droppedAnnouncements++;
					firstAnnouncementDropped.resolve();
					// Simulate a transport receipt without forwarding this exact frame.
					return [];
				}
				await forwardSubsequentAnnouncements.promise;
			}
			return originalSend(message, options);
		});

		let confirmationResolved = false;
		confirmation = source.log
			.replicate({ factor: 1 }, { confirm: { timeout: 30_000 } })
			.then(() => {
				confirmationResolved = true;
			});
		const sourceHash = source.node.identity.publicKey.hashcode();
		await waitForResolved(async () =>
			expect(
				await source!.log.replicationIndex.count({
					query: { hash: sourceHash },
				}),
			).to.be.greaterThan(0),
		);
		expect(confirmationResolved).to.be.false;
		expect(
			await target.log.replicationIndex.count({
				query: { hash: sourceHash },
			}),
		).to.equal(0);

		// Only now create peer0 -> relay -> peer2. Peer0 and peer2 never gain a
		// direct connection in this test.
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);
		await firstAnnouncementDropped.promise;
		expect(droppedAnnouncements).to.equal(1);
		expect(confirmationResolved).to.be.false;
		expect(
			await target.log.replicationIndex.count({
				query: { hash: sourceHash },
			}),
		).to.equal(0);

		releaseBlockedAnnouncements();
		await confirmation;
		expect(droppedAnnouncements).to.equal(1);
		expect(
			await target.log.replicationIndex.count({
				query: { hash: sourceHash },
			}),
		).to.be.greaterThan(0);
	});
});
