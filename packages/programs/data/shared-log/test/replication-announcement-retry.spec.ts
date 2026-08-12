import { TestSession } from "@peerbit/test-utils";
import { TimeoutError } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { EventStore } from "./utils/stores/index.js";

// B12: the legacy announcement retry/repair machinery is exercised only by
// explicit pre-v10 compatibility opens, which now reject. This dormant-by-
// default pin survives until the PR-3 outbound ratchet supersedes it with the
// no-legacy-machinery spec.
describe("replication announcement retries", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		sinon.restore();
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	it("keeps legacy announcement retry and repair dormant by default", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = store.log as any;
		const enqueueV2 = sinon.spy(log._v2Send, "enqueue");
		const send = sinon.stub(store.log.rpc, "send").resolves([] as any);
		const snapshot = sinon.spy(log, "getMyReplicationSegments");
		log._announcements.setupReplicationAnnouncementRetryFunction(10);
		log._announcements.setupReplicationAnnouncementRepairFunction(10, 3);

		await log._announcements.sendReplicationAnnouncement({
			added: { segments: [] },
		});
		expect(enqueueV2.calledOnce).to.be.true;
		expect(send.notCalled).to.be.true;
		expect(
			log._announcements.queueCurrentReplicationStateAnnouncementRetry(
				new TimeoutError("legacy retry must stay disabled"),
			),
		).to.be.false;
		log._announcements.queueCurrentReplicationStateAnnouncementRepair();
		expect(log._announcements._announcementRepairBinding.pending).to.be.false;

		log._announcements._announcementRepairBinding.pending = true;
		await log._announcements.repairCurrentReplicationStateAnnouncement();
		expect(log._announcements._announcementRepairBinding.pending).to.be.false;
		log._announcements._replicationAnnouncementRetryPending = true;
		await log._announcements.retryCurrentReplicationStateAnnouncement();
		expect(log._announcements._replicationAnnouncementRetryPending).to.be.false;
		expect(snapshot.notCalled).to.be.true;
		expect(send.notCalled).to.be.true;
	});
});
